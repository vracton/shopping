use std::ops::AddAssign;

use rustc_hash::FxHashMap;

use crate::ingest::{Batches, Click, Event};
use crate::input::{DAY_MS, WEEK_MS};

#[path = "aggregate_cells.rs"]
mod cells;

const SESSION_GAP_MS: i64 = 30 * 60 * 1000;
#[derive(Debug, PartialEq, Eq)]
pub enum CampaignDaily {
    Sparse(FxHashMap<(usize, i32), CampaignStats>),
    Dense {
        first: i32,
        days: usize,
        values: Vec<CampaignStats>,
    },
}

impl Default for CampaignDaily {
    fn default() -> Self {
        Self::Sparse(FxHashMap::default())
    }
}

impl CampaignDaily {
    pub fn dense(first: i32, days: usize, campaigns: usize) -> Self {
        // Fall back for an unusually large dimension. Generated catalogs fit in under 1 MiB per partition.
        if let Some(slots) = days.checked_mul(campaigns).filter(|&size| size <= 40_000) {
            Self::Dense {
                first,
                days,
                values: vec![CampaignStats::default(); slots],
            }
        } else {
            Self::default()
        }
    }

    fn at(&mut self, campaign: usize, day: i32) -> &mut CampaignStats {
        match self {
            Self::Sparse(map) => map.entry((campaign, day)).or_default(),
            Self::Dense {
                first,
                days,
                values,
            } => &mut values[campaign * *days + (day - *first) as usize],
        }
    }

    pub fn for_each(&self, mut visit: impl FnMut((usize, i32), CampaignStats)) {
        match self {
            Self::Sparse(map) => map
                .iter()
                .for_each(|(&key, value)| visit(key, value.clone())),
            Self::Dense {
                first,
                days,
                values,
            } => {
                for (index, value) in values.iter().enumerate() {
                    if value.sessions != 0 || value.assisted_sessions != 0 {
                        visit((index / days, first + (index % days) as i32), value.clone());
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CampaignStats {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assisted_sessions: i64,
}

impl AddAssign for CampaignStats {
    fn add_assign(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchasing_sessions += other.purchasing_sessions;
        self.revenue += other.revenue;
        self.assisted_sessions += other.assisted_sessions;
    }
}

#[derive(Clone, Copy)]
pub enum UserId<'a> {
    Bytes(&'a [u8]),
    Packed(crate::packed::Key),
}

pub trait TimedEvent {
    fn timestamp(&self) -> i64;
    fn purchase(&self) -> (u32, u32);
}

pub trait TimedClick {
    fn timestamp(&self) -> i64;
    fn campaign(&self) -> usize;
}

impl TimedEvent for Event<'_> {
    fn timestamp(&self) -> i64 {
        self.ts
    }
    fn purchase(&self) -> (u32, u32) {
        (self.price, self.category)
    }
}

impl TimedClick for Click<'_> {
    fn timestamp(&self) -> i64 {
        self.ts
    }
    fn campaign(&self) -> usize {
        self.campaign
    }
}

pub struct UserDay<'a> {
    pub user: UserId<'a>,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top_category: Option<u32>,
}

/// The cell sessionizer writes final day counts as soon as the next day
/// begins. Purchases update only the credited row's revenue and category.
pub(crate) trait CellDays {
    type User: Copy;
    fn len(&self) -> usize;
    fn append(&mut self, user: Self::User, day: i32, sessions: i64, events: i64);
    fn purchase(&mut self, index: usize) -> (&mut i64, &mut Option<u32>);
}

impl<'a> CellDays for Vec<UserDay<'a>> {
    type User = UserId<'a>;
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn append(&mut self, user: Self::User, day: i32, sessions: i64, events: i64) {
        self.push(UserDay {
            user,
            day,
            sessions,
            events,
            revenue: 0,
            top_category: None,
        });
    }
    fn purchase(&mut self, index: usize) -> (&mut i64, &mut Option<u32>) {
        let row = &mut self[index];
        (&mut row.revenue, &mut row.top_category)
    }
}

#[derive(Default)]
pub struct Daily<'a> {
    pub users: Vec<UserDay<'a>>,
    pub campaigns: CampaignDaily,
}

struct Session {
    start: i64,
    day: i32,
    revenue: i64,
}

fn day(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}

// With this width any two timestamps in one bin are at most one
// session gap apart. Only the gap from one nonempty bin to the next matters.
const BIN_MS: usize = SESSION_GAP_MS as usize + 1;

#[derive(Clone, Copy, Default)]
struct TimeBin {
    start: u32,
    end: u32,
    count: u32,
    revenue: i64,
    last_purchase: u32,
}

#[derive(Default)]
struct TimeBins {
    slots: Vec<TimeBin>,
    used: Vec<u64>,
    purchases: Vec<(u32, u32, u32)>,
}

impl TimeBins {
    fn add(&mut self, ts: u32, (price, category): (u32, u32)) {
        let index = ts as usize / BIN_MS;
        if index >= self.slots.len() {
            self.slots.resize(index + 1, TimeBin::default());
            self.used.resize(index / 64 + 1, 0);
        }
        let slot = &mut self.slots[index];
        if slot.count == 0 {
            slot.start = ts;
            slot.end = ts;
            self.used[index / 64] |= 1 << (index % 64);
        } else {
            slot.start = slot.start.min(ts);
            slot.end = slot.end.max(ts);
        }
        slot.count += 1;
        if price != 0 {
            slot.revenue += i64::from(price);
            self.purchases.push((price, category, slot.last_purchase));
            slot.last_purchase = self.purchases.len() as u32;
        }
    }
}

#[derive(Default)]
pub struct Person {
    sessions: Vec<Session>,
    categories: FxHashMap<u32, i64>,
    intervals: FxHashMap<usize, (i64, i64)>,
    bins: TimeBins,
    assist_ends: Option<Vec<usize>>,
    cells: Option<cells::Cells>,
}

impl Person {
    pub fn with_assist_sweep(campaigns: usize) -> Self {
        Self {
            assist_ends: Some(vec![0; campaigns]),
            cells: std::env::var_os("SHOPPING_SESSIONIZER")
                .is_some_and(|s| s == "cells" || s == "columns")
                .then(cells::Cells::default),
            ..Self::default()
        }
    }

    pub fn has_cells(&self) -> bool {
        self.cells.is_some()
    }

    pub fn try_cells<'a>(
        &mut self,
        user: UserId<'a>,
        plain: &[u32],
        events: &[impl TimedEvent],
        clicks: &[impl TimedClick],
        organic: usize,
        base: i64,
        output: &mut Daily<'a>,
    ) -> bool {
        self.try_cell_days(
            user,
            plain,
            events,
            clicks,
            organic,
            base,
            &mut output.users,
            &mut output.campaigns,
        )
    }

    pub(crate) fn try_cell_days<O: CellDays>(
        &mut self,
        user: O::User,
        plain: &[u32],
        events: &[impl TimedEvent],
        clicks: &[impl TimedClick],
        organic: usize,
        base: i64,
        days: &mut O,
        campaigns: &mut CampaignDaily,
    ) -> bool {
        let Some(cells) = &mut self.cells else {
            return false;
        };
        cells.run(&mut self.sessions, user, plain, events, base, days);
        self.finish_campaigns(clicks, organic, campaigns);
        true
    }

    pub fn aggregate<'a>(
        &mut self,
        user: UserId<'a>,
        events: &[impl TimedEvent],
        clicks: &[impl TimedClick],
        organic: usize,
        base: i64,
        output: &mut Daily<'a>,
    ) {
        self.sessions.clear();
        let mut previous = events[0].timestamp();
        let mut session = Session {
            start: previous,
            day: day(previous + base),
            revenue: 0,
        };
        let mut totals = new_user_day(user, session.day);
        totals.sessions = 1;
        for event in events {
            let ts = event.timestamp();
            if ts - previous > SESSION_GAP_MS {
                self.sessions.push(session);
                let next_day = day(ts + base);
                if next_day != totals.day {
                    finish_user_day(&mut totals, &mut self.categories);
                    output.users.push(totals);
                    totals = new_user_day(user, next_day);
                }
                totals.sessions += 1;
                session = Session {
                    start: ts,
                    day: next_day,
                    revenue: 0,
                };
            }
            let (price, category) = event.purchase();
            totals.events += 1;
            if price != 0 {
                let price = i64::from(price);
                *self.categories.entry(category).or_default() += price;
                totals.revenue += price;
                session.revenue += price;
            }
            previous = ts;
        }
        self.sessions.push(session);
        finish_user_day(&mut totals, &mut self.categories);
        output.users.push(totals);

        self.finish_campaigns(clicks, organic, &mut output.campaigns);
    }

    pub fn aggregate_binned<'a>(
        &mut self,
        user: UserId<'a>,
        events: &[impl TimedEvent],
        clicks: &[impl TimedClick],
        organic: usize,
        base: i64,
        output: &mut Daily<'a>,
    ) {
        self.sessions.clear();
        self.bins.purchases.clear();
        for event in events {
            self.bins.add(event.timestamp() as u32, event.purchase());
        }

        let mut previous_end = -SESSION_GAP_MS - 1;
        let mut session: Option<Session> = None;
        let mut totals: Option<UserDay<'a>> = None;
        for (word_index, word) in self.bins.used.iter_mut().enumerate() {
            let mut bits = std::mem::take(word);
            while bits != 0 {
                let index = word_index * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let cell = std::mem::take(&mut self.bins.slots[index]);
                let start = i64::from(cell.start);
                if start - previous_end > SESSION_GAP_MS {
                    if let Some(previous) = session.take() {
                        self.sessions.push(previous);
                    }
                    let session_day = day(start + base);
                    if totals.as_ref().is_none_or(|row| row.day != session_day) {
                        if let Some(mut previous) = totals.take() {
                            finish_user_day(&mut previous, &mut self.categories);
                            output.users.push(previous);
                        }
                        totals = Some(new_user_day(user, session_day));
                    }
                    totals.as_mut().unwrap().sessions += 1;
                    session = Some(Session {
                        start,
                        day: session_day,
                        revenue: 0,
                    });
                }
                let user_day = totals.as_mut().unwrap();
                user_day.events += i64::from(cell.count);
                user_day.revenue += cell.revenue;
                session.as_mut().unwrap().revenue += cell.revenue;
                let mut purchase = cell.last_purchase;
                while purchase != 0 {
                    let (price, category, next) = self.bins.purchases[purchase as usize - 1];
                    *self.categories.entry(category).or_default() += i64::from(price);
                    purchase = next;
                }
                previous_end = i64::from(cell.end);
            }
        }
        let mut totals = totals.unwrap();
        finish_user_day(&mut totals, &mut self.categories);
        output.users.push(totals);
        self.sessions.push(session.unwrap());
        self.finish_campaigns(clicks, organic, &mut output.campaigns);
    }

    fn finish_campaigns(
        &mut self,
        clicks: &[impl TimedClick],
        organic: usize,
        totals: &mut CampaignDaily,
    ) {
        if clicks.is_empty() {
            for session in &self.sessions {
                add_credit(session, organic, totals);
            }
        } else if let Some(ends) = &mut self.assist_ends {
            credit(&self.sessions, clicks, organic, totals);
            sweep_assists(&self.sessions, clicks, ends, totals);
        } else {
            credit(&self.sessions, clicks, organic, totals);
            assist(&self.sessions, clicks, &mut self.intervals, totals);
        }
    }
}

/// Both ends of a click's eligible session range advance monotonically.
/// For each campaign its last end is the first potentially uncounted session.
fn sweep_assists(
    sessions: &[Session],
    clicks: &[impl TimedClick],
    assist_ends: &mut [usize],
    totals: &mut CampaignDaily,
) {
    let mut begin = 0;
    let mut end = 0;
    let mut processed = 0;
    for click in clicks {
        let ts = click.timestamp();
        while begin < sessions.len() && sessions[begin].start < ts {
            begin += 1;
        }
        if begin == sessions.len() {
            break;
        }
        let expiry = ts + WEEK_MS;
        let campaign = click.campaign();
        end = end.max(begin);
        while end < sessions.len() && sessions[end].start <= expiry {
            end += 1;
        }
        let mut index = begin.max(assist_ends[campaign]);
        while index < end {
            let first = index;
            let day = sessions[index].day;
            index += 1;
            while index < end && sessions[index].day == day {
                index += 1;
            }
            totals.at(campaign, day).assisted_sessions += (index - first) as i64;
        }
        assist_ends[campaign] = end;
        processed += 1;
    }
    for click in &clicks[..processed] {
        assist_ends[click.campaign()] = 0;
    }
}

fn new_user_day(user: UserId<'_>, day: i32) -> UserDay<'_> {
    UserDay {
        user,
        day,
        sessions: 0,
        events: 0,
        revenue: 0,
        top_category: None,
    }
}

fn finish_user_day(row: &mut UserDay<'_>, totals: &mut FxHashMap<u32, i64>) {
    if !totals.is_empty() {
        let winner = totals
            .drain()
            .max_by_key(|&(category, revenue)| (revenue, std::cmp::Reverse(category)));
        row.top_category = winner.map(|(category, _)| category);
    }
}

fn add_credit(session: &Session, campaign: usize, totals: &mut CampaignDaily) {
    let total = totals.at(campaign, session.day);
    total.sessions += 1;
    total.revenue += session.revenue;
    total.purchasing_sessions += i64::from(session.revenue != 0);
}

fn credit<T: TimedClick>(
    sessions: &[Session],
    clicks: &[T],
    organic: usize,
    totals: &mut CampaignDaily,
) {
    let mut next = 0;
    let mut best: Option<&T> = None;
    for session in sessions {
        while next < clicks.len() && clicks[next].timestamp() <= session.start {
            let click = &clicks[next];
            // Clicks at equal timestamps are ordered by ID, so the first one wins.
            if best.is_none_or(|previous| previous.timestamp() < click.timestamp()) {
                best = Some(click);
            }
            next += 1;
        }
        let campaign = best
            .filter(|click| session.start - click.timestamp() <= WEEK_MS)
            .map_or(organic, |click| click.campaign());
        add_credit(session, campaign, totals);
    }
}

fn add_assists(
    sessions: &[Session],
    campaign: usize,
    start: i64,
    end: i64,
    totals: &mut CampaignDaily,
) {
    let begin = sessions.partition_point(|session| session.start < start);
    let end = sessions.partition_point(|session| session.start <= end);
    let mut matches = &sessions[begin..end];
    while let Some(first) = matches.first() {
        let count = matches.partition_point(|session| session.day == first.day);
        totals.at(campaign, first.day).assisted_sessions += count as i64;
        matches = &matches[count..];
    }
}

fn assist(
    sessions: &[Session],
    clicks: &[impl TimedClick],
    intervals: &mut FxHashMap<usize, (i64, i64)>,
    totals: &mut CampaignDaily,
) {
    let earliest = sessions[0].start - WEEK_MS;
    let latest = sessions.last().unwrap().start;
    let begin = clicks.partition_point(|click| click.timestamp() < earliest);
    for click in clicks[begin..]
        .iter()
        .take_while(|click| click.timestamp() <= latest)
    {
        let interval = intervals
            .entry(click.campaign())
            .or_insert((click.timestamp(), click.timestamp() + WEEK_MS));
        if click.timestamp() > interval.1 {
            add_assists(sessions, click.campaign(), interval.0, interval.1, totals);
            interval.0 = click.timestamp();
        }
        interval.1 = click.timestamp() + WEEK_MS;
    }
    for (campaign, (start, end)) in intervals.drain() {
        add_assists(sessions, campaign, start, end, totals);
    }
}

pub fn aggregate<'a>(events: &[Event<'a>], mut clicks: &[Click<'a>], organic: usize) -> Daily<'a> {
    let mut output = Daily::default();
    let mut scratch = Person::default();
    for events in events.chunk_by(|a, b| a.user == b.user) {
        let user = events[0].user;
        let start = clicks.partition_point(|click| click.user < user);
        clicks = &clicks[start..];
        let end = clicks.partition_point(|click| click.user == user);
        let (matching_clicks, remaining) = clicks.split_at(end);
        clicks = remaining;
        scratch.aggregate(
            UserId::Bytes(user),
            events,
            matching_clicks,
            organic,
            0,
            &mut output,
        );
    }
    output
}

pub fn split_ordered<'a, 'b>(
    ordered: &'b Batches<'a>,
    partitions: usize,
) -> Vec<(&'b [Event<'a>], &'b [Click<'a>])> {
    let mut events = ordered.events.as_slice();
    let mut clicks = ordered.clicks.as_slice();
    let target = events.len().div_ceil(partitions);
    let mut groups = Vec::new();
    while !events.is_empty() {
        let mut end = target.min(events.len());
        let last_user = events[end - 1].user;
        end += events[end..].partition_point(|event| event.user == last_user);
        let click_end = clicks.partition_point(|click| click.user <= last_user);
        let (event_group, event_tail) = events.split_at(end);
        let (click_group, click_tail) = clicks.split_at(click_end);
        groups.push((event_group, click_group));
        events = event_tail;
        clicks = click_tail;
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unordered_time_bins_match_sorted_sessions_and_campaigns() {
        let mut state = 19_u64;
        let mut sample = |limit| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) % limit
        };
        let mut first = Person::default();
        let mut second = Person::default();
        let mut third = Person {
            cells: Some(cells::Cells::default()),
            ..Person::default()
        };
        for round in 0..160_u64 {
            let mut events: Vec<_> = (0..round + 1)
                .map(|_| {
                    let ts = (sample(144) as i64 * SESSION_GAP_MS) + sample(3) as i64 - 1;
                    Event {
                        user: b"u",
                        ts: ts.max(0),
                        price: sample(2) as u32 * 500,
                        category: sample(5) as u32,
                    }
                })
                .collect();
            let mut clicks: Vec<_> = (0..round % 30)
                .map(|_| Click {
                    user: b"u",
                    id: b"k",
                    ts: sample(3 * 24 * 60) as i64 * 60_000,
                    campaign: sample(5) as usize,
                })
                .collect();
            clicks.sort_unstable_by_key(|click| click.ts);
            let mut unordered = Daily::default();
            let mut sorted = Daily::default();
            let base = DAY_MS * 25;
            first.aggregate_binned(
                UserId::Bytes(b"u"),
                &events,
                &clicks,
                5,
                base,
                &mut unordered,
            );
            let mut compact = Daily::default();
            assert!(third.try_cells(
                UserId::Bytes(b"u"),
                &[],
                &events,
                &clicks,
                5,
                base,
                &mut compact
            ));
            let mut split = Daily::default();
            let plain: Vec<_> = events
                .iter()
                .filter(|e| e.price == 0)
                .map(|e| e.ts as u32)
                .collect();
            let purchases: Vec<_> = events.iter().filter(|e| e.price != 0).copied().collect();
            assert!(third.try_cells(
                UserId::Bytes(b"u"),
                &plain,
                &purchases,
                &clicks,
                5,
                base,
                &mut split
            ));
            events.sort_unstable_by_key(|event| event.ts);
            second.aggregate(UserId::Bytes(b"u"), &events, &clicks, 5, base, &mut sorted);
            let summarize = |out: &Daily| {
                out.users
                    .iter()
                    .map(|row| {
                        (
                            row.day,
                            row.sessions,
                            row.events,
                            row.revenue,
                            row.top_category,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(summarize(&unordered), summarize(&sorted), "round {round}");
            assert_eq!(unordered.campaigns, sorted.campaigns, "round {round}");
            assert_eq!(summarize(&compact), summarize(&sorted), "round {round}");
            assert_eq!(compact.campaigns, sorted.campaigns, "round {round}");
            assert_eq!(summarize(&split), summarize(&sorted), "round {round}");
            assert_eq!(split.campaigns, sorted.campaigns, "round {round}");
        }
    }

    #[test]
    fn assisted_counts_and_credit_match_direct_search_under_perturbation() {
        let mut state = 1_u64;
        let mut sample = |limit| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) % limit
        };
        for round in 0..120 {
            let mut sessions: Vec<_> = (0..sample(70) + 1)
                .map(|_| {
                    let start = sample(30 * 24) as i64 * 3_600_000 + sample(2) as i64;
                    Session {
                        start,
                        day: day(start),
                        revenue: sample(2) as i64 * 10,
                    }
                })
                .collect();
            sessions.sort_unstable_by_key(|session| session.start);
            let ids: Vec<_> = (0..sample(170)).map(|i| format!("k{i:03}")).collect();
            let mut clicks: Vec<_> = ids
                .iter()
                .map(|id| {
                    let anchor = sessions[sample(sessions.len() as u64) as usize].start;
                    let ts = match sample(6) {
                        0 => anchor - WEEK_MS,
                        1 => anchor - WEEK_MS - 1,
                        2 => anchor,
                        3 => anchor + 1,
                        _ => sample(37 * 24) as i64 * 3_600_000 - WEEK_MS,
                    };
                    Click {
                        user: b"u",
                        ts,
                        id: id.as_bytes(),
                        campaign: sample(8) as usize,
                    }
                })
                .collect();
            clicks.sort_unstable_by_key(|click| (click.ts, click.id));

            let mut actual = CampaignDaily::default();
            credit(&sessions, &clicks, 8, &mut actual);
            assist(&sessions, &clicks, &mut FxHashMap::default(), &mut actual);
            let mut swept = CampaignDaily::default();
            let mut ends = [0; 9];
            credit(&sessions, &clicks, 8, &mut swept);
            sweep_assists(&sessions, &clicks, &mut ends, &mut swept);
            assert_eq!(ends, [0; 9]);
            assert_eq!(actual, swept, "round {round}");

            let mut expected = CampaignDaily::default();
            for session in &sessions {
                let qualifying = clicks
                    .iter()
                    .filter(|click| (session.start - WEEK_MS..=session.start).contains(&click.ts));
                let winner = qualifying
                    .clone()
                    .min_by_key(|click| (std::cmp::Reverse(click.ts), click.id));
                let campaign = winner.map_or(8, |click| click.campaign);
                let totals = expected.at(campaign, session.day);
                totals.sessions += 1;
                totals.revenue += session.revenue;
                totals.purchasing_sessions += i64::from(session.revenue != 0);
                let campaigns: rustc_hash::FxHashSet<_> =
                    qualifying.map(|click| click.campaign).collect();
                for campaign in campaigns {
                    expected.at(campaign, session.day).assisted_sessions += 1;
                }
            }
            assert_eq!(actual, expected, "round {round}");
        }
    }
}
