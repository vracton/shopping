use std::ops::AddAssign;

use rustc_hash::FxHashMap;

use crate::ingest::{Batches, Click, Event};
use crate::input::{DAY_MS, WEEK_MS};

const SESSION_GAP_MS: i64 = 30 * 60 * 1000;
pub type CampaignDaily = FxHashMap<(usize, i32), CampaignStats>;

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

pub struct UserDay<'a> {
    pub user: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top_category: Option<u32>,
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

fn add_session(
    user: &mut UserDay<'_>,
    events: &[Event<'_>],
    category_totals: &mut FxHashMap<u32, i64>,
) -> Session {
    let mut revenue = 0;
    for event in events.iter().filter(|event| event.price != 0) {
        let price = i64::from(event.price);
        *category_totals.entry(event.category).or_default() += price;
        revenue += price;
    }
    user.sessions += 1;
    user.events += events.len() as i64;
    user.revenue += revenue;
    Session {
        start: events[0].ts,
        day: user.day,
        revenue,
    }
}

fn create_sessions<'a>(
    events: &[Event<'a>],
    sessions: &mut Vec<Session>,
    user_days: &mut Vec<UserDay<'a>>,
    category_totals: &mut FxHashMap<u32, i64>,
) {
    sessions.clear();
    let groups = events.chunk_by(|a, b| b.ts - a.ts <= SESSION_GAP_MS);
    for group in groups {
        let start = group[0].ts;
        let session_day = day(start);
        if user_days
            .last()
            .is_none_or(|row| row.user != group[0].user || row.day != session_day)
        {
            if let Some(row) = user_days.last_mut() {
                finish_user_day(row, category_totals);
            }
            user_days.push(UserDay {
                user: group[0].user,
                day: session_day,
                sessions: 0,
                events: 0,
                revenue: 0,
                top_category: None,
            });
        }
        sessions.push(add_session(
            user_days.last_mut().unwrap(),
            group,
            category_totals,
        ));
    }
    if let Some(row) = user_days.last_mut() {
        finish_user_day(row, category_totals);
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

fn credit(sessions: &[Session], clicks: &[Click<'_>], organic: usize, totals: &mut CampaignDaily) {
    let mut next = 0;
    let mut best: Option<&Click> = None;
    for session in sessions {
        while next < clicks.len() && clicks[next].ts <= session.start {
            let click = &clicks[next];
            // Clicks at equal timestamps are ordered by ID, so the first one wins.
            if best.is_none_or(|previous| previous.ts < click.ts) {
                best = Some(click);
            }
            next += 1;
        }
        let campaign = best
            .filter(|click| session.start - click.ts <= WEEK_MS)
            .map_or(organic, |click| click.campaign);
        let total = totals.entry((campaign, session.day)).or_default();
        total.sessions += 1;
        total.revenue += session.revenue;
        total.purchasing_sessions += i64::from(session.revenue != 0);
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
        totals
            .entry((campaign, first.day))
            .or_default()
            .assisted_sessions += count as i64;
        matches = &matches[count..];
    }
}

fn assist(
    sessions: &[Session],
    clicks: &[Click<'_>],
    intervals: &mut FxHashMap<usize, (i64, i64)>,
    totals: &mut CampaignDaily,
) {
    let earliest = sessions[0].start - WEEK_MS;
    let latest = sessions.last().unwrap().start;
    let begin = clicks.partition_point(|click| click.ts < earliest);
    for click in clicks[begin..]
        .iter()
        .take_while(|click| click.ts <= latest)
    {
        let interval = intervals
            .entry(click.campaign)
            .or_insert((click.ts, click.ts + WEEK_MS));
        if click.ts > interval.1 {
            add_assists(sessions, click.campaign, interval.0, interval.1, totals);
            interval.0 = click.ts;
        }
        interval.1 = click.ts + WEEK_MS;
    }
    for (campaign, (start, end)) in intervals.drain() {
        add_assists(sessions, campaign, start, end, totals);
    }
}

pub fn aggregate<'a>(events: &[Event<'a>], mut clicks: &[Click<'a>], organic: usize) -> Daily<'a> {
    let mut output = Daily::default();
    let mut sessions = Vec::new();
    let mut category_totals = FxHashMap::default();
    let mut intervals = FxHashMap::default();
    for events in events.chunk_by(|a, b| a.user == b.user) {
        let user = events[0].user;
        let start = clicks.partition_point(|click| click.user < user);
        clicks = &clicks[start..];
        let end = clicks.partition_point(|click| click.user == user);
        let (matching_clicks, remaining) = clicks.split_at(end);
        clicks = remaining;
        create_sessions(
            events,
            &mut sessions,
            &mut output.users,
            &mut category_totals,
        );
        credit(&sessions, matching_clicks, organic, &mut output.campaigns);
        assist(
            &sessions,
            matching_clicks,
            &mut intervals,
            &mut output.campaigns,
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

            let mut expected = CampaignDaily::default();
            for session in &sessions {
                let qualifying = clicks
                    .iter()
                    .filter(|click| (session.start - WEEK_MS..=session.start).contains(&click.ts));
                let winner = qualifying
                    .clone()
                    .min_by_key(|click| (std::cmp::Reverse(click.ts), click.id));
                let campaign = winner.map_or(8, |click| click.campaign);
                let totals = expected.entry((campaign, session.day)).or_default();
                totals.sessions += 1;
                totals.revenue += session.revenue;
                totals.purchasing_sessions += i64::from(session.revenue != 0);
                let campaigns: rustc_hash::FxHashSet<_> =
                    qualifying.map(|click| click.campaign).collect();
                for campaign in campaigns {
                    expected
                        .entry((campaign, session.day))
                        .or_default()
                        .assisted_sessions += 1;
                }
            }
            assert_eq!(actual, expected, "round {round}");
        }
    }
}
