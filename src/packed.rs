use std::hash::BuildHasher;
use std::ops::Range;

use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::aggregate::{CampaignDaily, Daily, Person, TimedClick, TimedEvent, UserId};
use crate::catalog::Catalog;
use crate::input::{Calendar, ChunkRows, DAY_MS, RowWindows, WEEK_MS, data_rows, decimal, fields};
use crate::stages::Stages;

const INVALID: u32 = u32::MAX;
const PRICE_MASK: u32 = (1 << 24) - 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64);

impl Key {
    fn pack(id: &[u8]) -> Option<Self> {
        match id.len() {
            8 | 9 => {
                let word = u64::from_be_bytes(id[..8].try_into().unwrap());
                let tail = id.get(8).copied().unwrap_or(0);
                const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
                if (word | word.wrapping_sub(0x0101_0101_0101_0101) & !word) & HIGH_BITS != 0
                    || tail >= 128
                    || (tail == 0 && id.len() == 9)
                {
                    return None;
                }
                #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
                let octet = unsafe { std::arch::x86_64::_pext_u64(word, 0x7f7f_7f7f_7f7f_7f7f) };
                #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
                let octet = {
                    let pairs =
                        (word & 0x007f_007f_007f_007f) | ((word >> 1) & 0x3f80_3f80_3f80_3f80);
                    let quads =
                        (pairs & 0x0000_3fff_0000_3fff) | ((pairs >> 2) & 0x0fff_c000_0fff_c000);
                    (quads & 0x0000_0000_0fff_ffff) | ((quads >> 4) & 0x00ff_ffff_f000_0000)
                };
                Some(Self((octet << 7) | u64::from(tail)))
            }
            1..=7 => {
                let mut result = 0_u64;
                for (index, &byte) in id.iter().enumerate() {
                    if !(1..128).contains(&byte) {
                        return None;
                    }
                    result |= u64::from(byte) << (56 - 7 * index);
                }
                Some(Self(result))
            }
            _ => None,
        }
    }

    pub fn decode(self, buffer: &mut [u8; 9]) -> &str {
        let mut len = 0;
        for (index, byte) in buffer.iter_mut().enumerate() {
            *byte = ((self.0 >> (56 - 7 * index)) & 127) as u8;
            if *byte == 0 {
                break;
            }
            len += 1;
        }
        std::str::from_utf8(&buffer[..len]).unwrap()
    }

    fn hash(self) -> u64 {
        FxBuildHasher.hash_one(self.0)
    }
}

struct Links {
    ids: FxHashMap<Key, usize>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
    names: Vec<Key>,
}

impl Links {
    fn intern(&mut self, name: Key) -> usize {
        *self.ids.entry(name).or_insert_with(|| {
            let index = self.names.len();
            self.names.push(name);
            self.parents.push(index);
            self.sizes.push(1);
            index
        })
    }

    fn root(parents: &mut [usize], mut index: usize) -> usize {
        while index != parents[index] {
            parents[index] = parents[parents[index]];
            index = parents[index];
        }
        index
    }

    fn new<'a>(files: impl Iterator<Item = &'a [u8]>) -> Option<Self> {
        let mut links = Self {
            ids: FxHashMap::default(),
            parents: Vec::new(),
            sizes: Vec::new(),
            names: Vec::new(),
        };
        for [a, b] in files.flat_map(data_rows).filter_map(fields::<2>) {
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let a = links.intern(Key::pack(a)?);
            let b = links.intern(Key::pack(b)?);
            let mut a = Self::root(&mut links.parents, a);
            let mut b = Self::root(&mut links.parents, b);
            if a != b {
                if links.sizes[a] < links.sizes[b] {
                    std::mem::swap(&mut a, &mut b);
                }
                links.parents[b] = a;
                links.sizes[a] += links.sizes[b];
                links.names[a] = links.names[a].min(links.names[b]);
            }
        }
        for index in links.ids.values_mut() {
            *index = Self::root(&mut links.parents, *index);
        }
        links.parents = Vec::new();
        links.sizes = Vec::new();
        Some(links)
    }

    fn resolve(&self, key: Key) -> Key {
        self.ids.get(&key).map_or(key, |&index| self.names[index])
    }
}

#[derive(Clone, Copy, Default)]
struct Candidate {
    event: Key,
    user: Key,
    ingest: u64,
    ts: u32,
    purchase: u32,
}

#[derive(Clone, Copy)]
struct Event {
    user: Key,
    ts: u32,
    purchase: u32,
}

impl TimedEvent for Event {
    fn timestamp(&self) -> i64 {
        i64::from(self.ts)
    }
    fn purchase(&self) -> (u32, u32) {
        (self.purchase & PRICE_MASK, self.purchase >> 24)
    }
}

struct Click {
    user: Key,
    id: Key,
    ts: u32,
    campaign: u32,
}

impl TimedClick for Click {
    fn timestamp(&self) -> i64 {
        i64::from(self.ts)
    }
    fn campaign(&self) -> usize {
        self.campaign as usize
    }
}

struct Parser<'a, 'b> {
    catalog: &'b Catalog<'a>,
    base: i64,
    end: i64,
    start: i64,
    calendar: Calendar,
    cutoff_days: Vec<u64>,
}

impl Parser<'_, '_> {
    fn event(
        &self,
        [id, user, ts, ingest, kind, product, price]: [&[u8]; 7],
    ) -> Option<Option<Candidate>> {
        if id.is_empty() {
            return Some(None);
        }
        let event = Key::pack(id)?;
        let Some(ingest) = Calendar::ingest_code(ingest) else {
            return Some(None);
        };
        let mut candidate = Candidate {
            event,
            ingest,
            ts: INVALID,
            ..Candidate::default()
        };
        if user.is_empty() || product.is_empty() {
            return Some(Some(candidate));
        }
        let Some(ts) = self
            .calendar
            .timestamp(ts)
            .filter(|&t| self.start <= t && t < self.end)
        else {
            return Some(Some(candidate));
        };
        let relative = (ts - self.base) as u32;
        let cutoff = self.cutoff_days[(relative / DAY_MS as u32 + 1) as usize]
            | u64::from((relative % DAY_MS as u32) / 1000);
        if ingest > cutoff {
            return Some(Some(candidate));
        }
        candidate.purchase = match kind {
            b"view" | b"cart" => 0,
            b"purchase" => {
                let Some(price) = decimal(price).filter(|&price| (1..=10_000_000).contains(&price))
                else {
                    return Some(Some(candidate));
                };
                (self.catalog.category(product) as u32) << 24 | price as u32
            }
            _ => return Some(Some(candidate)),
        };
        candidate.user = Key::pack(user)?;
        candidate.ts = relative;
        Some(Some(candidate))
    }

    fn click(&self, [id, user, campaign, ts]: [&[u8]; 4]) -> Option<Option<Click>> {
        if id.is_empty() || user.is_empty() {
            return Some(None);
        }
        let Some(ts) = self
            .calendar
            .timestamp(ts)
            .filter(|&t| self.base <= t && t < self.end)
        else {
            return Some(None);
        };
        let Some(campaign) = self.catalog.campaign(campaign) else {
            return Some(None);
        };
        Some(Some(Click {
            user: Key::pack(user)?,
            id: Key::pack(id)?,
            ts: (ts - self.base) as u32,
            campaign: campaign as u32,
        }))
    }
}

fn bucket(key: Key, count: usize) -> usize {
    ((key.hash() >> 32) as usize) & (count - 1)
}

fn buckets<T>(count: usize) -> Vec<Vec<T>> {
    (0..count).map(|_| Vec::new()).collect()
}

fn transpose<T>(source: Vec<Vec<Vec<T>>>, count: usize) -> Vec<Vec<Vec<T>>> {
    let mut dest = buckets(count);
    for group in source {
        for (dest, block) in dest.iter_mut().zip(group) {
            if !block.is_empty() {
                dest.push(block);
            }
        }
    }
    dest
}

fn parse<T: Send, const N: usize>(
    chunks: &[&[u8]],
    count: usize,
    window_rows: bool,
    parse: impl Fn([&[u8]; N]) -> Option<Option<T>> + Sync,
    key: impl Fn(&T) -> Key + Sync,
) -> Option<Vec<Vec<Vec<T>>>> {
    let group_size = chunks
        .len()
        .div_ceil(rayon::current_num_threads() * 8)
        .max(1);
    let partial = chunks
        .par_chunks(group_size)
        .map(|chunks| {
            let mut output = buckets(count);
            for chunk in chunks {
                let mut push = |row| {
                    if let Some(item) = parse(row)? {
                        output[bucket(key(&item), count)].push(item);
                    }
                    Some(())
                };
                if window_rows {
                    for row in RowWindows::new(chunk) {
                        push(row)?;
                    }
                } else {
                    for row in ChunkRows::new(chunk) {
                        push(row)?;
                    }
                }
            }
            Some(output)
        })
        .collect::<Option<Vec<_>>>()?;
    Some(transpose(partial, count))
}

fn dedup(groups: Vec<Vec<Candidate>>, links: &Links, count: usize) -> Vec<Vec<Event>> {
    let rows: usize = groups.iter().map(Vec::len).sum();
    let mut output = buckets(count);
    if rows == 0 {
        return output;
    }
    let mut table = hashbrown::HashTable::with_capacity(rows);
    for candidate in groups.into_iter().flatten() {
        let hash = candidate.event.hash();
        match table.entry(
            hash,
            |old: &Candidate| old.event == candidate.event,
            |old| old.event.hash(),
        ) {
            hashbrown::hash_table::Entry::Occupied(mut entry) => {
                if candidate.ingest < entry.get().ingest {
                    *entry.get_mut() = candidate;
                }
            }
            hashbrown::hash_table::Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
        }
    }
    route(table.into_iter(), links, &mut output);
    output
}

/// Store only a fingerprint and a row coordinate in the dedup table.
/// The first pass marks losers in place. The second reads winners sequentially.
fn dedup_indexed(mut groups: Vec<Vec<Candidate>>, links: &Links, count: usize) -> Vec<Vec<Event>> {
    const ROW_BITS: u32 = 20;
    const ROW_MASK: u32 = (1 << ROW_BITS) - 1;
    if groups.len() >= (1 << (32 - ROW_BITS))
        || groups.iter().any(|group| group.len() > ROW_MASK as usize)
    {
        return dedup(groups, links, count);
    }
    let total: usize = groups.iter().map(Vec::len).sum();
    let mut output = buckets(count);
    if total == 0 {
        return output;
    }
    let mut slots = vec![0_u64; (total + total / 2 + 1).next_power_of_two()];
    let mask = slots.len() - 1;
    for group in 0..groups.len() {
        for index in 0..groups[group].len() {
            let row = groups[group][index];
            let hash = row.event.hash();
            let location = ((group as u32) << ROW_BITS) | index as u32;
            // The low word is biased by one, so zero is always an empty slot.
            let fingerprint = (hash & 0xffff_ffff_0000_0000) | u64::from(location + 1);
            let mut slot = hash as usize & mask;
            loop {
                let next = slots[slot];
                if next == 0 {
                    slots[slot] = fingerprint;
                    break;
                }
                if next >> 32 == fingerprint >> 32 {
                    let old = next as u32 - 1;
                    let old_group = (old >> ROW_BITS) as usize;
                    let old_index = (old & ROW_MASK) as usize;
                    let chosen = &mut groups[old_group][old_index];
                    if chosen.event == row.event {
                        if row.ingest < chosen.ingest {
                            chosen.ts = INVALID;
                            slots[slot] = fingerprint;
                        } else {
                            groups[group][index].ts = INVALID;
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
    drop(slots);
    route(groups.into_iter().flatten(), links, &mut output);
    output
}

fn route(rows: impl Iterator<Item = Candidate>, links: &Links, output: &mut [Vec<Event>]) {
    // Frequent aliases can bypass the larger catalog table.
    let mut cache = [(Key(0), Key(0)); 1024];
    let count = output.len();
    for row in rows.filter(|row| row.ts != INVALID) {
        let tag = row.user.hash();
        let entry = &mut cache[tag as usize & 1023];
        let user = if entry.0 == row.user {
            entry.1
        } else {
            let canonical = links.resolve(row.user);
            *entry = (row.user, canonical);
            canonical
        };
        output[bucket(user, count)].push(Event {
            user,
            ts: row.ts,
            purchase: row.purchase,
        });
    }
}

fn flatten<T>(groups: Vec<Vec<T>>) -> Vec<T> {
    let mut output = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for mut group in groups {
        output.append(&mut group)
    }
    output
}

struct TimelineClick {
    ts: u32,
    campaign: u32,
    id: Key,
}

impl TimedClick for TimelineClick {
    fn timestamp(&self) -> i64 {
        i64::from(self.ts)
    }
    fn campaign(&self) -> usize {
        self.campaign as usize
    }
}

struct TimelineEvent(u64);

impl TimedEvent for TimelineEvent {
    fn timestamp(&self) -> i64 {
        (self.0 >> 32) as i64
    }
    fn purchase(&self) -> (u32, u32) {
        let packed = self.0 as u32;
        (packed & PRICE_MASK, packed >> 24)
    }
}

type BucketPair = (Vec<Vec<Event>>, Vec<Vec<Click>>);

struct Job {
    buckets: Vec<BucketPair>,
    weight: usize,
}

fn coalesce(events: Vec<Vec<Vec<Event>>>, clicks: Vec<Vec<Vec<Click>>>) -> Vec<Job> {
    let mut buckets: Vec<_> = events
        .into_iter()
        .zip(clicks)
        .filter_map(|(events, clicks)| {
            let event_rows: usize = events.iter().map(Vec::len).sum();
            let click_rows: usize = clicks.iter().map(Vec::len).sum();
            (event_rows != 0).then_some((event_rows + click_rows, (events, clicks)))
        })
        .collect();
    buckets.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
    let mut jobs: Vec<_> = (0..rayon::current_num_threads().min(buckets.len()))
        .map(|_| Job {
            buckets: Vec::new(),
            weight: 0,
        })
        .collect();
    for (weight, bucket) in buckets {
        let job = jobs.iter_mut().min_by_key(|job| job.weight).unwrap();
        job.weight += weight;
        job.buckets.push(bucket);
    }
    jobs
}

/// Grouping writes a compact append-only arena. A person stores just two heads,
/// so it never allocates its own vector or requires a partition-wide sort.
#[derive(Default)]
struct TimelineGroups {
    people: FxHashMap<Key, usize>,
    heads: Vec<(Key, u32, u32)>,
    times: Vec<(u64, u32)>,
    touches: Vec<(TimelineClick, u32)>,
    person_events: Vec<TimelineEvent>,
    person_clicks: Vec<TimelineClick>,
    scratch: Person,
}

impl TimelineGroups {
    fn run(
        &mut self,
        (events, clicks): BucketPair,
        organic: usize,
        base: i64,
        out: &mut Daily<'static>,
    ) {
        let n: usize = events.iter().map(Vec::len).sum();
        let c: usize = clicks.iter().map(Vec::len).sum();
        if n >= INVALID as usize || c >= INVALID as usize {
            aggregate_sorted((events, clicks), organic, base, out);
            return;
        }
        self.people.clear();
        self.heads.clear();
        self.times.clear();
        self.touches.clear();
        self.times.reserve(n);
        self.touches.reserve(c);
        self.people.reserve(n / 24);
        for event in events.into_iter().flatten() {
            let person = *self.people.entry(event.user).or_insert_with(|| {
                let at = self.heads.len();
                self.heads.push((event.user, INVALID, INVALID));
                at
            });
            let head = &mut self.heads[person].1;
            let position = self.times.len() as u32;
            self.times.push((
                (u64::from(event.ts) << 32) | u64::from(event.purchase),
                *head,
            ));
            *head = position;
        }
        for click in clicks.into_iter().flatten() {
            if let Some(&person) = self.people.get(&click.user) {
                let head = &mut self.heads[person].2;
                let position = self.touches.len() as u32;
                self.touches.push((
                    TimelineClick {
                        ts: click.ts,
                        campaign: click.campaign,
                        id: click.id,
                    },
                    *head,
                ));
                *head = position;
            }
        }
        for &(user, mut events, mut clicks) in &self.heads {
            self.person_events.clear();
            self.person_clicks.clear();
            while events != INVALID {
                let (word, next) = self.times[events as usize];
                self.person_events.push(TimelineEvent(word));
                events = next;
            }
            while clicks != INVALID {
                let (ref click, next) = self.touches[clicks as usize];
                self.person_clicks.push(TimelineClick {
                    ts: click.ts,
                    campaign: click.campaign,
                    id: click.id,
                });
                clicks = next;
            }
            self.person_events.sort_unstable_by_key(|event| event.0);
            self.person_clicks
                .sort_unstable_by_key(|click| (click.ts, click.id));
            self.scratch.aggregate(
                UserId::Packed(user),
                &self.person_events,
                &self.person_clicks,
                organic,
                base,
                out,
            );
        }
    }
}

fn aggregate_sorted(
    (events, clicks): BucketPair,
    organic: usize,
    base: i64,
    daily: &mut Daily<'static>,
) {
    let mut events = flatten(events);
    let mut clicks = flatten(clicks);
    events.sort_unstable_by_key(|row| (row.user, row.ts));
    clicks.sort_unstable_by_key(|row| (row.user, row.ts, row.id));
    let mut clicks = clicks.as_slice();
    let mut scratch = Person::default();
    for events in events.chunk_by(|a, b| a.user == b.user) {
        let user = events[0].user;
        clicks = &clicks[clicks.partition_point(|row| row.user < user)..];
        let (matches, remainder) = clicks.split_at(clicks.partition_point(|row| row.user == user));
        clicks = remainder;
        scratch.aggregate(UserId::Packed(user), events, matches, organic, base, daily);
    }
}

pub struct Parsed {
    events: Vec<Vec<Vec<Candidate>>>,
    clicks: Vec<Vec<Click>>,
    links: Links,
    base: i64,
    first_day: i32,
    days: usize,
}

pub fn parse_inputs<'a>(
    events: &[&[u8]],
    clicks: &[&[u8]],
    link_files: impl Iterator<Item = &'a [u8]> + Send,
    catalog: &Catalog<'_>,
    window: Range<i64>,
    count: usize,
    dedup_count: usize,
    indexed: bool,
) -> Option<Parsed> {
    let base = (window.start - WEEK_MS).div_euclid(DAY_MS) * DAY_MS;
    if !count.is_power_of_two()
        || !dedup_count.is_power_of_two()
        || window.end - base >= i64::from(INVALID)
        || catalog.categories.len() > 256
    {
        return None;
    }
    let calendar = Calendar::new(window.start);
    let cutoff_days = calendar.cutoff_days(base, window.end, window.start)?;
    let parser = Parser {
        catalog,
        base,
        start: window.start,
        calendar,
        cutoff_days,
        end: window.end,
    };
    let window_rows =
        indexed && std::env::var_os("SHOPPING_SCANNER").is_none_or(|mode| mode != "block");
    let ((events, clicks), links) = rayon::join(
        || {
            rayon::join(
                || {
                    parse(
                        events,
                        dedup_count,
                        window_rows,
                        |r| parser.event(r),
                        |item| item.event,
                    )
                },
                || {
                    clicks
                        .par_iter()
                        .map(|chunk| {
                            let mut output = Vec::new();
                            let mut push = |row| {
                                if let Some(click) = parser.click(row)? {
                                    output.push(click);
                                }
                                Some(())
                            };
                            if window_rows {
                                for row in RowWindows::new(chunk) {
                                    push(row)?;
                                }
                            } else {
                                for row in ChunkRows::new(chunk) {
                                    push(row)?;
                                }
                            }
                            Some(output)
                        })
                        .collect::<Option<Vec<_>>>()
                },
            )
        },
        || Links::new(link_files),
    );
    Some(Parsed {
        events: events?,
        clicks: clicks?,
        links: links?,
        base,
        first_day: window.start.div_euclid(DAY_MS) as i32,
        days: ((window.end - 1).div_euclid(DAY_MS) - window.start.div_euclid(DAY_MS) + 1) as usize,
    })
}

impl Parsed {
    pub fn aggregate(
        self,
        count: usize,
        organic: usize,
        indexed: bool,
        stages: &mut Stages,
    ) -> Vec<Daily<'static>> {
        let Self {
            events,
            clicks,
            links,
            base,
            first_day,
            days,
        } = self;
        let choose = if indexed { dedup_indexed } else { dedup };
        let (events, clicks) = rayon::join(
            || {
                events
                    .into_par_iter()
                    .map(|group| choose(group, &links, count))
                    .collect()
            },
            || {
                clicks
                    .into_par_iter()
                    .map(|group| {
                        let mut output = buckets(count);
                        for mut click in group {
                            click.user = links.resolve(click.user);
                            output[bucket(click.user, count)].push(click);
                        }
                        output
                    })
                    .collect()
            },
        );
        let events = transpose(events, count);
        let clicks = transpose(clicks, count);
        stages.mark("packed_dedup_and_partition");
        if indexed {
            let linked = std::env::var_os("SHOPPING_TIMELINES").is_none_or(|value| value != "sort");
            coalesce(events, clicks)
                .into_par_iter()
                .map(|job| {
                    let mut daily = Daily {
                        users: Vec::new(),
                        campaigns: CampaignDaily::dense(first_day, days, organic + 1),
                    };
                    let mut grouping = TimelineGroups::default();
                    for pair in job.buckets {
                        if linked {
                            grouping.run(pair, organic, base, &mut daily);
                        } else {
                            aggregate_sorted(pair, organic, base, &mut daily);
                        }
                    }
                    daily
                })
                .collect()
        } else {
            events
                .into_par_iter()
                .zip(clicks)
                .map(|pair| {
                    let mut daily = Daily {
                        users: Vec::new(),
                        campaigns: CampaignDaily::dense(first_day, days, organic + 1),
                    };
                    aggregate_sorted(pair, organic, base, &mut daily);
                    daily
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Key;

    #[test]
    fn identifiers_round_trip_and_sort_exactly() {
        let ids: Vec<&[u8]> = vec![
            b"A",
            b"a",
            b"a ",
            b"a1234567",
            b"a12345678",
            b"a12345679",
            b"z99999999",
            b" G\r",
        ];
        for a in &ids {
            let encoded = Key::pack(a).unwrap();
            assert_eq!(encoded.decode(&mut [0; 9]).as_bytes(), *a);
            for b in &ids {
                assert_eq!(a.cmp(b), encoded.cmp(&Key::pack(b).unwrap()));
            }
        }
        for id in [&b"a\0bcdeff"[..], b"0123456789", "é".as_bytes()] {
            assert_eq!(Key::pack(id), None);
        }
    }
}
