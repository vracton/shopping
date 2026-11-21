use std::hash::BuildHasher;
use std::ops::Range;

use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::aggregate::{CampaignDaily, Daily, Person, TimedClick, TimedEvent, UserId};
use crate::catalog::Catalog;
use crate::fast_time::FastTime;
use crate::input::{Calendar, ChunkRows, DAY_MS, RowWindows, WEEK_MS, data_rows, decimal, fields};
use crate::stages::Stages;

#[path = "packed_links.rs"]
mod sparse_links;
#[path = "packed_route.rs"]
mod split;

#[path = "packed_claims.rs"]
mod claims;

const INVALID: u32 = u32::MAX;
const PRICE_MASK: u32 = (1 << 24) - 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64);

impl Key {
    #[inline(always)]
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

    #[inline(always)]
    fn hash(self) -> u64 {
        FxBuildHasher.hash_one(self.0)
    }
}

struct Links {
    ids: FxHashMap<Key, Key>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
    names: Vec<Key>,
}

impl Links {
    fn intern(&mut self, name: Key) -> usize {
        self.ids
            .entry(name)
            .or_insert_with(|| {
                let index = self.names.len();
                self.names.push(name);
                self.parents.push(index);
                self.sizes.push(1);
                Key(index as u64)
            })
            .0 as usize
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
            *index = links.names[Self::root(&mut links.parents, index.0 as usize)];
        }
        links.parents = Vec::new();
        links.sizes = Vec::new();
        links.names = Vec::new();
        Some(links)
    }

    fn resolve(&self, key: Key) -> Key {
        self.ids.get(&key).copied().unwrap_or(key)
    }
}

enum Aliases {
    Serial(Links),
    Sharded(Vec<FxHashMap<Key, Key>>),
    Sparse(sparse_links::Sparse),
}

impl Aliases {
    fn resolve(&self, key: Key) -> Key {
        match self {
            Self::Serial(links) => links.resolve(key),
            Self::Sparse(links) => links.resolve(key),
            Self::Sharded(parts) => parts[bucket(key, parts.len())]
                .get(&key)
                .copied()
                .unwrap_or(key),
        }
    }

    fn parallel(chunks: &[&[u8]]) -> Option<Self> {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

        const PARTS: usize = 32;
        let edges: Vec<Vec<(Key, Key)>> = chunks
            .par_iter()
            .map(|chunk| {
                let mut edges = Vec::with_capacity(chunk.len() / 20);
                for [a, b] in RowWindows::<2>::new(chunk) {
                    if !a.is_empty() && !b.is_empty() {
                        edges.push((Key::pack(a)?, Key::pack(b)?));
                    }
                }
                Some(edges)
            })
            .collect::<Option<Vec<_>>>()?;
        let names: Vec<Vec<Vec<Key>>> = edges
            .par_iter()
            .map(|part| {
                let mut names = buckets(PARTS);
                for &(a, b) in part {
                    names[bucket(a, PARTS)].push(a);
                    names[bucket(b, PARTS)].push(b);
                }
                names
            })
            .collect();
        let local: Vec<_> = transpose(names, PARTS)
            .into_par_iter()
            .map(|part| {
                let mut names = flatten(part);
                names.sort_unstable();
                names.dedup();
                names
            })
            .collect();
        let mut offsets = Vec::with_capacity(PARTS);
        let mut total = 0_usize;
        for part in &local {
            offsets.push(total);
            total += part.len();
        }
        let limit = u32::try_from(total).ok()?;
        let indices: Vec<FxHashMap<Key, u32>> = local
            .par_iter()
            .zip(offsets)
            .map(|(part, base)| {
                part.iter()
                    .enumerate()
                    .map(|(offset, &key)| (key, (base + offset) as u32))
                    .collect()
            })
            .collect();
        let names = flatten(local);
        let parent: Vec<_> = (0..limit).map(AtomicU32::new).collect();
        let root = |mut node: u32| {
            loop {
                let next = parent[node as usize].load(Relaxed);
                if next == node {
                    break node;
                }
                node = next;
            }
        };
        let index = |key| indices[bucket(key, PARTS)][&key];
        edges.par_iter().for_each(|part| {
            for &(left, right) in part {
                let (mut a, mut b) = (index(left), index(right));
                loop {
                    a = root(a);
                    b = root(b);
                    if a == b {
                        break;
                    }
                    // Every successful join points to a strictly smaller byte key.
                    if names[a as usize] < names[b as usize] {
                        std::mem::swap(&mut a, &mut b);
                    }
                    if parent[a as usize]
                        .compare_exchange_weak(a, b, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        });
        let output = indices
            .into_par_iter()
            .map(|part| {
                part.into_iter()
                    .map(|(key, index)| (key, names[root(index) as usize]))
                    .collect()
            })
            .collect();
        Some(Self::Sharded(output))
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
    fast_time: Option<FastTime>,
}

impl Parser<'_, '_> {
    #[inline(always)]
    fn timestamp(&self, bytes: &[u8]) -> Option<i64> {
        if let Some(fast) = &self.fast_time {
            fast.timestamp(bytes, &self.calendar)
        } else {
            self.calendar.timestamp(bytes)
        }
    }

    #[inline(always)]
    fn event(
        &self,
        [id, user, ts, ingest, kind, product, price]: [&[u8]; 7],
    ) -> Option<Option<Candidate>> {
        if id.is_empty() {
            return Some(None);
        }
        let event = Key::pack(id)?;
        let code = if self.fast_time.is_some() {
            FastTime::ingest(ingest)
        } else {
            Calendar::ingest_code(ingest)
        };
        let Some(ingest) = code else {
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
            .timestamp(ts)
            .filter(|&t| self.start <= t && t < self.end)
        else {
            return Some(Some(candidate));
        };
        let relative = (ts - self.base) as u32;
        let late = if let Some(fast) = &self.fast_time {
            fast.is_late(ingest, relative)
        } else {
            let cutoff = self.cutoff_days[(relative / DAY_MS as u32 + 1) as usize]
                | u64::from((relative % DAY_MS as u32) / 1000);
            ingest > cutoff
        };
        if late {
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

    #[inline(always)]
    fn click(&self, [id, user, campaign, ts]: [&[u8]; 4]) -> Option<Option<Click>> {
        if id.is_empty() || user.is_empty() {
            return Some(None);
        }
        let Some(ts) = self
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
            let capacity = chunks.iter().map(|bytes| bytes.len()).sum::<usize>() / 74 / count;
            let mut output: Vec<_> = (0..count).map(|_| Vec::with_capacity(capacity)).collect();
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

fn dedup(groups: Vec<Vec<Candidate>>, links: &Aliases, count: usize) -> Vec<Vec<Event>> {
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
fn dedup_indexed(
    mut groups: Vec<Vec<Candidate>>,
    links: &Aliases,
    count: usize,
) -> Vec<Vec<Event>> {
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
    route_groups(groups, links, &mut output);
    output
}

/// An exact duplicate filter. A clear collision bit proves a row's hash
/// position was used once. Every ambiguous row still gets full-key dedup.
fn dedup_filtered(
    mut groups: Vec<Vec<Candidate>>,
    links: &Aliases,
    count: usize,
) -> Vec<Vec<Event>> {
    const ROW_BITS: u32 = 20;
    const ROW_MASK: u32 = (1 << ROW_BITS) - 1;
    if groups.len() >= (1 << (32 - ROW_BITS))
        || groups.iter().any(|group| group.len() > ROW_MASK as usize)
    {
        return dedup(groups, links, count);
    }
    let rows: usize = groups.iter().map(Vec::len).sum();
    let mut output = buckets(count);
    if rows == 0 {
        return output;
    }
    let words = rows.div_ceil(4).next_power_of_two();
    let mut occupied = vec![0_u64; words];
    let mut collisions = vec![0_u64; words];
    let marker = |hash: u64| (((hash >> 6) as usize) & (words - 1), 1_u64 << (hash & 63));
    for row in groups.iter().flatten() {
        let (word, bit) = marker(row.event.hash());
        let previous = occupied[word];
        collisions[word] |= previous & bit;
        occupied[word] = previous | bit;
    }
    drop(occupied);
    let ambiguous: usize = collisions
        .iter()
        .map(|word| word.count_ones() as usize)
        .sum();
    let mut duplicates = hashbrown::HashTable::with_capacity(ambiguous * 2);
    for group in 0..groups.len() {
        for index in 0..groups[group].len() {
            let row = groups[group][index];
            let hash = row.event.hash();
            let (word, bit) = marker(hash);
            if collisions[word] & bit == 0 {
                continue;
            }
            let read = |location: u32| {
                &groups[(location >> ROW_BITS) as usize][(location & ROW_MASK) as usize]
            };
            let position = ((group as u32) << ROW_BITS) | index as u32;
            match duplicates.entry(
                hash,
                |previous: &u32| read(*previous).event == row.event,
                |previous| read(*previous).event.hash(),
            ) {
                hashbrown::hash_table::Entry::Vacant(entry) => {
                    entry.insert(position);
                }
                hashbrown::hash_table::Entry::Occupied(mut entry) => {
                    let previous = *entry.get();
                    let chosen = &mut groups[(previous >> ROW_BITS) as usize]
                        [(previous & ROW_MASK) as usize];
                    if row.ingest < chosen.ingest {
                        chosen.ts = INVALID;
                        *entry.get_mut() = position;
                    } else {
                        groups[group][index].ts = INVALID;
                    }
                }
            }
        }
    }
    drop(duplicates);
    route_groups(groups, links, &mut output);
    output
}

fn route_groups(groups: Vec<Vec<Candidate>>, links: &Aliases, output: &mut [Vec<Event>]) {
    if let Aliases::Sparse(table) = links {
        let count = output.len();
        for group in groups {
            for block in group.chunks(24) {
                for row in block {
                    table.prefetch(row.user);
                }
                for row in block.iter().filter(|row| row.ts != INVALID) {
                    let user = table.resolve(row.user);
                    output[bucket(user, count)].push(Event {
                        user,
                        ts: row.ts,
                        purchase: row.purchase,
                    });
                }
            }
        }
    } else {
        route(groups.into_iter().flatten(), links, output);
    }
}

fn route(rows: impl Iterator<Item = Candidate>, links: &Aliases, output: &mut [Vec<Event>]) {
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

fn flatten<T>(mut groups: Vec<Vec<T>>) -> Vec<T> {
    if groups.len() == 1 {
        return groups.pop().unwrap();
    }
    let mut output = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for mut group in groups {
        output.append(&mut group)
    }
    output
}

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
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

/// Count each shopper's records, then write numeric payloads into contiguous
/// ranges. A range is sorted independently, with no partition-wide key sort.
#[derive(Default)]
struct CountedTimelines {
    ordinals: FxHashMap<Key, usize>,
    names: Vec<Key>,
    locations: Vec<usize>,
    counts: Vec<usize>,
    click_destinations: Vec<(usize, TimelineClick)>,
    click_counts: Vec<usize>,
    cursor: Vec<usize>,
    click_cursor: Vec<usize>,
    ordered_events: Vec<TimelineEvent>,
    ordered_clicks: Vec<TimelineClick>,
    scratch: Person,
    bin_min: usize,
}

fn starts(counts: &[usize], offsets: &mut Vec<usize>) {
    let mut total = 0;
    offsets.clear();
    offsets.extend(counts.iter().map(|&count| {
        let output = total;
        total += count;
        output
    }));
}

impl CountedTimelines {
    fn run(
        &mut self,
        (events, clicks): BucketPair,
        organic: usize,
        base: i64,
        out: &mut Daily<'static>,
    ) {
        let rows = events.iter().map(Vec::len).sum();
        self.names.clear();
        self.ordinals.clear();
        self.counts.clear();
        self.locations.clear();
        self.locations.reserve(rows);
        self.ordinals.reserve(rows / 24);
        for event in events.iter().flatten() {
            let ordinal = *self.ordinals.entry(event.user).or_insert_with(|| {
                let at = self.names.len();
                self.names.push(event.user);
                self.counts.push(0);
                at
            });
            self.counts[ordinal] += 1;
            self.locations.push(ordinal);
        }
        starts(&self.counts, &mut self.cursor);
        self.ordered_events.resize(rows, TimelineEvent(0));
        for (event, ordinal) in events
            .into_iter()
            .flatten()
            .zip(self.locations.iter().copied())
        {
            let position = &mut self.cursor[ordinal];
            self.ordered_events[*position] =
                TimelineEvent((u64::from(event.ts) << 32) | u64::from(event.purchase));
            *position += 1;
        }
        self.click_counts.clear();
        self.click_counts.resize(self.names.len(), 0);
        self.click_destinations.clear();
        for click in clicks.into_iter().flatten() {
            if let Some(&ordinal) = self.ordinals.get(&click.user) {
                self.click_counts[ordinal] += 1;
                self.click_destinations.push((
                    ordinal,
                    TimelineClick {
                        ts: click.ts,
                        campaign: click.campaign,
                        id: click.id,
                    },
                ));
            }
        }
        starts(&self.click_counts, &mut self.click_cursor);
        self.ordered_clicks.resize(
            self.click_destinations.len(),
            TimelineClick {
                ts: 0,
                campaign: 0,
                id: Key(0),
            },
        );
        for &(ordinal, click) in &self.click_destinations {
            let position = &mut self.click_cursor[ordinal];
            self.ordered_clicks[*position] = click;
            *position += 1;
        }
        let (mut event_start, mut click_start) = (0, 0);
        for (ordinal, &name) in self.names.iter().enumerate() {
            let event_end = self.cursor[ordinal];
            let click_end = self.click_cursor[ordinal];
            let events = &mut self.ordered_events[event_start..event_end];
            let clicks = &mut self.ordered_clicks[click_start..click_end];
            clicks.sort_unstable_by_key(|click| (click.ts, click.id));
            if self.bin_min > 0 && events.len() >= self.bin_min {
                self.scratch.aggregate_binned(
                    UserId::Packed(name),
                    events,
                    clicks,
                    organic,
                    base,
                    out,
                );
            } else {
                events.sort_unstable_by_key(|event| event.0);
                self.scratch
                    .aggregate(UserId::Packed(name), events, clicks, organic, base, out);
            }
            (event_start, click_start) = (event_end, click_end);
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

enum EventInput {
    Claims(claims::ClaimBatch),
    ById(Vec<Vec<Vec<Candidate>>>),
    ByPerson(split::SplitEvents),
}

pub struct Parsed {
    events: EventInput,
    clicks: Vec<Vec<Click>>,
    links: Aliases,
    base: i64,
    first_day: i32,
    days: usize,
}

pub struct PackedPlan {
    pub partitions: usize,
    pub dedup_partitions: usize,
    pub indexed: bool,
    pub profile: bool,
}

pub fn parse_inputs(
    events: &[&[u8]],
    clicks: &[&[u8]],
    link_files: &[&[u8]],
    link_chunks: &[&[u8]],
    catalog: &Catalog<'_>,
    window: Range<i64>,
    plan: PackedPlan,
) -> Option<Parsed> {
    let PackedPlan {
        partitions: count,
        dedup_partitions: dedup_count,
        indexed,
        profile,
    } = plan;
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
    let fast_time =
        if indexed && std::env::var_os("SHOPPING_CLOCK").is_none_or(|mode| mode != "calendar") {
            Some(FastTime::new(&calendar, base, window.start, window.end)?)
        } else {
            None
        };
    let parser = Parser {
        catalog,
        base,
        start: window.start,
        calendar,
        cutoff_days,
        fast_time,
        end: window.end,
    };
    let window_rows =
        indexed && std::env::var_os("SHOPPING_SCANNER").is_none_or(|mode| mode != "block");
    let decode_clicks = || {
        Stages::span(profile, "click_decode", || {
            clicks
                .par_iter()
                .map(|chunk| {
                    let mut output = Vec::with_capacity(chunk.len() / 45);
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
        })
    };
    let decode_links = || {
        Stages::span(profile, "link_decode_and_union", || {
            if indexed
                && std::env::var_os("SHOPPING_LINKS").as_deref()
                    == Some(std::ffi::OsStr::new("sparse"))
            {
                sparse_links::Sparse::new(link_chunks).map(Aliases::Sparse)
            } else if indexed
                && std::env::var_os("SHOPPING_LINKS").as_deref()
                    == Some(std::ffi::OsStr::new("parallel"))
            {
                Aliases::parallel(link_chunks)
            } else {
                Links::new(link_files.iter().copied()).map(Aliases::Serial)
            }
        })
    };
    let split_route = indexed
        && std::env::var_os("SHOPPING_ROUTE").as_deref() == Some(std::ffi::OsStr::new("split"));
    let (events, clicks, links) = if split_route {
        let (links, clicks) = rayon::join(decode_links, decode_clicks);
        let links = links?;
        let events = Stages::span(profile, "split_decode", || {
            split::decode(events, &parser, &links, count, dedup_count, window_rows)
        })?;
        (EventInput::ByPerson(events), clicks?, links)
    } else {
        let ((events, clicks), links) = rayon::join(
            || {
                rayon::join(
                    || {
                        Stages::span(profile, "event_decode_and_scatter", || {
                            if indexed
                                && std::env::var_os("SHOPPING_DEDUP_MODE")
                                    .is_some_and(|m| m == "claims")
                            {
                                return claims::ClaimBatch::parse(
                                    events,
                                    dedup_count,
                                    &parser,
                                    window_rows,
                                )
                                .map(EventInput::Claims);
                            }
                            parse(
                                events,
                                dedup_count,
                                window_rows,
                                |row| parser.event(row),
                                |item| item.event,
                            )
                            .map(EventInput::ById)
                        })
                    },
                    decode_clicks,
                )
            },
            decode_links,
        );
        (events?, clicks?, links?)
    };
    Some(Parsed {
        events,
        clicks,
        links,
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
        let choose = if !indexed {
            dedup
        } else if std::env::var_os("SHOPPING_DEDUP_MODE").as_deref()
            == Some(std::ffi::OsStr::new("filter"))
        {
            dedup_filtered
        } else {
            dedup_indexed
        };
        let profile = stages.enabled();
        let (events, clicks) = rayon::join(
            || {
                Stages::span(profile, "event_dedup_and_route", || match events {
                    EventInput::Claims(batch) => batch.route(&links, count),
                    EventInput::ByPerson(routed) => routed.winners(count, profile),
                    EventInput::ById(groups) => {
                        let parts = groups
                            .into_par_iter()
                            .map(|group| choose(group, &links, count))
                            .collect();
                        transpose(parts, count)
                    }
                })
            },
            || {
                Stages::span(profile, "click_alias_and_route", || {
                    clicks
                        .into_par_iter()
                        .map(|group| {
                            let mut output = buckets(count);
                            if let Aliases::Sparse(table) = &links {
                                for block in group.chunks(24) {
                                    for row in block {
                                        table.prefetch(row.user);
                                    }
                                    for row in block {
                                        let user = table.resolve(row.user);
                                        output[bucket(user, count)].push(Click { user, ..*row });
                                    }
                                }
                                return output;
                            }
                            for mut click in group {
                                click.user = links.resolve(click.user);
                                output[bucket(click.user, count)].push(click);
                            }
                            output
                        })
                        .collect()
                })
            },
        );
        let clicks = transpose(clicks, count);
        stages.mark("packed_dedup_and_partition");
        if indexed {
            let mode = std::env::var("SHOPPING_TIMELINES").unwrap_or_else(|_| "counted".into());
            coalesce(events, clicks)
                .into_par_iter()
                .map(|job| {
                    let mut daily = Daily {
                        users: Vec::new(),
                        campaigns: CampaignDaily::dense(first_day, days, organic + 1),
                    };
                    let mut grouping = TimelineGroups::default();
                    let mut counted = CountedTimelines {
                        scratch: if std::env::var_os("SHOPPING_ASSISTS")
                            .is_some_and(|v| v == "sweep")
                            && matches!(daily.campaigns, CampaignDaily::Dense { .. })
                        {
                            Person::with_assist_sweep(organic + 1)
                        } else {
                            Person::default()
                        },
                        bin_min: std::env::var("SHOPPING_BIN_MIN")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(64),
                        ..CountedTimelines::default()
                    };
                    for pair in job.buckets {
                        match mode.as_str() {
                            "linked" => grouping.run(pair, organic, base, &mut daily),
                            "sort" => aggregate_sorted(pair, organic, base, &mut daily),
                            _ => counted.run(pair, organic, base, &mut daily),
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
