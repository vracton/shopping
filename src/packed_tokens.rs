//! Assign canonical identities during routing. A view or cart then crosses the
//! thread boundary in eight bytes, and the grouping worker can count by slot.
use super::*;
use std::sync::atomic::{
    AtomicU32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};

const PROBES: usize = 96;

#[derive(Default)]
struct Slot {
    key: AtomicU64,
    destination: AtomicU32,
}

#[derive(Default)]
#[repr(align(64))]
struct Used(AtomicU32);

pub(super) struct Directory {
    slots: Vec<Slot>,
    names: Vec<AtomicU64>,
    used: Vec<Used>,
    shift: u32,
    parts: usize,
}

impl Directory {
    pub(super) fn new(chunks: &[&[u8]], bytes: usize, parts: usize) -> Option<Self> {
        // Size is only a space/time heuristic. Every identity that cannot get
        // a slot takes the full-key path. Cap the key and name arrays at 192 MiB.
        let size = (bytes / 768)
            .max(parts)
            .max(64)
            .checked_next_power_of_two()?
            .min(1 << 23);
        if parts > size {
            return None;
        }
        let mut table = Self {
            slots: (0..size).map(|_| Slot::default()).collect(),
            names: (0..size).map(|_| AtomicU64::new(0)).collect(),
            used: (0..parts).map(|_| Used::default()).collect(),
            shift: (size / parts).trailing_zeros(),
            parts,
        };
        // During union, zero is a root; a nonzero parent is slot + 1.
        // A child is linked only to a current root. Reversing the joins
        // therefore makes every parent's final slot available before its children.
        let mut joined = Vec::new();
        // Decode a block of edges before chasing either endpoint. Most table
        // reads miss cache when following independent links one row at a time.
        let mut pending = Vec::with_capacity(64);
        for chunk in chunks {
            for [left, right] in RowWindows::<2>::new(chunk) {
                if left.is_empty() || right.is_empty() {
                    continue;
                }
                let (a, b) = (Key::pack(left)?, Key::pack(right)?);
                if a != b {
                    table.prefetch(a);
                    table.prefetch(b);
                    pending.push((a, b));
                    if pending.len() == 64 {
                        table.join_edges(&mut pending, &mut joined)?;
                    }
                }
            }
        }
        table.join_edges(&mut pending, &mut joined)?;
        // Flatten before changing the meaning of a slot's destination.
        for &child in joined.iter().rev() {
            let parent = *table.slots[child].destination.get_mut();
            let root = *table.slots[parent as usize - 1].destination.get_mut();
            if root != 0 {
                *table.slots[child].destination.get_mut() = root;
            }
        }
        // Within each partition, assign consecutive destinations to roots only.
        // An unlinked key discovered later takes the next free destination.
        for (i, entry) in table.slots.iter_mut().enumerate() {
            let key = *entry.key.get_mut();
            if key != 0 && *entry.destination.get_mut() == 0 {
                let part = i >> table.shift;
                let count = table.used[part].0.get_mut();
                let slot = (part << table.shift) + *count as usize;
                *count += 1;
                *table.names[slot].get_mut() = key;
                *entry.destination.get_mut() = slot as u32 + 1;
            }
        }
        for child in joined {
            let root = *table.slots[child].destination.get_mut() as usize - 1;
            *table.slots[child].destination.get_mut() = *table.slots[root].destination.get_mut();
        }
        Some(table)
    }

    fn join_edges(&mut self, pending: &mut Vec<(Key, Key)>, joined: &mut Vec<usize>) -> Option<()> {
        for (a, b) in pending.drain(..) {
            let a = self.insert(a)?;
            let b = self.insert(b)?;
            let a = self.root(a);
            let b = self.root(b);
            if a != b {
                let (child, parent) = if *self.slots[a].key.get_mut() > *self.slots[b].key.get_mut()
                {
                    (a, b)
                } else {
                    (b, a)
                };
                *self.slots[child].destination.get_mut() = parent as u32 + 1;
                joined.push(child);
            }
        }
        Some(())
    }

    fn insert(&mut self, key: Key) -> Option<usize> {
        let mask = self.slots.len() - 1;
        let mut i = key.hash() as usize & mask;
        for _ in 0..PROBES {
            let tag = self.slots[i].key.get_mut();
            if *tag == key.0 {
                return Some(i);
            }
            if *tag == 0 {
                *tag = key.0;
                return Some(i);
            }
            i = (i + 1) & mask;
        }
        None
    }

    fn root(&mut self, mut i: usize) -> usize {
        while *self.slots[i].destination.get_mut() != 0 {
            let parent = *self.slots[i].destination.get_mut() as usize - 1;
            let grandparent = *self.slots[parent].destination.get_mut();
            if grandparent == 0 {
                return parent;
            }
            *self.slots[i].destination.get_mut() = grandparent;
            i = grandparent as usize - 1;
        }
        i
    }

    pub(super) fn resolve(&self, key: Key) -> Key {
        self.intern(key)
            .map_or(key, |slot| Key(self.names[slot].load(Relaxed)))
    }

    #[inline(always)]
    fn intern(&self, key: Key) -> Option<usize> {
        let mask = self.slots.len() - 1;
        let mut index = key.hash() as usize & mask;
        for _ in 0..PROBES {
            let cell = &self.slots[index];
            let mut tag = cell.key.load(Relaxed);
            if tag == 0 {
                match cell.key.compare_exchange(0, key.0, Relaxed, Relaxed) {
                    Ok(_) => {
                        let part = index >> self.shift;
                        let local = self.used[part].0.fetch_add(1, Relaxed) as usize;
                        let slot = (part << self.shift) + local;
                        self.names[slot].store(key.0, Relaxed);
                        // Publish both the ordinal and its initialized canonical name.
                        cell.destination.store(slot as u32 + 1, Release);
                        return Some(slot);
                    }
                    Err(stored) => tag = stored,
                }
            }
            if tag == key.0 {
                loop {
                    let dest = cell.destination.load(Acquire);
                    if dest != 0 {
                        return Some(dest as usize - 1);
                    }
                    std::hint::spin_loop();
                }
            }
            index = (index + 1) & mask;
        }
        None
    }

    #[inline(always)]
    fn prefetch(&self, key: Key) {
        #[cfg(target_arch = "x86_64")]
        {
            let index = key.hash() as usize & (self.slots.len() - 1);
            unsafe {
                std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                    self.slots.as_ptr().add(index).cast(),
                )
            }
        }
    }

    fn aggregate_events(&self, batch: claims::ClaimBatch) -> Vec<Vec<Events>> {
        let mask = (1 << self.shift) - 1;
        batch
            .winners()
            .into_par_iter()
            .map(|rows| {
                let mut parts: Vec<_> = (0..self.parts)
                    .map(|_| Events {
                        plain: Vec::with_capacity(rows.len() / self.parts),
                        ..Events::default()
                    })
                    .collect();
                for block in rows.chunks(32) {
                    for row in block {
                        self.prefetch(row.user);
                    }
                    for row in block {
                        let ts = row.ts.load(Relaxed);
                        if ts == INVALID {
                            continue;
                        }
                        if let Some(slot) = self.intern(row.user) {
                            let part = &mut parts[slot >> self.shift];
                            let local = (slot & mask) as u32;
                            if row.purchase == 0 {
                                part.plain.push([local, ts]);
                            } else {
                                part.purchases.push([local, ts, row.purchase]);
                            }
                        } else {
                            let part = bucket(row.user, self.parts);
                            parts[part].spill.push(Event {
                                user: row.user,
                                ts,
                                purchase: row.purchase,
                            });
                        }
                    }
                }
                parts
            })
            .collect()
    }

    fn route_clicks(&self, clicks: Vec<Vec<Click>>) -> Vec<Vec<Clicks>> {
        let mask = (1 << self.shift) - 1;
        let group_size = clicks
            .len()
            .div_ceil(rayon::current_num_threads() * 4)
            .max(1);
        clicks
            .par_chunks(group_size)
            .map(|group| {
                let len: usize = group.iter().map(Vec::len).sum();
                let mut parts: Vec<_> = (0..self.parts)
                    .map(|_| Clicks {
                        hits: Vec::with_capacity(len / self.parts + len / self.parts / 4),
                        ..Clicks::default()
                    })
                    .collect();
                for rows in group {
                    for block in rows.chunks(32) {
                        for row in block {
                            self.prefetch(row.user);
                        }
                        for row in block {
                            if let Some(slot) = self.intern(row.user) {
                                let part = &mut parts[slot >> self.shift];
                                let local = (slot & mask) as u32;
                                if local | row.campaign <= u32::from(u16::MAX) {
                                    part.hits.push(Hit {
                                        id: row.id,
                                        ts: row.ts,
                                        slot: local as u16,
                                        campaign: row.campaign as u16,
                                    });
                                } else {
                                    part.wide.push((
                                        local,
                                        TimelineClick {
                                            ts: row.ts,
                                            campaign: row.campaign,
                                            id: row.id,
                                        },
                                    ));
                                }
                            } else {
                                parts[bucket(row.user, self.parts)]
                                    .spill
                                    .push(Click { ..*row });
                            }
                        }
                    }
                }
                parts
            })
            .collect()
    }

    pub(super) fn aggregate(
        self,
        batch: claims::ClaimBatch,
        clicks: Vec<Vec<Click>>,
        base: i64,
        first_day: i32,
        days: usize,
        organic: usize,
        stages: &mut Stages,
    ) -> Vec<Daily<'static>> {
        let (events, clicks) = rayon::join(
            || self.aggregate_events(batch),
            || self.route_clicks(clicks),
        );
        let mut parts: Vec<_> = (0..self.parts)
            .map(|index| Part {
                index,
                ..Part::default()
            })
            .collect();
        for group in events {
            for (part, events) in parts.iter_mut().zip(group) {
                if events.len() > 0 {
                    part.weight += events.len();
                    part.events.push(events);
                }
            }
        }
        for group in clicks {
            for (part, clicks) in parts.iter_mut().zip(group) {
                if clicks.len() != 0 {
                    part.weight += clicks.len();
                    part.clicks.push(clicks);
                }
            }
        }
        let parts = parts
            .into_iter()
            .filter(|p| !p.events.is_empty())
            .map(|p| (p.weight, p))
            .collect();
        let jobs = balance(parts);
        stages.mark("packed_dedup_and_partition");
        jobs.into_par_iter()
            .map(|job| {
                let mut daily = Daily {
                    users: Vec::new(),
                    campaigns: CampaignDaily::dense(first_day, days, organic + 1),
                };
                let mut counted = CountedTimelines {
                    scratch: if matches!(daily.campaigns, CampaignDaily::Dense { .. })
                        && std::env::var_os("SHOPPING_ASSISTS").is_some_and(|s| s == "sweep")
                    {
                        Person::with_assist_sweep(organic + 1)
                    } else {
                        Person::default()
                    },
                    bin_min: std::env::var("SHOPPING_BIN_MIN")
                        .ok()
                        .and_then(|x| x.parse().ok())
                        .unwrap_or(64),
                    ..CountedTimelines::default()
                };
                let columns = counted.scratch.has_cells()
                    && std::env::var_os("SHOPPING_SESSIONIZER").is_some_and(|s| s == "columns");
                let mut cursors = Vec::new();
                let mut column_cursors = Vec::new();
                let mut times = Vec::new();
                for part in job.buckets {
                    let span = 1 << self.shift;
                    let start = part.index * span;
                    let used = self.used[part.index].0.load(Relaxed) as usize;
                    let keys = &self.names[start..start + used];
                    if columns {
                        part.finish::<3>(
                            keys,
                            &mut column_cursors,
                            &mut times,
                            &mut counted,
                            organic,
                            base,
                            &mut daily,
                        );
                    } else {
                        part.finish::<2>(
                            keys,
                            &mut cursors,
                            &mut times,
                            &mut counted,
                            organic,
                            base,
                            &mut daily,
                        );
                    }
                }
                daily
            })
            .collect()
    }
}

#[derive(Default)]
struct Events {
    plain: Vec<[u32; 2]>,
    purchases: Vec<[u32; 3]>,
    spill: Vec<Event>,
}

impl Events {
    fn len(&self) -> usize {
        self.plain.len() + self.purchases.len() + self.spill.len()
    }
}

// Keep the common routed click at 16 bytes. The wide side retains both indices
// exactly when either the campaign dictionary or the local slot is larger.
struct Hit {
    id: Key,
    ts: u32,
    slot: u16,
    campaign: u16,
}

#[derive(Default)]
struct Clicks {
    hits: Vec<Hit>,
    wide: Vec<(u32, TimelineClick)>,
    spill: Vec<Click>,
}

impl Clicks {
    fn len(&self) -> usize {
        self.hits.len() + self.wide.len() + self.spill.len()
    }
}

#[derive(Default)]
struct Part {
    index: usize,
    weight: usize,
    events: Vec<Events>,
    clicks: Vec<Clicks>,
}

impl Part {
    // Width two stores events/clicks. In the columnar sessionizer width three
    // stores plain timestamps/clicks/purchases; the ordinary event payload
    // never has to expand back to eight bytes.
    fn finish<const WIDTH: usize>(
        self,
        keys: &[AtomicU64],
        cursors: &mut Vec<[usize; WIDTH]>,
        times: &mut Vec<u32>,
        counted: &mut CountedTimelines,
        organic: usize,
        base: i64,
        out: &mut Daily<'static>,
    ) {
        cursors.clear();
        cursors.resize(keys.len(), [0; WIDTH]);
        let purchase_column = if WIDTH == 3 { 2 } else { 0 };
        for group in &self.events {
            for &[slot, _] in &group.plain {
                cursors[slot as usize][0] += 1;
            }
            for &[slot, _, _] in &group.purchases {
                cursors[slot as usize][purchase_column] += 1;
            }
        }
        for group in &self.clicks {
            for hit in &group.hits {
                cursors[hit.slot as usize][1] += 1;
            }
            for &(slot, _) in &group.wide {
                cursors[slot as usize][1] += 1;
            }
        }
        let mut totals = [0; WIDTH];
        for cursor in &mut *cursors {
            let counts = *cursor;
            *cursor = totals;
            for column in 0..WIDTH {
                totals[column] += counts[column];
            }
        }
        if WIDTH == 3 {
            times.resize(totals[0], 0);
        }
        counted
            .ordered_events
            .resize(totals[purchase_column], TimelineEvent(0));
        counted.ordered_clicks.resize(
            totals[1],
            TimelineClick {
                ts: 0,
                campaign: 0,
                id: Key(0),
            },
        );
        let mut spill_events = Vec::new();
        for group in self.events {
            for [slot, ts] in group.plain {
                let position = &mut cursors[slot as usize][0];
                if WIDTH == 3 {
                    times[*position] = ts;
                } else {
                    counted.ordered_events[*position] = TimelineEvent(u64::from(ts) << 32);
                }
                *position += 1;
            }
            for [slot, ts, purchase] in group.purchases {
                let position = &mut cursors[slot as usize][purchase_column];
                counted.ordered_events[*position] =
                    TimelineEvent(u64::from(ts) << 32 | u64::from(purchase));
                *position += 1;
            }
            if !group.spill.is_empty() {
                spill_events.push(group.spill)
            }
        }
        let mut spill_clicks = Vec::new();
        for group in self.clicks {
            for hit in group.hits {
                let position = &mut cursors[hit.slot as usize][1];
                counted.ordered_clicks[*position] = TimelineClick {
                    id: hit.id,
                    ts: hit.ts,
                    campaign: u32::from(hit.campaign),
                };
                *position += 1;
            }
            for (slot, hit) in group.wide {
                let position = &mut cursors[slot as usize][1];
                counted.ordered_clicks[*position] = hit;
                *position += 1;
            }
            if !group.spill.is_empty() {
                spill_clicks.push(group.spill)
            }
        }
        let mut start = [0; WIDTH];
        for (slot, &end) in cursors.iter().enumerate() {
            if end[0] != start[0] || end[purchase_column] != start[purchase_column] {
                let name = Key(keys[slot].load(Relaxed));
                let events =
                    &mut counted.ordered_events[start[purchase_column]..end[purchase_column]];
                let clicks = &mut counted.ordered_clicks[start[1]..end[1]];
                if WIDTH == 3 {
                    clicks.sort_unstable_by_key(|click| (click.ts, click.id));
                    counted.scratch.try_cells(
                        UserId::Packed(name),
                        &times[start[0]..end[0]],
                        events,
                        clicks,
                        organic,
                        base,
                        out,
                    );
                } else {
                    finish_timeline(
                        &mut counted.scratch,
                        counted.bin_min,
                        name,
                        events,
                        clicks,
                        organic,
                        base,
                        out,
                    );
                }
            }
            start = end;
        }
        if !spill_events.is_empty() {
            counted.run((spill_events, spill_clicks), organic, base, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirects_and_colliding_singletons_remain_distinct_across_workers() {
        let table = Directory::new(&[b"c,b\nb,a\nd,c\na,b\n"], 1, 4).unwrap();
        let key = |s: &[u8]| Key::pack(s).unwrap();
        let canonical = table.intern(key(b"a")).unwrap();
        for name in [b"a", b"b", b"c", b"d"] {
            assert_eq!(table.intern(key(name)), Some(canonical));
        }
        assert_eq!(table.names[canonical].load(Relaxed), key(b"a").0);

        let names: Vec<_> = (1..100000)
            .map(Key)
            .filter(|k| k.hash() as usize % table.slots.len() == 0)
            .take(130)
            .collect();
        let parallel: Vec<_> = names.par_iter().map(|&k| table.intern(k)).collect();
        let mut spills = 0;
        for (name, slot) in names.into_iter().zip(parallel) {
            assert_eq!(table.intern(name), slot);
            if let Some(slot) = slot {
                assert_eq!(table.names[slot].load(Relaxed), name.0);
            } else {
                spills += 1;
            }
        }
        assert!(spills > 0);
        assert_eq!(table.intern(key(b"d")), Some(canonical));
    }
}
