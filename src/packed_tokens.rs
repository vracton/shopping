//! Assign canonical identities during routing. A view or cart then crosses the
//! thread boundary in eight bytes, and the grouping worker can count by slot.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const REDIRECT: u64 = 1 << 63;
const PROBES: usize = 96;

pub(super) struct Directory {
    keys: Vec<AtomicU64>,
    roots: Vec<u32>,
    shift: u32,
    parts: usize,
}

impl Directory {
    pub(super) fn new(chunks: &[&[u8]], bytes: usize, parts: usize) -> Option<Self> {
        // Size is only a space/time heuristic. Every identity that cannot get
        // a slot takes the full-key path. Cap this auxiliary table at 96 MiB.
        let size = (bytes / 768)
            .max(parts)
            .max(64)
            .checked_next_power_of_two()?
            .min(1 << 23);
        if parts > size {
            return None;
        }
        let mut table = Self {
            keys: (0..size).map(|_| AtomicU64::new(0)).collect(),
            roots: vec![0; size],
            shift: (size / parts).trailing_zeros(),
            parts,
        };
        // During union, zero is a root; a nonzero parent is slot + 1.
        // A child is linked only to a current root. Reversing the joins
        // therefore makes every parent's final slot available before its children.
        let mut joined = Vec::new();
        for chunk in chunks {
            for [left, right] in RowWindows::<2>::new(chunk) {
                if left.is_empty() || right.is_empty() {
                    continue;
                }
                let (a, b) = (Key::pack(left)?, Key::pack(right)?);
                if a == b {
                    continue;
                }
                let a = table.insert(a)?;
                let b = table.insert(b)?;
                let a = table.root(a);
                let b = table.root(b);
                if a != b {
                    let (child, parent) = if *table.keys[a].get_mut() > *table.keys[b].get_mut() {
                        (a, b)
                    } else {
                        (b, a)
                    };
                    table.roots[child] = parent as u32 + 1;
                    joined.push(child);
                }
            }
        }
        for child in joined.into_iter().rev() {
            let parent = table.roots[child] as usize - 1;
            let root = if *table.keys[parent].get_mut() & REDIRECT != 0 {
                table.roots[parent]
            } else {
                parent as u32
            };
            *table.keys[child].get_mut() |= REDIRECT;
            table.roots[child] = root;
        }
        Some(table)
    }

    fn insert(&mut self, key: Key) -> Option<usize> {
        let mask = self.keys.len() - 1;
        let mut i = key.hash() as usize & mask;
        for _ in 0..PROBES {
            let tag = self.keys[i].get_mut();
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
        while self.roots[i] != 0 {
            let parent = self.roots[i] as usize - 1;
            let grandparent = self.roots[parent];
            if grandparent == 0 {
                return parent;
            }
            self.roots[i] = grandparent;
            i = grandparent as usize - 1;
        }
        i
    }

    pub(super) fn resolve(&self, key: Key) -> Key {
        self.intern(key)
            .map_or(key, |slot| Key(self.keys[slot].load(Relaxed)))
    }

    #[inline(always)]
    fn intern(&self, key: Key) -> Option<usize> {
        let mask = self.keys.len() - 1;
        let mut index = key.hash() as usize & mask;
        for _ in 0..PROBES {
            let cell = &self.keys[index];
            let mut tag = cell.load(Relaxed);
            if tag == 0 {
                match cell.compare_exchange(0, key.0, Relaxed, Relaxed) {
                    Ok(_) => return Some(index),
                    Err(stored) => tag = stored,
                }
            }
            if tag == key.0 {
                return Some(index);
            }
            if tag == key.0 | REDIRECT {
                return Some(self.roots[index] as usize);
            }
            index = (index + 1) & mask;
        }
        None
    }

    #[inline(always)]
    fn prefetch(&self, key: Key) {
        #[cfg(target_arch = "x86_64")]
        {
            let index = key.hash() as usize & (self.keys.len() - 1);
            unsafe {
                std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                    self.keys.as_ptr().add(index).cast(),
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
        clicks
            .into_par_iter()
            .map(|rows| {
                let mut parts: Vec<_> = (0..self.parts).map(|_| Clicks::default()).collect();
                for block in rows.chunks(32) {
                    for row in block {
                        self.prefetch(row.user);
                    }
                    for row in block {
                        if let Some(slot) = self.intern(row.user) {
                            parts[slot >> self.shift].hits.push((
                                (slot & mask) as u32,
                                TimelineClick {
                                    id: row.id,
                                    ts: row.ts,
                                    campaign: row.campaign,
                                },
                            ));
                        } else {
                            parts[bucket(row.user, self.parts)]
                                .spill
                                .push(Click { ..*row });
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
        output: Option<&crate::output::BucketOutput>,
    ) -> crate::Result<Vec<Daily<'static>>> {
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
                if !clicks.hits.is_empty() || !clicks.spill.is_empty() {
                    part.weight += clicks.hits.len() + clicks.spill.len();
                    part.clicks.push(clicks);
                }
            }
        }
        let mut parts: Vec<_> = parts
            .into_iter()
            .filter(|p| !p.events.is_empty())
            .map(|p| (p.weight, p))
            .collect();
        let queue_enabled = std::env::var_os("SHOPPING_WORK_QUEUE").is_some();
        let mut backlog = Vec::new();
        let jobs = if queue_enabled {
            let n = rayon::current_num_threads().min(parts.len());
            parts.sort_unstable_by_key(|p| p.0);
            backlog = parts.into_iter().map(|(_, part)| part).collect();
            (0..n)
                .map(|_| Job {
                    buckets: Vec::new(),
                    weight: 0,
                })
                .collect()
        } else {
            balance(parts)
        };
        let backlog = std::sync::Mutex::new(backlog);
        stages.mark("packed_dedup_and_partition");
        jobs.into_par_iter()
            .enumerate()
            .map(|(index, job)| {
                let mut writer = output.map(|out| out.open(index)).transpose()?;
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
                let work = std::iter::from_fn(|| {
                    if queue_enabled {
                        backlog.lock().unwrap().pop()
                    } else {
                        None
                    }
                });
                for part in job.buckets.into_iter().chain(work) {
                    let span = 1 << self.shift;
                    let keys = &self.keys[part.index * span..(part.index + 1) * span];
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
                    if let (Some(out), Some(writer)) = (output, &mut writer) {
                        out.append(writer, &daily.users)?;
                        daily.users.clear();
                    }
                }
                if let Some(writer) = writer {
                    writer.close()?;
                }
                Ok(daily)
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

#[derive(Default)]
struct Clicks {
    hits: Vec<(u32, TimelineClick)>,
    spill: Vec<Click>,
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
            for &(slot, _) in &group.hits {
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
            for (slot, hit) in group.hits {
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
        assert_eq!(table.keys[canonical].load(Relaxed), key(b"a").0);

        let names: Vec<_> = (1..100000)
            .map(Key)
            .filter(|k| k.hash() as usize % table.keys.len() == 0)
            .take(130)
            .collect();
        let parallel: Vec<_> = names.par_iter().map(|&k| table.intern(k)).collect();
        let mut spills = 0;
        for (name, slot) in names.into_iter().zip(parallel) {
            assert_eq!(table.intern(name), slot);
            if let Some(slot) = slot {
                assert_eq!(table.keys[slot].load(Relaxed), name.0);
            } else {
                spills += 1;
            }
        }
        assert!(spills > 0);
        assert_eq!(table.intern(key(b"d")), Some(canonical));
    }
}
