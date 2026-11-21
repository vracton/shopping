use std::hash::BuildHasher;
use std::ops::Range;

use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::catalog::Catalog;
use crate::input::{DAY_MS, WEEK_MS, decimal, fields, rows, timestamp};
use crate::stages::Stages;

#[derive(Clone, Copy)]
pub struct Event<'a> {
    pub user: &'a [u8],
    pub ts: i64,
    pub price: u32,
    pub category: u32,
}

#[derive(Clone, Copy)]
pub struct Click<'a> {
    pub user: &'a [u8],
    pub ts: i64,
    pub id: &'a [u8],
    pub campaign: usize,
}

struct Candidate<'a> {
    id: &'a [u8],
    ingest: i64,
    event: Option<Event<'a>>,
}

pub struct Batches<'a> {
    pub events: Vec<Event<'a>>,
    pub clicks: Vec<Click<'a>>,
}

pub struct InputRows<'a, 'c> {
    pub event_chunks: &'c [&'a [u8]],
    pub click_chunks: &'c [&'a [u8]],
    pub catalog: &'c Catalog<'a>,
    pub window: Range<i64>,
}

impl<'a> InputRows<'a, '_> {
    fn event(&self, row: &'a [u8]) -> Option<Candidate<'a>> {
        let [id, user, ts, ingest, kind, product, price] = fields::<7>(row)?;
        if id.is_empty() {
            return None;
        }
        let ingest = timestamp(ingest)?;
        let event = (|| {
            let ts = timestamp(ts)?;
            if !self.window.contains(&ts)
                || ingest - ts > DAY_MS
                || user.is_empty()
                || product.is_empty()
            {
                return None;
            }
            let (price, category) = match kind {
                b"view" | b"cart" => (0, 0),
                b"purchase" => {
                    let price = decimal(price)?;
                    if !(1..=10_000_000).contains(&price) {
                        return None;
                    }
                    (price as u32, self.catalog.category(product) as u32)
                }
                _ => return None,
            };
            Some(Event {
                user: self.catalog.user(user),
                ts,
                price,
                category,
            })
        })();
        // Invalid values remain eligible to win deduplication, unlike malformed CSV rows.
        Some(Candidate { id, ingest, event })
    }

    fn click(&self, row: &'a [u8]) -> Option<Click<'a>> {
        let [id, user, campaign, ts] = fields::<4>(row)?;
        if id.is_empty() || user.is_empty() {
            return None;
        }
        let ts = timestamp(ts)?;
        if ts < self.window.start - WEEK_MS || ts >= self.window.end {
            return None;
        }
        Some(Click {
            id,
            user: self.catalog.user(user),
            ts,
            campaign: self.catalog.campaign(campaign)?,
        })
    }

    pub fn sort(&self, stages: &mut Stages) -> Batches<'a> {
        let (mut copies, mut clicks): (Vec<_>, Vec<_>) = rayon::join(
            || {
                self.event_chunks
                    .par_iter()
                    .flat_map_iter(|chunk| rows(chunk).filter_map(|row| self.event(row)))
                    .collect()
            },
            || {
                self.click_chunks
                    .par_iter()
                    .flat_map_iter(|chunk| rows(chunk).filter_map(|row| self.click(row)))
                    .collect()
            },
        );
        stages.mark("parse");
        copies.par_sort_unstable_by(|a, b| (a.id, a.ingest).cmp(&(b.id, b.ingest)));
        copies.dedup_by(|a, b| a.id == b.id);
        let mut events: Vec<_> = copies
            .into_par_iter()
            .filter_map(|copy| copy.event)
            .collect();
        stages.mark("event_dedup");
        rayon::join(|| sort_events(&mut events), || sort_clicks(&mut clicks));
        stages.mark("order");
        Batches { events, clicks }
    }

    pub fn hash(&self, partitions: usize, stages: &mut Stages) -> Vec<Batches<'a>> {
        let (copies, clicks) = rayon::join(
            || {
                partition_input(
                    self.event_chunks,
                    partitions,
                    |row| self.event(row),
                    |copy| copy.id,
                )
            },
            || {
                partition_input(
                    self.click_chunks,
                    partitions,
                    |row| self.click(row),
                    |click| click.user,
                )
            },
        );
        stages.mark("parse_and_partition");
        let by_person = copies
            .into_par_iter()
            .map(|groups| {
                let capacity = groups.iter().map(Vec::len).sum();
                let mut best = FxHashMap::with_capacity_and_hasher(capacity, FxBuildHasher);
                for copy in groups.into_iter().flatten() {
                    let chosen = best.entry(copy.id).or_insert((copy.ingest, copy.event));
                    if copy.ingest < chosen.0 {
                        *chosen = (copy.ingest, copy.event);
                    }
                }
                let mut buckets = buckets(partitions);
                for event in best.into_values().filter_map(|(_, event)| event) {
                    buckets[bucket(event.user, partitions)].push(event);
                }
                buckets
            })
            .collect();
        let by_person = transpose(by_person, partitions);
        stages.mark("event_dedup_and_partition");
        let batches = by_person
            .into_par_iter()
            .zip(clicks)
            .map(|(events, clicks)| {
                let mut events = flatten(events);
                let mut clicks = flatten(clicks);
                sort_events(&mut events);
                sort_clicks(&mut clicks);
                Batches { events, clicks }
            })
            .collect();
        stages.mark("order");
        batches
    }
}

fn buckets<T>(count: usize) -> Vec<Vec<T>> {
    (0..count).map(|_| Vec::new()).collect()
}

fn bucket(key: &[u8], count: usize) -> usize {
    (FxBuildHasher.hash_one(key) % count as u64) as usize
}

fn transpose<T>(rows: Vec<Vec<Vec<T>>>, count: usize) -> Vec<Vec<Vec<T>>> {
    let mut columns = buckets(count);
    for row in rows {
        for (column, entries) in columns.iter_mut().zip(row) {
            if !entries.is_empty() {
                column.push(entries);
            }
        }
    }
    columns
}

fn partition_input<'a, T: Send>(
    chunks: &[&'a [u8]],
    count: usize,
    parse: impl Fn(&'a [u8]) -> Option<T> + Sync,
    key: impl Fn(&T) -> &'a [u8] + Sync,
) -> Vec<Vec<Vec<T>>> {
    let rows = chunks
        .par_iter()
        .map(|chunk| {
            let mut buckets = buckets(count);
            for entry in rows(chunk).filter_map(&parse) {
                buckets[bucket(key(&entry), count)].push(entry);
            }
            buckets
        })
        .collect();
    transpose(rows, count)
}

fn flatten<T>(groups: Vec<Vec<T>>) -> Vec<T> {
    if groups.len() == 1 {
        return groups.into_iter().next().unwrap();
    }
    let mut output = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for mut group in groups {
        output.append(&mut group);
    }
    output
}

fn sort_events(events: &mut [Event<'_>]) {
    events.par_sort_unstable_by(|a, b| (a.user, a.ts).cmp(&(b.user, b.ts)));
}

fn sort_clicks(clicks: &mut Vec<Click<'_>>) {
    clicks.par_sort_unstable_by(|a, b| (a.user, a.ts, a.id).cmp(&(b.user, b.ts, b.id)));
    // Copies are identical after CSV decoding, so sorting places them next to each other.
    clicks.dedup_by(|a, b| a.id == b.id);
}
