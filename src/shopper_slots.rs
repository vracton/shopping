//! Stable, partitionable identities for the counted timeline path.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const TOKEN: u64 = 1 << 63; // All packed ASCII names fit in 63 bits.
const PROBES: usize = 64;

struct Slot {
    name: AtomicU64,
    target: AtomicU64,
}

pub(super) struct Directory {
    slots: Vec<Slot>,
    overflow: FxHashMap<Key, Key>,
}

impl Directory {
    pub(super) fn direct(chunks: &[&[u8]]) -> Option<Self> {
        let (_, capacity) = sparse_links::table_size(chunks)?;
        let capacity = capacity.checked_mul(2)?;
        let directory = Self {
            slots: (0..capacity)
                .map(|_| Slot {
                    name: AtomicU64::new(0),
                    target: AtomicU64::new(0),
                })
                .collect(),
            overflow: FxHashMap::default(),
        };
        chunks.par_iter().try_for_each(|chunk| {
            for [a, b] in RowWindows::<2>::new(chunk) {
                if a.is_empty() || b.is_empty() {
                    continue;
                }
                let (a, b) = (Key::pack(a)?, Key::pack(b)?);
                let (left, right) = (directory.slot(a)?, directory.slot(b)?);
                directory.join(left, right);
            }
            Some(())
        })?;
        directory
            .slots
            .par_iter()
            .enumerate()
            .for_each(|(index, slot)| {
                if slot.target.load(Relaxed) != 0 {
                    let root = directory.root(index);
                    slot.target.store(TOKEN | root as u64, Relaxed);
                }
            });
        Some(directory)
    }

    fn root(&self, mut index: usize) -> usize {
        loop {
            let parent = self.slots[index].target.load(Relaxed);
            if parent == 0 {
                return index;
            }
            let next = (parent & !TOKEN) as usize;
            let grandparent = self.slots[next].target.load(Relaxed);
            if grandparent == 0 {
                return next;
            }
            let _ =
                self.slots[index]
                    .target
                    .compare_exchange(parent, grandparent, Relaxed, Relaxed);
            index = (grandparent & !TOKEN) as usize;
        }
    }

    fn join(&self, mut a: usize, mut b: usize) {
        loop {
            a = self.root(a);
            b = self.root(b);
            if a == b {
                return;
            }
            if self.slots[a].name.load(Relaxed) < self.slots[b].name.load(Relaxed) {
                std::mem::swap(&mut a, &mut b);
            }
            if self.slots[a]
                .target
                .compare_exchange(0, TOKEN | b as u64, Relaxed, Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub(super) fn new(sparse: sparse_links::Sparse) -> Self {
        let pairs = sparse.into_pairs();
        let slots: Vec<_> = pairs
            .into_par_iter()
            .map(|(key, parent)| Slot {
                name: AtomicU64::new(key.0),
                target: AtomicU64::new(parent.0),
            })
            .collect();
        let mut dir = Self {
            slots,
            overflow: FxHashMap::default(),
        };
        // Each nonzero target starts as a canonical name. Canonical names
        // have no sparse entry of their own, so interning them only touches
        // empty slots and every such slot retains its zero target.
        dir.slots.par_iter().for_each(|slot| {
            let parent = slot.target.load(Relaxed);
            if parent != 0 {
                let token = dir.intern(Key(parent));
                slot.target.store(token.0, Relaxed);
            }
        });
        let mask = dir.slots.len() - 1;
        for (index, slot) in dir.slots.iter().enumerate() {
            let target = slot.target.load(Relaxed);
            let name = Key(slot.name.load(Relaxed));
            if target != 0 && (index.wrapping_sub(name.hash() as usize) & mask) >= PROBES {
                dir.overflow.insert(name, Key(target));
            }
        }
        dir
    }

    /// A singleton that cannot fit in its bounded probe window keeps its
    /// literal key. Occupancy only grows, so that decision remains stable.
    #[inline]
    fn slot(&self, key: Key) -> Option<usize> {
        let mask = self.slots.len() - 1;
        let home = key.hash() as usize;
        for i in 0..PROBES.min(self.slots.len()) {
            let at = home.wrapping_add(i) & mask;
            let slot = &self.slots[at];
            let mut found = slot.name.load(Relaxed);
            if found == 0 {
                found = match slot.name.compare_exchange(0, key.0, Relaxed, Relaxed) {
                    Ok(_) => key.0,
                    Err(found) => found,
                };
            }
            if found == key.0 {
                return Some(at);
            }
        }
        None
    }

    #[inline]
    pub(super) fn intern(&self, key: Key) -> Key {
        if let Some(at) = self.slot(key) {
            let target = self.slots[at].target.load(Relaxed);
            return if target == 0 {
                Key(TOKEN | at as u64)
            } else {
                Key(target)
            };
        }
        self.overflow.get(&key).copied().unwrap_or(key)
    }

    #[inline]
    pub(super) fn partition(&self, token: Key, count: usize) -> usize {
        if token.0 & TOKEN != 0 {
            token.0 as usize & (count - 1)
        } else {
            bucket(token, count)
        }
    }

    pub(super) fn name(&self, token: Key) -> Key {
        if token.0 & TOKEN != 0 {
            Key(self.slots[(token.0 & !TOKEN) as usize].name.load(Relaxed))
        } else {
            token
        }
    }

    pub(super) fn route(&self, rows: impl Iterator<Item = Candidate>, output: &mut [Vec<Event>]) {
        for row in rows.filter(|row| row.ts != INVALID) {
            let user = self.intern(row.user);
            output[self.partition(user, output.len())].push(Event {
                user,
                ts: row.ts,
                purchase: row.purchase,
            });
        }
    }

    pub(super) fn route_clicks(&self, rows: &[Click], output: &mut [Vec<Click>]) {
        for row in rows {
            let user = self.intern(row.user);
            output[self.partition(user, output.len())].push(Click { user, ..*row });
        }
    }
}

/// Count and scatter timelines with the ordinal already present in the row.
/// A small overflow map covers literal singleton keys outside the directory.
pub(super) fn group(
    dir: &Directory,
    count: usize,
    timelines: &mut CountedTimelines,
    (events, clicks): BucketPair,
    organic: usize,
    base: i64,
    out: &mut Daily<'static>,
) {
    let shift = count.trailing_zeros();
    let width = dir.slots.len().div_ceil(count);
    let ordinal = |key: Key| ((key.0 & !TOKEN) >> shift) as usize;
    let t = timelines;
    t.counts.clear();
    t.counts.resize(width, 0);
    t.ordinals.clear();
    t.names.clear();
    t.names.resize(width, Key(0));
    for row in events.iter().flatten() {
        let key = row.user;
        let at = if key.0 & TOKEN != 0 {
            ordinal(key)
        } else {
            *t.ordinals.entry(key).or_insert_with(|| {
                let at = t.counts.len();
                t.counts.push(0);
                t.names.push(Key(0));
                at
            })
        };
        t.names[at] = key;
        t.counts[at] += 1;
    }
    let position = |key: Key| {
        if key.0 & TOKEN != 0 {
            Some(ordinal(key))
        } else {
            t.ordinals.get(&key).copied()
        }
    };
    starts(&t.counts, &mut t.cursor);
    t.ordered_events
        .resize(events.iter().map(Vec::len).sum(), TimelineEvent(0));
    for row in events.into_iter().flatten() {
        let next = &mut t.cursor[position(row.user).unwrap()];
        t.ordered_events[*next] =
            TimelineEvent((u64::from(row.ts) << 32) | u64::from(row.purchase));
        *next += 1;
    }
    t.click_counts.clear();
    t.click_counts.resize(t.counts.len(), 0);
    for click in clicks.iter().flatten() {
        if let Some(at) = position(click.user) {
            t.click_counts[at] += 1;
        }
    }
    starts(&t.click_counts, &mut t.click_cursor);
    t.ordered_clicks.resize(
        t.click_counts.iter().sum(),
        TimelineClick {
            ts: 0,
            campaign: 0,
            id: Key(0),
        },
    );
    for click in clicks.into_iter().flatten() {
        if let Some(at) = position(click.user) {
            let next = &mut t.click_cursor[at];
            t.ordered_clicks[*next] = TimelineClick {
                ts: click.ts,
                campaign: click.campaign,
                id: click.id,
            };
            *next += 1;
        }
    }
    for (at, &n) in t.counts.iter().enumerate() {
        if n == 0 {
            continue;
        }
        let (end, last) = (t.cursor[at], t.click_cursor[at]);
        let events = &mut t.ordered_events[end - n..end];
        let clicks = &mut t.ordered_clicks[last - t.click_counts[at]..last];
        clicks.sort_unstable_by_key(|click| (click.ts, click.id));
        let user = UserId::Packed(dir.name(t.names[at]));
        if t.bin_min > 0 && events.len() >= t.bin_min {
            t.scratch
                .aggregate_binned(user, events, clicks, organic, base, out);
        } else {
            events.sort_unstable_by_key(|event| event.0);
            t.scratch
                .aggregate(user, events, clicks, organic, base, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_probe_overflow_keeps_literal_singleton_identity() {
        let dir = Directory {
            slots: (0..128)
                .map(|_| Slot {
                    name: 0.into(),
                    target: 0.into(),
                })
                .collect(),
            overflow: FxHashMap::default(),
        };
        let colliders: Vec<_> = (1..)
            .map(Key)
            .filter(|k| k.hash() & 127 == 23)
            .take(PROBES + 9)
            .collect();
        let tokens: Vec<_> = colliders.par_iter().map(|&k| dir.intern(k)).collect();
        assert_eq!(tokens.iter().filter(|k| k.0 & TOKEN != 0).count(), PROBES);
        for (&raw, &token) in colliders.iter().zip(&tokens) {
            assert_eq!(dir.name(token), raw);
            assert_eq!(dir.intern(raw), token);
        }
    }

    #[test]
    fn both_directories_preserve_names_through_concurrent_cycles_and_singletons() {
        let text = [
            b"x,Y\nZ,x\nU,W\n".as_slice(),
            b"U,Y\nY,U\n Q,Z\nx,x\nunterminated",
        ];
        let direct = Directory::direct(&text).unwrap();
        let converted = Directory::new(sparse_links::Sparse::new(&text).unwrap());
        for dir in [direct, converted] {
            let smallest = Key::pack(b" Q").unwrap();
            let canonical = dir.intern(smallest);
            for name in [b"Y".as_slice(), b"Z", b"U", b"W", b"x"] {
                assert_eq!(dir.intern(Key::pack(name).unwrap()), canonical);
            }
            assert_eq!(dir.name(canonical), smallest);
            for raw in (1..500).map(Key) {
                let token = dir.intern(raw);
                assert_eq!(dir.name(token), raw);
                assert_eq!(dir.intern(raw), token);
            }
        }
    }
}
