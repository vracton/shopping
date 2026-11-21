//! Append fixed-size pages per shopper. The common short timeline fits in
//! one page; long timelines are gathered into a single reusable sort buffer.
use super::*;

struct Page<T, const N: usize> {
    values: [T; N],
    previous_end: u32,
}

struct Pages<T, const N: usize> {
    blocks: Vec<Page<T, N>>,
    scratch: Vec<T>,
}

impl<T, const N: usize> Default for Pages<T, N> {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            scratch: Vec::new(),
        }
    }
}

impl<T: Copy + Default, const N: usize> Pages<T, N> {
    #[inline]
    fn push(&mut self, end: &mut u32, value: T) {
        let mut index = *end as usize;
        if index % N == 0 {
            index = self.blocks.len() * N;
            self.blocks.push(Page {
                values: [T::default(); N],
                previous_end: *end,
            });
        }
        self.blocks[index / N].values[index % N] = value;
        *end = (index + 1) as u32;
    }

    fn collect(&mut self, end: u32) -> &mut [T] {
        if end == 0 {
            return &mut [];
        }
        let last = end as usize - 1;
        let used = last % N + 1;
        if self.blocks[last / N].previous_end == 0 {
            return &mut self.blocks[last / N].values[..used];
        }
        self.scratch.clear();
        self.scratch
            .extend_from_slice(&self.blocks[last / N].values[..used]);
        let mut prev = self.blocks[last / N].previous_end;
        while prev != 0 {
            let block = &self.blocks[(prev as usize - 1) / N];
            self.scratch.extend_from_slice(&block.values);
            prev = block.previous_end;
        }
        &mut self.scratch
    }
}

#[derive(Copy, Clone, Default)]
struct Cursor {
    events: u32,
    clicks: u32,
}

#[derive(Default)]
struct Directory {
    slots: Vec<(Key, Cursor)>,
    collisions: FxHashMap<Key, Cursor>,
}

impl Directory {
    fn reset(&mut self, rows: usize) {
        self.slots.clear();
        self.slots
            .resize((rows / 8).max(1).next_power_of_two(), Default::default());
        self.collisions.clear();
    }

    #[inline]
    fn lookup<const INSERT: bool>(&mut self, key: Key) -> Option<&mut Cursor> {
        let mask = self.slots.len() - 1;
        let mut idx = key.hash() as usize & mask;
        for _ in 0..4 {
            let stored = self.slots[idx].0;
            if stored == key {
                return Some(&mut self.slots[idx].1);
            }
            if stored.0 == 0 {
                if INSERT {
                    self.slots[idx].0 = key;
                    return Some(&mut self.slots[idx].1);
                }
                return None;
            }
            idx = (idx + 1) & mask;
        }
        if INSERT {
            Some(self.collisions.entry(key).or_default())
        } else {
            self.collisions.get_mut(&key)
        }
    }
}

#[derive(Default)]
pub(super) struct Timelines {
    directory: Directory,
    events: Pages<TimelineEvent, 8>,
    clicks: Pages<TimelineClick, 4>,
}

impl Timelines {
    pub(super) fn run(
        &mut self,
        (events, clicks): BucketPair,
        organic: usize,
        base: i64,
        out: &mut Daily<'static>,
        counted: &mut CountedTimelines,
    ) {
        let rows: usize = events.iter().map(Vec::len).sum();
        let click_rows: usize = clicks.iter().map(Vec::len).sum();
        if rows > u32::MAX as usize / 8 || click_rows > u32::MAX as usize / 4 {
            counted.run((events, clicks), organic, base, out);
            return;
        }
        self.directory.reset(rows);
        self.events.blocks.clear();
        self.clicks.blocks.clear();
        for event in events.into_iter().flatten() {
            let cursor = self.directory.lookup::<true>(event.user).unwrap();
            self.events.push(
                &mut cursor.events,
                TimelineEvent(u64::from(event.ts) << 32 | u64::from(event.purchase)),
            );
        }
        for click in clicks.into_iter().flatten() {
            if let Some(cursor) = self.directory.lookup::<false>(click.user) {
                self.clicks.push(
                    &mut cursor.clicks,
                    TimelineClick {
                        ts: click.ts,
                        campaign: click.campaign,
                        id: click.id,
                    },
                );
            }
        }
        let shoppers = self
            .directory
            .slots
            .iter()
            .filter(|(user, _)| user.0 != 0)
            .map(|(k, v)| (k, v))
            .chain(self.directory.collisions.iter());
        for (&key, cursor) in shoppers {
            let events = self.events.collect(cursor.events);
            let clicks = self.clicks.collect(cursor.clicks);
            clicks.sort_unstable_by_key(|click| (click.ts, click.id));
            if counted.bin_min > 0 && events.len() >= counted.bin_min {
                counted.scratch.aggregate_binned(
                    UserId::Packed(key),
                    events,
                    clicks,
                    organic,
                    base,
                    out,
                );
            } else {
                events.sort_unstable_by_key(|event| event.0);
                counted
                    .scratch
                    .aggregate(UserId::Packed(key), events, clicks, organic, base, out);
            }
        }
    }
}
