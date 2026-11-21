//! Count by partial hash before resolving shopper identity. The first shopper
//! at each hash gets a direct range. Full-key collisions take the counted path.
use super::*;

#[derive(Clone, Copy, Default)]
struct Counts {
    events: u32,
    clicks: u32,
}

#[derive(Clone, Copy, Default)]
struct Slot {
    user: Key,
    start: Counts,
    end: Counts,
}

#[derive(Default)]
pub(super) struct Timelines {
    counts: Vec<Counts>,
    slots: Vec<Slot>,
    events: Vec<TimelineEvent>,
    clicks: Vec<TimelineClick>,
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
        let rows = events.iter().map(Vec::len).sum::<usize>();
        let touches = clicks.iter().map(Vec::len).sum::<usize>();
        if rows > u32::MAX as usize || touches > u32::MAX as usize {
            counted.run((events, clicks), organic, base, out);
            return;
        }
        let slots = (rows / 6).max(1).next_power_of_two();
        let bucket = |key: Key| key.hash() as usize & (slots - 1);
        self.counts.clear();
        self.counts.resize(slots, Counts::default());
        for e in events.iter().flatten() {
            self.counts[bucket(e.user)].events += 1;
        }
        for c in clicks.iter().flatten() {
            self.counts[bucket(c.user)].clicks += 1;
        }

        self.slots.clear();
        let mut start = Counts::default();
        self.slots.extend(self.counts.iter().map(|count| {
            let cursor = Slot {
                user: Key::default(),
                start,
                end: start,
            };
            start.events += count.events;
            start.clicks += count.clicks;
            cursor
        }));
        self.events.resize(rows, TimelineEvent::default());
        self.clicks.resize(touches, TimelineClick::default());
        let mut other_events = Vec::new();
        let mut other_clicks = Vec::new();
        for e in events.into_iter().flatten() {
            let slot = &mut self.slots[bucket(e.user)];
            if slot.user.0 == 0 {
                slot.user = e.user;
            }
            if slot.user == e.user {
                self.events[slot.end.events as usize] =
                    TimelineEvent(u64::from(e.ts) << 32 | u64::from(e.purchase));
                slot.end.events += 1;
            } else {
                other_events.push(e);
            }
        }
        for c in clicks.into_iter().flatten() {
            let slot = &mut self.slots[bucket(c.user)];
            if slot.user == c.user {
                self.clicks[slot.end.clicks as usize] = TimelineClick {
                    ts: c.ts,
                    campaign: c.campaign,
                    id: c.id,
                };
                slot.end.clicks += 1;
            } else if slot.user.0 != 0 {
                other_clicks.push(c);
            }
        }
        for s in self.slots.iter().filter(|s| s.user.0 != 0) {
            let events = &mut self.events[s.start.events as usize..s.end.events as usize];
            let clicks = &mut self.clicks[s.start.clicks as usize..s.end.clicks as usize];
            clicks.sort_unstable_by_key(|c| (c.ts, c.id));
            if counted.bin_min > 0 && events.len() >= counted.bin_min {
                counted.scratch.aggregate_binned(
                    UserId::Packed(s.user),
                    events,
                    clicks,
                    organic,
                    base,
                    out,
                );
            } else {
                events.sort_unstable_by_key(|e| e.0);
                counted.scratch.aggregate(
                    UserId::Packed(s.user),
                    events,
                    clicks,
                    organic,
                    base,
                    out,
                );
            }
        }
        if !other_events.is_empty() {
            counted.run((vec![other_events], vec![other_clicks]), organic, base, out);
        }
    }
}
