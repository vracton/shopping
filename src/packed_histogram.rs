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
    limit: u32,
    overflow: bool,
}

#[derive(Default)]
pub(super) struct Timelines {
    counts: Vec<Counts>,
    slots: Vec<Slot>,
    events: Vec<TimelineEvent>,
    clicks: Vec<TimelineClick>,
}

impl Timelines {
    pub(super) fn run<const SAMPLE: bool>(
        &mut self,
        (events, clicks): BucketPair,
        organic: usize,
        base: i64,
        out: &mut Daily<'static>,
        counted: &mut CountedTimelines,
    ) {
        let rows = events.iter().map(Vec::len).sum::<usize>();
        let touches = clicks.iter().map(Vec::len).sum::<usize>();
        if rows > u32::MAX as usize / 4 || touches > u32::MAX as usize {
            counted.run((events, clicks), organic, base, out);
            return;
        }
        let slots = (rows / 6).max(1).next_power_of_two();
        let bucket = |key: Key| key.hash() as usize & (slots - 1);
        self.counts.clear();
        self.counts.resize(slots, Counts::default());
        if SAMPLE {
            for group in &events {
                for e in group.iter().step_by(8) {
                    self.counts[bucket(e.user)].events += 1;
                }
            }
            let mut capacity = 0usize;
            for count in &mut self.counts {
                let estimate = 8 + 12 * count.events as usize;
                capacity += estimate;
                if capacity > rows * 4 {
                    counted.run((events, clicks), organic, base, out);
                    return;
                }
                count.events = estimate as u32;
            }
        } else {
            for e in events.iter().flatten() {
                self.counts[bucket(e.user)].events += 1;
            }
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
                limit: start.events + count.events,
                overflow: false,
            };
            start.events += count.events;
            start.clicks += count.clicks;
            cursor
        }));
        self.events
            .resize(start.events as usize, TimelineEvent::default());
        self.clicks.resize(touches, TimelineClick::default());
        let mut other_events = Vec::new();
        let mut other_clicks = Vec::new();
        for e in events.into_iter().flatten() {
            let slot = &mut self.slots[bucket(e.user)];
            if slot.user.0 == 0 {
                slot.user = e.user;
            }
            if slot.user == e.user {
                if SAMPLE && slot.end.events == slot.limit {
                    if !slot.overflow {
                        // Move the whole shopper on a miss so its sessions cannot split.
                        other_events.extend(
                            self.events[slot.start.events as usize..slot.end.events as usize]
                                .iter()
                                .map(|v| Event {
                                    ts: (v.0 >> 32) as u32,
                                    purchase: v.0 as u32,
                                    user: slot.user,
                                }),
                        );
                        slot.overflow = true;
                    }
                    other_events.push(e);
                } else {
                    self.events[slot.end.events as usize] =
                        TimelineEvent(u64::from(e.ts) << 32 | u64::from(e.purchase));
                    slot.end.events += 1;
                }
            } else {
                other_events.push(e);
            }
        }
        for c in clicks.into_iter().flatten() {
            let slot = &mut self.slots[bucket(c.user)];
            if slot.user == c.user && (!SAMPLE || !slot.overflow) {
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
        for s in self
            .slots
            .iter()
            .filter(|s| s.user.0 != 0 && (!SAMPLE || !s.overflow))
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undersampled_shopper_and_full_key_collision_keep_all_events_and_clicks() {
        const ROWS: usize = 4096;
        let slots = (ROWS / 6).next_power_of_two();
        let key = |i| Key::pack(format!("k{i:07}").as_bytes()).unwrap();
        let slot = |k: Key| k.hash() as usize & (slots - 1);
        let sampled = key(0);
        let skew = (1..50000)
            .map(key)
            .find(|&k| slot(k) != slot(sampled))
            .unwrap();
        let collision = (1..50000)
            .map(key)
            .find(|&k| k != skew && slot(k) == slot(skew))
            .unwrap();
        let batch = || {
            let events = (0..ROWS)
                .map(|i| Event {
                    user: if i % 8 == 0 {
                        sampled
                    } else if i % 19 == 0 {
                        collision
                    } else {
                        skew
                    },
                    ts: (i as u32 % 512) * 1_900_000,
                    purchase: ((i % 9) as u32) << 24 | 37,
                })
                .collect();
            let clicks = [skew, collision, sampled, skew]
                .into_iter()
                .enumerate()
                .map(|(i, user)| Click {
                    user,
                    id: key(i as i32 + 1),
                    ts: 1_900_000,
                    campaign: i as u32,
                })
                .collect();
            (vec![events], vec![clicks])
        };
        let mut expected = Daily::default();
        aggregate_sorted(batch(), 4, 0, &mut expected);
        let mut actual = Daily::default();
        Timelines::default().run::<true>(
            batch(),
            4,
            0,
            &mut actual,
            &mut CountedTimelines::default(),
        );
        let rows = |daily: &Daily| {
            let mut rows: Vec<_> = daily
                .users
                .iter()
                .map(|r| {
                    let UserId::Packed(key) = r.user else {
                        unreachable!()
                    };
                    (key, r.day, r.events, r.sessions, r.revenue, r.top_category)
                })
                .collect();
            rows.sort_unstable();
            rows
        };
        assert_eq!(rows(&actual), rows(&expected));
        assert_eq!(actual.campaigns, expected.campaigns);
    }
}
