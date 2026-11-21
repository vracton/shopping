//! The sessions eligible for an arbitrary click form one consecutive range.
//! OR those ranges per campaign, then count the marked starts on each day.
use super::{CampaignStats, Session, TimedClick, WEEK_MS};

#[derive(Default)]
pub(super) struct Attribution {
    best_at: Vec<usize>,
    marks: Vec<u64>,
    touched: Vec<usize>,
    day_ends: Vec<usize>,
}

const NO_CLICK: usize = usize::MAX;

impl Attribution {
    pub(super) fn run(
        &mut self,
        sessions: &[Session],
        clicks: &[impl TimedClick],
        organic: usize,
        first: i32,
        days: usize,
        stats: &mut [CampaignStats],
    ) {
        let n = sessions.len();
        let words = n.div_ceil(64);
        let slots = words * (organic + 1);
        self.marks.resize(self.marks.len().max(slots), 0);
        self.best_at.clear();
        self.best_at.resize(n, NO_CLICK);

        for (index, click) in clicks.iter().enumerate() {
            let ts = click.timestamp();
            let begin = sessions.partition_point(|s| s.start < ts);
            let expiry = ts + WEEK_MS;
            if begin == n || sessions[begin].start > expiry {
                continue;
            }
            let end = begin + 1 + sessions[begin + 1..].partition_point(|s| s.start <= expiry);
            let best = &mut self.best_at[begin];
            if *best == NO_CLICK
                || ts > clicks[*best].timestamp()
                || (ts == clicks[*best].timestamp() && click.smaller_id(&clicks[*best]))
            {
                *best = index;
            }
            let first_word = begin / 64;
            let last_word = (end - 1) / 64;
            let offset = click.campaign() * words;
            for word in first_word..=last_word {
                let low = if word == first_word {
                    u64::MAX << (begin % 64)
                } else {
                    u64::MAX
                };
                let high = if word == last_word {
                    u64::MAX >> (63 - (end - 1) % 64)
                } else {
                    u64::MAX
                };
                let at = offset + word;
                if self.marks[at] == 0 {
                    self.touched.push(at);
                }
                self.marks[at] |= low & high;
            }
        }

        let mut credited = organic;
        let mut expiry = i64::MIN;
        for (s, &winner) in sessions.iter().zip(&self.best_at) {
            if winner != NO_CLICK {
                let click = &clicks[winner];
                credited = click.campaign();
                expiry = click.timestamp() + WEEK_MS;
            }
            let campaign = if s.start <= expiry { credited } else { organic };
            let row = &mut stats[campaign * days + (s.day - first) as usize];
            row.sessions += 1;
            row.revenue += s.revenue;
            row.purchasing_sessions += i64::from(s.revenue != 0);
        }

        if self.touched.is_empty() {
            return;
        }
        self.day_ends.clear();
        self.day_ends.resize(n, n);
        for i in (0..n - 1).rev() {
            self.day_ends[i] = if sessions[i].day == sessions[i + 1].day {
                self.day_ends[i + 1]
            } else {
                i + 1
            };
        }
        for at in self.touched.drain(..) {
            let c = at / words;
            let first_bit = (at % words) * 64;
            let mut mask = std::mem::take(&mut self.marks[at]);
            while mask != 0 {
                let s = first_bit + mask.trailing_zeros() as usize;
                let last = (self.day_ends[s] - first_bit).min(64) - 1;
                let day_mask = u64::MAX >> (63 - last);
                stats[c * days + (sessions[s].day - first) as usize].assisted_sessions +=
                    i64::from((mask & day_mask).count_ones());
                mask &= !day_mask;
            }
        }
    }
}
