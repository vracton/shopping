//! Credit and assist windows share the first session at or after each click.
use super::{CampaignStats, Session, TimedClick, WEEK_MS};

pub(super) struct Sweep {
    counted_until: Vec<usize>,
    offset: usize,
    day_ends: Vec<usize>,
}

impl Sweep {
    pub(super) fn new(campaigns: usize) -> Self {
        Self {
            counted_until: vec![0; campaigns],
            offset: 0,
            day_ends: Vec::new(),
        }
    }

    pub(super) fn run(
        &mut self,
        sessions: &[Session],
        clicks: &[impl TimedClick],
        organic: usize,
        first: i32,
        days: usize,
        stats: &mut [CampaignStats],
    ) {
        if self.offset.checked_add(sessions.len()).is_none() {
            self.counted_until.fill(0);
            self.offset = 0;
        }
        // Numbering sessions across shoppers avoids clearing the campaign table.
        let offset = self.offset;
        self.offset += sessions.len();
        let jump_days = sessions.len() > 12 && clicks.len() > 1;
        if jump_days {
            self.day_ends.clear();
            self.day_ends.resize(sessions.len(), sessions.len());
            for i in (0..sessions.len() - 1).rev() {
                self.day_ends[i] = if sessions[i].day == sessions[i + 1].day {
                    self.day_ends[i + 1]
                } else {
                    i + 1
                };
            }
        }

        let (mut begin, mut end) = (0, 0);
        let mut best = organic;
        let mut best_time = i64::MIN;
        let mut best_expiry = i64::MIN;
        for click in clicks {
            let ts = click.timestamp();
            while begin < sessions.len() && sessions[begin].start < ts {
                let s = &sessions[begin];
                let c = if s.start <= best_expiry {
                    best
                } else {
                    organic
                };
                credit(&mut stats[c * days + (s.day - first) as usize], s);
                begin += 1;
            }
            if begin == sessions.len() {
                break;
            }
            let expiry = ts + WEEK_MS;
            let c = click.campaign();
            // Equal-time clicks arrive in ascending click ID order.
            if ts > best_time {
                best_time = ts;
                best_expiry = expiry;
                best = c;
            }
            end = begin.max(end);
            while end < sessions.len() && sessions[end].start <= expiry {
                end += 1;
            }
            let mut at = begin.max(self.counted_until[c].saturating_sub(offset));
            self.counted_until[c] = offset + end;
            while at < end {
                let s = &sessions[at];
                let next = if jump_days {
                    self.day_ends[at].min(end)
                } else {
                    let mut next = at + 1;
                    while next < end && sessions[next].day == s.day {
                        next += 1;
                    }
                    next
                };
                stats[c * days + (s.day - first) as usize].assisted_sessions += (next - at) as i64;
                at = next;
            }
        }
        for s in &sessions[begin..] {
            let c = if s.start <= best_expiry {
                best
            } else {
                organic
            };
            credit(&mut stats[c * days + (s.day - first) as usize], s);
        }
    }
}

#[inline]
fn credit(row: &mut CampaignStats, session: &Session) {
    row.sessions += 1;
    row.revenue += session.revenue;
    row.purchasing_sessions += i64::from(session.revenue != 0);
}
