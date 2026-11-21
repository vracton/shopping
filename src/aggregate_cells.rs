//! For packed u32 timestamps, 64 bitmap words cover the full range.
//! An occupied cell represents a single connected run. Its earliest and latest
//! timestamps decide exactly whether it connects to the next occupied cell.
use super::*;

// The gap spans more than one cell. Every pair inside a cell is
// connected, and the exact extrema still decide every cross-cell gap.
const SHIFT: u32 = 20;
const CELLS: usize = (u32::MAX >> SHIFT) as usize + 1;
const _: () = assert!((1 << SHIFT) - 1 <= SESSION_GAP_MS);
const WORDS: usize = CELLS.div_ceil(64);
const DAYS: usize = u32::MAX as usize / DAY_MS as usize + 2;
// Consecutive session starts differ by at least GAP + 1 milliseconds.
const MAX_SESSIONS: u32 = u32::MAX / (SESSION_GAP_MS as u32 + 1) + 1;
const SESSION_BITS: u32 = MAX_SESSIONS.next_power_of_two().trailing_zeros();
const SESSION_MASK: u32 = (1 << SESSION_BITS) - 1;
const _: () = assert!((DAYS as u32) <= (u32::MAX >> SESSION_BITS));
const CATEGORIES: usize = 256;
const _: () = assert!(WORDS <= 64);

#[derive(Clone, Copy)]
struct Cell {
    first: u32,
    last: u32,
    count: usize,
}

impl Cell {
    const EMPTY: Self = Self {
        first: u32::MAX,
        last: 0,
        count: 0,
    };
}

pub(super) struct Cells {
    slots: Vec<Cell>,
    occupancy: [u64; WORDS],
    owners: Vec<u32>,
    touched_categories: Vec<usize>,
    by_category: Vec<i64>,
}

impl Default for Cells {
    fn default() -> Self {
        Self {
            slots: vec![Cell::EMPTY; CELLS],
            occupancy: [0; WORDS],
            owners: vec![0; CELLS],
            touched_categories: Vec::new(),
            by_category: vec![0; DAYS * CATEGORIES],
        }
    }
}

impl Cells {
    pub(super) fn run<O: CellDays>(
        &mut self,
        sessions: &mut Vec<Session>,
        user: O::User,
        plain: &[u32],
        events: &[impl TimedEvent],
        base: i64,
        output: &mut O,
    ) {
        let mut occupied = 0u64;
        let times = plain
            .iter()
            .copied()
            .chain(events.iter().map(|e| e.timestamp() as u32));
        for ts in times {
            let index = (ts >> SHIFT) as usize;
            let cell = &mut self.slots[index];
            cell.first = cell.first.min(ts);
            cell.last = cell.last.max(ts);
            cell.count += 1;
            self.occupancy[index >> 6] |= 1 << (index & 63);
            occupied |= 1 << (index >> 6);
        }

        sessions.clear();
        let first_row = output.len();
        let mut day_ordinal = 0u32;
        let mut owner = 0;
        let mut current_day = i32::MIN;
        let mut day_sessions = 0i64;
        let mut day_events = 0i64;
        let mut previous_end = -SESSION_GAP_MS - 1;
        while occupied != 0 {
            let word = occupied.trailing_zeros() as usize;
            occupied &= occupied - 1;
            let mut bits = std::mem::take(&mut self.occupancy[word]);
            while bits != 0 {
                let index = word * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let cell = &mut self.slots[index];
                let start = i64::from(cell.first);
                if start - previous_end > SESSION_GAP_MS {
                    let session_day = day(base + start);
                    if current_day != session_day || day_sessions == 0 {
                        if day_sessions != 0 {
                            output.append(user, current_day, day_sessions, day_events);
                            day_ordinal += 1;
                        }
                        current_day = session_day;
                        day_sessions = 0;
                        day_events = 0;
                    }
                    owner = (day_ordinal << SESSION_BITS) | sessions.len() as u32;
                    sessions.push(Session {
                        start,
                        day: session_day,
                        revenue: 0,
                    });
                    day_sessions += 1;
                }
                day_events += cell.count as i64;
                previous_end = i64::from(cell.last);
                *cell = Cell::EMPTY;
                self.owners[index] = owner;
            }
        }
        if day_sessions != 0 {
            output.append(user, current_day, day_sessions, day_events);
        }
        for event in events {
            let (price, category) = event.purchase();
            if price == 0 {
                continue;
            }
            let category = category as usize;
            let index = ((event.timestamp() as u32) >> SHIFT) as usize;
            let owner = self.owners[index];
            let session = &mut sessions[(owner & SESSION_MASK) as usize];
            let offset = (owner >> SESSION_BITS) as usize;
            let (revenue, top_category) = output.purchase(first_row + offset);
            let price = i64::from(price);
            *revenue += price;
            session.revenue += price;
            let amounts = &mut self.by_category[offset * CATEGORIES..][..CATEGORIES];
            if amounts[category] == 0 {
                self.touched_categories.push(offset * CATEGORIES + category);
            }
            amounts[category] += price;
            if top_category.is_none_or(|prev| {
                (amounts[category], std::cmp::Reverse(category))
                    > (amounts[prev as usize], std::cmp::Reverse(prev as usize))
            }) {
                *top_category = Some(category as u32);
            }
        }
        for index in self.touched_categories.drain(..) {
            self.by_category[index] = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossing_bitmap_words_and_the_last_bin_preserves_exact_gaps() {
        let gap = SESSION_GAP_MS as u32;
        let first = (63 << SHIFT) + 17;
        let last = u32::MAX - 1;
        let plain = [last, first + gap, first + 2 * gap + 1, first, last - gap];
        let purchases = [(first + gap, 10, 7), (last, 11, 3)].map(|(ts, price, category)| Event {
            ts: i64::from(ts),
            price,
            category,
            user: b"u",
        });
        let mut scratch = Cells::default();
        let mut sessions = Vec::new();
        for _ in 0..2 {
            let mut output = Daily::default();
            scratch.run(
                &mut sessions,
                UserId::Bytes(b"u"),
                &plain,
                &purchases,
                0,
                &mut output.users,
            );
            assert_eq!(
                sessions
                    .iter()
                    .map(|s| (s.start as u32, s.revenue))
                    .collect::<Vec<_>>(),
                [(first, 10), (first + 2 * gap + 1, 0), (last - gap, 11)]
            );
            assert_eq!(
                output
                    .users
                    .iter()
                    .map(|row| (row.events, row.top_category))
                    .collect::<Vec<_>>(),
                [(4, Some(7)), (3, Some(3))]
            );
        }
    }
}
