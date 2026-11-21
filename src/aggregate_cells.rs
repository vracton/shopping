//! For packed u32 timestamps, at most 38 bitmap words cover the full range.
//! An occupied cell represents a single connected run. Its earliest and latest
//! timestamps decide exactly whether it connects to the next occupied cell.
use super::*;

const CELLS: usize = u32::MAX as usize / BIN_MS + 1;
const WORDS: usize = CELLS.div_ceil(64);
const DAYS: usize = u32::MAX as usize / DAY_MS as usize + 2;
const CATEGORIES: usize = 256;
const _: () = assert!(WORDS <= 64);

#[derive(Default, Clone, Copy)]
struct Cell {
    first: u32,
    last: u32,
    count: usize,
}

pub(super) struct Cells {
    slots: Vec<Cell>,
    occupancy: [u64; WORDS],
    purchases: Vec<(usize, usize, u32)>,
    by_category: Vec<i64>,
}

impl Default for Cells {
    fn default() -> Self {
        Self {
            slots: vec![Cell::default(); CELLS],
            occupancy: [0; WORDS],
            purchases: Vec::new(),
            by_category: vec![0; DAYS * CATEGORIES],
        }
    }
}

impl Cells {
    pub(super) fn run<'a>(
        &mut self,
        sessions: &mut Vec<Session>,
        user: UserId<'a>,
        events: &[impl TimedEvent],
        base: i64,
        output: &mut Daily<'a>,
    ) {
        let mut occupied = 0u64;
        self.purchases.clear();
        for event in events {
            let ts = event.timestamp() as u32;
            let index = ts as usize / BIN_MS;
            let cell = &mut self.slots[index];
            if cell.count == 0 {
                cell.first = ts;
                cell.last = ts;
                self.occupancy[index >> 6] |= 1 << (index & 63);
                occupied |= 1 << (index >> 6);
            } else {
                cell.first = cell.first.min(ts);
                cell.last = cell.last.max(ts);
            }
            cell.count += 1;
            let (price, category) = event.purchase();
            if price != 0 {
                self.purchases.push((index, category as usize, price));
            }
        }

        sessions.clear();
        let mut row_indices = [0; DAYS];
        let base_day = day(base);
        let mut current_day = i32::MIN;
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
                    if current_day != session_day {
                        current_day = session_day;
                        row_indices[(session_day - base_day) as usize] = output.users.len();
                        output.users.push(new_user_day(user, session_day));
                    }
                    sessions.push(Session {
                        start,
                        day: session_day,
                        revenue: 0,
                    });
                    output.users.last_mut().unwrap().sessions += 1;
                }
                output.users.last_mut().unwrap().events += cell.count as i64;
                previous_end = i64::from(cell.last);
                cell.count = 0;
                // The extrema have been consumed. Retain the owning session
                // for the sparse purchase payloads until the next shopper.
                cell.first = (sessions.len() - 1) as u32;
            }
        }
        for &(cell, category, price) in &self.purchases {
            let session = &mut sessions[self.slots[cell].first as usize];
            let offset = (session.day - base_day) as usize;
            let row = &mut output.users[row_indices[offset]];
            let price = i64::from(price);
            row.revenue += price;
            session.revenue += price;
            let amounts = &mut self.by_category[offset * CATEGORIES..][..CATEGORIES];
            amounts[category] += price;
            if row.top_category.is_none_or(|prev| {
                (amounts[category], std::cmp::Reverse(category))
                    > (amounts[prev as usize], std::cmp::Reverse(prev as usize))
            }) {
                row.top_category = Some(category as u32);
            }
        }
        for &(cell, category, _) in &self.purchases {
            let session = &sessions[self.slots[cell].first as usize];
            self.by_category[(session.day - base_day) as usize * CATEGORIES + category] = 0;
        }
    }
}
