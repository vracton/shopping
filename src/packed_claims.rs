//! Scatter compact fingerprint/location claims while keeping payloads in decode order.
//! Only potential duplicates read the full ID and ingest timestamp.
use super::*;

pub(super) struct MarkedEvent {
    pub(super) user: Key,
    pub(super) ts: std::sync::atomic::AtomicU32,
    pub(super) purchase: u32,
}

pub(super) struct ClaimBatch {
    metadata: Vec<Witnesses>,
    events: Vec<Vec<MarkedEvent>>,
    claims: Vec<Vec<Vec<u64>>>,
    row_bits: u32,
}

/// The claim keeps the low half and this array keeps the high half of a
/// permutation of the packed ID. Together they decide exact key equality.
/// Most ingest codes in a decode group share their high bits.
struct Witnesses {
    rows: Vec<(u32, u32)>,
    ingest_prefix: u64,
    exceptions: Vec<u64>,
}

impl Witnesses {
    fn new(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            ingest_prefix: 0,
            exceptions: Vec::new(),
        }
    }

    #[inline(always)]
    fn push(&mut self, residual: u32, ingest: u64) -> Option<()> {
        const EXCEPTION: u32 = 1 << 31;
        if self.rows.is_empty() {
            self.ingest_prefix = ingest >> 31;
        }
        let low = if ingest >> 31 == self.ingest_prefix {
            ingest as u32 & !EXCEPTION
        } else {
            let index = u32::try_from(self.exceptions.len()).ok()?;
            if index >= EXCEPTION {
                return None;
            }
            self.exceptions.push(ingest);
            index | EXCEPTION
        };
        self.rows.push((residual, low));
        Some(())
    }

    #[inline]
    fn get(&self, index: usize) -> (u32, u64) {
        let (residual, low) = self.rows[index];
        let ingest = if low < 1 << 31 {
            (self.ingest_prefix << 31) | u64::from(low)
        } else {
            self.exceptions[(low ^ (1 << 31)) as usize]
        };
        (residual, ingest)
    }
}

#[inline(always)]
fn claim_hash(key: Key) -> u64 {
    // Multiplication by an odd constant and rotation are both bijections.
    key.0.wrapping_mul(0xf135_7aea_2e62_a9c5).rotate_left(26)
}

impl ClaimBatch {
    pub(super) fn parse(
        chunks: &[&[u8]],
        dedup_count: usize,
        parser: &Parser<'_, '_>,
        windows: bool,
    ) -> Option<Self> {
        let group_size = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let group_count = chunks.len().div_ceil(group_size);
        let row_bits = 32_u32.checked_sub(
            group_count
                .max(2)
                .checked_next_power_of_two()?
                .trailing_zeros(),
        )?;
        if row_bits == 0 {
            return None;
        }
        let batches: Vec<_> = chunks
            .par_chunks(group_size)
            .enumerate()
            .map(|(i, chunks)| {
                let bytes: usize = chunks.iter().map(|c| c.len()).sum();
                let mut records = Witnesses::new(bytes / 74);
                let mut events = Vec::with_capacity(bytes / 74);
                let mut claims: Vec<_> = (0..dedup_count)
                    .map(|_| Vec::with_capacity(bytes / 74 / dedup_count))
                    .collect();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        // Zero is the empty-slot sentinel; every packed location must be biasable by one.
                        if events.len() >= (1_usize << row_bits) - 1 {
                            return None;
                        }
                        let h = claim_hash(row.event);
                        let loc = ((i as u32) << row_bits) | events.len() as u32;
                        claims[((h >> 32) as usize) & (dedup_count - 1)]
                            .push((h << 32) | u64::from(loc + 1));
                        events.push(MarkedEvent {
                            user: row.user,
                            ts: row.ts.into(),
                            purchase: row.purchase,
                        });
                        records.push((h >> 32) as u32, row.ingest)?;
                    }
                    Some(())
                };
                for chunk in chunks {
                    if windows {
                        for row in RowWindows::new(chunk) {
                            emit(row)?;
                        }
                    } else {
                        for row in ChunkRows::new(chunk) {
                            emit(row)?;
                        }
                    }
                }
                Some(((records, events), claims))
            })
            .collect::<Option<Vec<_>>>()?;
        let (records, claims): (Vec<_>, Vec<_>) = batches.into_iter().unzip();
        let (records, events) = records.into_iter().unzip();
        Some(Self {
            metadata: records,
            events,
            claims: transpose(claims, dedup_count),
            row_bits,
        })
    }

    pub(super) fn winners(self) -> Vec<Vec<MarkedEvent>> {
        use std::sync::atomic::Ordering::Relaxed;
        let Self {
            metadata,
            events,
            claims,
            row_bits,
        } = self;
        let mask = (1 << row_bits) - 1;
        let lookup = |loc: u32| metadata[(loc >> row_bits) as usize].get((loc & mask) as usize);
        let reject = |loc: u32| {
            events[(loc >> row_bits) as usize][(loc & mask) as usize]
                .ts
                .store(INVALID, Relaxed);
        };
        let screen = std::env::var_os("SHOPPING_CLAIMS").is_some_and(|s| s == "screen");
        claims.into_par_iter().for_each(|groups| {
            let n: usize = groups.iter().map(Vec::len).sum();
            if n == 0 {
                return;
            }
            let mut conflicts = Vec::new();
            let capacity = if screen {
                // A single sighting in this bitmap proves that no other row
                // can have the same full key. All remaining rows still use
                // the full-key contest below.
                let words = n.div_ceil(4).next_power_of_two();
                let mut seen = vec![0u64; words];
                conflicts.resize(words, 0);
                for &entry in groups.iter().flatten() {
                    let h = (entry >> 32) as usize;
                    let word = (h >> 6) & (words - 1);
                    let bit = 1u64 << (h & 63);
                    let old = seen[word];
                    conflicts[word] |= old & bit;
                    seen[word] = old | bit;
                }
                // A set bit in seen but not conflicts represents exactly one row.
                // Subtract those rows to obtain the exact upper bound for the table,
                // even when many different full keys map to a single fingerprint.
                let singletons: usize = seen
                    .iter()
                    .zip(&conflicts)
                    .map(|(&present, &repeated)| (present ^ repeated).count_ones() as usize)
                    .sum();
                n - singletons
            } else {
                n
            };
            let mut slots = vec![0_u64; (capacity + capacity / 2 + 1).next_power_of_two()];
            let mask = slots.len() - 1;
            for entry in groups.into_iter().flatten() {
                if screen {
                    let h = (entry >> 32) as usize;
                    if conflicts[(h >> 6) & (conflicts.len() - 1)] >> (h & 63) & 1 == 0 {
                        continue;
                    }
                }
                let mut pos = (entry >> 32) as usize & mask;
                loop {
                    let old = slots[pos];
                    if old == 0 {
                        slots[pos] = entry;
                        break;
                    }
                    // The upper word is only a fingerprint. Full keys decide identity.
                    if (old ^ entry) >> 32 == 0 {
                        let a = old as u32 - 1;
                        let b = entry as u32 - 1;
                        let (prev, new) = (lookup(a), lookup(b));
                        if prev.0 == new.0 {
                            if prev.1 > new.1 {
                                reject(a);
                                slots[pos] = entry;
                            } else {
                                reject(b);
                            }
                            break;
                        }
                    }
                    pos = (pos + 1) & mask;
                }
            }
        });
        events
    }

    pub(super) fn route(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let parts = self
            .winners()
            .into_par_iter()
            .map(|rows| {
                let mut output: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(rows.len() / count))
                    .collect();
                let candidates = rows.into_iter().map(|row| Candidate {
                    user: row.user,
                    ts: row.ts.into_inner(),
                    purchase: row.purchase,
                    ..Candidate::default()
                });
                route(candidates, links, &mut output);
                output
            })
            .collect();
        transpose(parts, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn witness_codes_keep_ingest_order_across_prefixes() {
        let mut witnesses = Witnesses::new(0);
        let times = [1 << 31, u64::MAX, u64::MAX - 1, 0, (1 << 32) - 1];
        for (i, &time) in times.iter().enumerate() {
            witnesses.push(i as u32, time).unwrap();
        }
        for (i, &time) in times.iter().enumerate() {
            assert_eq!(witnesses.get(i), (i as u32, time));
        }
    }

    #[test]
    fn equal_fingerprints_do_not_merge_keys_or_rescue_an_invalid_winner() {
        let key = |s: &[u8]| Key::pack(s).unwrap();
        let a = 1;
        let b = 2;
        let c = 3;
        let event = |ts: u32| MarkedEvent {
            user: key(b"user"),
            ts: ts.into(),
            purchase: 0,
        };
        // Deliberately give every claim the same fingerprint, including unrelated keys.
        let witnesses = |rows: [(u32, u64); 3]| {
            let mut values = Witnesses::new(rows.len());
            for (high, ingest) in rows {
                values.push(high, ingest).unwrap();
            }
            values
        };
        let batch = ClaimBatch {
            metadata: vec![
                witnesses([(a, 5), (b, 8), (c, 3)]),
                witnesses([(a, 1), (b, 4), (c, 3)]),
            ],
            events: vec![
                vec![event(10), event(20), event(30)],
                vec![event(INVALID), event(21), event(30)],
            ],
            claims: vec![vec![vec![1, 2, 3], vec![17, 18, 19]]],
            row_bits: 4,
        };
        let links = Aliases::Serial(Links::new(std::iter::empty()).unwrap());
        let mut kept: Vec<_> = batch
            .route(&links, 1)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.ts)
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, [21, 30]);
    }
}
