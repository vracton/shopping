//! Scatter compact fingerprint/location claims while keeping payloads in decode order.
//! Only potential duplicates read the full ID and ingest timestamp.
use super::*;

struct MarkedEvent {
    user: Key,
    ts: std::sync::atomic::AtomicU32,
    purchase: u32,
}

pub(super) struct ClaimBatch {
    metadata: Vec<Vec<(Key, u64)>>,
    events: Vec<Vec<MarkedEvent>>,
    claims: Vec<Vec<Vec<u64>>>,
    row_bits: u32,
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
                let mut records = Vec::with_capacity(bytes / 74);
                let mut events = Vec::with_capacity(bytes / 74);
                let mut claims: Vec<_> = (0..dedup_count)
                    .map(|_| Vec::with_capacity(bytes / 74 / dedup_count))
                    .collect();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        // Zero is the empty-slot sentinel; every packed location must be biasable by one.
                        if records.len() >= (1_usize << row_bits) - 1 {
                            return None;
                        }
                        let h = row.event.hash();
                        let loc = ((i as u32) << row_bits) | records.len() as u32;
                        claims[((h >> 32) as usize) & (dedup_count - 1)]
                            .push((h << 32) | u64::from(loc + 1));
                        events.push(MarkedEvent {
                            user: row.user,
                            ts: row.ts.into(),
                            purchase: row.purchase,
                        });
                        records.push((row.event, row.ingest));
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

    pub(super) fn route(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        use std::sync::atomic::Ordering::Relaxed;
        let Self {
            metadata,
            events,
            claims,
            row_bits,
        } = self;
        let mask = (1 << row_bits) - 1;
        let lookup = |loc: u32| metadata[(loc >> row_bits) as usize][(loc & mask) as usize];
        let reject = |loc: u32| {
            events[(loc >> row_bits) as usize][(loc & mask) as usize]
                .ts
                .store(INVALID, Relaxed);
        };
        claims.into_par_iter().for_each(|groups| {
            let n: usize = groups.iter().map(Vec::len).sum();
            if n == 0 {
                return;
            }
            let mut slots = vec![0_u64; (n + n / 2 + 1).next_power_of_two()];
            let mask = slots.len() - 1;
            for entry in groups.into_iter().flatten() {
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
        drop(metadata);
        let parts = events
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
    fn equal_fingerprints_do_not_merge_keys_or_rescue_an_invalid_winner() {
        let key = |s: &[u8]| Key::pack(s).unwrap();
        let a = key(b"A");
        let b = key(b"B");
        let c = key(b"C");
        let event = |ts: u32| MarkedEvent {
            user: key(b"user"),
            ts: ts.into(),
            purchase: 0,
        };
        // Deliberately give every claim the same fingerprint, including unrelated keys.
        let batch = ClaimBatch {
            metadata: vec![vec![(a, 5), (b, 8), (c, 3)], vec![(a, 1), (b, 4), (c, 3)]],
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
