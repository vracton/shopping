//! Route shopper payloads during decoding. The separate event-ID index only
//! touches payloads when a proven duplicate must be removed.
use super::*;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

struct Payload {
    person: Key,
    ts: AtomicU32,
    purchase: u32,
}

#[derive(Clone, Copy)]
struct Claim {
    event: Key,
    ingest: u64,
    payload: u32,
}

pub(super) struct SplitEvents {
    payloads: Vec<Vec<Vec<Payload>>>,
    claims: Vec<Vec<Vec<Claim>>>,
    row_bits: u32,
    partition_bits: u32,
}

pub(super) fn decode(
    chunks: &[&[u8]],
    parser: &Parser<'_, '_>,
    aliases: &Aliases,
    count: usize,
    dedup_count: usize,
    windows: bool,
) -> Option<SplitEvents> {
    let chunks_per_task = chunks
        .len()
        .div_ceil(rayon::current_num_threads() * 8)
        .max(1);
    let tasks = chunks.len().div_ceil(chunks_per_task);
    let partition_bits = count.trailing_zeros();
    let row_bits =
        32_u32.checked_sub(tasks.max(1).next_power_of_two().trailing_zeros() + partition_bits)?;
    if row_bits == 0 {
        return None;
    }
    let output = chunks
        .par_chunks(chunks_per_task)
        .enumerate()
        .map(|(task, chunks)| {
            let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
            let mut payloads: Vec<Vec<Payload>> = (0..count)
                .map(|_| Vec::with_capacity(bytes / 80 / count))
                .collect();
            let mut claims: Vec<Vec<Claim>> = (0..dedup_count)
                .map(|_| Vec::with_capacity(bytes / 74 / dedup_count))
                .collect();
            let mut recent = [(Key(0), Key(0)); 1024];
            let mut emit = |fields| {
                let Some(record) = parser.event(fields)? else {
                    return Some(());
                };
                let index = if record.ts == INVALID {
                    INVALID
                } else {
                    let hint = record.user.hash() as usize & 1023;
                    let (tag, cached) = recent[hint];
                    let person = if tag == record.user {
                        cached
                    } else {
                        let person = aliases.resolve(record.user);
                        recent[hint] = (record.user, person);
                        person
                    };
                    let part = bucket(person, count);
                    let rows = &mut payloads[part];
                    if rows.len() as u64 >= (1_u64 << row_bits) {
                        return None;
                    }
                    let index = ((((task as u64) << partition_bits) | part as u64) << row_bits)
                        | rows.len() as u64;
                    if index >= u64::from(INVALID) {
                        return None;
                    }
                    rows.push(Payload {
                        person,
                        ts: AtomicU32::new(record.ts),
                        purchase: record.purchase,
                    });
                    index as u32
                };
                claims[bucket(record.event, dedup_count)].push(Claim {
                    event: record.event,
                    ingest: record.ingest,
                    payload: index,
                });
                Some(())
            };
            for chunk in chunks {
                if windows {
                    for fields in RowWindows::new(chunk) {
                        emit(fields)?;
                    }
                } else {
                    for fields in ChunkRows::new(chunk) {
                        emit(fields)?;
                    }
                }
            }
            Some((payloads, claims))
        })
        .collect::<Option<Vec<_>>>()?;
    let (payloads, claims) = output.into_iter().unzip();
    Some(SplitEvents {
        payloads,
        claims: transpose(claims, dedup_count),
        row_bits,
        partition_bits,
    })
}

impl SplitEvents {
    pub(super) fn winners(self, partitions: usize, profile: bool) -> Vec<Vec<Vec<Event>>> {
        let Self {
            payloads,
            claims,
            row_bits,
            partition_bits,
        } = self;
        let mark_loser = |position: u32| {
            if position == INVALID {
                return;
            }
            let position = u64::from(position);
            let row = position & ((1_u64 << row_bits) - 1);
            let tile = position >> row_bits;
            let partition = tile & ((1_u64 << partition_bits) - 1);
            let task = tile >> partition_bits;
            payloads[task as usize][partition as usize][row as usize]
                .ts
                .store(INVALID, Relaxed);
        };
        Stages::span(profile, "claim_duplicate_losers", || {
            claims.into_par_iter().for_each(|regions| {
                let n: usize = regions.iter().map(Vec::len).sum();
                if n == 0 {
                    return;
                }
                // Only collision positions may contain duplicate IDs. Unique
                // positions stay out of the exact table entirely.
                let words = n.div_ceil(4).next_power_of_two();
                let mut occupied = vec![0_u64; words];
                let mut conflicts = vec![0_u64; words];
                let bit_for =
                    |hash: u64| (((hash >> 6) as usize) & (words - 1), 1_u64 << (hash & 63));
                for row in regions.iter().flatten() {
                    let (word, bit) = bit_for(row.event.hash());
                    conflicts[word] |= occupied[word] & bit;
                    occupied[word] |= bit;
                }
                let candidates: usize = conflicts
                    .iter()
                    .map(|word| word.count_ones() as usize)
                    .sum();
                let mut exact = hashbrown::HashTable::with_capacity(candidates * 2);
                for claim in regions.into_iter().flatten() {
                    let hash = claim.event.hash();
                    let (word, bit) = bit_for(hash);
                    if conflicts[word] & bit == 0 {
                        continue;
                    }
                    match exact.entry(
                        hash,
                        |row: &Claim| row.event == claim.event,
                        |row| row.event.hash(),
                    ) {
                        hashbrown::hash_table::Entry::Vacant(entry) => {
                            entry.insert(claim);
                        }
                        hashbrown::hash_table::Entry::Occupied(mut entry) => {
                            let winner = entry.get_mut();
                            if claim.ingest < winner.ingest {
                                mark_loser(winner.payload);
                                *winner = claim;
                            } else {
                                mark_loser(claim.payload);
                            }
                        }
                    }
                }
            });
        });
        let grouped: Vec<_> = transpose(payloads, partitions)
            .into_par_iter()
            .map(|group| {
                let capacity = group.iter().map(Vec::len).sum();
                let mut events = Vec::with_capacity(capacity);
                for payload in group.into_iter().flatten() {
                    let ts = payload.ts.into_inner();
                    if ts != INVALID {
                        events.push(Event {
                            user: payload.person,
                            ts,
                            purchase: payload.purchase,
                        });
                    }
                }
                vec![events]
            })
            .collect();
        grouped
    }
}
