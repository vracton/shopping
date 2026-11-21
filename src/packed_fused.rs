//! Scatter event payloads just once for canonical users that own their raw ID's bucket.
//! A separate event-ID index records only the loser positions; ingress order and
//! the complete identity remain available for every candidate, including invalid rows.
use super::*;

#[derive(Clone, Copy, Default)]
struct Claim {
    key: Key,
    low: u32,
    loc: u32,
}

/// Most ingests in a parse tile share an upper word. Other words (and the
/// reserved low-word value) keep their original full timestamp in a side map.
#[derive(Default)]
struct Ingest {
    prefix: Option<u64>,
    wide: FxHashMap<u32, u64>,
}

impl Ingest {
    #[inline]
    fn encode(&mut self, value: u64, loc: u32) -> u32 {
        let prefix = value & !u64::from(u32::MAX);
        if *self.prefix.get_or_insert(prefix) == prefix && value as u32 != u32::MAX {
            value as u32
        } else {
            self.wide.insert(loc, value);
            u32::MAX
        }
    }

    fn read(&self, c: Claim) -> u64 {
        if c.low == u32::MAX {
            self.wide[&c.loc]
        } else {
            self.prefix.unwrap() | u64::from(c.low)
        }
    }
}

pub(super) struct Batch {
    // First index is the raw (or pre-resolved) person partition. Empty tile
    // vectors are retained so locations can be decoded without a side table.
    events: Vec<Vec<Vec<Event>>>,
    claims: Vec<Vec<Vec<Claim>>>,
    ingests: Vec<Ingest>,
    row_bits: u32,
    partition_bits: u32,
    canonical: bool,
}

impl Batch {
    pub(super) fn parse(
        chunks: &[&[u8]],
        parser: &Parser<'_, '_>,
        links: Option<&Aliases>,
        count: usize,
        partitions: usize,
        windows: bool,
    ) -> Option<Self> {
        let width = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let tiles = chunks.len().div_ceil(width);
        let partition_bits = count.trailing_zeros();
        let tile_bits = tiles.max(1).checked_next_power_of_two()?.trailing_zeros();
        let row_bits = 32_u32.checked_sub(tile_bits + partition_bits)?;
        if row_bits == 0 {
            return None;
        }
        let decoded = chunks
            .par_chunks(width)
            .enumerate()
            .map(|(tile, chunks)| {
                let approx = chunks.iter().map(|c| c.len()).sum::<usize>() / 75;
                let mut events: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(approx / count))
                    .collect();
                let mut claims: Vec<_> = (0..partitions)
                    .map(|_| Vec::with_capacity(approx / partitions))
                    .collect();
                let mut ingest = Ingest::default();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        let user = links.map_or(row.user, |table| table.resolve(row.user));
                        let person_part = bucket(user, count);
                        let payloads = &mut events[person_part];
                        if payloads.len() as u64 >= 1_u64 << row_bits {
                            return None;
                        }
                        let loc = (((((tile << partition_bits) | person_part) as u64) << row_bits)
                            | payloads.len() as u64) as u32;
                        claims[bucket(row.event, partitions)].push(Claim {
                            key: row.event,
                            low: ingest.encode(row.ingest, loc),
                            loc,
                        });
                        payloads.push(Event {
                            user,
                            ts: row.ts,
                            purchase: row.purchase,
                        });
                    }
                    Some(())
                };
                for &chunk in chunks {
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
                Some((events, claims, ingest))
            })
            .collect::<Option<Vec<_>>>()?;
        let mut events: Vec<Vec<Vec<Event>>> =
            (0..count).map(|_| Vec::with_capacity(tiles)).collect();
        let mut claims = Vec::with_capacity(tiles);
        let mut ingests = Vec::with_capacity(tiles);
        for (parts, ids, times) in decoded {
            for (out, rows) in events.iter_mut().zip(parts) {
                out.push(rows);
            }
            claims.push(ids);
            ingests.push(times);
        }
        Some(Self {
            events,
            claims: transpose(claims, partitions),
            ingests,
            row_bits,
            partition_bits,
            canonical: links.is_some(),
        })
    }

    pub(super) fn finish(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let Self {
            events,
            claims,
            ingests,
            row_bits,
            partition_bits,
            canonical,
        } = self;
        let tile_shift = partition_bits + row_bits;
        let tile = |loc: u32| (u64::from(loc) >> tile_shift) as usize;
        let person = |loc: u32| (u64::from(loc) >> row_bits) as usize & (count - 1);
        let stamp = |c: Claim| ingests[tile(c.loc)].read(c);
        let losers: Vec<_> = claims
            .into_par_iter()
            .map(|groups| {
                let n: usize = groups.iter().map(Vec::len).sum();
                let mut slots = vec![Claim::default(); (n + n / 2 + 1).next_power_of_two()];
                let mask = slots.len() - 1;
                let mut losers = buckets(count);
                for new in groups.into_iter().flatten() {
                    let mut pos = new.key.hash() as usize & mask;
                    loop {
                        let prev = slots[pos];
                        if prev.key.0 == 0 {
                            slots[pos] = new;
                            break;
                        }
                        if prev.key == new.key {
                            let loc = if stamp(prev) > stamp(new) {
                                slots[pos] = new;
                                prev.loc
                            } else {
                                new.loc
                            };
                            losers[person(loc)].push(loc);
                            break;
                        }
                        pos = (pos + 1) & mask;
                    }
                }
                losers
            })
            .collect();
        let row_mask = (1_u64 << row_bits) - 1;
        let outputs: Vec<_> = events
            .into_par_iter()
            .zip(transpose(losers, count))
            .enumerate()
            .map(|(part, (mut tiles, losers))| {
                for loc in losers.into_iter().flatten() {
                    let row = (u64::from(loc) & row_mask) as usize;
                    tiles[tile(loc)][row].ts = INVALID;
                }
                let mut forward = buckets(count);
                let mut cache = [(Key(0), Key(0)); 2048];
                for rows in &mut tiles {
                    if canonical {
                        rows.retain(|row| row.ts != INVALID);
                        continue;
                    }
                    rows.retain_mut(|row| {
                        if row.ts == INVALID {
                            return false;
                        }
                        let slot = &mut cache[(row.user.hash() as usize) & 2047];
                        if slot.0 != row.user {
                            *slot = (row.user, links.resolve(row.user));
                        }
                        row.user = slot.1;
                        let dest = bucket(row.user, count);
                        if dest == part {
                            return true;
                        }
                        forward[dest].push(*row);
                        false
                    });
                }
                (tiles, forward)
            })
            .collect();
        let (resident, sent): (Vec<_>, Vec<_>) = outputs.into_iter().unzip();
        resident
            .into_iter()
            .zip(transpose(sent, count))
            .map(|(mut owner, mut incoming)| {
                owner.append(&mut incoming);
                owner
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_ingests_preserve_exceptional_and_boundary_words() {
        let mut ingests = Ingest::default();
        for (i, v) in [0x1_12345678, 0x1_ffffffff, 0x3_12345678, 0, u64::MAX]
            .into_iter()
            .enumerate()
        {
            let loc = i as u32;
            let low = ingests.encode(v, loc);
            assert_eq!(
                ingests.read(Claim {
                    key: Key(1),
                    low,
                    loc
                }),
                v
            );
        }
    }
}
