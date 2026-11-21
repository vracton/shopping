//! Keep decoded candidates in source order. Producers publish only low hash
//! bits. A hash observed once proves that its event is unique, while every
//! candidate at a repeated position still participates in full-key dedup.
use super::*;

enum Sketch {
    Bits { seen: Vec<u64>, repeated: Vec<u32> },
    Tickets(Vec<Vec<u32>>),
}

struct Tile {
    rows: Vec<Candidate>,
    sketch: Sketch,
}

pub(super) struct Batch {
    tiles: Vec<Tile>,
    words: usize,
    parts: usize,
    tickets: bool,
}

impl Batch {
    pub(super) fn parse(
        chunks: &[&[u8]],
        parser: &Parser<'_, '_>,
        parts: usize,
        windows: bool,
        tickets: bool,
    ) -> Option<Self> {
        let bytes: usize = chunks.iter().map(|chunk| chunk.len()).sum();
        // Size from bytes is only a load-factor hint. Clamping trades extra
        // exact comparisons for bounded space on large or unusually short rows.
        let words = (bytes / 75 / 8)
            .clamp(parts.min(1 << 20), 1 << 22)
            .next_power_of_two();
        if parts > words {
            return None;
        }
        let tiles = rayon::current_num_threads() * if tickets { 8 } else { 1 };
        let width = chunks.len().div_ceil(tiles).max(1);
        let tiles = chunks
            .par_chunks(width)
            .map(|chunks| {
                let capacity = chunks.iter().map(|c| c.len()).sum::<usize>() / 73;
                let mut rows = Vec::with_capacity(capacity);
                let mut sketch = if tickets {
                    Sketch::Tickets(
                        (0..parts)
                            .map(|_| Vec::with_capacity(capacity / parts))
                            .collect(),
                    )
                } else {
                    Sketch::Bits {
                        seen: vec![0; words],
                        repeated: Vec::new(),
                    }
                };
                let mask = (words * 64 - 1) as u32;
                let local_bits = (words * 64 / parts).trailing_zeros();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        let projection = row.event.hash() as u32 & mask;
                        match &mut sketch {
                            Sketch::Tickets(parts) => {
                                parts[(projection >> local_bits) as usize].push(projection);
                            }
                            Sketch::Bits { seen, repeated } => {
                                let bit = 1 << (projection & 63);
                                let word = &mut seen[(projection >> 6) as usize];
                                if *word & bit != 0 {
                                    repeated.push(projection);
                                }
                                *word |= bit;
                            }
                        }
                        rows.push(row);
                    }
                    Some(())
                };
                for &chunk in chunks {
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
                Some(Tile { rows, sketch })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            tiles,
            words,
            parts,
            tickets,
        })
    }

    fn conflicts(&mut self) -> Vec<u64> {
        let mut conflicts = vec![0; self.words];
        if self.tickets {
            let pieces = self
                .tiles
                .iter_mut()
                .map(|tile| match &mut tile.sketch {
                    Sketch::Tickets(tickets) => std::mem::take(tickets),
                    _ => unreachable!(),
                })
                .collect();
            let width = self.words / self.parts;
            conflicts
                .par_chunks_mut(width)
                .zip(transpose(pieces, self.parts))
                .for_each(|(conflict, group)| {
                    let mut used = vec![0; width];
                    for point in group.into_iter().flatten() {
                        let index = (point as usize >> 6) & (width - 1);
                        let bit = 1 << (point & 63);
                        conflict[index] |= used[index] & bit;
                        used[index] |= bit;
                    }
                });
        } else {
            // Disjoint cache-size pieces allow vectorized merge without atomics.
            conflicts
                .par_chunks_mut(2048)
                .enumerate()
                .for_each(|(group, conflict)| {
                    let start = group * 2048;
                    let mut used = [0_u64; 2048];
                    for tile in &self.tiles {
                        let Sketch::Bits { seen, .. } = &tile.sketch else {
                            unreachable!()
                        };
                        for ((all, seen), out) in
                            used.iter_mut().zip(&seen[start..]).zip(&mut *conflict)
                        {
                            *out |= *all & *seen;
                            *all |= *seen;
                        }
                    }
                });
            for tile in &self.tiles {
                let Sketch::Bits { repeated, .. } = &tile.sketch else {
                    unreachable!()
                };
                for point in repeated {
                    conflicts[(*point >> 6) as usize] |= 1 << (point & 63);
                }
            }
        }
        conflicts
    }

    pub(super) fn route(mut self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let conflicts = self.conflicts();
        let ambiguous = |row: &Candidate| {
            let hash = row.event.hash();
            conflicts[(hash as usize >> 6) & (conflicts.len() - 1)] & (1 << (hash & 63)) != 0
        };
        let parts = self.parts;
        // Split source-order data into short jobs even when a single producer
        // built each private sketch.
        let routed: Vec<_> = self
            .tiles
            .par_iter()
            .flat_map(|tile| tile.rows.par_chunks(96 * 1024))
            .map(|rows| {
                let mut unique: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(rows.len() / count))
                    .collect();
                let mut retry = buckets(parts);
                let survivors = rows.iter().copied().filter(|row| {
                    if ambiguous(row) {
                        retry[bucket(row.event, parts)].push(*row);
                        false
                    } else {
                        true
                    }
                });
                route(survivors, links, &mut unique);
                (unique, retry)
            })
            .collect();
        let (unique, retry): (Vec<_>, Vec<_>) = routed.into_iter().unzip();
        let late: Vec<_> = transpose(retry, parts)
            .into_par_iter()
            .map(|g| dedup_indexed(g, links, count))
            .collect();
        let mut out = transpose(unique, count);
        for (left, mut right) in out.iter_mut().zip(transpose(late, count)) {
            left.append(&mut right);
        }
        out
    }
}
