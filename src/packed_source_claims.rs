//! Retain source offsets in the payload of a zero-revenue event. Only an
//! actual fingerprint collision reads the ID and ingest fields again.
use super::*;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

const EXTENDED: u32 = 1 << 31;

struct Row {
    user: Key,
    ts: AtomicU32,
    evidence: u32,
}

struct Extended {
    key: Key,
    ingest: u64,
    purchase: u32,
}

struct Tile {
    rows: Vec<Row>,
    extended: Vec<Extended>,
}

impl Tile {
    #[inline]
    fn push(&mut self, row: Candidate, base: usize, id: &[u8], ingest: &[u8]) -> Option<()> {
        // These are slices returned by the CSV scanner from the same chunk.
        let offset = id.as_ptr() as usize - base;
        let delta = ingest.as_ptr() as usize - id.as_ptr() as usize;
        let evidence = if row.purchase == 0 && offset < 1 << 20 && delta < 128 {
            // Key::pack admitted a nonempty ID of at most nine bytes.
            ((offset as u32) << 11) | ((id.len() as u32) << 7) | delta as u32
        } else {
            let index = u32::try_from(self.extended.len()).ok()?;
            if index >= EXTENDED {
                return None;
            }
            self.extended.push(Extended {
                key: row.event,
                ingest: row.ingest,
                purchase: row.purchase,
            });
            index | EXTENDED
        };
        self.rows.push(Row {
            user: row.user,
            ts: row.ts.into(),
            evidence,
        });
        Some(())
    }

    #[inline]
    fn read(&self, code: u32, chunk: &[u8], fast: bool) -> (Key, u64) {
        if code & EXTENDED != 0 {
            let e = &self.extended[(code ^ EXTENDED) as usize];
            return (e.key, e.ingest);
        }
        let offset = (code >> 11) as usize;
        let len = ((code >> 7) & 15) as usize;
        let ts = offset + (code & 127) as usize;
        let id = &chunk[offset..offset + len];
        let ingest = &chunk[ts..ts + 20];
        // Both fields already passed these same parsers before insertion.
        let key = Key::pack(id).unwrap();
        let time = if fast {
            FastTime::ingest(ingest)
        } else {
            Calendar::ingest_code(ingest)
        };
        (key, time.unwrap())
    }
}

pub(super) struct SourceEvents(Vec<Vec<Tile>>);

impl SourceEvents {
    pub(super) fn parse(
        chunks: &[&[u8]],
        count: usize,
        parser: &Parser<'_, '_>,
        windows: bool,
    ) -> Option<Self> {
        let group_size = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let row_bits = 32_u32.checked_sub(
            chunks
                .len()
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
            .map(|(group, chunks)| {
                let mut tiles = Vec::with_capacity(chunks.len());
                let bytes: usize = chunks.iter().map(|c| c.len()).sum();
                let mut claims: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(bytes / 74 / count))
                    .collect();
                for (i, chunk) in chunks.iter().enumerate() {
                    let tile_id = (group * group_size + i) as u32;
                    let mut tile = Tile {
                        rows: Vec::with_capacity(chunk.len() / 74),
                        extended: Vec::new(),
                    };
                    let mut emit = |fields: [&[u8]; 7]| {
                        if let Some(row) = parser.event(fields)? {
                            if tile.rows.len() >= (1usize << row_bits) - 1 {
                                return None;
                            }
                            let loc = (tile_id << row_bits) | tile.rows.len() as u32;
                            let h = row.event.hash();
                            claims[((h >> 32) as usize) & (count - 1)]
                                .push((h << 32) | u64::from(loc + 1));
                            tile.push(row, chunk.as_ptr() as usize, fields[0], fields[3])?;
                        }
                        Some(())
                    };
                    if windows {
                        for fields in RowWindows::new(chunk) {
                            emit(fields)?
                        }
                    } else {
                        for fields in ChunkRows::new(chunk) {
                            emit(fields)?
                        }
                    }
                    tiles.push(tile);
                }
                Some((tiles, claims))
            })
            .collect::<Option<Vec<_>>>()?;
        let (tiles, claims): (Vec<_>, Vec<_>) = batches.into_iter().unzip();
        // Borrow original mapped or buffered bytes only within this call;
        // payloads remaining after adjudication have no borrowing requirement.
        let all: Vec<_> = tiles.iter().flatten().collect();
        let mask = (1 << row_bits) - 1;
        let lookup = |loc: u32| {
            let tile_id = (loc >> row_bits) as usize;
            let tile = all[tile_id];
            let row = &tile.rows[(loc & mask) as usize];
            tile.read(row.evidence, chunks[tile_id], parser.fast_time.is_some())
        };
        let reject = |loc: u32| {
            all[(loc >> row_bits) as usize].rows[(loc & mask) as usize]
                .ts
                .store(INVALID, Relaxed);
        };
        claims::mark_claims(transpose(claims, count), lookup, reject);
        Some(Self(tiles))
    }

    pub(super) fn route(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let router = claim_aliases::Router::new(links);
        let parts = self
            .0
            .into_par_iter()
            .map(|tiles| {
                let n: usize = tiles.iter().map(|t| t.rows.len()).sum();
                let mut output: Vec<_> =
                    (0..count).map(|_| Vec::with_capacity(n / count)).collect();
                let candidates = tiles.into_iter().flat_map(|tile| {
                    tile.rows.into_iter().map(move |row| {
                        let purchase = if row.evidence & EXTENDED == 0 {
                            0
                        } else {
                            tile.extended[(row.evidence ^ EXTENDED) as usize].purchase
                        };
                        Candidate {
                            user: row.user,
                            ts: row.ts.into_inner(),
                            purchase,
                            ..Candidate::default()
                        }
                    })
                });
                if let Some(router) = &router {
                    router.route(candidates, &mut output)
                } else {
                    route(candidates, links, &mut output)
                }
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
    fn source_witnesses_preserve_quotes_and_wide_offsets_in_both_scanners() {
        let mut data = vec![b'\n'; 1 << 20];
        let compact = b"\"evt\",user,0,\"2026-01-01T00:00:01Z\",view,p,\n";
        data.extend_from_slice(compact);
        for chunk in [compact.as_slice(), data.as_slice()] {
            for fields in [
                RowWindows::<7>::new(chunk).next().unwrap(),
                ChunkRows::<7>::new(chunk).next().unwrap(),
            ] {
                for fast in [false, true] {
                    let event = Key::pack(fields[0]).unwrap();
                    let ingest = if fast {
                        FastTime::ingest(fields[3])
                    } else {
                        Calendar::ingest_code(fields[3])
                    }
                    .unwrap();
                    for purchase in [0, 42] {
                        let row = Candidate {
                            event,
                            ingest,
                            purchase,
                            ..Candidate::default()
                        };
                        let mut tile = Tile {
                            rows: Vec::new(),
                            extended: Vec::new(),
                        };
                        tile.push(row, chunk.as_ptr() as usize, fields[0], fields[3])
                            .unwrap();
                        let code = tile.rows[0].evidence;
                        assert_eq!(
                            code & EXTENDED != 0,
                            chunk.len() >= 1 << 20 || purchase != 0
                        );
                        assert_eq!(tile.read(code, chunk, fast), (event, ingest));
                    }
                }
            }
        }
    }
}
