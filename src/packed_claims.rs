//! Scatter compact fingerprint/location claims while keeping payloads in decode order.
//! Only potential duplicates read the full ID and ingest timestamp.
use super::*;

/// Bits 0..32 record first sightings, bits 32..64 repeated sightings.
/// Singleton positions never need an exact slot. A repeated position can
/// contain arbitrarily many distinct full keys, so size the exact table by
/// subtracting the *number of singleton rows*, not by counting repeated bits.
fn repeated_positions(groups: &[Vec<u64>], rows: usize) -> (Vec<u32>, usize) {
    let words = rows.div_ceil(2).next_power_of_two();
    let mut counts = vec![0u64; words];
    for &entry in groups.iter().flatten() {
        let h = (entry >> 32) as usize;
        let index = (h >> 5) & (words - 1);
        let bit = 1u64 << (h & 31);
        let old = counts[index];
        counts[index] = old | bit | ((old & bit) << 32);
    }
    let mut ambiguous = rows;
    let repeated = counts
        .into_iter()
        .map(|pair| {
            let duplicate = (pair >> 32) as u32;
            ambiguous -= (pair as u32 & !duplicate).count_ones() as usize;
            duplicate
        })
        .collect();
    (repeated, ambiguous)
}

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
        let strategy = std::env::var_os("SHOPPING_CLAIMS");
        let screen = strategy.as_deref() == Some(std::ffi::OsStr::new("screen"));
        let paired = strategy.as_deref() == Some(std::ffi::OsStr::new("pair"));
        claims.into_par_iter().for_each(|groups| {
            let n: usize = groups.iter().map(Vec::len).sum();
            if n == 0 {
                return;
            }
            let mut conflicts = Vec::new();
            let mut repeated = Vec::new();
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
                // Exact slot capacity must fit even a pathological set of
                // different keys mapped onto one bit.
                groups
                    .iter()
                    .flatten()
                    .filter(|&&entry| {
                        let h = (entry >> 32) as usize;
                        conflicts[(h >> 6) & (words - 1)] >> (h & 63) & 1 != 0
                    })
                    .count()
            } else if paired {
                let (bitmap, ambiguous) = repeated_positions(&groups, n);
                repeated = bitmap;
                ambiguous
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
                if paired {
                    let h = (entry >> 32) as usize;
                    if repeated[(h >> 5) & (repeated.len() - 1)] >> (h & 31) & 1 == 0 {
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
        drop(metadata);
        route_marked(events, links, count)
    }
}

fn route_marked(
    events: Vec<Vec<MarkedEvent>>,
    links: &Aliases,
    count: usize,
) -> Vec<Vec<Vec<Event>>> {
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

#[derive(Clone, Copy)]
struct LocalClaim {
    key: Key,
    ingest: u32,
    loc: u32,
}

const WIDE: u32 = 1 << 31;

/// Absolute ingest codes often share a prefix. Preserve outliers by index.
#[derive(Default)]
struct IngestCodes {
    prefix: u64,
    wide: Vec<u64>,
}

impl IngestCodes {
    #[inline]
    fn encode(&mut self, ingest: u64) -> Option<u32> {
        if ingest >> 31 == self.prefix {
            return Some(ingest as u32 & !WIDE);
        }
        let index = u32::try_from(self.wide.len()).ok()?;
        if index >= WIDE {
            return None;
        }
        self.wide.push(ingest);
        Some(index | WIDE)
    }

    #[inline]
    fn decode(&self, code: u32) -> u64 {
        if code < WIDE {
            (self.prefix << 31) | u64::from(code)
        } else {
            self.wide[(code ^ WIDE) as usize]
        }
    }
}

pub(super) struct LocalClaims {
    events: Vec<Vec<MarkedEvent>>,
    claims: Vec<Vec<Vec<LocalClaim>>>,
    codes: Vec<IngestCodes>,
    row_bits: u32,
}

impl LocalClaims {
    pub(super) fn parse(
        chunks: &[&[u8]],
        dedup_count: usize,
        parser: &Parser<'_, '_>,
        windows: bool,
    ) -> Option<Self> {
        let size = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let tasks = chunks
            .len()
            .div_ceil(size)
            .max(2)
            .checked_next_power_of_two()?;
        let row_bits = 32_u32.checked_sub(tasks.trailing_zeros())?;
        let parts: Vec<_> = chunks
            .par_chunks(size)
            .enumerate()
            .map(|(task, chunks)| {
                let n = chunks.iter().map(|chunk| chunk.len()).sum::<usize>() / 74;
                let mut events = Vec::with_capacity(n);
                let mut claims: Vec<_> = (0..dedup_count)
                    .map(|_| Vec::with_capacity(n / dedup_count))
                    .collect();
                let mut codes = IngestCodes::default();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        if events.len() >= 1_usize << row_bits {
                            return None;
                        }
                        if events.is_empty() {
                            codes.prefix = row.ingest >> 31;
                        }
                        let loc = ((task as u32) << row_bits) | events.len() as u32;
                        let ingest = codes.encode(row.ingest)?;
                        claims[bucket(row.event, dedup_count)].push(LocalClaim {
                            key: row.event,
                            ingest,
                            loc,
                        });
                        events.push(MarkedEvent {
                            user: row.user,
                            ts: row.ts.into(),
                            purchase: row.purchase,
                        });
                    }
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
                Some(((events, codes), claims))
            })
            .collect::<Option<Vec<_>>>()?;
        let (meta, claims): (Vec<_>, Vec<_>) = parts.into_iter().unzip();
        let (events, codes) = meta.into_iter().unzip();
        Some(Self {
            events,
            claims: transpose(claims, dedup_count),
            codes,
            row_bits,
        })
    }

    pub(super) fn route(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let Self {
            events,
            claims,
            codes,
            row_bits,
        } = self;
        let mask = (1 << row_bits) - 1;
        let time = |row: &LocalClaim| codes[(row.loc >> row_bits) as usize].decode(row.ingest);
        let reject = |loc: u32| {
            events[(loc >> row_bits) as usize][(loc & mask) as usize]
                .ts
                .store(INVALID, std::sync::atomic::Ordering::Relaxed)
        };
        claims.into_par_iter().for_each(|groups| {
            let n: usize = groups.iter().map(Vec::len).sum();
            let mut slots: Vec<Option<&LocalClaim>> =
                vec![None; (n + n / 2 + 1).next_power_of_two()];
            let mask = slots.len() - 1;
            for row in groups.iter().flatten() {
                let mut slot = row.key.hash() as usize & mask;
                loop {
                    match slots[slot] {
                        None => {
                            slots[slot] = Some(row);
                            break;
                        }
                        Some(old) if old.key == row.key => {
                            if time(row) < time(old) {
                                reject(old.loc);
                                slots[slot] = Some(row);
                            } else {
                                reject(row.loc);
                            }
                            break;
                        }
                        _ => slot = (slot + 1) & mask,
                    }
                }
            }
        });
        route_marked(events, links, count)
    }
}

// Store the event itself in its claim partition. A normal zero-purchase row
// needs no payload sidecar. Uncommon large timestamps use the same wide path
// as purchases, including dates outside the compact relative range.
#[derive(Clone, Copy)]
struct ThinEvent {
    event: Key,
    user: Key,
    ingest: u32,
    payload: u32,
}

struct Region {
    rows: Vec<ThinEvent>,
    sidecar: Vec<(u32, u32)>,
    codes: IngestCodes,
}

impl Region {
    fn push(&mut self, row: Candidate) -> Option<()> {
        if self.rows.is_empty() {
            self.codes.prefix = row.ingest >> 31;
        }
        let payload = if row.ts == INVALID || (row.purchase == 0 && row.ts < WIDE) {
            row.ts
        } else {
            let index = u32::try_from(self.sidecar.len()).ok()?;
            if index >= WIDE - 1 {
                return None;
            }
            self.sidecar.push((row.ts, row.purchase));
            index | WIDE
        };
        self.rows.push(ThinEvent {
            event: row.event,
            user: row.user,
            ingest: self.codes.encode(row.ingest)?,
            payload,
        });
        Some(())
    }
}

pub(super) struct InlineClaims {
    parts: Vec<Vec<Region>>,
    row_bits: u32,
}

impl InlineClaims {
    pub(super) fn parse(
        chunks: &[&[u8]],
        dedup_count: usize,
        parser: &Parser<'_, '_>,
        windows: bool,
    ) -> Option<Self> {
        let size = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let tasks = chunks
            .len()
            .div_ceil(size)
            .max(2)
            .checked_next_power_of_two()?;
        let row_bits = 32_u32.checked_sub(tasks.trailing_zeros())?;
        let partial: Vec<_> = chunks
            .par_chunks(size)
            .map(|chunks| {
                let n = chunks.iter().map(|chunk| chunk.len()).sum::<usize>() / 74 / dedup_count;
                let mut regions: Vec<_> = (0..dedup_count)
                    .map(|_| Region {
                        rows: Vec::with_capacity(n),
                        codes: IngestCodes::default(),
                        sidecar: Vec::new(),
                    })
                    .collect();
                let mut emit = |fields| {
                    if let Some(row) = parser.event(fields)? {
                        regions[bucket(row.event, dedup_count)].push(row)?;
                    }
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
                regions
                    .iter()
                    .all(|r| r.rows.len() < (1_usize << row_bits))
                    .then_some(regions)
            })
            .collect::<Option<Vec<_>>>()?;
        let mut parts = buckets(dedup_count);
        for tile in partial {
            for (regions, group) in parts.iter_mut().zip(tile) {
                if !group.rows.is_empty() {
                    regions.push(group)
                }
            }
        }
        Some(Self { parts, row_bits })
    }

    pub(super) fn route(self, links: &Aliases, count: usize) -> Vec<Vec<Vec<Event>>> {
        let Self { parts, row_bits } = self;
        let row_mask = (1 << row_bits) - 1;
        let partial = parts
            .into_par_iter()
            .map(|mut regions| {
                let n = regions.iter().map(|r| r.rows.len()).sum::<usize>();
                let mut slots = vec![0u64; (n + n / 2 + 1).next_power_of_two()];
                let mask = slots.len() - 1;
                for g in 0..regions.len() {
                    for i in 0..regions[g].rows.len() {
                        let row = regions[g].rows[i];
                        let hash = row.event.hash();
                        let loc = ((g as u32) << row_bits) | i as u32;
                        let entry = (hash & 0xffff_ffff_0000_0000) | u64::from(loc + 1);
                        let mut pos = hash as usize & mask;
                        loop {
                            let old = slots[pos];
                            if old == 0 {
                                slots[pos] = entry;
                                break;
                            }
                            if (old ^ entry) >> 32 == 0 {
                                let old_loc = old as u32 - 1;
                                let j = (old_loc >> row_bits) as usize;
                                let k = (old_loc & row_mask) as usize;
                                let prev = regions[j].rows[k];
                                if prev.event == row.event {
                                    if regions[g].codes.decode(row.ingest)
                                        < regions[j].codes.decode(prev.ingest)
                                    {
                                        regions[j].rows[k].payload = INVALID;
                                        slots[pos] = entry;
                                    } else {
                                        regions[g].rows[i].payload = INVALID;
                                    }
                                    break;
                                }
                            }
                            pos = (pos + 1) & mask;
                        }
                    }
                }
                let survivors = regions.into_iter().flat_map(|region| {
                    region.rows.into_iter().filter_map(move |row| {
                        if row.payload == INVALID {
                            return None;
                        }
                        let (ts, purchase) = if row.payload < WIDE {
                            (row.payload, 0)
                        } else {
                            region.sidecar[(row.payload ^ WIDE) as usize]
                        };
                        Some(Candidate {
                            user: row.user,
                            ts,
                            purchase,
                            ..Candidate::default()
                        })
                    })
                });
                let mut output: Vec<_> =
                    (0..count).map(|_| Vec::with_capacity(n / count)).collect();
                route(survivors, links, &mut output);
                output
            })
            .collect();
        transpose(partial, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_sightings_count_every_ambiguous_row() {
        let entry = |h: u64| h << 32;
        let groups = [
            vec![entry(11), entry(17), entry(42)],
            vec![entry(17), entry(42), entry(42)],
        ];
        let (repeated, rows) = repeated_positions(&groups, 6);
        let bit = |h: usize| repeated[(h >> 5) & (repeated.len() - 1)] >> (h & 31) & 1;
        assert_eq!(rows, 5);
        assert_eq!([bit(11), bit(17), bit(42)], [0, 1, 1]);
    }

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
