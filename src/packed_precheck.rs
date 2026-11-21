//! Look at each physical row's ID prefix while the links are being resolved.
//! This pass may include malformed rows: they only make the exact path busier.
//! Singleton projections need neither a stored event ID nor a full dedup claim.
use super::*;

#[inline(always)]
fn point(word: u64) -> u32 {
    ((word ^ word.rotate_left(23)).wrapping_mul(0xc2b2ae3d27d4eb4f) >> 32) as u32
}

#[inline(always)]
fn has_byte(word: u64, byte: u8) -> bool {
    let dif = word ^ u64::from_le_bytes([byte; 8]);
    dif.wrapping_sub(0x0101010101010101) & !dif & 0x8080808080808080 != 0
}

/// Any short ID maps to zero. A long ID's first eight content bytes have
/// no quotes or separators under the input dialect, even when quoted.
#[inline(always)]
fn projection_at_row(suffix: &[u8]) -> u32 {
    let bytes = suffix.strip_prefix(b"\"").unwrap_or(suffix);
    let Some(prefix) = bytes.first_chunk::<8>() else {
        return 0;
    };
    let word = u64::from_le_bytes(*prefix);
    if has_byte(word, b',') || has_byte(word, b'"') {
        0
    } else {
        point(word)
    }
}

#[inline(always)]
fn projection_for_field(id: &[u8]) -> u32 {
    id.first_chunk::<8>()
        .map_or(0, |word| point(u64::from_le_bytes(*word)))
}

fn row_starts(chunk: &[u8], mut visit: impl FnMut(&[u8])) {
    if chunk.is_empty() {
        return;
    }
    visit(chunk);
    let mut used = 0;
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        while let Some(block) = chunk[used..].first_chunk::<32>() {
            // SAFETY: The slice contains a complete 32-byte block. The
            // unaligned load needs no additional address guarantee.
            let mut mask = unsafe {
                let v = _mm256_loadu_si256(block.as_ptr().cast());
                _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8))) as u32
            };
            while mask != 0 {
                let pos = used + mask.trailing_zeros() as usize + 1;
                if pos < chunk.len() {
                    visit(&chunk[pos..]);
                }
                mask &= mask - 1;
            }
            used += 32;
        }
    }
    for i in memchr::memchr_iter(b'\n', &chunk[used..]) {
        if used + i + 1 < chunk.len() {
            visit(&chunk[used + i + 1..]);
        }
    }
}

pub(super) struct Screen(Vec<u64>);

impl Screen {
    pub(super) fn build(chunks: &[&[u8]], parts: usize) -> Option<Self> {
        const MAX_WORDS: usize = 1 << 23;
        if parts > MAX_WORDS {
            return None;
        }
        let bytes: usize = chunks.iter().map(|c| c.len()).sum();
        let words = (bytes / 75 / 4).clamp(parts, MAX_WORDS).next_power_of_two();
        let mask = (words * 64 - 1) as u32;
        let width = words / parts;
        let shift = (64 * width).trailing_zeros();
        let span = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 4)
            .max(1);
        let all = chunks
            .par_chunks(span)
            .map(|group| {
                let approx = group.iter().map(|c| c.len()).sum::<usize>() / 75 / parts;
                let mut out: Vec<_> = (0..parts).map(|_| Vec::with_capacity(approx)).collect();
                for &chunk in group {
                    row_starts(chunk, |suffix| {
                        let h = projection_at_row(suffix) & mask;
                        out[(h >> shift) as usize].push(h);
                    });
                }
                out
            })
            .collect();
        let mut collisions = vec![0_u64; words];
        collisions
            .par_chunks_mut(width)
            .zip(transpose(all, parts))
            .for_each(|(repeated, group)| {
                let mut seen = vec![0_u64; width];
                for p in group.into_iter().flatten() {
                    let i = (p as usize / 64) & (width - 1);
                    let bit = 1 << (p & 63);
                    repeated[i] |= seen[i] & bit;
                    seen[i] |= bit;
                }
            });
        Some(Self(collisions))
    }

    fn ambiguous(&self, id: &[u8]) -> bool {
        let p = projection_for_field(id);
        self.0[(p as usize >> 6) & (self.0.len() - 1)] & (1 << (p & 63)) != 0
    }

    pub(super) fn decode(
        self,
        chunks: &[&[u8]],
        parser: &Parser<'_, '_>,
        links: &Aliases,
        count: usize,
        parts: usize,
        windows: bool,
    ) -> Option<Vec<Vec<Vec<Event>>>> {
        let width = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let decoded = chunks
            .par_chunks(width)
            .map(|group| {
                let approx = group.iter().map(|c| c.len()).sum::<usize>() / 75;
                let mut out: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(approx / count))
                    .collect();
                let mut retry = buckets(parts);
                let mut buffer = [Event {
                    user: Key(0),
                    ts: 0,
                    purchase: 0,
                }; 32];
                let mut used = 0;
                let mut flush = |events: &[Event]| {
                    if let Aliases::Sparse(table) = links {
                        for row in events {
                            table.prefetch(row.user);
                        }
                    }
                    for row in events {
                        let user = links.resolve(row.user);
                        out[bucket(user, count)].push(Event { user, ..*row });
                    }
                };
                let mut emit = |fields| {
                    if let Some(row) = parser.event_with_key(fields, |id| {
                        if self.ambiguous(id) {
                            Key::pack(id)
                        } else {
                            Some(Key(0))
                        }
                    })? {
                        if row.event.0 != 0 {
                            retry[bucket(row.event, parts)].push(row);
                        } else if row.ts != INVALID {
                            buffer[used] = Event {
                                user: row.user,
                                ts: row.ts,
                                purchase: row.purchase,
                            };
                            used += 1;
                            if used == buffer.len() {
                                flush(&buffer);
                                used = 0;
                            }
                        }
                    }
                    Some(())
                };
                for &chunk in group {
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
                flush(&buffer[..used]);
                Some((out, retry))
            })
            .collect::<Option<Vec<_>>>()?;
        let (out, retry): (Vec<_>, Vec<_>) = decoded.into_iter().unzip();
        let repaired = transpose(retry, parts)
            .into_par_iter()
            .map(|g| dedup_indexed(g, links, count))
            .collect();
        let mut out = transpose(out, count);
        for (left, mut right) in out.iter_mut().zip(transpose(repaired, count)) {
            left.append(&mut right);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn valid_quoted_and_unquoted_rows_project_identically_across_chunk_edges() {
        for len in 1..=24 {
            let id = vec![b'x'; len];
            let p = projection_for_field(&id);
            for n in 0..64 {
                let mut bytes = vec![b'z'; n];
                bytes.push(b'\n');
                bytes.extend(&id);
                bytes.extend_from_slice(b",u,x,x,v,p,\n\"");
                bytes.extend(&id);
                bytes.extend_from_slice(b"\",u,x,x,v,p,");
                let mut points = Vec::new();
                row_starts(&bytes, |row| points.push(projection_at_row(row)));
                assert_eq!(&points[1..], &[p, p]);
            }
        }
    }
}
