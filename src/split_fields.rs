//! Decode the prefix and suffix delimiter ranks independently when the first
//! SIMD vector contains the two IDs. All other layouts use the general scanner.
use std::arch::x86_64::*;

struct Masks {
    commas: u32,
    quotes: u32,
    newlines: u32,
}

#[inline(always)]
fn join(low: u32, high: u32) -> u64 {
    u64::from(low) | (u64::from(high) << 32)
}

// Caller provides 32 readable bytes.
#[inline(always)]
unsafe fn masks(p: *const u8) -> Masks {
    unsafe {
        let v = _mm256_loadu_si256(p.cast());
        let eq = |c| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32;
        Masks {
            commas: eq(b',' as i8),
            quotes: eq(b'"' as i8),
            newlines: eq(b'\n' as i8),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Prefix {
    signature: u64,
    id: u8,
    id_len: u8,
    user: u8,
    user_len: u8,
    ts: u8,
    ts_quote: u8,
}

const fn prefix_slot(signature: u64) -> usize {
    (signature.wrapping_mul(0x9e3779b97f4a7c15) >> 53) as usize
}

#[derive(Clone, Copy, Default)]
struct Tail {
    commas: u64,
    quotes: u64,
    starts: [u8; 4],
    lengths: [u8; 4],
}

const fn tail_slot(commas: u64, quotes: u64) -> usize {
    ((commas.wrapping_mul(0xc13fa9a902a6328f) ^ quotes.wrapping_mul(0x91e10da5c79e7b1d)) >> 51)
        as usize
}

struct Layout {
    heads: Vec<Prefix>,
    tails: Vec<Tail>,
}

static TABLE: std::sync::OnceLock<Layout> = std::sync::OnceLock::new();

// Learn field boundaries only from rows the general CSV parser accepted.
// Different inputs, collisions, and sampling order can change the hit rate
// but never which rows or field values the scanner emits.
pub(crate) fn observe(chunks: &[&[u8]]) {
    TABLE.get_or_init(|| {
        let mut table = Layout {
            heads: vec![Prefix::default(); 2048],
            tails: vec![Tail::default(); 8192],
        };
        let mut head_votes = vec![0u16; 2048];
        let mut tail_votes = vec![0u16; 8192];
        for chunk in chunks.iter().step_by((chunks.len() / 64).max(1)).take(64) {
            for row in super::rows(chunk).take(128) {
                if !(34..96).contains(&row.len()) {
                    continue;
                }
                let Some(fields) = super::fields::<7>(row) else {
                    continue;
                };
                let mut comma = 0u64;
                let mut quotes = 0u64;
                let mut tail_c = 0u64;
                let mut tail_q = 0u64;
                for (i, &byte) in row.iter().enumerate() {
                    let (c, q, bit) = if i < 32 {
                        (&mut comma, &mut quotes, 1u64 << i)
                    } else {
                        (&mut tail_c, &mut tail_q, 1u64 << (i - 32))
                    };
                    if byte == b',' {
                        *c |= bit;
                    }
                    if byte == b'"' {
                        *q |= bit;
                    }
                }
                if comma.count_ones() != 2 || comma >> 31 != 0 {
                    continue;
                }
                let cut = fields[2].as_ptr() as usize - row.as_ptr() as usize;
                let ts_open = row[comma.ilog2() as usize + 1] == b'"';
                let ts_end = cut + fields[2].len() + usize::from(ts_open);
                if ts_end < 32 || cut > 31 || (ts_open && ts_end == 32) {
                    continue;
                }
                let sig = join(comma as u32, quotes as u32);
                let si = prefix_slot(sig);
                let head = Prefix {
                    signature: sig,
                    id: (fields[0].as_ptr() as usize - row.as_ptr() as usize) as u8,
                    id_len: fields[0].len() as u8,
                    user: (fields[1].as_ptr() as usize - row.as_ptr() as usize) as u8,
                    user_len: fields[1].len() as u8,
                    ts: cut as u8,
                    ts_quote: u8::from(ts_open),
                };
                // A majority vote keeps a frequent shape if several share a slot.
                if head_votes[si] == 0 || table.heads[si].signature == sig {
                    head_votes[si] += 1;
                    table.heads[si] = head;
                } else {
                    head_votes[si] -= 1;
                }
                // Normalize the suffix at the delimiter after the timestamp so
                // different timestamp widths can reuse the same layout.
                let sc = (tail_c | 1u64 << (row.len() - 32)) >> (ts_end - 32);
                let sq = tail_q >> (ts_end - 32);
                let ti = tail_slot(sc, sq);
                if tail_votes[ti] == 0
                    || (table.tails[ti].commas, table.tails[ti].quotes) == (sc, sq)
                {
                    tail_votes[ti] += 1;
                    table.tails[ti] = Tail {
                        commas: sc,
                        quotes: sq,
                        starts: std::array::from_fn(|i| {
                            (fields[i + 3].as_ptr() as usize - row.as_ptr() as usize - ts_end) as u8
                        }),
                        lengths: std::array::from_fn(|i| fields[i + 3].len() as u8),
                    };
                } else {
                    tail_votes[ti] -= 1;
                }
            }
        }
        table
    });
}

#[inline(always)]
pub(super) fn event<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
    if let Some(ahead) = data.get(640) {
        unsafe { _mm_prefetch::<_MM_HINT_T0>(std::ptr::from_ref(ahead).cast()) };
    }
    let data = data.get(..96)?;
    if N != 7 {
        return None;
    }
    let (first, middle, last) = unsafe {
        (
            masks(data.as_ptr()),
            masks(data.as_ptr().add(32)),
            masks(data.as_ptr().add(64)),
        )
    };
    let signature = join(first.commas, first.quotes);
    let table = TABLE.get()?;
    let prefix = table.heads[prefix_slot(signature)];
    if first.newlines != 0 || signature == 0 || prefix.signature != signature {
        return None;
    }
    let lf = join(middle.newlines, last.newlines);
    if lf == 0 {
        return None;
    }
    let end = 32 + lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let separators = (join(middle.commas, last.commas) & inside) | (lf & lf.wrapping_neg());
    let quotes = join(middle.quotes, last.quotes) & inside;
    let shift = separators.trailing_zeros();
    let time_end = 32 + shift as usize;
    let before = (1u64 << shift) - 1;
    // The prefix checked everything through byte 31. In the rest of the
    // timestamp, only its closing quote is allowed.
    let expected_close = (before ^ before.wrapping_shr(1)) * u64::from(prefix.ts_quote);
    if quotes & before != expected_close || (shift == 0 && prefix.ts_quote != 0) {
        return None;
    }
    let tail_commas = separators >> shift;
    let tail_quotes = quotes >> shift;
    let tail = table.tails[tail_slot(tail_commas, tail_quotes)];
    if tail.commas != tail_commas || tail.quotes != tail_quotes {
        return None;
    }
    Some((
        end,
        Some(std::array::from_fn(|i| {
            let (start, stop) = match i {
                0 => (prefix.id as usize, usize::from(prefix.id + prefix.id_len)),
                1 => (
                    prefix.user as usize,
                    usize::from(prefix.user + prefix.user_len),
                ),
                2 => (prefix.ts as usize, time_end - usize::from(prefix.ts_quote)),
                _ => {
                    let start = time_end + tail.starts[i - 3] as usize;
                    (start, start + tail.lengths[i - 3] as usize)
                }
            };
            // Both masks match parsed exemplars. The prefix fixes the first
            // two fields, and the normalized suffix includes the row terminator.
            unsafe { data.get_unchecked(start..stop) }
        })),
    ))
}

#[inline(always)]
pub(super) fn short<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
    let data = data.get(..64)?;
    let expected_commas = N.checked_sub(1)?;
    let (first, second) = unsafe { (masks(data.as_ptr()), masks(data.as_ptr().add(32))) };
    let lf = join(first.newlines, second.newlines);
    if lf == 0 {
        return None;
    }
    let end = lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let mut commas = join(first.commas, second.commas) & inside;
    if end == 0 || commas.count_ones() as usize != expected_commas {
        return Some((end, None));
    }
    let boundaries = commas | (lf & lf.wrapping_neg());
    let starts = commas << 1 | 1;
    let quotes = join(first.quotes, second.quotes) & inside;
    let opened = quotes & starts;
    if (quotes ^ opened) << 1 != ((!boundaries).wrapping_add(opened) & boundaries) {
        return Some((end, None));
    }
    let open = unsafe { _pext_u64(opened, starts) };
    let mut start = 0;
    Some((
        end,
        Some(std::array::from_fn(|i| {
            let stop = if i + 1 == N {
                end
            } else {
                let n = commas.trailing_zeros() as usize;
                commas &= commas - 1;
                n
            };
            let q = (open as usize >> i) & 1;
            // The arity and paired quote checks prove this range is ordered
            // and ends at or before the first newline.
            let field = unsafe { data.get_unchecked(start + q..stop - q) };
            start = stop + 1;
            field
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::{RowWindows, fields};

    #[test]
    fn learned_descriptors_preserve_malformed_rows_and_vector_edges() {
        let mut samples = Vec::new();
        for a in [1, 5, 8, 9, 13, 16] {
            for b in [0, 4, 7, 9, 12] {
                for mask in 0u8..128 {
                    let fields = [
                        vec![b'x'; a],
                        vec![b'y'; b],
                        b"2028-01-02T03:04:05Z".to_vec(),
                        b"2028-01-03T04:05:06Z".to_vec(),
                        b"view".to_vec(),
                        b"p123".to_vec(),
                        vec![b'0'; usize::from(mask % 9)],
                    ];
                    let fields: Vec<_> = fields
                        .into_iter()
                        .enumerate()
                        .map(|(i, mut x)| {
                            if mask >> i & 1 != 0 {
                                x.insert(0, b'"');
                                x.push(b'"');
                            }
                            x
                        })
                        .collect();
                    samples.push(fields.join(&b','));
                }
            }
        }
        let corpus: Vec<_> = samples
            .chunks(128)
            .map(|rows| {
                let mut chunk = rows.join(&b'\n');
                chunk.push(b'\n');
                chunk
            })
            .collect();
        super::observe(&corpus.iter().map(Vec::as_slice).collect::<Vec<_>>());
        assert!(
            super::TABLE
                .get()
                .unwrap()
                .heads
                .iter()
                .any(|h| h.signature != 0)
        );
        let mut hits = 0;
        for row in samples.iter().rev() {
            let mut data = row.clone();
            data.push(b'\n');
            data.extend_from_slice(&[b'_'; 128]);
            hits += usize::from(super::event::<7>(&data).is_some());
            for i in 0..row.len() {
                for c in [row[i], b'"', b',', b'\r'] {
                    data[i] = c;
                    let expected = fields::<7>(&data[..row.len()]);
                    let got = RowWindows::<7>::new(&data).next();
                    assert_eq!(got, expected, "{i} {c} {row:?}");
                }
                data[i] = row[i];
            }
        }
        assert!(hits > 0);
    }

    #[test]
    fn split_ranks_match_scalar_at_vector_edges() {
        for first in [0, 1, 8, 15, 29, 30, 31] {
            for ts in [0, 1, 7, 13, 20, 25, 32, 64] {
                for field in 0..7 {
                    let mut values = [
                        vec![b'x'; first],
                        b"u".to_vec(),
                        vec![b't'; ts],
                        b"2028-01-02T03:04:05Z".to_vec(),
                        b"view".to_vec(),
                        b"p".to_vec(),
                        vec![],
                    ];
                    for spelling in [
                        b"\"\"".as_slice(),
                        b"\"",
                        b"\"x\"",
                        b"x\"",
                        b"\"x",
                        b"x\rx",
                        b"\",\"",
                        b"\0",
                        b"\xff",
                    ] {
                        values[field] = spelling.to_vec();
                        let row = values.join(&b',');
                        let expected = fields::<7>(&row);
                        let mut data = row.clone();
                        data.push(b'\n');
                        data.extend_from_slice(&[b'x'; 128]);
                        assert_eq!(RowWindows::<7>::new(&data).next(), expected, "{row:?}");
                    }
                }
            }
        }
    }
}
