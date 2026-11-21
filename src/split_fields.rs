//! Decode the prefix and suffix delimiter ranks independently when the first
//! SIMD vector contains the two IDs. All other layouts use the general scanner.
use std::arch::x86_64::*;

pub(super) struct Masks {
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
pub(super) unsafe fn masks(p: *const u8) -> Masks {
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

#[inline(always)]
pub(super) fn event<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
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
    event_masks(
        data,
        first,
        join(middle.newlines, last.newlines),
        join(middle.commas, last.commas),
        join(middle.quotes, last.quotes),
        suffix_ranks,
    )
}

#[inline(always)]
pub(super) fn suffix_ranks(separators: u64, inside: u64) -> [usize; 4] {
    let time_end = 32 + separators.trailing_zeros() as usize;
    let ingest_end = 32 + (separators & (separators - 1)).trailing_zeros() as usize;
    let before_last = separators & inside;
    let product_end = 95 - before_last.leading_zeros() as usize;
    let kind_end = unsafe { 32 + _pdep_u64(4, separators).trailing_zeros() as usize };
    [time_end, ingest_end, kind_end, product_end]
}

#[inline(always)]
pub(super) fn event_masks<const N: usize>(
    data: &[u8],
    first: Masks,
    lf: u64,
    commas: u64,
    quote_mask: u64,
    ranks: impl FnOnce(u64, u64) -> [usize; 4],
) -> Option<(usize, Option<[&[u8]; N]>)> {
    if N != 7 || data.len() < 96 {
        return None;
    }
    if first.newlines != 0 || first.commas.count_ones() != 2 || first.commas >> 31 != 0 {
        return None;
    }
    if lf == 0 {
        return None;
    }
    let end = 32 + lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let separators = (commas & inside) | (lf & lf.wrapping_neg());
    if separators.count_ones() != 5 {
        return Some((end, None));
    }
    let quotes = quote_mask & inside;
    // Each quote must touch exactly one end of its field. The high word
    // starts inside field 3, so three opening bits and two closing bits
    // precede it. A quote at byte 31 can close at the first high separator.
    let starts1 = first.commas << 1 | 1;
    let ends1 = first.commas >> 1 | (separators as u32) << 31;
    let starts2 = separators << 1;
    if first.quotes & !(starts1 ^ ends1) != 0 || quotes & !(starts2 ^ (separators >> 1)) != 0 {
        return Some((end, None));
    }
    let (open, close) = unsafe {
        (
            _pext_u32(first.quotes, starts1) | ((_pext_u64(quotes, starts2) as u32) << 3),
            _pext_u32(first.quotes << 1, first.commas)
                | ((_pext_u64(quotes << 1 | u64::from(first.quotes >> 31), separators) as u32)
                    << 2),
        )
    };
    if open != close {
        return Some((end, None));
    }
    // Two ID boundaries are at opposite ends of a 32-bit mask. The four
    // remaining commas can be selected without waiting for the IDs' cuts.
    let id_end = first.commas.trailing_zeros() as usize;
    let user_end = 31 - first.commas.leading_zeros() as usize;
    let [time_end, ingest_end, kind_end, product_end] = ranks(separators, inside);
    let cuts = [
        0,
        id_end + 1,
        user_end + 1,
        time_end + 1,
        ingest_end + 1,
        kind_end + 1,
        product_end + 1,
        end + 1,
    ];
    Some((
        end,
        Some(std::array::from_fn(|i| {
            let q = (open as usize >> i) & 1;
            // Increasing separators are inside the first newline. A trim of
            // one requires distinct opening and closing quotes in the field.
            unsafe { data.get_unchecked(cuts[i] + q..cuts[i + 1] - 1 - q) }
        })),
    ))
}

#[inline(always)]
pub(super) fn short<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
    let data = data.get(..64)?;
    let (first, second) = unsafe { (masks(data.as_ptr()), masks(data.as_ptr().add(32))) };
    short_masks(
        data,
        join(first.newlines, second.newlines),
        join(first.commas, second.commas),
        join(first.quotes, second.quotes),
    )
}

#[inline(always)]
pub(super) fn short_masks<const N: usize>(
    data: &[u8],
    lf: u64,
    comma_mask: u64,
    quote_mask: u64,
) -> Option<(usize, Option<[&[u8]; N]>)> {
    if data.len() < 64 {
        return None;
    }
    let expected_commas = N.checked_sub(1)?;
    if lf == 0 {
        return None;
    }
    let end = lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let mut commas = comma_mask & inside;
    if end == 0 || commas.count_ones() as usize != expected_commas {
        return Some((end, None));
    }
    let boundaries = commas | (lf & lf.wrapping_neg());
    let starts = commas << 1 | 1;
    let quotes = quote_mask & inside;
    let open = unsafe { _pext_u64(quotes, starts) };
    if quotes & !(starts ^ (boundaries >> 1)) != 0
        || open != unsafe { _pext_u64(quotes << 1, boundaries) }
    {
        return Some((end, None));
    }
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
