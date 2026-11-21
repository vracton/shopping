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
    if first.newlines != 0 || first.commas.count_ones() != 2 || first.commas >> 31 != 0 {
        return None;
    }
    let lf = join(middle.newlines, last.newlines);
    if lf == 0 {
        return None;
    }
    let end = 32 + lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let separators = (join(middle.commas, last.commas) & inside) | (lf & lf.wrapping_neg());
    if separators.count_ones() != 5 {
        return Some((end, None));
    }
    let quotes = join(middle.quotes, last.quotes) & inside;
    // An opening quote adds a carry through the field's non-separator
    // bits. Each carry stops at exactly that field's next delimiter.
    // All remaining quotes must be immediately before those delimiters.
    let bounds = u128::from(first.commas) | (u128::from(separators) << 32);
    let quoted = u128::from(first.quotes) | (u128::from(quotes) << 32);
    let starts = bounds << 1 | 1;
    let opened = quoted & starts;
    let closes = quoted ^ opened;
    if closes << 1 != ((!bounds).wrapping_add(opened) & bounds) {
        return Some((end, None));
    }
    let open = unsafe {
        _pext_u32(first.quotes, starts as u32)
            | ((_pext_u64(quotes, (starts >> 32) as u64) as u32) << 3)
    };
    // Two ID boundaries are at opposite ends of a 32-bit mask. The four
    // remaining commas can be selected without waiting for the IDs' cuts.
    let id_end = first.commas.trailing_zeros() as usize;
    let user_end = 31 - first.commas.leading_zeros() as usize;
    let time_end = 32 + separators.trailing_zeros() as usize;
    let ingest_end = 32 + (separators & (separators - 1)).trailing_zeros() as usize;
    let before_last = separators & inside;
    let product_end = 95 - before_last.leading_zeros() as usize;
    let kind_end = unsafe { 32 + _pdep_u64(4, separators).trailing_zeros() as usize };
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
