//! Decode the prefix and suffix delimiter ranks independently when the first
//! SIMD vector contains the two IDs. All other layouts use the general scanner.
use std::arch::x86_64::*;

#[inline(always)]
unsafe fn masks(p: *const u8) -> (u32, u32, u32) {
    unsafe {
        let v = _mm256_loadu_si256(p.cast());
        let eq = |c| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32;
        (eq(b',' as i8), eq(b'"' as i8), eq(b'\n' as i8))
    }
}

#[inline(always)]
pub(super) fn event<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
    debug_assert!(data.len() >= 96 && N == 7);
    let (a, b, c) = unsafe {
        (
            masks(data.as_ptr()),
            masks(data.as_ptr().add(32)),
            masks(data.as_ptr().add(64)),
        )
    };
    if a.2 != 0 || a.0.count_ones() != 2 || a.0 >> 31 != 0 {
        return None;
    }
    let join = |x, y| u64::from(x) | (u64::from(y) << 32);
    let lf = join(b.2, c.2);
    if lf == 0 {
        return None;
    }
    let end = 32 + lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let separators = (join(b.0, c.0) & inside) | (lf & lf.wrapping_neg());
    if separators.count_ones() != 5 {
        return Some((end, None));
    }
    let quotes = join(b.1, c.1) & inside;
    let starts1 = a.0 << 1 | 1;
    let ends1 = a.0 >> 1 | (separators as u32) << 31;
    let starts2 = separators << 1;
    if a.1 & !(starts1 ^ ends1) != 0 || quotes & !(starts2 ^ (separators >> 1)) != 0 {
        return Some((end, None));
    }
    let (open, close) = unsafe {
        (
            _pext_u32(a.1, starts1) | ((_pext_u64(quotes, starts2) as u32) << 3),
            _pext_u32(a.1 << 1, a.0)
                | ((_pext_u64(quotes << 1 | u64::from(a.1 >> 31), separators) as u32) << 2),
        )
    };
    if open != close {
        return Some((end, None));
    }
    // Two ID boundaries are at opposite ends of a 32-bit mask. The four
    // remaining commas can be selected without waiting for the IDs' cuts.
    let d0 = a.0.trailing_zeros() as usize;
    let d1 = 31 - a.0.leading_zeros() as usize;
    let d2 = 32 + separators.trailing_zeros() as usize;
    let d3 = 32 + (separators & (separators - 1)).trailing_zeros() as usize;
    let before_last = separators & inside;
    let d5 = 95 - before_last.leading_zeros() as usize;
    let d4 = unsafe { 32 + _pdep_u64(4, separators).trailing_zeros() as usize };
    let cuts = [0, d0 + 1, d1 + 1, d2 + 1, d3 + 1, d4 + 1, d5 + 1, end + 1];
    Some((
        end,
        Some(std::array::from_fn(|i| {
            let q = (open as usize >> i) & 1;
            unsafe { data.get_unchecked(cuts[i] + q..cuts[i + 1] - 1 - q) }
        })),
    ))
}

#[inline(always)]
pub(super) fn short<const N: usize>(data: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
    debug_assert!(data.len() >= 64);
    let (a, b) = unsafe { (masks(data.as_ptr()), masks(data.as_ptr().add(32))) };
    let join = |x, y| u64::from(x) | (u64::from(y) << 32);
    let lf = join(a.2, b.2);
    if lf == 0 {
        return None;
    }
    let end = lf.trailing_zeros() as usize;
    let inside = !lf & lf.wrapping_sub(1);
    let mut commas = join(a.0, b.0) & inside;
    if end == 0 || commas.count_ones() as usize != N - 1 {
        return Some((end, None));
    }
    let boundaries = commas | (lf & lf.wrapping_neg());
    let starts = commas << 1 | 1;
    let quotes = join(a.1, b.1) & inside;
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
