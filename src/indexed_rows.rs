//! Build the field spans of a short run of complete rows in one streaming scan.
//! A block with irregular structure is decoded by the general row scanner.
use super::SingleRowWindows;
use memchr::{memchr, memrchr};
use std::arch::x86_64::*;

const BLOCK: usize = 1024;

#[derive(Clone, Copy, Default)]
struct Boundary {
    end: u16,
    next: u16,
}

pub struct RowWindows<'a, const N: usize> {
    remaining: &'a [u8],
    block: &'a [u8],
    cuts: [Boundary; BLOCK + 1],
    pos: usize,
    count: usize,
    fallback: SingleRowWindows<'a, N>,
}

impl<'a, const N: usize> RowWindows<'a, N> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            remaining: bytes,
            block: &[],
            cuts: [Boundary::default(); BLOCK + 1],
            pos: 0,
            count: 0,
            fallback: SingleRowWindows::new(&[]),
        }
    }

    #[inline(never)]
    fn refill(&mut self) {
        let data = self.remaining;
        self.pos = 0;
        self.count = 0;
        if data.len() > BLOCK + 32 && (2..=16).contains(&N) {
            if let Some(last) = memrchr(b'\n', &data[..BLOCK]) {
                let len = last + 1;
                (self.block, self.remaining) = data.split_at(len);
                if let Some(n) = index::<N>(data, len, &mut self.cuts) {
                    self.count = n;
                } else {
                    self.fallback = SingleRowWindows::new(self.block);
                }
                return;
            }
        }
        let len = if data.len() > BLOCK + 32 {
            memchr(b'\n', &data[BLOCK..]).map_or(data.len(), |at| BLOCK + at + 1)
        } else {
            data.len()
        };
        (self.block, self.remaining) = data.split_at(len);
        self.fallback = SingleRowWindows::new(self.block);
    }
}

impl<'a, const N: usize> Iterator for RowWindows<'a, N> {
    type Item = [&'a [u8]; N];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos < self.count {
                let first = self.pos;
                self.pos += N;
                // index() validates the arity of every row and both quote ends
                // of every field. Each start <= end is inside this block.
                return Some(std::array::from_fn(|i| unsafe {
                    let start = self.cuts.get_unchecked(first + i).next as usize;
                    let end = self.cuts.get_unchecked(first + i + 1).end as usize;
                    self.block.get_unchecked(start..end)
                }));
            }
            if let Some(row) = self.fallback.next() {
                return Some(row);
            }
            if self.remaining.is_empty() {
                return None;
            }
            self.refill();
        }
    }
}

fn index<const N: usize>(
    data: &[u8],
    len: usize,
    cuts: &mut [Boundary; BLOCK + 1],
) -> Option<usize> {
    // One bit at the last field of each row, repeated for the next 32+N fields.
    let mut newline_pattern = 0u64;
    for i in (N - 1..64).step_by(N) {
        newline_pattern |= 1 << i;
    }
    let mut count = 0;
    let mut prev = b'\n';
    let mut open = u64::from(data[0] == b'"');
    cuts[0].next = open as u16;
    for offset in (0..len).step_by(32) {
        // Caller has at least 33 readable bytes at every vector start.
        let (delims, lf, q) = unsafe {
            let v = _mm256_loadu_si256(data.as_ptr().add(offset).cast());
            let cmp = |c| _mm256_cmpeq_epi8(v, _mm256_set1_epi8(c));
            let mask = |v| u64::from(_mm256_movemask_epi8(v) as u32);
            let lf = cmp(b'\n' as i8);
            (
                mask(_mm256_or_si256(lf, cmp(b',' as i8))),
                mask(lf),
                mask(cmp(b'"' as i8)),
            )
        };
        let following = data[offset + 32];
        let before = (delims << 1) | u64::from(matches!(prev, b',' | b'\n'));
        let after = (delims >> 1) | (u64::from(matches!(following, b',' | b'\n')) << 31);
        let used = (1_u64 << (len - offset).min(32)) - 1;
        // Each quote must touch exactly one delimiter. This rejects interior
        // quotes and a field containing only one double quote.
        if q & used & !(before ^ after) != 0 {
            return None;
        }
        let mut seps = delims & used;
        let n = seps.count_ones();
        let bits = (1u64 << n) - 1;
        let (mut starts, mut ends, newlines) = unsafe {
            (
                _pext_u64((q >> 1) | (u64::from(following == b'"') << 31), seps),
                _pext_u64((q << 1) | u64::from(prev == b'"'), seps),
                _pext_u64(lf, seps),
            )
        };
        let openings = (starts << 1) | open;
        if openings & bits != ends || newlines != (newline_pattern >> (count % N)) & bits {
            return None;
        }
        open = openings >> n;
        while seps != 0 {
            let at = offset + seps.trailing_zeros() as usize;
            count += 1;
            // At most one separator per input byte, and len <= BLOCK.
            unsafe {
                *cuts.get_unchecked_mut(count) = Boundary {
                    end: at as u16 - (ends as u16 & 1),
                    next: at as u16 + 1 + (starts as u16 & 1),
                };
            }
            ends >>= 1;
            starts >>= 1;
            seps &= seps - 1;
        }
        prev = data[offset + 31];
    }
    Some(count)
}
