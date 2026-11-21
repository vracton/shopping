//! One 512-bit load covers the entire suffix after the first 32 bytes.
//! Dispatch outside the row loop so unsupported CPUs never enter wide code.
use super::{RowWindows, fields, split_fields};
use std::{arch::asm, sync::OnceLock};

#[derive(Clone, Copy)]
pub(crate) struct Scanner {
    compress: bool,
}

// Opt in on hardware with native wide vectors; evaluation on an emulated
// processor cannot establish a performance benefit.
pub(crate) fn scanner() -> Option<Scanner> {
    static VERSION: OnceLock<Option<Scanner>> = OnceLock::new();
    *VERSION.get_or_init(|| {
        let mode = std::env::var_os("SHOPPING_SCANNER")?;
        if (mode != "wide" && mode != "bw")
            || !is_x86_feature_detected!("avx512f")
            || !is_x86_feature_detected!("avx512bw")
        {
            return None;
        }
        Some(Scanner {
            compress: mode == "wide"
                && is_x86_feature_detected!("avx512vbmi2")
                && is_x86_feature_detected!("avx512vl"),
        })
    })
}

impl Scanner {
    #[inline(always)]
    pub(super) fn short<const N: usize>(self, bytes: &[u8]) -> Option<(usize, Option<[&[u8]; N]>)> {
        let bytes = bytes.get(..64)?;
        let (lf, comma, quote) = unsafe { masks(bytes.as_ptr()) };
        split_fields::short_masks(bytes, lf, comma, quote)
    }

    pub(crate) fn visit<'a>(
        self,
        bytes: &'a [u8],
        emit: &mut impl FnMut([&'a [u8]; 7]) -> Option<()>,
    ) -> Option<()> {
        // scanner() checked both the CPU features and their OS availability.
        if self.compress {
            unsafe { scan::<true>(bytes, emit) }
        } else {
            unsafe { scan::<false>(bytes, emit) }
        }
    }
}

// Only scanner() can construct this mode for a caller. These functions
// keep the few wide operations in inline assembly so they also build on
// the project's minimum Rust version. The rest of the row loop compiles
// with the same target features as the consumer.
#[inline(always)]
unsafe fn masks(suffix: *const u8) -> (u64, u64, u64) {
    let (newlines, commas, quotes);
    unsafe {
        asm!(
            "vmovdqu8 zmm16, [{src}]",
            "vpbroadcastb zmm17, {lf:e}",
            "vpcmpeqb k1, zmm16, zmm17",
            "kmovq {newlines}, k1",
            "vpbroadcastb zmm17, {comma:e}",
            "vpcmpeqb k1, zmm16, zmm17",
            "kmovq {commas}, k1",
            "vpbroadcastb zmm17, {quote:e}",
            "vpcmpeqb k1, zmm16, zmm17",
            "kmovq {quotes}, k1",
            src=in(reg) suffix, lf=in(reg)10u32, comma=in(reg)44u32, quote=in(reg)34u32,
            newlines=out(reg) newlines, commas=out(reg) commas, quotes=out(reg) quotes,
            out("zmm16") _, out("zmm17") _, out("k1") _, options(pure,readonly,nostack,preserves_flags)
        );
    }
    (newlines, commas, quotes)
}

#[inline(always)]
unsafe fn ranks(mask: u64) -> [usize; 4] {
    static POSITIONS: [u8; 64] = {
        let mut a = [0; 64];
        let mut i = 0;
        while i < 64 {
            a[i] = i as u8 + 32;
            i += 1;
        }
        a
    };
    let out: u32;
    unsafe {
        asm!(
            "vmovdqu8 zmm16,[{positions}]",
            "kmovq k1,{mask}",
            "vpcompressb zmm16{{k1}}{{z}},zmm16",
            "vmovd {out:e},xmm16",
            positions=in(reg) POSITIONS.as_ptr(),mask=in(reg)mask, out=lateout(reg)out,
            out("zmm16") _,out("k1") _,options(pure,readonly,nostack,preserves_flags)
        );
    }
    out.to_le_bytes().map(usize::from)
}

#[inline(never)]
unsafe fn scan<'a, const COMPRESS: bool>(
    mut bytes: &'a [u8],
    emit: &mut impl FnMut([&'a [u8]; 7]) -> Option<()>,
) -> Option<()> {
    while bytes.len() >= 96 {
        let (first, (lf, comma, quotes)) = unsafe {
            let p = bytes.as_ptr();
            (split_fields::masks(p), masks(p.add(32)))
        };
        let row = split_fields::event_masks(bytes, first, lf, comma, quotes, |mask, inside| {
            if COMPRESS {
                unsafe { ranks(mask) }
            } else {
                split_fields::suffix_ranks(mask, inside)
            }
        });
        let (end, row) = row.unwrap_or_else(|| {
            let end = memchr::memchr(b'\n', bytes).unwrap_or(bytes.len());
            (end, fields(&bytes[..end]))
        });
        if let Some(row) = row {
            emit(row)?;
        }
        if end == bytes.len() {
            return Some(());
        }
        bytes = &bytes[end + 1..];
    }
    for row in RowWindows::new(bytes) {
        emit(row)?;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wide_matches_scalar_for_malformed_fields_and_chunk_tails() {
        let normal = [
            b"eventidab".as_slice(),
            b"userident",
            b"2028-03-01T05:01:03Z",
            b"2028-03-01T05:03:14Z",
            b"view",
            b"item123",
            b"",
        ];
        let mut corpus = Vec::new();
        for field in 0..7 {
            for len in [
                0, 1, 2, 8, 9, 10, 23, 30, 31, 32, 63, 64, 65, 95, 96, 97, 129,
            ] {
                let value = vec![b'x'; len];
                let last = len.saturating_sub(1);
                for (i, byte) in [
                    (0, b'"'),
                    (last, b'"'),
                    (len / 2, b'"'),
                    (len / 2, b','),
                    (len / 2, b'\n'),
                    (len / 2, b'\r'),
                ] {
                    let mut value = value.clone();
                    if let Some(v) = value.get_mut(i) {
                        *v = byte;
                    }
                    let mut row = normal;
                    row[field] = &value;
                    corpus.extend_from_slice(&row.join(&b','));
                    corpus.push(b'\n');
                }
            }
        }
        corpus.extend_from_slice(&normal.join(&b','));
        let expected = corpus
            .split(|&b| b == b'\n')
            .filter_map(fields::<7>)
            .collect::<Vec<_>>();
        for compress in [false, true] {
            if !is_x86_feature_detected!("avx512bw")
                || !is_x86_feature_detected!("avx512f")
                || (compress
                    && !(is_x86_feature_detected!("avx512vbmi2")
                        && is_x86_feature_detected!("avx512vl")))
            {
                continue;
            }
            let mut actual = Vec::new();
            (Scanner { compress })
                .visit(&corpus, &mut |row| {
                    actual.push(row);
                    Some(())
                })
                .unwrap();
            assert_eq!(actual, expected);
        }
    }
}
