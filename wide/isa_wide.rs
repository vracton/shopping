// The claims/event and click visitors dispatch once per chunk. Only mask
// extraction and, with VBMI2, comma ranking differ from the narrow scanner.
use super::super::{RowWindows, visit_narrow};
use super::{Masks, event_masks, short_masks, suffix_ranks};
use std::{arch::x86_64::*, sync::OnceLock};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Avx2,
    Bw,
    Vbmi2,
}

fn mode() -> Mode {
    static MODE: OnceLock<Mode> = OnceLock::new();
    *MODE.get_or_init(|| {
        let setting = std::env::var_os("SHOPPING_SCANNER");
        if setting.as_deref() == Some(std::ffi::OsStr::new("avx2"))
            || !is_x86_feature_detected!("avx512f")
            || !is_x86_feature_detected!("avx512bw")
        {
            Mode::Avx2
        } else if setting.as_deref() != Some(std::ffi::OsStr::new("bw"))
            && is_x86_feature_detected!("avx512vbmi2")
        {
            Mode::Vbmi2
        } else {
            Mode::Bw
        }
    })
}

#[inline(always)]
pub(crate) fn visit_windows<'a, const N: usize>(
    bytes: &'a [u8],
    emit: &mut impl FnMut([&'a [u8]; N]) -> Option<()>,
) -> Option<()> {
    // Runtime detection checks CPU and OS support before calling a visitor.
    match mode() {
        Mode::Avx2 => visit_narrow(bytes, emit),
        Mode::Vbmi2 if N == 7 => unsafe { visit_vbmi2(bytes, emit) },
        _ => unsafe { visit_bw(bytes, emit) },
    }
}

// Keep the byte vectors in a single cache-aligned table. The visitor takes an
// opaque reference once per chunk so LLVM can fold constants into comparison
// memory operands even across callbacks which clobber all vector registers.
#[repr(align(64))]
struct Constants {
    newline: [u8; 64],
    comma: [u8; 64],
    quote: [u8; 64],
    positions: [u8; 64],
}
static CONSTANTS: Constants = Constants {
    newline: [b'\n'; 64],
    comma: [b','; 64],
    quote: [b'"'; 64],
    positions: {
        let mut bytes = [0; 64];
        let mut i = 0;
        while i < 64 {
            bytes[i] = (32 + i) as u8;
            i += 1;
        }
        bytes
    },
};

#[inline]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn wide_masks(p: *const u8, c: &Constants) -> (u64, u64, u64) {
    // The scan loop proves the entire 64-byte window is readable.
    unsafe {
        let v = _mm512_loadu_si512(p.cast());
        let eq =
            |bytes: &[u8; 64]| _mm512_cmpeq_epi8_mask(v, _mm512_loadu_si512(bytes.as_ptr().cast()));
        (eq(&c.newline), eq(&c.comma), eq(&c.quote))
    }
}

#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn visit_bw<'a, const N: usize>(
    bytes: &'a [u8],
    emit: &mut impl FnMut([&'a [u8]; N]) -> Option<()>,
) -> Option<()> {
    let constants = std::hint::black_box(&CONSTANTS);
    unsafe { scan(bytes, |p| wide_masks(p, constants), suffix_ranks, emit) }
}

#[target_feature(enable = "avx512f,avx512bw,avx512vbmi2")]
unsafe fn visit_vbmi2<'a, const N: usize>(
    bytes: &'a [u8],
    emit: &mut impl FnMut([&'a [u8]; N]) -> Option<()>,
) -> Option<()> {
    let constants = std::hint::black_box(&CONSTANTS);
    let positions = unsafe { _mm512_loadu_si512(constants.positions.as_ptr().cast()) };
    unsafe {
        scan(
            bytes,
            |p| wide_masks(p, constants),
            |mask, _| {
                let packed = _mm512_maskz_compress_epi8(mask, positions);
                let cuts = _mm_cvtsi128_si32(_mm512_castsi512_si128(packed));
                cuts.to_le_bytes().map(usize::from)
            },
            emit,
        )
    }
}

// The callbacks must return the exact masks of the 64 readable bytes at p
// and the exact first four suffix separator positions, respectively.
#[inline(always)]
unsafe fn scan<'a, const N: usize>(
    mut bytes: &'a [u8],
    wide: impl Fn(*const u8) -> (u64, u64, u64),
    ranks: impl Fn(u64, u64) -> [usize; 4],
    emit: &mut impl FnMut([&'a [u8]; N]) -> Option<()>,
) -> Option<()> {
    while bytes.len() >= if N == 7 { 96 } else { 64 } {
        let result = if N == 7 {
            // The first 32 bytes and the following 64 bytes fit in the window.
            unsafe {
                let (lf, comma, quote) = wide(bytes.as_ptr());
                let first = Masks {
                    newlines: lf as u32,
                    commas: comma as u32,
                    quotes: quote as u32,
                };
                let (lf, comma, quote) = wide(bytes.as_ptr().add(32));
                event_masks(bytes, first, lf, comma, quote, &ranks)
            }
        } else {
            let (lf, comma, quote) = wide(bytes.as_ptr());
            unsafe { short_masks(bytes, lf, comma, quote) }
        };
        if let Some((end, row)) = result {
            bytes = &bytes[end + 1..];
            if let Some(row) = row {
                emit(row)?;
            }
        } else {
            // Preserve the general scanner for other layouts and long rows.
            let Some(row) = next_general(&mut bytes) else {
                return Some(());
            };
            emit(row)?;
        }
    }
    visit_narrow(bytes, emit)
}

#[cold]
#[inline(never)]
fn next_general<'a, const N: usize>(bytes: &mut &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut fallback = RowWindows::<N>::new(bytes);
    let row = fallback.next();
    *bytes = fallback.remaining;
    row
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{fields, rows};

    fn compare<const N: usize>(base: [&[u8]; N]) {
        let mut bytes = Vec::new();
        for col in 0..N {
            for n in [
                0, 1, 2, 9, 30, 31, 32, 33, 62, 63, 64, 65, 94, 95, 96, 97, 127, 128, 129,
            ] {
                let mut field = vec![b'x'; n];
                for marker in [
                    b"".as_slice(),
                    b"\"",
                    b"\"\"",
                    b"\nx",
                    b"\r",
                    b",",
                    b"\"x\"",
                    b"\xff",
                ] {
                    let at = n / 2;
                    field.splice(at..at, marker.iter().copied());
                    let mut row = base;
                    row[col] = &field;
                    bytes.extend_from_slice(&row.join(&b','));
                    bytes.push(b'\n');
                    field.drain(at..at + marker.len());
                }
            }
        }
        bytes.extend_from_slice(&base.join(&b','));
        let expected: Vec<_> = rows(&bytes).filter_map(fields::<N>).collect();
        let mut actual = Vec::new();
        visit_windows(&bytes, &mut |row| {
            actual.push(row);
            Some(())
        })
        .unwrap();
        assert_eq!(actual, expected);
        let mut calls = 0;
        assert_eq!(
            visit_windows::<N>(&bytes, &mut |_| {
                calls += 1;
                None
            }),
            None
        );
        assert_eq!(calls, 1);
    }

    #[test]
    fn dispatched_masks_match_scalar_on_malformed_rows_and_tails() {
        compare([
            b"event1234",
            b"userident",
            b"2028-01-01T01:02:03Z",
            b"2028-01-02T01:02:03Z",
            b"view",
            b"product",
            b"",
        ]);
        compare([
            b"click123",
            b"userident",
            b"campaign",
            b"2028-01-01T01:02:03Z",
        ]);
    }
}
