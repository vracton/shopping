//! Keep trusted ingest timestamps in sortable decimal form and recognize
//! in-window UTC dates by their exact bytes.
use crate::input::{Calendar, DAY_MS};

const DIGITS: u64 = 0x0f0f_0f0f_0f0f_0f0f;
const TIME_NIBBLES: u64 = 0x0f0f_000f_0f00_0f0f;
const DAY_NIBBLES: u64 = 0x0f0f_0f0f_000f_0f00;

#[derive(Clone, Copy, Default)]
struct DateAtom {
    prefix: u64,
    day: u16,
    epoch_day: i32,
}

pub struct FastTime {
    atoms: [DateAtom; 128],
    cutoffs: Vec<u64>,
}

impl FastTime {
    pub fn new(calendar: &Calendar, base: i64, start: i64, end: i64) -> Option<Self> {
        let cutoff = calendar.cutoff_days(base, end, start)?;
        let cutoffs = cutoff
            .into_iter()
            .map(|day| {
                let date = day >> 17;
                let (year, month, day) = (date >> 9, (date >> 5) & 15, date & 31);
                (bcd(year, 4) << 16) | (bcd(month, 2) << 8) | bcd(day, 2)
            })
            .collect();
        let mut result = Self {
            atoms: [DateAtom::default(); 128],
            cutoffs,
        };
        for (year, month, day, epoch_day) in calendar.nearby_dates() {
            if epoch_day < base.div_euclid(DAY_MS) - 1 || epoch_day > end.div_euclid(DAY_MS) + 1 {
                continue;
            }
            let date = format!("{year:04}-{month:02}-{day:02}");
            if date.len() != 10 {
                continue;
            }
            let prefix = u64::from_le_bytes(date.as_bytes()[..8].try_into().unwrap());
            let day = u16::from_le_bytes(date.as_bytes()[8..10].try_into().unwrap());
            let mut index = date_slot(prefix, day);
            while result.atoms[index].prefix != 0 {
                index = (index + 1) & 127;
            }
            result.atoms[index] = DateAtom {
                prefix,
                day,
                epoch_day: epoch_day as i32,
            };
        }
        Some(result)
    }

    #[inline(always)]
    pub fn ingest(bytes: &[u8]) -> Option<u64> {
        let bytes: &[u8; 20] = bytes.try_into().ok()?;
        // Ingest is guaranteed to be valid F1. Decimal nibbles preserve its
        // lexical order, so dedup never needs to parse it as epoch time.
        let front = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let day = (u64::from(bytes[8] & 15) << 4) | u64::from(bytes[9] & 15);
        let clock = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
        Some((extract(front, DAY_NIBBLES) << 32) | (day << 24) | extract(clock, TIME_NIBBLES))
    }

    #[inline(always)]
    pub fn is_late(&self, code: u64, relative: u32) -> bool {
        let cutoff_date = self.cutoffs[(relative / DAY_MS as u32 + 1) as usize];
        let date = code >> 24;
        date > cutoff_date
            || (date == cutoff_date
                && clock_seconds(code) > u64::from((relative % DAY_MS as u32) / 1000))
    }

    #[inline(always)]
    pub fn timestamp(&self, bytes: &[u8], calendar: &Calendar) -> Option<i64> {
        match bytes.len() {
            13 => decimal_thirteen(bytes.try_into().unwrap()),
            20 | 25 if bytes[10] == b'T' => {
                let prefix = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let day = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
                let mut index = date_slot(prefix, day);
                let epoch_day = loop {
                    let atom = self.atoms[index];
                    if atom.prefix == prefix && atom.day == day {
                        break i64::from(atom.epoch_day);
                    }
                    if atom.prefix == 0 {
                        return calendar.timestamp(bytes);
                    }
                    index = (index + 1) & 127;
                };
                let clock = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
                const DIGIT_BYTES: u64 = 0xffff_00ff_ff00_ffff;
                if (clock & !DIGIT_BYTES) != 0x0000_3a00_003a_0000
                    || !ascii_digits(clock, DIGIT_BYTES)
                {
                    return None;
                }
                let code = extract(clock, TIME_NIBBLES);
                // Convert all three BCD pairs to binary in parallel.
                let pairs = code - 6 * ((code >> 4) & 0x0f0f0f);
                if (pairs + 0x684444) & 0x808080 != 0 {
                    return None;
                }
                let seconds =
                    ((pairs >> 16) * 3600 + ((pairs >> 8) & 255) * 60 + (pairs & 255)) as i64;
                let offset = match (bytes.len(), bytes[19]) {
                    (20, b'Z') => 0,
                    (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
                        let pair = |a: u8, b: u8| {
                            let a = a.wrapping_sub(b'0');
                            let b = b.wrapping_sub(b'0');
                            (a < 10 && b < 10).then_some(i64::from(a) * 10 + i64::from(b))
                        };
                        let hours = pair(bytes[20], bytes[21])?;
                        let minutes = pair(bytes[23], bytes[24])?;
                        if hours >= 24 || minutes >= 60 {
                            return None;
                        }
                        let offset = hours * 3600 + minutes * 60;
                        if sign == b'+' { offset } else { -offset }
                    }
                    _ => return None,
                };
                Some(epoch_day * DAY_MS + (seconds - offset) * 1000)
            }
            _ => calendar.timestamp(bytes),
        }
    }
}

fn bcd(mut n: u64, digits: u32) -> u64 {
    let mut result = 0;
    for i in 0..digits {
        result |= (n % 10) << (4 * i);
        n /= 10;
    }
    result
}

#[inline(always)]
fn date_slot(prefix: u64, day: u16) -> usize {
    // Separate adjacent months and advance one slot per day. The exact atom
    // comparison and linear probe also handle year/month wrapping.
    (((prefix >> 48) * 31 + u64::from(day) * 10 + u64::from(day >> 8)) & 127) as usize
}

#[inline(always)]
fn extract(word: u64, mask: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // The release target guarantees BMI2.
        unsafe { std::arch::x86_64::_pext_u64(word, mask) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut mask = mask;
        let mut result = 0;
        let mut offset = 0;
        while mask != 0 {
            let bit = mask & mask.wrapping_neg();
            result |= u64::from(word & bit != 0) << offset;
            offset += 1;
            mask &= mask - 1;
        }
        result
    }
}

#[inline(always)]
fn clock_parts(code: u64) -> (u64, u64, u64) {
    let pair = |shift: u32| ((code >> (shift + 4)) & 15_u64) * 10 + ((code >> shift) & 15_u64);
    (pair(16), pair(8), pair(0))
}

#[inline(always)]
fn clock_seconds(code: u64) -> u64 {
    let (hour, minute, second) = clock_parts(code);
    (hour * 60 + minute) * 60 + second
}

#[inline(always)]
fn ascii_digits(word: u64, lanes: u64) -> bool {
    let high = (word ^ 0x3030_3030_3030_3030) & 0xf0f0_f0f0_f0f0_f0f0;
    let overflow = ((word & DIGITS) + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010;
    (high | overflow) & lanes == 0
}

#[inline(always)]
fn decimal_thirteen(bytes: &[u8; 13]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
    unsafe {
        use std::arch::x86_64::*;
        // Two overlapping eight-byte loads stay inside the field. Arrange
        // the digits as sixteen lanes padded on the left with three zeros.
        let first = _mm_loadl_epi64(bytes.as_ptr().cast());
        let last = _mm_loadl_epi64(bytes.as_ptr().add(5).cast());
        let parts = _mm_unpacklo_epi64(first, last);
        let digits = _mm_shuffle_epi8(
            _mm_sub_epi8(parts, _mm_set1_epi8(b'0' as i8)),
            _mm_setr_epi8(-1, -1, -1, 0, 1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14, 15),
        );
        let in_range = _mm_cmpeq_epi8(_mm_min_epu8(digits, _mm_set1_epi8(9)), digits);
        if _mm_movemask_epi8(in_range) != 0xffff {
            return None;
        }
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0064));
        let eights = _mm_madd_epi16(_mm_packs_epi32(quads, quads), _mm_set1_epi32(0x0001_2710));
        let both = _mm_cvtsi128_si64(eights) as u64;
        Some(i64::from(both as u32) * 100_000_000 + (both >> 32) as i64)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
    {
        let head = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let tail = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
        Some(
            (decimal_eight(head)? * 100_000 + decimal_eight((tail & !0xff_ffff) | 0x30_3030)?)
                as i64,
        )
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
#[inline(always)]
fn decimal_eight(word: u64) -> Option<u64> {
    if !ascii_digits(word, u64::MAX) {
        return None;
    }
    let digits = word & DIGITS;
    let pairs = (digits.wrapping_mul(10) + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
    let groups = (pairs.wrapping_mul(100) + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
    Some((groups & 0xffff) * 10_000 + (groups >> 32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::timestamp;

    #[test]
    fn matches_calendar_for_all_single_byte_mutations() {
        let start = timestamp(b"2028-03-01T00:00:00Z").unwrap();
        let calendar = Calendar::new(start);
        let fast =
            FastTime::new(&calendar, start - 7 * DAY_MS, start, start + 33 * DAY_MS).unwrap();
        for reference in [
            b"2028-02-29T23:59:59Z".as_slice(),
            b"2028-04-01T00:09:13-23:59",
            b"0001234567890",
            b"00000000012345678901",
        ] {
            for at in 0..reference.len() {
                let mut bytes = reference.to_vec();
                for value in 0..=255 {
                    bytes[at] = value;
                    assert_eq!(
                        fast.timestamp(&bytes, &calendar),
                        calendar.timestamp(&bytes),
                        "{bytes:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn exact_atoms_and_decimal_lanes_match_the_calendar() {
        let start = timestamp(b"2028-02-26T00:00:00Z").unwrap();
        let base = start - 7 * DAY_MS;
        let calendar = Calendar::new(start);
        let fast = FastTime::new(&calendar, base, start, start + 14 * DAY_MS).unwrap();
        let cases: &[&[u8]] = &[
            b"2028-02-29T23:59:59Z",
            b"2028-02-30T00:00:00Z",
            b"2028-03-01T00:00:00Z",
            b"2028-03-01T24:00:00Z",
            b"2028-03-01T23:60:00Z",
            b"2028-03-01T23:59:60Z",
            b"2028-03-01T00:00:01+05:00",
            b"2028-03-01T0A:00:00Z",
            b"2028-03-01T00/00:00Z",
            b"1234567890123",
            b"0000123456789",
            b"12345678901:3",
            b"123-567890123",
        ];
        for &bytes in cases {
            assert_eq!(
                fast.timestamp(bytes, &calendar),
                calendar.timestamp(bytes),
                "{bytes:?}"
            );
        }

        let event = timestamp(b"2028-02-29T23:59:58Z").unwrap() + 999;
        let relative = (event - base) as u32;
        let cases: &[&[u8]] = &[
            b"2028-02-29T23:59:59Z",
            b"2028-03-01T23:59:57Z",
            b"2028-03-01T23:59:58Z",
            b"2028-03-01T23:59:59Z",
            b"2028-03-02T00:00:00Z",
        ];
        let mut previous = 0;
        for &bytes in cases {
            let code = FastTime::ingest(bytes).unwrap();
            assert!(code > previous);
            assert_eq!(
                fast.is_late(code, relative),
                timestamp(bytes).unwrap() - event > DAY_MS
            );
            previous = code;
        }
    }
}
