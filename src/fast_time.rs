//! Keep trusted ingest timestamps in sortable decimal form and recognize
//! in-window UTC dates by their exact bytes.
use crate::input::{Calendar, DAY_MS};

const DIGITS: u64 = 0x0f0f_0f0f_0f0f_0f0f;
const TIME_NIBBLES: u64 = 0x0f0f_000f_0f00_0f0f;
const DAY_NIBBLES: u64 = 0x0f0f_0f0f_000f_0f00;

#[cfg(all(
    target_arch = "x86_64",
    target_feature = "ssse3",
    target_feature = "sse4.1"
))]
#[derive(Clone, Copy, Default)]
struct Month {
    prefix: u64,
    first_day: i32,
    length: i16,
}

#[derive(Clone, Copy, Default)]
struct DateAtom {
    prefix: u64,
    day: u16,
    epoch_day: i32,
}

pub struct FastTime {
    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "ssse3",
        target_feature = "sse4.1"
    ))]
    months: [Month; 16],
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
            #[cfg(all(
                target_arch = "x86_64",
                target_feature = "ssse3",
                target_feature = "sse4.1"
            ))]
            months: [Month::default(); 16],
            cutoffs,
        };
        for (year, month, day, epoch_day) in calendar.nearby_dates() {
            #[cfg(all(
                target_arch = "x86_64",
                target_feature = "ssse3",
                target_feature = "sse4.1"
            ))]
            {
                let first_day = epoch_day - (day as i64 - 1);
                if first_day <= end.div_euclid(DAY_MS) + 1
                    && first_day + 31 >= base.div_euclid(DAY_MS) - 1
                {
                    let date = format!("{year:04}-{month:02}-");
                    if let Ok(prefix) = date.as_bytes().try_into() {
                        let prefix = u64::from_le_bytes(prefix);
                        result.months[(prefix >> 48) as usize & 15] = Month {
                            prefix,
                            first_day: first_day as i32,
                            length: day as i16,
                        };
                    }
                }
            }
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
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "ssse3",
            target_feature = "sse4.1"
        ))]
        if matches!(bytes.len(), 20 | 25) {
            let prefix = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let month = self.months[(prefix >> 48) as usize & 15];
            // The last month digit only selects a slot. Compare YYYY-MM-
            // before interpreting the rest of the timestamp relative to it.
            if month.length != 0 && month.prefix == prefix {
                let offset = match (bytes.len(), bytes[19]) {
                    (20, b'Z') => 0,
                    (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
                        let pair = |i: usize| {
                            let a = bytes[i].wrapping_sub(b'0');
                            let b = bytes[i + 1].wrapping_sub(b'0');
                            (a < 10 && b < 10).then_some(i64::from(a) * 10 + i64::from(b))
                        };
                        let (h, m) = (pair(20)?, pair(23)?);
                        if h >= 24 || m >= 60 {
                            return None;
                        }
                        let seconds = (h * 60 + m) * 60;
                        if sign == b'+' { seconds } else { -seconds }
                    }
                    _ => return None,
                };
                // Both accepted ISO spellings contain bytes 3 through 18.
                let second = unsafe { month_seconds(bytes, month.length) }?;
                return Some(
                    i64::from(month.first_day) * DAY_MS + (i64::from(second) - offset) * 1000,
                );
            }
        }
        match bytes.len() {
            13 => {
                let head = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let tail = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
                Some(
                    (decimal_eight(head)? * 100_000
                        + decimal_eight((tail & !0xff_ffff) | 0x30_3030)?)
                        as i64,
                )
            }
            20 if bytes[10] == b'T' && bytes[19] == b'Z' => {
                let prefix = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let day = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
                let mut index = date_slot(prefix, day);
                let epoch_day = loop {
                    let atom = self.atoms[index];
                    // An empty slot has an all-zero key, which malformed
                    // input can supply literally. Test occupancy first.
                    if atom.prefix == 0 {
                        return calendar.timestamp(bytes);
                    }
                    if atom.prefix == prefix && atom.day == day {
                        break i64::from(atom.epoch_day);
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
                let (hour, minute, second) = clock_parts(code);
                if hour >= 24 || minute >= 60 || second >= 60 {
                    return None;
                }
                Some(epoch_day * DAY_MS + ((hour * 3600 + minute * 60 + second) * 1000) as i64)
            }
            _ => calendar.timestamp(bytes),
        }
    }
}

#[cfg(all(
    target_arch = "x86_64",
    target_feature = "ssse3",
    target_feature = "sse4.1"
))]
#[inline(always)]
unsafe fn month_seconds(bytes: &[u8], days: i16) -> Option<i32> {
    use std::arch::x86_64::*;
    unsafe {
        // The low five lanes have already been checked against the month.
        // The other eleven must match DDThh:mm:ss, including punctuation.
        let value = _mm_loadu_si128(bytes.as_ptr().add(3).cast());
        let zero = _mm_setr_epi8(0, 0, 0, 0, 0, 48, 48, 84, 48, 48, 58, 48, 48, 58, 48, 48);
        let digits = _mm_sub_epi8(value, zero);
        let limit = _mm_setr_epi8(-1, -1, -1, -1, -1, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9);
        let bad = _mm_subs_epu8(digits, limit);
        let pairs = _mm_shuffle_epi8(
            digits,
            _mm_setr_epi8(5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1),
        );
        let values = _mm_maddubs_epi16(pairs, _mm_set1_epi16(0x010a));
        // Zero-base the day. Day 00 wraps above the unsigned upper bound.
        let values = _mm_sub_epi16(values, _mm_setr_epi16(1, 0, 0, 0, 0, 0, 0, 0));
        let range = _mm_subs_epu16(values, _mm_setr_epi16(days - 1, 23, 59, 59, 0, 0, 0, 0));
        let errors = _mm_or_si128(bad, range);
        if _mm_testz_si128(errors, errors) == 0 {
            return None;
        }
        // First pair yields minutes since month start, second pair seconds in hour.
        let totals = _mm_madd_epi16(values, _mm_setr_epi16(1440, 60, 60, 1, 0, 0, 0, 0));
        Some(_mm_cvtsi128_si32(totals) * 60 + _mm_extract_epi32::<1>(totals))
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
    ((prefix >> 40).wrapping_mul(241) ^ u64::from(day).wrapping_mul(109) ^ (u64::from(day) >> 8))
        as usize
        & 127
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
    fn empty_date_atom_cannot_accept_a_timestamp_at_epoch() {
        let base = -7 * DAY_MS;
        let calendar = Calendar::new(0);
        let fast = FastTime::new(&calendar, base, 0, DAY_MS).unwrap();
        let mut bytes = *b"1970-01-01T03:04:05Z";
        bytes[..10].fill(0);
        assert_eq!(fast.timestamp(&bytes, &calendar), None);
    }

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
