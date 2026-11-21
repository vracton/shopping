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
            let index = date_slot(prefix, day);
            result.atoms[index] = DateAtom {
                prefix,
                day,
                epoch_day: epoch_day as i32,
            };
        }
        Some(result)
    }

    #[inline(always)]
    fn midnight(&self, timestamp: &[u8]) -> Option<i64> {
        let prefix = u64::from_le_bytes(timestamp[..8].try_into().unwrap());
        let day = u16::from_le_bytes(timestamp[8..10].try_into().unwrap());
        let atom = self.atoms[date_slot(prefix, day)];
        (atom.prefix != 0 && atom.prefix == prefix && atom.day == day)
            .then_some(i64::from(atom.epoch_day) * DAY_MS)
    }

    /// Event and ingest clocks occupy the two halves of one SIMD register.
    /// The ingest date stays lexically ordered, including outside the window.
    #[inline(always)]
    pub fn event_times(
        &self,
        ts: &[u8],
        ingest: &[u8],
        calendar: &Calendar,
    ) -> Option<(u64, Option<i64>)> {
        let ingest: &[u8; 20] = ingest.try_into().ok()?;
        let date = ingest_date(ingest);
        #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
        if matches!(ts.len(), 20 | 25) && ts[4] == b'-' {
            let (event_clock, ingest_clock) = paired_clocks(
                ts[11..19].try_into().unwrap(),
                ingest[11..19].try_into().unwrap(),
            );
            let value = if let Some(midnight) = self.midnight(ts) {
                event_clock.and_then(|seconds| {
                    Some(midnight + (i64::from(seconds) - utc_offset_seconds(ts)?) * 1000)
                })
            } else {
                calendar.timestamp(ts)
            };
            return Some((date | u64::from(ingest_clock), value));
        }
        let clock = u64::from_be_bytes(ingest[11..19].try_into().unwrap());
        Some((
            date | extract(clock, TIME_NIBBLES),
            self.timestamp(ts, calendar),
        ))
    }

    #[cfg(test)]
    #[inline(always)]
    fn ingest(bytes: &[u8]) -> Option<u64> {
        let bytes: &[u8; 20] = bytes.try_into().ok()?;
        // Ingest is guaranteed to be valid F1. Decimal nibbles preserve its
        // lexical order, so dedup never needs to parse it as epoch time.
        let clock = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
        Some(ingest_date(bytes) | extract(clock, TIME_NIBBLES))
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
                let Some(midnight) = self.midnight(bytes) else {
                    return calendar.timestamp(bytes);
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
                Some(midnight + ((hour * 3600 + minute * 60 + second) * 1000) as i64)
            }
            _ => calendar.timestamp(bytes),
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
fn utc_offset_seconds(ts: &[u8]) -> Option<i64> {
    if ts[10] != b'T' {
        return None;
    }
    match (ts.len(), ts[19]) {
        (20, b'Z') => Some(0),
        (25, sign @ (b'+' | b'-')) if ts[22] == b':' => {
            let hours = two_digits(ts[20], ts[21])?;
            let minutes = two_digits(ts[23], ts[24])?;
            if hours >= 24 || minutes >= 60 {
                return None;
            }
            let seconds = hours * 3600 + minutes * 60;
            Some(if sign == b'+' { seconds } else { -seconds })
        }
        _ => None,
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
fn two_digits(a: u8, b: u8) -> Option<i64> {
    let a = a.wrapping_sub(b'0');
    let b = b.wrapping_sub(b'0');
    (a < 10 && b < 10).then_some(i64::from(a) * 10 + i64::from(b))
}

#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
fn paired_clocks(event: &[u8; 8], ingest: &[u8; 8]) -> (Option<u32>, u32) {
    use std::arch::x86_64::*;
    unsafe {
        let both = _mm_unpacklo_epi64(
            _mm_loadl_epi64(event.as_ptr().cast()),
            _mm_loadl_epi64(ingest.as_ptr().cast()),
        );
        let digits = _mm_sub_epi8(both, _mm_set1_epi64x(i64::from_le_bytes(*b"00:00:00")));
        let ordered = _mm_shuffle_epi8(
            digits,
            _mm_setr_epi8(6, 7, 3, 4, 0, 1, -1, -1, 14, 15, 11, 12, 8, 9, -1, -1),
        );
        // Event pairs are binary, ingest pairs are BCD for the dedup order.
        let pairs = _mm_maddubs_epi16(
            ordered,
            _mm_set_epi64x(0x0110011001100110, 0x010a010a010a010a),
        );
        let compact = _mm_packus_epi16(pairs, pairs);
        let code = _mm_cvtsi128_si32(_mm_srli_si128(compact, 4)) as u32;
        let invalid = _mm_subs_epu8(
            digits,
            _mm_set1_epi64x(i64::from_le_bytes([9, 9, 0, 9, 9, 0, 9, 9])),
        );
        let range = _mm_subs_epu8(compact, _mm_set_epi32(-1, -1, -1, 0x0017_3b3b));
        let bad = _mm_cvtsi128_si64(invalid) | i64::from(_mm_cvtsi128_si32(range));
        let seconds = _mm_madd_epi16(pairs, _mm_setr_epi16(1, 60, 3600, 0, 0, 0, 0, 0));
        let total = _mm_cvtsi128_si32(_mm_add_epi32(seconds, _mm_srli_si128(seconds, 4))) as u32;
        ((bad == 0).then_some(total), code)
    }
}

#[inline(always)]
fn ingest_date(bytes: &[u8; 20]) -> u64 {
    let front = u64::from_be_bytes(bytes[..8].try_into().unwrap());
    let day = (u64::from(bytes[8] & 15) << 4) | u64::from(bytes[9] & 15);
    (extract(front, DAY_NIBBLES) << 32) | (day << 24)
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
    // Select the month parity and decimal day nibbles. Collisions with
    // other months are harmless because the full date is checked.
    (((prefix >> 48) as usize & 1) << 6) | ((day as usize & 3) << 4) | ((day as usize >> 8) & 15)
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
                    let ingest = b"2011-12-31T01:12:39Z";
                    assert_eq!(
                        fast.event_times(&bytes, ingest, &calendar),
                        Some((
                            FastTime::ingest(ingest).unwrap(),
                            calendar.timestamp(&bytes)
                        )),
                        "{bytes:?}"
                    );
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
    fn paired_clock_preserves_ingest_order_throughout_day() {
        let when = timestamp(b"2028-02-29T00:00:00Z").unwrap();
        let calendar = Calendar::new(when);
        let fast = FastTime::new(&calendar, when - WEEK, when, when + DAY_MS).unwrap();
        const WEEK: i64 = 7 * DAY_MS;
        for seconds in 0..86400 {
            let stamp = format!(
                "2028-02-29T{:02}:{:02}:{:02}Z",
                seconds / 3600,
                (seconds / 60) % 60,
                seconds % 60
            );
            let b = stamp.as_bytes();
            assert_eq!(
                fast.event_times(b, b, &calendar),
                Some((FastTime::ingest(b).unwrap(), Some(when + seconds * 1000)))
            );
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
