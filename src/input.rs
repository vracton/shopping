use std::fs::{self, File};
use std::io;
use std::path::Path;

use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};

pub const DAY_MS: i64 = 86_400_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;

pub fn map_file(path: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)
        .map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", path.display())))?;
    if file.metadata()?.len() == 0 {
        return Ok(None);
    }
    // Input files stay immutable for the lifetime of the mapping, as on the scorer mount.
    unsafe { MmapOptions::new().map(&file).map(Some) }
}

pub enum Shard {
    Mapped(Mmap),
    Buffered(Vec<u8>),
}

impl AsRef<[u8]> for Shard {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Mapped(bytes) => bytes,
            Self::Buffered(bytes) => bytes,
        }
    }
}

pub fn read_shards(path: &Path, buffered: bool) -> io::Result<Vec<Shard>> {
    if !buffered {
        return Ok(map_shards(path)?.into_iter().map(Shard::Mapped).collect());
    }
    use rayon::prelude::*;
    let paths: Vec<_> = fs::read_dir(path)?
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|entry| {
            entry.file_name().as_encoded_bytes().ends_with(b".csv") && entry.path().is_file()
        })
        .map(|entry| entry.path())
        .collect();
    paths
        .par_iter()
        .map(|path| fs::read(path).map(Shard::Buffered))
        .collect()
}

pub fn map_shards(path: &Path) -> io::Result<Vec<Mmap>> {
    let mut maps = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name().as_encoded_bytes().ends_with(b".csv") && entry.path().is_file() {
            maps.extend(map_file(&entry.path())?);
        }
    }
    Ok(maps)
}

fn without_header(bytes: &[u8]) -> &[u8] {
    memchr(b'\n', bytes).map_or(&[], |end| &bytes[end + 1..])
}

pub fn data_rows(file: &[u8]) -> impl Iterator<Item = &[u8]> {
    rows(without_header(file))
}

pub fn rows(mut bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        if let Some(end) = memchr(b'\n', bytes) {
            let row = &bytes[..end];
            bytes = &bytes[end + 1..];
            Some(row)
        } else {
            Some(std::mem::take(&mut bytes))
        }
    })
}

fn byte_chunks(mut rest: &[u8], chunk_bytes: usize) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        if rest.len() > chunk_bytes {
            // Include the newline in the previous chunk when the target lands on a boundary.
            let target = chunk_bytes - 1;
            if let Some(newline) = memchr(b'\n', &rest[target..]) {
                let (chunk, tail) = rest.split_at(target + newline + 1);
                rest = tail;
                return Some(chunk);
            }
        }
        Some(std::mem::take(&mut rest))
    })
}

pub fn chunks<T: AsRef<[u8]>>(files: &[T], chunk_bytes: usize) -> Vec<&[u8]> {
    files
        .iter()
        .flat_map(|file| byte_chunks(without_header(file.as_ref()), chunk_bytes))
        .collect()
}

pub fn fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    if row.is_empty() {
        return None;
    }
    let mut values = [&[][..]; N];
    let mut start = 0;
    let boundaries = memchr_iter(b',', row).chain(std::iter::once(row.len()));
    let mut index = 0;
    for end in boundaries {
        let field = &row[start..end];
        let value = if field.first() == Some(&b'"') {
            if field.len() < 2 || field.last() != Some(&b'"') {
                return None;
            }
            &field[1..field.len() - 1]
        } else {
            field
        };
        if memchr(b'"', value).is_some() {
            return None;
        }
        *values.get_mut(index)? = value;
        index += 1;
        start = end + 1;
    }
    (index == N).then_some(values)
}

fn ascii_digit(byte: u8) -> Option<i64> {
    byte.is_ascii_digit()
        .then_some(i64::from(byte.wrapping_sub(b'0')))
}

pub fn decimal(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    bytes.iter().try_fold(0_i64, |number, &byte| {
        number.checked_mul(10)?.checked_add(ascii_digit(byte)?)
    })
}

fn pair(bytes: &[u8], start: usize) -> Option<i64> {
    Some(ascii_digit(bytes[start])? * 10 + ascii_digit(bytes[start + 1])?)
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if bytes.get(4) != Some(&b'-') {
        return decimal(bytes);
    }
    if !matches!(bytes.len(), 20 | 25)
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = pair(bytes, 0)? * 100 + pair(bytes, 2)?;
    let month = pair(bytes, 5)?;
    let day = pair(bytes, 8)?;
    let hour = pair(bytes, 11)?;
    let minute = pair(bytes, 14)?;
    let second = pair(bytes, 17)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = [
        31,
        28 + i64::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if year == 0
        || !(1..=12).contains(&month)
        || !(1..=days_in_month[month as usize - 1]).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let offset = match (bytes.len(), bytes[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
            let hours = pair(bytes, 20)?;
            let minutes = pair(bytes, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let offset = hours * 60 + minutes;
            if sign == b'+' { offset } else { -offset }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    Some(days * DAY_MS + ((hour * 60 + minute - offset) * 60 + second) * 1000)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Scans a complete newline-aligned chunk. A single delimiter mask covers the
/// three bytes that affect record structure in this CSV dialect.
pub struct ChunkRows<'a, const N: usize> {
    bytes: &'a [u8],
    block: usize,
    separators: u32,
    quotes: u32,
    next_block: usize,
    start: usize,
}

impl<'a, const N: usize> ChunkRows<'a, N> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            block: 0,
            separators: 0,
            quotes: 0,
            next_block: 0,
            start: 0,
        }
    }

    #[inline(always)]
    fn boundary(&mut self) -> (usize, u32) {
        let mut count = 0;
        while self.separators == 0 {
            count += self.quotes.count_ones();
            self.quotes = 0;
            if self.next_block == self.bytes.len() {
                return (self.next_block, count);
            }
            let remaining = &self.bytes[self.next_block..];
            self.block = self.next_block;
            self.next_block += remaining.len().min(32);
            (self.separators, self.quotes) = delimiter_masks(remaining);
        }
        let offset = self.separators.trailing_zeros();
        let preceding = (1 << offset) - 1;
        count += (self.quotes & preceding).count_ones();
        self.quotes &= !preceding;
        self.separators &= self.separators - 1;
        (self.block + offset as usize, count)
    }
}

impl<'a, const N: usize> Iterator for ChunkRows<'a, N> {
    type Item = [&'a [u8]; N];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        while self.start < self.bytes.len() {
            let mut values = [&[][..]; N];
            let mut count = 0;
            let mut invalid = false;
            let nonempty_row_start = self.start;
            loop {
                let (at, quotes) = self.boundary();
                if count < N && !invalid {
                    let field = &self.bytes[self.start..at];
                    if quotes == 0 {
                        values[count] = field;
                    } else if quotes == 2
                        && field.first() == Some(&b'"')
                        && field.last() == Some(&b'"')
                    {
                        values[count] = &field[1..field.len() - 1];
                    } else {
                        invalid = true;
                    }
                }
                self.start = at + 1;
                count += 1;
                if self.bytes.get(at) != Some(&b',') {
                    if !invalid && count == N && at != nonempty_row_start {
                        return Some(values);
                    }
                    break;
                }
            }
        }
        None
    }
}

#[inline]
fn delimiter_masks(bytes: &[u8]) -> (u32, u32) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if bytes.len() >= 32 {
        use std::arch::x86_64::*;
        // The full 32-byte vector is within this block.
        unsafe {
            let values = _mm256_loadu_si256(bytes.as_ptr().cast());
            let newline = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(b'\n' as i8));
            let comma = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(b',' as i8));
            let quote = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(b'"' as i8));
            return (
                _mm256_movemask_epi8(_mm256_or_si256(newline, comma)) as u32,
                _mm256_movemask_epi8(quote) as u32,
            );
        }
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    if bytes.len() >= 32 {
        use std::arch::x86_64::*;
        // Both 16-byte reads fit in this block, and SSE2 is baseline on x86-64.
        unsafe {
            let block = |offset| {
                let values = _mm_loadu_si128(bytes.as_ptr().add(offset).cast());
                let newline = _mm_cmpeq_epi8(values, _mm_set1_epi8(b'\n' as i8));
                let comma = _mm_cmpeq_epi8(values, _mm_set1_epi8(b',' as i8));
                let quote = _mm_cmpeq_epi8(values, _mm_set1_epi8(b'"' as i8));
                (
                    _mm_movemask_epi8(_mm_or_si128(newline, comma)) as u32,
                    _mm_movemask_epi8(quote) as u32,
                )
            };
            let (separators0, quotes0) = block(0);
            let (separators1, quotes1) = block(16);
            return (separators0 | (separators1 << 16), quotes0 | (quotes1 << 16));
        }
    }
    bytes
        .iter()
        .take(32)
        .enumerate()
        .fold((0, 0), |(separators, quotes), (index, byte)| {
            (
                separators | (u32::from(matches!(byte, b'\n' | b',')) << index),
                quotes | (u32::from(*byte == b'"') << index),
            )
        })
}

/// Calendar conversion for the dates close enough to be in a compact window.
pub struct Calendar {
    first_year: i64,
    days: Box<[i32]>,
}

impl Calendar {
    pub fn new(window_start: i64) -> Self {
        let epoch_day = window_start.div_euclid(DAY_MS);
        let (mut lower, mut upper) = (1, 10_000);
        while lower + 1 < upper {
            let mid = (lower + upper) / 2;
            if days_from_civil(mid, 1, 1) <= epoch_day {
                lower = mid
            } else {
                upper = mid
            }
        }
        let first_year = (lower - 1).max(1);
        let mut days = vec![i32::MIN; 3 * 12 * 32].into_boxed_slice();
        for year in first_year..first_year + 3 {
            for month in 1..=12 {
                let start = days_from_civil(year, month, 1);
                let next = if month == 12 {
                    days_from_civil(year + 1, 1, 1)
                } else {
                    days_from_civil(year, month + 1, 1)
                };
                let index = ((year - first_year) * 384 + (month - 1) * 32) as usize;
                for day in 1..=next - start {
                    days[index + day as usize] = (start + day - 1) as i32;
                }
            }
        }
        Self { first_year, days }
    }

    #[inline]
    fn lookup(&self, year: i64, month: i64, day: i64) -> Option<i64> {
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        let year = year - self.first_year;
        if !(0..3).contains(&year) {
            return None;
        }
        let result = self.days[(year * 384 + (month - 1) * 32 + day) as usize];
        (result != i32::MIN).then_some(i64::from(result))
    }

    #[inline]
    pub fn nearby_dates(&self) -> impl Iterator<Item = (i64, usize, usize, i64)> + '_ {
        self.days
            .iter()
            .enumerate()
            .filter_map(|(index, &epoch_day)| {
                (epoch_day != i32::MIN).then_some((
                    self.first_year + (index / 384) as i64,
                    index % 384 / 32 + 1,
                    index % 32,
                    i64::from(epoch_day),
                ))
            })
    }

    pub fn cutoff_days(&self, base: i64, end: i64, start: i64) -> Option<Vec<u64>> {
        let mut codes = vec![0; ((end - base) / DAY_MS + 2) as usize];
        let base_day = base.div_euclid(DAY_MS);
        for (index, &day) in self.days.iter().enumerate() {
            if day == i32::MIN {
                continue;
            }
            let index_in_window = i64::from(day) - base_day;
            if index_in_window < 0 {
                continue;
            }
            if let Some(code) = codes.get_mut(index_in_window as usize) {
                let year = self.first_year as u64 + (index / 384) as u64;
                let month = (index % 384 / 32 + 1) as u64;
                let day = (index % 32) as u64;
                *code = ((year << 9) | (month << 5) | day) << 17;
            }
        }
        let required = ((start - base) / DAY_MS) as usize + 1;
        let last = ((end - base - 1) / DAY_MS) as usize + 1;
        codes[required..=last]
            .iter()
            .all(|&code| code != 0)
            .then_some(codes)
    }

    #[inline]
    pub fn ingest_code(bytes: &[u8]) -> Option<u64> {
        // F1 is guaranteed UTC with whole-second precision.
        let value: &[u8; 20] = bytes.try_into().ok()?;
        let pair =
            |at: usize| (u64::from(value[at]) * 10 + u64::from(value[at + 1])).wrapping_sub(528);
        let (year, month, day) = (pair(0) * 100 + pair(2), pair(5), pair(8));
        let seconds = (pair(11) * 60 + pair(14)) * 60 + pair(17);
        Some((((year << 9) | (month << 5) | day) << 17) | seconds)
    }

    #[inline]
    pub fn timestamp(&self, bytes: &[u8]) -> Option<i64> {
        if let Ok(digits) = <&[u8; 13]>::try_from(bytes) {
            let mut invalid = 0;
            let mut number = 0_i64;
            for &byte in digits {
                let digit = byte.wrapping_sub(b'0');
                invalid |= digit | 9_u8.wrapping_sub(digit);
                number = number * 10 + i64::from(digit);
            }
            return (invalid & 128 == 0).then_some(number);
        }
        if !matches!(bytes.len(), 20 | 25) || bytes[4] != b'-' {
            return decimal(bytes);
        }
        if bytes[7] != b'-' || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
            return None;
        }
        let year = pair(bytes, 0)? * 100 + pair(bytes, 2)?;
        let days = self.lookup(year, pair(bytes, 5)?, pair(bytes, 8)?)?;
        let hour = pair(bytes, 11)?;
        let minute = pair(bytes, 14)?;
        let second = pair(bytes, 17)?;
        if hour >= 24 || minute >= 60 || second >= 60 {
            return None;
        }
        let offset = match (bytes.len(), bytes[19]) {
            (20, b'Z') => 0,
            (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
                let (hour, minute) = (pair(bytes, 20)?, pair(bytes, 23)?);
                if hour >= 24 || minute >= 60 {
                    return None;
                }
                let offset = hour * 60 + minute;
                if sign == b'+' { offset } else { -offset }
            }
            _ => return None,
        };
        Some(days * DAY_MS + ((hour * 60 + minute - offset) * 60 + second) * 1000)
    }
}

#[cfg(all(
    target_arch = "x86_64",
    target_feature = "avx2",
    target_feature = "bmi2"
))]
#[path = "split_fields.rs"]
mod split_fields;

/// A row-oriented scanner that extracts every field boundary from two masks.
/// Fixed-width loads are only used when the complete load is inside the shard.
pub struct RowWindows<'a, const N: usize> {
    remaining: &'a [u8],
}

impl<'a, const N: usize> RowWindows<'a, N> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
}

impl<'a, const N: usize> Iterator for RowWindows<'a, N> {
    type Item = [&'a [u8]; N];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        while !self.remaining.is_empty() {
            let bytes = self.remaining;
            #[cfg(all(
                target_arch = "x86_64",
                target_feature = "avx2",
                target_feature = "bmi2"
            ))]
            if let Some((end, fields)) = if N == 7 && bytes.len() >= 96 {
                split_fields::event(bytes)
            } else if N != 7 && bytes.len() >= 64 {
                split_fields::short(bytes)
            } else {
                None
            } {
                self.remaining = &bytes[end + 1..];
                if let Some(fields) = fields {
                    return Some(fields);
                }
                continue;
            }
            if bytes.len() >= 128 {
                let (newline, commas, quotes) = row_masks(bytes);
                if newline != 0 {
                    let end = newline.trailing_zeros() as usize;
                    self.remaining = &bytes[end + 1..];
                    let inside = (1_u128 << end) - 1;
                    let mut commas = commas & inside;
                    let quotes = quotes & inside;
                    if end == 0 || commas.count_ones() as usize != N - 1 {
                        continue;
                    }
                    let boundaries = commas | (1 << end);
                    let starts = (commas << 1) | 1;
                    let ends = boundaries >> 1;
                    if quotes & !(starts ^ ends) != 0 {
                        continue;
                    }
                    // Compare opening and closing flags in field order.
                    let opened = extract_bits(quotes, starts);
                    if opened != extract_bits(quotes << 1, boundaries) {
                        continue;
                    }
                    let mut start = 0;
                    return Some(std::array::from_fn(|i| {
                        let stop = if i + 1 == N {
                            end
                        } else {
                            let stop = commas.trailing_zeros() as usize;
                            commas &= commas - 1;
                            stop
                        };
                        let trim = ((opened >> i) & 1) as usize;
                        // The mask gave N increasing boundaries before the
                        // newline. A quoted field has two distinct quote bytes.
                        let field = unsafe { bytes.get_unchecked(start + trim..stop - trim) };
                        start = stop + 1;
                        field
                    }));
                }
            }
            let row = if let Some(end) = memchr(b'\n', bytes) {
                self.remaining = &bytes[end + 1..];
                &bytes[..end]
            } else {
                std::mem::take(&mut self.remaining)
            };
            if let Some(fields) = fields(row) {
                return Some(fields);
            }
        }
        None
    }
}

#[inline(always)]
fn extract_bits(word: u128, mask: u128) -> u128 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        use std::arch::x86_64::_pext_u64;
        let low = mask as u64;
        u128::from(_pext_u64(word as u64, low))
            | (u128::from(_pext_u64((word >> 64) as u64, (mask >> 64) as u64)) << low.count_ones())
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut mask = mask;
        let mut result = 0;
        let mut target = 1;
        while mask != 0 {
            result |= u128::from(word & (mask & mask.wrapping_neg()) != 0) * target;
            mask &= mask - 1;
            target <<= 1;
        }
        result
    }
}

#[inline]
fn row_masks(bytes: &[u8]) -> (u128, u128, u128) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let mut result = (0_u128, 0_u128, 0_u128);
        for offset in [0, 32, 64, 96] {
            // The caller provides at least 128 readable bytes.
            let (newline, comma, quote) = unsafe {
                let data = _mm256_loadu_si256(bytes.as_ptr().add(offset).cast());
                (
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(b'\n' as i8)))
                        as u32,
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(b',' as i8)))
                        as u32,
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(data, _mm256_set1_epi8(b'"' as i8)))
                        as u32,
                )
            };
            result.0 |= u128::from(newline) << offset;
            result.1 |= u128::from(comma) << offset;
            result.2 |= u128::from(quote) << offset;
            if newline != 0 {
                break;
            }
        }
        result
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let mut result = (0_u128, 0_u128, 0_u128);
        for (index, &byte) in bytes[..128].iter().enumerate() {
            let bit = 1_u128 << index;
            match byte {
                b'\n' => {
                    result.0 |= bit;
                    break;
                }
                b',' => result.1 |= bit,
                b'"' => result.2 |= bit,
                _ => {}
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_dialect() {
        assert_eq!(
            fields::<3>(b"\" a\",\"\",b\r"),
            Some([&b" a"[..], b"", b"b\r"])
        );
        for bad in [
            &b"a,b"[..],
            b"a,b,c,d",
            b"a,b,\"c\"\r",
            b"a,b,c\"",
            b"a,b,\"\"\"",
        ] {
            assert_eq!(fields::<3>(bad), None);
        }
        assert_eq!(
            data_rows(b"header\n\na\r\nb").collect::<Vec<_>>(),
            [&b""[..], b"a\r", b"b"]
        );
    }

    #[test]
    fn chunk_boundaries_preserve_rows() {
        let bytes = b"abc\n\nz\na rather long line\nlast";
        let expected = rows(bytes).collect::<Vec<_>>();
        for size in 1..=bytes.len() {
            let actual = byte_chunks(bytes, size).flat_map(rows).collect::<Vec<_>>();
            assert_eq!(actual, expected, "chunk size {size}");
        }
    }

    #[test]
    fn chunk_parser_matches_reference_for_mixed_dialects() {
        let mut state = 3_u64;
        let mut data = Vec::new();
        let alphabet = b"abcde0123,\n\r\",,";
        for size in 1..=600 {
            data.clear();
            for _ in 0..size {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                data.push(alphabet[(state >> 32) as usize % alphabet.len()]);
            }
            assert_eq!(
                ChunkRows::<2>::new(&data).collect::<Vec<_>>(),
                rows(&data).filter_map(fields::<2>).collect::<Vec<_>>()
            );
            assert_eq!(
                ChunkRows::<4>::new(&data).collect::<Vec<_>>(),
                rows(&data).filter_map(fields::<4>).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn window_scanner_matches_scalar() {
        let mut state = 5_u64;
        let alphabet = b"abcd12,\n\r\",,";
        for size in 0..900 {
            let mut bytes = Vec::new();
            for _ in 0..size {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                bytes.push(alphabet[(state >> 32) as usize % alphabet.len()]);
            }
            bytes.extend_from_slice(b"\" abc\",b,c,\"d\"\n\"\"x,y,z,q\na,,c,d\n");
            assert_eq!(
                RowWindows::<2>::new(&bytes).collect::<Vec<_>>(),
                rows(&bytes).filter_map(fields::<2>).collect::<Vec<_>>()
            );
            assert_eq!(
                RowWindows::<4>::new(&bytes).collect::<Vec<_>>(),
                rows(&bytes).filter_map(fields::<4>).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn scanners_match_scalar_across_quote_and_vector_boundaries() {
        let mut data = Vec::new();
        let mut state = 7_u64;
        for pass in 0..3 {
            data.clear();
            for row in 0..6000 {
                for col in 0..7 {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let quote = state >> 63 != 0;
                    let len = (state >> 32) as usize % 17;
                    if quote {
                        data.push(b'"');
                    }
                    data.extend(std::iter::repeat_n(b'A' + col, len));
                    if pass == 1 && row % 143 == 19 && col == 2 {
                        data.push(b'"');
                    }
                    if quote {
                        data.push(b'"');
                    }
                    if col != 6 {
                        data.push(b',');
                    }
                }
                if pass == 2 && row % 7 == 0 {
                    data.extend_from_slice(b",unexpected");
                }
                data.push(b'\n');
            }
            data.extend(std::iter::repeat_n(b'x', 2200));
            data.extend_from_slice(b",b,c,d,e,f,\"\"\nlast,b,c,d,e,f,g".as_slice());
            assert_eq!(
                RowWindows::<7>::new(&data).collect::<Vec<_>>(),
                rows(&data).filter_map(fields::<7>).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn timestamps_are_strict_and_normalized() {
        assert_eq!(timestamp(b"1970-01-01T00:00:00Z"), Some(0));
        let instant = timestamp(b"2028-02-29T20:01:07Z").unwrap();
        assert_eq!(timestamp(b"2028-03-01T01:01:07+05:00"), Some(instant));
        assert_eq!(timestamp(format!("000{instant}").as_bytes()), Some(instant));
        assert_eq!(
            timestamp(b"2026-08-31T00:00:00Z"),
            timestamp(b"2026-09-01T00:00:00Z").map(|ts| ts - DAY_MS)
        );
        assert_eq!(timestamp(b"1969-12-31T23:59:59Z"), Some(-1000));
        for bad in [
            "2027-02-29T10:00:00Z",
            "1900-02-29T10:00:00Z",
            "2028-02-29T24:00:00Z",
            "2028-02-29T10:00:60Z",
            "2028-02-29T10:00:00+24:00",
            "2028-02-29T10:00:00Z\r",
            "2028-02-29T10:00:00-00:60",
            "99999999999999999999999",
            "٢٥٠",
        ] {
            assert_eq!(timestamp(bad.as_bytes()), None, "{bad}");
        }
    }
}
