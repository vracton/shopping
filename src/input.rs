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

pub fn chunks(files: &[Mmap], chunk_bytes: usize) -> Vec<&[u8]> {
    files
        .iter()
        .flat_map(|file| byte_chunks(without_header(file), chunk_bytes))
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
