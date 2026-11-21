//! Each BCD pair selects both a validation result and its time contribution.
//! Invalid pairs set a high bit outside the range of a legal wall clock.
const INVALID: u32 = 1 << 29;

const fn pairs(limit: u32, scale: u32) -> [u32; 256] {
    let mut table = [INVALID; 256];
    let mut i = 0;
    while i < limit {
        table[(i / 10 * 16 + i % 10) as usize] = i * scale;
        i += 1;
    }
    table
}
static HOURS: [u32; 256] = pairs(24, 3600);
static MINUTES: [u32; 256] = pairs(60, 60);
static SECONDS: [u32; 256] = pairs(60, 1);

#[inline(always)]
pub(super) fn wall_seconds(bytes: &[u8]) -> Option<i64> {
    let ascii = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
    if ascii & 0xf0f0_fff0_f0ff_f0f0 != 0x3030_3a30_303a_3030 {
        return None;
    }
    let extract = |clock| super::extract(clock, super::TIME_NIBBLES);
    let code = extract(ascii);
    let local = HOURS[(code >> 16) as usize]
        + MINUTES[((code >> 8) as u8) as usize]
        + SECONDS[(code as u8) as usize];
    if local >= INVALID {
        return None;
    }
    if bytes.len() == 20 {
        return (bytes[19] == b'Z').then_some(i64::from(local));
    }
    let zone = u64::from_be_bytes(bytes[17..25].try_into().unwrap());
    if zone & 0x0000_00f0_f0ff_f0f0 != 0x0000_0030_303a_3030 {
        return None;
    }
    let zone_code = super::extract(zone, 0x0f0f_000f0f);
    let offset = HOURS[(zone_code >> 8) as usize] + MINUTES[(zone_code as u8) as usize];
    if offset >= INVALID {
        return None;
    }
    match bytes[19] {
        b'+' => Some(i64::from(local) - i64::from(offset)),
        b'-' => Some(i64::from(local) + i64::from(offset)),
        _ => None,
    }
}
