use rustc_hash::FxHashMap;

#[derive(Default)]
struct Group {
    fixed: u64,
    varying: u64,
    values: Vec<u32>,
}

pub(super) struct Index<'a> {
    groups: [Group; 9],
    other: FxHashMap<&'a [u8], usize>,
}

#[inline(always)]
fn word(key: &[u8]) -> Option<u64> {
    // Match length separately. Together the first and last four bytes
    // preserve the complete key, with no unaligned read past its ends.
    if !(4..=8).contains(&key.len()) {
        return None;
    }
    let first = u32::from_le_bytes(key[..4].try_into().unwrap());
    let last = u32::from_le_bytes(key[key.len() - 4..].try_into().unwrap());
    Some(u64::from(first) | u64::from(last) << (8 * (key.len() - 4)))
}

#[inline(always)]
fn extract(word: u64, mask: u64) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(word, mask) as usize
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut bits = 0;
        let mut mask = mask;
        let mut bit = 1;
        while mask != 0 {
            let selected = mask & mask.wrapping_neg();
            if word & selected != 0 {
                bits |= bit;
            }
            mask ^= selected;
            bit <<= 1;
        }
        bits
    }
}

impl<'a> Index<'a> {
    pub(super) fn new(rows: impl Iterator<Item = (&'a [u8], usize)> + Clone) -> Self {
        let mut groups = std::array::from_fn::<Group, 9, _>(|_| Group::default());
        let mut counts = [0usize; 9];
        for (key, value) in rows.clone() {
            if let Some(word) = word(key) {
                let group = &mut groups[key.len()];
                if counts[key.len()] == 0 {
                    group.fixed = word;
                }
                group.varying |= group.fixed ^ word;
                if value >= u32::MAX as usize {
                    group.varying = u64::MAX;
                }
                counts[key.len()] += 1;
            }
        }
        for (group, count) in groups.iter_mut().zip(counts) {
            let bits = group.varying.count_ones();
            if count != 0 && bits <= 20 && 1usize << bits <= count.saturating_mul(32) {
                group.fixed &= !group.varying;
                group.values.resize(1 << bits, 0);
            }
        }
        let mut other = FxHashMap::default();
        for (key, value) in rows {
            if let Some(word) = word(key) {
                let group = &mut groups[key.len()];
                if !group.values.is_empty() {
                    group.values[extract(word, group.varying)] = value as u32 + 1;
                    continue;
                }
            }
            other.insert(key, value);
        }
        Self { groups, other }
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<usize> {
        if let Some(word) = word(key) {
            let group = &self.groups[key.len()];
            if !group.values.is_empty() {
                if word & !group.varying != group.fixed {
                    return None;
                }
                // The table spans all combinations of the selected bits.
                let value = group.values[extract(word, group.varying)];
                return value.checked_sub(1).map(|v| v as usize);
            }
        }
        self.other.get(key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_and_hashed_ids_match_the_complete_keys() {
        let rows = [
            (b"ab10".as_slice(), 0),
            (b"ab12", 7),
            (b"ab13", 5),
            (b"", 3),
            (b"some-long-id", 4),
            (b"ab12", 2),
            (b"ab12x", u32::MAX as usize),
        ];
        let index = Index::new(rows.into_iter());
        let expected = FxHashMap::from_iter(rows);
        for (key, _) in rows {
            for at in 0..key.len() {
                let mut changed = key.to_vec();
                for byte in 0..=255 {
                    changed[at] = byte;
                    assert_eq!(
                        index.get(&changed),
                        expected.get(changed.as_slice()).copied()
                    );
                }
            }
            assert_eq!(index.get(key), expected.get(key).copied());
        }
    }
}
