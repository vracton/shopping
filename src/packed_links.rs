use super::{Key, RowWindows};

/// A sparse union-find that stores a key only after its component root
/// joins a smaller root. Keys absent from the table resolve to themselves.
pub(super) struct Sparse(Vec<(Key, Key)>);

impl Sparse {
    #[inline]
    pub(super) fn prefetch(&self, key: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let index = key.hash() as usize & (self.0.len() - 1);
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                self.0.as_ptr().add(index).cast(),
            )
        }
    }

    #[inline]
    fn find(&self, key: Key) -> usize {
        let mask = self.0.len() - 1;
        let mut index = key.hash() as usize & mask;
        loop {
            let (found, _) = self.0[index];
            if found == key || found.0 == 0 {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    fn root(&mut self, mut key: Key) -> (Key, usize) {
        let mut index = self.find(key);
        loop {
            let parent = self.0[index].1;
            if parent.0 == 0 {
                return (key, index);
            }
            let next = self.find(parent);
            let grandparent = self.0[next].1;
            if grandparent.0 == 0 {
                return (parent, next);
            }
            self.0[index].1 = grandparent;
            key = grandparent;
            index = self.find(key);
        }
    }

    pub(super) fn new(chunks: &[&[u8]]) -> Option<Self> {
        let rows: usize = chunks
            .iter()
            .map(|chunk| {
                memchr::memchr_iter(b'\n', chunk).count()
                    + usize::from(!chunk.is_empty() && !chunk.ends_with(b"\n"))
            })
            .sum();
        let size = rows
            .checked_add(rows / 2)?
            .checked_add(1)?
            .checked_next_power_of_two()?;
        let mut forest = Self(vec![(Key(0), Key(0)); size]);
        // Every successful join fills one previously absent root. Even
        // with duplicate and self links, the edge count bounds occupancy.
        let mut joined = Vec::with_capacity(rows);
        for &chunk in chunks {
            for [a, b] in RowWindows::<2>::new(chunk) {
                if a.is_empty() || b.is_empty() {
                    continue;
                }
                let (a, b) = (Key::pack(a)?, Key::pack(b)?);
                if a == b {
                    continue;
                }
                let (left, slot_left) = forest.root(a);
                let (right, slot_right) = forest.root(b);
                if left != right {
                    let (child, parent, slot) = if left > right {
                        (left, right, slot_left)
                    } else {
                        (right, left, slot_right)
                    };
                    forest.0[slot] = (child, parent);
                    joined.push(slot);
                }
            }
        }
        // A child points to a key that was still a root at insertion.
        // Parent entries therefore come later, so reverse insertion order
        // resolves the entire forest in one pass.
        for slot in joined.into_iter().rev() {
            forest.0[slot].1 = forest.resolve(forest.0[slot].1);
        }
        Some(forest)
    }

    #[inline]
    pub(super) fn resolve(&self, key: Key) -> Key {
        let parent = self.0[self.find(key)].1;
        if parent.0 == 0 { key } else { parent }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packed::Links;

    #[test]
    fn sparse_links_match_union_find_for_chains_cycles_and_collisions() {
        let names: Vec<_> = (1..1500).map(|index| format!("{index:09}")).collect();
        let mut state = 17_u64;
        for trial in 0..20 {
            let mut body = String::new();
            for edge in 0..4000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let a = state as usize % names.len();
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let b = if trial == 0 {
                    // Descending joins exercise the reverse-order flatten.
                    a.saturating_sub(1)
                } else if edge % 11 == 0 {
                    a
                } else {
                    state as usize % names.len()
                };
                body.push_str(&format!("\"{}\",{}\n", names[a], names[b]));
            }
            body.push_str("a,b,c\n,\"\"\nbad,\"interior\"quote\"\n");
            body.push_str("\" G\r\",\" a\"\n\" a\",000000001");
            let file = format!("a,b\n{body}");
            let reference = Links::new(std::iter::once(file.as_bytes())).unwrap();
            let chunks: Vec<_> = body
                .as_bytes()
                .split_inclusive(|&byte| byte == b'\n')
                .collect();
            let table = Sparse::new(&chunks).unwrap();
            let tokens = super::super::tokens::Directory::new(&chunks, 4 << 20, 8).unwrap();
            for name in names
                .iter()
                .map(String::as_bytes)
                .chain([&b"absent"[..], b" G\r", b" a"])
            {
                let key = Key::pack(name).unwrap();
                assert_eq!(table.resolve(key), reference.resolve(key), "{name:?}");
                assert_eq!(tokens.resolve(key), reference.resolve(key), "{name:?}");
            }
        }
    }

    #[test]
    fn sparse_links_handle_empty_input_and_request_wide_fallback() {
        let table = Sparse::new(&[]).unwrap();
        assert_eq!(table.resolve(Key(1)), Key(1));
        assert!(Sparse::new(&[b"short,too_long_for_packed_ids\n"]).is_none());
    }
}
