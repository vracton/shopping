use super::{Candidate, Event, INVALID, Key, RowWindows, bucket};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub(super) fn table_size(chunks: &[&[u8]]) -> Option<(usize, usize)> {
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
    Some((rows, size))
}

#[derive(Default)]
struct EdgeSlot {
    child: AtomicU64,
    parent: AtomicU64,
}

struct Concurrent(Vec<EdgeSlot>);

impl Concurrent {
    /// Claim only the larger endpoint. Every edge points to a strictly smaller
    /// key. If that endpoint already has an edge, joining the two smaller
    /// endpoints preserves the connected component.
    fn join(&self, mut a: Key, mut b: Key) {
        let mask = self.0.len() - 1;
        while a != b {
            if a < b {
                std::mem::swap(&mut a, &mut b);
            }
            let mut at = a.hash() as usize & mask;
            loop {
                let slot = &self.0[at];
                let mut found = slot.child.load(Relaxed);
                if found == 0 {
                    found = match slot.child.compare_exchange(0, a.0, Relaxed, Relaxed) {
                        Ok(_) => a.0,
                        Err(found) => found,
                    };
                }
                if found == a.0 {
                    let mut old = slot.parent.load(Relaxed);
                    while old == 0 || b.0 < old {
                        match slot
                            .parent
                            .compare_exchange_weak(old, b.0, Relaxed, Relaxed)
                        {
                            Ok(_) if old == 0 => return,
                            Ok(_) => break,
                            Err(now) => old = now,
                        }
                    }
                    a = Key(old);
                    break;
                }
                at = (at + 1) & mask;
            }
        }
    }

    fn parent(&self, key: Key) -> Option<&AtomicU64> {
        let mask = self.0.len() - 1;
        let mut at = key.hash() as usize & mask;
        loop {
            let slot = &self.0[at];
            match slot.child.load(Relaxed) {
                0 => return None,
                found if found == key.0 => return Some(&slot.parent),
                _ => at = (at + 1) & mask,
            }
        }
    }

    fn canonical(&self, slot: &EdgeSlot) -> Key {
        let start = Key(slot.parent.load(Relaxed));
        let mut root = start;
        while let Some(parent) = self.parent(root) {
            root = Key(parent.load(Relaxed));
        }
        // All joins have finished. Any path compression by other workers now
        // stores this same final root. Compress intermediate keys as well to
        // bound the work for a single long descending chain.
        let mut next = start;
        while next != root {
            let cell = self.parent(next).unwrap();
            next = Key(cell.swap(root.0, Relaxed));
        }
        slot.parent.store(root.0, Relaxed);
        root
    }
}

/// A sparse union-find that stores a key only after its component root
/// joins a smaller root. Keys absent from the table resolve to themselves.
pub(super) struct Sparse(Vec<(Key, Key)>);

impl Sparse {
    pub(super) fn into_pairs(self) -> Vec<(Key, Key)> {
        self.0
    }
    pub(super) fn route_batched(
        &self,
        rows: impl Iterator<Item = Candidate>,
        output: &mut [Vec<Event>],
    ) {
        const BLOCK: usize = 32;
        let mut block = [Event {
            user: Key(0),
            ts: 0,
            purchase: 0,
        }; BLOCK];
        let mut len = 0;
        let scatter = |block: &[Event], output: &mut [Vec<Event>]| {
            for row in block {
                let user = self.resolve(row.user);
                output[bucket(user, output.len())].push(Event { user, ..*row });
            }
        };
        for row in rows.filter(|row| row.ts != INVALID) {
            self.prefetch(row.user);
            block[len] = Event {
                user: row.user,
                ts: row.ts,
                purchase: row.purchase,
            };
            len += 1;
            if len == BLOCK {
                scatter(&block, output);
                len = 0;
            }
        }
        scatter(&block[..len], output);
    }
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
        let (rows, size) = table_size(chunks)?;
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

    pub(super) fn concurrent(chunks: &[&[u8]]) -> Option<Self> {
        let (_, size) = table_size(chunks)?;
        let graph = Concurrent((0..size).map(|_| EdgeSlot::default()).collect());
        chunks.par_iter().try_for_each(|chunk| {
            for [a, b] in RowWindows::<2>::new(chunk) {
                if !a.is_empty() && !b.is_empty() {
                    graph.join(Key::pack(a)?, Key::pack(b)?);
                }
            }
            Some(())
        })?;
        let flat = graph
            .0
            .par_iter()
            .map(|slot| {
                let child = Key(slot.child.load(Relaxed));
                let root = if child.0 == 0 {
                    Key(0)
                } else {
                    graph.canonical(slot)
                };
                (child, root)
            })
            .collect();
        Some(Self(flat))
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
            let concurrent = Sparse::concurrent(&chunks).unwrap();
            for name in names
                .iter()
                .map(String::as_bytes)
                .chain([&b"absent"[..], b" G\r", b" a"])
            {
                let key = Key::pack(name).unwrap();
                assert_eq!(table.resolve(key), reference.resolve(key), "{name:?}");
                assert_eq!(concurrent.resolve(key), reference.resolve(key), "{name:?}");
            }
        }
    }

    #[test]
    fn sparse_links_handle_empty_input_and_request_wide_fallback() {
        let table = Sparse::new(&[]).unwrap();
        assert_eq!(table.resolve(Key(1)), Key(1));
        assert!(Sparse::new(&[b"short,too_long_for_packed_ids\n"]).is_none());
        assert!(Sparse::concurrent(&[b"short,too_long_for_packed_ids\n"]).is_none());
    }
}
