use super::*;

/// A blocked Bloom filter over every non-identity sparse alias.
/// Negative probes prove that this shopper is already canonical.
pub(super) struct Router<'a> {
    sparse: &'a sparse_links::Sparse,
    blocks: Vec<u64>,
}

impl<'a> Router<'a> {
    pub(super) fn new(links: &'a Aliases) -> Option<Self> {
        if !std::env::var_os("SHOPPING_CLAIM_ALIASES").is_some_and(|m| m == "bitmap") {
            return None;
        }
        let Aliases::Sparse(sparse) = links else {
            return None;
        };
        let keys = sparse.keys();
        let n = (keys.len() / 16).max(1);
        let mut blocks = vec![0u64; n];
        for key in keys.filter(|key| key.0 != 0) {
            let hash = key.hash();
            blocks[hash as usize & (n - 1)] |= mask(hash);
        }
        Some(Self { sparse, blocks })
    }

    pub(super) fn route(
        &self,
        candidates: impl Iterator<Item = Candidate>,
        output: &mut [Vec<Event>],
    ) {
        let mut cache = [(Key(0), Key(0)); 1024];
        let parts = output.len() - 1;
        for row in candidates.filter(|row| row.ts != INVALID) {
            let hash = row.user.hash();
            let required = mask(hash);
            let bits = self.blocks[hash as usize & (self.blocks.len() - 1)];
            let (user, bucket_hash) = if bits & required != required {
                (row.user, hash)
            } else {
                let cached = &mut cache[hash as usize & 1023];
                if cached.0 != row.user {
                    *cached = (row.user, self.sparse.resolve(row.user));
                }
                (cached.1, cached.1.hash())
            };
            output[(bucket_hash >> 32) as usize & parts].push(Event {
                user,
                ts: row.ts,
                purchase: row.purchase,
            });
        }
    }
}

#[inline]
fn mask(hash: u64) -> u64 {
    (1u64 << ((hash >> 32) & 63)) | (1u64 << ((hash >> 48) & 63))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn false_positives_still_resolve_the_full_user() {
        let sparse = sparse_links::Sparse::new(&[b"z,m\nm,A\n"]).unwrap();
        let router = Router {
            sparse: &sparse,
            blocks: vec![u64::MAX],
        };
        let key = |s: &[u8]| Key::pack(s).unwrap();
        let rows = [key(b"z"), key(b"m"), key(b"free")].map(|user| Candidate {
            user,
            ts: 2,
            ..Candidate::default()
        });
        let mut out = buckets(8);
        router.route(rows.into_iter(), &mut out);
        let mut users = Vec::new();
        for (i, group) in out.into_iter().enumerate() {
            for event in group {
                assert_eq!(bucket(event.user, 8), i);
                users.push(event.user);
            }
        }
        users.sort_unstable();
        assert_eq!(users, [key(b"A"), key(b"A"), key(b"free")]);
    }
}
