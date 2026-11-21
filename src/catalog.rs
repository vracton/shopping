use rustc_hash::FxHashMap;

#[path = "catalog_bits.rs"]
mod bits;

use crate::input::{RowWindows, data_rows, fields, without_header};

pub struct Catalog<'a> {
    users: FxHashMap<&'a [u8], &'a [u8]>,
    products: bits::Index<'a>,
    campaigns_by_id: bits::Index<'a>,
    pub categories: Vec<&'a [u8]>,
    pub campaigns: Vec<Campaign<'a>>,
    pub organic: usize,
    unknown: usize,
}

pub struct Campaign<'a> {
    pub id: &'a [u8],
    pub channel: &'a [u8],
}

impl<'a> Catalog<'a> {
    pub fn new(
        links: impl Iterator<Item = &'a [u8]>,
        products: &'a [u8],
        campaigns: &'a [u8],
    ) -> Self {
        let users = linked_users(links);
        let mut names: Vec<&[u8]> = vec![b"unknown"];
        let mut category_ids = FxHashMap::default();
        category_ids.insert(names[0], 0);
        let body = without_header(products);

        let mut product_rows = Vec::with_capacity(body.len() / 12);
        for [id, category] in RowWindows::<2>::new(body) {
            let number = *category_ids.entry(category).or_insert_with(|| {
                let n = names.len();
                names.push(category);
                n
            });
            product_rows.push((id, number));
        }
        let mut categories = names.clone();
        categories.sort_unstable();
        let remap: Vec<_> = names
            .iter()
            .map(|category| categories.binary_search(category).unwrap())
            .collect();
        let unknown = remap[0];
        let products = bits::Index::new(
            product_rows
                .iter()
                .map(|&(id, category)| (id, remap[category])),
        );
        let mut campaigns: Vec<_> = data_rows(campaigns)
            .filter_map(fields::<2>)
            .map(|[id, channel]| Campaign { id, channel })
            .collect();
        let campaigns_by_id = bits::Index::new(
            campaigns
                .iter()
                .enumerate()
                .map(|(index, row)| (row.id, index)),
        );
        let organic = campaigns.len();
        campaigns.push(Campaign {
            id: b"organic",
            channel: b"organic",
        });
        Self {
            users,
            products,
            campaigns_by_id,
            categories,
            campaigns,
            organic,
            unknown,
        }
    }

    pub fn user(&self, id: &'a [u8]) -> &'a [u8] {
        self.users.get(id).copied().unwrap_or(id)
    }

    pub fn category(&self, product: &[u8]) -> usize {
        self.products.get(product).unwrap_or(self.unknown)
    }

    pub fn campaign(&self, id: &[u8]) -> Option<usize> {
        self.campaigns_by_id.get(id)
    }
}

#[derive(Default)]
struct Links<'a> {
    ids: FxHashMap<&'a [u8], usize>,
    names: Vec<&'a [u8]>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
}

impl<'a> Links<'a> {
    fn intern(&mut self, name: &'a [u8]) -> usize {
        *self.ids.entry(name).or_insert_with(|| {
            let id = self.names.len();
            self.names.push(name);
            self.parents.push(id);
            self.sizes.push(1);
            id
        })
    }

    fn root(&mut self, mut id: usize) -> usize {
        let start = id;
        while id != self.parents[id] {
            self.parents[id] = self.parents[self.parents[id]];
            id = self.parents[id];
        }
        self.parents[start] = id;
        id
    }

    fn join(&mut self, left: &'a [u8], right: &'a [u8]) {
        let left = self.intern(left);
        let right = self.intern(right);
        let mut a = self.root(left);
        let mut b = self.root(right);
        if a == b {
            return;
        }
        if self.sizes[a] < self.sizes[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parents[b] = a;
        self.sizes[a] += self.sizes[b];
    }
}

fn linked_users<'a>(files: impl Iterator<Item = &'a [u8]>) -> FxHashMap<&'a [u8], &'a [u8]> {
    let mut links = Links::default();
    for [left, right] in files.flat_map(data_rows).filter_map(fields::<2>) {
        if !left.is_empty() && !right.is_empty() {
            links.join(left, right);
        }
    }
    let mut minima = links.names.clone();
    for id in 0..links.names.len() {
        let root = links.root(id);
        minima[root] = minima[root].min(links.names[id]);
    }
    links
        .ids
        .into_iter()
        .map(|(name, id)| (name, minima[links.parents[id]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_are_transitive_and_include_unused_names() {
        let files = [
            &b"header\nx,z\ny,x\nx,x\n"[..],
            &b"header\nz,A\ny,\n,missing\nq,z,\"bad"[..],
        ];
        let map = linked_users(files.into_iter());
        for key in [b"x", b"y", b"z", b"A"] {
            assert_eq!(map.get(key.as_slice()), Some(&&b"A"[..]));
        }
        assert_eq!(map.len(), 4);
    }
}
