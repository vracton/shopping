use rustc_hash::FxHashMap;

use crate::input::{data_rows, fields};

pub struct Catalog<'a> {
    users: FxHashMap<&'a [u8], &'a [u8]>,
    products: FxHashMap<&'a [u8], usize>,
    campaigns_by_id: FxHashMap<&'a [u8], usize>,
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
        let product_rows: Vec<_> = data_rows(products).filter_map(fields::<2>).collect();
        let mut categories = vec![&b"unknown"[..]];
        categories.extend(product_rows.iter().map(|row| row[1]));
        categories.sort_unstable();
        categories.dedup();
        let unknown = categories.binary_search(&&b"unknown"[..]).unwrap();
        let products = product_rows
            .into_iter()
            .map(|[id, category]| (id, categories.binary_search(&category).unwrap()))
            .collect();
        let mut campaigns: Vec<_> = data_rows(campaigns)
            .filter_map(fields::<2>)
            .map(|[id, channel]| Campaign { id, channel })
            .collect();
        let campaigns_by_id = campaigns
            .iter()
            .enumerate()
            .map(|(index, row)| (row.id, index))
            .collect();
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
        self.products.get(product).copied().unwrap_or(self.unknown)
    }

    pub fn campaign(&self, id: &[u8]) -> Option<usize> {
        self.campaigns_by_id.get(id).copied()
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
