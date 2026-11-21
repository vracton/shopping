use super::*;

pub(super) enum ClickInput {
    ByChunk(Vec<Vec<Click>>),
    ByPerson(Vec<Vec<Vec<Click>>>),
}

/// Resolve a small block together so independent hash probes overlap.
fn scatter(block: &[Click], links: &Aliases, output: &mut [Vec<Click>]) {
    if let Aliases::Sparse(table) = links {
        for row in block {
            table.prefetch(row.user);
        }
        for row in block {
            let user = table.resolve(row.user);
            output[bucket(user, output.len())].push(Click { user, ..*row });
        }
    } else {
        for row in block {
            let user = links.resolve(row.user);
            output[bucket(user, output.len())].push(Click { user, ..*row });
        }
    }
}

impl ClickInput {
    pub(super) fn decode_routed(
        chunks: &[&[u8]],
        parser: &Parser<'_, '_>,
        links: &Aliases,
        count: usize,
        window_rows: bool,
    ) -> Option<Self> {
        let group_size = chunks
            .len()
            .div_ceil(rayon::current_num_threads() * 8)
            .max(1);
        let groups = chunks
            .par_chunks(group_size)
            .map(|chunks| {
                let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
                let mut output: Vec<_> = (0..count)
                    .map(|_| Vec::with_capacity(bytes / 50 / count))
                    .collect();
                let mut pending = Vec::with_capacity(32);
                let mut accept = |row| {
                    if let Some(click) = parser.click(row)? {
                        pending.push(click);
                        if pending.len() == 32 {
                            scatter(&pending, links, &mut output);
                            pending.clear();
                        }
                    }
                    Some(())
                };
                for chunk in chunks {
                    if window_rows {
                        for row in RowWindows::new(chunk) {
                            accept(row)?;
                        }
                    } else {
                        for row in ChunkRows::new(chunk) {
                            accept(row)?;
                        }
                    }
                }
                scatter(&pending, links, &mut output);
                Some(output)
            })
            .collect::<Option<_>>()?;
        Some(Self::ByPerson(groups))
    }
}
