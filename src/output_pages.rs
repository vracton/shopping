//! A flat user-day row group maps directly to six Parquet data pages. Keep the
//! compact physical encodings local to this writer; the logical schema is the
//! same schema used by the Arrow writer.
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowSchemaConverter, encode_arrow_schema};
use parquet::basic::Encoding;
use parquet::column::page::{CompressedPage, Page, PageWriter};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataWriter,
    RowGroupMetaData,
};
use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
use parquet::schema::types::{ColumnDescPtr, SchemaDescPtr};

use crate::Result;
use crate::aggregate::{Daily, UserDay, UserId};
use crate::catalog::Catalog;

fn varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        buf.push(value as u8 | 128);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn signed(buf: &mut Vec<u8>, value: i64) {
    varint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

// DELTA_BINARY_PACKED. Round each miniblock's width up to a native word
// size for bulk little-endian stores. The last miniblock is zero-padded.
fn write_deltas<const INT32: bool>(out: &mut Vec<u8>, values: &[i64]) {
    out.extend_from_slice(&[0x80, 1, 4]); // 128 deltas, 4 miniblocks.
    varint(out, values.len() as u64);
    let mut previous = values[0];
    signed(out, previous);
    for block in values[1..].chunks(128) {
        let mut deltas = [0i64; 128];
        for (slot, &value) in deltas.iter_mut().zip(block) {
            *slot = if INT32 {
                i64::from((value as i32).wrapping_sub(previous as i32))
            } else {
                value.wrapping_sub(previous)
            };
            previous = value;
        }
        let min = *deltas[..block.len()].iter().min().unwrap();
        signed(out, min);
        let header = out.len();
        out.extend_from_slice(&[0; 4]);
        deltas[block.len()..].fill(min);
        for (index, chunk) in deltas[..block.len().next_multiple_of(32)]
            .chunks_exact(32)
            .enumerate()
        {
            let chunk: &[i64; 32] = chunk.try_into().unwrap();
            let max = chunk.iter().max().unwrap().wrapping_sub(min) as u64;
            macro_rules! emit {
                ($ty:ty) => {{
                    out[header + index] = <$ty>::BITS as u8;
                    let bytes = chunk.map(|value| (value.wrapping_sub(min) as $ty).to_le_bytes());
                    out.extend_from_slice(bytes.as_flattened());
                }};
            }
            match max {
                0 => {}
                1..=0xff => emit!(u8),
                0x100..=0xffff => emit!(u16),
                0x10000..=0xffff_ffff => emit!(u32),
                _ => emit!(u64),
            }
        }
    }
}

struct DataPage {
    values: Vec<u8>,
    encoding: Encoding,
    levels_len: usize,
    nulls: usize,
}

impl DataPage {
    fn required(values: Vec<u8>, encoding: Encoding) -> Self {
        Self {
            values,
            encoding,
            levels_len: 0,
            nulls: 0,
        }
    }

    fn write(
        self,
        dst: &mut TrackedWrite<File>,
        column: ColumnDescPtr,
        rows: u32,
    ) -> Result<ColumnChunkMetaData> {
        let size = self.values.len();
        let page = Page::DataPageV2 {
            buf: self.values.into(),
            num_values: rows,
            num_rows: rows,
            num_nulls: self.nulls as u32,
            encoding: self.encoding,
            def_levels_byte_len: self.levels_len as u32,
            rep_levels_byte_len: 0,
            is_compressed: false,
            statistics: None,
        };
        let spec = SerializedPageWriter::new(dst).write_page(CompressedPage::new(page, size))?;
        let mut encodings = vec![self.encoding];
        if self.levels_len != 0 {
            encodings.push(Encoding::RLE);
        }
        Ok(ColumnChunkMetaData::builder(column)
            .set_num_values(i64::from(rows))
            .set_total_compressed_size(spec.compressed_size as i64)
            .set_total_uncompressed_size(spec.uncompressed_size as i64)
            .set_data_page_offset(spec.offset as i64)
            .set_encodings(encodings)
            .build()?)
    }
}

fn integers<const INT32: bool>(
    rows: &[UserDay<'_>],
    get: impl Fn(&UserDay<'_>) -> i64,
) -> DataPage {
    let values: Vec<_> = rows.iter().map(get).collect();
    let mut out = Vec::with_capacity(rows.len() * 2);
    write_deltas::<INT32>(&mut out, &values);
    DataPage::required(out, Encoding::DELTA_BINARY_PACKED)
}

fn identifiers(rows: &[UserDay<'_>]) -> DataPage {
    let mut previous = None;
    let mut previous_len = 0;
    let mut prefixes = Vec::with_capacity(rows.len());
    let mut lengths = Vec::with_capacity(rows.len());
    let mut suffix = Vec::with_capacity(rows.len() * 4);
    for row in rows {
        let UserId::Packed(key) = row.user else {
            unreachable!()
        };
        if previous == Some(key) {
            prefixes.push(previous_len);
            lengths.push(0);
        } else {
            let mut buf = [0; 9];
            let id = key.decode(&mut buf).as_bytes();
            previous = Some(key);
            previous_len = id.len() as i64;
            prefixes.push(0);
            lengths.push(previous_len);
            suffix.extend_from_slice(id);
        }
    }
    let mut out = Vec::with_capacity(rows.len() * 3 + suffix.len());
    write_deltas::<true>(&mut out, &prefixes);
    write_deltas::<true>(&mut out, &lengths);
    out.extend_from_slice(&suffix);
    DataPage::required(out, Encoding::DELTA_BYTE_ARRAY)
}

fn categories(rows: &[UserDay<'_>], catalog: &Catalog<'_>) -> Result<DataPage> {
    let mut out = Vec::with_capacity(rows.len() * 2);
    // One literal run of definition bits; unused bits in the last byte are zero.
    varint(&mut out, (rows.len().div_ceil(8) * 2 + 1) as u64);
    let mut defined = 0;
    for group in rows.chunks(8) {
        let bits = group.iter().enumerate().fold(0u8, |mask, (i, row)| {
            mask | ((row.top_category.is_some() as u8) << i)
        });
        out.push(bits);
        defined += bits.count_ones() as usize;
    }
    let levels_len = out.len();
    for index in rows.iter().filter_map(|r| r.top_category) {
        let text = std::str::from_utf8(catalog.categories[index as usize])?.as_bytes();
        out.extend_from_slice(&(text.len() as u32).to_le_bytes());
        out.extend_from_slice(text);
    }
    Ok(DataPage {
        values: out,
        encoding: Encoding::PLAIN,
        levels_len,
        nulls: rows.len() - defined,
    })
}

// If a category could exceed the physical page length limit, use Arrow's
// regular page sizing. Packed identifiers have a bounded decoded length.
fn row_group_size(catalog: &Catalog<'_>, requested: usize) -> Option<usize> {
    let rows = requested.min(64 * 1024);
    let max_category = catalog
        .categories
        .iter()
        .map(|c| c.len())
        .max()
        .unwrap_or(0);
    (max_category.saturating_add(8).saturating_mul(rows) <= i32::MAX as usize).then_some(rows)
}

pub(crate) struct BucketOutput<'a> {
    path: PathBuf,
    catalog: &'a Catalog<'a>,
    rows: usize,
}

impl<'a> BucketOutput<'a> {
    pub(crate) fn prepare(
        dir: &Path,
        catalog: &'a Catalog<'a>,
        requested: usize,
    ) -> Result<Option<Self>> {
        if !std::env::var_os("SHOPPING_WRITER").is_some_and(|s| s == "bucket") {
            return Ok(None);
        }
        let Some(rows) = row_group_size(catalog, requested) else {
            return Ok(None);
        };
        let path = dir.join("user_daily");
        std::fs::create_dir_all(&path)?;
        Ok(Some(Self {
            path,
            catalog,
            rows,
        }))
    }

    pub(crate) fn open(&self, index: usize) -> Result<PartWriter> {
        PartWriter::open(&self.path.join(format!("part-{index}.parquet")))
    }

    pub(crate) fn append(&self, part: &mut PartWriter, rows: &[UserDay<'_>]) -> Result<()> {
        for group in rows.chunks(self.rows) {
            part.append(group, self.catalog)?;
        }
        Ok(())
    }
}

pub(super) fn try_write(
    path: &Path,
    parts: &[Daily<'_>],
    catalog: &Catalog<'_>,
    row_group: usize,
) -> Result<bool> {
    let Some(row_group) = row_group_size(catalog, row_group) else {
        return Ok(false);
    };
    if parts
        .iter()
        .flat_map(|p| &p.users)
        .any(|r| matches!(r.user, UserId::Bytes(_)))
    {
        return Ok(false);
    }
    let mut writer = PartWriter::open(path)?;
    for rows in parts.iter().flat_map(|p| p.users.chunks(row_group)) {
        writer.append(rows, catalog)?;
    }
    writer.close()?;
    Ok(true)
}

pub(crate) struct PartWriter {
    dst: TrackedWrite<File>,
    schema: SchemaDescPtr,
    groups: Vec<RowGroupMetaData>,
    num_rows: i64,
}

impl PartWriter {
    fn open(path: &Path) -> Result<Self> {
        let schema = Arc::new(ArrowSchemaConverter::new().convert(&super::user_schema())?);
        let mut dst = TrackedWrite::new(File::create(path)?);
        dst.write_all(b"PAR1")?;
        Ok(Self {
            dst,
            schema,
            groups: Vec::new(),
            num_rows: 0,
        })
    }

    fn append(&mut self, rows: &[UserDay<'_>], catalog: &Catalog<'_>) -> Result<()> {
        let pages = [
            identifiers(rows),
            integers::<true>(rows, |r| i64::from(r.day)),
            integers::<false>(rows, |r| r.sessions),
            integers::<false>(rows, |r| r.events),
            integers::<false>(rows, |r| r.revenue),
            categories(rows, catalog)?,
        ];
        let mut columns = Vec::with_capacity(pages.len());
        for (index, page) in pages.into_iter().enumerate() {
            columns.push(page.write(
                &mut self.dst,
                self.schema.column(index),
                rows.len() as u32,
            )?);
        }
        let bytes = columns
            .iter()
            .map(|column| column.uncompressed_size())
            .sum();
        self.groups.push(
            RowGroupMetaData::builder(Arc::clone(&self.schema))
                .set_column_metadata(columns)
                .set_num_rows(rows.len() as i64)
                .set_total_byte_size(bytes)
                .build()?,
        );
        self.num_rows += rows.len() as i64;
        Ok(())
    }

    pub(crate) fn close(mut self) -> Result<()> {
        let kv = KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            encode_arrow_schema(&super::user_schema()),
        );
        let file = FileMetaData::new(
            2,
            self.num_rows,
            Some("shopping-sessions".into()),
            Some(vec![kv]),
            self.schema,
            None,
        );
        // No page indexes: all page locations in the footer are already absolute.
        let metadata = ParquetMetaData::new(file, self.groups);
        let mut footer = Vec::new();
        ParquetMetaDataWriter::new(&mut footer, &metadata).finish()?;
        self.dst.write_all(&footer)?;
        self.dst.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn flat_pages_roundtrip_extremes_and_partial_blocks() {
        let catalog = Catalog::new(std::iter::empty(), "id,category\np,é\n".as_bytes(), b"");
        let path =
            std::env::temp_dir().join(format!("shopping-pages-{}.parquet", std::process::id()));
        for len in [0, 1, 31, 32, 33, 127, 128, 129, 130, 257] {
            let rows: Vec<_> = (0..len)
                .map(|i| UserDay {
                    user: UserId::Packed(Default::default()),
                    day: [i32::MIN, i32::MAX, 0][i % 3],
                    sessions: [i64::MIN, i64::MAX, 0][i % 3],
                    events: i as i64 * 100_003,
                    revenue: 0,
                    top_category: (i % 8 != 0).then_some(1),
                })
                .collect();
            let expected =
                crate::output::user_batch(&rows, &catalog, crate::output::user_schema()).unwrap();
            let part = Daily {
                users: rows,
                ..Daily::default()
            };
            assert!(try_write(&path, &[part], &catalog, 65_536).unwrap());
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            assert_eq!(builder.schema(), &expected.schema());
            let actual = builder
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            if len == 0 {
                assert!(actual.is_empty());
            } else {
                assert_eq!(actual, vec![expected]);
            }
        }
        std::fs::remove_file(path).unwrap();
    }
}
