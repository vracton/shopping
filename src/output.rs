use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Date32Array, Int64Array, RecordBatch, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::Result;
use crate::aggregate::{CampaignStats, Daily, UserDay, UserId};
use crate::catalog::Catalog;

fn user_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn campaign_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]))
}

fn file_writer(
    path: &Path,
    schema: SchemaRef,
    row_group: usize,
) -> Result<ArrowWriter<BufWriter<File>>> {
    let compression =
        if std::env::var_os("SHOPPING_CODEC").as_deref() == Some(std::ffi::OsStr::new("plain")) {
            Compression::UNCOMPRESSED
        } else {
            Compression::SNAPPY
        };
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(row_group))
        .build();
    Ok(ArrowWriter::try_new(
        BufWriter::new(File::create(path)?),
        schema,
        Some(properties),
    )?)
}

fn user_batch(
    rows: &[UserDay<'_>],
    catalog: &Catalog<'_>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut user = StringBuilder::with_capacity(rows.len(), rows.len() * 9);
    let mut category = StringBuilder::with_capacity(rows.len(), rows.len());
    for row in rows {
        match row.user {
            UserId::Bytes(value) => user.append_value(std::str::from_utf8(value)?),
            UserId::Packed(value) => user.append_value(value.decode(&mut [0; 9])),
        }
        match row.top_category {
            Some(index) => {
                category.append_value(std::str::from_utf8(catalog.categories[index as usize])?)
            }
            None => category.append_null(),
        }
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(user.finish()),
            Arc::new(Date32Array::from_iter_values(
                rows.iter().map(|row| row.day),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.sessions),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.events),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.revenue),
            )),
            Arc::new(category.finish()),
        ],
    )?)
}

fn write_user_part(
    path: &Path,
    partitions: &[Daily<'_>],
    catalog: &Catalog<'_>,
    row_group: usize,
) -> Result<()> {
    let schema = user_schema();
    let mut writer = file_writer(path, Arc::clone(&schema), row_group)?;
    for rows in partitions
        .iter()
        .flat_map(|partition| partition.users.chunks(row_group))
    {
        writer.write(&user_batch(rows, catalog, Arc::clone(&schema))?)?;
    }
    writer.close()?;
    Ok(())
}

fn write_users(
    path: &Path,
    partitions: &[Daily<'_>],
    catalog: &Catalog<'_>,
    workers: usize,
    row_group: usize,
) -> Result<()> {
    if partitions.is_empty() {
        return write_user_part(&path.join("part-0.parquet"), partitions, catalog, row_group);
    }
    let per_file = partitions.len().div_ceil(workers);
    partitions
        .par_chunks(per_file)
        .enumerate()
        .try_for_each(|(index, group)| {
            write_user_part(
                &path.join(format!("part-{index}.parquet")),
                group,
                catalog,
                row_group,
            )
        })
}

fn write_campaigns(
    path: &Path,
    partitions: &[Daily<'_>],
    catalog: &Catalog<'_>,
    row_group: usize,
) -> Result<()> {
    let mut totals: rustc_hash::FxHashMap<_, CampaignStats> = rustc_hash::FxHashMap::default();
    for part in partitions {
        part.campaigns
            .for_each(|key, stats| *totals.entry(key).or_default() += stats);
    }
    let rows: Vec<_> = totals.into_iter().collect();
    let schema = campaign_schema();
    let mut writer = file_writer(&path.join("part-0.parquet"), Arc::clone(&schema), row_group)?;
    for rows in rows.chunks(row_group) {
        let mut campaign = StringBuilder::new();
        let mut channel = StringBuilder::new();
        for &((key, _), _) in rows {
            let row = &catalog.campaigns[key];
            campaign.append_value(std::str::from_utf8(row.id)?);
            channel.append_value(std::str::from_utf8(row.channel)?);
        }
        writer.write(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(campaign.finish()),
                Arc::new(channel.finish()),
                Arc::new(Date32Array::from_iter_values(
                    rows.iter().map(|row| row.0.1),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.1.sessions),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.1.purchasing_sessions),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.1.revenue),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.1.assisted_sessions),
                )),
            ],
        )?)?;
    }
    writer.close()?;
    Ok(())
}

pub fn write(
    output: &Path,
    partitions: &[Daily<'_>],
    catalog: &Catalog<'_>,
    workers: usize,
    row_group: usize,
) -> Result<()> {
    let user_dir = output.join("user_daily");
    let campaign_dir = output.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&campaign_dir)?;
    let profile = std::env::var_os("BENCH_STAGES").is_some_and(|value| value == "1");
    let (users, campaigns) = rayon::join(
        || {
            crate::stages::Stages::span(profile, "user_parquet", || {
                write_users(&user_dir, partitions, catalog, workers, row_group)
            })
        },
        || {
            crate::stages::Stages::span(profile, "campaign_parquet", || {
                write_campaigns(&campaign_dir, partitions, catalog, row_group)
            })
        },
    );
    users?;
    campaigns
}
