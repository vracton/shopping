mod aggregate;
mod catalog;
mod fast_time;
mod ingest;
mod input;
mod output;
mod packed;
mod stages;

use std::env;
use std::error::Error;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use rayon::prelude::*;
use serde_json::Value;

use crate::aggregate::{Daily, aggregate, split_ordered};
use crate::catalog::Catalog;
use crate::ingest::InputRows;
use crate::input::{chunks, map_file, map_shards, read_shards, timestamp};
use crate::stages::Stages;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

enum Architecture {
    Sort,
    Hash,
    Packed,
    Indexed,
}

struct Config {
    data: PathBuf,
    output: PathBuf,
    architecture: Architecture,
    workers: usize,
    chunk_bytes: usize,
    partitions: usize,
    dedup_partitions: usize,
    parquet_rows: usize,
}

impl Config {
    fn from_env() -> Result<Self> {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let workers = positive_setting("NCPU", available.min(8))?;
        let architecture = match env::var("SHOPPING_ARCH").as_deref().unwrap_or("packed") {
            "sort" => Architecture::Sort,
            "hash" => Architecture::Hash,
            "packed" => Architecture::Packed,
            "indexed" => Architecture::Indexed,
            value => {
                return Err(format!(
                    "invalid SHOPPING_ARCH {value:?}, expected indexed, packed, sort or hash"
                )
                .into());
            }
        };
        let (chunk_kib, partitions, parquet_rows) = match architecture {
            Architecture::Sort => (4096, 32, 65_536),
            Architecture::Hash | Architecture::Packed => (2048, 64, 16_384),
            Architecture::Indexed => (1024, 128, 16_384),
        };
        let indexed = matches!(architecture, Architecture::Indexed);
        Ok(Self {
            data: env::var_os("DATA_DIR").map_or_else(|| PathBuf::from("/data"), PathBuf::from),
            output: env::var_os("OUTPUT_DIR")
                .map_or_else(|| PathBuf::from("/output"), PathBuf::from),
            architecture,
            workers,
            chunk_bytes: positive_setting("SHOPPING_CHUNK_KIB", chunk_kib)?
                .checked_mul(1024)
                .ok_or("SHOPPING_CHUNK_KIB is too large")?,
            partitions: positive_setting("SHOPPING_PARTITIONS", partitions)?,
            dedup_partitions: positive_setting(
                "SHOPPING_DEDUP_PARTITIONS",
                if indexed { 512 } else { 64 },
            )?,
            parquet_rows: positive_setting("SHOPPING_PARQUET_ROWS", parquet_rows)?,
        })
    }
}

fn positive_setting(name: &str, default: usize) -> Result<usize> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    value
        .to_str()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| format!("{name} must be a positive integer").into())
}

fn window(path: &Path) -> Result<Range<i64>> {
    let manifest: Value = serde_json::from_reader(File::open(path)?)?;
    let time = |name| {
        manifest
            .get(name)
            .and_then(Value::as_str)
            .and_then(|value| timestamp(value.as_bytes()))
            .ok_or_else(|| format!("invalid {name} in {}", path.display()))
    };
    let start = time("window_start")?;
    let end = time("window_end")?;
    if start >= end {
        return Err("manifest window_start must precede window_end".into());
    }
    Ok(start..end)
}

fn run(config: &Config) -> Result<()> {
    let mut stages = Stages::new();
    let window = window(&config.data.join("manifest.json"))?;
    let links = map_shards(&config.data.join("links"))?;
    let products = map_file(&config.data.join("products.csv"))?;
    let campaigns = map_file(&config.data.join("campaigns.csv"))?;
    let buffered = match env::var("SHOPPING_IO").as_deref().unwrap_or("mmap") {
        "mmap" => false,
        "buffered" => true,
        value => {
            return Err(format!("invalid SHOPPING_IO {value:?}, expected mmap or buffered").into());
        }
    };
    let (event_maps, click_maps) = rayon::join(
        || read_shards(&config.data.join("events"), buffered),
        || read_shards(&config.data.join("clicks"), buffered),
    );
    let (event_maps, click_maps) = (event_maps?, click_maps?);
    stages.mark("open_and_map");
    let input_chunks = |files| {
        if matches!(config.architecture, Architecture::Indexed) {
            input::scheduled_chunks(files, config.chunk_bytes)
        } else {
            chunks(files, config.chunk_bytes)
        }
    };
    let packed = matches!(
        config.architecture,
        Architecture::Packed | Architecture::Indexed
    );
    let ((event_chunks, click_chunks), catalog) = rayon::join(
        || {
            rayon::join(
                || input_chunks(&event_maps[..]),
                || input_chunks(&click_maps[..]),
            )
        },
        || {
            packed.then(|| {
                Catalog::new(
                    std::iter::empty(),
                    products.as_deref().unwrap_or_default(),
                    campaigns.as_deref().unwrap_or_default(),
                )
            })
        },
    );
    let link_chunks = chunks(&links, config.chunk_bytes);
    stages.mark("chunk_boundaries_and_catalog");
    if let Some(catalog) = catalog {
        if let Some(parsed) = packed::parse_inputs(
            &event_chunks,
            &click_chunks,
            &links.iter().map(|mapping| &mapping[..]).collect::<Vec<_>>(),
            &link_chunks,
            &catalog,
            window.clone(),
            packed::PackedPlan {
                partitions: config.partitions,
                dedup_partitions: config.dedup_partitions,
                indexed: matches!(config.architecture, Architecture::Indexed),
                profile: stages.enabled(),
            },
        ) {
            stages.mark("packed_parse_and_links");
            drop((event_chunks, click_chunks, link_chunks));
            let exit = env::var("SHOPPING_EXIT").unwrap_or_default();
            let fast_exit = matches!(exit.as_str(), "kernel" | "unmap");
            let mut resources = Some((event_maps, click_maps, links));
            let bucket_output =
                output::BucketOutput::prepare(&config.output, &catalog, config.parquet_rows)?;
            let aggregate = || {
                parsed.aggregate(
                    config.partitions,
                    catalog.organic,
                    matches!(config.architecture, Architecture::Indexed),
                    &mut stages,
                    bucket_output.as_ref(),
                )
            };
            let (daily, users_written) = if exit == "kernel" {
                aggregate()
            } else {
                rayon::join(|| drop(resources.take()), aggregate).1
            }?;
            stages.mark("packed_order_and_aggregate");
            output::write(
                users_written,
                &config.output,
                &daily,
                &catalog,
                config.workers,
                config.parquet_rows,
            )?;
            stages.mark("parquet");
            if fast_exit {
                // The writers have closed successfully. Let process teardown
                // reclaim the large anonymous and read-only file mappings.
                std::process::exit(0);
            }
            drop(daily);
            stages.mark("drop_aggregates");
            return Ok(());
        }
    }
    let catalog = Catalog::new(
        links.iter().map(|mapping| &mapping[..]),
        products.as_deref().unwrap_or_default(),
        campaigns.as_deref().unwrap_or_default(),
    );
    let input = InputRows {
        event_chunks: &event_chunks,
        click_chunks: &click_chunks,
        catalog: &catalog,
        window,
    };

    let daily: Vec<Daily> = match config.architecture {
        Architecture::Sort => {
            let ordered = input.sort(&mut stages);
            split_ordered(&ordered, config.partitions)
                .into_par_iter()
                .map(|(events, clicks)| aggregate(events, clicks, catalog.organic))
                .collect()
        }
        Architecture::Hash | Architecture::Packed | Architecture::Indexed => input
            .hash(config.partitions, &mut stages)
            .into_par_iter()
            .map(|batch| aggregate(&batch.events, &batch.clicks, catalog.organic))
            .collect(),
    };
    stages.mark("sessions_and_attribution");
    output::write(
        false,
        &config.output,
        &daily,
        &catalog,
        config.workers,
        config.parquet_rows,
    )?;
    stages.mark("parquet");
    Ok(())
}

fn start() -> Result<()> {
    let config = Config::from_env()?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()?
        .install(|| run(&config))
}

fn main() -> ExitCode {
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("shopping-sessions: {error}");
            ExitCode::FAILURE
        }
    }
}
