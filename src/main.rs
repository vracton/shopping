mod aggregate;
mod catalog;
mod ingest;
mod input;
mod output;
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
use crate::input::{chunks, map_file, map_shards, timestamp};
use crate::stages::Stages;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

enum Architecture {
    Sort,
    Hash,
}

struct Config {
    data: PathBuf,
    output: PathBuf,
    architecture: Architecture,
    workers: usize,
    chunk_bytes: usize,
    partitions: usize,
    parquet_rows: usize,
}

impl Config {
    fn from_env() -> Result<Self> {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let workers = positive_setting("NCPU", available.min(8))?;
        let architecture = match env::var("SHOPPING_ARCH").as_deref().unwrap_or("hash") {
            "sort" => Architecture::Sort,
            "hash" => Architecture::Hash,
            value => {
                return Err(
                    format!("invalid SHOPPING_ARCH {value:?}, expected sort or hash").into(),
                );
            }
        };
        let (chunk_kib, partitions, parquet_rows) = match architecture {
            Architecture::Sort => (4096, 32, 65_536),
            Architecture::Hash => (2048, 64, 16_384),
        };
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
    let catalog = Catalog::new(
        links.iter().map(|mapping| &mapping[..]),
        products.as_deref().unwrap_or_default(),
        campaigns.as_deref().unwrap_or_default(),
    );
    let event_maps = map_shards(&config.data.join("events"))?;
    let click_maps = map_shards(&config.data.join("clicks"))?;
    let event_chunks = chunks(&event_maps, config.chunk_bytes);
    let click_chunks = chunks(&click_maps, config.chunk_bytes);
    stages.mark("setup");
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
        Architecture::Hash => input
            .hash(config.partitions, &mut stages)
            .into_par_iter()
            .map(|batch| aggregate(&batch.events, &batch.clicks, catalog.organic))
            .collect(),
    };
    stages.mark("sessions_and_attribution");
    output::write(
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
