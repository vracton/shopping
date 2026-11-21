# Shopping Sessions notes

## Goal and status

- Build a fast, exact, single-container pipeline for the [Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions). The practice leaderboard showed **0.68 s** when checked. Practice has about 20M events and allows two submissions per hour. Finals use about 60M fresh events with a 180 s cap on each run.
- The clock includes process startup, reading input, and writing both Parquet outputs. The scorer has 8 pinned vCPUs and 24 GB RAM. Submission requires a public repository with a root `Dockerfile`. Keep generated data outside the repository.
- The repo has three starter skeletons, a Rust data generator, 36 fixtures per starter, and the public dev answer in `dev-answer/dev-answer/`. The production pipeline is still to be implemented. The local benchmark is in `scripts/bench.py`.

## Correctness to preserve

- Read every CSV shard, including header-only files. The [rules](rules.md) define an LF-only dialect with optional whole-field quotes, exact byte comparisons, significant spaces and carriage returns, and ASCII digits. Normalize the three timestamp forms to UTC milliseconds.
- Resolve links transitively with union-find. Use the smallest ID by bytes as the canonical name, including IDs used only in links.
- Dedup well-formed events globally by event ID and earliest ingest time before validating the winning copy. Later duplicates can name a different user. Validate the retained event and drop it if more than 24 hours late.
- Partition by canonical person, sort by timestamp, and split at gaps over 30 minutes. Group all activity in a session by its UTC start date. Compute the most valuable category across the full person-day, with byte-wise tie-breaking.
- Credit the latest qualifying click in the inclusive seven-day window ending at session start, using the smallest click ID on ties. Count each qualifying campaign once per session for assists. Include assist-only rows and organic credit.
- `user_daily` and `campaign_daily` must contain the exact columns and Parquet `DATE` days. Check all 36 fixtures and the seed-1 public dev set before a scored run. Use other seeds and sizes for performance and differential checks.

## Native implementation direction

1. Start with a native Rust pipeline. Add a fast byte parser for the common cases and retain strict behavior on unusual rows. Intern IDs and keep time and other hot fields as integers. If hashing IDs, resolve collisions by comparing the original bytes.
2. Schedule large input shards in chunks on newline boundaries to balance the workers. Deduplicate by event ID before partitioning by canonical person. Time read/parse, dedup, sort/sessionization, attribution/assists, and Parquet output separately before specializing any stage further.
3. Use a sorted click sweep for last-click credit. For assists, merge overlapping eligibility intervals per person and campaign and count session starts by UTC day within the merged intervals. Benchmark skewed users and shared-device components, where the same person can own a large share of events.
4. Compare against the supplied answers first. Keep straightforward calculations available for differential testing as the production path becomes more specialized.

## Local benchmarks

From the repository root on Linux. The 20M-event, 5M-click mix is a local practice proxy. Use 60M/15M and another seed for a final-scale check.

```sh
cargo build --release --manifest-path generator/generator/Cargo.toml --target-dir /tmp/shopping-generator
/tmp/shopping-generator/release/generate --seed 1 --events 1000000 --clicks 250000 --threads 8 --output /tmp/shopping-dev
/tmp/shopping-generator/release/generate --seed 2 --events 20000000 --clicks 5000000 --threads 8 --output /tmp/shopping-practice
python3 scripts/bench.py scan /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 5
```

When the native executable exists, run the commands below with its actual path. The harness pins itself and its child to the first requested CPUs in its allowed mask, prints individual wall times and the median, and gives each run fresh output and scratch directories. `--expected` invokes the starter comparator with Polars outside the timed interval. Use `--work-dir` to put output and scratch on the filesystem you want to measure.

```sh
python3 scripts/bench.py run /tmp/shopping-dev --expected dev-answer/dev-answer -- ./path/to/pipeline
python3 scripts/bench.py run /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 5 -- ./path/to/pipeline
```

The scan reads all input bytes into reusable buffers. It does no parsing and leaves the OS page cache in its natural state. Report the first observation and the repeated measurements separately. The command benchmark uses host execution with CPU affinity. Use the starter Docker runner as a separate check for scorer-like container limits and startup.

### Initial measurements

Seed 2, 20M events and 5M clicks, **1,901,148,800 bytes across 117 files**, measured on the local Linux/WSL2 workspace. No cache reset was performed. The one-CPU test followed the eight-CPU test on the same files.

| Raw scan | First observation | Median of next 5 |
| --- | ---: | ---: |
| 8 CPUs | 1.533 s | 0.124 s |
| 1 CPU | 0.263 s | 0.152 s |

The repeated scan gives a local I/O reference. The 0.68 s leaderboard score is end to end and measured on different hardware and data.
