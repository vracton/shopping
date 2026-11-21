# Shopping Sessions notes

## Goal and status

- Build a fast, exact, single-container pipeline for the [Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions). The practice leaderboard showed **0.68 s** when checked. Practice has about 20M events and allows two submissions per hour. Finals use about 60M fresh events with a 180 s cap on each run.
- The clock includes process startup, reading input, and writing both Parquet outputs. The scorer has 8 pinned vCPUs and 24 GB RAM. Submission requires a public repository with a root `Dockerfile`. Keep generated data outside the repository.
- The local challenge kit has three starter skeletons, 36 fixtures per starter, and the public dev answer in `dev-answer/dev-answer/`. Those downloads and generated CSVs are excluded from Git. The Rust generator source, native container scaffold, and benchmark tools are tracked. The scaffold exits with an explicit error until the pipeline is implemented.

## Correctness to preserve

- Read every CSV shard, including header-only files. The [rules](rules.md) define an LF-only dialect with optional whole-field quotes, exact byte comparisons, significant spaces and carriage returns, and ASCII digits. Normalize the three timestamp forms to UTC milliseconds.
- Resolve links transitively with union-find. Use the smallest ID by bytes as the canonical name, including IDs used only in links.
- Dedup well-formed events globally by event ID and earliest ingest time before validating the winning copy. Later duplicates can name a different user. Validate the retained event and drop it if more than 24 hours late.
- Partition by canonical person, sort by timestamp, and split at gaps over 30 minutes. Group all activity in a session by its UTC start date. Compute the most valuable category across the full person-day, with byte-wise tie-breaking.
- Credit the latest qualifying click in the inclusive seven-day window ending at session start, using the smallest click ID on ties. Count each qualifying campaign once per session for assists. Include assist-only rows and organic credit.
- `user_daily` and `campaign_daily` must contain the exact columns and Parquet `DATE` days. Check all 36 fixtures and the seed-1 public dev set before a scored run. Use other seeds and sizes for performance and differential checks.

## Architecture options

1. **Parallel sort baseline.** Resolve links with union-find. Map or read CSV shards in newline-aligned chunks and parse byte slices into compact records. Sort globally by `(event_id, ingest_ts)` to select the dedup winners, then sort the surviving events by `(canonical_person, timestamp)`. This makes correctness straightforward to inspect and provides stage timings for the next two options. Two comparison sorts can drive memory traffic and cache misses at 20M and 60M.
2. **Hash-partition dedup and person-local sort.** Send all versions of an event ID to the same partition, compare full key bytes on collisions, and keep the earliest well-formed copy even when it fails validation. Repartition winners by canonical person and sort each partition by person and time. This keeps the global event-ID sort out of the pipeline. The tradeoffs are a scatter between stages and potential skew from whales and shared-device components. Use more work partitions than workers and account for the largest person explicitly if it becomes a bottleneck.
3. **Packed keys and radix partitioning.** Use dense IDs and window-relative milliseconds where a checked representation fits. For generated-length IDs, try inline keys with an exact path for longer IDs and unusual byte values. Packed keys can make sorting and partitioning more cache-friendly. Use stage timings and 20M-scale evidence to justify this extra complexity. Keep full byte equality and byte-wise tie-breaking.

For I/O, compare memory mapping with buffered parallel reads. Mapping allows byte slices to borrow input directly, while buffered reads give explicit control over scheduling and read-ahead. On either path, split large shards on newline boundaries so one worker does not inherit the largest file. Time read/parse, dedup, event ordering/sessionization, attribution/assists, and Parquet output separately. Keep pools within eight logical CPUs and measure peak RSS as well as elapsed time. One 56-byte record array at 20M rows is about 1.12 GB before duplicates and spare capacity. Budget for concurrent buffers, indexes, output writers, and the 60M final as well.

For attribution, start with a sorted last-click sweep. For assists, compare a rolling active-campaign set with merging the eligibility intervals per person and campaign. Merged intervals can count session starts per day by index range, which avoids a row for every session-click combination.

## Test plan before optimization

- Unit-test strict CSV and timestamp parsing, leading zeros, malformed versus invalid dedup winners, link chains with a link-only canonical ID, and the 30-minute and seven-day boundaries at millisecond precision.
- Run all 36 fixtures with the starter harness, then compare seed 1 against the public answer with the exact Parquet comparator. Include empty-table output and assist-only campaigns.
- Compare optimized code with a straightforward implementation on small randomized inputs. In particular, check interval assists against an explicit seven-day scan and compare the whole pipeline across input shard orders and thread counts. Seeds 2 and 3 have no supplied full-size answers. Use exact differential checks when the reference is available, and check rollup invariants in the meantime.
- Use the three 1M seeds for the routine timing loop. Track first observation, mean, median, spread, and stage timings. Use the same CPU affinity for comparisons. Reserve 20M and 60M runs for scaling milestones and leaderboard runs for verified candidates.

## Local benchmarks

From the repository root on Linux, regenerate the three routine datasets with `scripts/generate-test-data.sh`. Each seed requests 1M events and 250K clicks before the generator adds duplicates and malformed rows. The script defaults to `.bench/data/`.

```sh
scripts/generate-test-data.sh
for seed in 1 2 3; do
    python3 scripts/bench.py scan ".bench/data/seed-$seed" --cpus 8 --warmups 1 --repeats 5
done
```

The 20M-event, 5M-click mix below is a local practice proxy for scaling milestones. Use 60M/15M and another seed for a final-scale check.

```sh
cargo build --release --manifest-path generator/generator/Cargo.toml --target-dir /tmp/shopping-generator
/tmp/shopping-generator/release/generate --seed 2 --events 20000000 --clicks 5000000 --threads 8 --output /tmp/shopping-practice
python3 scripts/bench.py scan /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 5
```

When the executable implements the pipeline, run the commands below. The harness pins itself and its child to the first requested CPUs in its allowed mask, prints individual wall times and summary statistics, and gives each run fresh output and scratch directories. Use `taskset` to select a specific CPU mask before launching the harness. `--expected` invokes the starter comparator with Polars outside the timed interval. Use `--work-dir` to put output and scratch on the filesystem you want to measure.

```sh
cargo build --release --locked
python3 scripts/bench.py run .bench/data/seed-1 --expected dev-answer/dev-answer -- ./target/release/shopping-sessions
python3 scripts/bench.py run /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 5 -- ./target/release/shopping-sessions
```

The scan reads all input bytes into reusable buffers. It does no parsing and leaves the OS page cache in its natural state. Report the first observation and the repeated measurements separately. The command benchmark uses host execution with CPU affinity. Use the starter Docker runner as a separate check for scorer-like container limits and startup.

### Initial measurements

Three 1M seeds on CPUs 0–7 in the local WSL2 workspace. These eight logical CPUs are paired siblings on four physical cores here. Each row reports the first scan and statistics for the next five. No cache reset was performed.

| Seed | Input | First scan | Mean | Median | Min–max |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 96.21 MB | 0.276 s | 0.055 s | 0.037 s | 0.037–0.124 s |
| 2 | 95.95 MB | 0.222 s | 0.060 s | 0.043 s | 0.041–0.132 s |
| 3 | 96.87 MB | 0.209 s | 0.059 s | 0.046 s | 0.044–0.113 s |

Seed 2, 20M events and 5M clicks, **1,901,148,800 bytes across 117 files**, measured on the local Linux/WSL2 workspace. No cache reset was performed. The one-CPU test followed the eight-CPU test on the same files.

| Raw scan | First observation | Median of next 5 |
| --- | ---: | ---: |
| 8 CPUs | 1.533 s | 0.124 s |
| 1 CPU | 0.263 s | 0.152 s |

The repeated scan gives a local I/O reference. The 0.68 s leaderboard score is end to end and measured on different hardware and data.
