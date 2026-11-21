# Shopping Sessions notes

## Goal and status

- Build a fast, exact, single-container pipeline for the [Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions). The practice leaderboard showed **0.68 s** when checked. Practice has about 20M events and allows two submissions per hour. Finals use about 60M fresh events with a 180 s cap on each run.
- The clock includes process startup, reading input, and writing both Parquet outputs. The scorer has 8 pinned vCPUs and 24 GB RAM. Submission requires a public repository with a root `Dockerfile`. Keep generated data outside the repository.
- The local challenge kit has three starter skeletons, 36 fixtures per starter, and the public dev answer in `dev-answer/dev-answer/`. Those downloads and generated CSVs are excluded from Git. The Rust pipeline implements both global sort and hash-partition dedup behind the same entry point and shares validation, attribution, and Parquet writing.
- The default is `packed` with 2 MiB parse chunks, 64 event dedup buckets, 64 person partitions, and 16,384 Parquet rows per group. `SHOPPING_ARCH=hash` and `SHOPPING_ARCH=sort` select the byte-slice baselines. The packed path falls back to hash if an ID, window, or category dimension needs a wider representation.

## Correctness to preserve

- Read every CSV shard, including header-only files. The [rules](rules.md) define an LF-only dialect with optional whole-field quotes, exact byte comparisons, significant spaces and carriage returns, and ASCII digits. Normalize the three timestamp forms to UTC milliseconds.
- Resolve links transitively with union-find. Use the smallest ID by bytes as the canonical name, including IDs used only in links.
- Dedup well-formed events globally by event ID and earliest ingest time before validating the winning copy. Later duplicates can name a different user. Validate the retained event and drop it if more than 24 hours late.
- Partition by canonical person, sort by timestamp, and split at gaps over 30 minutes. Group all activity in a session by its UTC start date. Compute the most valuable category across the full person-day, with byte-wise tie-breaking.
- Credit the latest qualifying click in the inclusive seven-day window ending at session start, using the smallest click ID on ties. Count each qualifying campaign once per session for assists. Include assist-only rows and organic credit.
- `user_daily` and `campaign_daily` must contain the exact columns and Parquet `DATE` days. Check all 36 fixtures and the seed-1 public dev set before a scored run. Use other seeds and sizes for performance and differential checks.

## Architecture options

1. **Parallel sort baseline (`SHOPPING_ARCH=sort`).** Resolve links with union-find, map CSV shards, and parse newline-aligned chunks in parallel. Sort globally by `(event_id, ingest_ts)` to select the dedup winners, then sort the surviving events by `(canonical_person, timestamp)`. Two comparison sorts can drive memory traffic and cache misses at 20M and 60M.
2. **Hash-partition dedup and person-local sort (`SHOPPING_ARCH=hash`).** Send all versions of an event ID to the same partition, compare full key bytes on collisions, and keep the earliest well-formed copy even when it fails validation. Repartition winners by canonical person and sort each partition by person and time. The tradeoffs are a scatter between stages and potential skew from whales and shared-device components.
3. **Packed keys and radix partitioning.** Use dense IDs and window-relative milliseconds where a checked representation fits. For generated-length IDs, try inline keys with an exact path for longer IDs and unusual byte values. Packed keys can make sorting and partitioning more cache-friendly. Use stage timings and 20M-scale evidence to justify this extra complexity. Keep full byte equality and byte-wise tie-breaking.

For I/O, compare memory mapping with buffered parallel reads. Mapping allows byte slices to borrow input directly, while buffered reads give explicit control over scheduling and read-ahead. On either path, split large shards on newline boundaries so one worker does not inherit the largest file. Time read/parse, dedup, event ordering/sessionization, attribution/assists, and Parquet output separately. Keep pools within eight logical CPUs and measure peak RSS as well as elapsed time. One 56-byte record array at 20M rows is about 1.12 GB before duplicates and spare capacity. Budget for concurrent buffers, indexes, output writers, and the 60M final as well.

Attribution uses a sorted last-click sweep and merged eligibility intervals per person and campaign. Merged intervals count session starts per day by index range, which avoids a row for every session-click combination. The randomized unit test compares both credit and assists against a direct click scan.

## Test plan before optimization

- Unit-test strict CSV and timestamp parsing, leading zeros, malformed versus invalid dedup winners, link chains with a link-only canonical ID, and the 30-minute and seven-day boundaries at millisecond precision.
- Run all 36 fixtures with the starter harness, then compare seed 1 against the public answer with the exact Parquet comparator. Include empty-table output and assist-only campaigns.
- Compare optimized code with a straightforward implementation on small randomized inputs. In particular, check interval assists against an explicit seven-day scan and compare the whole pipeline across input shard orders and thread counts. Seeds 2 and 3 have no supplied full-size answers. Use exact differential checks when the reference is available, and check rollup invariants in the meantime.
- Use the three 1M seeds for the routine timing loop. Track first observation, mean, median, spread, and stage timings. Use the same CPU affinity for comparisons. Reserve 20M and 60M runs for scaling milestones and leaderboard runs for verified candidates.

Current verification passed 216 fixture runs, five Rust unit tests, and exact output comparisons across both paths on the three 1M seeds and a 20M proxy. Reversing rows and redistributing seed 3 across 17 shards per input type preserved both outputs. The default `linux/amd64` image matched the public seed-1 answer with the starter's scorer-like run flags.

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

For pipeline measurements, the harness pins itself and its child to the first requested CPUs in its allowed mask, prints individual wall times and summary statistics, and gives each run fresh output and scratch directories. Use `taskset` to select a specific CPU mask before launching the harness. `--expected` invokes the starter comparator with Polars outside the timed interval. Use `--work-dir` to put output and scratch on the filesystem you want to measure.

```sh
cargo build --release --locked
scripts/check-native.sh
python3 scripts/bench.py run .bench/data/seed-1 --expected dev-answer/dev-answer -- ./target/release/shopping-sessions
python3 scripts/bench.py run /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 5 -- ./target/release/shopping-sessions
```

`check-native.sh` checks all three architectures on the 36 fixtures with three settings, compares their outputs on seeds 1–3, and compares seed 1 to the public answer. Its generated-input checks exercise buffered reads on the packed path. It needs the downloaded DuckDB starter files and Polars. Set `PYTHON`, `STARTER_DIR`, `DATA_ROOT`, or `PUBLIC_ANSWER` to select a different environment or dataset location.

Set `BENCH_STAGES=1` for timings by stage. The performance settings are `SHOPPING_CHUNK_KIB`, `SHOPPING_PARTITIONS`, `SHOPPING_PARQUET_ROWS`, and `NCPU`. `sweep-native.py` warms the data and interleaves candidates in seeded order. It takes repeated `--candidate ARCH,CHUNK_KIB,PARTITIONS,PARQUET_ROWS,WORKERS` options to recheck finalists.

```sh
python3 scripts/sweep-native.py .bench/data/seed-2 --repeats 4 > .bench/sweep.csv
python3 scripts/sweep-native.py .bench/data/seed-1 .bench/data/seed-2 .bench/data/seed-3 --repeats 5 \
    --candidate sort,4096,32,65536,8 --candidate hash,2048,64,16384,8 \
    --candidate hash,2048,64,65536,8 > .bench/finalists.csv
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

### Native pipeline measurements

The one-factor sweep tried 32 configurations on seed 2 with four repeats per configuration. It varied chunk size from 256 KiB to 8 MiB, partitions from 7 to 128, row groups from 4,096 to 262,144 rows, and worker count from one to eight. Confirmation used eight candidates, three seeds, and five interleaved repeats per seed. All numbers below are host process wall time on CPUs 0–7. The pooled summaries use all 15 runs per candidate.

| Architecture | Chunk | Partitions | Parquet rows | Mean | Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sort | 4 MiB | 32 | 65,536 | 436 ms | 422 ms |
| Hash | 2 MiB | 64 | 16,384 | 214 ms | 209 ms |
| Hash | 2 MiB | 64 | 65,536 | 213 ms | 216 ms |

The finalists also ran three times each on the existing 20M-event seed-2 proxy. Results varied substantially on the local WSL2 host, especially during the first two runs. Use these as scale and memory checks. They have no controlled cache reset, and the local machine has about 8 GiB RAM.

| Architecture and row group | Individual wall times | Median | Peak RSS across runs |
| --- | --- | ---: | ---: |
| Sort, 65,536 | 27.20 s, 14.40 s, 15.94 s | 15.94 s | 4.26 GiB |
| Hash, 16,384 | 27.09 s, 6.80 s, 6.79 s | 6.80 s | 4.39 GiB |
| Hash, 65,536 | 7.02 s, 9.30 s, 6.62 s | 7.02 s | 4.38 GiB |

In the 6.79 s hash run, parsing and partitioning took 1.80 s, dedup and redistribution 1.30 s, local ordering 1.74 s, and Parquet writing 0.14 s. Parsing and ordering are the first places to investigate for further speedups. The 60M final scale has yet to be measured.

### Optimization audit

- Prioritize compact, exact ID keys and contiguous per-person timelines. Replace repeated byte-slice comparisons with integer grouping and sort timestamps within each canonical person's range. Keep an exact representation for long or unusual IDs.
- Narrow the records that cross the event dedup stage, reuse the partition hash when inserting into the dedup table, and measure the bytes copied by per-chunk buffers, repartitioning, and flattening. Global dedup and earliest-invalid-wins behavior are required.
- Prototype a fused delimiter and quote scan and a manifest-derived calendar table for timestamp conversion. Keep dialect, validity, and UTC-window checks. `ingest_ts` alone is guaranteed valid F1, so it can use a specialized decoder.
- The 6.79 s run spent 0.98 s in setup and 0.41 s in sessions and attribution. Two later, much slower instrumented runs attributed almost all setup time to catalog construction and measured 1.18–1.99 s in post-write cleanup. The raw input scan varied from 0.37 s in the previous warmed run to 4.36 s in the audit. These runs illustrate host and cache variability. Test overlap of catalog construction with raw parsing and compare input methods after records stop borrowing mapped pages.
- Isolated A/B probes tried the trusted F1 ingest decoder, skipping the identical-click dedup pass, and masking power-of-two partition indexes. All three passed the 36 fixtures and the public answer. None produced a repeatable end-to-end gain across seven interleaved trials on each of the three 1M seeds. Identical click copies leave the last-click winner and the union of assist intervals unchanged, so their separate dedup pass is optional for correctness.

### Packed path and the 3.5 s target

The packed path uses exact, order-preserving short ASCII keys and 16-byte events. It resolves links in parallel with parsing, reuses the alias map from union-find, and releases input buffers while deduplication runs. The scanner splits fields and verifies quotes with byte masks. Manifest-relative timestamps use a date table, and fixed-format ingest timestamps compare as sortable integers. Dedup uses compact hash tables with full key equality and retains invalid earliest copies. Person partitions sort integers, the aggregation loop accumulates counts and revenue while detecting gaps, and campaign-day counters use a dense table when the dimensions fit. Identical click copies need no standalone pass. Arrow preallocates output builders and campaign rows are emitted without sorting.

Local 20M-event results on the 1.90 GB practice proxy with eight logical CPUs and the release binary compiled for `x86-64-v3`:

| Input and CPU affinity | Measured wall times | Median |
| --- | --- | ---: |
| File-backed mmap, four physical cores with both siblings | 2.415, 2.445, 2.228 s | 2.415 s |
| File-backed mmap, eight physical cores | 5.154, 2.436, 2.082 s | 2.436 s |
| tmpfs mmap, eight physical cores | 2.710, 2.403, 2.663, 2.694 s | 2.679 s |

Each file-backed series followed a warmup. The first warmup on the spread CPU mask took 10.590 s. Cache state was uncontrolled, and neither these times nor the tmpfs diagnostic represent an official scorer result. The tmpfs sweep compared 64 and 256 dedup buckets and 2 and 8 MiB chunks. The selected 64-bucket, 2 MiB combination had the most consistent times. File-backed mmap had better local results than buffering entire shards, so `SHOPPING_IO=buffered` remains opt-in. Both modes keep all intermediate records in memory. Final-scale 60M performance remains unmeasured.

The local Docker image matched the 20M sort reference exactly, including all 2,240,415 user-day rows and 28,014 campaign-day rows. Docker runs pinned to eight CPUs with a 24 GB container limit on the 8 GiB WSL2 guest took 3.842–15.133 s on file-backed input. The later tmpfs Docker diagnostics were also slower than the host-process benchmark. The 3.5 s target is verified for the repeated host-process benchmark. The user reported 1.78 s on the official scorer. All three original architectures passed the 36 fixtures under three configurations, the three 1M seeds matched across architectures, and seed 1 matched the public answer.

On the representative 2.415 s run, parsing and links took 0.923 s, dedup and redistribution 0.695 s, ordering and aggregation 0.357 s, and Parquet 0.148 s. To reproduce on a scorer-compatible x86-64 host:

```sh
cargo rustc --release --locked -- -C target-cpu=x86-64-v3
BENCH_STAGES=1 python3 scripts/bench.py run /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 3 -- ./target/release/shopping-sessions
```

### Experiments after the 1.78 s submission

The reported comparison was 1.78 s for our submission and 1.05 s for Xero. Xero's public code suggested smaller dedup tables and sorting timestamps within each person's group. The external checkout and benchmark outputs are kept outside the repository.

| Stage | Experiment | Observed outcome |
| --- | --- | --- |
| Input and parsing | Row-oriented SIMD scanner, exact date atoms, BCD ingest comparisons, and SWAR decimal timestamps | All fixtures and the public answer matched. One filtered run spent 0.505 s in the event decode and scatter span. Overlapping alias work took 0.648 s and total parsing took 0.656 s. |
| Dedup and alias routing | Small index table with loser marking, a collision bitmap that escalates ambiguous IDs to an exact table, and an optional parallel union-find | In a comparable faster run, filtered dedup and routing took 0.383 s versus 0.575 s for the packed baseline. The filtered run lost the gain in later stages and overall. |
| Ordering, sessions, and attribution | Append-only linked timelines, counted contiguous timelines, and temporal bins with width 30 minutes + 1 ms | Linked timelines were slower in the initial comparison. Temporal bins avoid sorting event timestamps for large shoppers and produced exact user and campaign tables, including the 20M reference. The randomized test compares them against sorted sessions. |
| Payload routing | Place valid payloads in shopper partitions at decode time. A separate event-ID index uses atomic tombstones for exact duplicates, including invalid earliest copies. | Passed the 36 fixtures, public answer, and full 20M reference. Its profiled run took 13.22 s under variable WSL2 conditions, with no demonstrated end-to-end win. |
| Output and exit | Profile user and campaign output separately, try uncompressed Parquet and kernel cleanup after writers close | Both options preserved exact output. Local measurements did not establish a repeatable total-time win. The allocator experiment also showed no repeatable win and was removed. |

The fastest observed local 20M time in this sweep was 1.688 s with the previous packed binary. The filtered candidate reached 1.727 s, and other runs varied substantially on the same input and CPU mask. The sub-second target was not reached. Keep `packed` as the default and select `indexed` explicitly to benchmark the experiments. `BENCH_STAGES=1` shows non-overlapping wall stages and separate `overlap` spans for concurrent work.

```sh
SHOPPING_ARCH=indexed SHOPPING_ROUTE=split SHOPPING_LINKS=parallel BENCH_STAGES=1 \
    python3 scripts/bench.py run /tmp/shopping-practice --cpus 8 --warmups 1 --repeats 3 -- ./target/release/shopping-sessions
```
