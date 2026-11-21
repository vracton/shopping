#!/usr/bin/env python3
"""Benchmark input reads and pipeline runtimes on Linux."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from statistics import mean, median, pstdev
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import local
from time import perf_counter


MIB = 1024 ** 2
GIB = 1024 ** 3
RANGE_BYTES = 32 * MIB
BUFFER_BYTES = 2 * MIB
COMPARE = (Path(__file__).resolve().parents[1] / "starter-duckdb" / "starter-duckdb"
           / "scripts" / "compare.py")


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_args(argv):
    split = argv.index("--") if "--" in argv else len(argv)
    command = argv[split + 1:]
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("data", type=Path)
    common.add_argument("--cpus", type=positive_int, default=8)
    common.add_argument("--repeats", type=positive_int, default=3)
    common.add_argument("--warmups", type=int, default=0)

    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    modes.add_parser("scan", parents=[common], help="read every input byte")
    run = modes.add_parser("run", parents=[common], help="time a command supplied after --")
    run.add_argument("--work-dir", type=Path, help="parent directory for temporary output and scratch")
    run.add_argument("--expected", type=Path, help="compare each result outside the timed interval (requires Polars)")
    args = parser.parse_args(argv[:split])
    if args.warmups < 0:
        parser.error("--warmups must be nonnegative")
    if (args.mode == "run") != bool(command):
        parser.error("run requires a command after --, scan takes none")
    return args, command


def input_files(data):
    files = [data / name for name in ("manifest.json", "products.csv", "campaigns.csv")]
    for folder in ("events", "clicks", "links"):
        if not (data / folder).is_dir():
            raise FileNotFoundError(data / folder)
        files.extend(path for path in (data / folder).glob("*.csv") if path.is_file())
    return [(path, path.stat().st_size) for path in files]


def scan(files, workers):
    # Split large shards so a single file cannot leave the other workers idle.
    ranges = [(path, offset, min(RANGE_BYTES, size - offset))
              for path, size in sorted(files, key=lambda item: -item[1])
              for offset in range(0, size, RANGE_BYTES)]
    thread = local()

    def read_range(item):
        path, offset, size = item
        with path.open("rb", buffering=0) as source:
            remaining = size
            while remaining:
                count = os.preadv(source.fileno(), [thread.buffer[:remaining]], offset)
                if count == 0:
                    raise OSError(f"unexpected EOF in {path} at byte {offset}")
                remaining -= count
                offset += count
        return size

    def allocate_buffer():
        thread.buffer = memoryview(bytearray(BUFFER_BYTES))

    start = perf_counter()
    with ThreadPoolExecutor(max_workers=workers, initializer=allocate_buffer) as pool:
        read_bytes = sum(pool.map(read_range, ranges))
    return perf_counter() - start, read_bytes


def run_pipeline(args, command, workers, overrides=None):
    with TemporaryDirectory(prefix="shopping-bench-", dir=args.work_dir) as directory:
        output = Path(directory) / "output"
        scratch = Path(directory) / "scratch"
        output.mkdir()
        scratch.mkdir()
        env = dict(os.environ, DATA_DIR=str(args.data), OUTPUT_DIR=str(output),
                   SCRATCH_DIR=str(scratch), HOME=str(scratch), NCPU=str(workers), MEM_GB="24")
        env.update(overrides or {})
        start = perf_counter()
        subprocess.run(command, env=env, check=True)
        elapsed = perf_counter() - start
        if args.expected:
            subprocess.run([sys.executable, str(COMPARE), str(output), str(args.expected)], check=True)
    return elapsed


def main():
    args, command = parse_args(sys.argv[1:])
    args.data = args.data.resolve()
    files = input_files(args.data)
    total_bytes = sum(size for _, size in files)
    cpus = sorted(os.sched_getaffinity(0))[:args.cpus]
    os.sched_setaffinity(0, cpus)
    print(f"{args.data}: {total_bytes:,} bytes across {len(files)} files, CPUs {cpus}", flush=True)
    samples = []
    for index in range(args.warmups + args.repeats):
        if args.mode == "scan":
            elapsed, read_bytes = scan(files, len(cpus))
            detail = f", {read_bytes / elapsed / GIB:.2f} GiB/s"
        else:
            elapsed = run_pipeline(args, command, len(cpus))
            detail = ""
        warmup = index < args.warmups
        label = f"warmup {index + 1}" if warmup else f"run {index + 1 - args.warmups}"
        print(f"{label}: {elapsed:.3f} s{detail}", flush=True)
        if not warmup:
            samples.append(elapsed)
    print(f"mean: {mean(samples):.3f} s, median: {median(samples):.3f} s, "
          f"min: {min(samples):.3f} s, max: {max(samples):.3f} s, "
          f"stddev: {pstdev(samples):.3f} s")


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
