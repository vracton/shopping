#!/usr/bin/env python3
"""Interleave local timing runs across native pipeline configurations."""

import argparse
from collections import defaultdict
import csv
import os
from pathlib import Path
from random import Random
from statistics import mean, median, pstdev
import sys
from types import SimpleNamespace

from bench import positive_int, run_pipeline


def parse_candidate(value):
    parts = value.split(",")
    if len(parts) != 5 or parts[0] not in ("sort", "hash"):
        raise argparse.ArgumentTypeError("expected ARCH,CHUNK_KIB,PARTITIONS,PARQUET_ROWS,WORKERS")
    return (parts[0], *(positive_int(part) for part in parts[1:]))


def baseline(arch, workers):
    return (arch, 2048, 32, 65536, workers)


def default_sweep(workers):
    for arch in ("sort", "hash"):
        default = baseline(arch, workers)
        yield default
        dimensions = {
            1: (256, 512, 1024, 4096, 8192),
            2: (7, 16, 64, 128),
            3: (4096, 16384, 262144),
            4: (1, 2, 4),
        }
        for index, values in dimensions.items():
            for value in values:
                if value != default[index] and (index != 4 or value <= workers):
                    yield default[:index] + (value,) + default[index + 1:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path, nargs="+")
    parser.add_argument("--binary", type=Path, default=Path("target/release/shopping-sessions"))
    parser.add_argument("--cpus", type=positive_int, default=8)
    parser.add_argument("--repeats", type=positive_int, default=3)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--candidate", action="append", type=parse_candidate,
                        help="ARCH,CHUNK_KIB,PARTITIONS,PARQUET_ROWS,WORKERS; repeat to compare finalists")
    args = parser.parse_args()

    cpus = sorted(os.sched_getaffinity(0))[:args.cpus]
    os.sched_setaffinity(0, cpus)
    configs = list(dict.fromkeys(args.candidate or default_sweep(len(cpus))))
    if any(config[-1] > len(cpus) for config in configs):
        parser.error("a candidate requests more workers than the CPU mask allows")
    print(f"CPUs {cpus}, {len(configs)} configurations, {args.repeats} repeats", file=sys.stderr)

    def run(data, config):
        arch, chunk, partitions, row_group, workers = config
        settings = {
            "SHOPPING_ARCH": arch,
            "SHOPPING_CHUNK_KIB": str(chunk),
            "SHOPPING_PARTITIONS": str(partitions),
            "SHOPPING_PARQUET_ROWS": str(row_group),
            "BENCH_STAGES": "0",
        }
        paths = SimpleNamespace(data=data, work_dir=args.work_dir, expected=None)
        return run_pipeline(paths, [str(args.binary.resolve())], workers, settings)

    samples = defaultdict(list)
    random = Random(1)
    for data in map(Path.resolve, args.data):
        for arch in sorted({config[0] for config in configs}):
            run(data, baseline(arch, len(cpus)))
        for trial in range(args.repeats):
            order = configs.copy()
            random.shuffle(order)
            for config in order:
                samples[data, config].append(run(data, config))
            print(f"{data.name}: completed repeat {trial + 1}", file=sys.stderr)

    output = csv.writer(sys.stdout)
    output.writerow(["data", "arch", "chunk_kib", "partitions", "parquet_rows", "workers",
                     "mean_s", "median_s", "min_s", "max_s", "stddev_s", "samples_s"])
    for (data, config), times in sorted(samples.items(), key=lambda item: (
            item[0][0], item[0][1][0], median(item[1]))):
        stats = [mean(times), median(times), min(times), max(times), pstdev(times)]
        output.writerow([str(data), *config, *(f"{value:.6f}" for value in stats),
                         " ".join(f"{value:.6f}" for value in times)])


if __name__ == "__main__":
    main()
