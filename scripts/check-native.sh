#!/bin/sh
set -eu

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
python=${PYTHON:-python3}
starter=${STARTER_DIR:-starter-duckdb/starter-duckdb}
data=${DATA_ROOT:-.bench/data}
answer=${PUBLIC_ANSWER:-dev-answer/dev-answer}
binary=${PIPELINE_BIN:-"$repo/target/release/shopping-sessions"}
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

run_pipeline() {
    DATA_DIR="$2" OUTPUT_DIR="$3" NCPU="$4" SHOPPING_ARCH="$1" \
        SHOPPING_CHUNK_KIB="$5" SHOPPING_PARTITIONS="$6" SHOPPING_PARQUET_ROWS="$7" "$binary"
}

for arch in sort hash packed; do
    # Exercise single-bucket, single-thread execution and non-power-of-two sizing.
    for configuration in '8 2048 32 65536' '1 1 1 1' '4 7 7 17'; do
        set -- $configuration
        printf '%s fixtures (workers, chunk KiB, partitions, row group: %s)\n' "$arch" "$configuration"
        NCPU="$1" SHOPPING_ARCH="$arch" SHOPPING_CHUNK_KIB="$2" SHOPPING_PARTITIONS="$3" SHOPPING_PARQUET_ROWS="$4" \
            "$python" "$starter/scripts/run_fixtures.py" "$starter/fixtures" -- "$binary" > "$temporary/fixtures.log" || {
                cat "$temporary/fixtures.log"
                exit 1
            }
        tail -1 "$temporary/fixtures.log"
    done
done

for seed in 1 2 3; do
    printf 'Cross-checking seed %s\n' "$seed"
    left="$temporary/seed-$seed-sort"
    right="$temporary/seed-$seed-hash"
    packed="$temporary/seed-$seed-packed"
    run_pipeline sort "$data/seed-$seed" "$left" 8 2048 32 65536
    run_pipeline hash "$data/seed-$seed" "$right" 4 512 7 4096
    "$python" "$starter/scripts/compare.py" "$left" "$right"
    SHOPPING_IO=buffered SHOPPING_DEDUP_PARTITIONS=64 run_pipeline packed "$data/seed-$seed" "$packed" 8 2048 64 16384
    "$python" "$starter/scripts/compare.py" "$left" "$packed"
    if [ "$seed" = 1 ]; then
        "$python" "$starter/scripts/compare.py" "$left" "$answer"
    fi
done
