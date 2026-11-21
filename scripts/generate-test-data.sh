#!/bin/sh
set -eu

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:-"$repo/.bench/data"}
threads=${NCPU:-8}
target="$repo/.bench/generator"

cargo build --locked --release --manifest-path "$repo/generator/generator/Cargo.toml" --target-dir "$target"
for seed in 1 2 3; do
    "$target/release/generate" --seed "$seed" --events 1000000 --clicks 250000 --threads "$threads" \
        --output "$output/seed-$seed"
done
