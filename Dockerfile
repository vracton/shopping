ARG SHOPPING_WRITER=bucket
ARG SHOPPING_CLAIMS=screen
FROM rust:1.94-bookworm AS build
WORKDIR /app
RUN rustup component add llvm-tools-preview
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY generator/generator/ generator/
ARG TARGETARCH
ARG SHOPPING_WRITER
ARG SHOPPING_CLAIMS
ENV CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
RUN set -eu; \
    if [ "$TARGETARCH" = amd64 ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --locked --release --manifest-path generator/Cargo.toml --target-dir /tmp/gen; \
    cargo rustc --locked --release -- -C profile-generate=/tmp/profile; \
    mkdir -p /tmp/profile; \
    for seed in 1 3; do \
      /tmp/gen/release/generate --seed "$seed" --events 1000000 --clicks 250000 --threads 8 --output "/tmp/input-$seed"; \
      for mode in filter claims; do \
        DATA_DIR="/tmp/input-$seed" OUTPUT_DIR=/tmp/output NCPU=8 \
        SHOPPING_ARCH=indexed SHOPPING_DEDUP_MODE="$mode" SHOPPING_LINKS=sparse \
        SHOPPING_ASSISTS=sweep SHOPPING_TIMELINES=tokens SHOPPING_SESSIONIZER=columns \
        SHOPPING_CLAIMS="$SHOPPING_CLAIMS" SHOPPING_WRITER="$SHOPPING_WRITER" SHOPPING_EXIT=unmap \
        LLVM_PROFILE_FILE=/tmp/profile/%p-%m.profraw target/release/shopping-sessions; \
      done; \
    done; \
    host=$(rustc -vV | sed -n 's/^host: //p'); \
    "$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata" merge /tmp/profile -o /tmp/training.profdata; \
    cargo rustc --locked --release -- -C profile-use=/tmp/training.profdata

# The build stage above produces the shipping executable from unchanged source.
# The optional scanner has its own profile and shares the training inputs.
FROM build AS wide
COPY wide/scanner.patch wide/launch.rs /tmp/wide/
COPY wide/isa_wide.rs src/isa_wide.rs
RUN set -eu; \
    rustc -O -C strip=symbols /tmp/wide/launch.rs -o /tmp/shopping-launch; \
    git apply /tmp/wide/scanner.patch; \
    if [ "$TARGETARCH" = amd64 ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    rm -rf /tmp/profile; \
    cargo rustc --locked --release -- -C profile-generate=/tmp/profile; \
    mkdir -p /tmp/profile; \
    for seed in 1 3; do \
      for mode in filter claims; do \
        DATA_DIR="/tmp/input-$seed" OUTPUT_DIR="/tmp/wide-$seed-$mode" NCPU=8 \
        SHOPPING_ARCH=indexed SHOPPING_DEDUP_MODE="$mode" SHOPPING_LINKS=sparse \
        SHOPPING_ASSISTS=sweep SHOPPING_TIMELINES=tokens SHOPPING_SESSIONIZER=columns \
        SHOPPING_CLAIMS="$SHOPPING_CLAIMS" SHOPPING_WRITER="$SHOPPING_WRITER" SHOPPING_EXIT=unmap \
        LLVM_PROFILE_FILE=/tmp/profile/%p-%m.profraw target/release/shopping-sessions; \
      done; \
    done; \
    host=$(rustc -vV | sed -n 's/^host: //p'); \
    "$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata" merge /tmp/profile -o /tmp/wide.profdata; \
    cargo rustc --locked --release -- -C profile-use=/tmp/wide.profdata

FROM debian:bookworm-slim
ARG SHOPPING_WRITER
ARG SHOPPING_CLAIMS
RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions-stock
COPY --from=wide /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions-wide
COPY --from=wide /tmp/shopping-launch /usr/local/bin/shopping-sessions
ENV LD_PRELOAD=libjemalloc.so.2 \
    MALLOC_CONF=thp:always,metadata_thp:always,dirty_decay_ms:-1,muzzy_decay_ms:-1 \
    SHOPPING_ARCH=indexed SHOPPING_DEDUP_MODE=claims SHOPPING_DEDUP_PARTITIONS=128 \
    SHOPPING_LINKS=sparse SHOPPING_ASSISTS=sweep SHOPPING_EXIT=unmap \
    SHOPPING_TIMELINES=tokens SHOPPING_SESSIONIZER=columns \
    SHOPPING_WRITER=${SHOPPING_WRITER} SHOPPING_CLAIMS=${SHOPPING_CLAIMS}
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
