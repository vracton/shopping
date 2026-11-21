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

# Build the stable mimalloc 2.4.5 release independently of Rust PGO.
FROM rust:1.94-bookworm AS allocator
WORKDIR /mimalloc
RUN curl -fsSL --retry 3 https://codeload.github.com/microsoft/mimalloc/tar.gz/cde3f7a0de6de5e243aacd25ba2dbe7fa64d3254 -o /tmp/mimalloc.tgz \
    && echo "f1e9f9d677320d84bcb603ae40d54fd76b0b4b045b2b58b558d48924a1cf5bf7  /tmp/mimalloc.tgz" | sha256sum -c - \
    && tar -xzf /tmp/mimalloc.tgz --strip-components=1 \
    && gcc -std=gnu11 -O3 -DNDEBUG -DMI_SHARED_LIB -DMI_SHARED_LIB_EXPORT -DMI_MALLOC_OVERRIDE \
       -fPIC -fvisibility=hidden -fno-builtin-malloc -shared -I/mimalloc/include -I/mimalloc/src \
       /mimalloc/src/static.c -pthread -o /libmimalloc.so.2

FROM debian:bookworm-slim
ARG SHOPPING_WRITER
ARG SHOPPING_CLAIMS
COPY --from=allocator /libmimalloc.so.2 /usr/local/lib/libmimalloc.so.2
COPY --from=allocator /mimalloc/LICENSE /usr/share/doc/mimalloc/copyright
COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENV LD_PRELOAD=/usr/local/lib/libmimalloc.so.2 \
    MIMALLOC_ALLOW_THP=1 MIMALLOC_EAGER_COMMIT=1 MIMALLOC_EAGER_COMMIT_DELAY=0 \
    MIMALLOC_ARENA_EAGER_COMMIT=1 MIMALLOC_PURGE_DELAY=-1 \
    SHOPPING_ARCH=indexed SHOPPING_DEDUP_MODE=claims SHOPPING_DEDUP_PARTITIONS=128 \
    SHOPPING_LINKS=sparse SHOPPING_ASSISTS=sweep SHOPPING_EXIT=unmap \
    SHOPPING_TIMELINES=tokens SHOPPING_SESSIONIZER=columns \
    SHOPPING_WRITER=${SHOPPING_WRITER} SHOPPING_CLAIMS=${SHOPPING_CLAIMS}
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
