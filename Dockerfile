FROM rust:1.94-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
ARG TARGETARCH
RUN if [ "$TARGETARCH" = amd64 ]; then \
      cargo rustc --release --locked -- -C target-cpu=x86-64-v3; \
    else \
      cargo build --release --locked; \
    fi

FROM debian:bookworm-slim
COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
