# Worker image (issue #1989). docs/adr/0006-worker-container-image.md has the
# decision.
#
# The image holds the `harvest` CLI, `harvest-replay` and the reference
# `standalone-runner`. It holds no user workflow. To ship your own worker,
# copy your binary into this image and set `CMD`.
#
# Dependabot updates each digest. docs/audits/worker-image-contract.py checks
# that the build image matches rust-toolchain.toml.

FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS build
# rust-toolchain.toml also asks for rustfmt and clippy. The build uses
# neither, so name the toolchain here and skip that download.
ENV RUSTUP_TOOLCHAIN=1.99.0 \
    CARGO_TERM_COLOR=never
RUN cargo install cargo-auditable --version 0.7.7 --locked
WORKDIR /src
COPY . .
# `cargo auditable` embeds the dependency list, so `cargo audit bin` can scan
# a binary from the image. The cache mounts stay on the builder host.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo auditable build --release --locked \
        -p autumn-harvest-cli -p standalone-runner \
    && mkdir -p /out \
    && cp target/release/harvest target/release/harvest-replay \
        target/release/standalone-runner /out/

# glibc, libgcc and CA certificates. No shell, no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/autumn-foundation/autumn-harvest" \
      org.opencontainers.image.description="Harvest CLI and reference standalone worker" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build /out/harvest /out/harvest-replay /out/standalone-runner /usr/local/bin/
# `harvest migrate run --include-dir` reads these. The binary embeds only the
# core migrations.
COPY autumn-harvest-plugin/migrations/harvest /usr/share/autumn-harvest/migrations/harvest
# The runner binds to localhost by default. A pod must accept probe traffic.
ENV STANDALONE_RUNNER_ADDR=0.0.0.0:8082
USER 65532:65532
EXPOSE 8082
CMD ["standalone-runner"]
