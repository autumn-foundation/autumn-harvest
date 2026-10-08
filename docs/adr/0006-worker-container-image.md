# ADR 0006: The worker container image and Helm chart

## Status

Accepted (issue #1989).

## Context

- Harvest is a library. A workflow is Rust code that compiles into the
  binary of the user. An image cannot hold the workflows of a user. See
  [ADR 0002](0002-rust-native-execution-boundary.md).
- An operator still needs the CLI. It applies migrations, gates a deploy
  with `harvest migrate status --check`, seeds the first API token and runs
  `harvest preflight`.
- The probes ([#1812](../operations/kubernetes-probes.md)) and the drain
  (#1813) set how a pod starts and stops. Nothing packaged them.
- The repository had no `Dockerfile`, no chart and no image release.

## Decision

### 1. One image with the CLI and the reference worker

The release publishes `ghcr.io/autumn-foundation/autumn-harvest:<version>`.
It holds these files, and no user workflow:

| Path | Use |
|---|---|
| `/usr/local/bin/harvest` | The `harvest` CLI: `migrate run`, `migrate status --check`, `token bootstrap`, `preflight`, `worker drain`. |
| `/usr/local/bin/harvest-replay` | `harvest-replay`, the offline replay check. |
| `/usr/local/bin/standalone-runner` | `standalone-runner`, the reference worker from `examples/standalone-runner`. It is the default `CMD`. Its workflows are demos. |
| `/usr/share/autumn-harvest/migrations/harvest` | The plugin migrations for `harvest migrate run --include-dir`. The CLI embeds the core migrations only. |

The image runs as user `65532:65532`. `STANDALONE_RUNNER_ADDR` is
`0.0.0.0:8082`, so a probe from the kubelet reaches the runner.

### 2. A user worker builds on the image

Build your worker binary in your own build stage. Copy it into the Harvest
image. The worker then has the CLI for its migration job, the same runtime
libraries and the same non-root user.

```dockerfile
FROM rust:1.99.0-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin my-worker

FROM ghcr.io/autumn-foundation/autumn-harvest:0.7.0
COPY --from=build /src/target/release/my-worker /usr/local/bin/
CMD ["my-worker"]
```

Pin the base by the digest that `cosign verify` checked.

### 3. A distroless runtime

The runtime stage is `gcr.io/distroless/cc-debian12:nonroot`. It holds
glibc, libgcc and CA certificates. It has no shell and no package manager.

No other shared library is necessary. The Postgres client and SQLite are
compiled in (`pq-src`, bundled `libsqlite3-sys`), and TLS is rustls. The
CA certificates are necessary: `harvest migrate` checks the server
certificate against the platform store.

Each base image is pinned by digest. Dependabot updates the digests. The
build image must use the toolchain of `rust-toolchain.toml`.

### 4. Build, sign and publish

The release workflow keeps the split of issue #1826. The job that runs
third-party build code holds no write token.

- `image` builds with `cargo auditable`, so `cargo audit bin` can scan each
  binary. It runs each binary once and checks the user. It saves the image
  as a workflow artifact. A pull request that changes the image inputs runs
  it as a dry run.
- `publish-image` runs on a tag push only, after `validate`. It pushes the
  image to GHCR and signs the pushed digest with keyless Sigstore. It
  verifies the signature as a user does. It pushes a build provenance
  attestation to the registry. It runs no repository code.
- The GitHub Release waits for `publish-image`.

Verify an image:

```bash
cosign verify ghcr.io/autumn-foundation/autumn-harvest@sha256:<digest> \
  --certificate-identity-regexp '^https://github.com/autumn-foundation/autumn-harvest/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/autumn-foundation/autumn-harvest@sha256:<digest> \
  --repo autumn-foundation/autumn-harvest
```

### 5. A Helm chart for one worker

[`charts/autumn-harvest-worker`](../../charts/autumn-harvest-worker/README.md)
installs one worker Deployment. It sets:

- the liveness probe on `/api/harvest/health/live` and the readiness probe
  on `/api/harvest/health/ready`;
- a `preStop` sleep, and a grace period that covers the sleep and the
  drain. The chart refuses a shorter grace period;
- a `harvest migrate run` Job as a pre-install and pre-upgrade hook, and a
  `harvest migrate status --check` init container;
- a non-root, read-only security context and a PodDisruptionBudget.

CI runs `helm lint --strict` on each pull request.
`docs/audits/worker-image-contract.py` renders the chart and checks this
list.

## Consequences

- The image and the chart follow the workspace version. The audit fails
  when `appVersion` differs.
- A toolchain bump must also bump the build image. The audit fails on a
  mismatch.
- The image is `linux/amd64` only. The release archives also have no Linux
  arm64 build.
- The chart is not published to a chart registry. Install it from a
  checkout or a release tag.
- Outside `dev`, the operator seeds the first API token with
  `harvest token bootstrap`. The chart does not write to the database.

## Alternatives rejected

- **A generic runner that loads workflows at run time.** A workflow is Rust
  code. Harvest has no plugin ABI for it.
- **An image with the Rust toolchain.** It is large. The user chooses the
  build stage.
- **Debian slim as the runtime.** A shell and a package manager give an
  attacker more tools.
- **A static musl binary on `scratch`.** The release archives use glibc,
  and `scratch` has no CA certificates.
- **An operator.** A chart covers one worker fleet. An operator is a larger
  product. Open a new issue when a chart is not sufficient.
- **Migrations at worker start.** Only `dev` does this. In a fleet, each
  replica would race to migrate. The upgrade guides apply migrations before
  the roll.
