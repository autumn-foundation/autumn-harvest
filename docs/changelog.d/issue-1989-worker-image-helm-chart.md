## Packaging — worker container image and Helm chart (issue #1989)

**Decision.** [ADR 0006](../adr/0006-worker-container-image.md) states what
the image holds: the `harvest` CLI, `harvest-replay`, the reference
`standalone-runner` and the plugin migrations. It holds no user workflow. A
user worker builds on the image. The runtime is distroless `cc`, as user
`65532:65532`. Each base image is pinned by digest, and Dependabot updates
the digests.

**Release.** `image` builds the image with `cargo auditable` and no write
token. It then migrates a Postgres service, starts the runner, waits for
`/health/ready`, and stops it with SIGTERM. `publish-image` runs on a tag
push only, after `validate`. It pushes to
`ghcr.io/autumn-foundation/autumn-harvest`, signs the digest with keyless
Sigstore, verifies it, and pushes a provenance attestation. The GitHub
Release waits for it and names the image digest.

**Chart.** `charts/autumn-harvest-worker` installs a worker Deployment with
the probes of #1812 and the drain of #1813. It has a `preStop` sleep, a grace
period that the chart checks against the drain, a `harvest migrate run`
pre-install and pre-upgrade hook, a `harvest migrate status --check` init
container, a non-root read-only security context and a PodDisruptionBudget.

**CI.** The `lint` job runs `helm lint --strict` and
`docs/audits/worker-image-contract.py`. The audit renders the chart and
checks the contract. Five tests in `supply_chain_ci.rs` guard the release
jobs and the CI wiring.

No `WorkflowEvent` change. No migration.
