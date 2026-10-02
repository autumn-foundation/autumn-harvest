## CI — repair chaos.yml and guard workflow YAML (Semaphore)

`chaos.yml` was invalid YAML: two `run:` scalars held `chaos::` followed by a space, which YAML reads as a mapping value. GitHub ran the file as a zero-job failed run on every push and never fired the nightly cron. Each command stays on one physical line, as the `chaos_docs` guard requires, and is now a single-quoted scalar.

`docs/audits/workflow-yaml-parse.py` parses every workflow file in the `lint` job, so an unparsable workflow fails the build instead of failing silently.

Two other red checks on this PR are fixed here. `wasmtime` moves from 46.0.3 to 48.0.3 to clear RUSTSEC-2026-0316 (the `wasm-activities` feature MSRV is now Rust 1.95). The Docker pre-pull step now includes `softwaremill/elasticmq:1.5.2`, the one testcontainers image that a "bytes remaining on stream" pull failure hit unretried.
