## Testing — Nightly coverage and a ratchet on the critical modules (issue #1818)

CI now measures line and branch coverage every night. A ratchet stops the
coverage of seven critical modules from dropping. There is no new
`WorkflowEvent` variant, no migration and no change to engine behavior.

**Nightly job.** `.github/workflows/coverage-nightly.yml` runs the
`autumn-harvest` lib tests and every `linux` manifest row under
`cargo llvm-cov --branch`. The DB rows use the 21 shards of `test-db-linux`.
`show-env` instruments the plain `cargo test` calls of `run-suites.sh`, so the
manifest stays the one list of suites. The `report` job merges the shards. It
publishes per-module line and branch coverage as the `coverage-report`
artifact and as the run summary. A failed scheduled run opens an issue. A PR
that changes the coverage tooling runs the whole pipeline.

**Toolchain.** Branch coverage needs `-Zcoverage-options=branch`, so the
workflow sets `RUSTUP_TOOLCHAIN` to `nightly-2026-08-14`, the fuzz pin. The
LCOV exporter of LLVM 23 crashes on `worker.rs`. Each job exports
`llvm-cov` JSON, and `.github/ci/coverage.py convert` turns it into LCOV. The
line rule is the one of LLVM `LineCoverageStats`. On four critical modules it
gives the same lines and counts as the LCOV exporter.

**Ratchet.** `.github/ci/coverage-baseline.json` records a line floor and a
branch floor for `queue.rs`, `worker.rs`, `scheduler.rs`, `timeout.rs`,
`poison_pill.rs`, `codec_rotation.rs` and `erase.rs`. The run fails when a
module drops more than the tolerance below its floor, or has no data. The
ratchet runs only when every test job passed, because a failed test cuts
coverage short. `coverage.py ratchet --update` raises the floors and never
lowers one.

**Tests.** `coverage_nightly_wiring.rs` checks the cron, the toolchain, the
shards, the artifacts, the ratchet step, the alert job and the baseline.
`coverage.py --self-test` runs in the `lint` job. The Docker pre-pull of
`test-db-linux` moved into `.github/ci/pull-test-images.sh`, so both
workflows share it. `docs/testing/coverage.md` tells how to read the report
and how to raise a floor.
