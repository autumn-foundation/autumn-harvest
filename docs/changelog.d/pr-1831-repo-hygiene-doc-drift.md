## Fix — tracked artifacts and doc drift (issue #1831)

**Artifacts removed.** `test_debug`, `test_keys`, their sources and
`autumn-harvest/src/analyzer.rs.orig` are gone. `.gitignore` now ignores
`*.orig` and `*.rej`.

**Two new lint gates.** Both are pure Python, and both run a self-test first.

- `docs/audits/tracked-artifacts.py` fails on a tracked binary or patch
  leftover. The binary rule is git's: a NUL byte in the first 8000 bytes.
  `echo.wasm` is the one allowed binary.
- `docs/audits/doc-claim-drift.py` checks each doc claim against the shipped
  code. A stated `WorkflowEvent` variant count matches `event.rs`. No two
  rows of one table in `docs/architecture.md` share a first cell. The newest
  version in `RELEASE_NOTES.md`, if any, is the workspace version. Each
  `STALE_CLAIMS` pattern stays absent from its file.

**Docs reconciled.**

- `docs/comparison.md` now marks cross-region DR (#954) and data-residency
  pinning (#697) as shipped. Cross-shard children (#956) and quiescent
  rebalancing (#964) replace "no cross-shard workflows".
- `docs/architecture.md` merges the two `event.rs` rows and drops the variant
  count. It also drops a stale table count from the `schema.rs` and
  `models.rs` rows. The sharding section cites rebalancing (#964), and the
  child fan-out section cites the `_placed` variants (#956).
- `docs/sharding.md` no longer calls cross-region failover or rebalancing out
  of scope.
- `RELEASE_NOTES.md` points to `CHANGELOG.md`. The release workflow writes the
  notes for each GitHub Release.
- The `autumn-harvest-redis` crate doc says that the worker uses the crate as
  a dispatch channel for task references. The standalone queue is separate.

**#606 step 9.** The step is wired: `build_activity_enqueue_plan` writes
`session_id` and hard-pins the task row. The stale TODO and the three
`#[allow(dead_code)]` attributes are gone, so the compiler now checks that
the fields are read.

**Test evidence.** Each audit's `--self-test` covers its rules with fixtures.
Against the pre-change tree, `tracked-artifacts.py` reports 3 findings and
`doc-claim-drift.py` reports 16. Both report 0 after the change. `cargo
clippy -p autumn-harvest --all-features --tests -- -D warnings` passes
without the `#[allow(dead_code)]` attributes.

No `WorkflowEvent` variant, no migration.
