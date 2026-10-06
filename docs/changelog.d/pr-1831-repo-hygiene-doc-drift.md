## Hygiene — tracked artifacts and doc drift (issue #1831)

**Artifacts removed.** `test_debug`, `test_keys`, their sources and
`autumn-harvest/src/analyzer.rs.orig` are gone. `.gitignore` now ignores
`*.orig` and `*.rej`.

**Two new lint gates.** Both are pure Python, and both run a self-test first.

- `docs/audits/tracked-artifacts.py` fails on a tracked binary or patch
  leftover. The binary rule is git's: a NUL byte in the first 8000 bytes.
  `echo.wasm` is the one allowed binary.
- `docs/audits/doc-claim-drift.py` fails on a doc claim that shipped code
  made false. A stated `WorkflowEvent` variant count must match `event.rs`.
  A table in `docs/architecture.md` names each row once. The newest version
  in `RELEASE_NOTES.md`, if any, is the workspace version. A list of known
  stale claims must stay absent.

**Docs reconciled.**

- `docs/comparison.md`: cross-region DR (#954) and data-residency pinning
  (#697) are shipped. Cross-shard children (#956) and quiescent rebalancing
  (#964) replace "no cross-shard workflows".
- `docs/architecture.md`: one `event.rs` row with no variant count. The
  sharding section cites rebalancing (#964). The `schema.rs` and `models.rs`
  rows drop a stale table count.
- `docs/sharding.md`: cross-region failover is no longer out of scope.
- `RELEASE_NOTES.md`: a pointer to `CHANGELOG.md`. The release job writes
  the notes for each GitHub Release.
- `autumn-harvest-redis`: the crate doc says the worker uses it as a
  dispatch channel for task references. The standalone queue is separate.

**#606 step 9.** The step is wired: `build_activity_enqueue_plan` writes
`session_id` and hard-pins the task row. The stale TODO and the three
`#[allow(dead_code)]` attributes are gone, so the compiler now checks that
the fields are read.

No `WorkflowEvent` variant, no migration.
