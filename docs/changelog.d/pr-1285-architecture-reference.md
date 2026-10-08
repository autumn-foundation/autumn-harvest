## Docs — Restore the engine reference as `docs/architecture.md` (PR #1285)

**Docs-only** (implemented). `562c781` ("fix: claude.md") reduced `CLAUDE.md` to
workflow instructions — correctly, since `CLAUDE.md` is agent instructions and
not a place for project documentation to accumulate. `docs/shipped-work.md`
restored the phase list that reduction took. This restores the other half: the
architecture and API reference (workspace layout, crate relationships, design
decisions, module guide, macro-usage patterns, development commands, DB schema
quick reference), verbatim from `CLAUDE.md` as of 89442c4, at
`docs/architecture.md`.

**Why it mattered.** That half was never restored anywhere, so no file on the
branch contained `### Worker Sessions`, `### Standalone Start`, the
`Never change this tagging` contract, or the add-a-shard procedure — while ~20
cross-references in `docs/` and in source comments still pointed at them. Every
one was a dead end. All now resolve, with section anchors where the target is a
named subsection: `docs/sharding.md` (×2), `docs/streaming-progress.md` (×2),
`docs/runbooks/safe-handler-removal.md`, `docs/getting-started/10-operations.md`,
`docs/performance-verify.md`, `docs/performance-sqlite-runtime-drive.md`, plus
doc comments in `api.rs`, `mcp_tools.rs`, `shard_fanout.rs`, two plugin tests
and two examples. `mcp_tools_http_tests.rs`'s reference to the #373 hardening
notes points at `docs/shipped-work.md`, which is where that record now lives.

**Convention repair.** `docs/changelog.d/README.md` still told authors the phase
list lives in `CLAUDE.md`; it now names `docs/shipped-work.md` and states
outright that a changelog entry never goes in `CLAUDE.md`. `CLAUDE.md` and
`README.md` gain pointers to both documents so the next reader finds them
without grepping.

**Deliberately untouched.** Every reference to `CLAUDE.md`'s live
`Engine Invariants` and `Database Migrations` sections (`erase.rs`,
`codec_rotation.rs`, `lib.rs`, `timeout.rs`, `docs/adr/0003-payload-codec-event-boundary.md`,
`migration_hygiene.rs`, `docs/upgrading/0.5.0.md`,
`docs/operations/codec-key-rotation.md`) — those sections exist and the
references are correct. Historical prose inside `docs/shipped-work.md` and the
changelog fragments is also left as written: it records what a past PR did at
the time, and rewriting it would collide with an in-flight collation sweep.

**No code change, no new `WorkflowEvent` variant, no migration, no replay
impact.** Comment and documentation text only. `cargo fmt --all -- --check`
clean; the full `--test integration` suite is 1715/1715 green (both doc-guard
suites, `performance_docs` and `migrating_from_temporal_docs`, included);
`cargo check -p autumn-harvest-plugin --all-targets` and
`cargo check -p autumn-harvest --examples` clean.
