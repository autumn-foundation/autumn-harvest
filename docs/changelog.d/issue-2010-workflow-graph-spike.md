## R&D spike — workflows as analyzable artifacts: the flow graph and saga coverage (issue #2010)

`harvest-verify` now emits each workflow's step, signal, update and
compensation graph as data. A model check runs over that data alone.
Write-up and verdict: `docs/rnd/workflow-graph-spike.md`. Design:
`DESIGN-2010.md`.

What shipped, all in `autumn-harvest-verify`:

- **Flow graph.** Each body in the `--emit-structure` manifest gains a
  `flow` graph. A node is an event: entry, step, call, handler, saga
  operation or exit. An edge joins two events with no other event between
  them. The manifest names the format as `flow: "harvest-flow/1"`. The
  manifest format stays `harvest-structure/1`, because the new fields are
  additive. The upgrade check does not read them.
- **Handlers.** Each workflow lists its signal, update and query handler
  registrations, with the name when the MIR shows it.
- **Saga compensation coverage.** `--check-structure FILE` reads a manifest
  and gives each workflow `no-saga`, `covered`, `gap` or `unknown`. A gap is
  an error exit that a completed forward step reaches with no unwind. Exit
  `1` on a gap. Only `--format` and `--strict` combine with the flag.

Analyzer fixes the spike needed:

- A closure that returns an `async` block, passed to a callee with no body
  here (such as `Saga::step`), now has that block analyzed. Before, every
  step inside a saga closure was invisible to the graph and to the
  determinism verdict. Only a block the closure itself builds is followed.
  A block that captures a `&mut` reference is a boundary.
- The type `{async fn body of f<.., {closure@..}>}` no longer reads as an
  unresolved callback. Before, each `saga.step(..).await` gave a false
  `unresolved-callback` boundary.
- `switchInt` keeps its case values in the AST.
- The driver now finds the MIR of a bin target. Cargo uplifts a bin from
  `deps/`, and the driver searched only the uplifted directory.

**Model version `2026.10.1`.** The same MIR now gives another manifest, with
more bodies and fewer boundaries. The upgrade check compares manifests only
under one model version, so emit the baseline and the candidate manifests
with the same `harvest-verify`.

No engine change. No migration. No new `WorkflowEvent` variant.

Tests: `tests/saga_graph.rs` (fixture `tests/fixtures/saga_graph/`),
`tests/async_block_follow.rs` (fixture `tests/fixtures/async_block_follow/`),
the `--check-structure` cases in `tests/cli.rs`, and unit tests in
`flow.rs`, `saga.rs`, `summary.rs`, `parse.rs` and `driver.rs`.
