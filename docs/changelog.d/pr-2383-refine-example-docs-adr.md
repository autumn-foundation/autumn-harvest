## The refine example, guide section and ADR (issue #2383)

The refine loop of epic #2369 now has an example, a guide section and an
ADR. The design is in `docs/design/DESIGN-2383.md`.

- **Example.** `autumn-harvest-agent/examples/agent_refine.rs` runs the
  loop on SQLite with scripted models and no credentials. Op 1, a
  recorded `agent_loop` run, misses a refund rule, and the `ExactMatch`
  scorer rejects it. `agent_refine` with k = 1 proposes an overlay that
  adds the rule. The example approves it at the gate. Op 2, an op of a
  durable agent of the kind, reads version 1 and passes. The example
  prints the version before and after and the result of op 2. It exits
  non-zero when op 2 fails. The refine workflow needs `eval`, so run it
  with `--features sqlite,eval`.
- **Guide.** `docs/agent-adapter.md` section 25, "Refine an agent",
  explains the loop, the budget identity, the ceilings, the probes and
  their safety, adoption, the `RefinePolicy` settings and the lesson
  library. It links the settings table of section 18 and does not copy
  it.
- **ADR.** `docs/adr/0016-agent-refinement.md` records four decisions:
  operators decide, the loop is offline, overlays stay under operator
  ceilings, and by default a person adopts and probes only read.
- **ADR index.** The new `docs/adr/README.md` lists each ADR.
  `docs/design/README.md` links it.
- **CI.** The agent test shard runs the example with `cargo run`. The
  crate test step runs its three tests.

No engine change, no new `WorkflowEvent` variant and no migration.

Test evidence: the example tests show that op 1 fails, that op 2 passes
after the approval with 3 sessions and 2 probes, that one lesson is
written, and that op 2 fails after a reject.
`autumn-harvest-agent/tests/refine_docs.rs` guards the guide section, the
ADR, both indexes, this entry and the CI step.
