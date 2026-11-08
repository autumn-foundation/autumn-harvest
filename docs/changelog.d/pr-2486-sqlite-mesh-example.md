## A small runnable mesh example (issue #2486)

The agent mesh now has an on-ramp between `sqlite_agent` and the
`examples/agent-mesh/` capstone.

- **Example.** `autumn-harvest-agent/examples/sqlite_mesh.rs` runs on
  SQLite with a scripted model and no credentials. A planner holds a
  mandate that names one peer, a read-only researcher with one tool. The
  planner delegates a question, and the researcher answers. The planner
  then delegates to a roster peer that the mandate does not name, and gets
  `not_permitted`. Last, it asks for a gated write. The process drops the
  runtime at the approval wait, opens the file again, approves and
  finishes. The example prints the `AgentReport::delegations` roll-up. It
  exits non-zero when a claim fails: one delegation runs, one is refused,
  and the restart sends one model call and runs no tool again.
- **Guide.** `docs/agent-mesh.md` section 2 gets "Run a small mesh", with
  the command and the output. `docs/agent-adapter.md` section 2 and the
  crate README point to it.
- **CI.** The agent test shard runs the example with `cargo run`. The
  crate test step runs its three tests.

No engine change, no new `WorkflowEvent` variant and no migration.

Test evidence: the example tests show one completed delegation, one
`not_permitted` refusal with no child run, five model calls before the
restart and one after it, and one run of each tool. A third test checks
that the guide shows the real output of the example.
