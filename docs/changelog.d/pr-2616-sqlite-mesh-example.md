## A small runnable mesh example (issue #2486, PR #2616)

The agent mesh now has an on-ramp between `sqlite_agent` and the
`examples/agent-mesh/` capstone.

- **Example.** `autumn-harvest-agent/examples/sqlite_mesh.rs` runs on
  SQLite with a scripted model and no credentials. A planner holds a
  mandate that names one peer, a read-only researcher with one tool. The
  planner delegates a question, and the researcher answers. The planner
  then delegates to a roster peer that the mandate does not name. The
  harness refuses it with `not_permitted`. Last, the planner asks for a
  gated write. The example drops the runtime at the approval wait and
  opens the file again. It approves the write, and the run finishes. The
  example prints the `AgentReport::delegations` roll-up. It exits non-zero
  when a claim of `Outcome::check` fails.
- **Guide.** `docs/agent-mesh.md` section 2 gets "Run a small mesh", with
  the command and the output. `docs/agent-adapter.md` section 2 and the
  crate README point to it.
- **CI.** The agent test shard runs the example with `cargo run`. The
  crate test step runs its three tests.

No engine change, no new `WorkflowEvent` variant and no migration.

Test evidence: the tests show one completed delegation and one
`not_permitted` refusal with no child run. Before the restart, the run
sends five model calls. After it, the run sends one, and each tool runs
once. A test proves that `Outcome::check` names a failed claim. Another
test fails when the guide output differs from the real output.
