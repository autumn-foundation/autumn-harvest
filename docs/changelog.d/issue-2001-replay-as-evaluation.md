## Feature — Replay-as-evaluation for agent runs (issue #2001)

**What shipped.** `autumn_harvest_agent::eval::evaluate` re-drives a recorded
`agent_loop` run with a candidate model or prompt. It reports where the
decisions of the two runs diverge. It is behind the new `eval` feature of
`autumn-harvest-agent`.

**How it runs.** The harness runs the real `agent_loop` body in the in-memory
test engine. The candidate model and the candidate policy answer each model
turn live. Every other effect is recorded or stubbed:

- A tool call gets the recorded outcome of the same call, or an error stub.
  No tool runs.
- A memory snapshot is the recorded one, or an empty one.
- A delivery is a stub. No report leaves the harness.
- The harness sends again each approval for a wait that the source opened,
  when it arrived in time. It drops every other signal.
- A candidate call that differs from the recorded call gets a new `eval_`
  id, so no recorded approval can release it.

**The diff.** `Evaluation` holds one `TurnDiff` per model turn. A turn
diverges when the tool calls, the arguments, the policy decisions or the stop
reason differ. Two final answers with the same stop reason and different text
get `Reworded`. `first_divergence` names the first divergent turn.
`end_diverged` compares how the two runs end.

**Cost.** `Candidate::max_turns` caps the live model calls. The default cap
is the recorded turn count plus `DEFAULT_EXTRA_TURNS`.

**Fork rules.** An evaluation is an in-memory fork at the first event. The
harness leaves the source unchanged, accepts a completed source, records or
stubs each effect, and refuses an erased source. It also refuses a source
that has not ended, or that was cancelled or timed out. The database fork of
issue #2000 is separate.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change. No database access. The `eval` feature turns on the engine's
`testing` feature.

**Tests.** `autumn-harvest-agent/tests/eval.rs` records each source on the
SQLite engine. `no_tool_activity_executes_live_during_evaluation` proves that
no tool runs on a divergent path. Other tests cover the following:

- a late approval and a reused provider id;
- memory snapshots across follow-up segments;
- an erased source and an in-flight source;
- a changed run stop and the turn cap.

CI runs the agent suite with `--features sqlite,eval`.
