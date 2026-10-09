## Agents — replay-as-evaluation (issue #2001)

**What shipped.** `autumn_harvest_agent::eval::evaluate` re-drives a recorded
`agent_loop` run with a candidate model or prompt. It reports where the
decisions diverge. It is behind the new `eval` feature of
`autumn-harvest-agent`.

**How it runs.** The harness runs the real `agent_loop` body in the in-memory
test engine. The candidate model answers each model turn live. Every other
effect is recorded or stubbed:

- A tool call gets the recorded outcome of the same call, or an error stub.
  No tool runs.
- A memory snapshot is the recorded one, or an empty one.
- A delivery is a stub. No report leaves the harness.
- An approval that the source received in time is sent again. An approval
  that arrived after its deadline is dropped.

**The diff.** `Evaluation` holds one `TurnDiff` per model turn. A turn
diverges when the tool calls, the arguments, the policy decisions or the stop
reason differ. Two final answers in other words are `Reworded`.
`first_divergence` names the first divergent turn.

**Fork rules.** An evaluation is an in-memory fork at the first event. The
source stays unchanged, a completed source is accepted, effects are recorded
or stubbed, and an erased source is refused. The database fork of issue #2000
is separate.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change. No database access.

**Tests.** `autumn-harvest-agent/tests/eval.rs` records each source on the
SQLite engine. `no_tool_activity_executes_live_during_evaluation` proves that
no tool runs on a divergent path. Other tests cover a late approval, an erased
source and the delivery stub. CI runs the agent suite with
`--features sqlite,eval`.
