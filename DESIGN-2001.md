# Design — Issue #2001: replay-as-evaluation

Issue #2001 asks for a harness that re-drives a recorded agent run. The
harness uses the recorded tool outputs and a live candidate model or prompt.
It then reports where the decisions diverge.

**No migration. No new `WorkflowEvent` variant. No route change. No database.**
The harness is a new `eval` module in `autumn-harvest-agent`, behind a new
`eval` feature.

---

## 0. Planning record

### 0.1 Facts found before the plan

- One model call is one `agent_model_turn` activity. Its input is a
  `ModelTurnRequest` and its output is a `ModelTurn`.
- One tool call is one `agent_tool_call` activity. Its input is a
  `ToolCallRequest` and its output is a `ToolOutcome`.
- The other activities of `agent_loop` are `agent_memory_snapshot` and
  `agent_deliver`. A gated call waits on an approval signal with a deadline.
  A follow-up waits on a timer.
- `ModelTurn.decisions` holds one policy decision per tool call. Replay reads
  them back, so they are part of the recorded decision.
- `AgentHarness::model_turn` is public. A candidate harness can answer a
  recorded request with no engine.
- Tool-call ids come from the provider. A new model call gives new ids.
- `WorkflowTestEnv` runs a real workflow body in memory. Activities resolve
  from synchronous mocks and timers fire on a virtual clock.
- The approval deadline is the timer `__signal_timeout:{seq}:{signal}`.
- The replay debugger (#949) diffs commands. A changed prompt changes the
  first request, so its diff always stops at the first model turn.
- The non-destructive fork (#2000) is open. It has no code yet.
- An erased history holds `{"_harvest_erased": true}` tombstones (#495).

### 0.2 Brainstorm — where can the harness live, and how can it run?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Fork the run in Postgres (#2000), then run the fork on a worker with live model calls. | Rejected for this issue. #2000 has no code. A database fork also needs a worker, a queue and cleanup for a read-only question. |
| B2 | Strict replay through `WorkflowReplayer`. | Rejected. Replay returns the recorded model output. It never asks the candidate. |
| B3 | Diff two `ReplayDebugger` traces. | Rejected as the main diff. A new prompt changes every request payload, so the command diff stops at turn 0. |
| B4 | Write a second agent loop for evaluation. | Rejected. Two loops drift apart. The harness must drive the real `agent_loop`. |
| B5 | Drive the real `agent_loop` in `WorkflowTestEnv`. Mock the model activity with the live candidate. Mock every other activity with the recorded result or a stub. | **Adopted.** It is an in-memory fork at event 0 with live effects off. The source is a borrowed slice, so it cannot change. |
| B6 | Put the harness in the core crate. | Rejected. The core crate does not know the agent payloads. |
| B7 | Match a recorded tool output by tool-call id. | Rejected. Ids change per model call. Match by step, tool name and arguments. |

### 0.3 Reverse brainstorm — how can this harness do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A tool runs live during evaluation, for example a payment. | The harness never calls `AgentHarness::tool_call`. A tool call gets its recorded outcome or an error stub. A test proves that no tool runs, also on a divergent path. |
| R2 | A report reaches a user during evaluation. | `agent_deliver` is always a stub. It never calls the delivery. A test proves it. |
| R3 | An unknown activity runs live. | The test env has no real activity handlers. An activity with no mock fails the candidate run. |
| R4 | An erased source leaks a tombstone into a prompt. | The harness refuses a history that holds a tombstone. This matches the last fork rule of #2000. |
| R5 | A late approval in the source releases a call in the evaluation. | The harness drops an approval that arrived after its deadline timer fired. |
| R6 | A recorded output answers a different call. | The key is step, name and arguments. A call with no exact match gets an error stub. |
| R7 | New tool-call ids break the approval names and the transcript. | A candidate call that matches the recorded call at the same turn and position takes the recorded id. |
| R8 | The report hides a divergence as "reworded". | Only a final answer with the same stop reason and different text is "reworded". Any change in calls, arguments, policy decisions or stop reason is a divergence. A changed run end is a divergence too. |
| R9 | A current-thread runtime panics in `block_in_place`. | The harness checks the runtime first and returns `EvalError::MultiThreadRuntimeRequired`. |
| R10 | The candidate model fails. | The failure is the candidate outcome in the report. It is not a harness error. |
| R11 | A reused provider id lets a recorded approval release another call. | A candidate call that differs from the recorded call gets a new `eval_` id. |
| R12 | A crafted history makes the evaluation spend without bound. | A turn cap stops the live model. The default is the recorded turn count plus `DEFAULT_EXTRA_TURNS`. |
| R13 | An in-flight source drives the candidate past the recorded frontier. | The harness refuses a source with no terminal event. |
| R14 | The policy sees another run id and decides otherwise. | The candidate request takes the recorded run id. The docs name the policy as a live call. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Model and tool calls are activities. The payloads hold the decisions. `WorkflowTestEnv` runs a body in memory. #2000 has no code. |
| Red | Teams fear a model change most when it can act. A harness that can run a tool by mistake is worse than no harness. |
| Black | A live model call costs money. Ids differ per call. A divergent path has no recorded outputs. The fork sibling is not ready. |
| Yellow | The harness uses the real loop, so it cannot drift from production. It needs no database, so it runs in CI against exported histories. |
| Green | Id adoption keeps the approval names stable. A per-turn report shows every divergence, not only the first. A later CLI can wrap it. |
| Blue | Red: tests for divergence, no live tools, no live delivery, erased source, late approval. Green: the `eval` module. Refactor: docs, CI, review. |

### 0.5 The fork dependency

The issue says the harness builds on the fork sibling (#2000). The harness
needs the fork semantics, not the fork's database rows. It applies the fork
rules in memory:

- The harness reads a borrowed slice, so the source stays unchanged.
- The harness accepts a completed source.
- Each effect gets a recorded result or a stub. No option turns on live
  effects.
- The harness always refuses an erased source.

The database fork in #2000 stays open as its own work.

---

## 1. Change

- `autumn-harvest-agent/src/eval.rs`, behind the `eval` feature:
  - `Candidate`: the candidate harness and an optional system prompt.
  - `evaluate(history, &candidate) -> Result<Evaluation, EvalError>`.
  - `Evaluation`: one `TurnDiff` per model turn, the first divergence, both
    run ends, `end_diverged`, the counts of replayed and stubbed tool calls,
    and `turn_cap_reached`.
- The `eval` feature turns on `autumn-harvest/testing`.
- CI runs the agent tests and clippy with `eval`.

## 2. Tests

| Test | Proves |
|------|--------|
| The same model gives no divergence. | The baseline is clean. |
| A candidate that calls another tool diverges at that turn. | The report names the turn. |
| A candidate that changes arguments diverges. | Arguments are part of the decision. |
| A candidate prompt reaches the model. | The prompt override works. |
| No tool runs, on the same path and on a divergent path. | Issue AC 3. |
| No report is delivered. | Live effects are off. |
| An erased history is refused. | The last fork rule. |
| A late approval is not delivered. | R5. |
| A current-thread runtime is refused. | R9. |
| A reused provider id does not take a recorded approval. | R11. |
| The turn cap stops the candidate model. | R12. |
| An in-flight source is refused. | R13. |
| The candidate policy sees the recorded run id. | R14. |
| A changed run stop diverges when every turn agrees. | R8. |
| Each segment sees its recorded memory snapshot. | Memory and follow-ups replay. |
