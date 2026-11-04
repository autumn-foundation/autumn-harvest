# DESIGN-2378: a live probe on a fork

Issue #2378. Parent: #2369. Needs #2375, #2394 and #2000.

## Problem

A replay probe (#2375) answers a new tool call with an error stub. So it
cannot judge a candidate that changes its tool plan. A live probe runs the
candidate on a fork, with real tools. It must run only the tool effects
that the operator allows. The default is `[read_only]`.

## Decision

`eval::live_probe(host, harness, source, overlay, allowed)` forks the
source `agent_loop` run at event 0. The fork uses `ForkEffects::Live`. Its
input is the source task under the overlay, with a `probe` mark.

| Part | Rule |
|---|---|
| Mark | `AgentTask::probe` holds the `allowed` set, as a one-byte mask. The fork records it in `WorkflowStarted`. The `WorkflowForked` event names the source. |
| Gate | `AgentHarness::tool_call` reads `ToolCallRequest::probe_effects`. A tool with an effect outside the set gets `probe_effect_refused`. `Tool::execute` does not run. |
| Policy | The gate is in the activity body. The policy cannot change it. A permissive policy cannot bypass it. |
| Tools seen | The model sees every tool, as in the source. A refusal is an outcome, so the probe records the plan of the candidate. |
| Delivery | `drive` sends no report for a probe run. |
| Children | `child_task` copies the mark. An inline child and a fan-out child are probes with the same set. |
| Durable peer | A probe refuses a delegation to a durable peer. That peer runs its own effects. |
| Workspace | A probe never moves the workspace head. The head belongs to the source scope. |
| Follow-ups | The probe input has no follow-ups. A follow-up waits on a durable timer, so the probe would not end. |
| Draw | The probe input has no draw. A draw root sets the run id, and the probe needs its own. |
| Ledger | The model turns of the fork record ledger rows under the fork execution id. |
| SQLite | The SQLite host returns `EvalError::Unsupported`. SQLite has no fork. |

`agent_refine` (#2380) is not built yet. It will pass
`RefinePolicy::probe_effects` as `allowed`.

## Brainstorm

- Put the gate in the policy, with `RefinePolicy::probe_rules`.
- Put the gate in the activity body, with a new request field.
- Reuse `AgentTask::allowed_effects` (#2439) as the probe set.
- Run the probe in the test engine, with real tool activities.
- Fork with `ForkEffects::Recorded` and override each tool activity.
- Hide each refused tool from the model.
- Mark the probe in a search attribute, not in the input.

The activity gate with a new field wins. The issue requires the activity
body. `allowed_effects` hides tools from the model, so the probe would not
see the real plan. The test engine has no worker, no ledger and no fan-out
child runs. A search attribute is not part of the recorded input.

## Reverse brainstorm: how to make the probe unsafe

| Way to fail | Guard |
|---|---|
| A permissive policy allows a `Write` call. | The activity checks the effect after the policy. |
| A delegated child runs without the mark. | `child_task` copies it. A test covers a fan-out child. |
| A durable peer runs the delegated work. | The probe refuses the delegation. |
| The probe sends a report to a person. | `drive` skips the delivery. A test counts deliveries. |
| The probe writes the source history. | A fork only inserts new rows. A test hashes the source rows. |
| The probe spend counts on the source. | The fork has its own execution id. A draw root is cleared. A test reads the ledger. |
| A read-only tool moves the shared workspace head. | A probe never saves the workspace. |
| The probe waits on a follow-up timer for days. | The probe input has no follow-ups. |
| A new tool path skips the gate. | Each app tool goes through `tool_call`. The gate runs before each other check. |
| An unknown tool runs. | The lookup refuses it first, as before. |
| An overlay widens a bound. | `overlay::apply` with the source ceilings refuses it. |
| An erased source leaks into a prompt. | `task_of` refuses an erased history first. |

## Six hats

- **White (facts).** `fork.rs` supports a live fork at event 0 with a new
  input. `ToolEffect` has four values. `RefinePolicy::probe_effects`
  defaults to `[read_only]`. Ledger rows key on the execution id.
- **Red (feeling).** A live probe on production data is risky. The default
  must refuse each write. An operator must widen it on purpose.
- **Black (risk).** A gated call waits for an approval that nobody gives.
  It times out to a deny. Bound the wait with `postgres::wait_run`. A
  probe in a mesh spends from the mesh budget.
- **Yellow (value).** The refine loop can judge a candidate that changes
  its tool plan, with real read results.
- **Green (options).** A later issue can add a probe report that diffs the
  fork against the source, as `evaluate` does.
- **Blue (process).** Write the red tests first. Then add the mark, the
  gate, the delivery skip and the fork call. Then refactor and document.

## Test plan

`autumn-harvest-agent/tests/live_probe_postgres.rs` runs on a Postgres
worker. `autumn-harvest-agent/tests/live_probe.rs` runs on SQLite. Unit
tests in `harness.rs` cover the gate order.
