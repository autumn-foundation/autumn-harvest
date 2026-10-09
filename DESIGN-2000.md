# Design — Issue #2000: non-destructive fork of any run

Issue #2000 asks for a fork that copies a run to a new workflow id. The
source stays unchanged. By default, the fork does not run an effect again.

**No migration.** Two new `WorkflowEvent` variants. One new route:
`POST /workflows/{id}/fork`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Reset (`reset.rs`) seals the source `TERMINATED`. It rejects `COMPLETED`
  and `TERMINATED` sources. It refuses an erased source only when an
  in-process caller sets `refuse_erased_source`.
- Reset already copies a history prefix to a new execution on the same
  shard. It inserts new rows and never changes a source row.
- Replay matches an activity by position and name, not by id. An
  `ActivityExecId` is a random UUID. So a fork must find a recorded result by
  name and occurrence, not by id.
- The worker writes `ActivityScheduled` and enqueues the task in one
  transaction. `fail_activities_for_broken_sessions` already writes a
  synthetic outcome in that transaction and wakes the workflow. The plain
  and the mixed-batch paths both call it.
- Lineage of issue #621 follows `parent_id`. A fork is a new root, so it
  cannot use `parent_id`. The provenance pair of issue #740
  (`start_source`, `start_source_ref`) has no `CHECK` constraint.
- Erasure writes a `{"_harvest_erased": true}` tombstone into the
  execution `input` and into each payload field of each event.
- No `MutexReleased` event exists. A history prefix cannot show whether a
  granted mutex is still held.

### 0.2 Brainstorm — how can a fork work?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add a `non_destructive` flag to reset. | Rejected. Reset semantics (seal, signal drain, state gate) would branch on every step. |
| B2 | A new `fork.rs` module that reuses reset's copy helpers. | **Adopted.** One function, no seal, no drain. |
| B3 | Snapshot the source results into the fork marker. | Rejected. Nested payloads skip the codec and erasure. Plaintext would land at rest. |
| B4 | Read the source history when the fork schedules an activity. | **Adopted.** Results stay under the codec. A later erasure of the source makes the result a tombstone, which the fork refuses. |
| B5 | Intercept the activity at task pick-up. | Rejected. It adds a read to every activity task of every run. |
| B6 | Resolve the activity in the scheduling transaction, as broken sessions do. | **Adopted.** It costs nothing for a run that is not a fork. |
| B7 | Serve local activities, children and external effects from records too. | Deferred. Six persistence paths. A fail-closed guard covers them. |
| B8 | Link through `parent_id`. | Rejected. A child gets parent-close cascades. |
| B9 | Link through `start_source = 'fork'` and the source id in `start_source_ref`, plus a history marker. | **Adopted.** No migration. |
| B10 | Store the effects mode in a new column. | Rejected. The mode lives in the fork marker, so no migration. |

### 0.3 Reverse brainstorm — how can a fork do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The fork charges a card again under a new identity. | Default `effects = recorded`. A remote activity takes the recorded result. With no record, it fails non-retryably with `ForkEffectUnavailable`. It never runs. |
| R2 | The fork reaches an effect that recorded mode cannot serve (local activity, child, external activity, external signal or cancel). | A guard fails the fork run before the effect runs. A fork-time check refuses a source whose suffix holds such an effect. |
| R3 | The fork resurrects erased data. | The fork refuses an erased source under a row lock. A recorded result that is a tombstone is unavailable. |
| R4 | The fork changes the source. | The fork only reads source rows. A test compares the source row and its events before and after. |
| R5 | A recorded result from a different input is served. | A recorded result is served only when the scheduled input matches. |
| R6 | The fork inherits a mutex grant without the lock row. | A prefix that holds `MutexGranted` is refused. |
| R7 | The fork replays a terminal event and ends at once. | A prefix that holds a terminal or `DecisionCommitted` tail is refused. |
| R8 | The fork id collides with a running workflow. | A new workflow id. A collision returns `409`. |
| R9 | An input override diverges from the carried prefix. | An input override needs fork point `0`. |
| R10 | An override bypasses encryption at rest. | Each override is its own event with a top-level `output`. The codec encodes it. Erasure tombstones it. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Reset copies a prefix. Replay matches by position and name. No fork route exists. |
| Red | A fork that can pay twice is unacceptable. Fail closed. |
| Black | Recorded mode is strict. A fork that takes a new path fails. That is the intent: `effects = live` opts in. |
| Yellow | Evaluation and what-if runs become safe on completed runs. Overrides let a caller test a different result. |
| Green | Later: serve local activities and children from records. |
| Blue | Red: tests for each AC. Green: `fork.rs`, two events, two worker hooks, route. Refactor: share reset helpers. Then a multi-angle review. |

---

## 1. Change

### 1.1 Engine (`autumn-harvest/src/fork.rs`)

`fork_workflow_execution(conn, source_id, WorkflowForkRequest, registry)`:

1. Lock the source row `FOR SHARE`. Refuse an erased source.
2. Load and decode the source history.
3. Resolve the fork point. The default is event `0`.
4. Validate the point with `reset::validate_reset_point`. Also refuse a
   prefix with a terminal tail or a `MutexGranted`.
5. In recorded mode, refuse a suffix with an effect it cannot serve.
6. Insert a new root row: a new workflow id, `start_source = 'fork'`,
   `start_source_ref = <source id>`.
7. Copy the prefix. Append `WorkflowForked`, then one
   `ForkActivityResultOverridden` per override.
8. Enqueue the first workflow task.

Accepted source states: every state. A continue-as-new history is refused.

### 1.2 Events

- `WorkflowForked { forked_from_exec_id, fork_event_id, effects, reason, operator_id }`
- `ForkActivityResultOverridden { activity_name, occurrence, output }`

Replay skips both, as it skips `WorkflowResetFork`.

### 1.3 Worker

- `fork::serve_recorded_activities` runs in the scheduling transaction of
  `persist_scheduled_activities` and `persist_mixed_suspension_batch`. It is
  a no-op unless `start_source = 'fork'`. For each scheduled activity, it
  writes the override, the recorded outcome, or `ForkEffectUnavailable`. It
  then fails the task row so no worker runs it.
- `fork::live_effect_refusal` runs after each decision of a recorded fork.
  It fails the run when the decision holds an effect that recorded mode
  cannot serve.

### 1.4 API

`POST /workflows/{id}/fork` returns `201` with the new execution id. Errors:
`400` invalid point or override, `404` unknown source, `409` erased source,
unservable effect, carried mutex, or a workflow id in use.

## 2. Tests

| AC | Test |
|----|------|
| A completed run can be forked; the source is unchanged. | `fork_of_a_completed_run_leaves_the_source_unchanged` |
| By default, a fork does not run an activity again. | `recorded_fork_does_not_run_a_completed_activity_again` |
| Live side effects need an explicit flag. | `effects_default_to_recorded`, `live_fork_runs_the_activity` |
| An erased source is always refused. | `fork_refuses_an_erased_source` |
