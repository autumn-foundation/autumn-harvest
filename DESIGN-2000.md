# Design — Issue #2000: non-destructive fork of any run

Issue #2000 asks for a fork that copies a run to a new workflow id. The
source stays unchanged. By default, the fork does not run an effect again.

**No migration.** Two new `WorkflowEvent` variants. One new route:
`POST /workflows/{id}/fork`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Reset (`reset.rs`) seals the source `TERMINATED`. It rejects `COMPLETED`
  and `TERMINATED` sources. It refused an erased source only when an
  in-process caller set `refuse_erased_source`. Issue #1999 later removed
  that flag, so every reset now refuses an erased source.
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
| R11 | A later run of the fork lineage runs live. | Continue-as-new is refused. A reset keeps fork provenance. A fork row has no retry policy. |

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
  cancels the task row, so no worker runs it. It then writes the override,
  the recorded outcome, or `ForkEffectUnavailable`. A source race loser stays
  pending, so the same branch wins again.
- `fork::recorded_outcome_refusal` runs after each decision of a recorded
  fork. It fails the run before a local activity, a child, an external
  effect, a continue-as-new or a mutex acquire.
- A fork row with no marker counts as recorded. A reset of a fork keeps
  `start_source = fork` and appends a marker with the mode of that fork. A
  carried ancestor marker with another mode then cannot decide the mode.

### 1.4 API

`POST /workflows/{id}/fork` returns `201` with the new execution id. Errors:
`400` invalid point, continue-as-new history, or invalid override; `404`
unknown source; `409` erased source or lineage, unservable effect, carried
mutex, or a workflow id in use; `422` unknown field; `503` no runtime yet.

### 1.5 Review fixes

A multi-angle review found these gaps. Each one has a fix and a test.

| Gap | Fix |
|-----|-----|
| A fork continues as new, and the successor runs live. | Refuse continue-as-new in recorded mode. |
| A fork of a fork reads the first marker. | The last marker counts. Only its own overrides count. |
| A reset of a recorded fork runs live. | A reset of a fork keeps `start_source = fork`. |
| A source race loser is served as a real failure. | Hold the activity pending. Do not serve an abandoned dispatch. |
| Offloaded payloads never match, and blobs of a deleted source vanish. | Match inflated payloads. Copy the payload references. |
| A recorded fork takes a production mutex. | Refuse a mutex acquire in recorded mode. |
| Erasure does not reach a fork. | Refuse a fork whose lineage reaches an erased run. Document per-fork erasure. |
| An input override skips start checks. | Check the input schema and the byte cap. |
| A missing runtime writes overrides with no codec. | Return `503`. |
| A member of a closed source session fails with `SessionBroken`. | Resolve from the record before the session check. Skip each settled activity there. |
| A transient store fault becomes `ForkEffectUnavailable`. | Return the load error. The decision rolls back and retries. |
| A live fork stalls on a fault in the source history. | Load the source only in recorded mode. |
| A text search of blob keys misses an escaped key. | Match the keys of parsed envelopes exactly. |
| A prefix of 16,384 events or more exceeds the bind limit. | Insert the prefix in chunks of 1,000 rows. |
| A request with 16,383 overrides or more exceeds the bind limit. | Refuse more than 1,000 overrides with `400`. |
| An override output skips the result byte cap. | Check each output against the cap of its activity. |
| A fork skips tenant quota admission. | Admit the fork under the quota key of its input, and store the key. |
| A held race loser parks the run when the fork no longer runs its winner. | Hold a loser only beside a served source sibling. Otherwise fail it closed. |
| A source with no stored quota key skips admission. | Resolve the key from the decoded source input. |
| The override cap refuses an output that the offloader would store. | Exempt it, as the worker exempts a real result. |
| 16,384 payload references or more exceed the bind limit. | Insert the references in chunks of 1,000 rows. |
| An activity failure with the race text is held as a race loser. | Match the whole shape of the engine terminal. |
| A race won by a timer fails its loser closed. | Also hold a loser beside a pending timer of the same fork decision. |
| A migrated fork misses its source and its lineage on the target shard. | A `ForkLineage` quiescence blocker keeps a fork on its shard. |
| Retention deletes an erased ancestor, and the gap reads as a clean lineage. | Refuse a lineage with a missing ancestor (`LineageGap`, `409`). |
| A kept input reuses a stale quota key. | Resolve the key from the decoded input under the current policy. |
| A fork skips the admission gate and load shedding. | Run `admit_fresh_start` in `GateMode::Check` before the insert. |
| The history cap does not see the copied prefix. | Add the prefix bytes to the usage before the insert. |
| The history cap misses a new input and the appended events. | Measure the stored fork rows after the insert with `pg_column_size`. A fork over a cap rolls back. |
| A held race loser gets no loser terminal, so its `ActivityScheduled` stays open. | Mark the held task; `cancel_activity_task` treats the mark as open. |
| Retention can delete a record source while a recorded fork runs. | Not fixed: the fork fails closed. The runbook says to fork inside the retention window or hold the source. |
| A shard migration can move a record source away from its live fork. | The `LiveFork` quiescence blocker keeps each run in the fork lineage of a live fork on its shard. The walk reaches past a fork that a reset sealed. |
| A fork history at the worker event cap or byte cap dead-letters on its first task. | Refuse the fork with `409`. The check reads the stored rows, and the refusal rolls the fork back. |
| A loser is held beside a served sibling of an enclosing join, so nothing cancels it. | Hold only for the provable winner of the same race: the one sibling that resolved before the cancel. |
| A fork audit goes to the default shard, not the source shard. | Audit on the source-shard connection, as a reset does. |

## 2. Tests

| AC | Test |
|----|------|
| A completed run can be forked; the source is unchanged. | `fork_of_a_completed_run_leaves_the_source_unchanged` |
| By default, a fork does not run an activity again. | `recorded_fork_does_not_run_a_completed_activity_again` |
| Live side effects need an explicit flag. | `effects_default_to_recorded`, `live_fork_runs_the_activity` |
| An erased source is always refused. | `fork_refuses_an_erased_source`, `a_fork_of_a_fork_of_an_erased_source_is_refused` |
