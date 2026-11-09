# DESIGN-2577: `agent_refine` follow-ups

Issue #2577. Parent: #2369. Follows #2380 and its design,
[`DESIGN-2380.md`](DESIGN-2380.md).

## Problem

`DESIGN-2380.md` records four limits of the `agent_refine` workflow:

1. A durable `agent` run of the kind passes the kind check. The start step
   then refuses it, because `eval` replays `agent_loop` runs only. An
   `agent` run of another kind gets the wrong error.
2. A worker crash during the wait of a live re-probe retries
   `agent_refine_probe`. The retry forks the source again.
3. A probe writes one cost ledger row for the sum of its turns, under the
   unknown model id.
4. A failed attempt that a later attempt replaces is in neither ledger.

## Decision

### 1. Refuse a durable `agent` source first

`brief_of` refuses an `agent` run directly after the workflow type check.
The refusal comes before the `ended` check and the kind check. So each
`agent` run gets `RefineSourceNotReplayable`, of any kind and in any
state. `kind_of` then reads `agent_loop` runs only.

A replay probe of an `agent` run is a larger change. A durable agent runs
many ops in one entity history, and `eval` replays one task. That change
stays a later step.

### 2. Key the live fork on the activity

`agent_refine_probe` reads its idempotency key. The key is the same for
each attempt of one scheduled probe. The probe passes the key to the host
in `LiveRequest::fork_key`.

`eval::live_probe_keyed` forks under the workflow id
`agent-probe-<key>`. `LiveProbeHost::fork_live_as` first looks for
a run of that id on the source shard:

- A run that holds the id, and that the source forked, is the fork. The
  host returns it, and forks nothing.
- A run that holds the id, but that another source forked, is an error.
- With no such run, the host forks under the id. A concurrent fork that
  wins the id makes the fork fail with `WorkflowIdInUse`. The host then
  looks again and returns the winner.

A fork id is held while its run is not terminated, so a completed or a
cancelled fork is found too. Its retry reads its end and starts no second
fork. Only a terminated fork frees the id.

The workflow history does not change. No patch marker is needed.

### 3. One probe ledger row for each turn

`Evaluation::candidate_calls` holds one `Charge` for each candidate turn
that called a model, in order. The charge names the model id of the route
of the turn, its tokens and its cost. A cache hit adds none.
`probe_calls` writes one cost ledger row for each charge. So the sum of the
rows is the probe spend, and each row names its model.

The anonymised pool leaves out `candidate_calls`. A model id names a
model.

### 4. The spend of a replaced attempt

The refine ledger cannot hold it. The reasons are in the engine:

- A retry of an activity writes no event. The workflow reads the result of
  the last attempt only.
- A retry attempt reads the error text of the attempt before it, from
  `ActivityContext::previous_failure`. The text holds no failure details,
  so it holds no spend.
- A heartbeat flushes after the call returns. A requeue can come first, so
  a spend in a heartbeat can be lost.
- A worker crash ends an attempt with no report at all.

A workflow-level retry could count the first three cases. It would copy the
retry policy of the engine into the workflow, and it would still miss a
crash. The cost ledger would still hold no row of the failed attempt, so
the two ledgers would disagree. The fix belongs in the engine: an attempt
failure that keeps its recorded calls.

The failure details of the last attempt still reach the refine ledger, as
before.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| A durable `agent` source | A replay probe of the entity history. A refusal before the kind check. | The refusal. `eval` replays one task, and an entity runs many. |
| Where the refusal goes | Before the kind check. Before the `ended` check too. | Before both. An entity run can run for a long time, so "in flight" names the wrong cause. |
| A reused fork | Key the fork on the activity. Split the fork and the wait into two activities. | The key. A split changes the history shape, so it needs a patch and two steps per probe. |
| The fork key | The attempt number. The activity idempotency key. | The idempotency key. It is the same for each attempt. |
| Where the lookup runs | In the refine probe. In the probe host. | The host. It knows the shard and the engine errors. |
| The model of a turn | The route name. The model id of the route. | The model id, as a live turn records it. |
| Where the turn spend lives | `ProbeResult`. `Evaluation`. | `Evaluation`. Other `eval` callers get it too. |
| A replaced attempt | A workflow-level retry. A heartbeat. A design statement. | A design statement. The other two miss a crash, and one is lossy. |

## Reverse brainstorm

How can the change spend twice, or record a false row? Each failure has a
guard.

| Failure | Guard |
|---|---|
| A retry forks a second time. | The fork id comes from the idempotency key. The host finds the first fork. |
| Two attempts fork at the same time. | The engine refuses a held id. The loser reads the winner. |
| The key names a run of another source. | The host checks the fork marker. A mismatch is an error. |
| A reused fork ran to its end before the crash. | The host returns it. The wait reads its end at once. |
| An operator terminated the fork. | The id is then free. The retry forks again, as the operator asked. |
| A per-turn row sums to a value other than the probe. | `candidate_calls` and `candidate_usage` come from the same turns. A test compares the sums. |
| A model id leaks into the pool. | `anonymise` names `candidate_calls` an identity and drops it. |
| A bad model id fails the ledger write. | `Charge::ledger_call` records it under the unknown model id. |
| An evaluation of an older release has no turns. | `candidate_calls` decodes with none. |
| An `agent` run of another kind gets `SourceOtherKind`. | The refusal comes before the kind check. |

## Six hats

- **White.** `IdempotencyKey` is stable across attempts. A fork takes a
  caller workflow id and refuses a held one. A live turn records
  `AgentModel::model_id`. A retry writes no event.
- **Red.** A second fork runs real tools again. That is the worst outcome
  here, so its test kills the probe during the wait.
- **Black.** A held fork id blocks a later probe with the same key. Only
  the same probe has the key, so that is the wanted result. The turn list
  makes each probe result longer.
- **Yellow.** A crash costs no second fork. An operator can read the spend
  of each model in a refine run.
- **Green.** An engine change can keep the calls of a failed attempt. Then
  item 4 can close.
- **Blue.** Spec first, then red tests, then the code, then the gates and
  a multi-angle review.

## Tests

- `refine_run.rs`: an `agent` source of another kind, and a live one, fail
  with `RefineSourceNotReplayable`. Each probe ledger row names the model.
- `refine_probe_postgres.rs`: a probe that dies during the wait, then a
  retry with the same key, makes one fork.
- `eval.rs` unit tests and a proptest: the turn charges sum to the
  candidate spend.
- `eval/probe.rs` unit tests: the keyed probe reuses a fork, and refuses a
  fork of another source.
