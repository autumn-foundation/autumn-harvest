## Engine — non-destructive fork of any run (issue #2000)

**New route.** `POST /workflows/{id}/fork` (admin-only). It copies a history
prefix of a source run to a new execution with a new workflow id. The source
is never written, so a run in any state can be forked, `COMPLETED` and
`TERMINATED` included. Engine entry point:
`fork::fork_workflow_execution`. Runbook: `docs/runbooks/fork-a-run.md`.

**Effects.** `effects` defaults to `recorded`:

- A remote activity takes a caller override, or the result that the source
  recorded for the same name, occurrence and input. With neither, it fails
  non-retryably with `ForkEffectUnavailable`. The worker resolves it in the
  transaction that schedules it, so no worker runs it.
- A local activity, a child workflow, an external activity, an external
  signal, an external cancel, a continue-as-new or a mutex acquire fails the
  fork run before it runs. A successor run would hold no fork marker.
- A race branch that lost in the source stays pending, so the same branch
  wins again. It stays pending only when the same decision serves a sibling
  that won in the source. Otherwise nothing would wake the run, so the
  branch fails with `ForkEffectUnavailable`. A race loser is the exact
  terminal that the engine writes, not any failure with the same text. A
  held loser keeps a mark on its task, so the race still records its loser
  terminal when the winner resolves. A
  loser also stays pending when the same fork decision started a timer that
  has not fired, because a timer can win a race too.
- The fork sends no completion callback and fires no completion trigger.
- A reset of a fork keeps `start_source = fork` and appends a marker with the
  mode of that fork, so it keeps that mode. It copies the overrides of that
  fork after the new marker. A fork of a fork uses its own, last, marker.
- The worker reads the record source `FOR SHARE` and checks it for erasure
  before it copies a recorded result. A failed read of the source rolls the
  decision back, so it retries.
- The record settles an activity before the broken-session check. A member
  of a session that the source closed gets its recorded result, not
  `SessionBroken`.

`"effects": "live"` runs effects for real. An override applies in both modes.
A live fork never reads the source history after the fork exists.

**Overrides.** `input` replaces the workflow input at fork point `0`. It
passes the input schema (issue #373) and the byte cap (issue #252).
`activity_overrides` sets the result of one activity occurrence after the
fork point. A request sets at most 1,000 overrides
(`fork::MAX_ACTIVITY_OVERRIDES`). Each output passes the result byte cap of
its activity (issue #252), unless the offloader stores it out of line.

**Refusals.** An erased source (issue #495) is always refused, under a
`FOR SHARE` lock on the source row. So is a fork whose fork lineage reaches
an erased run. Erasure does not reach a fork, which is a new root, so erase
each fork on its own. A fork point at or after a terminal
event, a carried `MutexGranted` and a continue-as-new history are refused.
As for a rerun, a draining source shard and a business key held on any
shard are refused. The erased-lineage walk locks each ancestor and fails
closed past 64 links. A lineage that reaches a deleted run fails closed
too, because retention can delete an erased run.
In recorded mode, a source suffix with an effect that the mode cannot serve
is refused with `409`. A mutex grant after the fork point counts as one. A
fork history that reaches the worker history event cap or byte cap (issue
#1804) is refused with `409` too. The first workflow task of that fork would
dead-letter it.

**Admission.** A fork is a fresh start. An admission gate (issue #618)
refuses it with `503`, and load shedding (issue #1794) with `429`.

**Quota.** A fork is admitted under the tenant quota of its workflow type
(issue #946), as a start is. The key resolves from the fork input under
the current policy: a kept input is decoded, and a new input is used as
is. A fork over a cap is refused with
`429`, and the fork row stores its key. A fork starts with a history, so
the history quota and the worker caps count it. The fork measures its
stored rows after the insert, in the same transaction, with
`pg_column_size`. That is the measure of the quota and the worker, so it is
exact. A fork over a cap rolls back.

**Shard rebalancing.** A fork never migrates on its own (issue #964). It
reads its source and walks its lineage on its own shard, so the new
`ForkLineage` quiescence blocker keeps it with them. The source cannot move
away from a fork either. The new `LiveFork` blocker keeps a run on its
shard while it is in the fork lineage of a live fork. A reset of a fork
reads the source of the fork it resets, so the walk follows the lineage up.

**Lineage.** The fork row has `start_source = fork` and
`start_source_ref = <source id>`. Its history holds a `WorkflowForked`
marker. A fork is a new root, not a child of the source.

**Payloads.** The fork copies the payload references of the source whose
blob key an offload envelope in its stored rows or kept input names, so
retention of the source keeps the shared blobs. The key match is exact. An input override replaces only the stored `data.input`, so an
offloaded carryover stays an envelope. Matching inflates offloaded payloads
first.

**Invariants.** Two new `WorkflowEvent` variants: `WorkflowForked` and
`ForkActivityResultOverridden`. Replay skips both. The override `output` is a
payload field, so the codec encodes it and erasure tombstones it. A fork
inserts new event rows only. It is not an exception to the append-only rule
of `harvest_events`. New `StartSource::Fork` and audit operation
`workflow.fork`. A fork is audited on the shard of its source, as a reset
is. No migration.

**Rolling deploy.** A worker that predates this change cannot decode the new
events. Fork a run only after every worker runs this version.

**Tests.** `fork_tests.rs` (DB): a completed run forks and its row and
history stay byte-identical; a recorded fork does not run a completed
activity again; a live fork does; a fork with no record fails closed; an
override replaces a result; a recorded fork fails before a local activity;
a recorded fork serves a member of a closed session; a live fork runs
with a source blob gone; a prefix of 16,501 events copies in chunks;
an erased source is refused in both modes; a fork of a fork of an erased run
is refused; a later fork point carries the prefix; a reset of a recorded
fork stays recorded; only a recorded fork suppresses completion
notifications; a running source stays running; a workflow id in use is
refused. `workflow_fork_integration.rs` covers the HTTP contract. Unit tests in `fork.rs` cover occurrence
matching, input mismatch, erased records, overrides, recorded failures,
race losers, the last-marker rule, a missing marker, the fork-point checks
and the live-effect guard.
