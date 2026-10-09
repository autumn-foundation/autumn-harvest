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
  signal or an external cancel fails the fork run before it runs.
- The fork sends no completion callback and fires no completion trigger.

`"effects": "live"` runs effects for real. An override applies in both modes.

**Overrides.** `input` replaces the workflow input at fork point `0`.
`activity_overrides` sets the result of one activity occurrence after the
fork point.

**Refusals.** An erased source (issue #495) is always refused, under a
`FOR SHARE` lock on the source row. A fork point at or after a terminal
event, a carried `MutexGranted` and a continue-as-new history are refused.
In recorded mode, a source suffix with an effect that the mode cannot serve
is refused with `409`.

**Lineage.** The fork row has `start_source = fork` and
`start_source_ref = <source id>`. Its history holds a `WorkflowForked`
marker. A fork is a new root, not a child of the source.

**Invariants.** Two new `WorkflowEvent` variants: `WorkflowForked` and
`ForkActivityResultOverridden`. Replay skips both. The override `output` is a
payload field, so the codec encodes it and erasure tombstones it. A fork
inserts new event rows only. It is not an exception to the append-only rule
of `harvest_events`. New `StartSource::Fork` and audit operation
`workflow.fork`. No migration.

**Rolling deploy.** A worker that predates this change cannot decode the new
events. Fork a run only after every worker runs this version.

**Tests.** `fork_tests.rs` (DB): a completed run forks and its row and
history stay byte-identical; a recorded fork does not run a completed
activity again; a live fork does; a fork with no record fails closed; an
override replaces a result; a recorded fork fails before a local activity;
an erased source is refused in both modes; a running source stays running;
a workflow id in use is refused. Unit tests in `fork.rs` cover occurrence
matching, input mismatch, erased records, overrides, recorded failures, the
fork-point checks and the live-effect guard.
