# Runbook: Replay Non-Determinism Block (issue #603)

A deploy changed workflow code in a way that makes in-flight executions
**diverge on replay**: the code now generates a different command than the one
recorded in the execution's history. Before issue #603 this terminally failed
every replaying execution. Now the engine **blocks** each affected execution
non-terminally:

- **No terminal event.** No `WorkflowFailed` is appended; the append-only
  history is untouched by the divergent cycle (all of its pending commands are
  discarded).
- **State stays `RUNNING`.** The execution remains re-claimable; the workflow
  task is re-pended with `scheduled_at = now + backoff`.
- **Bounded retry rate.** Backoff is capped-exponential: `5s * 2^n`, ceiling
  **300s** (`ND_BLOCK_BACKOFF_BASE_SECS` / `ND_BLOCK_BACKOFF_CAP_SECS` in
  `worker.rs`). Retries are otherwise **unbounded** — the block is
  rate-limited, not attempt-capped, so a rollback at *any* later time still
  resumes the execution. A permanently diverging history costs one dispatch
  per ≤300s, never a hot loop.
- **Diagnostic stamped.** The execution row records `nd_blocked_at` (most
  recent observation), `nd_block_reason` (the divergence error), and
  `nd_block_count` (consecutive blocks — drives the backoff); `search_attrs`
  carries the structured fields `failure_cause=non_determinism`, `expected`,
  `actual`, `event_index`, `workflow_type`, `build_id`.
- **Parents are not notified.** A blocked child workflow never delivers
  `ChildWorkflowFailed`; its parent simply stays suspended and resumes when
  the child completes after the rollback.
- **Author errors are unaffected.** A workflow body's own `Err(...)` carries
  no divergence details and still terminates `FAILED` exactly as before. Only
  engine-detected divergence blocks.

## Detect

- **Metric**: `harvest.workflow.nondeterministic_block{workflow, queue}`
  increments on every block entry (alert `harvest_workflow_non_determinism`
  in the starter pack pages on it within one scrape interval). The companion
  detection counter `harvest.workflow.non_determinism{workflow, build_id}`
  (#480) also still fires.
- **Fleet query**:
  ```bash
  GET /api/harvest/workflows?nd_blocked=true
  ```
  Lists every RUNNING execution currently blocked, with the diagnostic columns
  on each row. Composes with the other list filters (`workflow_name=`,
  `no_progress_minutes=`, pagination).

## Identify the offending build

For any blocked row, read `search_attrs`:

| Field | Meaning |
|---|---|
| `expected` | what the **currently deployed code** asked for at the divergence point |
| `actual` | what the **recorded history** holds at that position |
| `event_index` | history position of the divergence |
| `workflow_type` | the workflow type — the whole in-flight cohort of this type is affected |
| `build_id` | the worker build that observed the divergence — this is the deploy to roll back |

`GET /api/harvest/workflows/{id}` returns the same fields plus
`nd_block_reason` / `nd_block_count` / `nd_blocked_at` on the embedded
execution object.

## Roll back

Roll the identified `build_id` back to the last known-good version (or ship a
fix that restores replay compatibility — `ctx.version()` gates, or build-id
routing per `docs/runbooks/safe-deploy.md`, prevent this class of incident
up front).

No Harvest-side action is required: each blocked execution re-dispatches on
its own backoff schedule (≤300s), replays cleanly under the compatible build,
and continues from exactly where it was — zero data loss, no manual reset.

## Confirm resume

- `GET /api/harvest/workflows?nd_blocked=true` drains to empty.
- On a recovered execution, `nd_blocked_at` / `nd_block_reason` are `NULL`,
  `nd_block_count` is `0`, and the six diagnostic keys are removed from
  `search_attrs` (cleared atomically with the first clean cycle's persisted
  outcome).
- The block metric rate returns to zero.

## Behavior while blocked

- **Signals** are durable but not processed until the next backoff
  re-dispatch — worst case one backoff period (≤300s) of added latency. The
  re-dispatch will just re-diverge under the bad build, so early wakes are
  deliberately not honored.
- **Cancel / terminate / pause** all work normally (the state is `RUNNING`).
  They are the escape hatches for an execution you decide not to recover.
  Note: cancel/terminate leave the stale `nd_block*` marker on the closed row
  (the `nd_blocked=true` filter hides non-RUNNING rows). Note: a terminate
  landing in the narrow window between the block transaction's row-lock check
  and its commit can surface as a transient error on that one dispatch cycle
  instead of a graceful no-op (the same pre-existing race any ordinary
  terminal-`Failed` persistence shares) — retried automatically on the next
  poll, it is not a permanent failure.
- **Execution timeout** (#243) still applies: a blocked execution whose
  `deadline_at` elapses is timed out terminally by the scanner. If a long
  rollback is expected, consider whether affected runs carry tight execution
  timeouts.
- **Sticky affinity is cleared** on block, so the re-dispatch can be claimed
  by any worker — including one already running the rolled-back build during
  a mixed-fleet rollout.

## Escalation: reset-from-history

Rollback is the fix for the overwhelming majority of divergences (the replay
fails while re-checking the recorded prefix, before anything new is written).
Two cases need the reset API (#148/#538) instead:

1. **Rollback impossible** (the old build cannot be restored).
2. **Still blocked under the rolled-back build**: if the divergent build's
   decision cycle appended inline events (e.g. a local-activity batch) *before*
   diverging, the old code may now diverge against those events. `nd_blocked_at`
   keeps re-stamping with `build_id` of the rolled-back build — that is the
   signal for this case.

```bash
POST /api/harvest/workflows/{execution_id}/reset
```

Reset to an event index before the divergence (`event_index` in the
diagnostic), which forks a fresh run from the compatible prefix.

## Prevention (complementary, not replaced by this safety net)

- CI replay gate: `WorkflowReplayer` fixtures (#250).
- Pre-deploy canary over live histories (#512).
- `ctx.version()` gates for intentional logic changes (DD-3).
- Build-id routing so old executions only run on compatible builds (#171,
  `docs/runbooks/safe-deploy.md`).
