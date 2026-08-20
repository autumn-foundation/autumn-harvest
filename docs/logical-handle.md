# The logical handle — routing across the workflow-level retry chain

*Issue #843. Builds on workflow-level retry (#523) and the chained result wait (#842).*

## The problem

Workflow-level retry does **not** resume a failed run in place. Each attempt is a
**fresh execution** with its own `exec_id`, its own UUID `workflow_id`, and its
own clean event history, linked back to its predecessor by `retry_of_exec_id`.
The predecessor stays sealed `FAILED` forever — that is what makes replay of the
failed attempt reproducible.

That leaves a caller holding the id `start` returned with a stale pointer. Before
#843, the *result wait* already followed the chain (#842), but every interactive
and mutating operation did not:

| Operation on a retried run's original id | Before #843 |
|---|---|
| `cancel` | Cancels the sealed `FAILED` predecessor; the retry keeps running |
| `terminate` | Silently no-ops (terminate is idempotent on any terminal state) — the sharpest failure mode |
| `signal` | Queues a row against the sealed predecessor; **never** delivered |
| `query` | `WorkflowNotRunning`, or reads the failed attempt's state |
| `update` | Rejected — the addressed execution is not `RUNNING` |
| `result_snapshot()` (zero-wait) | Reports `FAILED` while the retry is still running |

## The contract

**The id `start` returned names the *logical run*, not one attempt.** Every
interactive and mutating operation is routed to the **live attempt**: the
deepest execution reachable by following `retry_of_exec_id` successors from
`FAILED` rows.

Resolution (`autumn_harvest::execution::resolve_live_attempt`) is:

> While the current row is `FAILED` **and** a successor with
> `retry_of_exec_id = id` exists, advance to that successor; otherwise stop.

So it returns:

* the row itself for any non-`FAILED` state — a strict **no-op** for every
  workflow that has no retry policy, and for a live `RUNNING`/`PAUSED` run;
* the row itself for a `FAILED` run that is the chain's **final** outcome (no
  retry was scheduled), so post-mortem operations still target it;
* otherwise the deepest, most recent attempt.

### There is no "no execution row" window

The issue that motivated this work worried about a gap "between attempt 1 sealing
`FAILED` and attempt 2 being claimed." **There is no such gap at the row level.**
The retry successor is INSERTed in the *same transaction* that seals the
predecessor `FAILED` and appends `WorkflowRetryScheduled`. An external reader
either sees the predecessor still live, or sees it `FAILED` with its successor
already present.

What *is* delayed is the successor's **task** (a retry with a backoff delay has a
future `scheduled_at`). That is a window with no *claimed* task, not a window
with no *execution*, which is why routing alone is sufficient.

## What each operation does

### Signals — "deliver on the next attempt"

A signal is routed to the live attempt and queued in `harvest_signals` as usual.
If the live attempt's task is still delayed, the signal simply waits there and is
ingested into history when the task is claimed — exactly the behaviour a signal
sent to any not-yet-claimed workflow already gets. **No signal is rejected, and
no synthetic buffering layer exists.**

**Unconsumed signals are forwarded across the retry boundary.** When a retry is
scheduled, the same transaction moves every still-unconsumed `harvest_signals`
row from the predecessor to the successor. This mirrors the long-standing
continue-as-new precedent: a signal that was never ingested into any history is
not reflected anywhere, so moving it loses nothing, while leaving it behind would
strand it against a sealed row forever. Already-consumed rows stay put for audit.

**Idempotency keys (#521) keep their scope.** Dedupe stays keyed on
`(workflow_exec_id, idempotency_key)`. A key that landed on attempt *N* does not
suppress the same key on attempt *N+1* — each attempt is a genuinely distinct
execution with a fresh history, so re-delivering the signal to the new attempt is
the correct behaviour, not a duplicate.

### Cancel — cancels the live attempt *and* stops a queued retry

`cancel` routes to the live attempt. That also covers "prevent a queued retry
from starting", by two mechanisms already in the engine:

* a retry whose start delay has **not** elapsed has its still-delayed `PENDING`
  workflow task deleted by the cancel; and
* a retry whose task is already claimable is sealed `CANCELLED` before it can
  commit any non-cancelled terminal — both `update_workflow_execution_completed`
  and `_failed` filter on `state = 'RUNNING'`, and the body observes
  `ctx.is_cancelled()` as true from its first line.

So a cancelled chain can never spawn a further attempt.

### Terminate — force-seals the live attempt

`terminate` routes to the live attempt and fails **every** open (`PENDING` +
`RUNNING`) task row of that attempt, so a queued retry cannot subsequently be
claimed.

### Queries and updates — the live attempt

Both route to the live attempt: a query replays the live attempt's history and
serves its reconstructed state; an update is admitted onto the live attempt.
Because the successor row always exists (above), there is no "gap" behaviour to
define — an update admitted while the retry's task is still delayed is handled
when that task is claimed, exactly like any admitted update on a parked run.

### `result_snapshot()` — follows the chain

The zero-wait snapshot now follows the chain, matching what `result_snapshot_with_wait`
and the HTTP `GET /workflows/{id}/result` route already did. The core handle was
the outlier and was internally incoherent with its own waiting sibling.

### Describe is deliberately **not** routed

`GET /workflows/{id}` (describe), history export, the timeline, the stack, and
the DLQ are **specific-`exec_id` reads**. They must keep reporting the addressed
row so an operator can inspect exactly the attempt that failed. Use
`GET /workflows/{id}/run-chain` (#701) or the `retry_of_exec_id` linkage to walk
attempts explicitly.

## Surfaces

Routing is applied consistently across:

* the core `WorkflowHandle` (`cancel`, `terminate`, `signal`, `query`, `update`,
  `result`, `result_snapshot`);
* `TypedWorkflowHandle`, which wraps the same untyped handle;
* the HTTP routes `POST /workflows/{id}/signal/{name}`, `/cancel`, `/terminate`,
  `GET|POST /workflows/{id}/query/{name}`, `POST /workflows/{id}/update/{name}`,
  and `GET /workflows/{id}/result`.

### Observing where an operation landed

`POST /workflows/{id}/signal/{signal_name}` reports an additional
`routed_execution_id` field **only when** the addressed id differed from the live
attempt the signal actually landed on. It is omitted otherwise, so a
non-retried run's response is byte-for-byte what it was before #843.
`/cancel` and `/terminate` already carry `execution_id`, which now reports the
attempt that was acted on.

## Races

A mutating operation resolves the live attempt, acts on it, and — **only when the
act provably did not take effect** — re-resolves and tries again. Re-driving is
correct exactly when the chain advanced underneath the operation (the attempt we
acted on sealed `FAILED` and spawned its successor between our resolution and our
act).

The loop is deliberately **never** entered after an operation that *did* take
effect: re-driving a delivered signal would double-deliver it. Concretely:

* **signal** — re-drives only on an `Err`. A rejected insert is rolled back, so
  nothing was delivered.
* **update** — re-drives only on an `Err`. `admit_update_event` verifies
  `RUNNING` under the same `FOR UPDATE` lock it inserts under and rolls back on
  rejection, so a re-driven admit can never double-admit.
* **cancel** — re-drives only on an `Err`.
* **terminate** — additionally re-drives on an *idempotent no-op against a
  `FAILED` row*, which is the only signal available that the chain advanced (a
  no-op against any other terminal state is the final answer).

The loop terminates because each re-drive strictly descends the chain, and
because an unchanged target ends it — which is what stops a genuine error (an
unknown id, or an exhausted chain whose final outcome really is `FAILED`) from
being retried forever. `RETRY_CHAIN_MAX_DEPTH` (256) bounds both the walk and the
re-drive count so a pathological database state can never spin a request forever.

## Invariants

No new `WorkflowEvent` variant, no migration, no change to the adjacently-tagged
event JSON, and no replay-determinism impact. Routing is a read-side resolution
plus a re-target of already-existing mutating primitives; signal forwarding
reuses the existing `harvest_signals.workflow_exec_id` column and the same
transaction the retry already commits.
