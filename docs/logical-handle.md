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

**The whole signal mailbox is forwarded across the retry boundary, re-armed.**
When a retry is scheduled, the same transaction moves *every* `harvest_signals`
row from the predecessor to the successor and resets `consumed` to `false`.

Forwarding the *unconsumed* rows is obvious: a signal that was never ingested
into any history is not reflected anywhere, so leaving it behind would strand it
against a sealed row forever.

Forwarding the **consumed** ones is the less obvious half, and it is required.
`consumed = true` means "already ingested into *that attempt's* history" — and a
retry starts from a completely fresh, empty history and re-executes every step
from zero. A signal the failed attempt observed is therefore one the retry must
observe again, or a workflow that replays back to its `wait_for_signal` blocks
forever on a signal its caller was already told had been delivered (and has no
reason to re-send). Re-observing is consistent with the retry re-running every
activity: nothing from the discarded attempt carries over except the mailbox.
The failed attempt's own `SignalReceived` events stay in its history, so the
audit record of what it observed is unaffected by the move.

This deliberately **diverges** from the narrower reassignment `continue_as_new`
performs (unconsumed only). A continue-as-new is *voluntary*, and the workflow
explicitly carries whatever it still needs into the successor's input; a retry
carries nothing forward at all.

**Idempotency keys (#521) keep their scope.** Dedupe stays keyed on
`(workflow_exec_id, idempotency_key)`. Because a forwarded row takes its key with
it, a caller's at-least-once re-send of an already-delivered key dedupes against
the **live attempt** rather than being swallowed by a sealed predecessor. A key
that landed on attempt *N* and whose row has moved to attempt *N+1* therefore
still dedupes; a key delivered after the move that never existed on the chain is
delivered normally.

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

`GET /workflows/{id}/update/{update_id}/result` — the paired read for a
`wait=admitted` admission, whose `202` carries only the `update_id` — resolves
the chain the same way, so the caller's single follow-up id keeps working.

**Known limitation — an admitted-but-unresolved update is not carried across a
retry.** Unlike signals (a table), an update lives in the attempt's *history* as
an `UpdateAdmitted` event, and a retry starts from an empty history. An update
admitted onto attempt *N* that *N* fails before resolving is therefore lost: the
poll for its result on attempt *N+1* finds nothing and times out. Re-submit the
update against the logical handle (it routes to the live attempt). Carrying
admitted updates across the boundary would need a durable pending-update record
outside the event log and is tracked separately.

### `result_snapshot()` — follows the chain

The zero-wait snapshot now follows the chain, matching what `result_snapshot_with_wait`
and the HTTP `GET /workflows/{id}/result` route already did. The core handle was
the outlier and was internally incoherent with its own waiting sibling.

### Pause and resume — the live attempt

Both route. Pause is the *reversible* containment lever the runbook
(`docs/runbooks/contain-runaway-execution.md`) tells operators to reach for
first, with cancel/terminate as escalation — and `pause_workflow_execution`
accepts only a `RUNNING`/`PAUSED` row, so an unrouted pause of a retried run
returns `409 … is already terminal (FAILED)`. Leaving it unrouted while cancel
and terminate *were* routed would have inverted the containment ladder, leaving
an operator holding the logical handle only the destructive levers. Resume is an
idempotent success no-op against a non-paused row, so unrouted it would report
success while the paused live attempt stayed parked — the same silent-no-op
failure mode routing closes for terminate.

### Describe is deliberately **not** routed

`GET /workflows/{id}` (describe), history export, the timeline, the stack, the
awaitables, the per-execution logs, the event stream, `/diagnose`, the registered
`/queries` listing, and the DLQ are **specific-`exec_id` reads**. They must keep
reporting the addressed row so an operator can inspect exactly the attempt that
failed. Use `GET /workflows/{id}/run-chain` (#701) or the `retry_of_exec_id`
linkage to walk attempts explicitly.

### Operations deliberately **not** routed

Routing is scoped to the *interactive and mutating* surface issue #843 names
(signal, cancel, terminate, pause, resume, query, update, `result_snapshot`).
These mutating operations are deliberately left addressing the specific
execution, because each one acts on the **recorded artifact** of one attempt
rather than steering the live run:

| Operation | Why it stays specific-`exec_id` |
|---|---|
| `POST /workflows/{id}/reset` (#148) | Forks a *new* run from a specific attempt's recorded history; the attempt is the input, not a pointer to be re-aimed |
| `POST /workflows/{id}/erase-payloads` (#495) | Scrubs one terminal attempt's stored payloads. Erasing a logical run across a whole chain is a separate, wider operation |
| `POST /workflows/{id}/legal-hold` (#747) | Holds one attempt's history against retention, same reasoning as erase |
| `POST /workflows/{id}/triage` (#814) | Annotates one attempt's post-mortem record |
| `.../activities/{id}/retry-now`, `.../fail-now` (#516, #765) | Address a specific task row, which belongs to exactly one attempt |
| `POST /dead-letters/{id}/replay`, `/redrive` | Address a DLQ row, likewise |

`POST /workflows/{id}/rerun` (#777) takes the **opposite** policy and *rejects*
a chain predecessor outright, with an error telling the operator to re-run the
chain's latest attempt. That is deliberate: re-run mints a brand-new logical run
from an attempt's inputs, so silently re-aiming it at a different attempt would
change which inputs the new run gets. Routing it would be a behaviour change
outside this issue's scope.

An erase / legal-hold that spans the whole retry chain is a genuine (GDPR-
relevant) gap; it is tracked separately rather than smuggled into this change.

## Surfaces

Routing is applied consistently across:

* the core `WorkflowHandle` — `cancel`, `terminate`, the in-process query and
  update paths, `result`, and `result_snapshot`. (`WorkflowHandle` itself has no
  `signal` method; signalling from Rust goes through the generated typed stub
  below.)
* `TypedWorkflowHandle`, which wraps the same untyped handle — including
  `result_snapshot`, whose typed `error_type`/`error_details`/`non_retryable`
  are loaded from the execution the snapshot was actually read from, so a
  routed snapshot can never mix an outer run's error with an inner attempt's
  typed metadata;
* the `#[signal]`-generated typed client stubs, both `signal_{name}` and its
  `signal_{name}_idempotent` sibling;
* the HTTP routes `POST /workflows/{id}/signal/{name}` (and the
  `by-id/{workflow_name}/{workflow_id}` sibling), `/cancel`, `/terminate`,
  `/pause`, `/resume`, `GET|POST /workflows/{id}/query/{name}`,
  `POST /workflows/{id}/update/{name}`,
  `GET /workflows/{id}/update/{update_id}/result`, and
  `GET /workflows/{id}/result`.

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

* **signal** — re-drives on an `Err` (a rejected insert is rolled back, so
  nothing was delivered) *and* on a keyed dedup that reported "not delivered".
  A keyed insert short-circuits on the unique-index conflict **before** the
  state check, so it can report success against a row that has since sealed
  `FAILED`; treating that as final would swallow a re-send the live attempt
  still needs. Both shapes mean nothing was queued, so neither can
  double-deliver.
* **update** — re-drives only on an `Err`. `admit_update_event` verifies
  `RUNNING` under the same `FOR UPDATE` lock it inserts under and rolls back on
  rejection, so a re-driven admit can never double-admit.
* **cancel** and **pause** — re-drive only on an `Err`.
* **resume** — like terminate, additionally re-drives on an idempotent no-op
  against a `FAILED` row.
* **terminate** — additionally re-drives on an *idempotent no-op against a
  `FAILED` row*, which is the only signal available that the chain advanced (a
  no-op against any other terminal state is the final answer).

The loop terminates because each re-drive strictly descends the chain, and
because an unchanged target ends it — which is what stops a genuine error (an
unknown id, or an exhausted chain whose final outcome really is `FAILED`) from
being retried forever. `RETRY_CHAIN_MAX_DEPTH` (256) bounds both the walk and the
re-drive count so a pathological database state can never spin a request forever.
Exhausting the walk depth is logged (`tracing::warn!`) rather than silently
returning a possibly-stale attempt as if it were live — it is unreachable for any
real chain, so hitting it means a corrupted `retry_of_exec_id` graph.

## Invariants

No new `WorkflowEvent` variant, no change to the adjacently-tagged event JSON,
and no replay-determinism impact. Routing is a read-side resolution plus a
re-target of already-existing mutating primitives; signal forwarding reuses the
existing `harvest_signals.workflow_exec_id` / `consumed` columns and the same
transaction the retry already commits.

One **index-only** migration (`20260723000000_harvest_retry_chain_index`) adds a
partial index on `retry_of_exec_id`. Routing moves the successor lookup onto the
hot path of every mutating operator endpoint, and the costly case is the *miss*
— proving "this `FAILED` run has no retry successor" — which without an index is
a full sequential scan of the hub table. It mirrors
`idx_harvest_wfx_continued_from`, the structurally identical successor-link index
for the continue-as-new chain (#701). No column is added and no data is
rewritten.
