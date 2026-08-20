## Phase 3.x — Logical-handle routing across the workflow-level retry chain (issue #843)

Workflow-level retry (#523) starts each attempt as a **fresh execution** with a
new `exec_id`, a new UUID `workflow_id`, and a clean history, linked to its
predecessor by `retry_of_exec_id`; the predecessor stays sealed `FAILED`. The
*result wait* already followed that chain (#842), but every **interactive and
mutating** operation did not — a caller still holding the id `start` returned
would cancel a sealed predecessor while the retry kept running, silently no-op a
`terminate` (terminate is idempotent on any terminal state — the sharpest failure
mode), queue a signal against a row that will never be claimed, or be told the
run is not running by a query/update. This slice defines and implements the
**logical handle** contract: the original id names the *logical run*, and every
interactive/mutating operation is routed to the **live attempt** (the deepest
execution reachable by following `retry_of_exec_id` successors from `FAILED`
rows).

**No new `WorkflowEvent` variant, no change to the adjacently-tagged event JSON,
no replay-determinism impact** — this is a read-side resolution plus a re-target
of existing mutating primitives, and signal forwarding reuses the existing
`harvest_signals.workflow_exec_id` / `consumed` columns inside the transaction the
retry already commits. **One index-only migration**
(`20260723000000_harvest_retry_chain_index`) adds a partial index on
`retry_of_exec_id`: routing moves the successor lookup onto the hot path of every
mutating operator endpoint, and the costly case is the *miss* (proving a `FAILED`
run has no successor), which without an index is a full sequential scan of the hub
table — measured ~63 ms parallel seq scan vs ~0.03 ms index scan on 500k rows. It
mirrors `idx_harvest_wfx_continued_from`, the structurally identical successor-link
index for the continue-as-new chain (#701). No column added, no data rewritten.

**Refuted the issue's motivating premise.** The issue worried about a window
"between attempt 1 sealing `FAILED` and attempt 2 being claimed" with no running
execution. Verified in `worker.rs` that the successor row is INSERTed in the *same
transaction* that seals the predecessor `FAILED` and appends
`WorkflowRetryScheduled`, so an external reader either sees the predecessor still
live or sees it `FAILED` with its successor already present. **There is no
row-existence gap** — only a window with no *claimed task*. That collapsed the
"needs design" concern: signals need no synthetic buffering layer and no rejection
window, because "deliver on the next attempt" falls out of routing plus the
engine's existing claim-time signal ingest.

**Core (`execution.rs`).** New `resolve_live_attempt` / `resolve_live_attempt_id`
(walk while `FAILED` and a successor exists; a strict no-op for every non-`FAILED`
row and for a `FAILED` run that is the chain's final outcome, so post-mortem
operations still target it), `RETRY_CHAIN_MAX_DEPTH` (256) bounding both the walk
and the re-drive count, the pure `redrive_target` decision helper, and the routed
`cancel_live_attempt` / `terminate_live_attempt`.

**Race handling — re-drive only when the act provably did not take effect.**
A blanket "re-resolve after every op and redo" design was rejected: re-driving a
*delivered* signal would double-deliver it. So signal/update/cancel re-drive only
on an `Err` (a rejected insert is rolled back; `admit_update_event` verifies
`RUNNING` under the same `FOR UPDATE` lock it inserts under, so a re-driven admit
cannot double-admit); terminate and resume additionally re-drive on an *idempotent
no-op against a `FAILED` row* — the only available signal that the chain advanced (a
no-op against any other terminal state is the final answer); and a **keyed** signal
additionally re-drives on a dedup that reported "not delivered", because a keyed
insert short-circuits on the unique-index conflict *before* the state check and can
therefore report success against a row that has since sealed `FAILED`, swallowing a
re-send the live attempt still needs. Every re-driven shape provably queued nothing,
so none can double-deliver. Exhausting the walk depth is logged (`tracing::warn!`)
rather than silently returning a possibly-stale attempt as if it were live, and the
successor lookup carries a deterministic `ORDER BY` tie-break so the walk is total
and stable across the two resolutions of a re-drive. The loop terminates
because each re-drive strictly descends the chain and an unchanged target ends it,
which is also what stops a genuine error (unknown id, exhausted chain) from being
retried forever.

**Signals — "deliver on the next attempt", with the whole mailbox forwarded.** New
`signal::send_signal_to_live_attempt` (returning `RoutedSignalDelivery { target,
delivered }`), its already-resolved-target sibling `send_signal_from_resolved`, and
`signal::forward_signals_to_retry_attempt`. The retry transaction in `worker.rs`
now moves **every** `harvest_signals` row from the predecessor to the successor and
resets `consumed` to `false`. Forwarding the *unconsumed* rows is obvious (a signal
never ingested into any history is not reflected anywhere, so leaving it behind
strands it against a sealed row forever). Forwarding the **consumed** ones is the
less obvious half and is required: `consumed = true` means "already ingested into
*that attempt's* history", and a retry starts from a completely fresh, empty history
and re-executes every step from zero — so a signal the failed attempt observed is
one the retry must observe again, or a workflow that replays back to its
`wait_for_signal` blocks forever on a signal its caller was already told had landed
(and has no reason to re-send). The failed attempt's own `SignalReceived` events
stay in its history, so the audit record is unaffected. This deliberately diverges
from the narrower reassignment `continue_as_new` performs (unconsumed only) — a
continue-as-new is *voluntary* and carries what it needs in the successor's input,
whereas a retry carries nothing forward at all. **#521 idempotency keys keep their
`(workflow_exec_id, idempotency_key)` scope**, and because a forwarded row takes its
key with it, an at-least-once re-send of an already-delivered key now dedupes
against the **live attempt** rather than being swallowed by a sealed predecessor.

**Known limitation — an admitted-but-unresolved update is not carried across a
retry.** Unlike signals (a table), an update lives in the attempt's *history* as an
`UpdateAdmitted` event, and a retry starts from an empty history. An update admitted
onto attempt *N* that *N* fails before resolving is lost; re-submit it against the
logical handle. Carrying admitted updates across the boundary needs a durable
pending-update record outside the event log and is tracked separately.

**Cancel also satisfies "prevent a queued retry from starting"** by two mechanisms
already in the engine: a retry whose start delay has not elapsed has its
still-delayed `PENDING` workflow task deleted by the cancel, and a retry whose task
is already claimable is sealed `CANCELLED` before it can commit any non-cancelled
terminal (both `update_workflow_execution_completed` and `_failed` filter
`state = 'RUNNING'`, and the body observes `ctx.is_cancelled()` from line 1).
Terminate fails **every** open (`PENDING`+`RUNNING`) task row of the attempt it
seals.

**`result_snapshot()` follows the chain** — matching what `result_snapshot_with_wait`
and the HTTP `/result` route already did; the core handle was the outlier and was
internally incoherent with its own waiting sibling.

**Pause and resume route too** (`pause_live_attempt` / `resume_live_attempt`).
Pause is the *reversible* containment lever `docs/runbooks/contain-runaway-execution.md`
tells operators to reach for first, and `pause_workflow_execution` accepts only a
`RUNNING`/`PAUSED` row — so unrouted it returned `409 … is already terminal (FAILED)`
on a retried run, leaving an operator holding the logical handle only the destructive
levers and inverting the containment ladder. Resume is an idempotent success no-op
against a non-paused row, so unrouted it would report success while the paused live
attempt stayed parked.

**Surfaces.** Applied consistently to the core `WorkflowHandle`
(`cancel`/`terminate`/ the in-process query and update paths /`result_snapshot`), to
`TypedWorkflowHandle` — including `result_snapshot`, whose typed
`error_type`/`error_details`/`non_retryable` are now loaded from the execution the
snapshot was actually read from (via the new `WorkflowHandle::result_snapshot_resolved`),
so a routed snapshot can never mix an outer run's `error` with an inner attempt's
typed metadata — to the `#[signal]`-generated typed client stubs (both
`signal_{name}` and `signal_{name}_idempotent`), and to the HTTP routes
`POST /workflows/{id}/signal/{name}` (and its `by-id` sibling), `/cancel`,
`/terminate`, `/pause`, `/resume`, `GET|POST /workflows/{id}/query/{name}`,
`POST /workflows/{id}/update/{name}`, and
`GET /workflows/{id}/update/{update_id}/result` — the paired read for a
`wait=admitted` admission, whose `202` carries only the `update_id`, so an unrouted
read would 404 forever against the addressed predecessor.
`hydrate_ctx_for_query` and `admit_update` resolve the live attempt before
replaying / admitting; the plugin's duplicated chain walker
(`load_execution_following_retries`) was consolidated onto the core resolver.

**Deliberately NOT routed, and documented as such:** reset (#148), erase-payloads
(#495), legal-hold (#747), triage (#814), the per-activity `retry-now`/`fail-now`
(#516/#765), and the DLQ replay/redrive routes each act on the **recorded artifact**
of one attempt rather than steering the live run. `POST /workflows/{id}/rerun` (#777)
takes the opposite policy and *rejects* a chain predecessor outright, which is
deliberate — re-run mints a brand-new logical run from an attempt's inputs, so
silently re-aiming it would change which inputs the new run gets. An erase /
legal-hold spanning a whole retry chain is a genuine (GDPR-relevant) gap, tracked
separately rather than smuggled into this change.

**Describe is deliberately NOT routed.** `GET /workflows/{id}`, history export, the
timeline, the stack, the awaitables, the per-execution logs, the event stream,
`/diagnose`, the registered `/queries` listing, and the DLQ are specific-`exec_id`
reads and must keep reporting the addressed row so an operator can inspect exactly
the attempt that failed — pinned by a test.

**Audit fidelity.** A routed cancel / terminate / pause / resume / signal now writes
its **success** audit row keyed on the execution actually mutated, not the addressed
id, so an auditor asking "who terminated attempt 3?" finds the row. The failure path
keeps the addressed id (nothing was mutated), and the addressed id stays
reconstructible either way because the `retry_of_exec_id` chain is durable.

**Observability.** `POST /workflows/{id}/signal/{signal_name}` reports an additive
`routed_execution_id` **only when** the addressed id differed from the attempt the
signal landed on (`skip_serializing_if`), so a non-retried run's response is
byte-for-byte unchanged. Registered in `management_api_response_fields()` and
`docs/api-contract.json` for both the exec-id and by-id signal routes.

**Docs.** New `docs/logical-handle.md` (the full contract: resolution rule, the
no-row-gap proof, per-operation semantics, the describe carve-out, the race/re-drive
rules, and the invariants), cross-linked from the signal-delivery section of
`docs/management-api.md`.

**Tests, TDD RED→GREEN.** New core suite
`autumn-harvest/tests/integration/retry_chain_routing_tests.rs` (15 tests, real
`Worker`-driven two-attempt chain fixture): cancel / terminate / signal / update /
pause+resume routed to the live attempt, the same via the `WorkflowHandle` surface
(`handle_cancel_*`, `handle_terminate_*`), `cancel_removes_a_queued_retrys_delayed_task`
(AC2's "prevent a queued retry from starting" against a genuinely *delayed*
`PENDING` task, using a 1-hour backoff so the retry has not started),
`cancelled_chain_never_spawns_a_further_attempt`, keyed-signal dedupe on the routed
attempt, whole-mailbox forwarding + re-arm, the worker-wiring guard
(`the_worker_forwards_the_mailbox_when_it_schedules_a_retry`, which fails if the
`worker.rs` call site is deleted), and the two resolver no-op/walk guards. New
plugin HTTP suite `autumn-harvest-plugin/tests/retry_chain_routing_integration.rs`
(9 tests): signal (+`routed_execution_id` present/omitted), cancel, terminate,
pause+resume, `GET`/`POST` query, update, the paired update-**result** read, and the
describe-unchanged guard. `typed_workflow_failure_tests` pins AC4's mixed-source
guarantee (a routed `result_snapshot` must load `error_type`/`error_details`/
`non_retryable` from the execution it was READ FROM, never the addressed id).
RED was verified **behaviourally** by neutering `resolve_live_attempt` with an
early return: 11/15 core tests fail, and the 4 that still pass are exactly the
chain-independent controls (the fixture sanity check, the no-chain no-op guard, and
the two mailbox-forwarding tests, which exercise the worker/helper rather than
resolution). Both suites registered in `.github/ci/integration-suites.txt` and
executed green against a real local Postgres 16.
