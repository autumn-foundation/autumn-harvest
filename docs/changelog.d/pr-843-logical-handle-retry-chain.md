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

**No new `WorkflowEvent` variant, no migration, no change to the adjacently-tagged
event JSON, no replay-determinism impact** — this is a read-side resolution plus a
re-target of existing mutating primitives, and signal forwarding reuses the
existing `harvest_signals.workflow_exec_id` column inside the transaction the
retry already commits.

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
cannot double-admit), and terminate additionally re-drives on an *idempotent no-op
against a `FAILED` row* — the only available signal that the chain advanced (a
no-op against any other terminal state is the final answer). The loop terminates
because each re-drive strictly descends the chain and an unchanged target ends it,
which is also what stops a genuine error (unknown id, exhausted chain) from being
retried forever.

**Signals — "deliver on the next attempt", with forwarding.** New
`signal::send_signal_to_live_attempt` (returning `RoutedSignalDelivery { target,
delivered }`) and `signal::forward_unconsumed_signals`. The retry transaction in
`worker.rs` now moves every still-**unconsumed** `harvest_signals` row from the
predecessor to the successor, mirroring the long-standing continue-as-new
precedent — a signal never ingested into any history is not reflected anywhere, so
moving it loses nothing, while leaving it behind would strand it against a sealed
row forever. Already-consumed rows stay put for audit. **#521 idempotency keys keep
their `(workflow_exec_id, idempotency_key)` scope**: a key that landed on attempt
*N* does not suppress the same key on attempt *N+1*, which is correct — each
attempt is a genuinely distinct execution with a fresh history.

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

**Surfaces.** Applied consistently to the core `WorkflowHandle`
(`cancel`/`terminate`/`signal`/`query`/`update`/`result_snapshot`), to
`TypedWorkflowHandle` (which wraps the same untyped handle), and to the HTTP routes
`POST /workflows/{id}/signal/{name}`, `/cancel`, `/terminate`,
`GET|POST /workflows/{id}/query/{name}`, and `POST /workflows/{id}/update/{name}`.
`hydrate_ctx_for_query` and `admit_update` resolve the live attempt before
replaying / admitting; the plugin's duplicated chain walker
(`load_execution_following_retries`) was consolidated onto the core resolver.

**Describe is deliberately NOT routed.** `GET /workflows/{id}`, history export, the
timeline, the stack, and the DLQ are specific-`exec_id` reads and must keep
reporting the addressed row so an operator can inspect exactly the attempt that
failed — pinned by a test.

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
`autumn-harvest/tests/integration/retry_chain_routing_tests.rs` (10 tests, real
`Worker`-driven two-attempt chain fixture): cancel/terminate/signal/update routed to
the live attempt, `cancelled_chain_never_spawns_a_further_attempt`, keyed-signal
dedupe on the routed attempt, unconsumed-signal forwarding, and the two resolver
no-op/walk guards. New plugin HTTP suite
`autumn-harvest-plugin/tests/retry_chain_routing_integration.rs` (7 tests): signal
(+`routed_execution_id` present/omitted), cancel, terminate, `GET`/`POST` query,
update, and the describe-unchanged guard. RED was verified **behaviourally** by
neutering `resolve_live_attempt` with an early return: 7/10 core tests and 5/7
plugin tests fail, and the ones that still pass are exactly the chain-independent
negative controls. Both suites registered in `.github/ci/integration-suites.txt`
and executed green against a real local Postgres 16.
