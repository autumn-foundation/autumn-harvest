# Transactional Workflow Start

## The dual-write problem, one level up

[`ctx.run_transactional`](transactional-activities.md) closes the dual-write gap
at the **activity → completion** boundary: a domain write and the
`ActivityCompleted` event commit atomically. Starting a workflow from *outside*
harvest has the identical problem, one level earlier in the pipeline.

An HTTP handler that inserts an `orders` row and then needs to kick off a
`fulfill_order` workflow relies on two separate commits:

1. The domain write (`INSERT INTO orders ...`), committed on its own.
2. The workflow start — a `WorkflowStarted` event, an execution row, and a
   dispatchable task-queue row, committed separately by a second call.

If the process crashes, the connection drops, or the second call simply fails
between these two steps, the domain row exists with **no fulfillment ever
kicked off** — a silent gap discoverable only by a customer complaint or an
operator's reconciliation query, days later. Closing this gap by hand
conventionally means an outbox table plus a polling reconciliation job.

`WorkflowHandleClient::start_workflow_transactional` removes the need for
either: it accepts a caller-owned Diesel connection (or an already-open
transaction on one) and stages the workflow start on it directly.

## Using `start_workflow_transactional`

```rust
use autumn_harvest::prelude::*;
use autumn_harvest::{TransactionalStartOptions, WorkflowHandleClient};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, input: OrderInput) -> Result<(), String> {
    // ... charge, ship, notify ...
    Ok(())
}

async fn place_order(
    conn: &mut AsyncPgConnection,
    client: &WorkflowHandleClient,
    order_id: String,
    amount_cents: i64,
) -> Result<autumn_harvest::ExecutionId, autumn_harvest::HarvestError> {
    let outcome = Box::pin(conn.transaction::<_, autumn_harvest::HarvestError, _>({
        let order_id = order_id.clone();
        move |conn| Box::pin(async move {
            // 1. The caller's own domain write, on the SAME connection.
            diesel::sql_query("INSERT INTO orders (id, amount_cents) VALUES ($1, $2)")
                .bind::<diesel::sql_types::Text, _>(&order_id)
                .bind::<diesel::sql_types::BigInt, _>(amount_cents)
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;

            // 2. Stage the workflow start on the SAME transaction.
            client
                .start_workflow_transactional(
                    conn,
                    "fulfill_order",
                    &order_id,
                    serde_json::json!({ "order_id": order_id, "amount_cents": amount_cents }),
                    TransactionalStartOptions::new(),
                )
                .await
        })
    }))
    .await?;

    let exec_id = outcome.exec_id;
    outcome.finish().await; // optional — see "The finish() call" below
    Ok(exec_id)
}
```

The client must know about the target workflow's `WorkflowInfo` up front —
register it via `.with_workflows([fulfill_order_info()])` when constructing the
`WorkflowHandleClient`. This is what lets `start_workflow_transactional` resolve
the same defaults (execution timeout, SLA, per-key concurrency, published input
schema) that `POST /workflows/{name}/start` resolves.

```rust
let client = WorkflowHandleClient::single(pool, notification_url)
    .with_workflows([fulfill_order_info()]);
```

When the call returns `Ok(outcome)`:

1. The user writes made via `conn` are staged in the open transaction.
2. The `WorkflowStarted` event is staged in the same transaction.
3. The execution row is staged in the same transaction.
4. The initial dispatchable task-queue row is staged in the same transaction.

None of it is visible to any worker — and no worker can claim the run's first
task — until the caller's own transaction **commits**. If the closure (or
anything wrapping it) returns `Err` and the transaction rolls back, none of
these four things exist. There is no partial, orphaned state in either
direction.

### The `finish()` call

`start_workflow_transactional` returns a `#[must_use] TransactionalStartOutcome`
carrying the assigned `ExecutionId` plus any deferred completion-trigger
follow-ups the start decision produced (this only happens for the
`WorkflowIdConflictPolicy::Terminate` collision case, where resolving the
conflict cancels a prior execution that itself had registered completion
triggers — the overwhelming majority of calls produce none). Call
`outcome.finish().await` **after** your outer transaction has committed to
eagerly dispatch those follow-ups.

This is a **latency optimization only, never a correctness requirement** — a
periodic background scanner delivers the same follow-ups on its own schedule
if `finish()` is skipped entirely (or the process crashes before it runs).
Dropping the outcome without calling `finish()` is safe.

## Replaying a transactionally-started workflow

From the workflow's own perspective, and from every other client of the
management API, a transactionally-started run is indistinguishable from one
started through `POST /workflows/{name}/start` — same event shapes, same
replay engine, same everything. **No new `WorkflowEvent` variant was added for
this feature.** The only observable difference is provenance: the execution
row's `start_source` column records `StartSource::Transactional`
(`GET /workflows/{id}` surfaces it), so an operator triaging a run can tell it
was started this way.

## In a sharded deployment

Harvest can spread workflow state across independent Postgres databases (see
[`docs/sharding.md`](sharding.md)). Because `start_workflow_transactional`
stages its writes on the caller's own connection, the started workflow lands
on **whichever shard that connection is actually talking to** — there is no
way to route a cross-shard transaction, so this call never attempts one.

- **Single-shard deployment** (built via `WorkflowHandleClient::single(...)`):
  no extra step needed. The caller's connection is definitionally on the only
  shard there is.
- **Multi-shard deployment**: pass
  `TransactionalStartOptions::new().with_shard(shard_id)` naming the shard that
  backs `conn`. Omitting it is rejected with `HarvestError::Config` — silently
  guessing would mint an execution id whose *encoded* shard (the one every
  later exec-id-routed lookup — signal, cancel, describe, the timeout scanner —
  uses to find the row) disagrees with wherever `conn` actually wrote it,
  producing a run that committed successfully but can never be found again.

## Start-semantics parity

`start_workflow_transactional` resolves the same `WorkflowInfo` defaults,
admission-gate decision, and id-reuse/conflict semantics as the HTTP route and
the non-transactional in-process start — it is not a reduced *policy*
surface. It **is**, however, a narrower *per-call override* surface today:
`TransactionalStartOptions` does not yet expose every knob
`StartWorkflowParams` supports, so a handful of fields are hardcoded to their
class default on every transactional start regardless of what the HTTP route
would otherwise accept for the same workflow. See the note below the table.

| Behavior | Source |
|---|---|
| `WorkflowInfo` defaults (execution timeout, SLA clamp, concurrency key/limit, owner metadata) | Resolved from the registered `WorkflowInfo`, mirroring `POST /workflows/{name}/start` |
| `WorkflowIdReusePolicy` / `WorkflowIdConflictPolicy` | `TransactionalStartOptions::with_reuse_policy` / `with_conflict_policy` |
| Idempotency-key dedup (issue #808 semantics) | `TransactionalStartOptions::with_idempotency_key` — a duplicate key resolves to the existing run without a second write, and a *rolled-back* reservation is not permanently "burned": a retry with the same key after a genuine crash still succeeds fresh |
| Published input-schema validation (issue #373) | Validated **before any database call** — a schema violation returns `HarvestError::InputValidationFailed` and writes nothing |
| Memo / search attributes / parent id | `TransactionalStartOptions::with_memo` / `with_search_attrs` / `with_parent_id` |
| Fleet-wide / queue / workflow-name admission gates (issue #618) | Checked synchronously via `GateMode::CheckCached` (not `Check`): a matching gate blocks with `HarvestError::AdmissionBlocked` and counts `harvest.admission.blocked`, same as the HTTP route. Unlike the HTTP route, `CheckCached` never fails closed — a caller with no `HarvestPlugin` running in-process (which is the sole owner of the gate cache) is completely unaffected rather than being permanently blocked. See `StartProducer::Transactional` in `admission_gate.rs` for the full rationale. |
| Workflow-level retry attempt numbering (issue #523) | A fresh transactional start is attempt `1`, identical to every other fresh-start producer |

Authorization for a conflict policy that force-terminates a running prior
execution is **not** separately gated here the way the HTTP route gates it
with an admin check — an in-process caller is already as trusted as any other
code running in the same process, so there is no cross-boundary auth boundary
to enforce.

### Not yet exposed via `TransactionalStartOptions`

Every transactional start currently resolves the following fields to their
class default, with **no per-call override**, even though `StartWorkflowParams`
(and, for most of them, `POST /workflows/{name}/start`) supports one:

- **Priority** (issue #249) — always `Priority::default()` (`Normal`); no
  `.with_priority(...)`.
- **Delayed / scheduled-future start** (issue #322) — `start_at`/`delay` are
  always `None`; a transactional start always begins immediately. There is no
  transactional equivalent of "start this workflow 10 minutes from now."
- **Ambient context headers** (issue #481) — always `None`; propagating
  string key/value context to child activities/workflows without threading it
  through function signatures is not available on this path.
- **Per-execution completion-callback targets** (issue #605) — always `None`;
  only builder-wide default callback targets (if configured) apply. A caller
  cannot register a one-off callback URL for a single transactionally-started
  run.
- **W3C trace-context propagation** (`trace_context`) — always `None`; the
  request is not stitched into an existing distributed trace.
- **Operator attribution / start-source correlation ref** (issue #740) —
  `start_source_ref` and `started_by` are always `None`; only the bare
  `StartSource::Transactional` classifier is recorded.

None of these are architectural limitations of the transactional-start
primitive itself — each is a straightforward addition to
`TransactionalStartOptions` (a new field + builder method + one line threading
it into `StartWorkflowParams` in `handle.rs`) that simply has not been done
yet. If your use case needs one of them, that is the extension point.

## When to use `start_workflow_transactional` vs alternatives

| Scenario | Recommendation |
|---|---|
| Your app's own domain write must never succeed without the reactive workflow also starting (order placement, subscription creation) | `start_workflow_transactional` |
| A webhook receiver needs to start-or-attach and signal in one atomic step | [`signal_with_start_workflow_execution`](../autumn-harvest/examples/signal_with_start_webhook.rs) — a different atomicity boundary (start + signal, not caller-domain-write + start) |
| The workflow can tolerate at-least-once admission (a retry simply attaches to the existing run under `AllowDuplicate`) and there's no caller-owned domain write to tie it to | Plain `POST /workflows/{name}/start` (or the non-transactional `start_or_load_workflow_execution`) with an idempotency key |
| Starting from a scheduler tick, a completion trigger, or any other engine-internal producer | Not applicable — this API is for embedding-app code reacting to its own domain writes |

## Restrictions

- **The target workflow must not carry a debounce, batch, or throttle policy.**
  All three defer admission — a debounced/batched/throttled start may resolve
  to a *different* `ExecutionId` than the one requested, minutes later, which
  cannot be returned synchronously inside the caller's transaction.
  `start_workflow_transactional` rejects these up front with
  `HarvestError::Config` rather than silently doing the wrong thing.

  ```rust
  // ✅ Fine — a plain workflow with no deferred-admission policy.
  client.start_workflow_transactional(conn, "fulfill_order", &id, input, opts).await?;

  // ❌ Rejected — `throttled_workflow` carries a #[workflow(throttle(...))] policy.
  client.start_workflow_transactional(conn, "throttled_workflow", &id, input, opts).await?;
  ```

- **Not available over HTTP.** This is an in-process API only — there is no
  `POST` route for it, because the whole point is composing with a Diesel
  transaction your own handler already has open. An HTTP request/response
  cycle has no equivalent "caller's still-open transaction" to compose with.
- **The workflow must be registered on the client.** Call
  `WorkflowHandleClient::with_workflows([...])` with every workflow you intend
  to start this way. An unregistered name is rejected with
  `HarvestError::Config` before any database call.
- **A multi-shard client requires `TransactionalStartOptions::with_shard(...)`.**
  See "In a sharded deployment" above.
- **Keep the transaction short.** The caller's own domain write and the start
  itself both run inside one open Postgres transaction; the longer it stays
  open, the longer the newly-inserted execution and task-queue rows are locked
  from a worker's perspective (a worker simply won't see them until commit —
  there's no deadlock risk, just added start-to-claim latency proportional to
  how long the caller's transaction stays open after the start call returns).

## Testing

`tests/integration/transactional_start_tests.rs` is the reference test suite,
organized cheapest-and-most-decisive first:

1. **Atomicity** — commit makes the domain row, the `WorkflowStarted` event,
   the execution row, and the task-queue row all visible together; rollback
   leaves none of them.
   - The headline test,
     `fault_injection_zero_orphans_across_five_hundred_randomized_crash_points`,
     randomizes the "crash point" (a dropped connection with no `COMMIT` — the
     same abort behavior Postgres exhibits for an actual process kill) across
     500 iterations against the API's canonical ordering (the workflow start
     runs first, so the domain write can reference the returned
     `outcome.exec_id` — the shape every 5-line usage example follows),
     asserting the domain row and the workflow's full footprint — event,
     execution row, *and* task-queue row — are always **both present or both
     absent**, zero orphans in either direction.
   - `atomicity_holds_when_domain_row_is_written_before_the_start` and
     `rollback_after_domain_first_write_leaves_neither_placeholder_nor_workflow`
     cover the *other* legitimate ordering — a placeholder domain row written
     first and patched with the real `exec_id` once the start returns — proving
     atomicity holds regardless of which write comes first, not just across
     crash points within one fixed ordering.
   - `outer_rollback_after_a_terminate_existing_collision_undoes_the_cancellation_too`
     proves the `WorkflowIdConflictPolicy::TerminateExisting` path (cancelling
     a still-active prior execution, then sealing it and starting fresh — both
     nested via `SAVEPOINT` on the caller's own connection, entirely inside
     `start_or_load_workflow_execution_collect`'s own nested transaction, with
     no separate pre-check step the way the HTTP route's native
     `TerminateIfRunning` reuse policy has) is atomic as a *whole*: an outer
     rollback after the collision resolves undoes the prior's cancellation
     right along with the fresh start, rather than durably stranding the
     prior sealed `CONTINUED_AS_NEW` (with a stray `WorkflowCancelled` event
     in its history) with no successor.
     `finish_after_commit_dispatches_diagnostics_for_a_terminate_existing_collision`
     covers the *committed* half of the same path: the prior's `state` column
     ends up `CONTINUED_AS_NEW` (not `CANCELLED`) once `replace_execution`
     seals it — `inline_cancel` runs first and durably appends the
     `WorkflowCancelled` event, but `replace_execution` immediately overwrites
     the row's `state`/`completed_at` afterward, in the same branch, for
     *every* Terminate-resolving collision (both the native `TerminateIfRunning`
     reuse policy and `conflict_policy = TerminateExisting`) — pre-existing,
     shared `execution.rs` behavior from issue #685, unrelated to
     `start_workflow_transactional` and out of this issue's scope to change.
2. **Start-semantics parity** — `WorkflowInfo` defaults (execution timeout,
   SLA, concurrency key — the issue text names all three explicitly), id-reuse
   / conflict policy, idempotency-key dedup (including that a *rolled-back*
   reservation is not permanently "burned" — a retry with the same key still
   succeeds fresh), schema-validation-failure-aborts-without-writing, the
   debounce/batch/throttle rejection (all three policies, independently), the
   unregistered-workflow-name rejection, and the admission-gate (issue #618)
   interaction — a matching gate blocks and writes nothing, an *unrelated*
   workflow name is unaffected by the same still-armed gate, and disarming the
   gate restores normal admission.
3. **Sharding** — lands on the shard backing the connection (proven with two
   genuinely separate Postgres databases), a multi-shard client without
   `.with_shard(...)` is rejected and writes nothing, and a single-shard
   deployment needs no `.with_shard(...)` call at all.

Run it with a real Postgres available (`HARVEST_TEST_DATABASE_URL`) or let it
boot its own via `testcontainers`:

```bash
cargo test -p autumn-harvest --features db --test integration -- transactional_start --test-threads=1
```

`--test-threads=1` is a **hard requirement**, not just caution: every test in
this file builds its own `WorkflowHandleClient`/`ShardedDbPool`, and
`ShardedDbPool::single()`/`::from_map()` write to the process-wide
`GLOBAL_SHARDED_POOL`/`GLOBAL_SHARD_ROUTER` statics as a side effect of
construction (read by `DeferredTriggerStart::spawn()`, used from
`TransactionalStartOutcome::finish()`, and by background scanners). Running
this file's tests in parallel within one process would have them clobber each
other's global pool/router registration. The admission-gate test additionally
arms the process-global `GLOBAL_ADMISSION_GATE_CACHE` for its duration — safe
under `--test-threads=1` because it always disarms the gate before returning,
matching every other gate-mutating test in this crate.
