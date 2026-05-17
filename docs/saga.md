# Saga + Cancellation Semantics and Idempotency Contract

`Saga` (`autumn-harvest/src/saga.rs`) composes multi-step distributed
transactions with explicit LIFO compensation.  This document specifies the
interaction between `Saga` and the workflow cancellation primitive, and the
idempotency invariants that compensation activities must satisfy.

---

## Cancellation interaction

### Semantic chosen: cancellation does NOT auto-compensate

When an operator calls `cancel_workflow_execution`, a `WorkflowCancelled`
event is appended to the execution's event history.  On the next worker
pick-up, the executor replays the workflow function with a `WorkflowContext`
where `ctx.is_cancelled()` returns `true`.

**The `Saga` struct never observes this.**  It holds a plain `Vec` of pending
compensation closures and has no visibility into `WorkflowContext` state
during forward execution.  Cancellation therefore does **not** trigger
automatic compensation.

### Rationale

This matches Temporal's well-documented model and avoids two classes of
surprising behaviour in long-running sagas:

1. **Implicit partial-unwind surprise.** In a ten-step saga, automatic
   compensation after step 4 may be worse than no compensation: steps 5–10
   never ran, so there is nothing to undo for them.  Forcing the author to
   call `compensate_all()` explicitly makes the decision visible.
2. **Compensation-as-side-effect.** Compensation activities can be expensive
   (refund calls, seat releases, inventory restores).  Triggering them silently
   on every cancellation—including operator test cancels or retry-storm
   mitigations—creates unpredictable cost.

### Recommended pattern

Observe `ctx.is_cancelled()` in the workflow function and call
`saga.compensate_all()` explicitly:

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, order: Order) -> Result<(), String> {
    let mut saga = Saga::new(ctx);

    let charge_id = saga
        .step(
            || ctx.execute_activity_raw("charge_payment", &order, "payments"),
            |charge_id| ctx.execute_activity_raw("refund_payment", &charge_id, "payments"),
        )
        .await
        .map_err(|e| e.to_string())?;

    let reservation_id = saga
        .step(
            || ctx.execute_activity_raw("reserve_inventory", &order, "inventory"),
            |rsv_id| ctx.execute_activity_raw("release_reservation", &rsv_id, "inventory"),
        )
        .await
        .map_err(|e| e.to_string())?;

    // ── Check for cancellation before committing ──────────────────────
    if ctx.is_cancelled() {
        saga.compensate_all()
            .await
            .map_err(|e| e.to_string())?;
        return Err(ctx.cancellation_reason().unwrap_or("cancelled").to_string());
    }
    // ─────────────────────────────────────────────────────────────────

    ctx.execute_activity_raw("confirm_order", &(charge_id, reservation_id), "default")
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
```

`ctx.check_cancellation()?` is a shorthand that returns `Err(HarvestError::Cancelled(…))`
without running compensations.  Use it for fast-exit points where no
compensations are registered yet.

---

## Idempotency contract

### Why compensations re-run

Compensation closures are re-registered on **every** workflow replay.  The
`Saga` struct is rebuilt from scratch each time the workflow function executes;
it has no persistent identity in `harvest_events`.  If a worker crashes after
`compensate_all()` has started but before it finishes, the next worker will:

1. Replay all forward steps (returning their results from recorded history).
2. Re-register all compensation closures via the same `Saga::step()` calls.
3. Call `compensate_all()` again, running **all** compensations from the top of
   the LIFO stack — including compensations that already ran before the crash.

**Consequence: compensation activities must be idempotent.**

### Good pattern — release by ID

```rust
// Safe to call twice: the second call is a no-op when the reservation is
// already released.
|rsv_id: String| ctx.execute_activity_raw(
    "release_reservation",
    &rsv_id,      // ← specific, stable identifier from forward step result
    "inventory",
)
```

The `rsv_id` is sourced from the `ActivityCompleted` event recorded when the
forward step ran, so it is the same value on every replay.

### Anti-pattern — release most-recent

```rust
// Dangerous: the second invocation releases whichever reservation was
// created most recently at that moment — which may belong to a different order.
|_| ctx.execute_activity_raw("release_last_reservation", &(), "inventory")
```

On a replay after a crash, `release_last_reservation` would release a
reservation that was never part of this saga.

---

## Replay-determinism contract

The `compensate` closure in `Saga::step` receives the forward step's `T`
result.  When the forward step calls `ctx.execute_activity_raw(...)`, that
result is sourced from the recorded `ActivityCompleted` event on replay rather
than re-executing the activity.  **Do not place non-deterministic or
side-effecting logic directly inside the compensation closure body**; invoke
an activity via `ctx.execute_activity_raw(...)` instead, so that the
compensation itself is durable and replay-safe.

---

## Test coverage

The three integration tests in `autumn-harvest/tests/saga_tests.rs` lock in
these semantics:

| Test | What it proves |
|------|----------------|
| `saga_cancellation_does_not_auto_compensate` | `Saga` leaves compensations pending when `ctx.is_cancelled()` is true; no automatic unwind occurs |
| `saga_compensate_all_on_cancel_pattern` | The recommended explicit cancel-and-compensate pattern works end-to-end; LIFO order is preserved |
| `saga_compensation_idempotency_under_replay` | On a simulated second execution (replay after crash), all compensations re-run; by-ID compensations are safe, release-most-recent would produce double-effects |

---

## Out of scope

- `Saga::with_auto_compensate_on_cancel(true)` — separate issue if demand emerges.
- Cross-shard saga semantics — a `Saga` is scoped to a single workflow execution
  and therefore a single shard via `ExecutionId::shard()`.
- Saga + Update (#140) or saga + child-workflow interaction — separate specs.
