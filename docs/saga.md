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
3. Call `compensate_all()` again, re-invoking all compensation closures from the top of
   the LIFO stack — including closures that already ran before the crash.

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

## Observability (issue #801)

Compensation is a first-class, alertable signal. Two engine counters are
emitted from inside `Saga::run_compensations` (the single choke point both
`compensate_all()` and automatic step-failure rollback funnel through):

| Metric | Fires | Labels |
|--------|-------|--------|
| `harvest.saga.compensated` | Exactly once per **real compensation sequence** — a non-empty unwind actually running forward, counted at unwind start | `workflow`, `queue` |
| `harvest.saga.compensation_failed` | Exactly once per unwind that finishes with ≥1 compensation error (`HarvestError::SagaCompensationFailed` — the dangling-state case) | `workflow`, `queue` |

`execution.id` is never a label (ADR-0001 §7). A saga that never compensates
emits nothing; an empty `compensate_all()` is not a sequence.

**Exactly-once across replays.** Compensations re-run wholesale on every
replay (see the idempotency contract above), so the counters cannot simply
fire per call. Each unwind is keyed to durable `MarkerRecorded` dedup
markers reusing the existing event variant — `saga_compensated:{seq}`
recorded at unwind start (persisted in the same command batch as the first
compensation's own dispatch, so a crash at any point mid-unwind resumes
silent) and `saga_compensation_failed:{seq}` recorded at failed-unwind end.
The counter fires only when its marker is first recorded on the live
frontier; every replay observes the marker and stays silent. **No new
`WorkflowEvent` variant and no migration** — the markers are opaque names on
`MarkerRecorded`, exactly like `fan_out:{n}` / `race:{seq}` / `patch:{id}`.

**Backward compatibility.** A pre-#801 history (no saga markers) replays
untouched and uncounted: the marker matcher's `Absent` arm never moves the
cursor, so the recorded events still match the compensation's own commands —
never a divergence, never a retroactive count.

**In-Saga emission (not the worker terminal boundary).** The failure counter
fires even when the workflow author catches `SagaCompensationFailed` and the
run goes on to COMPLETE, and the compensated counter fires for
cancel-and-compensate unwinds in runs that never terminally fail — both
cases a terminal-boundary emission would structurally miss. It is therefore
fully separable from `harvest.workflow.terminal{outcome=failed}`.

**Per-unwind coherence (post-review hardening).** The unwind's disposition
(counted or not) is resolved **once**, at unwind start, and the failure
counter follows it — so the pair can never disagree about one logical
unwind, and the invariant **`failed ≤ compensated`** holds per unwind (ratio
dashboards never divide by zero or exceed 100%). Two concrete consequences:

- A *counted* unwind's failure is always counted, **including past a
  trailing un-awaited signal** at the failed-unwind end (e.g. a retried
  cancel webhook ingested at the final unwind cycle's wake) — the failure
  marker is recorded past the drained signal, replay-consistently.
- The **cancel-and-compensate pattern is counted**: a trailing
  `WorkflowCancelled` has no workflow-command counterpart and never leaves
  the replay cursor, so the marker matcher treats it as transparent — the
  unwind of a freshly-cancelled run is the live frontier.

**Accepted edges** (documented deliberately):

- An unwind entered while unconsumed non-marker, non-cancellation events sit
  at the replay cursor is conservatively **uncounted as a whole** (both
  counters) — for a metrics-only feature, a silent under-count of a rare
  edge beats any chance of recording a marker at a wrong history position.
- **Signal-with-start caveat** (inherited from `ctx.patched()`): an unwind
  entered at a drained-signal frontier — canonically a signal-with-start run
  whose unwind begins before the staged signal is awaited — is
  conservatively **uncounted as a whole**, deterministically on every
  replay. The failure counter follows (never `failed=1` with
  `compensated=0`).
- **Crash window (at-least-once), both counters, durable and in-memory
  unwinds alike:** the sample is emitted in-process within the
  single-decision-cycle gap before its marker's batch commits (start marker:
  before the unwind's first dispatch batch persists; failure marker: before
  the post-unwind batch persists). A worker crash inside that gap re-emits
  on resume. A crash *mid-unwind* — between compensations, after the first
  batch committed — is exactly-once. A **pure in-memory** unwind (zero
  durable footprint) is the maximal case: a crash-resume anywhere within its
  single cycle can re-count — the metric mirror of the "compensations re-run
  wholesale" contract.
- **Pause race (at-least-once):** an operator pause (#383) committing while
  the unwind's first decision cycle is in flight discards the cycle's
  pending commands — including the freshly pushed marker — after the sample
  already fired; the re-derived cycle after resume emits again. Same class
  as the crash window.
- **Forward compatibility:** a history recorded by #801+ code does not
  replay under pre-#801 builds (the marker is unknown to the old matcher);
  a rollback nd-blocks (#603, non-terminal, recoverable) until rolled
  forward. Same rule as every marker feature.

Starter alerts `harvest_saga_compensation_spike` and
`harvest_saga_compensation_failed` ship in
`docs/alerts/starter-pack-v0.1.0.json` with runbook sections in
`docs/runbooks/harvest-alerts.md`.

---

## Test coverage

The integration tests in `autumn-harvest/tests/integration/saga_tests.rs`
lock in these semantics:

| Test | What it proves |
|------|----------------|
| `saga_cancellation_does_not_auto_compensate` | `Saga` leaves compensations pending when `ctx.is_cancelled()` is true; no automatic unwind occurs |
| `saga_compensate_all_on_cancel_pattern` | The recommended explicit cancel-and-compensate pattern works end-to-end; LIFO order is preserved — and (post-review) the unwind is counted, with its dedup marker recorded past the `WorkflowCancelled` event |
| `saga_compensation_idempotency_under_replay` | On a simulated second execution (replay after crash), all compensations re-run; by-ID compensations are safe, release-most-recent would produce double-effects |
| `saga_compensated_metric_emitted_once_across_crash_replay` | The `harvest.saga.compensated` counter reflects real compensation sequences, not the replay count — the durable marker suppresses re-emission on crash-resume (issue #801, AC3) |
| `saga_compensation_failed_metric_distinct_and_once` | The failure counter is a distinct signal, emitted exactly once and deduped across replays by its own marker |
| `saga_compensation_failed_emitted_even_when_author_catches_error` | An author-caught `SagaCompensationFailed` is still counted (in-Saga emission) |
| `saga_success_path_emits_no_saga_metrics_and_no_marker_commands` | A saga that never compensates emits nothing new (issue #801, AC6) |
| `pre_801_history_mid_unwind_replays_clean_and_uncounted` | A pre-#801 marker-less history — including one captured mid-unwind — replays without divergence and without emission |
| `trailing_unawaited_signal_does_not_suppress_the_failure_counter` | A counted unwind's failure fires exactly once even with a trailing un-awaited signal at the failed-unwind end (post-review P2-1) |
| `signal_with_start_shape_keeps_the_failed_counter_coupled_to_compensated` | An uncounted (drained-signal-frontier) unwind's failure stays uncounted — `failed ≤ compensated` holds per unwind (post-review P2-2) |
| `two_saga_unwinds_in_one_workflow_count_independently` | Two compensation sequences in one workflow allocate distinct seq markers and each count exactly once |

---

## Out of scope

- `Saga::with_auto_compensate_on_cancel(true)` — separate issue if demand emerges.
- Cross-shard saga semantics — a `Saga` is scoped to a single workflow execution
  and therefore a single shard via `ExecutionId::shard()`.
- Saga + Update (#140) or saga + child-workflow interaction — separate specs.
