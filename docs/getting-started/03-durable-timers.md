# Chapter 3 — Durable timers

[← Your first workflow and activity](02-first-workflow.md) · [Index](README.md) · [Next: Signals →](04-signals.md)

---

Workflows can sleep. Not with `tokio::time::sleep` — that wouldn't survive a
restart — but with `ctx.timer()`, which records a `TimerStarted` event in
Postgres and suspends the workflow until the deadline elapses.

```rust
#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<String> {
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",
    )
    .await?;

    // 30-second drip — durably suspended.
    ctx.timer("post-welcome-drip", 30).await?;

    let nudge = ctx
        .execute_activity_raw(
            "send_followup_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    Ok(nudge["status"].as_str().unwrap_or("sent").to_owned())
}
```

This is the durability demonstration to actually try in person:

1. Start the workflow.
2. While it's parked on the timer, hit `Ctrl+C`.
3. Run `cargo run` again.
4. Watch the dashboard. The welcome activity is **not re-executed** — its
   result is replayed from the event log. The timer resumes wherever the
   30-second budget left off, then the follow-up activity runs.

That replay-and-resume pattern is the whole point of the engine.

## Cancellable and renewable timers

`ctx.timer()` (and its absolute-deadline sibling `ctx.sleep_until()`) are
**fire-once**: once armed they always fire. Some orchestrations need a timer
they can **cancel** (the work finished early, so an SLA timer must not fire) or
**reset** (a sliding-window / idle-session timeout that renews on every event).
For those, arm a durable timer with `ctx.start_timer()` and drive it through the
returned `TimerHandle` (issue #768):

```rust
#[workflow]
async fn fulfillment(ctx: &WorkflowContext, order: Order) -> HarvestResult<String> {
    // Non-suspending: arm the SLA timer and keep running.
    let mut sla = ctx.start_timer("fulfillment-sla", 3600);

    for item in order.items {
        ctx.execute_activity_raw("pick_item", serde_json::json!(item), "default").await?;
        // Each item pushes the SLA deadline forward — reset cancels the old
        // arming and starts a fresh one, so there is never an orphaned timer
        // left to fire late. NOTE: this does NOT interrupt a long-running
        // `pick_item`; an armed timer is never observed mid-activity. The
        // deadline is only *checked* when the workflow reaches `await_fire()`
        // below.
        sla.reset(3600)?;
    }

    if order.shipped_early {
        // Cancel so the SLA timer never fires.
        sla.cancel()?;
        Ok("shipped".into())
    } else {
        // Suspend until the SLA fires (or is cancelled elsewhere).
        match sla.await_fire().await? {
            TimerOutcome::Fired     => Ok("sla_breached".into()),
            TimerOutcome::Cancelled => Ok("cancelled".into()),
        }
    }
}
```

- **`start_timer` does not suspend** — it records the arming (a single
  `TimerStarted` event) and returns a handle immediately, so the workflow keeps
  running. It does **not** yet make the timer fire-eligible (see below).
- **`cancel()`** records a `TimerCancelled` event (and deletes the durable timer
  row if one has been created), so **no `TimerFired` is ever produced** for a
  cancelled timer.
- **`reset(secs)`** = cancel + re-arm; intentionally O(1) history per reset
  (two events) with **zero orphaned firings**.
- **`await_fire()`** creates the durable `harvest_timers` row (`fires_at = now +
  duration` at *this* instant) and suspends until the timer fires
  (`TimerOutcome::Fired`) or is cancelled (`TimerOutcome::Cancelled`).

**The deadline is measured from `await_fire()`, not from `start_timer()`.** A
cancellable timer becomes fire-eligible only when it is *awaited* — arming
records the event but inserts no durable row. An armed-but-unawaited timer is
therefore **never observed** while the workflow is parked on some other wait (an
activity, a child workflow, a signal): it cannot fire spuriously mid-activity,
and a `reset()` or `await_fire()` reached *after* that other wait is never
confronted with a stale fire (which would otherwise leave an unconsumed
`TimerFired` and break the run). In the loop above, the timer's `fires_at` is set
when the loop's final `await_fire()` runs, so the SLA is measured from there —
not from `start_timer`. This suits the intended idle-timeout / debounce / lease
patterns, which always await or reset the timer. If you need a deadline anchored
to `start_timer` time, capture `ctx.system_now()` at arm time and pass the
residual to `ctx.sleep_until()`.

> **Bounding a *signal* wait with a deadline.** An armed cancellable timer alone
> will not wake a workflow parked on a `wait_for_signal` — the two are separate
> waits. To race a signal against a deadline in one call, use
> [`ctx.receive_signal_timeout`](04-signals.md) (below), which arms the deadline
> and the signal wait together as one race.

**Fire-vs-cancel is decided by recorded-history order, not the wall clock.** If a
timer genuinely races its own cancellation, whichever of `TimerFired` /
`TimerCancelled` is recorded first in history wins on **every** replay,
regardless of timing on the replaying worker. Like `ctx.timer`, fires are
anchored to the Postgres clock (`fires_at = db_now + remaining`), so absolute
honoring is subject to worker↔database clock skew — this is not a skew-proof
absolute-time guarantee.

For a two-branch **"wait for a signal OR a deadline"** race (an approval that
auto-rejects after 24h), reach for
[`ctx.receive_signal_timeout`](04-signals.md) instead — it returns
`Some(payload)` when the signal arrives first and `None` when the deadline
fires. Composing a resettable `start_timer` handle *with* a signal wait in one
call is a natural follow-up; today, drive the reset from workflow logic as above
or use `receive_signal_timeout` for the pure two-branch shape.

See `examples/cancellable_timer_sla.rs` for a complete, tested example.

## Business-day timers

Support SLAs, contractual response windows, and settlement deadlines are written
in **business days**, not calendar days. `ctx.timer("escalate", 2 * 86_400)` is
wrong the moment the window straddles a weekend or a public holiday.
`ctx.timer_business_days()` (issue #806) is the one-call answer:

```rust
#[workflow]
async fn ticket_sla(ctx: &WorkflowContext, ticket: Ticket) -> HarvestResult<String> {
    // "Escalate after 2 business days" — weekends and holidays stepped over.
    let deadline = ctx.timer_business_days("escalate", 2, "us-support").await?;
    // ... escalate ...
    Ok(format!("escalated at {deadline}"))
}
```

There is also a non-suspending sibling, `ctx.business_days_from_now(id, n, cal)`,
which resolves and freezes the deadline **without** arming a timer — useful when
you want to record or return a business-day date rather than wait for it.

### Registering a calendar

The holiday set is resolved **on the worker** from a `BusinessCalendars`
snapshot in shared state — one builder call, no per-call DB read:

```rust
// Load the operator-managed holiday set once at startup and DECLARE how far it
// covers, so a resolution past the data's end fails loudly instead of quietly
// answering weekends-only.
let holidays = calendar::load_exclusions_for_calendar(&mut conn, "us-support").await?;
let covers_through = NaiveDate::from_ymd_opt(2027, 12, 31).expect("valid date");

let calendars = BusinessCalendars::new()
    .with_calendar_covering("us-support", holidays, covers_through);

HarvestBuilder::new().state(calendars) /* ... */;
```

`with_calendar(name, holidays)` is the horizon-less sibling: convenient for
tests and correct for a genuinely weekends-only calendar, but for a holiday set
loaded from a finite data source it silently answers weekends-only past the
data's end. Prefer `with_calendar_covering` in production — `harvest_calendars`
carries no coverage column, so the horizon is a deliberate operator declaration,
not something the loader can infer.

`BusinessCalendars::builtin()` ships `"us-federal-holidays"` and `"nyse"` for
demos and tests; see the expiry warning below before using it in production.

### Semantics

- **Weekends are always non-business days**, whatever the calendar is named —
  the named calendar contributes *holidays*. This deliberately differs from the
  scheduler's `"weekends-off"` calendar-naming convention (issue #337): the
  scheduler decides whether to *skip a firing*, while this primitive decides
  *when a deadline lands*, so weekends are unconditional here.
- **UTC dates, anchor time-of-day.** Business days are counted on UTC dates and
  the deadline preserves the anchor's UTC time-of-day. This matches the
  `DATE`-typed calendar exclusion rows and is DST-free. *Known limitation:* a
  deployment whose local business day is offset from UTC is off by one business
  day near the UTC-midnight boundary — and for `n = 0` the roll-forward can be
  suppressed entirely (a local Saturday morning east of UTC is still a UTC
  Friday, so `n = 0` fires immediately instead of rolling to the local Monday).
  Register a calendar and choose `n` in UTC terms, or wait for the per-call
  timezone follow-up.
- **`n = 0` rolls forward** — it fires at the anchor when the anchor's UTC date
  is a business day, otherwise at the next business date at the same
  time-of-day. It never means "one business day later".
- **Coverage horizon.** A calendar registered with a *declared* horizon
  (`with_calendar_covering`, and the shipped built-ins, which declare
  **2026-12-31**) only answers for dates it covers. A resolution needing a later
  date is **rejected**, never silently answered weekends-only — a silently-wrong
  SLA date is worse than a loud failure. A calendar registered with plain
  `with_calendar` declares no horizon and is never rejected on coverage grounds.
- **Unknown calendar name** → a typed `HarvestError::NotFound`. Never a panic,
  never a silent fire-now.

### Determinism — the calendar is read once

The resolution runs **once**, on the first live execution, from an anchor
captured at that moment. The resolved deadline is frozen into history using the
existing `SideEffectRecorded` event (issue #384) and the wait is carried by the
existing `TimerStarted` event — so there is **no new `WorkflowEvent` variant and
no migration**.

On every replay the frozen deadline is used **verbatim** and the resolution is
**never re-run**. An operator adding a holiday after a timer is armed therefore
can *never* move that timer's deadline; it only affects timers armed afterwards.
Both halves ride a single suspension batch, so arming a business-day timer costs
**one** decision cycle, not two.

Errors split into two classes on purpose:

| Class | Examples | Recorded? | Recovery |
|---|---|---|---|
| **Prologue** | no `BusinessCalendars` registered; the calendar **name** is not in the registered snapshot; `n` over `MAX_BUSINESS_DAYS`; the timer id collides with a **live cancellable `ctx.start_timer` handle** | No — zero commands | Fix config and redeploy; the call then succeeds |
| **Frozen** | coverage exhausted (the calendar cannot reach the resolved date) | Yes — replays identically forever | Workflow reset |

Every *deployment* misconfiguration is a prologue error, so correcting it and
redeploying is enough — no per-execution reset. Only the coverage rejection is
frozen, because it is a function of the captured anchor and a workflow that
catches it has already committed to a fallback branch.

> **`BusinessCalendars::builtin()` has an expiry date.** Its arrays declare a
> horizon (currently 2026-12-31), so once wall-clock time approaches it, every
> `builtin()`-backed resolution starts freezing coverage rejections fleet-wide —
> and extending the arrays later does not recover executions that already froze
> one. Treat the built-ins as demo/test data and register an operator-owned
> calendar (loaded from `harvest_calendar_exclusions`) in production.

Reusing one timer id across two *business-day* calls is permitted (each
invocation freezes under its own sequence number); it only makes a drift
diagnostic harder to read. The prologue rejects only a collision with the
**cancellable** `ctx.start_timer` API.

### Relationship to the other timer primitives

| Use | Reach for |
|---|---|
| Wait a fixed number of seconds | `ctx.timer(id, secs)` |
| Wait until an **absolute instant** you already have | `ctx.sleep_until(id, deadline)` (issue #749) |
| Wait **N business days**, skipping weekends + holidays | `ctx.timer_business_days(id, n, cal)` |
| A timer you can **cancel or reset** | `ctx.start_timer(id, secs)` → `TimerHandle` (issue #768) |
| Skip a *scheduled firing* on a holiday | Calendar-aware schedules (issue #337) |

Business-day timers are **fire-once**, like `ctx.timer` and `ctx.sleep_until` —
there is no cancellable business-day variant today. Sequential composition works
(arm one, await it, arm the next, each with its own timer id). Racing a
business-day timer against `receive_signal_timeout` in a *single* suspension is
**not supported**: the engine allows one `StartTimer` per suspension batch and
rejects the mixed batch loudly rather than silently mis-arming.

See `examples/business_day_escalation.rs` for a complete, tested example.

---

[← Your first workflow and activity](02-first-workflow.md) · [Index](README.md) · [Next: Signals →](04-signals.md)
