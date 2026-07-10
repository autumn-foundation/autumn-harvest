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
        // Each item renews the SLA window — reset cancels the old arming and
        // starts a fresh one, so there is never an orphaned timer left to fire late.
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

- **`start_timer` does not suspend** — it records the arming and returns a
  handle immediately, so the workflow keeps running.
- **`cancel()`** deletes the durable timer row and records a `TimerCancelled`
  event, so **no `TimerFired` is ever produced** for a cancelled timer.
- **`reset(secs)`** = cancel + re-arm; intentionally O(1) history per reset
  (two events) with **zero orphaned firings**.
- **`await_fire()`** suspends until the timer fires (`TimerOutcome::Fired`) or
  is cancelled (`TimerOutcome::Cancelled`).

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

---

[← Your first workflow and activity](02-first-workflow.md) · [Index](README.md) · [Next: Signals →](04-signals.md)
