# Chapter 11 — Testing your workflow code

[← Operating the service](10-operations.md) · [Index](README.md)

---

Workflow code is deterministic, so it's testable without a database. Two
levels:

**1. Unit-test handlers in isolation.** Build a `WorkflowContext::new_test()`
or `ActivityContext::new_test()` (gated by the `testing` feature) and call
your function directly. Activities that read inputs and produce outputs are
trivial under this.

**2. Replay-test against recorded histories.** When you change a workflow
function, run it against histories captured from production with
`autumn_harvest::testing::WorkflowReplayer`:

```rust
use autumn_harvest::testing::{WorkflowReplayer, ReplayStatus};

#[tokio::test]
async fn checkout_replays() {
    let history = std::fs::read_to_string("fixtures/checkout_v3.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("checkout", checkout_handler)
        .replay_from_json(&history)
        .await
        .expect("fixture parses");

    assert!(matches!(report.status, ReplayStatus::ReplaySucceeded), "{report}");
}
```

The replayer never executes activities or touches Postgres — it runs the
workflow function in pure replay mode and compares the commands it emits
against the recorded history. A failure tells you exactly which event
diverged. Run this in CI on every workflow code change to catch
non-determinism *before* it produces DLQ entries.

See [`docs/runbooks/replay-fixture-export.md`](../runbooks/replay-fixture-export.md)
for capturing fixtures from a running service.

---

## `WorkflowTestEnv` — in-process harness with time-skipping

For workflows that branch on elapsed time (billing cycles, trial expiry, backoff
windows, SLA deadlines) the `WorkflowReplayer` alone is not enough — you need to
*drive* the workflow to completion and assert what `ctx.now()` returned at each
step. `WorkflowTestEnv` does exactly that, without Postgres or a real clock.

### Virtual-clock contract

| Operation | Clock effect |
|-----------|-------------|
| `ctx.timer(id, duration)` | Advances `ctx.now()` by `duration` when the timer fires from history |
| `ctx.receive_signal_timeout(signal, timeout)` — timer wins | Advances `ctx.now()` by `timeout` |
| `ctx.receive_signal_timeout(signal, timeout)` — signal wins | **No advance** (no `TimerStarted` event recorded) |
| Activity, local activity, child workflow, signal receive | **No advance** (instantaneous) |
| `WorkflowTestEnv::now()` | Always returns the construction-time `simulated_now`; unchanged |

The `WorkflowStarted` timestamp (and therefore `WorkflowTestEnv::now()`) remains
the construction-time value for the lifetime of the test. Only the *in-workflow*
`ctx.now()` advances as each durable timer fires from history. This matches
Temporal's "time-skipping" test feature.

### Billing-loop example

```rust
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct BillingOutput {
    charge1_date: String,
    charge2_date: String,
}

#[workflow]
async fn billing_cycle(ctx: &WorkflowContext, _: ()) -> Result<BillingOutput, String> {
    ctx.timer("month1", Duration::from_secs(30 * 24 * 3600))
        .await
        .map_err(|e| e.to_string())?;
    let charge1_date = ctx.now().format("%Y-%m-%d").to_string();

    ctx.timer("month2", Duration::from_secs(30 * 24 * 3600))
        .await
        .map_err(|e| e.to_string())?;
    let charge2_date = ctx.now().format("%Y-%m-%d").to_string();

    Ok(BillingOutput { charge1_date, charge2_date })
}

#[tokio::test]
async fn billing_cycle_dates_advance() {
    let env = WorkflowTestEnv::new();
    let outcome = env.run(billing_cycle_handler, ()).await;

    let result = outcome.result.as_ref().expect("workflow completed");
    let output: BillingOutput = serde_json::from_value(result.clone()).unwrap();

    // ctx.now() reflects elapsed virtual time at each billing point.
    assert!(output.charge1_date < output.charge2_date);

    // outcome.elapsed() = sum of all TimerStarted duration_secs = 60 days.
    assert_eq!(outcome.elapsed().num_days(), 60);

    // outcome.final_now() = construction time + 60 days.
    assert_eq!(outcome.final_now(), env.now() + chrono::Duration::days(60));

    // Wall-clock: no real sleeping — completes in milliseconds.
}
```

### Querying elapsed time from `TestRunOutcome`

`WorkflowTestEnv::run()` returns a `TestRunOutcome` with two time-skipping
accessors:

| Accessor | Returns |
|----------|---------|
| `outcome.final_now()` | Construction-time `start_time` + Σ `duration_secs` of every `TimerStarted` event along the taken execution path |
| `outcome.elapsed()` | `outcome.final_now() - start_time` as a `chrono::Duration` |

These accessors compute from the recorded event history — signal-preempted timers
produce no `TimerStarted` event and therefore contribute nothing to the sum,
matching the virtual-clock rule above.

### Signal pre-empting a timer (no advance)

When a signal arrives before the deadline the virtual clock does **not** advance:

```rust
let mut env = WorkflowTestEnv::new();
env.queue_signal("approved", serde_json::json!({ "approver": "alice" }));

let outcome = env.run(approval_workflow_handler, ()).await;
// Signal fired first — no TimerStarted recorded — clock unchanged.
assert_eq!(outcome.elapsed().num_seconds(), 0);
```

### 365-day sleep in under 50 ms

No real sleeping occurs. A workflow that durable-sleeps for an entire year runs
to completion in a few milliseconds:

```rust
#[tokio::test]
async fn year_of_timers_is_fast() {
    let start = std::time::Instant::now();
    let env = WorkflowTestEnv::new();
    let outcome = env.run(yearly_schedule_handler, ()).await;

    assert_eq!(outcome.final_now(), env.now() + chrono::Duration::days(365));
    assert!(start.elapsed().as_millis() < 50, "time-skipping must be fast");
}
```

---

[← Operating the service](10-operations.md) · [Index](README.md)

You've reached the end of the guide. Head back to the [index](README.md) for
links to the reference example, runbooks, and architecture docs.
