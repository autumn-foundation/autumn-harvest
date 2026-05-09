# Workflow Determinism Guide

Harvest replays the workflow function body every time a workflow resumes. The engine re-runs the function from the top, replaying recorded events from `harvest_events` instead of re-executing side-effects. This means the workflow function **must be deterministic**: given the same recorded history, every re-execution must produce exactly the same sequence of commands.

This guide explains the rule catalog (`autumn_harvest::guardrail`) that documents the required determinism constraints and the Harvest-safe alternatives.

---

## Why replay determinism matters

When a workflow suspends (waiting for an activity, a timer, or a signal), Harvest saves its progress as an ordered sequence of events in `harvest_events`. On resume, the engine re-invokes the workflow function and drives it forward by replaying those saved events. If the function produces a different command on replay than it produced originally, the engine raises a `NonDeterminismError` and moves the execution to the dead-letter queue.

Common replay footguns all share the same root cause: **code that returns a different value each time it is called**.

---

## The guardrail rule catalog

Each rule has a stable ID (`HVGxxx`), a severity level, and actionable guidance. Hard blockers indicate patterns that will definitely break replay; warnings indicate patterns that may be acceptable in some contexts but should be reviewed.

The catalog is available at runtime:

```rust
use autumn_harvest::guardrail::{catalog, rule_by_id};

for rule in catalog() {
    println!("{}: {:?} — {}", rule.id, rule.severity, rule.explanation);
}

let rule = rule_by_id("HVG001").unwrap();
```

---

## Allowed vs. disallowed patterns

### HVG001 — Wall-clock time (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `let now = std::time::SystemTime::now();` |
| **Disallowed** | `let ts = chrono::Utc::now();` |
| **Disallowed** | `let t = std::time::Instant::now();` |
| **Allowed** | `let now = ctx.current_time();` |
| **Allowed** | Move the timestamp read inside an `#[activity]` and return it as the result |

Each replay produces a different "now", causing the workflow to diverge from its recorded history.

**Migration example:**

```rust
// Before — breaks replay
#[workflow]
async fn billing(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    let invoice_date = chrono::Utc::now(); // ← HVG001
    // ...
}

// After — deterministic
#[workflow]
async fn billing(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    let invoice_date = ctx.current_time();  // recorded in history
    // ...
}
```

---

### HVG002 — Randomness / ad-hoc UUIDs (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `let n: u64 = rand::random();` |
| **Disallowed** | `let id = uuid::Uuid::new_v4();` |
| **Disallowed** | `use rand::Rng; rng.gen::<f64>()` |
| **Allowed** | Pass randomness as a workflow input parameter |
| **Allowed** | Generate randomness inside an `#[activity]` and return it |
| **Allowed** | `ctx.execution_id()` for a replay-safe unique identifier |

**Migration example:**

```rust
// Before — breaks replay
#[workflow]
async fn process(ctx: &WorkflowContext) -> Result<String, String> {
    let trace_id = uuid::Uuid::new_v4().to_string(); // ← HVG002
    Ok(trace_id)
}

// After — deterministic
#[activity]
async fn generate_trace_id(_ctx: &ActivityContext) -> Result<String, String> {
    Ok(uuid::Uuid::new_v4().to_string()) // fine in an activity
}

#[workflow]
async fn process(ctx: &WorkflowContext) -> Result<String, String> {
    let trace_id = ctx.execute_activity_raw("generate_trace_id", serde_json::Value::Null).await?;
    Ok(trace_id.to_string())
}
```

---

### HVG003 — Process/environment reads (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `std::env::var("DATABASE_URL")` |
| **Disallowed** | `std::env::args().collect::<Vec<_>>()` |
| **Allowed** | Read config at worker startup, store in `WorkerConfig` state |
| **Allowed** | Pass values as workflow input parameters |
| **Allowed** | `ctx.state::<T>()` for typed state registered on the builder |

Environment variables may differ between the original worker and a replay worker, causing divergence.

---

### HVG004 — Direct sleep / timer primitives (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `std::thread::sleep(Duration::from_secs(60))` |
| **Disallowed** | `tokio::time::sleep(Duration::from_secs(60)).await` |
| **Allowed** | `ctx.sleep(Duration::from_secs(60)).await` |
| **Allowed** | `DagBuilder` with `Schedule::Interval(...)` for periodic workflows |

Direct sleeps block the worker task and write nothing to `harvest_events`. After a worker restart the sleep is gone and timing changes.

---

### HVG005 — Background task spawning (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `tokio::spawn(async { do_work().await });` |
| **Disallowed** | `std::thread::spawn(|| heavy_computation());` |
| **Allowed** | `futures::join!(ctx.execute_activity_raw(...), ctx.execute_activity_raw(...))` |
| **Allowed** | `#[activity(local = true)]` for lightweight in-process work |

Spawned tasks run outside Harvest supervision: they are not retried, not recorded, and are silently abandoned on worker restart.

---

### HVG006 — Direct network / database / filesystem I/O (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `reqwest::get("https://api.example.com/data").await` |
| **Disallowed** | `sqlx::query("SELECT ...").fetch_all(&pool).await` |
| **Disallowed** | `std::fs::read_to_string("config.json")` |
| **Allowed** | Wrap all I/O in `#[activity]` functions |
| **Allowed** | Return I/O results as activity outputs, which are recorded in history |

I/O is non-idempotent: replaying it sends duplicate requests, corrupts database state, or fails because external state has changed.

---

### HVG007 — Process-global state mutation (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `static COUNTER: AtomicU64 = AtomicU64::new(0); COUNTER.fetch_add(1, Ordering::Relaxed);` |
| **Disallowed** | `lazy_static! { static ref REGISTRY: Mutex<Vec<String>> = ...; } REGISTRY.lock().push(...)` |
| **Allowed** | Accumulate state in local variables across `ctx.sleep()` and activity boundaries |
| **Allowed** | Emit metrics/updates inside activities, not in workflow code |

Global mutations are re-applied on every replay, causing double-counting or inconsistent state across workers.

---

## Machine-readable findings and suppressions

A future checker will emit `GuardrailFinding` values. Each finding is serializable and carries the rule ID, severity, category, message, alternative, workflow name (if known), and source location (if available).

```rust
use autumn_harvest::guardrail::{GuardrailFinding, RuleCategory, Severity, rule_by_id};

let entry = rule_by_id("HVG001").unwrap();
let finding = GuardrailFinding::from_rule(
    entry,
    "chrono::Utc::now() called at workflows/billing.rs:34",
    Some("billing".to_string()),
    Some("src/workflows/billing.rs:34".to_string()),
);

assert!(matches!(finding.severity, Severity::HardBlocker));
assert!(matches!(finding.category, RuleCategory::WallClock));
```

### Suppressing a finding

Suppressions require an explicit, non-empty reason string. Empty or whitespace-only reasons are rejected at construction time so suppressions are always auditable.

```rust
use autumn_harvest::guardrail::{GuardrailSuppression, GuardrailSuppressionError};

// Accepted — reason is meaningful
let s = GuardrailSuppression::new(
    "HVG001",
    "This workflow uses a seeded deterministic clock under test; covered by replay fixture.",
).unwrap();

// Rejected — reason is empty
let err = GuardrailSuppression::new("HVG001", "").unwrap_err();
assert_eq!(err, GuardrailSuppressionError::EmptyReason);
```

---

## Composing with the release playbook

The determinism rule catalog is an early-stage guardrail, not the final proof of replay safety. The recommended release sequence:

1. **Determinism check** (this catalog): catch obvious footguns before any workflow has history.
2. **History export** ([issue #169](https://github.com/madmax983/autumn-harvest/issues/169)): export event histories from staging as replay fixtures.
3. **WorkflowReplayer** (`autumn_harvest::testing::WorkflowReplayer`): verify the new code replays all exported fixtures without divergence.
4. **Version gate** (`ctx.version()`): use version gates for intentional non-determinism across deploys, and retire old gates after the fleet has fully rolled forward.
5. **Build-id routing** (`WorkerConfig::with_build_id`): gate new executions on the new build until compatibility is declared.

Each layer catches a different class of problem. All five together provide defence-in-depth for safe rolling deploys.
