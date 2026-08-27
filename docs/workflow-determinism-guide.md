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

## Compile-time enforcement

Since version 0.3.0, the `#[workflow]` attribute macro automatically scans the annotated function body at compile-time to enforce these guardrails:

- **Hard Blockers** (`HVG001` through `HVG008`, `HVG010`, and `HVG011` when the flagged loop schedules commands): Trigger compilation errors at the exact site of the violation, preventing the build from succeeding with unsafe code.
- **Warnings** (`HVG009`, and `HVG011` when the flagged loop is command-free): Emit standard deprecation compiler warnings (`note = "..."`) at the exact site to encourage migration without breaking CI or blocking local development.

### Suppressing compile-time guardrails

If a workflow legitimately needs to invoke non-deterministic APIs directly, the compile-time checks can be completely disabled by providing the `allow_nondeterministic_apis` attribute flag:

```rust
#[workflow(allow_nondeterministic_apis)]
async fn legacy_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    // HardBlockers and Warnings are now allowed at compile time
    let now = chrono::Utc::now(); 
    tracing::info!("bare logging");
    Ok(())
}
```

The flag also supports explicit boolean syntax:
```rust
#[workflow(allow_nondeterministic_apis = true)]
```

> [!NOTE]
> The compile-time linter performs a shallow AST traversal (matching path segments and patterns). It is designed to catch the most common patterns, but it does not replace runtime verification. Always run [WorkflowReplayer](replay-verify.md) tests for critical production workflows.

---

## Allowed vs. disallowed patterns

### HVG001 — Wall-clock time (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `let now = std::time::SystemTime::now();` |
| **Disallowed** | `let ts = chrono::Utc::now();` |
| **Disallowed** | `let t = std::time::Instant::now();` |
| **Allowed** | `let now = ctx.now();` |
| **Allowed** | Move the timestamp read inside an `#[activity]` and return it as the result |

Each replay produces a different "now", causing the workflow to diverge from its recorded history. `ctx.now()` returns the `WorkflowStarted` timestamp, which is recorded once and replayed identically on every subsequent run.

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
    let invoice_date = ctx.now(); // recorded as WorkflowStarted timestamp, replays identically
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
| **Allowed** | `ctx.random_uuid(id)` for a replay-safe unique identifier |

`ctx.random_uuid(id)` generates a UUID on first execution, records it in history under the given stable `id`, and replays the same UUID on every subsequent run.

**Migration example:**

```rust
// Before — breaks replay
#[workflow]
async fn process(ctx: &WorkflowContext) -> Result<String, String> {
    let trace_id = uuid::Uuid::new_v4().to_string(); // ← HVG002
    Ok(trace_id)
}

// After — deterministic (option A: ctx.random_uuid)
#[workflow]
async fn process(ctx: &WorkflowContext) -> Result<String, String> {
    let trace_id = ctx.random_uuid("trace-id")
        .map_err(|e| e.to_string())?
        .to_string();
    Ok(trace_id)
}

// After — deterministic (option B: activity)
#[activity]
async fn generate_trace_id(_ctx: &ActivityContext) -> Result<String, String> {
    Ok(uuid::Uuid::new_v4().to_string()) // fine inside an activity
}

#[workflow]
async fn process(ctx: &WorkflowContext) -> Result<String, String> {
    let trace_id = ctx
        .execute_activity_raw("generate_trace_id", serde_json::Value::Null, "default")
        .await?;
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
| **Allowed** | `ctx.timer("my-timer", 60).await` |
| **Allowed** | `DagBuilder` with `Schedule::Interval(...)` for periodic workflows |

Direct sleeps block the worker task and write nothing to `harvest_events`. After a worker restart the sleep is gone and timing changes. `ctx.timer(timer_id, duration_secs)` emits a `TimerStarted` event into durable history and is enforced by the harvest timeout scanner across restarts.

---

### HVG005 — Background task spawning (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `tokio::spawn(async { do_work().await });` |
| **Disallowed** | `std::thread::spawn(|| heavy_computation());` |
| **Allowed** | `futures::join!(ctx.execute_activity_raw("a", input, "q"), ctx.execute_activity_raw("b", input, "q"))` |
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
| **Allowed** | Accumulate state in local variables across `ctx.timer()` and activity boundaries |
| **Allowed** | `ctx.metrics().counter(...)` / `ctx.metrics().histogram(...)` (replay-safe) for workflow-body metrics |
| **Allowed** | Emit other side-channel updates (non-metric writes to external systems) inside activities, not in workflow code |

Global mutations are re-applied on every replay, causing double-counting or inconsistent state across workers.

For business metrics specifically, do **not** reach for a global registry or a
raw `metrics::counter!(...)` call — use the sanctioned replay-safe primitive
instead: `ctx.metrics().counter/gauge/histogram` on both `WorkflowContext` and
`ActivityContext` (issue #532). Workflow-side emission is suppressed while
`ctx.is_replaying()` is true, so a counter incremented once in workflow code
increments the backend exactly once no matter how many replay cycles the
executor runs; activity-side emission fires on every attempt (each retry is a
separate execution). Names are auto-namespaced under `harvest.user.*`, label
keys must stay low-cardinality (`execution.id`/`workflow.id` as a label is a
rejected anti-pattern, ADR-0001 §7), and delivery is best-effort
at-least-once across a worker crash — a crash after the emission but before
the next event commits re-emits for the re-executed segment. See the "Custom
workflow/activity metrics" section of `docs/telemetry.md` and
`examples/business_metrics.rs`.

---

### HVG008 — Non-deterministic predicates in await_condition (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `ctx.await_condition(|| Instant::now() > start_time)` |
| **Disallowed** | `ctx.await_condition(|| rand::random())` |
| **Allowed** | `ctx.await_condition(|| local_approvals_count >= 2)` |

Predicates evaluated inside `await_condition` and `await_condition_timeout` must be purely deterministic projections of workflow local state (variables rehydrated by replaying events). Using non-deterministic values (like the current system time `Instant::now()` or random numbers) inside these closures will yield different results during replay than in the original execution, leading to early/late completion or early/late timer triggers, which causes `NonDeterminismError` during history matching.

**Migration example:**

```rust
// Before — breaks replay
#[workflow]
async fn wait_for_timeout(ctx: &WorkflowContext) -> Result<(), String> {
    let start = std::time::Instant::now();
    ctx.await_condition(move || {
        start.elapsed() >= Duration::from_secs(60) // ← HVG008
    })
    .await?;
    // ...
}

// After — deterministic
#[workflow]
async fn wait_for_timeout(ctx: &WorkflowContext) -> Result<(), String> {
    ctx.timer("delay-timer", 60).await?; // recorded in history and replays identically
    // ...
}
```

---

### HVG009 — Bare tracing calls inside a workflow body (Warning)

| | Example |
|---|---|
| **Disallowed** | `tracing::info!("order {} started", order_id)` |
| **Disallowed** | `tracing::warn!("retrying payment")` |
| **Allowed** | `ctx.logger().info("order started")` |
| **Allowed** | `ctx.log_info("order started")` |

The workflow executor re-runs the function body from the top on every suspend/resume cycle (replay). A bare `tracing::info!()` call placed in the workflow body therefore fires **N times** for a workflow that suspends N times: once on the original live run, and once on each subsequent replay before the suspension point is reached. This amplifies log volume in proportion to replay depth and produces duplicate lines in Loki/Elastic that lack correlation keys, making incident triage much harder.

#### The Harvest-safe alternative

`ctx.logger()` returns a [`WorkflowLogger`](../autumn-harvest/src/context.rs) that:
- **Suppresses** all output when `ctx.is_replaying()` is `true` — so each log statement fires at most once per execution, regardless of replay depth.
- **Auto-tags** every event with `workflow_id`, `execution_id`, `workflow_type`, and `replay = false` — so a single `loki | jq 'select(.execution_id == "…")'` returns a clean chronological narrative of the run.

**Migration example:**

```rust
// Before — fires once per replay cycle (N times for N suspensions)
#[workflow]
async fn process_order(ctx: &WorkflowContext, order_id: String) -> Result<(), String> {
    tracing::info!("processing order {}", order_id); // ← HVG009: fires on every replay

    ctx.execute_activity(&charge_card_info(), order_id.clone()).await?;
    ctx.execute_activity(&ship_order_info(), order_id).await?;
    Ok(())
}

// After — fires exactly once, on the live (non-replay) execution
#[workflow]
async fn process_order(ctx: &WorkflowContext, order_id: String) -> Result<(), String> {
    ctx.logger().info("processing order");          // suppressed during replay ✓
    // or the equivalent shorthand:
    ctx.log_info("processing order");               // same behaviour

    ctx.execute_activity(&charge_card_info(), order_id.clone()).await?;
    ctx.execute_activity(&ship_order_info(), order_id).await?;
    Ok(())
}
```

#### Auto-tagged structured fields

Every event emitted by `ctx.logger()` carries:

| Field | Value | Purpose |
|---|---|---|
| `workflow_id` | Business-level workflow key (e.g. `"order-42"`) | Correlate all events for one logical workflow instance |
| `execution_id` | Unique run UUID | Correlate all events for one specific run |
| `workflow_type` | Registered function name (e.g. `"process_order"`) | Filter by workflow type across all runs |
| `replay` | `false` | Confirm event was not emitted during replay |

These match the Temporal / Cadence / DBOS convention so existing log dashboards need no changes.

#### Reading a run's lines back without a log aggregator

`ctx.logger()` emits to the host app's `tracing` subscriber, so reading a
specific run's lines back normally means correlating by `execution_id` in
Loki/Elastic/OTel. Opt in to the **durable per-execution sink** (issue #790) and
the same `ctx.log_*` calls are also persisted per execution, readable in one
call — `GET /api/harvest/workflows/{id}/logs`, `harvest workflow logs <id>`, or
the Vantage execution-detail **Logs** panel. The workflow body does not change;
see [`docs/workflow-logs.md`](workflow-logs.md).

#### Guardrail severity

HVG009 is a **Warning** (not a HardBlocker): bare tracing calls do not break determinism or corrupt workflow state — they only amplify log volume. The rule is surfaced so authors can fix it without CI being blocked by a false positive.

---

### HVG010 — `tokio::select!` / `futures::select!` over ctx awaitables (HardBlocker)

| | Example |
|---|---|
| **Disallowed** | `tokio::select! { r = ctx.timer("t", 60) => {}, s = ctx.wait_for_signal("approve") => {} }` |
| **Disallowed** | `futures::select! { a = fut_a.fuse() => {}, b = fut_b.fuse() => {} }` |
| **Disallowed** | `futures::future::select(fut_a, fut_b).await` (also `select_all` / `select_ok` / `try_select`) |
| **Allowed** | `ctx.race().timer(Duration::from_secs(60)).signal("approve").run().await?` |
| **Allowed** | `ctx.race().activity_raw("fetch_a", input, "q").activity_raw("fetch_b", input, "q").run().await?` |

HVG010 flags both the select **macros** (`tokio::select!`, `futures::select!`, `futures::select_biased!`) and their function-call siblings, the `futures::future::{select, select_all, select_ok, try_select}` **combinators** (issue #799) — they carry the identical footgun. (Inside an `#[activity]` body `select!` is fine: only the activity's recorded *result* matters, not its internal control flow, so activities may race freely.)

Harvest already sanctions `futures::join!`/`futures::try_join!` for wait-**all** concurrency (see HVG005 above: "Harvest records each branch's result durably and re-joins them correctly on replay"). There is no equivalent sanction for wait-**first** — `select!` is a double footgun in a replay engine:

1. **Non-deterministic winner.** The branch that wins depends on poll/arrival order on whichever worker happens to be replaying. A replayed history can pick a *different* branch than the original live run and diverge, since `select!` has no durable record of which branch actually completed first.
2. **No durable cancellation of the losers.** Dropping the losing branches' futures does not durably cancel the underlying work: a scheduled activity keeps running to completion on its worker (and its eventual `ActivityCompleted`/`ActivityFailed` event lands in history unconsumed), and a durable timer row in `harvest_timers` stays live indefinitely.

`ctx.race()` (issue #600) is the deterministic alternative: it records the winning branch via the existing `MarkerRecorded` event (no new `WorkflowEvent` variant — the same idiom `execute_activity_fan_out` already uses for its count marker), so replay always resolves the identical winner, and it durably cancels every losing branch — a still-open activity's task row is cancelled and a synthetic terminal is recorded, a losing child workflow is cancelled via the same primitive `ctx.request_cancel_external_workflow` uses, and a losing durable timer row is removed.

**Migration example:**

```rust
// Before — non-deterministic winner, losers leak
#[workflow]
async fn hedge_providers(ctx: &WorkflowContext, req: Value) -> Result<Value, String> {
    tokio::select! {                                            // ← HVG010
        a = ctx.execute_activity_raw("fetch_primary", req.clone(), "default") => a.map_err(|e| e.to_string()),
        b = ctx.execute_activity_raw("fetch_fallback", req, "default") => b.map_err(|e| e.to_string()),
    }
}

// After — deterministic winner, loser durably cancelled
#[workflow]
async fn hedge_providers(ctx: &WorkflowContext, req: Value) -> Result<Value, String> {
    let winner = ctx
        .race()
        .activity_raw("fetch_primary", req.clone(), "default")
        .activity_raw("fetch_fallback", req, "default")
        .run()
        .await
        .map_err(|e| e.to_string())?;
    Ok(winner.value)
}
```

`ctx.race()` currently supports three shapes in one call: a homogeneous race of activity branches, a homogeneous race of child-workflow branches, or exactly one timer branch paired with exactly one signal branch (a thin wrapper over `receive_signal_timeout`/`wait_for_signal_timeout`, issue #476). Mixing branch kinds (e.g. an activity racing a timer in the same call) is out of scope for this slice and returns `HarvestError::Config` — bound an individual activity with its own `start_to_close`/`schedule_to_close` timeout, or compose a separate `receive_signal_timeout`, to express a deadline-bounded branch instead. See the `WorkflowContext::race` rustdoc for the full determinism contract.

#### Guardrail severity

HVG010 is a **HardBlocker**: an unguarded `select!` over ctx-managed awaitables can silently diverge a replay or leak in-flight activities/timers, both of which are worse than a build failure.

#### Heuristic pre-check vs. authoritative guardrail

The compile-time HVG010 proc-macro guardrail (syn-based, operating on the parsed AST) is the **authoritative** gate — it always hard-blocks these forms, including turbofished calls like `future::select::<_, _>(a, b)` (it strips path arguments before matching). Its `det_check` twin (`DET011`) is a best-effort **text** pre-check that mirrors the guardrail so problems surface early in review or CI without a full build; being text-based it can lag on exotic syntax (turbofish, unusual spacing, multi-line calls). When the two disagree, trust the compile-time guardrail — it is the safety net — and reach for the escape hatch (`#[workflow(allow_nondeterministic_apis)]`, or a `// harvest-suppress: DET011 "reason"` comment for `det_check`) only when the race is provably safe.

---

### HVG011 — `HashMap`/`HashSet` iteration order (HardBlocker*, command-aware)

> **Rule-ID note:** issue #785's text proposed HVG010, but HVG010 was already permanently assigned to SelectMacro (issue #600) and rule IDs are never reused — the iteration-order rule ships as **HVG011** in the catalog/macro lint and **DET010** in `det_check`.

| | Example |
|---|---|
| **Disallowed** | `for (k, v) in &my_hash_map { ctx.execute_activity_raw(...).await?; }` |
| **Disallowed** | `for key in my_hash_map.keys() { ... }` (also `.values()`, `.iter()`, `.drain()`, `.into_iter()`, `.into_keys()`, `.into_values()`) |
| **Allowed** | Iterate a `BTreeMap`/`BTreeSet` instead — deterministic key order |
| **Allowed** | `let mut keys: Vec<_> = map.keys().cloned().collect(); keys.sort(); for k in keys { ... }` |
| **Allowed** | Point lookups on a `HashMap`/`HashSet` that is never iterated |

`HashMap`/`HashSet` iteration order is hash-randomized per process (`RandomState` seeds differ across workers and restarts), so a replay on another worker can visit the entries in a different order than the original run. When the loop body schedules commands (`ctx.execute_activity*`, `ctx.spawn_child_workflow*`, `ctx.execute_local_activity*`, `ctx.timer`, `ctx.side_effect`), the command sequence is recorded in history **in iteration order** — a reordered replay produces a different command sequence and diverges (non-determinism error / nd-block, issue #603 semantics).

#### Guardrail severity

HVG011 is command-aware: the macro lint and `det_check` inspect the loop body. A loop that schedules commands (`ctx.execute_activity*`, `ctx.spawn_child_workflow*`, `ctx.execute_local_activity*`, `ctx.timer`, `ctx.side_effect`, `ctx.race`) is a **HardBlocker** (compile error / `DetSeverity::Error`); a command-free loop is downgraded to a **Warning** (deprecation-note mechanism, like HVG009), since it only risks leaking the non-deterministic order into workflow-local state that a later branch might observe.

**Syntactic boundary (deliberately narrow, false positives are the top risk):** only *locally `let`-bound* collections are tracked — hash-typed via an explicit type annotation whose root type is `HashMap`/`HashSet` (`let m: HashMap<K, V> = …`; a nested mention like `Vec<HashMap<..>>` or `Option<HashMap<..>>` does not track), a constructor call (`HashMap::new()`, `HashSet::from(..)`, `default`, `with_capacity` variants), or a `.collect::<HashMap<..>>()` turbofish (including the fallible `.collect::<Result<HashMap<..>, E>>()?` / `Option`-wrapped forms). Binding tracking is **lexically scoped**: re-binding the same ident to a non-hash type shadows it for that binding's scope, an inner-block hash binding never leaks past the block exit, and (in the macro lint) match-arm patterns, closure parameters, destructuring `let`s, and for-loop patterns all shadow outer tracked idents. Only a bare tracked ident, `&ident` / `&mut ident`, or exactly one argument-free iteration method call on it is flagged — longer chains (`map.keys().sorted()`) are never flagged, so already-sorted iterators always pass. Function parameters and struct fields are never flagged.

**Surface divergences (line-based `det_check` vs. syn-based macro lint):** the macro lint is scope-exact; `det_check`'s line-based pass can still over-flag an ident re-bound by a *match-arm pattern* or a *closure parameter* (pattern masking there is not tractable line-by-line — suppress with `// harvest-suppress: DET010 "reason"` when intended), and misses a `for` header split across lines (`for (k, v) in` / `&m`) — the macro lint catches that shape. `det_check` also shares the pre-existing DET001–DET009 lexer caveat that the continuation lines of a multi-line string literal are lexed as code and can perturb binding tracking.

**Migration example:**

```rust
// Before — breaks replay: debit order follows the randomized hash order
#[workflow]
async fn settle(ctx: &WorkflowContext, input: Value) -> Result<(), String> {
    let mut amounts: HashMap<String, u64> = HashMap::new();
    // ... populate from input ...
    for (account, amount) in &amounts {                          // ← HVG011
        ctx.execute_activity_raw("debit", json!({ "account": account, "amount": amount }), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// After (option A) — BTreeMap: deterministic key order
#[workflow]
async fn settle(ctx: &WorkflowContext, input: Value) -> Result<(), String> {
    let mut amounts: BTreeMap<String, u64> = BTreeMap::new();
    // ... populate from input ...
    for (account, amount) in &amounts {                          // deterministic ✓
        ctx.execute_activity_raw("debit", json!({ "account": account, "amount": amount }), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// After (option B) — keep the HashMap, iterate sorted keys
#[workflow]
async fn settle(ctx: &WorkflowContext, input: Value) -> Result<(), String> {
    let mut amounts: HashMap<String, u64> = HashMap::new();
    // ... populate from input ...
    let mut accounts: Vec<String> = amounts.keys().cloned().collect();
    accounts.sort();
    for account in accounts {                                    // deterministic ✓
        let amount = amounts[&account];
        ctx.execute_activity_raw("debit", json!({ "account": account, "amount": amount }), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
```

**Suppression:** in `det_check`, place `// harvest-suppress: DET010 "reason"` — or the guardrail-catalog spelling `// harvest-suppress: HVG011 "reason"`, honored as an alias — on the `for` line or the standalone comment line above it (echoed into `DetCheckReport::suppressions` with whichever id you wrote, for auditability). The macro lint has no per-site suppression — only the whole-function `#[workflow(allow_nondeterministic_apis)]` escape hatch, which bypasses HVG011 like every other rule. Iteration inside a `ctx.side_effect(..)` closure is not flagged by the compile-time macro lint (the closure's value is recorded once and replayed verbatim); the `det_check` scanner may still surface a DET010 Warning there — suppress with `// harvest-suppress: DET010 "reason"` if intended.

---

## Machine-readable findings and suppressions

`GuardrailFinding` and `GuardrailSuppression` both implement `serde::Serialize`/`Deserialize` and can be serialized to JSON for CI reports or external tooling.

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

// Serialize to JSON for CI output
let json = serde_json::to_string_pretty(&finding).unwrap();
```

### Suppressing a finding

Suppressions require an explicit, non-empty reason string and a non-empty rule ID. Empty or whitespace-only values are rejected at construction time so suppressions are always auditable.

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

// Rejected — rule ID is empty
let err = GuardrailSuppression::new("", "some reason").unwrap_err();
assert_eq!(err, GuardrailSuppressionError::EmptyRuleId);
```

---

## Running the check in CI

The `det_check` engine ships behind a runnable front door: the **`harvest det-check`** CLI subcommand ([issue #778](https://github.com/autumn-foundation/autumn-harvest/issues/778)). It statically flags non-deterministic API calls reachable from your `#[workflow]` bodies — including through **one hop** of first-party helper functions the body calls directly — so you catch replay foot-guns at PR/CI time instead of post-deploy when in-flight executions DLQ.

```console
# Scan the current directory (default). Directories are scanned recursively;
# `target` and hidden directories (`.git`, …) are skipped.
$ harvest det-check

# Scan specific paths (files or directories).
$ harvest det-check src examples

# Machine-readable output for CI consumption.
$ harvest det-check --format json src

# Also fail on warnings (DET005/DET009 and command-free DET010).
$ harvest det-check --deny-warnings src

# Audit the escape-hatch inventory: list every active suppression.
$ harvest det-check --list-suppressions src
```

### Flags and exit-code contract

| Flag | Effect |
|------|--------|
| `[PATHS...]` | Source paths to scan. Default: `.` (current directory). |
| `--format text\|json` | Output format. `text` (default) prints `file:line:col DETxxx  (safe alternative: …)`, one line per finding, with a transitive finding also naming `[in helper `H` reached from workflow `W`]`, followed by a `suppressed:` audit footer. `json` emits a full `DetCheckReport` (findings + suppressions). |
| `--deny-warnings` | Also gate (exit `1`) when any warning-severity finding is present. |
| `--list-suppressions` | Print every active `harvest-suppress` with its reason and location, then exit `0`. |

Exit codes: **`0`** when there are no hard-blocker findings; **`1`** when any `Error`-severity finding is present. Warning-severity findings (DET005 process reads, DET009 bare tracing, and a command-free DET010 loop) never fail the build unless `--deny-warnings` is passed. The findings (or JSON) are always printed to stdout *before* the non-zero exit, so CI logs are self-explanatory.

### Transitive coverage and its boundary

A hard-blocker call located in a first-party function that a `#[workflow]` body reaches **via a direct free-function call** is flagged, naming both the offending site and the workflow entry point. The boundary deliberately mirrors the `#[workflow]` compile-time lint ([issue #386](https://github.com/autumn-foundation/autumn-harvest/issues/386)) — the following are **out of scope** and are *not* detected:

- **`#[activity]` bodies** — activities are allowed to be non-deterministic by design and are never scanned (directly or via reachability).
- **Method calls** (`x.helper()`) and path-qualified calls (`m::helper()`) — only bare free-function calls (`helper()`) are resolved. This is the trait-dispatch / receiver boundary #386 also draws.
- **Two or more hops** — reachability follows exactly one hop. A workflow → `helper_a` → `helper_b` (violation) chain is *not* flagged.
- Third-party crates, trait-object dispatch, function pointers, and closures captured by reference.

The catalog docs continue to own the "you must also avoid this transitively beyond one first-party hop" warning. Everything det-check skips here is a false-*negative* that the compile-time `#[workflow]` guardrail ([issue #386](https://github.com/autumn-foundation/autumn-harvest/issues/386)) **also** misses — #386 is likewise **body-only**, so it does *not* hard-block a non-deterministic call hidden inside a helper det-check skips (closing that body-only blind spot is exactly what det-check's one-hop pass partially does). det-check deliberately mirrors #386's boundary for what it *attempts* to analyze, and #386 remains the complementary compile-time check for **direct-in-body** violations — but neither is the net for these transitive skips. That net is the **runtime** determinism detection: `harvest-replay` / `WorkflowReplayer` run against recorded histories in tests/CI (post-hoc, once a history exists), and the live `HistoryMatcher` non-determinism check at execution time (which surfaces as a DLQ'd run) — plus manual code review.

### Known limitations (conservative, safe-direction)

`det_check` is a fast text pre-filter, not the authoritative gate. Its reachability resolution deliberately errs toward **missing** a transitive violation rather than reporting a false one — every case below is a false-*negative* (never a false-positive). These transitive false-negatives are **not** caught by the compile-time `#[workflow]` HVG guardrail ([issue #386](https://github.com/autumn-foundation/autumn-harvest/issues/386)) either: #386 is itself **body-only** — it scans only the annotated workflow body, so it does *not* hard-block a non-deterministic call hidden inside a helper det_check skips (that same body-only blind spot is exactly what det_check's one-hop pass was built to partially close). The backstop for what static analysis can't resolve is the **runtime** determinism detection — `WorkflowReplayer` / `harvest-replay` against recorded histories in tests/CI, and the live `HistoryMatcher` non-determinism check at execution time (which DLQs the run) — plus manual code review. When in doubt, trust `WorkflowReplayer` and a careful read of the helper, not the compile-time guardrail (which cannot see into these helpers).

- **Module scope vs. methods.** `#[workflow]` entry points and their first-party helpers are resolved at **module scope** — top level *and* inside `mod NAME { … }` blocks, at any nesting depth. Methods declared inside `impl`/`trait` blocks are **never** indexed as free helpers and a call to one (`self.method()`, `Type::method()`) is never resolved (the #386 method-exclusion boundary).
- **One-hop resolution is same-module only.** A bare call `helper()` resolves to a first-party helper **only when that helper is declared in the caller's own module** (same file + `module_path`) — the one case a bare call provably reaches without an import the line-based scanner cannot see. A helper in a **different module or file** (reached via a `use` import, including an aliased `use ... as helper`) is **not** resolved; it is treated as ambiguous and skipped (a safe false-negative). This is deliberate: the text front-door cannot resolve `use` imports/aliases, so *any* cross-module resolution keeps producing false positives on innocent imported/aliased code — and for a CI gate a false positive (failing CI on innocent code) is the worst outcome, outranking marginal recall. Even a **globally unique** helper name is not resolved cross-module. This ends the Codex round-4 (same-file-different-module) and round-5 (aliased-import, globally-unique) false-positive family structurally. A **block-local `use` import inside the workflow body** rebinds a same-module name too (a `use crate::x::helper;` in the body makes `helper()` resolve to the import, not a sibling `fn helper`); such imports are added to the shadow set below so this last same-module rebind vector no longer false-positives (a module/file-level `use` + a same-name `fn` is itself a compile error, so a block-local `use` is the only way to shadow a same-module helper — Codex r6). Because the compile-time `#[workflow]` guardrail (#386) is itself body-only, it does *not* catch a violation inside such a skipped cross-module/imported helper either; `WorkflowReplayer` (and the live `HistoryMatcher` at execution time) is the backstop for this false-negative, not #386.
- **Local shadowing (by binding kind, matching Rust scoping).** A call whose name is bound by the caller — a fn-pointer / closure **parameter**, a local **closure** binding, a shadowing `let`, a **block-local `use` import** (simple, `as`-aliased, or grouped; an unnameable glob `use ...::*;` suppresses conservatively), or a **block-local `fn NAME(...)` item** — is treated as the local/import/nested-fn, not a same-named free helper. `#[workflow(fn ...)]` fn-pointer params and closures are exactly the forms #386 excludes. A nested `fn NAME(` **declaration** is itself never miscounted as a call — its own name token, preceded by the `fn` keyword, is skipped by the call collector (Codex r10). Suppression differs by binding kind, matching Rust scoping: **params** bind for the whole body and always suppress; a **`let`** shadow is **source-order + column-aware** — it suppresses a call on a strictly-later source line, or a **same-line** call whose column is past the binding's statement-terminating `;` (the point the binding enters scope). A call that runs *before* the binding is in scope still resolves to (and is scanned as) the free helper: `let helper = ...; helper();` suppresses the trailing call, while the RHS of `let bad = bad();` (before the `;`) and a pre-shadow `bad(); let bad = ...;` call (before the binding) both stay flagged; a **body-local `use` import** (and glob) and a **body-local `fn` item** shadow the name for the **whole body**, *not* source-order — a Rust block `use`/`fn` item is in scope for the entire enclosing block regardless of textual position, so a bare call may **precede** the `use`/`fn` and still resolve to it (Codex r7/r10). Adding a `use`/glob/`fn` to the shadow set only ever suppresses *more*, so it introduces no new false-positive. Residual safe false-negatives: a `let` shadow bound in an inner block that has already closed before an outer call still suppresses that call (source-order, not full brace-scope tracking); **block-local `use`/`fn` items are treated as whole-body shadows, not brace-scoped** — a nested-block `use`/`fn` that Rust confines to that inner block still suppresses a same-named call in a sibling or outer block *after* it (a line-based-granularity boundary; precise block-scoping would add brace-range-per-item machinery and risk reopening the ordering false-*positive* the whole-body treatment was chosen to eliminate, so it is declined in favor of the zero-false-positive whole-body rule); and if a body both shadows a name *and* legitimately calls a real free fn of that name, that call is conservatively skipped. Every case here is a false-*negative* (under-reports, never over-reports); the runtime replay net (`WorkflowReplayer` / the live `HistoryMatcher`) plus manual review is the backstop.
- **Path-qualified one-hop calls.** `self::helper()`, `crate::mod::helper()`, and `super::helper()` are **not** resolved — only bare-ident calls (`helper()`, including a turbofish `helper::<T>()`) are matched, and only against a helper in the caller's own module (see "One-hop resolution is same-module only" above). A `use`-imported or aliased call therefore never resolves, because its real target is always in another module the scanner cannot follow. Resolving qualified / associated-function (`Type::assoc()`) calls would risk associated-function/method false positives, which is exactly the #386 boundary.
- **Call-form gaps.** Space-before-paren (`helper ()`) call forms are not matched by the one-hop resolver. (Turbofish calls — `helper::<T>()`, including nested generics — *are* resolved.)

### Flag scope

`det-check` has its **own local** `--format text|json` flag and **ignores** the CLI's global network flags (`--base-url`, `--output`, auth) — it is read-only source analysis that never touches the management API, so those flags do not apply. `--format` controls only how the `DetCheckReport` (or, with `--list-suppressions`, the suppression inventory) is rendered.

### Relationship to DET010 / DET011

`det-check` surfaces the **entire** shared `det_check` engine, including **DET010** (`HashMap`/`HashSet` iteration order, [issue #785](https://github.com/autumn-foundation/autumn-harvest/issues/785)) and **DET011** (`select!` / futures-select combinators, [issue #799](https://github.com/autumn-foundation/autumn-harvest/issues/799)). The `det-check` CLI slice ([issue #778](https://github.com/autumn-foundation/autumn-harvest/issues/778)) adds **no new** `HashMap`/data-structure linting — those rules pre-exist. Listing "HashMap iteration" as out-of-scope for #778 means this slice adds no such *new* rule, **not** that the CLI hides the engine's existing DET010 output — a `HashMap`-iteration hard blocker is reported by `det-check` exactly as DET010.

### GitHub Actions

```yaml
name: determinism
on: [pull_request]
jobs:
  det-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      # Runs the CLI over the whole repo (default `.`) and fails the job on any
      # hard blocker. Build artifacts (`target`), hidden directories, and the
      # trybuild `compile_fail/` fixtures are skipped automatically.
      - name: harvest det-check
        run: cargo run -q -p autumn-harvest-cli --bin harvest -- det-check
```

The command exits non-zero on a hard blocker, so the step fails the job automatically — no extra shell plumbing needed. Add `--deny-warnings` to the run line to also fail on warnings. The argless form scans the repo root; in a larger downstream workspace where you want to scope the scan, enumerate specific crate `src` directories instead (e.g. `det-check crate-a/src crate-b/src`).

### Pre-commit hook

Drop this into `.git/hooks/pre-commit` (make it executable with `chmod +x`):

```sh
#!/bin/sh
# Block commits that introduce a workflow-determinism hard blocker.
if ! cargo run -q -p autumn-harvest-cli --bin harvest -- det-check; then
    echo "det-check found determinism hard blockers — fix them or add a" >&2
    echo "// harvest-suppress: DETxxx \"reason\" comment (see the guide)." >&2
    exit 1
fi
```

The shipped tree passes the check: a bare `harvest det-check` at the repo root reports zero hard-blocker findings (the deliberately non-deterministic trybuild fixtures under `autumn-harvest/tests/compile_fail/` are *true* positives that exist to be rejected by the compile-time guardrail — the scanner skips that directory, along with `target` and any hidden directory, automatically).

---

## Gating payload-schema changes in CI

`det-check` covers non-deterministic **code**. The sibling hazard is a
non-deterministic **payload**: renaming, retyping, or adding a required field to
a workflow's input/output/error type compiles green but silently breaks every
in-flight execution, whose recorded `harvest_events` JSON no longer deserializes.

That is gated by **`harvest schema check`**
([issue #794](https://github.com/autumn-foundation/autumn-harvest/issues/794)),
which diffs the schemas your app publishes ([issue #373](https://github.com/autumn-foundation/autumn-harvest/issues/373))
against the checked-in baseline `docs/workflow-schema-contract.json` and fails
the build on any change that would break replay:

```console
$ cargo run --quiet --bin dump-schema-contract > /tmp/current.json
$ harvest schema check --current /tmp/current.json
```

`dump-schema-contract` is a **three-line binary you add to your own crate** —
Harvest is a library, so only your process can enumerate the workflow registry.
[The schema-contract guide](workflow-schema-contract-guide.md#generating---current)
has the snippet; `autumn-harvest/examples/schema_workflow.rs --emit-contract` is
a working reference.

Exit `0` when every delta is compatible, `1` when any is breaking. A deliberate
migration is acknowledged — never suppressed — with `harvest schema update
--acknowledge "<why this is safe>"`, which records the justification in the
artifact so it lands in the same reviewable diff.

Like `det-check`, it needs no database and no services, so it belongs in the
same cheap lint job. See **[the schema-contract guide](workflow-schema-contract-guide.md)**
for the full ruleset, the generator, the escape hatch, and CI/pre-commit recipes.

---

## Composing with the release playbook

The determinism rule catalog is an early-stage guardrail, not the final proof of replay safety. The recommended release sequence:

1. **Determinism check** (this catalog): catch obvious footguns before any workflow has history.
2. **Schema-contract check** ([issue #794](https://github.com/autumn-foundation/autumn-harvest/issues/794)): catch backward-incompatible *payload* changes — the sibling hazard to non-deterministic code — before they DLQ in-flight runs. See [the guide](workflow-schema-contract-guide.md).
3. **History export** ([issue #169](https://github.com/autumn-foundation/autumn-harvest/issues/169)): export event histories from staging as replay fixtures.
4. **WorkflowReplayer** (`autumn_harvest::testing::WorkflowReplayer`): verify the new code replays all exported fixtures without divergence. Two flavours, for two different populations:
   - **Curated *completed* fixtures**, replayed strictly — `ReplayVerifier::verify_dir`. See [`replay-verify.md`](replay-verify.md).
   - **A live *in-flight* sample**, replayed frontier-tolerantly — `harvest history export-sample` + `WorkflowReplayer::replay_bundle` ([issue #798](https://github.com/autumn-foundation/autumn-harvest/issues/798)). This is the one that answers "will the executions running *right now* survive this deploy?", and it is the layer that catches a regression the curated fixtures happen not to cover. See [`replay-drift-gate.md`](replay-drift-gate.md).
5. **Patch gate** (`ctx.patched()` / `ctx.deprecate_patch()`, with `ctx.version()` as the multi-version escape hatch): fence intentional non-determinism across deploys behind `ctx.patched(id)` for the common two-state change, deprecate the gate with `ctx.deprecate_patch(id)` once pre-patch runs have drained, and delete it after the marker-bearing runs drain too. Reach for `ctx.version()` only when a gate needs more than two concurrent versions.
6. **Build-id routing** (`WorkerConfig::with_build_id`): gate new executions on the new build until compatibility is declared.

Each layer catches a different class of problem. All six together provide defence-in-depth for safe rolling deploys.

---

## Update Handlers and Determinism

Workflow Update Handlers allow external clients to interact with running workflows and receive synchronous results (issue #140). However, update handlers run asynchronously and their lifecycles are decoupled from the main workflow function.

### Orphaned Update Handlers

If a workflow completes (or fails, times out, is cancelled/terminated) while update handlers are still in progress, these updates become **orphaned**. Orphaned updates will be terminated immediately, and their clients will receive a `409 Conflict` response with an `update_orphaned` error payload.

### Waiting for Update Handlers to Complete

To prevent orphaned updates and ensure all clients receive their results, you can gate the workflow completion by waiting for all admitted update handlers to resolve using the `WorkflowContext::all_handlers_finished()` and `WorkflowContext::unfinished_update_handler_count()` helpers inside your workflow logic:

```rust
// Block workflow completion until all update handlers are resolved.
ctx.await_condition(|| ctx.all_handlers_finished()).await;
```

This ensures a clean exit and prevents unhandled update requests from being aborted.
