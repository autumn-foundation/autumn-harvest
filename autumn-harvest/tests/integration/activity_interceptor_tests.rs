#![cfg(feature = "db")]
//! Activity execution interceptor behavioural tests — issue #680.
//!
//! Drives interceptors end-to-end against a real Postgres instance + worker poll
//! loop (harness modelled on `panic_containment_tests.rs`), proving the on-worker
//! contract:
//!
//! - An interceptor wraps a **regular** activity: it observes the invocation
//!   exactly once, its result transform is what gets *recorded* in the
//!   `ActivityCompleted` event and returned to the workflow.
//! - On **replay** of a completed activity the interceptor is NOT re-invoked
//!   (the executor reads recorded events; handlers/interceptors never run in
//!   replay). Two sequential activities ⇒ exactly two interceptor invocations,
//!   never inflated by the intervening replay cycles.
//! - The same interceptor wraps a **local** (inline) activity, observing
//!   `invocation.is_local() == true`.
//! - A **panicking** interceptor is contained exactly like a handler panic
//!   (issue #782): terminal `ActivityFailed` with `error_type = "HandlerPanic"`,
//!   the worker process survives, no poison-pill / DLQ row.
//! - An interceptor returning `Err` **participates in the normal retry path**
//!   with no special-casing — retried per the activity's `RetryPolicy`, then a
//!   terminal `ActivityFailed` carrying the interceptor's error, identical to a
//!   handler error (regular activity failures write no DLQ row by design).
//! - A **zero-interceptor** control run (regular + local activity) completes
//!   normally — the default path is unchanged.
//! - **Registration order**: first-registered interceptor is the OUTERMOST
//!   wrapper, end-to-end through a shared recorder.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! `INIT_SQL`. Each test uses a unique `ExecutionId`, a **unique task queue**
//! (its worker polls only that queue, and its workflow + activity tasks all land
//! on it — activities are dispatched onto `ctx.queue_name()`, never a shared
//! `"default"`), and its own per-registry recorder captured by the interceptor
//! instance. That makes the suite robust to a shared `HARVEST_TEST_DATABASE_URL`
//! run in parallel: no worker can cross-claim another test's tasks (FIX 5 / S3),
//! and no process-global state or cross-test scrubbing is required.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{ERROR_TYPE_CIRCUIT_OPEN, ERROR_TYPE_HANDLER_PANIC};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::interceptor::{
    ActivityInterceptor, ActivityInterceptorFuture, ActivityInterceptorNext, ActivityInvocation,
};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::policy::CircuitBreakerPolicy;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers (mirrors
// panic_containment_tests.rs so this suite EXECUTES against a local cluster when
// HARVEST_TEST_DATABASE_URL is set).
// ---------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #747: per-execution legal hold columns (renamed migration; inlined
    // verbatim so the schema is self-contained and immune to a directory rename
    // — see the same note in panic_containment_tests.rs).
    "\n",
    "ALTER TABLE harvest_workflow_executions
        ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL,
        ADD COLUMN IF NOT EXISTS legal_hold_until  TIMESTAMPTZ NULL,
        ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL,
        ADD COLUMN IF NOT EXISTS legal_hold_actor  TEXT NULL;
    CREATE INDEX IF NOT EXISTS idx_harvest_executions_legal_hold
        ON harvest_workflow_executions (legal_hold_set_at)
        WHERE legal_hold_set_at IS NOT NULL;",
    "\n",
    // issue #701: continue-as-new run-chain back-links.
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
);

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Interceptors (per-registry state captured by the instance — no globals).
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Records `(name, is_local)` for each invocation and wraps the Ok result as
/// `{"intercepted": <result>}`. Errors pass through unchanged.
struct CountingTransformInterceptor {
    seen: Arc<Mutex<Vec<(String, bool)>>>,
}

impl ActivityInterceptor for CountingTransformInterceptor {
    fn intercept<'a>(
        &'a self,
        invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        let seen = self.seen.clone();
        let name = invocation.name().to_string();
        let is_local = invocation.is_local();
        Box::pin(async move {
            seen.lock().unwrap().push((name, is_local));
            let out = next.run(input).await?;
            Ok(serde_json::json!({ "intercepted": out }))
        })
    }
}

/// Interceptor that always panics before calling `next`.
struct PanicInterceptor;
impl ActivityInterceptor for PanicInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        _input: serde_json::Value,
        _next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        Box::pin(async move { panic!("interceptor boom") })
    }
}

/// Interceptor that always returns Err (counting attempts), never calling next.
///
/// Uses a `Mutex<usize>` rather than `AtomicUsize` deliberately: `diesel_async`'s
/// `RunQueryDsl::load` has a blanket impl on `Arc<T>` that shadows
/// `AtomicUsize::load` at method resolution, so `attempts.load(..)` would resolve
/// to the diesel query method in this file's import scope.
struct ErrorInterceptor {
    attempts: Arc<Mutex<usize>>,
}
impl ActivityInterceptor for ErrorInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        _input: serde_json::Value,
        _next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        let attempts = self.attempts.clone();
        Box::pin(async move {
            *attempts.lock().unwrap() += 1;
            Err("interceptor error boom".to_string())
        })
    }
}

/// Records `{tag}-before` / `{tag}-after` around `next` into a shared log.
struct OrderInterceptor {
    tag: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}
impl ActivityInterceptor for OrderInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        let log = self.log.clone();
        let tag = self.tag;
        Box::pin(async move {
            log.lock().unwrap().push(format!("{tag}-before"));
            let out = next.run(input).await;
            log.lock().unwrap().push(format!("{tag}-after"));
            out
        })
    }
}

/// Reads a named context header (issue #481) from the live `ActivityContext`
/// and records the observed value, proving header propagation reaches the
/// interceptor end-to-end. Passes input/result through unchanged.
struct HeaderReadingInterceptor {
    header_key: &'static str,
    observed: Arc<Mutex<Option<String>>>,
}
impl ActivityInterceptor for HeaderReadingInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        ctx: &'a autumn_harvest::ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        let observed = self.observed.clone();
        let value = ctx.header(self.header_key).map(str::to_string);
        Box::pin(async move {
            *observed.lock().unwrap() = value;
            next.run(input).await
        })
    }
}

/// Injects `{"injected_by_interceptor": true}` into the (object) input BEFORE
/// calling `next`, so a downstream echoing handler genuinely computes over the
/// transformed input (issue #680 AC2, input-transform end-to-end).
struct InputInjectInterceptor;
impl ActivityInterceptor for InputInjectInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        mut input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        Box::pin(async move {
            if let Some(obj) = input.as_object_mut() {
                obj.insert(
                    "injected_by_interceptor".to_string(),
                    serde_json::json!(true),
                );
            }
            next.run(input).await
        })
    }
}

/// Maps the chain's `Ok` result to `Err` AFTER calling `next` (issue #680 Codex
/// P2). For a transactional activity the inner handler has already sealed its
/// `ActivityCompleted` + task-COMPLETED atomically before `next.run` returns, so
/// this error transform is silently discarded from history — the point of the
/// consistency fix is that metrics / circuit-breaker must reflect the committed
/// (sealed) outcome, NOT this discarded post-`next` `Err`.
struct OkToErrInterceptor;
impl ActivityInterceptor for OkToErrInterceptor {
    fn intercept<'a>(
        &'a self,
        _invocation: &'a ActivityInvocation<'a>,
        _ctx: &'a autumn_harvest::ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        Box::pin(async move {
            // If the handler committed Ok, discard the committed value and map to
            // Err (a `?` propagates a genuine handler Err unchanged).
            let _committed = next.run(input).await?;
            Err("interceptor mapped a committed Ok to Err".to_string())
        })
    }
}

// ---------------------------------------------------------------------------
// Handlers (benign — interceptors carry the behaviour under test).
// ---------------------------------------------------------------------------

/// Regular activity: echoes its input. Also records "handler" into an optional
/// shared log for the ordering test.
fn echo_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Transactional activity: seals `ActivityCompleted { output: input }` + task
/// COMPLETED atomically via `ctx.run_transactional`, then returns the committed
/// value (issue #680 Codex P2 fixture). A benign `SELECT 1` proves the
/// transaction's connection is usable; the closure returning `Ok(input)` records
/// `input` as the committed output. Because the seal happens inside the user's
/// transaction — BEFORE any outer interceptor regains control from `next.run` —
/// an outer interceptor cannot alter the recorded outcome.
fn txn_commit_activity(
    ctx: &autumn_harvest::ActivityContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.run_transactional(|conn| {
            Box::pin(async move {
                diesel::sql_query("SELECT 1")
                    .execute(conn)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(input)
            })
        })
        .await
    })
}

/// Workflow: run one transactional activity (`txn_commit`), return its result.
fn wf_one_txn_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("txn_commit", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Second regular activity: echoes its input.
fn echo2_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Local activity: echoes its input.
fn echo_local(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Workflow: run one regular activity, return its (interceptor-transformed) result.
///
/// Activities are dispatched onto the workflow's own task queue
/// (`ctx.queue_name()`) rather than a hardcoded `"default"`, so a per-test
/// unique queue (FIX 5 / S3) isolates every task this run produces and the suite
/// is robust to a shared `HARVEST_TEST_DATABASE_URL` run in parallel.
fn wf_one_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("echo", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow: run two regular activities sequentially, return the second result.
fn wf_two_activities(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let a = ctx
            .execute_activity_raw("echo", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?;
        let b = ctx
            .execute_activity_raw("echo2", a, &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(b)
    })
}

/// Workflow: run one local activity, return its (interceptor-transformed) result.
fn wf_one_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw(
            "echo_local",
            input,
            Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
            Some(5),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

/// Workflow: run a regular then a local activity (control for zero-interceptor).
fn wf_regular_and_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let r = ctx
            .execute_activity_raw("echo", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?;
        let l = ctx
            .execute_local_activity_raw(
                "echo_local",
                r,
                Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
                Some(5),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(l)
    })
}

// ---------------------------------------------------------------------------
// Registry / worker construction.
// ---------------------------------------------------------------------------

fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    interceptors: Vec<Arc<dyn ActivityInterceptor>>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::default());
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_activity_interceptors(interceptors),
    )
}

/// Like [`build_registry`] but wires a capturing [`MetricsRecorder`] so a test
/// can assert exactly which activity outcome the worker recorded (issue #680
/// Codex P2 — proving metrics reflect the committed transactional outcome).
fn build_registry_with_telemetry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    interceptors: Vec<Arc<dyn ActivityInterceptor>>,
    metrics: Arc<dyn MetricsRecorder>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_activity_interceptors(interceptors),
    )
}

/// A capturing [`MetricsRecorder`] recording only the calls the transactional
/// consistency tests assert on. `completions` is `(activity_name, status_str)`.
#[derive(Default)]
struct RecordingMetrics {
    completions: Mutex<Vec<(String, String)>>,
    failed: Mutex<Vec<String>>,
    tripped: Mutex<Vec<String>>,
    retried: Mutex<Vec<String>>,
}
impl MetricsRecorder for RecordingMetrics {
    fn record_activity_completed_with_error_type(
        &self,
        activity_name: &str,
        _queue: &str,
        _duration_secs: f64,
        status: ActivityStatus,
        _error_type: Option<&str>,
    ) {
        self.completions
            .lock()
            .unwrap()
            .push((activity_name.to_string(), status.as_str().to_string()));
    }
    fn record_activity_failed(
        &self,
        activity_name: &str,
        _workflow_type: &str,
        _error_type: &str,
        _non_retryable: bool,
    ) {
        self.failed.lock().unwrap().push(activity_name.to_string());
    }
    fn record_circuit_tripped(&self, activity_name: &str) {
        self.tripped.lock().unwrap().push(activity_name.to_string());
    }
    fn record_activity_retried(&self, activity_name: &str, _queue: &str) {
        self.retried.lock().unwrap().push(activity_name.to_string());
    }
}

fn build_worker(worker_id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue: &str,
) -> ExecutionId {
    seed_workflow_with_headers(conn, workflow_name, input, queue, None).await
}

/// Like [`seed_workflow`] but attaches ambient context headers (issue #481) to
/// the execution row, so the engine propagates them into every activity task's
/// `context_headers` and an interceptor can read them via `ctx.header(..)`.
async fn seed_workflow_with_headers(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue: &str,
    context_headers: Option<serde_json::Value>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn count_dead_letters(url: &str, exec_id: ExecutionId) -> (usize, usize) {
    let mut conn = connect(url).await;
    let errors: Vec<String> = harvest_dead_letters::table
        .filter(harvest_dead_letters::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(harvest_dead_letters::error)
        .load(&mut conn)
        .await
        .expect("load dead letters");
    let poison = errors.iter().filter(|e| e.contains("PoisonPill")).count();
    (errors.len(), poison)
}

async fn load_tasks(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("reload task rows")
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

/// Drive a worker until `exec_id` reaches `want`, then shut it down cleanly.
async fn run_to_state(
    url: &str,
    pool: &DbPool,
    worker: Arc<Worker>,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let execution = wait_for_state(url, exec_id, want, timeout).await;
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    execution
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "activity_interceptor_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn act_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    is_local: bool,
    retry: Option<RetryPolicy>,
) -> ActivityInfo {
    act_info_with_breaker(name, handler, is_local, retry, None)
}

/// Like [`act_info`] but with an optional per-activity `CircuitBreakerPolicy`
/// (issue #369), used by the interceptor-vs-circuit-breaker interaction test.
fn act_info_with_breaker(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    is_local: bool,
    retry: Option<RetryPolicy>,
    circuit_breaker: Option<CircuitBreakerPolicy>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_interceptor_tests",
        default_retry_policy: retry,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

/// Extract the `output` of the (single) `ActivityCompleted` event.
fn activity_completed_output(history: &[WorkflowEvent]) -> serde_json::Value {
    history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityCompleted { output, .. } => Some(output.clone()),
            _ => None,
        })
        .expect("an ActivityCompleted event must exist")
}

fn workflow_completed_output(history: &[WorkflowEvent]) -> serde_json::Value {
    history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowCompleted { output, .. } => Some(output.clone()),
            _ => None,
        })
        .expect("a WorkflowCompleted event must exist")
}

// ---------------------------------------------------------------------------
// Test 1 — interceptor wraps a regular activity; transform is recorded + seen.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_wraps_regular_activity_and_records_transformed_value() {
    let (url, _container) = setup_db().await;
    let queue = "q-wrap-regular";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let interceptor = Arc::new(CountingTransformInterceptor { seen: seen.clone() });
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None)],
        vec![interceptor],
    );
    let worker = build_worker("worker-wrap-regular", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    // (a) interceptor invoked exactly once, on a regular (non-local) activity.
    assert_eq!(
        *seen.lock().unwrap(),
        vec![("echo".to_string(), false)],
        "interceptor must run exactly once for the single regular activity"
    );

    let history = load_history(&url, exec_id).await;
    // (b) the RECORDED ActivityCompleted output is the interceptor's transform.
    assert_eq!(
        activity_completed_output(&history),
        serde_json::json!({ "intercepted": { "v": 1 } }),
        "the recorded activity result must be the interceptor-transformed value"
    );
    // (c) the workflow observed the transformed value (returned it).
    assert_eq!(
        workflow_completed_output(&history),
        serde_json::json!({ "intercepted": { "v": 1 } }),
        "the workflow must see the interceptor-transformed activity result"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — interceptor is NOT re-invoked when a completed activity is replayed.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_not_reinvoked_on_replay() {
    let (url, _container) = setup_db().await;
    let queue = "q-no-replay";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_two_activities",
        serde_json::json!({"v": 2}),
        queue,
    )
    .await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let interceptor = Arc::new(CountingTransformInterceptor { seen: seen.clone() });
    let registry = build_registry(
        vec![wf_info("wf_two_activities", wf_two_activities)],
        vec![
            act_info("echo", echo_activity, false, None),
            act_info("echo2", echo2_activity, false, None),
        ],
        vec![interceptor],
    );
    let worker = build_worker("worker-no-replay-reinvoke", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    // The workflow runs TWO activities. The second workflow-task cycle replays
    // the FIRST activity's ActivityCompleted from history (a replay of a
    // completed activity) before dispatching the second. If interceptors fired
    // on replay, `echo` would appear more than once. Exactly one invocation per
    // distinct live activity execution ⇒ [echo, echo2], length 2.
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![("echo".to_string(), false), ("echo2".to_string(), false)],
        "interceptor must fire exactly once per LIVE activity execution and never on replay"
    );

    // Independently EVIDENCE the intervening replay from the durable, append-only
    // history rather than inferring it from the coroutine model. The workflow
    // suspends after emitting each `ScheduleActivity` command and cannot emit the
    // NEXT one until it is re-driven (replayed from the top) after the prior
    // activity completes. So `ActivityScheduled(echo2)` appearing in history
    // AFTER `ActivityCompleted(echo)` proves the workflow was re-driven in a
    // second workflow-task cycle — a cycle that necessarily replayed
    // WorkflowStarted + ActivityScheduled(echo) + ActivityCompleted(echo) before
    // producing ActivityScheduled(echo2). The interceptor count staying at
    // exactly 2 (asserted above), across that proven replay of a completed
    // activity, is the property under test: replay does not re-invoke interceptors.
    let history = load_history(&url, exec_id).await;
    let echo_completed_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .expect("echo must have an ActivityCompleted event");
    let echo2_scheduled_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "echo2"))
        .expect("echo2 must have an ActivityScheduled event");
    assert!(
        echo_completed_idx < echo2_scheduled_idx,
        "echo2 must be scheduled only after echo completed, evidencing the intervening \
         replay cycle in which the interceptor was NOT re-invoked; history={history:?}"
    );

    // Structural note (AC4): the deterministic replay engine — WorkflowReplayer
    // in `src/testing.rs` — has NO way to invoke an interceptor at all. It never
    // executes activity handlers (it reads recorded ActivityCompleted/Failed
    // events; see the module doc there), and interceptors live exclusively on the
    // worker dispatch path (`process_activity_task` / `run_local_activity_inline`
    // in `worker.rs`), reachable only through a HandlerRegistry the replayer never
    // constructs. A pure-replayer fixture therefore cannot even register an
    // interceptor to count zero invocations — the property is guaranteed by
    // construction, and this DB test additionally proves a real worker replay
    // cycle does not re-invoke one.
}

// ---------------------------------------------------------------------------
// Test 3 — interceptor wraps a LOCAL activity (is_local == true).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_wraps_local_activity() {
    let (url, _container) = setup_db().await;
    let queue = "q-wrap-local";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_local",
        serde_json::json!({"v": 3}),
        queue,
    )
    .await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let interceptor = Arc::new(CountingTransformInterceptor { seen: seen.clone() });
    let registry = build_registry(
        vec![wf_info("wf_one_local", wf_one_local)],
        vec![act_info("echo_local", echo_local, true, None)],
        vec![interceptor],
    );
    let worker = build_worker("worker-wrap-local", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![("echo_local".to_string(), true)],
        "the interceptor must wrap the local activity and observe is_local() == true"
    );

    let history = load_history(&url, exec_id).await;
    let local_output = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::LocalActivityCompleted { output, .. } => Some(output.clone()),
            _ => None,
        })
        .expect("a LocalActivityCompleted event must exist");
    assert_eq!(
        local_output,
        serde_json::json!({ "intercepted": { "v": 3 } }),
        "the recorded local activity result must be the interceptor-transformed value"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — a panicking interceptor is contained like a handler panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_interceptor_becomes_activity_failed_and_worker_survives() {
    let (url, _container) = setup_db().await;
    let queue = "q-panic-int";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_panic_int",
        serde_json::json!({"v": 4}),
        queue,
    )
    .await;

    let registry = build_registry(
        vec![wf_info("wf_panic_int", wf_one_activity)],
        // 2 attempts: interceptor panics both times → terminal on attempt 2.
        vec![act_info(
            "echo",
            echo_activity,
            false,
            Some(RetryPolicy::fixed(2, Duration::from_millis(50))),
        )],
        vec![Arc::new(PanicInterceptor)],
    );
    let worker = build_worker("worker-panic-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    // Reaching FAILED at all proves the worker process survived the panic.
    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(execution.state, "FAILED");

    let history = load_history(&url, exec_id).await;
    let activity_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityFailed {
                error,
                error_type,
                attempt,
                ..
            } => Some((error.clone(), error_type.clone(), *attempt)),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_failures.len(),
        1,
        "exactly one terminal ActivityFailed; history={history:?}"
    );
    let (message, error_type, attempt) = &activity_failures[0];
    assert_eq!(
        error_type, ERROR_TYPE_HANDLER_PANIC,
        "an interceptor panic must be contained on the HandlerPanic path"
    );
    assert!(
        message.contains("interceptor boom"),
        "the interceptor panic message must surface, got {message:?}"
    );
    assert_eq!(*attempt, 2, "terminal on the 2nd (final) attempt");

    // Contained panic ⇒ no poison-pill / DLQ row, crash_strikes untouched.
    let (total_dlq, poison_dlq) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        poison_dlq, 0,
        "no PoisonPill DLQ row for a contained interceptor panic"
    );
    assert_eq!(
        total_dlq, 0,
        "no DLQ row at all for a contained activity panic"
    );
    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash_strikes must never be incremented by a contained interceptor panic"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — an interceptor Err participates in the normal retry path.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_error_participates_in_retry_then_terminal() {
    let (url, _container) = setup_db().await;
    let queue = "q-err-int";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_err_int", serde_json::json!({"v": 5}), queue).await;

    let attempts = Arc::new(Mutex::new(0usize));
    let registry = build_registry(
        vec![wf_info("wf_err_int", wf_one_activity)],
        // 3 attempts, short backoff: interceptor Err each time → terminal on 3.
        vec![act_info(
            "echo",
            echo_activity,
            false,
            Some(RetryPolicy::fixed(3, Duration::from_millis(30))),
        )],
        vec![Arc::new(ErrorInterceptor {
            attempts: attempts.clone(),
        })],
    );
    let worker = build_worker("worker-err-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(execution.state, "FAILED");

    // Retried per the activity RetryPolicy (no special-casing): the interceptor
    // was invoked once per attempt, 3 total.
    assert_eq!(
        *attempts.lock().unwrap(),
        3,
        "the interceptor Err must be retried per the activity's RetryPolicy"
    );

    let history = load_history(&url, exec_id).await;
    // Terminal ActivityFailed carrying the interceptor's error — identical to a
    // handler error. (Regular activity failures write no DLQ row by design.)
    let activity_failures: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityFailed { error, attempt, .. } => Some((error.clone(), *attempt)),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_failures.len(),
        1,
        "exactly one terminal ActivityFailed; history={history:?}"
    );
    let (message, attempt) = &activity_failures[0];
    assert!(
        message.contains("interceptor error boom"),
        "the interceptor error must surface on the terminal failure, got {message:?}"
    );
    assert_eq!(*attempt, 3, "terminal on the 3rd (final) attempt");

    let (total_dlq, _poison) = count_dead_letters(&url, exec_id).await;
    assert_eq!(
        total_dlq, 0,
        "a regular activity error writes no DLQ row, interceptor or handler alike"
    );
}

// ---------------------------------------------------------------------------
// Test 6 — zero-interceptor control: regular + local activity complete normally.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_interceptor_execution_is_unchanged() {
    let (url, _container) = setup_db().await;
    let queue = "q-zero-int";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_regular_and_local",
        serde_json::json!({"v": 6}),
        queue,
    )
    .await;

    let registry = build_registry(
        vec![wf_info("wf_regular_and_local", wf_regular_and_local)],
        vec![
            act_info("echo", echo_activity, false, None),
            act_info("echo_local", echo_local, true, None),
        ],
        // No interceptors registered — the default path.
        Vec::new(),
    );
    let worker = build_worker("worker-zero-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    // With no interceptors, the values are untransformed all the way through.
    assert_eq!(
        workflow_completed_output(&history),
        serde_json::json!({ "v": 6 }),
        "a zero-interceptor run must be byte-for-byte the default behaviour"
    );
    assert_eq!(
        activity_completed_output(&history),
        serde_json::json!({ "v": 6 }),
        "regular activity output is untransformed when no interceptor is registered"
    );
}

// ---------------------------------------------------------------------------
// Test 7 — registration order is outermost-first, end-to-end.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_order_is_outermost_first_end_to_end() {
    let (url, _container) = setup_db().await;
    let queue = "q-order";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_order", serde_json::json!({"v": 7}), queue).await;

    let log = Arc::new(Mutex::new(Vec::new()));
    // The handler just echoes (the module-level `echo_activity`). We can't inject
    // a "handler" marker through the fn pointer easily, so the ordering is
    // asserted purely on the interceptors' before/after markers, which is
    // sufficient to prove i1 is OUTERMOST (its before is first, after is last).
    let registry = build_registry(
        vec![wf_info("wf_order", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None)],
        vec![
            Arc::new(OrderInterceptor {
                tag: "i1",
                log: log.clone(),
            }),
            Arc::new(OrderInterceptor {
                tag: "i2",
                log: log.clone(),
            }),
        ],
    );
    let worker = build_worker("worker-order", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        *log.lock().unwrap(),
        vec!["i1-before", "i2-before", "i2-after", "i1-after"],
        "the first-registered interceptor (i1) must be the OUTERMOST wrapper"
    );
}

// ---------------------------------------------------------------------------
// Test 8 (FIX 2 / AC6) — an interceptor's retryable Err feeds the per-activity
// circuit breaker EXACTLY like a handler error, with no special-casing.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_error_feeds_circuit_breaker_like_a_handler_error() {
    let (url, _container) = setup_db().await;
    let queue = "q-cb-int";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_cb_int", serde_json::json!({"v": 8}), queue).await;

    // failure_threshold = 2: two retryable failures trip the breaker. A generous
    // retry budget (5) lets a THIRD dispatch attempt occur — which the now-open
    // breaker short-circuits BEFORE the interceptor chain runs, terminating the
    // activity with the typed CircuitOpen failure. The ONLY source of failures
    // feeding the breaker is the interceptor (it returns Err without calling
    // `next`, so the handler never runs), so a trip proves interceptor errors
    // flow through `handle_activity_result` -> CircuitBreaker::on_result
    // identically to a handler error.
    let attempts = Arc::new(Mutex::new(0usize));
    let registry = build_registry(
        vec![wf_info("wf_cb_int", wf_one_activity)],
        vec![act_info_with_breaker(
            "echo",
            echo_activity,
            false,
            Some(RetryPolicy::fixed(5, Duration::from_millis(30))),
            Some(CircuitBreakerPolicy::new(
                2,
                Duration::from_secs(30),
                Duration::from_secs(60),
            )),
        )],
        vec![Arc::new(ErrorInterceptor {
            attempts: attempts.clone(),
        })],
    );
    let worker = build_worker("worker-cb-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(execution.state, "FAILED");

    // The breaker trips solely from interceptor errors: threshold=2 ⇒ the
    // interceptor is invoked on exactly the first two attempts. The third
    // dispatch is short-circuited by the open breaker BEFORE the chain runs
    // (interceptors sit after the circuit-breaker admit gate), so the count is
    // never inflated to 3.
    assert_eq!(
        *attempts.lock().unwrap(),
        2,
        "interceptor runs on the two tripping attempts only; the open-circuit \
         short-circuit runs before the interceptor chain"
    );

    // Terminal failure is the typed CircuitOpen — proof the interceptor errors
    // opened the breaker, identical to a handler error opening it.
    let history = load_history(&url, exec_id).await;
    let terminal_error_type = history
        .iter()
        .rev()
        .find_map(|e| match e {
            WorkflowEvent::ActivityFailed { error_type, .. } => Some(error_type.clone()),
            _ => None,
        })
        .expect("a terminal ActivityFailed must exist");
    assert_eq!(
        terminal_error_type, ERROR_TYPE_CIRCUIT_OPEN,
        "the breaker must trip from the interceptor's retryable errors and short-circuit \
         a later dispatch with the typed CircuitOpen failure; history={history:?}"
    );

    // And the breaker is observably open on the shared registry the worker used.
    let snap = registry
        .circuit_breakers()
        .snapshot("echo", std::time::Instant::now())
        .expect("the breaker must be tracked");
    assert_eq!(
        snap.state, "open",
        "the interceptor errors must leave the breaker open"
    );
}

// ---------------------------------------------------------------------------
// Test 9 (FIX 3 / AC8) — an interceptor reads a context header (#481)
// propagated end-to-end into the activity's ActivityContext.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_reads_propagated_context_header() {
    let (url, _container) = setup_db().await;
    let queue = "q-header-int";
    let mut conn = connect(&url).await;
    // Seed the execution with an ambient context header; the engine propagates
    // it into every activity task's context_headers, so the interceptor's
    // `ctx.header(..)` observes it without any per-activity plumbing.
    let exec_id = seed_workflow_with_headers(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 9}),
        queue,
        Some(serde_json::json!({ "x-correlation-id": "corr-99" })),
    )
    .await;

    let observed = Arc::new(Mutex::new(None));
    let interceptor = Arc::new(HeaderReadingInterceptor {
        header_key: "x-correlation-id",
        observed: observed.clone(),
    });
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None)],
        vec![interceptor],
    );
    let worker = build_worker("worker-header-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        observed.lock().unwrap().as_deref(),
        Some("corr-99"),
        "the interceptor must read the propagated x-correlation-id context header \
         from the activity ctx (issue #481 propagation, end-to-end)"
    );
}

// ---------------------------------------------------------------------------
// Test 10 (FIX 6 / AC2) — an interceptor INPUT transform is genuinely seen by
// the handler through the real worker path (recorded ActivityCompleted output).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_input_transform_reaches_handler_end_to_end() {
    let (url, _container) = setup_db().await;
    let queue = "q-input-int";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 10}),
        queue,
    )
    .await;

    // The interceptor injects a field into the input BEFORE calling next; the
    // echo handler returns whatever input it received. So the recorded
    // ActivityCompleted.output reflecting the injected field proves the handler
    // genuinely computed over the interceptor-transformed input through the real
    // worker dispatch path (not merely a result transform).
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None)],
        vec![Arc::new(InputInjectInterceptor)],
    );
    let worker = build_worker("worker-input-int", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    let expected = serde_json::json!({ "v": 10, "injected_by_interceptor": true });
    assert_eq!(
        activity_completed_output(&history),
        expected,
        "the handler must echo the interceptor-transformed input, so the recorded \
         ActivityCompleted output carries the injected field"
    );
    assert_eq!(
        workflow_completed_output(&history),
        expected,
        "the workflow must observe the handler's output over the transformed input"
    );
}

// ---------------------------------------------------------------------------
// Test 11 (issue #680 Codex P2 — load-bearing) — an OUTER interceptor that maps
// a transactional activity's committed Ok → Err must NOT trip the circuit
// breaker and must NOT record the attempt as Failed. A transactional activity
// seals its own `ActivityCompleted` + task-COMPLETED atomically inside the
// user's transaction; the interceptor's error transform is discarded from
// history, so metrics + the circuit breaker must reflect the committed (sealed)
// outcome, not the discarded post-`next` Err.
//
// Pre-fix this FAILS: the worker consumed the post-interceptor `Err`, so the
// breaker tripped and metrics recorded Failed on an outcome history records as
// ActivityCompleted — a real observability/history divergence NEW to
// interceptors.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_error_transform_on_transactional_activity_does_not_trip_breaker() {
    let (url, _container) = setup_db().await;
    let queue = "q-txn-int-err";
    let mut conn = connect(&url).await;
    let value = serde_json::json!({ "v": 11, "committed": true });
    let exec_id = seed_workflow(&mut conn, "wf_one_txn_activity", value.clone(), queue).await;

    // failure_threshold = 1 (one retryable failure trips the breaker); retry
    // budget 5 (pre-fix the worker also spuriously requeues the COMPLETED task).
    let metrics = Arc::new(RecordingMetrics::default());
    let registry = build_registry_with_telemetry(
        vec![wf_info("wf_one_txn_activity", wf_one_txn_activity)],
        vec![act_info_with_breaker(
            "txn_commit",
            txn_commit_activity,
            false,
            Some(RetryPolicy::fixed(5, Duration::from_millis(30))),
            Some(CircuitBreakerPolicy::new(
                1,
                Duration::from_secs(30),
                Duration::from_secs(60),
            )),
        )],
        vec![Arc::new(OkToErrInterceptor)],
        metrics.clone(),
    );
    let worker = build_worker("worker-txn-int-err", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    // The workflow observes the committed ActivityCompleted and completes; the
    // interceptor's Err never reaches history (run_to_state asserts COMPLETED).
    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    // (a) The RECORDED history is the committed value — the transform was NOT
    // applied to history, and there is no terminal ActivityFailed.
    let history = load_history(&url, exec_id).await;
    assert_eq!(
        activity_completed_output(&history),
        value,
        "the committed ActivityCompleted output must be the transactional value V"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "the discarded interceptor Err must never be recorded as an ActivityFailed; \
         history={history:?}"
    );

    // (b) The workflow observes V.
    assert_eq!(
        workflow_completed_output(&history),
        value,
        "the workflow must observe the committed value V"
    );

    // (c) LOAD-BEARING: the breaker did NOT trip — it reflects the committed
    // (Success) outcome, not the interceptor's discarded retryable Err.
    let snap = registry
        .circuit_breakers()
        .snapshot("txn_commit", std::time::Instant::now())
        .expect("the breaker must be tracked");
    assert_eq!(
        snap.state, "closed",
        "a transactional self-commit must be treated as Success by the breaker, so it \
         stays closed even though an outer interceptor mapped the committed Ok to Err"
    );
    assert!(
        metrics.tripped.lock().unwrap().is_empty(),
        "no circuit-tripped metric for a committed transactional activity"
    );

    // (c cont.) Metrics record the attempt as Completed, never Failed.
    assert_eq!(
        *metrics.completions.lock().unwrap(),
        vec![("txn_commit".to_string(), "completed".to_string())],
        "the duration/attempt metric must record the committed Completed outcome once"
    );
    assert!(
        metrics.failed.lock().unwrap().is_empty(),
        "no per-attempt activity.failed metric for a committed outcome"
    );

    // (d) No spurious retry and no DLQ row — the sealed task is not requeued.
    assert!(
        metrics.retried.lock().unwrap().is_empty(),
        "a self-committed activity must never be requeued for retry; got {:?}",
        metrics.retried.lock().unwrap()
    );
    assert_eq!(
        count_dead_letters(&url, exec_id).await,
        (0, 0),
        "a committed transactional activity must not produce a dead-letter row"
    );
    // The single activity task is sealed COMPLETED on its first attempt — the
    // fix skips the finalize/retry path, so it is never re-pended.
    let activity_tasks: Vec<_> = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .filter(|t| t.task_type == "activity")
        .collect();
    assert_eq!(
        activity_tasks.len(),
        1,
        "one activity task for one dispatch"
    );
    assert_eq!(
        (activity_tasks[0].state.as_str(), activity_tasks[0].attempt),
        ("COMPLETED", 1),
        "the sealed task must be COMPLETED and never requeued to a second attempt"
    );
}

// ---------------------------------------------------------------------------
// Test 12 (issue #680 Codex P2) — an OUTER interceptor's Ok→Ok′ result
// transform on a transactional activity is observably IGNORED: history keeps
// the committed value V (not V′), the workflow observes V, and metrics record
// Completed. This documents that an interceptor observes but cannot transform a
// self-sealed transactional outcome; history == metrics == breaker all reflect V.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interceptor_result_transform_on_transactional_activity_is_observably_ignored() {
    let (url, _container) = setup_db().await;
    let queue = "q-txn-int-ok";
    let mut conn = connect(&url).await;
    let value = serde_json::json!({ "v": 12 });
    let exec_id = seed_workflow(&mut conn, "wf_one_txn_activity", value.clone(), queue).await;

    // CountingTransformInterceptor maps the handler's Ok(V) → Ok({"intercepted": V}).
    let seen = Arc::new(Mutex::new(Vec::new()));
    let metrics = Arc::new(RecordingMetrics::default());
    let registry = build_registry_with_telemetry(
        vec![wf_info("wf_one_txn_activity", wf_one_txn_activity)],
        vec![act_info("txn_commit", txn_commit_activity, false, None)],
        vec![Arc::new(CountingTransformInterceptor {
            seen: seen.clone(),
        })],
        metrics.clone(),
    );
    let worker = build_worker("worker-txn-int-ok", queue, Arc::clone(&registry));
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    // The recorded output is the committed V, NOT the interceptor's {"intercepted": V}.
    assert_eq!(
        activity_completed_output(&history),
        value,
        "the committed ActivityCompleted output must be V, not the interceptor's Ok′ \
         transform — a transactional activity seals its own outcome"
    );
    assert_eq!(
        workflow_completed_output(&history),
        value,
        "the workflow must observe the committed V, not the discarded Ok′ transform"
    );
    // Metrics reflect the committed Completed outcome.
    assert_eq!(
        *metrics.completions.lock().unwrap(),
        vec![("txn_commit".to_string(), "completed".to_string())],
        "metrics must record Completed for the committed transactional activity"
    );
    assert!(
        metrics.failed.lock().unwrap().is_empty(),
        "no activity.failed metric for a committed Ok outcome"
    );
    // The interceptor still ran exactly once over the regular (non-local) activity.
    assert_eq!(
        *seen.lock().unwrap(),
        vec![("txn_commit".to_string(), false)],
        "the interceptor observes the invocation once even though its transform is ignored"
    );
}
