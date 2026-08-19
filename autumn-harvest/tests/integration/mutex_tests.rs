//! Durable mutex (`ctx.mutex`) DB integration tests — issue #691.
//!
//! These tests are the **correctness oracle** for the hand-reconstructed SQL in
//! `mutex.rs` and the worker park/grant/release + lease-renewal/reclaim wiring.
//! They drive the real [`Worker`] poll loop against a live Postgres and use a
//! **mutual-exclusion witness**: a `critical_section` activity that is dispatched
//! *only while a workflow holds the guard*, tracking per-key in-flight count and
//! max-seen. If the mutex is correct, the max concurrent critical sections per
//! key is exactly **1**.
//!
//! DB source of truth: `full_migrations_sql()` (the paved-path bundle) applied
//! to a fresh per-test database (when `HARVEST_TEST_DATABASE_URL` points at an
//! admin server) or a throwaway testcontainer (CI default). Run with
//! `--test-threads=1` — several tests set the process-global mutex lease TTL.
#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::too_many_lines,
    clippy::significant_drop_tightening
)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, SharedState, WorkflowContext};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::reset::{
    ResetSignalReapplyPolicy, WorkflowResetError, WorkflowResetRequest, preview_workflow_reset,
    reset_workflow_execution,
};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{
    ExecutionId, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::integration_e2e::{build_runtime_worker, build_test_pool, spawn_test_worker};

// ─────────────────────────────── ME witness ───────────────────────────────

/// Shared, in-process witness for mutual exclusion. All contending workflows in
/// a test run on a single worker, so the counter is authoritative.
#[derive(Default)]
struct Witness {
    /// key -> (current in-flight critical sections, max ever seen concurrently)
    per_key: std::sync::Mutex<HashMap<String, (i64, i64)>>,
    /// grant order (labels), for FIFO assertions
    order: std::sync::Mutex<Vec<String>>,
    /// total critical sections entered (any key)
    entered: AtomicI64,
    /// #1126 barrier: the losing race branch has begun executing (its
    /// `ActivityStarted` is already durably recorded before the handler body
    /// runs), so the fast winner may complete while the loser is still
    /// in-flight — reproducing the "loser in-flight at resolution" condition.
    race_loser_started: AtomicBool,
}

impl Witness {
    fn max_seen(&self, key: &str) -> i64 {
        self.per_key
            .lock()
            .unwrap()
            .get(key)
            .map_or(0, |(_, max)| *max)
    }

    fn entered(&self) -> i64 {
        // UFCS: `diesel_async::RunQueryDsl`'s blanket impl otherwise shadows the
        // inherent `AtomicI64::load` here.
        AtomicI64::load(&self.entered, Ordering::SeqCst)
    }

    fn order(&self) -> Vec<String> {
        self.order.lock().unwrap().clone()
    }

    /// UFCS: `diesel_async::RunQueryDsl`'s blanket impl shadows the inherent
    /// `AtomicBool::load`/`store` here (same reason as `entered` above).
    fn race_loser_has_started(&self) -> bool {
        AtomicBool::load(&self.race_loser_started, Ordering::SeqCst)
    }
    fn mark_race_loser_started(&self) {
        AtomicBool::store(&self.race_loser_started, true, Ordering::SeqCst);
    }
}

fn shared_state(witness: Arc<Witness>) -> SharedState {
    let mut map: HashMap<std::any::TypeId, Box<dyn std::any::Any + Send + Sync>> = HashMap::new();
    map.insert(
        std::any::TypeId::of::<Arc<Witness>>(),
        Box::new(witness) as Box<dyn std::any::Any + Send + Sync>,
    );
    Arc::new(map)
}

/// Records the `harvest.workflow.nondeterministic_block` counter (issue #603 /
/// #1126). The counter fires monotonically on every nd-block *entry* and is NOT
/// un-fired when the run later recovers and `nd_blocked_at` is cleared — so it
/// is the reliable, timing-independent signal that a TRANSIENT nd-block ever
/// occurred, which a post-hoc `nd_blocked_at IS NULL` check cannot see.
#[derive(Default)]
struct RecordingMetrics {
    nd_blocks: std::sync::Mutex<Vec<(String, String)>>,
}

impl RecordingMetrics {
    fn nd_block_count(&self) -> usize {
        self.nd_blocks.lock().unwrap().len()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_nondeterministic_block(&self, workflow_name: &str, queue: &str) {
        self.nd_blocks
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), queue.to_string()));
    }
}

// ─────────────────────────────── activities ───────────────────────────────

/// The mutual-exclusion witness activity. Dispatched only while the calling
/// workflow holds the guard. Brackets a short sleep with an increment/decrement
/// of a per-key in-flight counter, recording the max seen. If two workflows ever
/// run this concurrently for the same key, the max exceeds 1 — a real ME bug.
fn critical_section_activity<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let label = input["label"].as_str().unwrap_or("").to_string();
        let witness = ctx
            .state::<Arc<Witness>>()
            .expect("witness state must be registered")
            .clone();

        // Enter the critical section.
        {
            let mut m = witness.per_key.lock().unwrap();
            let e = m.entry(key.clone()).or_insert((0, 0));
            e.0 += 1;
            if e.0 > e.1 {
                e.1 = e.0;
            }
        }
        witness.order.lock().unwrap().push(label);
        witness.entered.fetch_add(1, Ordering::SeqCst);

        // Hold the critical section wide enough that a genuine overlap is caught.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Leave the critical section.
        {
            let mut m = witness.per_key.lock().unwrap();
            if let Some(e) = m.get_mut(&key) {
                e.0 -= 1;
            }
        }
        Ok(json!({"ok": true}))
    })
}

/// Trivial *local* activity used to exercise the early-release-across-inline-
/// re-drive path (FIX-B).
fn noop_local_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!("noop")) })
}

// ─────────────────────────────── workflows ────────────────────────────────

/// Acquire `key`, run a critical section, release; repeat `cycles` times.
fn contend_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let label = input["label"].as_str().unwrap_or("").to_string();
        let cycles = input["cycles"].as_u64().unwrap_or(1);
        for _ in 0..cycles {
            let guard = ctx
                .mutex(key.clone())
                .acquire()
                .await
                .map_err(|e| e.to_string())?;
            ctx.execute_activity_raw(
                "critical_section",
                json!({"key": key, "label": label}),
                "default",
            )
            .await
            .map_err(|e| e.to_string())?;
            guard.release();
        }
        Ok(json!("done"))
    })
}

/// Acquire `key`, park on the `release` signal while holding the lock, then
/// release on completion. Exercises "held across suspension stays held".
fn hold_wait_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
        ctx.wait_for_signal("release")
            .await
            .map_err(|e| e.to_string())?;
        guard.release();
        Ok(json!("released"))
    })
}

/// Acquire `key` and then *leak* the guard (never releases via Drop). Only the
/// terminal auto-release sweep can free the lock. Completes immediately.
fn leak_guard_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
        std::mem::forget(guard);
        Ok(json!("leaked"))
    })
}

/// Hold `key`, then re-acquire the same key — a synchronous self-deadlock.
fn self_deadlock_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let _g1 = ctx
            .mutex(key.clone())
            .acquire()
            .await
            .map_err(|e| e.to_string())?;
        match ctx.mutex(key).acquire().await {
            Ok(_) => Ok(json!("unexpected: second acquire succeeded")),
            Err(e) if e.is_mutex_self_deadlock() => Err("self_deadlock".to_string()),
            Err(e) => Err(format!("unexpected error: {e}")),
        }
    })
}

/// Co-batch an `AcquireMutex` with an activity dispatch via `join!` — the mixed
/// batch must fail loud (it is not a solo acquire).
fn co_batch_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let (guard, act) = tokio::join!(
            ctx.mutex(key.clone()).acquire(),
            ctx.execute_activity_raw(
                "critical_section",
                json!({"key": key, "label": "co"}),
                "default",
            ),
        );
        let _g = guard.map_err(|e| e.to_string())?;
        let _a = act.map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

/// FIX-B: acquire, park on `go` (holding), release, then run a *local* activity
/// (an inline re-drive), then park on `finish` — still RUNNING. The early
/// release (batched with the local-activity command) must free the lock so a
/// blocked contender proceeds while this workflow keeps running.
fn early_release_local_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
        ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        guard.release();
        ctx.execute_local_activity_raw("noop_local", json!(null), None, None)
            .await
            .map_err(|e| e.to_string())?;
        ctx.wait_for_signal("finish")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

/// Acquire, release early, then park on `finish` while RUNNING and holding no
/// lock. Used by the reset-after-release test.
fn release_then_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let guard = ctx.mutex(key).acquire().await.map_err(|e| e.to_string())?;
        guard.release();
        ctx.wait_for_signal("finish")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

// ─────────────────────────────── registry ─────────────────────────────────

fn wf(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "mutex_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
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

fn act(
    name: &'static str,
    is_local: bool,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "mutex_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ─────────────────────── #1126 race regression pieces ─────────────────────

/// The FAST (winning) race branch. Spins until the slow loser branch has begun
/// executing, then returns immediately — so the winner completes while the
/// loser is genuinely in-flight (its `ActivityStarted` recorded, no terminal),
/// reproducing issue #1126. Bounded so a bug can never hang the test.
fn race_fast_activity<'a>(
    ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let witness = ctx
            .state::<Arc<Witness>>()
            .expect("witness state must be registered")
            .clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !witness.race_loser_has_started() {
            if tokio::time::Instant::now() >= deadline {
                return Err("race_fast: loser never started (barrier timeout)".to_string());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(json!({"provider": "fast"}))
    })
}

/// The SLOW (losing) race branch. Signals that it has started (its
/// `ActivityStarted` is already durable by the time the handler body runs),
/// then sleeps long enough to remain in-flight at race resolution — it is
/// cancelled by the winner's `CancelRaceLosers` before it would finish.
fn race_slow_activity<'a>(
    ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let witness = ctx
            .state::<Arc<Witness>>()
            .expect("witness state must be registered")
            .clone();
        witness.mark_race_loser_started();
        // Long enough that the loser is durably still-in-flight (its
        // `ActivityStarted` recorded, no terminal) for the entire window in
        // which the winner completes and the follow-on decision cycle runs, so
        // the #1126 in-flight-loser condition is reliably reproduced rather than
        // racing the loser's own completion.
        tokio::time::sleep(Duration::from_secs(15)).await;
        Ok(json!({"provider": "slow"}))
    })
}

/// Trivial follow-on activity for the race→activity regression. Bumps the
/// `entered` counter so the test can assert it actually ran.
fn after_race_activity<'a>(
    ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let witness = ctx
            .state::<Arc<Witness>>()
            .expect("witness state must be registered")
            .clone();
        witness.entered.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"ok": true}))
    })
}

/// #1126: race a fast winner against a slow (in-flight) loser, then issue a
/// plain durable activity in the SAME decision cycle. Pre-fix this nd-blocked
/// on the loser's leftover `ActivityStarted`; post-fix it completes cleanly.
fn race_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("race_fast", json!({}), "default")
            .activity_raw("race_slow", json!({}), "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("after_race", json!({"winner": winner.index}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner_index": winner.index}))
    })
}

/// #1126 flagship: race a fast winner against a slow (in-flight) loser, then
/// `ctx.mutex(key).acquire()` in the SAME decision cycle + a critical section.
/// This is the exact pattern that was documented as a known limitation on
/// `ctx.mutex` (issue #691) and is now regression coverage.
fn race_then_mutex_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let key = input["key"].as_str().unwrap_or("k").to_string();
        let label = input["label"].as_str().unwrap_or("").to_string();
        let winner = ctx
            .race()
            .activity_raw("race_fast", json!({}), "default")
            .activity_raw("race_slow", json!({}), "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let guard = ctx
            .mutex(key.clone())
            .acquire()
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw(
            "critical_section",
            json!({"key": key, "label": label}),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
        guard.release();
        Ok(json!({"winner_index": winner.index}))
    })
}

/// The workflow + activity handler lists shared by every registry constructor.
fn registry_handlers() -> (Vec<WorkflowInfo>, Vec<ActivityInfo>) {
    (
        vec![
            wf("contend", contend_workflow),
            wf("hold_wait_signal", hold_wait_signal_workflow),
            wf("leak_guard", leak_guard_workflow),
            wf("self_deadlock", self_deadlock_workflow),
            wf("co_batch", co_batch_workflow),
            wf("early_release_local", early_release_local_workflow),
            wf("release_then_wait", release_then_wait_workflow),
            wf("race_then_activity", race_then_activity_workflow),
            wf("race_then_mutex", race_then_mutex_workflow),
        ],
        vec![
            act("critical_section", false, critical_section_activity),
            act("noop_local", true, noop_local_activity),
            act("race_fast", false, race_fast_activity),
            act("race_slow", false, race_slow_activity),
            act("after_race", false, after_race_activity),
        ],
    )
}

/// Full registry with every workflow/activity used by these tests + the witness.
fn full_registry(witness: Arc<Witness>) -> Arc<HandlerRegistry> {
    let (workflows, activities) = registry_handlers();
    Arc::new(HandlerRegistry::with_state(
        workflows,
        activities,
        shared_state(witness),
    ))
}

/// Like [`full_registry`] but wires a `MetricsRecorder` so a test can assert on
/// engine metrics (issue #1126 uses `record_workflow_nondeterministic_block`).
fn full_registry_with_metrics(
    witness: Arc<Witness>,
    metrics: Arc<dyn MetricsRecorder + Send + Sync>,
) -> Arc<HandlerRegistry> {
    let (workflows, activities) = registry_handlers();
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        shared_state(witness),
        telemetry,
    ))
}

// ─────────────────────────────── DB / setup ───────────────────────────────

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// Rewrite the database name in a Postgres URL (keeping any query string).
fn with_db_name(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// Create an isolated, migrated database and return its URL.
///
/// When `HARVEST_TEST_DATABASE_URL` is set it is treated as an *admin* URL: a
/// fresh per-test database is created on that server (full isolation, no leaked
/// rows between tests). Otherwise a throwaway testcontainer is started. Either
/// way the returned URL points at a freshly migrated database.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("mutex_t_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        let url = with_db_name(&admin_url, &db);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect fresh db");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migrations");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

// ─────────────────────────────── DB queries ───────────────────────────────

async fn exec_state(url: &str, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Row>(&mut conn)
        .await
        .expect("state query")
        .state
}

async fn exec_error(url: &str, exec_id: ExecutionId) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        error: Option<String>,
    }
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT error FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Row>(&mut conn)
        .await
        .expect("error query")
        .error
}

/// Whether the execution was non-terminally nd-blocked (issue #603) — set when
/// a replay divergence wedges the run. Used by the #1126 regression tests to
/// prove the follow-on command did NOT diverge.
async fn nd_blocked(url: &str, exec_id: ExecutionId) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        blocked: bool,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(
        "SELECT (nd_blocked_at IS NOT NULL) AS blocked \
         FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<Row>(&mut conn)
    .await
    .expect("nd_blocked query")
    .blocked
}

/// The current holder of `key`, if any (from `harvest_mutex_locks`).
async fn lock_holder(url: &str, key: &str) -> Option<Uuid> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        holder_exec_id: Uuid,
    }
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT holder_exec_id FROM harvest_mutex_locks WHERE lock_key = $1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<Row>(&mut conn)
        .await
        .optional_ext()
        .map(|r| r.holder_exec_id)
}

/// The FIFO-ordered waiter exec ids for `key` (from `harvest_mutex_waiters`).
async fn waiter_ids(url: &str, key: &str) -> Vec<Uuid> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        waiter_exec_id: Uuid,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(
        "SELECT waiter_exec_id FROM harvest_mutex_waiters WHERE lock_key = $1 ORDER BY id ASC",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .load::<Row>(&mut conn)
    .await
    .expect("waiter query")
    .into_iter()
    .map(|r| r.waiter_exec_id)
    .collect()
}

/// Whether the execution's history contains an event of the given `type`.
async fn history_has_event(url: &str, exec_id: ExecutionId, event_type: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_data->>'type' = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(event_type)
    .get_result::<Row>(&mut conn)
    .await
    .expect("history count")
    .n > 0
}

/// Tiny `.optional()` shim for `get_result` on a `QueryableByName` row.
trait OptionalExt<T> {
    fn optional_ext(self) -> Option<T>;
}
impl<T> OptionalExt<T> for Result<T, diesel::result::Error> {
    fn optional_ext(self) -> Option<T> {
        match self {
            Ok(v) => Some(v),
            Err(diesel::result::Error::NotFound) => None,
            Err(e) => panic!("query error: {e}"),
        }
    }
}

// ─────────────────────────────── polling ──────────────────────────────────

async fn poll_until<F, Fut>(timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "poll_until timed out after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

async fn wait_for_state(url: &str, exec_id: ExecutionId, target: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let s = exec_state(url, exec_id).await;
        if s == target {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "execution {exec_id} did not reach {target} (last state {s}) within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

// ─────────────────────────────── start helper ─────────────────────────────

/// Start a workflow via the modern start path (which also enqueues the initial
/// workflow task, so the running worker picks it up).
async fn start(url: &str, workflow_name: &str, workflow_id: &str, input: Value) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let mut conn = connect(url).await;
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");
    exec_id
}

async fn send_signal(url: &str, exec_id: ExecutionId, name: &str) {
    let mut conn = connect(url).await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, name, json!(null))
        .await
        .expect("send signal");
}

/// Run a worker for the duration of `body`, cleanly shutting it down afterwards.
async fn with_worker<F, Fut, T>(url: &str, witness: Arc<Witness>, concurrency: usize, body: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let registry = full_registry(witness);
    let worker = build_runtime_worker("mutex-worker", concurrency, concurrency, registry);
    let pool = build_test_pool(url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);
    let out = body().await;
    worker.shutdown();
    let _ = handle.await;
    out
}

/// Run a worker wired with a `RecordingMetrics` recorder for the duration of
/// `body`; returns `(body_output, metrics)` so a test can assert on engine
/// metrics (issue #1126: the nd-block counter).
async fn with_worker_metrics<F, Fut, T>(
    url: &str,
    witness: Arc<Witness>,
    concurrency: usize,
    body: F,
) -> (T, Arc<RecordingMetrics>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let metrics = Arc::new(RecordingMetrics::default());
    let registry = full_registry_with_metrics(witness, Arc::clone(&metrics) as Arc<_>);
    let worker = build_runtime_worker("mutex-worker", concurrency, concurrency, registry);
    let pool = build_test_pool(url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);
    let out = body().await;
    worker.shutdown();
    let _ = handle.await;
    (out, metrics)
}

/// Set the process-global mutex lease TTL and restore the default afterwards.
fn set_lease_ttl(secs: u64) {
    autumn_harvest::mutex::set_mutex_lease_ttl(Duration::from_secs(secs));
}
fn reset_lease_ttl() {
    autumn_harvest::mutex::set_mutex_lease_ttl(autumn_harvest::mutex::DEFAULT_MUTEX_LEASE_TTL);
}

// ═══════════════════════════════ tests ════════════════════════════════════

/// The core mutual-exclusion oracle: 8 workflows × 3 cycles contend on one key.
/// The witness max in-flight critical sections must be exactly 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_exclusion_under_contention() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());

    let key = "contend-key";
    let n = 8u32;
    let cycles = 3u64;

    let exec_ids: Vec<ExecutionId> = with_worker(&url, Arc::clone(&witness), 16, || async {
        let mut ids = Vec::new();
        for i in 0..n {
            let id = start(
                &url,
                "contend",
                &format!("me-{i}"),
                json!({"key": key, "label": format!("wf{i}"), "cycles": cycles}),
            )
            .await;
            ids.push(id);
        }
        for id in &ids {
            wait_for_state(&url, *id, "COMPLETED", Duration::from_secs(90)).await;
        }
        ids
    })
    .await;

    assert_eq!(exec_ids.len(), n as usize);
    assert_eq!(
        witness.max_seen(key),
        1,
        "MUTUAL EXCLUSION VIOLATED: {} concurrent critical sections observed for key {key}",
        witness.max_seen(key)
    );
    assert_eq!(
        witness.entered(),
        i64::from(n) * cycles as i64,
        "every workflow's every cycle must have entered the critical section exactly once"
    );
    reset_lease_ttl();
}

/// Heavier stress variant: 12 keys × 6 workflows × 5 cycles. Per-key max == 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress; run explicitly with --ignored"]
async fn mutual_exclusion_stress() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());

    let keys: Vec<String> = (0..12).map(|k| format!("stress-key-{k}")).collect();
    let per_key = 6u32;
    let cycles = 5u64;

    with_worker(&url, Arc::clone(&witness), 32, || async {
        let mut ids = Vec::new();
        for key in &keys {
            for w in 0..per_key {
                let id = start(
                    &url,
                    "contend",
                    &format!("{key}-{w}"),
                    json!({"key": key, "label": format!("{key}-{w}"), "cycles": cycles}),
                )
                .await;
                ids.push(id);
            }
        }
        for id in &ids {
            wait_for_state(&url, *id, "COMPLETED", Duration::from_secs(180)).await;
        }
    })
    .await;

    for key in &keys {
        assert_eq!(
            witness.max_seen(key),
            1,
            "MUTUAL EXCLUSION VIOLATED for key {key}: max {}",
            witness.max_seen(key)
        );
    }
    assert_eq!(
        witness.entered(),
        i64::from(keys.len() as u32) * i64::from(per_key) * cycles as i64
    );
    reset_lease_ttl();
}

/// FIFO: a holder blocks, waiters B then C enqueue in order, and on release the
/// grants happen in B, C order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifo_grant_order() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "fifo-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        // Holder acquires and parks on the signal, holding the lock.
        let holder = start(&url, "hold_wait_signal", "fifo-holder", json!({"key": key})).await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        // B enqueues as a waiter.
        let b = start(
            &url,
            "contend",
            "fifo-b",
            json!({"key": key, "label": "B", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&b.as_uuid())
        })
        .await;

        // C enqueues as a waiter (strictly after B — later BIGSERIAL id).
        let c = start(
            &url,
            "contend",
            "fifo-c",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&c.as_uuid())
        })
        .await;

        // Release the holder; grants must flow B then C.
        send_signal(&url, holder, "release").await;
        wait_for_state(&url, holder, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, b, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, c, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(
        witness.order(),
        vec!["B".to_string(), "C".to_string()],
        "grants must follow FIFO request order"
    );
    assert_eq!(witness.max_seen(key), 1);
    reset_lease_ttl();
}

/// The crux: a holder that suspends on a signal mid-hold keeps the lock; a
/// contender does NOT run its critical section until the holder releases.
/// Falsifies the timeout-guard borrow bug (releasing the guard during the
/// holder's first suspension).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_across_suspension_stays_held() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "suspend-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let holder = start(&url, "hold_wait_signal", "sus-holder", json!({"key": key})).await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        let contender = start(
            &url,
            "contend",
            "sus-contender",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        // The contender must enqueue as a waiter behind the (suspended) holder.
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&contender.as_uuid())
        })
        .await;

        // While the holder is parked on its signal, the lock stays held and the
        // contender never enters its critical section.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            lock_holder(&url, key).await,
            Some(holder.as_uuid()),
            "lock must remain held by the suspended holder"
        );
        assert_ne!(
            exec_state(&url, contender).await,
            "COMPLETED",
            "contender must not proceed while the holder holds the lock across suspension"
        );
        assert_eq!(
            witness.entered(),
            0,
            "no critical section may run while the holder holds the lock"
        );

        // Release the holder → the contender is granted and runs.
        send_signal(&url, holder, "release").await;
        wait_for_state(&url, holder, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, contender, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    assert_eq!(witness.max_seen(key), 1);
    reset_lease_ttl();
}

/// A workflow that leaks its guard (never releases via Drop) still frees the
/// lock via the terminal auto-release sweep, so a later contender proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_auto_release_on_completion() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "terminal-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let leaker = start(&url, "leak_guard", "leaker", json!({"key": key})).await;
        wait_for_state(&url, leaker, "COMPLETED", Duration::from_secs(30)).await;
        assert!(
            history_has_event(&url, leaker, "MutexGranted").await,
            "the leaker must have actually acquired the lock (recorded MutexGranted)"
        );

        // The terminal sweep must have released the leaked lock.
        poll_until(Duration::from_secs(10), || async {
            lock_holder(&url, key).await.is_none()
        })
        .await;

        // A contender can now acquire and complete.
        let contender = start(
            &url,
            "contend",
            "term-contender",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        wait_for_state(&url, contender, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    reset_lease_ttl();
}

/// Cancelling a holder frees the lock so a waiter proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_holder_releases() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "cancel-holder-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let holder = start(
            &url,
            "hold_wait_signal",
            "cancel-holder",
            json!({"key": key}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        let waiter = start(
            &url,
            "contend",
            "cancel-waiter",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&waiter.as_uuid())
        })
        .await;

        // Cancel the holder → sweep releases → waiter granted.
        let mut conn = connect(&url).await;
        autumn_harvest::cancel_workflow_execution(&mut conn, holder, "test cancel", &NoOpMetrics)
            .await
            .expect("cancel holder");

        wait_for_state(&url, holder, "CANCELLED", Duration::from_secs(30)).await;
        wait_for_state(&url, waiter, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    assert_eq!(witness.max_seen(key), 1);
    reset_lease_ttl();
}

/// Cancelling a *waiter* removes its waiter row and FIFO advances to the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_waiter_cleanup() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "cancel-waiter-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let holder = start(&url, "hold_wait_signal", "cw-holder", json!({"key": key})).await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        let b = start(
            &url,
            "contend",
            "cw-b",
            json!({"key": key, "label": "B", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&b.as_uuid())
        })
        .await;

        let c = start(
            &url,
            "contend",
            "cw-c",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&c.as_uuid())
        })
        .await;

        // Cancel the head waiter B.
        let mut conn = connect(&url).await;
        autumn_harvest::cancel_workflow_execution(&mut conn, b, "cancel waiter", &NoOpMetrics)
            .await
            .expect("cancel waiter b");
        wait_for_state(&url, b, "CANCELLED", Duration::from_secs(30)).await;

        // B's waiter row is gone; C remains queued.
        poll_until(Duration::from_secs(10), || async {
            let waiters = waiter_ids(&url, key).await;
            !waiters.contains(&b.as_uuid()) && waiters.contains(&c.as_uuid())
        })
        .await;

        // Release the holder → C (now head) is granted and completes.
        send_signal(&url, holder, "release").await;
        wait_for_state(&url, holder, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, c, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(
        witness.order(),
        vec!["C".to_string()],
        "only C should have run its critical section (B was cancelled)"
    );
    reset_lease_ttl();
}

/// A crashed holder's expired lease is reclaimed by the worker's own timeout
/// scanner and the next waiter is granted (observable progress).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holder_crash_lease_reclaim() {
    set_lease_ttl(2);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "crash-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        // Holder acquires and parks forever (never signalled) → stops renewing.
        let holder = start(
            &url,
            "hold_wait_signal",
            "crash-holder",
            json!({"key": key}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        let waiter = start(
            &url,
            "contend",
            "crash-waiter",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;

        // The worker's enforce_timeouts_once reclaims the expired lease and
        // grants the waiter — observable as the waiter completing.
        wait_for_state(&url, waiter, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    assert_eq!(witness.max_seen(key), 1);
    reset_lease_ttl();
}

/// A PAUSED holder's lease is NOT reclaimed even past its TTL; on resume the
/// lease refreshes and no concurrent critical section is ever admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_holder_lease_not_reclaimed() {
    set_lease_ttl(2);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "paused-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let holder = start(
            &url,
            "hold_wait_signal",
            "paused-holder",
            json!({"key": key}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        // Pause the holder.
        {
            let mut conn = connect(&url).await;
            autumn_harvest::execution::pause_workflow_execution(
                &mut conn,
                holder,
                Some("incident"),
                "operator",
                &NoOpMetrics,
            )
            .await
            .expect("pause holder");
        }

        let waiter = start(
            &url,
            "contend",
            "paused-waiter",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&waiter.as_uuid())
        })
        .await;

        // Wait well past the (2s) TTL so the scanner runs repeatedly.
        tokio::time::sleep(Duration::from_secs(4)).await;

        // The paused holder's lock must NOT have been reclaimed, and the waiter
        // must still be blocked.
        assert_eq!(
            lock_holder(&url, key).await,
            Some(holder.as_uuid()),
            "a PAUSED holder's expired lease must NOT be reclaimed"
        );
        assert_ne!(
            exec_state(&url, waiter).await,
            "COMPLETED",
            "the waiter must remain blocked while the paused holder still holds the lock"
        );
        assert_eq!(witness.entered(), 0, "no critical section ran while paused");

        // Resume (refreshes the lease), then release → waiter proceeds.
        {
            let mut conn = connect(&url).await;
            autumn_harvest::execution::resume_workflow_execution(
                &mut conn,
                holder,
                "operator",
                &NoOpMetrics,
            )
            .await
            .expect("resume holder");
        }
        wait_for_state(&url, holder, "RUNNING", Duration::from_secs(15)).await;
        send_signal(&url, holder, "release").await;
        wait_for_state(&url, holder, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, waiter, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    assert_eq!(
        witness.max_seen(key),
        1,
        "no concurrent critical section may ever be admitted"
    );
    reset_lease_ttl();
}

/// Re-acquiring a held key returns the typed self-deadlock error synchronously
/// (the workflow surfaces it and fails with a clear message, no hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_deadlock_returns_typed_error() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "self-deadlock-key";

    let exec = with_worker(&url, Arc::clone(&witness), 4, || async {
        let id = start(&url, "self_deadlock", "sd", json!({"key": key})).await;
        wait_for_state(&url, id, "FAILED", Duration::from_secs(30)).await;
        id
    })
    .await;

    let err = exec_error(&url, exec).await.unwrap_or_default();
    assert!(
        err.contains("self_deadlock"),
        "self-deadlock must surface as a typed error, got: {err}"
    );
    reset_lease_ttl();
}

/// A guard drop at the end of a critical section grants the next waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_on_guard_drop_grants_next() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "drop-grant-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        let a = start(
            &url,
            "contend",
            "drop-a",
            json!({"key": key, "label": "A", "cycles": 1}),
        )
        .await;
        let b = start(
            &url,
            "contend",
            "drop-b",
            json!({"key": key, "label": "B", "cycles": 1}),
        )
        .await;
        wait_for_state(&url, a, "COMPLETED", Duration::from_secs(30)).await;
        wait_for_state(&url, b, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(
        witness.entered(),
        2,
        "both workflows must run their section"
    );
    assert_eq!(witness.max_seen(key), 1, "but never concurrently");
    reset_lease_ttl();
}

/// A `join!(acquire, activity)` mixed batch must fail loud (unsupported command
/// set), not silently mis-handle the acquire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn co_batch_acquire_fails_loud() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());

    let exec = with_worker(&url, Arc::clone(&witness), 4, || async {
        let id = start(&url, "co_batch", "cobatch", json!({"key": "cobatch-key"})).await;
        wait_for_state(&url, id, "FAILED", Duration::from_secs(30)).await;
        id
    })
    .await;

    let err = exec_error(&url, exec).await.unwrap_or_default();
    assert!(
        err.contains("unsupported commands") || err.contains("AcquireMutex"),
        "a co-batched acquire+activity must fail loud, got: {err}"
    );
    reset_lease_ttl();
}

/// FIX-B: a holder that releases early and then keeps running across an inline
/// local-activity re-drive frees the lock for a blocked contender.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_release_before_terminal_grants_contender() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "early-release-key";

    with_worker(&url, Arc::clone(&witness), 8, || async {
        // Early holder acquires and parks on "go" (holding the lock).
        let holder = start(
            &url,
            "early_release_local",
            "early-holder",
            json!({"key": key}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;

        // Contender enqueues as a waiter behind the holder.
        let contender = start(
            &url,
            "contend",
            "early-contender",
            json!({"key": key, "label": "C", "cycles": 1}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            waiter_ids(&url, key).await.contains(&contender.as_uuid())
        })
        .await;

        // Signal "go" → holder releases early (batched with a local activity)
        // and then parks on "finish" (still RUNNING). The early release must
        // wake the contender.
        send_signal(&url, holder, "go").await;

        // The contender must proceed to COMPLETED while the holder is still
        // RUNNING (not terminal) — proving the release happened at the early
        // point, not via a terminal sweep.
        wait_for_state(&url, contender, "COMPLETED", Duration::from_secs(30)).await;
        assert_ne!(
            exec_state(&url, holder).await,
            "COMPLETED",
            "the early holder must still be running (parked on 'finish') after the contender completes"
        );

        // Finish the holder.
        send_signal(&url, holder, "finish").await;
        wait_for_state(&url, holder, "COMPLETED", Duration::from_secs(30)).await;
    })
    .await;

    assert_eq!(witness.entered(), 1);
    assert_eq!(witness.max_seen(key), 1);
    reset_lease_ttl();
}

/// Resetting a workflow that holds a mutex is rejected with `HolderHoldsMutex`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_rejected_while_holder_holds_mutex() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "reset-reject-key";

    let holder = with_worker(&url, Arc::clone(&witness), 8, || async {
        let holder = start(
            &url,
            "hold_wait_signal",
            "reset-holder",
            json!({"key": key}),
        )
        .await;
        poll_until(Duration::from_secs(15), || async {
            lock_holder(&url, key).await == Some(holder.as_uuid())
        })
        .await;
        holder
    })
    .await;

    let mut conn = connect(&url).await;
    let result = preview_workflow_reset(
        &mut conn,
        holder,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "test reset".into(),
            operator_id: "op".into(),
            signal_reapply: ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
    )
    .await;

    match result {
        Err(WorkflowResetError::HolderHoldsMutex { exec_id, key: k }) => {
            assert_eq!(exec_id, holder);
            assert_eq!(k, key);
        }
        other => panic!("expected HolderHoldsMutex, got {other:?}"),
    }
    reset_lease_ttl();
}

/// Once the mutex is released, resetting the (still RUNNING) source succeeds —
/// the mutex guard no longer blocks the reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_after_release_succeeds() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());
    let key = "reset-after-key";

    let source = with_worker(&url, Arc::clone(&witness), 8, || async {
        // Acquire, release early, park on "finish" — RUNNING, holding no lock.
        // Wait for a MutexGranted (proves it acquired) AND a freed lock (proves
        // it released) — `lock_holder.is_none()` alone is also true *before* the
        // workflow ever acquires.
        let src = start(&url, "release_then_wait", "reset-src", json!({"key": key})).await;
        poll_until(Duration::from_secs(15), || async {
            history_has_event(&url, src, "MutexGranted").await
                && lock_holder(&url, key).await.is_none()
                && exec_state(&url, src).await == "RUNNING"
        })
        .await;
        src
    })
    .await;

    // Preview must no longer be blocked by a mutex guard.
    {
        let mut conn = connect(&url).await;
        let preview = preview_workflow_reset(
            &mut conn,
            source,
            WorkflowResetRequest {
                reset_to_event_id: Some(0),
                reset_point: None,
                reason: "preview".into(),
                operator_id: "op".into(),
                signal_reapply: ResetSignalReapplyPolicy::default(),
                allow_terminal_source: false,
                refuse_erased_source: false,
            },
        )
        .await;
        assert!(
            !matches!(preview, Err(WorkflowResetError::HolderHoldsMutex { .. })),
            "after release, reset must not be blocked by a held mutex: {preview:?}"
        );
    }

    // And the full reset succeeds.
    let mut conn = connect(&url).await;
    let result = reset_workflow_execution(
        &mut conn,
        source,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "reset after release".into(),
            operator_id: "op".into(),
            signal_reapply: ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        None,
    )
    .await;
    assert!(
        result.is_ok(),
        "reset after mutex release must succeed: {result:?}"
    );
    reset_lease_ttl();
}

// ─────────────────────────── #1126 regression tests ───────────────────────

/// Issue #1126: a plain durable activity issued in the SAME decision cycle
/// immediately after `ctx.race()` resolves — with a losing branch still
/// in-flight — must NOT nd-block.
///
/// The deterministic oracle is the `harvest.workflow.nondeterministic_block`
/// counter (issue #603), captured by a `RecordingMetrics` recorder. Pre-fix,
/// the follow-on `match_activity` diverges on the loser's leftover
/// `ActivityStarted`, so the run enters an nd-block (counter fires ≥ 1) that
/// only clears once the loser eventually finishes — a `nd_blocked_at IS NULL`
/// check at COMPLETED time cannot see that transient block, but the monotonic
/// counter can. Post-fix the follow-on lands cleanly and the counter stays 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn race_followon_activity_does_not_nd_block() {
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());

    let (exec_id, metrics) = with_worker_metrics(&url, Arc::clone(&witness), 8, || async {
        let id = start(&url, "race_then_activity", "race-act-1", json!({})).await;
        wait_for_state(&url, id, "COMPLETED", Duration::from_secs(90)).await;
        id
    })
    .await;

    assert_eq!(
        metrics.nd_block_count(),
        0,
        "race->activity in one cycle must not nd-block (issue #1126); \
         the follow-on command diverged on the loser's leftover ActivityStarted"
    );
    assert!(
        !nd_blocked(&url, exec_id).await,
        "the completed run must not be left nd-blocked"
    );
    // The fast branch (index 0) won and the follow-on activity ran.
    assert!(
        witness.entered() >= 1,
        "the follow-on after_race activity must have executed"
    );
}

/// Issue #1126 flagship: `ctx.mutex(key).acquire()` issued in the SAME decision
/// cycle immediately after `ctx.race()` resolves (loser in-flight) reaches
/// COMPLETED without ever nd-blocking. This flips the `ctx.race()` interop
/// documented on `ctx.mutex` (issue #691 / PR #1122) from a known limitation to
/// regression coverage. Uses the same monotonic nd-block-counter oracle as the
/// activity case, plus asserts the mutex ran its critical section exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn race_followon_mutex_acquire_does_not_nd_block() {
    set_lease_ttl(60);
    let (url, _c) = setup().await;
    let witness = Arc::new(Witness::default());

    let key = "race-mutex-key";
    let (exec_id, metrics) = with_worker_metrics(&url, Arc::clone(&witness), 8, || async {
        let id = start(
            &url,
            "race_then_mutex",
            "race-mutex-1",
            json!({"key": key, "label": "wf"}),
        )
        .await;
        wait_for_state(&url, id, "COMPLETED", Duration::from_secs(90)).await;
        id
    })
    .await;

    assert_eq!(
        metrics.nd_block_count(),
        0,
        "race->mutex.acquire in one cycle must not nd-block \
         (issue #1126, flips the #691 known limitation)"
    );
    assert!(
        !nd_blocked(&url, exec_id).await,
        "the completed run must not be left nd-blocked"
    );
    assert_eq!(
        witness.max_seen(key),
        1,
        "the mutex critical section must have run exactly once for key {key}"
    );
    // The lock must be released on completion (no dangling holder).
    assert!(
        lock_holder(&url, key).await.is_none(),
        "mutex must be released after the workflow completes"
    );
    reset_lease_ttl();
}
