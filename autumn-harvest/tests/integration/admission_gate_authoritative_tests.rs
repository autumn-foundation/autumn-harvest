#![cfg(feature = "db")]
#![allow(clippy::await_holding_lock)]
//! Completion-trigger admission-gate authority — issue #618, AC1.
//!
//! Drives the real `evaluate_triggers_for_execution` core against a Postgres
//! instance and asserts that a completion-trigger-initiated start is:
//!
//! - BLOCKED when a matching admission gate is active (Fleet, or a scoped
//!   gate that matches), recording the same `record_admission_blocked` outcome
//!   a direct API start records, dropping the start (no target execution) and
//!   writing an exactly-once resolved-skip fires row with
//!   `outcome = 'admission_blocked'` — never rolling back the source terminal;
//! - STARTED normally when no gate cache is installed (byte-identical to
//!   pre-#618), and when a scoped gate does NOT match.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (the process-global gate cache and shard router force
//! single-threaded execution; each test scrubs first). Otherwise a fresh
//! testcontainers Postgres is booted with the full migration bundle.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use autumn_harvest::admission_gate::{
    AdmissionGate, AdmissionGateCache, AdmissionGateId, GateScope, set_global_admission_gate_cache,
    set_global_admission_metrics,
};
use autumn_harvest::completion_trigger::{TerminalState, evaluate_triggers_for_execution};
use autumn_harvest::context::SharedStateMap;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId, UpdateId, WorkflowIdReusePolicy};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    DagCatalog, SchedulerMonitor, SignalWithStartParams, StartWorkflowParams,
    UpdateWithStartParams, signal_with_start_workflow_execution_with_metrics,
    start_or_load_workflow_execution, tick_once, update_with_start_workflow_execution_with_metrics,
};
use chrono::Utc;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// Serialises the process-global gate cache + shard router across tests.
///
/// `pub(crate)` so sibling scanner suites that now consult the same
/// process-global gate cache at fire time (e.g. `throttle_tests`, issue #1053)
/// can hold this guard while firing, preventing a gate armed by a concurrent
/// gate test from spuriously blocking (or being stomped by) their fires.
// `pub(crate)` is required (a sibling test module references it via
// `crate::admission_gate_authoritative_tests::TEST_SERIAL`); clippy's
// `redundant_pub_crate` flags it only because this test module is private.
#[allow(clippy::redundant_pub_crate)]
pub(crate) static TEST_SERIAL: Mutex<()> = Mutex::new(());

// The test DB is seeded from `autumn_harvest::full_migrations_sql()` — the
// build.rs-generated full migration bundle (identical to `diesel migration run`),
// regenerated automatically from the whole `migrations/` directory. There is
// nothing to hand-maintain or regenerate here: every table/column
// `start_or_load_workflow_execution` and the completion-trigger path read is
// present by construction, so the schema is drift-proof (a hand-picked subset
// would silently rot as the start path gains column reads — e.g. issue #618, where
// `harvest_build_policies` from build_routing AND `legal_hold_set_at` from a much
// later migration were both missing from an earlier subset). The applied superset
// never over-constrains a test.
fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Per-workflow schedule columns the cross-shard test's fresh DBs need. These are
/// already present in the full migration bundle (the `harvest_schedules`/`schedule_id`
/// migrations add `workflow_name`/`queue_name`), so the `ADD COLUMN IF NOT EXISTS`
/// clauses are idempotent no-ops kept for defensive clarity; `resolve_target_queue`
/// selects `queue_name WHERE workflow_name`.
const SCHED_COLS: &str = "ALTER TABLE harvest_schedules ALTER COLUMN dag_name DROP NOT NULL; \
     ALTER TABLE harvest_schedules ADD COLUMN IF NOT EXISTS workflow_name TEXT; \
     ALTER TABLE harvest_schedules ADD COLUMN IF NOT EXISTS queue_name TEXT;";

/// Capturing metrics recorder — records `record_admission_blocked` calls
/// (`scope_kind`, reason) so a test can assert a completion-trigger block is
/// counted identically to a direct API start.
#[derive(Default)]
struct CapturingMetrics {
    blocked: Mutex<Vec<(String, String)>>,
    fired: Mutex<Vec<(String, String)>>,
    bypassed: Mutex<Vec<String>>,
    skipped: Mutex<Vec<(String, String, String)>>,
}

impl CapturingMetrics {
    fn blocked(&self) -> Vec<(String, String)> {
        self.blocked.lock().unwrap().clone()
    }
    fn fired(&self) -> Vec<(String, String)> {
        self.fired.lock().unwrap().clone()
    }
    fn bypassed(&self) -> Vec<String> {
        self.bypassed.lock().unwrap().clone()
    }
    fn skipped(&self) -> Vec<(String, String, String)> {
        self.skipped.lock().unwrap().clone()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_admission_blocked(&self, scope_kind: &str, reason_hash: &str) {
        self.blocked
            .lock()
            .unwrap()
            .push((scope_kind.to_string(), reason_hash.to_string()));
    }
    fn record_completion_trigger_fired(&self, trigger: &str, outcome: &str) {
        self.fired
            .lock()
            .unwrap()
            .push((trigger.to_string(), outcome.to_string()));
    }
    fn record_admission_bypassed(&self, producer: &str) {
        self.bypassed.lock().unwrap().push(producer.to_string());
    }
    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        self.skipped
            .lock()
            .unwrap()
            .push((kind.to_string(), name.to_string(), reason.to_string()));
    }
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
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
        .max_size(4)
        .build()
        .expect("pool build failed")
}

/// Replace the database-name path segment of a Postgres URL (no query params in
/// the test URLs). Used by the cross-shard test to derive per-shard DB URLs on
/// the same cluster.
fn swap_db_name(url: &str, new_db: &str) -> String {
    url.rfind('/').map_or_else(
        || format!("{url}/{new_db}"),
        |i| format!("{}/{}", &url[..i], new_db),
    )
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_trigger_fires",
        "DELETE FROM harvest_completion_trigger_outbox",
        "DELETE FROM harvest_completion_triggers",
        "DELETE FROM harvest_admission_gates",
        "DELETE FROM harvest_debounce",
        "DELETE FROM harvest_start_throttle",
        "DELETE FROM harvest_event_batches",
        "DELETE FROM harvest_rate_limit_buckets",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        // Ignore "table doesn't exist" only if the migration set omitted it;
        // all listed tables are in the full migration bundle, so a real error
        // should surface.
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct OutcomeRow {
    #[diesel(sql_type = Nullable<Text>)]
    outcome: Option<String>,
}

async fn insert_trigger(conn: &mut AsyncPgConnection, trigger_id: Uuid) {
    diesel::sql_query(
        "INSERT INTO harvest_completion_triggers
            (id, source_workflow_name, terminal_states, target_workflow_name, input_mapping)
         VALUES ($1, 'ag_source_wf', '[\"Completed\"]'::jsonb, 'ag_target_wf',
                 '{\"type\":\"Passthrough\"}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(conn)
    .await
    .expect("insert trigger");
}

/// Starts an `ag_source_wf` execution on shard 0 and transitions it to
/// COMPLETED with an output, returning its exec id.
async fn start_completed_source(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_source_completed_on(conn, exec_id, workflow_id).await;
    exec_id
}

/// Starts an `ag_source_wf` execution with an explicit `exec_id` (so a caller
/// can pin it to a specific shard) and transitions it to COMPLETED.
async fn start_source_completed_on(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_id: &str,
) {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "ag_source_wf",
            workflow_id,
            exec_id,
            input: json!({"hello": "world"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: autumn_harvest::types::Priority::default(),
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start source");

    diesel::sql_query(
        "UPDATE harvest_workflow_executions
         SET state = 'COMPLETED', output = '{\"result\":\"done\"}'::jsonb, completed_at = NOW()
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("complete source");
}

async fn target_exec_count(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count target")
    .n
}

async fn target_exec_count_named(conn: &mut AsyncPgConnection, workflow_name: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<Text, _>(workflow_name)
    .get_result::<CountRow>(conn)
    .await
    .expect("count named target")
    .n
}

/// issue #1053 helper — read a rate-limit bucket's current token balance so the
/// fire-time-gate tests can assert a BLOCKED deferred throttle fire REFUNDED the
/// token it reserved before the start (nothing admitted → nothing spent).
async fn bucket_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    #[derive(diesel::QueryableByName)]
    struct TokensRow {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<TokensRow>(conn)
        .await
        .expect("read bucket tokens")
        .tokens
}

/// issue #1053 helper — count pending `harvest_start_throttle` rows for a
/// specific `workflow_id`, so a test can assert a BLOCKED deferred fire LEFT the
/// row (re-deferred, not deleted).
async fn throttle_rows_for(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_start_throttle WHERE workflow_id = $1")
        .bind::<Text, _>(workflow_id)
        .get_result::<CountRow>(conn)
        .await
        .expect("count throttle rows")
        .n
}

/// issue #1053 (review B-G1) helper — read the `deferred_at` of a pending
/// `harvest_start_throttle` row so a test can assert a gate-blocked fire BUMPED it
/// forward (the re-defer backoff that rotates a gated row behind un-gated keys).
async fn throttle_deferred_at_for(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> chrono::DateTime<Utc> {
    #[derive(diesel::QueryableByName)]
    struct DeferredRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        deferred_at: chrono::DateTime<Utc>,
    }
    diesel::sql_query("SELECT deferred_at FROM harvest_start_throttle WHERE workflow_id = $1")
        .bind::<Text, _>(workflow_id)
        .get_result::<DeferredRow>(conn)
        .await
        .expect("read throttle deferred_at")
        .deferred_at
}

/// issue #1053 helper — directly seed a prior `ag_target_wf` execution row
/// (shard 0 — the single-shard `ShardRouter::default()` used here) in a given
/// terminal/active `state`. Mirrors the plugin suite's `seed_execution`; used by
/// the (b) force-terminate residual test to plant a TERMINATED prior (a sealed
/// state excluded by the active-uniqueness partial index, so a fresh CREATE with
/// the same `workflow_id` is allowed and must therefore face the fire-time gate).
async fn seed_target_execution_state(conn: &mut AsyncPgConnection, workflow_id: &str, state: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'ag_target_wf', $2, 0, '{}'::jsonb, 'default', $3)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .execute(conn)
    .await
    .expect("seed prior target execution");
}

fn sched_noop_handler<'a>(
    _ctx: &'a autumn_harvest::WorkflowContext,
    input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async move { Ok(input) })
}

fn fleet_cache(reason: &str) -> Arc<AdmissionGateCache> {
    let cache = Arc::new(AdmissionGateCache::new());
    cache.refresh(vec![AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope: GateScope::Fleet,
        reason: reason.to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    cache
}

fn scoped_cache(scope: GateScope) -> Arc<AdmissionGateCache> {
    let cache = Arc::new(AdmissionGateCache::new());
    cache.refresh(vec![AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope,
        reason: "scoped-incident".to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    cache
}

/// AC1: a Fleet gate blocks a completion-trigger start (same block outcome as a
/// direct API start), records an exactly-once `admission_blocked` fires row, and
/// never rolls back the source terminal.
#[tokio::test]
async fn completion_trigger_blocked_by_fleet_gate() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-fleet").await;

    // Raise a Fleet gate.
    let cache = fleet_cache("incident-618");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    let deferred = evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate must not error (block is a clean skip)");

    // Clean up global state before assertions can panic.
    set_global_admission_gate_cache(None);

    // No cross-shard deferred start was produced.
    assert!(
        deferred.is_empty(),
        "a blocked start produces no deferred outbox row"
    );
    // No target workflow was started.
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "the completion-trigger target must NOT start under a Fleet gate"
    );
    // The block was counted exactly like a direct API start.
    let blocked = metrics.blocked();
    assert_eq!(blocked.len(), 1, "exactly one admission-block recorded");
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(blocked[0].1, "incident-618");
    // Exactly-once resolved-skip fires row with the admission_blocked outcome.
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1, "one fires row recorded");
    assert_eq!(fires[0].outcome.as_deref(), Some("admission_blocked"));
    // The source terminal was NOT rolled back.
    let src_state: Vec<OutcomeRow> =
        diesel::sql_query("SELECT state AS outcome FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("load source state");
    assert_eq!(src_state[0].outcome.as_deref(), Some("COMPLETED"));
}

/// Finding B (F-round5): a completion-trigger start blocked by a gate on a
/// terminal path that passes `metrics: None` (cancel / terminate /
/// parent-close-cascade in `execution.rs`) is STILL counted on
/// `harvest.admission.blocked`, via the process-global recorder fallback the
/// plugin publishes at boot. Without the fallback, `evaluate_triggers_for_execution`
/// with `metrics: None` drops-and-records the block but never counts it — a hole
/// in the "zero un-counted blocks" bar. Here the source is CANCELLED and the
/// recorder is supplied ONLY via the global, never as the `metrics` param.
#[tokio::test]
async fn completion_trigger_block_counted_via_global_recorder_on_cancel_path() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    // A trigger that fires on Cancelled (the cancel/terminate terminal path).
    let trigger_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_completion_triggers
            (id, source_workflow_name, terminal_states, target_workflow_name, input_mapping)
         VALUES ($1, 'ag_source_wf', '[\"Cancelled\"]'::jsonb, 'ag_target_wf',
                 '{\"type\":\"Passthrough\"}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .expect("insert cancel trigger");

    let source_exec_id = start_completed_source(&mut conn, "ag-src-cancel").await;
    diesel::sql_query("UPDATE harvest_workflow_executions SET state = 'CANCELLED' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("cancel source");

    // Raise a Fleet gate.
    let cache = fleet_cache("cancel-incident-618");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    // Publish the recorder ONLY via the global (never as the `metrics` param) —
    // exactly how the cancel/terminate paths reach `evaluate_triggers_for_execution`.
    let recorder = Arc::new(CapturingMetrics::default());
    set_global_admission_metrics(Some(
        Arc::clone(&recorder) as Arc<dyn autumn_harvest::telemetry::MetricsRecorder>
    ));

    let deferred = evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Cancelled,
        None, // <-- the cancel/terminate path passes None
    )
    .await
    .expect("evaluate must not error (block is a clean skip)");

    // Clean up global state before assertions can panic.
    set_global_admission_gate_cache(None);
    set_global_admission_metrics(None);

    assert!(
        deferred.is_empty(),
        "a blocked start produces no deferred row"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "the completion-trigger target must NOT start under a Fleet gate"
    );
    // The block was counted EXACTLY ONCE via the global recorder fallback.
    let blocked = recorder.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "a block on the metrics:None cancel path must be counted via the global recorder"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(blocked[0].1, "cancel-incident-618");
    // Exactly-once resolved-skip fires row.
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1, "one fires row recorded");
    assert_eq!(fires[0].outcome.as_deref(), Some("admission_blocked"));
    // Source terminal not rolled back.
    let src_state: Vec<OutcomeRow> =
        diesel::sql_query("SELECT state AS outcome FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("load source state");
    assert_eq!(src_state[0].outcome.as_deref(), Some("CANCELLED"));
}

/// AC (backward compat): with no gate cache installed, the completion trigger
/// starts the target normally — byte-identical to pre-#618.
#[tokio::test]
async fn completion_trigger_starts_when_no_gate_cache() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-none").await;

    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate");

    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "target must start normally when no gate cache is installed"
    );
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].outcome, None, "a real fire records NULL outcome");
}

/// F3: a fail-closed / uninitialized gate cache with NO cached gate matching the
/// start must NOT drop a completion-trigger start. It is in-flight continuation
/// of already-committed work, so evaluate PROCEEDS: the target starts, no
/// `admission_blocked` count, and the fires row is a real fire (NULL outcome),
/// not a block. (The companion `..._with_cached_gate_blocks` covers the case
/// where a real gate IS in the last-known snapshot.)
#[tokio::test]
async fn completion_trigger_fail_closed_no_cached_gate_proceeds() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-failclosed").await;

    // Install a fail-closed cache with an EMPTY snapshot (no cached gate).
    set_global_admission_gate_cache(Some(Arc::new(AdmissionGateCache::new_fail_closed())));

    let metrics = CapturingMetrics::default();
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate must proceed under fail-closed when no cached gate matches");

    set_global_admission_gate_cache(None);

    // The target STARTED (not dropped) — in-flight continuation is preserved.
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "fail-closed with no cached gate must NOT drop the completion-trigger start"
    );
    // No admission block was counted.
    assert!(
        metrics.blocked().is_empty(),
        "fail-closed with no cached gate must not record an admission_blocked"
    );
    // The fires row is a real fire (NULL outcome), not an admission_blocked skip.
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1);
    assert_eq!(
        fires[0].outcome, None,
        "a proceed is a real fire (NULL outcome), not admission_blocked"
    );
}

/// Codex P1 (issue #618): under fail-closed, a REAL gate already loaded into the
/// last-known cached snapshot MUST still block a matching completion-trigger
/// start (block + drop + count) — otherwise completion triggers would bypass an
/// active gate uncounted during exactly the incident the gate exists for, while
/// direct API starts still block. `set_fail_closed()` leaves the snapshot
/// intact, so the cached gate is available to match against.
#[tokio::test]
async fn completion_trigger_fail_closed_with_cached_gate_blocks() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-failclosed-cached").await;

    // A real Fleet gate is loaded into the cache, THEN a refresh fails
    // (set_fail_closed) — the snapshot is retained.
    let cache = fleet_cache("cached-incident");
    cache.set_fail_closed();
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate must not error (block is a clean skip)");

    set_global_admission_gate_cache(None);

    // The known-active cached gate blocks the start even under fail-closed.
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "a real cached gate must block the completion-trigger start under fail-closed"
    );
    let blocked = metrics.blocked();
    assert_eq!(blocked.len(), 1, "the block was counted exactly once");
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(blocked[0].1, "cached-incident");
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].outcome.as_deref(), Some("admission_blocked"));
}

/// F4: re-evaluating the SAME (source, trigger) with the gate still active must
/// NOT double-count `record_admission_blocked`. The fires row stays exactly-once
/// (ON CONFLICT DO NOTHING); the second evaluate records a `deduped` outcome
/// instead of a second block, mirroring the sibling `condition_unmet` path.
#[tokio::test]
async fn completion_trigger_block_is_not_double_counted_on_reentry() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-reentry").await;

    let cache = fleet_cache("reentry-incident");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    // First evaluate: blocked + counted once.
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("first evaluate");
    // Second evaluate (cascade re-entry): must NOT count a second block.
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("second evaluate");

    set_global_admission_gate_cache(None);

    assert_eq!(
        metrics.blocked().len(),
        1,
        "the block must be counted exactly once across re-entry"
    );
    // The second evaluate recorded a `deduped` completion-trigger-fired outcome.
    let fired_outcomes = metrics.fired();
    assert!(
        fired_outcomes
            .iter()
            .any(|(_, outcome)| outcome == "deduped"),
        "cascade re-entry must record a `deduped` outcome, not a second block: {fired_outcomes:?}"
    );
    // Still exactly one fires row (admission_blocked), never a duplicate.
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].outcome.as_deref(), Some("admission_blocked"));
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "the target never starts under the gate across re-entry"
    );
}

/// F1/F6: the deferred-fire producers relay a start admitted BEFORE a gate was
/// raised. `debounce` and `event_batch` remain `GatedAtAdmission` — with a Fleet
/// gate active each still fires (starts the target) AND increments
/// `harvest.admission.bypassed{producer}` so no admission is un-counted. Since
/// issue #1053 `throttle` is `GatedAtRelay`: under an active gate it BLOCKS at
/// fire time (re-defers the row, refunds the token, starts nothing) and counts a
/// `harvest.admission.blocked` — like the completion-trigger cross-shard relay
/// (which drops its row instead of re-deferring). This is the falsifiable "zero
/// un-counted admissions" bar for the scanner producers, running Docker-backed
/// in CI.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deferred_scanner_fires_are_counted_as_bypass() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    // Raise a Fleet gate. The pure deferred-fire scanners (debounce, throttle,
    // event-batch) are exempt — their start was already admitted before the gate,
    // so each fire is counted as a bypass. The completion-trigger CROSS-SHARD
    // relay is the exception (Finding B, F-round7): it materializes a *new*
    // pre-committed start on the target shard, so it is gated AUTHORITATIVELY at
    // relay time — under this Fleet gate it BLOCKS (drops the row, records
    // admission_blocked) rather than bypassing.
    let cache = fleet_cache("scanner-incident");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    // ── completion-trigger CROSS-SHARD relay ──
    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox
            (source_exec_id, trigger_id, target_shard, target_workflow_name,
             target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
         VALUES ($1, $2, 0, 'ag_target_wf', 'ct-outbox-scan', '{}'::jsonb, 'default',
                 '0'::jsonb, 1048576)",
    )
    .bind::<diesel::sql_types::Uuid, _>(ExecutionId::new_for_shard(ShardId::new(0)).as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .unwrap();
    let sharded = ShardedDbPool::single(pool.clone());
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut conn,
        metrics_ref,
        &Some(sharded),
        &[ShardId::new(0)],
    )
    .await
    .unwrap();
    // Finding B (F-round7): the CT cross-shard relay is now gated AUTHORITATIVELY
    // at relay time, so under the Fleet gate it BLOCKS (drops the row + records
    // admission_blocked) rather than bypassing. It still "processes" the row (it
    // was dropped), so processed_count is 1 — but it is a BLOCK, not a bypass.
    assert_eq!(
        relayed, 1,
        "CT cross-shard relay processes the row (blocked + dropped) under the gate"
    );

    // ── debounce scanner ──
    diesel::sql_query(
        "INSERT INTO harvest_debounce
            (workflow_name, debounce_key, workflow_id, queue_name, last_input,
             start_options, effective_fire_at, max_fire_at)
         VALUES ('ag_target_wf', 'k-deb', 'deb-scan', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second', NOW() + INTERVAL '1 hour')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let deb =
        autumn_harvest::debounce::fire_due_debounced_starts(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(deb, 1, "debounce scanner fires despite the gate");

    // ── throttle scanner (seed a token so it debits + fires) ──
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-thr");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-thr', $1, 'thr-scan', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    let thr =
        autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    // issue #1053: throttle is now GatedAtRelay — under the Fleet gate the
    // deferred fire BLOCKS + RE-DEFERS (leaves the row, refunds the token,
    // starts nothing) instead of firing gate-exempt.
    assert_eq!(thr, 0, "throttle scanner blocks + re-defers under the gate");
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-scan").await,
        1,
        "the blocked throttle row is re-deferred (left), not deleted"
    );
    assert!(
        (bucket_tokens(&mut conn, &bucket).await - 10.0).abs() < f64::EPSILON,
        "the token reserved before the blocked start is refunded"
    );

    // ── event-batch scanner ──
    diesel::sql_query(
        "INSERT INTO harvest_event_batches
            (workflow_name, batch_key, workflow_id, queue_name, buffered_payloads,
             start_options, fire_at, max_size)
         VALUES ('ag_target_wf', 'k-batch', 'batch-scan', 'default', '[{}]'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second', 100)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let batch =
        autumn_harvest::event_batch::fire_due_event_batches(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(batch, 1, "event-batch scanner fires despite the gate");

    set_global_admission_gate_cache(None);

    // The remaining GatedAtAdmission deferred-fire producers (debounce,
    // event_batch) counted their bypass — zero un-counted admissions. Throttle
    // is now GatedAtRelay (issue #1053): under the Fleet gate it BLOCKS at fire
    // time, so it counts a block (not a bypass). The completion-trigger
    // cross-shard relay is likewise GatedAtRelay and blocked.
    let mut bypassed = metrics.bypassed();
    bypassed.sort();
    let mut expected = vec!["debounce".to_string(), "event_batch".to_string()];
    expected.sort();
    assert_eq!(
        bypassed, expected,
        "each GatedAtAdmission scanner fire counts a bypass; throttle + the CT relay do not"
    );
    // Exactly two admissions were blocked at relay/fire time: the completion-
    // trigger cross-shard relay (dropped its row) and the throttle scanner
    // (re-deferred its row). Both are GatedAtRelay (issue #1053). The block is
    // counted once per producer (block-vs-bypass never double-counts).
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        2,
        "the CT cross-shard relay + throttle are gated at relay/fire time"
    );
    assert!(
        blocked.iter().all(|(scope, _)| scope == "fleet"),
        "both blocks came from the Fleet gate"
    );
    // Two targets started (debounce, event-batch); the CT relay and throttle
    // were blocked, so their targets never started.
    assert_eq!(
        target_exec_count(&mut conn).await,
        2,
        "debounce + event-batch started; throttle + CT relay blocked"
    );
}

/// issue #1053 (MONEY): the throttle scanner honours the admission gate at FIRE
/// time. Under a CLOSED gate a deferred throttle start must BLOCK + RE-DEFER —
/// leave the `harvest_start_throttle` row (do NOT delete), refund the token it
/// reserved before the start, create NO execution, and count the block exactly
/// once on the passed recorder. Once the gate OPENS, the held start FIRES
/// exactly once (row deleted, execution created, a `throttle` bypass counted).
///
/// This is the falsifiable RED for the core mechanism: against unchanged code
/// the throttle scanner is gate-exempt by construction (`_collect(.., None, None)`),
/// so Phase A FIRES despite the gate (`n == 1`, row deleted, execution created,
/// token spent, no block counted) and the `assert_eq!(n, 0)` fails.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn throttle_scanner_re_defers_under_a_closed_gate_and_fires_when_open() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    // Clean baseline (a prior panicking RED test may have left a gate installed).
    set_global_admission_gate_cache(None);

    // Full bucket (10 tokens, no refill) + one due throttle row for `thr-redefer`.
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-thr");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-thr', $1, 'thr-redefer', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    // Capture the row's original (past) `deferred_at` so Phase A can prove the
    // gate-block re-defer BUMPED it forward (review B-G1 backoff).
    let deferred_before = throttle_deferred_at_for(&mut conn, "thr-redefer").await;

    // ── Phase A: gate CLOSED — block + re-defer ──
    set_global_admission_gate_cache(Some(fleet_cache("redefer-incident")));
    let n = autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
        .await
        .unwrap();
    assert_eq!(n, 0, "throttle scanner blocks + re-defers under the gate");
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-redefer").await,
        1,
        "the blocked row is LEFT (re-deferred), not deleted"
    );
    // review B-G1: the gate-blocked row's `deferred_at` is bumped forward by
    // GATE_REDEFER_BACKOFF, rotating it behind un-gated keys in the fire ordering.
    let deferred_after = throttle_deferred_at_for(&mut conn, "thr-redefer").await;
    assert!(
        deferred_after > deferred_before,
        "the gate-blocked row's deferred_at is advanced by the re-defer backoff \
         (before={deferred_before}, after={deferred_after})"
    );
    assert!(
        deferred_after > Utc::now(),
        "the backed-off deferred_at is now in the future (behind newly-arriving un-gated rows)"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "no execution is created for a blocked fire"
    );
    assert!(
        (bucket_tokens(&mut conn, &bucket).await - 10.0).abs() < 1e-9,
        "the token reserved before the start is refunded on a block"
    );
    assert_eq!(
        metrics.blocked().len(),
        1,
        "the block is counted exactly once on the passed recorder (metrics threading)"
    );
    assert_eq!(
        metrics.blocked()[0].0,
        "fleet",
        "the block came from the Fleet gate"
    );
    assert!(
        !metrics.bypassed().contains(&"throttle".to_string()),
        "a blocked fire counts NO bypass"
    );

    // ── Phase B: gate OPEN — the held start fires ──
    // The re-defer backoff pushed `deferred_at` into the future; make the row
    // due again before driving the open-gate fire (defensive: the candidate
    // SELECT has no `deferred_at <= NOW()` floor today, so it would fire anyway,
    // but resetting keeps the assertion robust if a floor is ever added).
    diesel::sql_query(
        "UPDATE harvest_start_throttle SET deferred_at = NOW() - INTERVAL '1 second' \
         WHERE workflow_id = 'thr-redefer'",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    set_global_admission_gate_cache(None);
    let n2 =
        autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(n2, 1, "the held start fires once the gate opens");
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-redefer").await,
        0,
        "the row is deleted once the start fires"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "the execution is created once the gate opens"
    );
    assert!(
        metrics.bypassed().contains(&"throttle".to_string()),
        "an open-gate fire counts a throttle bypass"
    );
}

/// issue #1053 — AC (a) "gate-armed-after-deferral looseness". A throttle start
/// deferred while the gate was open/absent must be HELD (not fired gate-exempt)
/// once the gate arms before its scanner fire. This is the pre-existing
/// looseness #1053 closes; it is the general case of which (b) is a subset.
///
/// RED against unchanged code: the scanner fires despite the now-armed gate
/// (`n == 1`), failing `assert_eq!(n, 0)`.
#[tokio::test]
async fn throttle_row_deferred_before_gate_is_held_when_gate_arms_before_fire() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    // Seed a full bucket + a due throttle row for `thr-late-gate` WHILE NO GATE
    // is installed (a start deferred while the gate was open/absent).
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-late");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-late', $1, 'thr-late-gate', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    // NOW arm the Fleet gate (it was armed AFTER the row was deferred), then fire.
    set_global_admission_gate_cache(Some(fleet_cache("late-gate-incident")));
    let n = autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
        .await
        .unwrap();
    assert_eq!(
        n, 0,
        "a start deferred while the gate was open is HELD once the gate arms before its fire"
    );
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-late-gate").await,
        1,
        "the row is re-deferred (left), not dropped"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "no execution is created under the gate"
    );
    assert!(
        (bucket_tokens(&mut conn, &bucket).await - 10.0).abs() < 1e-9,
        "the reserved token is refunded on a block"
    );
    assert_eq!(
        metrics.blocked().len(),
        1,
        "the block is counted on the passed recorder"
    );
}

/// issue #1053 — AC (b) "bypass + force-terminate residual" (the narrow PR-#1051
/// residual). A bypass-eligible prior (was RUNNING → attach → the defer-time
/// unlocked read bypassed the admission gate) is force-terminated to TERMINATED
/// after the read. The deferred fire would now CREATE a fresh replacement
/// (`start_will_create_new_execution(None-from-TERMINATED, AllowDuplicate) == true`);
/// the fire-time `CheckCached` gate must catch it and re-defer. (b) ⊂ (a): any
/// deferred fire faces the fire-time gate — force-terminate is merely what turns
/// a would-be attach into a would-be CREATE.
///
/// RED against unchanged code: TERMINATED is excluded from the active-uniqueness
/// partial index, so the gate-exempt scanner CREATEs a fresh replacement
/// (`n == 1`, a 2nd `ag_target_wf` row), failing `assert_eq!(n, 0)`.
#[tokio::test]
async fn throttle_deferred_fire_over_a_force_terminated_prior_faces_the_gate() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    // A prior for `thr-forceterm` that was force-terminated to TERMINATED after
    // the defer-time unlocked read.
    seed_target_execution_state(&mut conn, "thr-forceterm", "TERMINATED").await;

    // Seed a full bucket + a due throttle row for the SAME workflow_id.
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-ft");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-ft', $1, 'thr-forceterm', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    set_global_admission_gate_cache(Some(fleet_cache("forceterm-incident")));
    let n = autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
        .await
        .unwrap();
    assert_eq!(
        n, 0,
        "the deferred fire would CREATE a fresh replacement over the TERMINATED \
         prior; the fire-time gate catches it and re-defers"
    );
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-forceterm").await,
        1,
        "the row is re-deferred (left), not dropped"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "only the pre-seeded TERMINATED prior exists; no fresh replacement is created"
    );
    assert!(
        (bucket_tokens(&mut conn, &bucket).await - 10.0).abs() < 1e-9,
        "the reserved token is refunded on a block"
    );
    assert_eq!(
        metrics.blocked().len(),
        1,
        "the block is counted on the passed recorder"
    );
}

/// issue #1053 (review B-G1 regression pin): a queue-scoped gate must NOT starve
/// throttle starts on OTHER (un-gated) queues. A gate-blocked throttle row's
/// `deferred_at` is bumped forward (the re-defer backoff), so it rotates behind
/// un-gated rows in the `ORDER BY deferred_at ASC` fire ordering; an un-gated row
/// on a different queue FIRES in the same tick while the gated row is HELD and
/// backed off.
///
/// The gate is scoped `GateScope::Queue("gated-q")`, matching only the gated
/// row's queue. The un-gated row targets `open-q` (gate does not match) and must
/// fire even though the gated row is OLDER (would otherwise be selected first and,
/// without the backoff, keep monopolizing the fire batch across ticks).
#[tokio::test]
async fn throttle_gate_backoff_does_not_starve_ungated_keys() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    // Two full buckets + two due throttle rows on DIFFERENT queues. The gated row
    // is OLDER (deferred earlier) so without the backoff it would sit ahead of the
    // un-gated row and be re-selected every tick.
    let gated_bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-gated");
    let open_bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-open");
    for b in [&gated_bucket, &open_bucket] {
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets
                (key, refill_rate, burst, tokens, last_refilled_at)
             VALUES ($1, 0.0, 10.0, 10.0, NOW())",
        )
        .bind::<Text, _>(b)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    // Gated row on `gated-q`, older deferral.
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-gated', $1, 'gated-starve', 'gated-q', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '10 seconds')",
    )
    .bind::<Text, _>(&gated_bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    // Un-gated row on `open-q`, newer deferral.
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-open', $1, 'ung-starve', 'open-q', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&open_bucket)
    .execute(&mut conn)
    .await
    .unwrap();

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    let gated_deferred_before = throttle_deferred_at_for(&mut conn, "gated-starve").await;

    // Arm a gate scoped ONLY to `gated-q`, then fire once.
    set_global_admission_gate_cache(Some(scoped_cache(GateScope::Queue("gated-q".to_string()))));
    let n = autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
        .await
        .unwrap();

    // The un-gated row FIRED; the gated row was HELD + backed off.
    assert_eq!(
        n, 1,
        "exactly the un-gated row fires under a queue-scoped gate"
    );
    assert_eq!(
        throttle_rows_for(&mut conn, "ung-starve").await,
        0,
        "the un-gated row FIRES (row deleted) — it is not starved by the gated row"
    );
    assert_eq!(
        throttle_rows_for(&mut conn, "gated-starve").await,
        1,
        "the gated row is HELD (re-deferred), not fired and not dropped"
    );
    let gated_deferred_after = throttle_deferred_at_for(&mut conn, "gated-starve").await;
    assert!(
        gated_deferred_after > gated_deferred_before,
        "the gated row's deferred_at is advanced by the backoff so it rotates behind \
         un-gated keys (before={gated_deferred_before}, after={gated_deferred_after})"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "only the un-gated start created an execution"
    );
    assert!(
        metrics.bypassed().contains(&"throttle".to_string()),
        "the un-gated fire counts a throttle bypass"
    );
    assert_eq!(
        metrics.blocked().len(),
        1,
        "exactly the gated row's block is counted"
    );
}

/// issue #1053 (review B-G2 / FIX-2 contract pin): a throttle row held under a
/// CLOSED gate that reaches its own `schedule_to_start` deadline is DROPPED as
/// stale (issue #607 AC-c) — NOT held forever by the re-defer. The gate does not
/// exempt a start from its own staleness bound; the AC-c expiry check runs before
/// the gate is ever consulted, so an expired row drops regardless of the gate.
#[tokio::test]
async fn throttle_gated_row_past_schedule_to_start_deadline_is_dropped_not_held() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    // Full bucket + a due throttle row whose schedule_to_start deadline has ALREADY
    // passed (expires_at in the past).
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-stale");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at, expires_at)
         VALUES ('ag_target_wf', 'k-stale', $1, 'thr-stale', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '10 seconds', NOW() - INTERVAL '1 second')",
    )
    .bind::<Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    // Keep the gate CLOSED; the row is past its deadline, so it must be dropped as
    // stale (not held indefinitely by the re-defer).
    set_global_admission_gate_cache(Some(fleet_cache("stale-under-gate-incident")));
    let n = autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
        .await
        .unwrap();

    assert_eq!(n, 0, "no start fires for a stale row");
    assert_eq!(
        throttle_rows_for(&mut conn, "thr-stale").await,
        0,
        "the row past its schedule_to_start deadline is DROPPED (deleted), not held under the gate"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "no execution is created for a dropped-stale row"
    );
    // The AC-c expiry check runs BEFORE the token reserve and BEFORE the gate, so
    // the drop is a staleness timeout — NOT a gate block. The token is untouched
    // (never reserved) and no `harvest.admission.blocked` is counted.
    assert!(
        (bucket_tokens(&mut conn, &bucket).await - 10.0).abs() < 1e-9,
        "no token is consumed for a stale-dropped row (expiry precedes the token reserve)"
    );
    assert_eq!(
        metrics.blocked().len(),
        0,
        "a staleness drop is NOT a gate block — the gate is never consulted for an expired row"
    );
}

/// F-round17 (Finding A): the event-batch IMMEDIATE (max_size-reached) flush starts
/// the workflow synchronously in the request path, so it must count an
/// `event_batch` bypass exactly like the scanner's deferred fire. A deferred
/// (not-yet-full) admission counts nothing until its flush, and an
/// immediately-flushed batch is never also fired (or counted) by the scanner —
/// exactly-once, immediate XOR scanner.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn immediate_max_size_flush_counts_bypass_exactly_once() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    // A Fleet gate is raised (as if during an incident, after the HTTP pre-check).
    // The event-batch producer is exempt-with-bypass-counter, so the immediate flush
    // proceeds and is COUNTED (never silently slips the gate).
    let cache = fleet_cache("immediate-flush-incident");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    // ── Phase A: max_size = 1 → the single admission flushes immediately. ──
    let recorder_a = Arc::new(CapturingMetrics::default());
    let ref_a: &(dyn MetricsRecorder + Send + Sync) = recorder_a.as_ref();
    let (outcome_a, _deferred) = autumn_harvest::event_batch::admit_batched_start(
        &mut conn,
        autumn_harvest::event_batch::AdmitBatchParams {
            workflow_name: "ag_target_wf".to_string(),
            batch_key: "k-immediate".to_string(),
            workflow_id: "batch-immediate".to_string(),
            queue_name: "default".to_string(),
            payload: json!({}),
            start_options: autumn_harvest::debounce::DebounceStartOptions::default(),
            max_wait: std::time::Duration::from_secs(3600),
            max_size: 1,
            shard_id: 0,
        },
        Some(ref_a),
    )
    .await
    .expect("admit_batched_start must not error")
    .expect("admit returns an outcome");
    assert!(
        outcome_a.is_flushed,
        "max_size = 1 flushes the batch immediately in the request path"
    );
    assert!(
        outcome_a.flushed_execution_id.is_some(),
        "the immediate flush started a workflow"
    );
    assert_eq!(
        recorder_a.bypassed(),
        vec!["event_batch".to_string()],
        "the immediate max_size flush counts the event_batch bypass exactly once"
    );

    // The scanner must NOT re-fire (or re-count) the flushed batch — the immediate
    // flush DELETEd the row inside its transaction, so immediate XOR scanner holds.
    let scanner_fired =
        autumn_harvest::event_batch::fire_due_event_batches(&mut conn, &None, &[], ref_a)
            .await
            .expect("scanner");
    assert_eq!(
        scanner_fired, 0,
        "an immediately-flushed batch is not re-fired by the scanner"
    );
    assert_eq!(
        recorder_a.bypassed(),
        vec!["event_batch".to_string()],
        "no double-count: the scanner did not add a second bypass"
    );

    // ── Phase B: max_size = 2, one admission → DEFERRED, counts nothing yet. ──
    let recorder_b = Arc::new(CapturingMetrics::default());
    let ref_b: &(dyn MetricsRecorder + Send + Sync) = recorder_b.as_ref();
    let (outcome_b, _deferred_b) = autumn_harvest::event_batch::admit_batched_start(
        &mut conn,
        autumn_harvest::event_batch::AdmitBatchParams {
            workflow_name: "ag_target_wf".to_string(),
            batch_key: "k-deferred".to_string(),
            workflow_id: "batch-deferred".to_string(),
            queue_name: "default".to_string(),
            payload: json!({}),
            start_options: autumn_harvest::debounce::DebounceStartOptions::default(),
            max_wait: std::time::Duration::from_secs(3600),
            max_size: 2,
            shard_id: 0,
        },
        Some(ref_b),
    )
    .await
    .expect("admit_batched_start must not error")
    .expect("admit returns an outcome");
    assert!(
        !outcome_b.is_flushed,
        "a single admission into a max_size = 2 batch defers (no immediate flush)"
    );
    assert!(
        recorder_b.bypassed().is_empty(),
        "a deferred (not-yet-full) admission counts no bypass until it flushes"
    );

    set_global_admission_gate_cache(None);
}

/// F-round17 (Finding B): the scheduler is a *gated* producer. A due schedule
/// blocked by an active gate must appear in `harvest.admission.blocked` (via
/// `record_admission_blocked`) in addition to the schedule-domain
/// `record_schedule_skipped(..., "admission_blocked")` signal — so a scheduled
/// start blocked during an incident is not missing from the "zero-uncounted gated
/// producer" contract.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scheduled_start_blocked_by_gate_records_admission_blocked() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    diesel::sql_query("DELETE FROM harvest_schedules")
        .execute(&mut conn)
        .await
        .expect("scrub schedules");
    install_global_router(ShardRouter::default());

    let wf_name = "ag_sched_wf";

    // A due schedule (next_run_at in the past).
    diesel::sql_query(
        "INSERT INTO harvest_schedules
            (id, workflow_name, schedule_expr, timezone, catchup, max_active_runs,
             is_paused, next_run_at, jitter_secs, overlap_policy, buffered_runs,
             buffer_all_max, skip_policy)
         VALUES (gen_random_uuid(), $1, 'interval:60', 'UTC', false, 10, false,
                 NOW() - INTERVAL '5 seconds', 0, 'skip', '[]'::jsonb, 100, 'skip')",
    )
    .bind::<Text, _>(wf_name)
    .execute(&mut conn)
    .await
    .expect("insert due schedule");

    // A matching Fleet gate persisted in the DB — the scheduler loads gates from the
    // central store at tick time (not the global cache).
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Fleet,
        "sched-incident",
        None,
        "test",
        None,
    )
    .await
    .expect("persist gate");

    // Build a registry whose telemetry recorder is our capturing recorder, so the
    // tick's block/skip metric calls are observable.
    let recorder = Arc::new(CapturingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "admission_gate_authoritative_tests",
            handler: sched_noop_handler,
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
        }],
        vec![],
        Arc::new(SharedStateMap::default()),
        telemetry,
    ));

    tick_once(
        pool.clone(),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once");

    // Cleanup persisted gate/schedule before asserting.
    diesel::sql_query("DELETE FROM harvest_admission_gates")
        .execute(&mut conn)
        .await
        .expect("scrub gates");
    diesel::sql_query("DELETE FROM harvest_schedules")
        .execute(&mut conn)
        .await
        .expect("scrub schedules");

    // The block appears in harvest.admission.blocked with the Fleet scope kind…
    let blocked = recorder.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "a due schedule blocked by a gate records exactly one admission block"
    );
    assert_eq!(
        blocked[0].0, "fleet",
        "the admission block carries the matched gate's scope kind"
    );
    // …AND the schedule-domain skip signal is still emitted.
    let skipped = recorder.skipped();
    assert!(
        skipped.iter().any(|(kind, name, reason)| kind == "workflow"
            && name == wf_name
            && reason == "admission_blocked"),
        "the schedule-skip signal is still recorded alongside the admission block: {skipped:?}"
    );
    // The blocked schedule started no workflow.
    assert_eq!(target_exec_count_named(&mut conn, wf_name).await, 0);
}

/// AC2 semantics at the trigger layer: a scoped gate that does NOT match the
/// target lets the start proceed; a matching scoped gate blocks it.
#[tokio::test]
async fn completion_trigger_scoped_gate_matches_and_misses() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;

    // Non-matching scope (a different workflow name) → start proceeds.
    let miss_source = start_completed_source(&mut conn, "ag-src-miss").await;
    set_global_admission_gate_cache(Some(scoped_cache(GateScope::WorkflowName(
        "some_other_wf".to_string(),
    ))));
    evaluate_triggers_for_execution(&mut conn, miss_source, TerminalState::Completed, None)
        .await
        .expect("evaluate miss");
    set_global_admission_gate_cache(None);
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "a non-matching scoped gate must not block the target start"
    );

    // Matching scope (the target workflow name) → start blocked.
    let hit_source = start_completed_source(&mut conn, "ag-src-hit").await;
    set_global_admission_gate_cache(Some(scoped_cache(GateScope::WorkflowName(
        "ag_target_wf".to_string(),
    ))));
    let metrics = CapturingMetrics::default();
    evaluate_triggers_for_execution(
        &mut conn,
        hit_source,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate hit");
    set_global_admission_gate_cache(None);
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "a matching scoped gate must block the second start (count stays at 1)"
    );
    assert_eq!(metrics.blocked().len(), 1);
    assert_eq!(metrics.blocked()[0].0, "workflow_name");
}

/// F1 (Codex re-review, issue #618): a persisted admission gate loaded at
/// startup via the REAL `load_active_gates` boot-load path — the one the plugin
/// now runs BEFORE spawning workers/scanners — must block a completion trigger
/// that fires in the boot window. This exercises the boot-load → cache →
/// trigger-block chain end-to-end (persist a gate row, load it with the exact
/// boot-load call, refresh + publish the cache, evaluate a matching trigger).
///
/// The plugin boot ORDERING itself (`load_active_gates` + `refresh` BEFORE
/// `HarvestRunner::start`) is verified by code reading: `start_harvest_runtime`
/// is a private `async fn` that needs a fully-spawned runner, so a true
/// startup-race test is impractical. This test proves the behavioural guarantee
/// that reordering exists to provide — a persisted gate, once loaded into the
/// cache, blocks a completion trigger rather than being bypassed against an
/// empty snapshot.
#[tokio::test]
async fn boot_load_of_persisted_gate_blocks_completion_trigger() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let source_exec_id = start_completed_source(&mut conn, "ag-src-bootload").await;

    // Persist a Fleet gate row, then run the EXACT boot-load path the plugin
    // runs before workers spawn: load_active_gates -> refresh.
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Fleet,
        "boot-incident",
        None,
        "test",
        None,
    )
    .await
    .expect("persist gate");
    let cache = Arc::new(AdmissionGateCache::new());
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .expect("boot-load gates");
    assert_eq!(gates.len(), 1, "the persisted gate is loaded at boot");
    cache.refresh(gates);
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    let deferred = evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate must not error (block is a clean skip)");

    set_global_admission_gate_cache(None);
    // Remove the persisted gate so it can't leak into a sibling test even if an
    // assertion below panics (the next test's `scrub` also clears it).
    diesel::sql_query("DELETE FROM harvest_admission_gates")
        .execute(&mut conn)
        .await
        .expect("scrub gates");

    assert!(
        deferred.is_empty(),
        "a boot-loaded gate blocks the trigger start (no deferred outbox row)"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "a persisted gate loaded at boot must block the completion-trigger target"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the boot-loaded gate block is counted once"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(blocked[0].1, "boot-incident");
    let fires: Vec<OutcomeRow> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires WHERE source_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load fires");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].outcome.as_deref(), Some("admission_blocked"));
}

/// F2 (Codex re-review, issue #618): for a CROSS-SHARD completion trigger
/// (target hashing to shard 0, source on a nonzero shard, no explicit trigger
/// queue), the inline admission-gate check must resolve the target queue on the
/// TARGET shard — the same queue the outbox relay uses at fire time — NOT on the
/// source transaction connection. This focuses on the exact function the fix
/// introduces, `resolve_cross_shard_target_queue`, and proves it resolves the
/// queue on the target shard where the pre-fix `resolve_target_queue(source_conn,
/// …, shard 0)` path resolved it on the WRONG (source) connection and missed it.
///
/// Genuine two-database multi-shard setup: shard 0 (target) owns the schedule
/// mapping `ag_target_wf -> ag_priority_q`; shard 1 (source) has NO such
/// schedule. It focuses on `harvest_schedules` (no workflow start), so it RUNS
/// against a real cluster (local Postgres or testcontainers) via two fresh
/// databases seeded from the full migration bundle. The full evaluate → gate →
/// block chain for a cross-shard target additionally needs
/// `start_or_load_workflow_execution`; that path is covered by the same-shard
/// block tests above (all now backed by the full start-path migration bundle,
/// which includes `harvest_build_policies`).
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn cross_shard_gate_check_resolves_target_queue_on_target_shard() {
    use diesel_async::SimpleAsyncConnection;
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (base_url, _c) = setup_db().await;

    // Two fresh databases on the same cluster: s0 = shard 0 (target), s1 =
    // shard 1 (source). Unique names so repeated local runs never collide.
    let s0_name = format!("ag618_s0_{}", Uuid::new_v4().simple());
    let s1_name = format!("ag618_s1_{}", Uuid::new_v4().simple());
    {
        let base_pool = build_pool(&base_url);
        let mut admin = base_pool.get().await.unwrap();
        for name in [&s0_name, &s1_name] {
            diesel::sql_query(format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await
                .expect("create shard db");
        }
    }
    let s0_url = swap_db_name(&base_url, &s0_name);
    let s1_url = swap_db_name(&base_url, &s1_name);

    // Migrate both fresh DBs and add the per-workflow schedule columns.
    for u in [&s0_url, &s1_url] {
        let p = build_pool(u);
        let mut c = p.get().await.unwrap();
        c.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate fresh shard db");
        c.batch_execute(SCHED_COLS)
            .await
            .expect("add schedule columns");
    }

    let pool0 = build_pool(&s0_url);
    let pool1 = build_pool(&s1_url);

    // writable = [0] so the target always hashes to shard 0; readable = [0, 1].
    install_global_router(ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    ));
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), pool1.clone());
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    // shard 0 (target): the schedule mapping ag_target_wf -> ag_priority_q.
    {
        let mut c0 = pool0.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_schedules (id, workflow_name, queue_name, schedule_expr) \
             VALUES (gen_random_uuid(), 'ag_target_wf', 'ag_priority_q', '@daily')",
        )
        .execute(&mut c0)
        .await
        .expect("insert target schedule");
    }

    // A source (shard 1) connection — the connection the inline gate check holds.
    let mut src_conn = pool1.get().await.unwrap();

    // FIX: resolve on the TARGET shard (0) → finds the target's real queue.
    // No source connection is passed — the resolver goes to the target shard's
    // own pool (installed in `sharded`), so it reads shard 0's schedules.
    let resolved = autumn_harvest::completion_trigger::resolve_cross_shard_target_queue(
        "ag_target_wf",
        ShardId::new(0),
    )
    .await;

    // PRE-FIX behaviour, for contrast: `resolve_target_queue(source_conn, …,
    // shard 0)` treats shard 0 as directly queryable on the connection it holds —
    // the SOURCE connection (shard 1), which has no ag_target_wf schedule — so it
    // misses it. The fixed cross-shard resolver never takes a source connection,
    // so this wrong-shard read is now structurally impossible from it.
    let buggy = autumn_harvest::completion_trigger::resolve_target_queue(
        &mut src_conn,
        "ag_target_wf",
        ShardId::new(0),
    )
    .await;

    // FALLBACK path (F2 re-review round 2): the target-shard (0) pool is
    // UNAVAILABLE. Seed shard 1 (source) with a WRONG queue for ag_target_wf and
    // install a sharded pool that maps ONLY shard 1 — so `exact_pool_for(0)` is
    // None and the resolver takes the fallback. It must return the shard-
    // independent default queue, NEVER the source shard's `ag_source_wrong_q`
    // (which would prove it wrongly queried the source connection's schedules).
    diesel::sql_query(
        "INSERT INTO harvest_schedules (id, workflow_name, queue_name, schedule_expr) \
         VALUES (gen_random_uuid(), 'ag_target_wf', 'ag_source_wrong_q', '@daily')",
    )
    .execute(&mut src_conn)
    .await
    .expect("seed source-shard wrong schedule");
    let mut only_src = BTreeMap::new();
    only_src.insert(ShardId::new(1), pool1.clone());
    // `from_map` self-installs into GLOBAL_SHARDED_POOL; it maps only shard 1,
    // so `exact_pool_for(0)` is None and the resolver takes the fallback.
    let sharded_no_target = ShardedDbPool::from_map(only_src, ShardId::new(1));
    let fallback = autumn_harvest::completion_trigger::resolve_cross_shard_target_queue(
        "ag_target_wf",
        ShardId::new(0),
    )
    .await;

    // Restore single-shard globals for sibling tests, then best-effort drop the
    // per-shard databases (unique names → a leaked DB on failure never collides).
    install_global_router(ShardRouter::default());
    let _ = ShardedDbPool::single(build_pool(&base_url));
    drop(src_conn);
    drop(sharded);
    drop(sharded_no_target);
    drop(pool0);
    drop(pool1);
    {
        let base_pool = build_pool(&base_url);
        if let Ok(mut admin) = base_pool.get().await {
            for name in [&s0_name, &s1_name] {
                let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await;
            }
        }
    }

    assert_eq!(
        resolved, "ag_priority_q",
        "the fix must resolve the target queue on the TARGET shard (0), not the \
         source connection"
    );
    assert_ne!(
        buggy, "ag_priority_q",
        "the pre-fix source-connection resolution must NOT find the target queue \
         (it queries the source shard's schedules, missing the gate)"
    );
    // F2 re-review round 2: with the target-shard pool unavailable, the fallback
    // must NOT read the source shard's schedules — so it returns neither the
    // source's wrong queue nor the (unreachable) target's queue.
    assert_ne!(
        fallback, "ag_source_wrong_q",
        "the target-shard-unavailable fallback must NOT resolve against the source \
         connection's harvest_schedules (that is the wrong-shard bug F2 fixed)"
    );
    assert_ne!(
        fallback, "ag_priority_q",
        "the fallback cannot reach the unavailable target shard's schedules either"
    );
    assert_eq!(
        fallback, "default",
        "the fallback returns the shard-independent default queue"
    );
}

/// Finding (F-round6): the IMMEDIATE cross-shard completion-trigger relay via
/// `DeferredTriggerStart::spawn()` — the common successful path — must count the
/// `completion_trigger_outbox` bypass, not just the later scanner. Round-1 added
/// the counter only in `enforce_completion_triggers_outbox`, so a gate raised
/// after the source commit but before the immediate relay left the exempt start
/// UNCOUNTED. The count is emitted via the process-global recorder (spawn has no
/// metrics param), gated on the outbox-row delete actually removing the row, so
/// the scanner can never re-process and re-count the same relay (exactly-once).
///
/// Genuine two-database multi-shard test: source shard 0 owns the outbox row,
/// target shard 1 receives the start. Runs against a real cluster via two fresh
/// full-schema databases; `spawn()` is fire-and-forget, so we poll for the count.
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn immediate_outbox_relay_counts_the_bypass_exactly_once() {
    use diesel_async::SimpleAsyncConnection;
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (base_url, _c) = setup_db().await;

    // source shard 0 (owns the outbox row), target shard 1 (receives the start).
    let s0_name = format!("ag618_src_{}", Uuid::new_v4().simple());
    let s1_name = format!("ag618_tgt_{}", Uuid::new_v4().simple());
    {
        let base_pool = build_pool(&base_url);
        let mut admin = base_pool.get().await.unwrap();
        for name in [&s0_name, &s1_name] {
            diesel::sql_query(format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await
                .expect("create shard db");
        }
    }
    let s0_url = swap_db_name(&base_url, &s0_name);
    let s1_url = swap_db_name(&base_url, &s1_name);
    for u in [&s0_url, &s1_url] {
        let p = build_pool(u);
        let mut c = p.get().await.unwrap();
        c.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate fresh shard db");
    }

    let src_pool = build_pool(&s0_url);
    let tgt_pool = build_pool(&s1_url);
    install_global_router(ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    ));
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), src_pool.clone());
    pools.insert(ShardId::new(1), tgt_pool.clone());
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    // Insert the outbox row on the SOURCE shard; capture its id + fires-row key.
    let outbox_id = Uuid::new_v4();
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();
    let trigger_key_id = Uuid::new_v4();
    {
        let mut src_conn = src_pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_outbox
                (id, source_exec_id, trigger_id, target_shard, target_workflow_name,
                 target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
             VALUES ($1, $2, $3, 1, 'ag_target_wf', 'ct-immediate-relay', '{}'::jsonb,
                     'default', '0'::jsonb, 1048576)",
        )
        .bind::<diesel::sql_types::Uuid, _>(outbox_id)
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert outbox row");
    }

    // Publish the recorder via the global (spawn resolves it there).
    let recorder = Arc::new(CapturingMetrics::default());
    set_global_admission_metrics(Some(
        Arc::clone(&recorder) as Arc<dyn autumn_harvest::telemetry::MetricsRecorder>
    ));

    // Fire the IMMEDIATE relay (fire-and-forget).
    let deferred = autumn_harvest::completion_trigger::DeferredTriggerStart {
        outbox_id,
        source_exec_id: src_exec_id,
        trigger_id: trigger_key_id,
        source_shard: ShardId::new(0),
        target_shard: ShardId::new(1),
        target_workflow_name: "ag_target_wf".to_string(),
        target_workflow_id: "ct-immediate-relay".to_string(),
        target_input: json!({}),
        queue_name: Some("default".to_string()),
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 1_048_576,
        trigger_name: "ct_immediate".to_string(),
        owner: None,
        runbook_url: None,
        severity: None,
        sla: None,
        retry_policy: None,
        max_workflow_attempts_ceiling: None,
    };
    deferred.spawn();

    // Poll for the spawned task to finish (count is gated on the delete, so a
    // non-empty bypassed list means the start + delete + count all completed).
    let mut settled = false;
    for _ in 0..200 {
        if !recorder.bypassed().is_empty() {
            settled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Now run the SCANNER over the same source shard: the row was deleted by the
    // immediate relay, so it processes nothing and MUST NOT re-count.
    let scanner_metrics: &(dyn MetricsRecorder + Send + Sync) = &*recorder;
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut src_pool.get().await.unwrap(),
        scanner_metrics,
        &Some(sharded.clone()),
        &[ShardId::new(0)],
    )
    .await
    .unwrap();

    // Verify the target started and the outbox row is gone (on the source).
    let target_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(&mut tgt_pool.get().await.unwrap())
    .await
    .expect("count target")
    .n;
    let outbox_remaining: i64 =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox")
            .get_result::<CountRow>(&mut src_pool.get().await.unwrap())
            .await
            .expect("count outbox")
            .n;

    // Cleanup globals + drop the per-shard databases.
    set_global_admission_metrics(None);
    install_global_router(ShardRouter::default());
    let _ = ShardedDbPool::single(build_pool(&base_url));
    drop(sharded);
    drop(src_pool);
    drop(tgt_pool);
    {
        let base_pool = build_pool(&base_url);
        if let Ok(mut admin) = base_pool.get().await {
            for name in [&s0_name, &s1_name] {
                let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await;
            }
        }
    }

    assert!(
        settled,
        "the immediate spawn relay did not complete within the poll window"
    );
    assert_eq!(
        target_count, 1,
        "the immediate relay started the target on shard 1"
    );
    assert_eq!(
        outbox_remaining, 0,
        "the immediate relay deleted the outbox row"
    );
    assert_eq!(
        relayed, 0,
        "the scanner had nothing to relay (row already gone)"
    );
    // Exactly ONE bypass count across the immediate relay + the scanner.
    let bypassed = recorder.bypassed();
    assert_eq!(
        bypassed,
        vec!["completion_trigger_outbox".to_string()],
        "the immediate relay counts the completion_trigger_outbox bypass exactly once, \
         and the scanner does not double-count it"
    );
}

/// Finding B (F-round7): the cross-shard relay must gate AUTHORITATIVELY at relay
/// time on the REAL target queue. A schedule sets `queue_name = 'real-q'` for
/// `ag_target_wf` on the default shard (where the relay resolves a nonzero
/// target's queue), the `DeferredTriggerStart` carries NO explicit queue (so the
/// relay resolves `'real-q'`), and an active `Queue('real-q')` gate exists. The
/// immediate `spawn()` relay must BLOCK: target NOT started, `admission_blocked`
/// counted once, outbox row DROPPED, and the scanner afterward relays 0 (no
/// double-processing). Against pre-F-round7 code the relay had no gate check and
/// started on `'real-q'` uncounted, so this fails first.
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn immediate_outbox_relay_blocks_on_real_queue_gate() {
    use diesel_async::SimpleAsyncConnection;
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (base_url, _c) = setup_db().await;

    let s0_name = format!("ag618_bsrc_{}", Uuid::new_v4().simple());
    let s1_name = format!("ag618_btgt_{}", Uuid::new_v4().simple());
    {
        let base_pool = build_pool(&base_url);
        let mut admin = base_pool.get().await.unwrap();
        for name in [&s0_name, &s1_name] {
            diesel::sql_query(format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await
                .expect("create shard db");
        }
    }
    let s0_url = swap_db_name(&base_url, &s0_name);
    let s1_url = swap_db_name(&base_url, &s1_name);
    for u in [&s0_url, &s1_url] {
        let p = build_pool(u);
        let mut c = p.get().await.unwrap();
        c.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate fresh shard db");
    }

    let src_pool = build_pool(&s0_url);
    let tgt_pool = build_pool(&s1_url);
    install_global_router(ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    ));
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), src_pool.clone());
    pools.insert(ShardId::new(1), tgt_pool.clone());
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    // The relay resolves the queue for a nonzero target shard on the DEFAULT
    // shard's schedules (schedules live on the default shard) — here shard 0.
    // Insert ag_target_wf -> 'real-q' there so the relay resolves 'real-q'.
    {
        let mut c0 = src_pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_schedules (id, workflow_name, queue_name, schedule_expr) \
             VALUES (gen_random_uuid(), 'ag_target_wf', 'real-q', '@daily')",
        )
        .execute(&mut c0)
        .await
        .expect("insert default-shard schedule");
    }

    // SOURCE shard: the outbox row (NULL queue → relay resolves 'real-q') PLUS the
    // fires row inserted at inline evaluate time with outcome NULL (= "fired"). The
    // relay-time block must UPDATE this fires row to 'admission_blocked' so the
    // durable audit does not wrongly report a successful fire (issue #618, F-round10).
    let outbox_id = Uuid::new_v4();
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();
    let trigger_key_id = Uuid::new_v4();
    {
        let mut src_conn = src_pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_fires
                (source_exec_id, trigger_id, fired_at, outcome)
             VALUES ($1, $2, NOW(), NULL)",
        )
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert fires row (outcome NULL = fired)");
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_outbox
                (id, source_exec_id, trigger_id, target_shard, target_workflow_name,
                 target_workflow_id, target_input, priority, max_workflow_input_bytes)
             VALUES ($1, $2, $3, 1, 'ag_target_wf', 'ct-relay-block', '{}'::jsonb,
                     '0'::jsonb, 1048576)",
        )
        .bind::<diesel::sql_types::Uuid, _>(outbox_id)
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert outbox row");
    }

    // Raise a Queue('real-q') gate + publish the recorder via the global.
    set_global_admission_gate_cache(Some(scoped_cache(GateScope::Queue("real-q".to_string()))));
    let recorder = Arc::new(CapturingMetrics::default());
    set_global_admission_metrics(Some(
        Arc::clone(&recorder) as Arc<dyn autumn_harvest::telemetry::MetricsRecorder>
    ));

    // Fire the immediate relay with NO explicit queue → it resolves 'real-q'.
    let deferred = autumn_harvest::completion_trigger::DeferredTriggerStart {
        outbox_id,
        source_exec_id: src_exec_id,
        trigger_id: trigger_key_id,
        source_shard: ShardId::new(0),
        target_shard: ShardId::new(1),
        target_workflow_name: "ag_target_wf".to_string(),
        target_workflow_id: "ct-relay-block".to_string(),
        target_input: json!({}),
        queue_name: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 1_048_576,
        trigger_name: "ct_relay_block".to_string(),
        owner: None,
        runbook_url: None,
        severity: None,
        sla: None,
        retry_policy: None,
        max_workflow_attempts_ceiling: None,
    };
    deferred.spawn();

    // Poll for the block (gated on the delete, so a non-empty blocked list means
    // the block + row-drop + count all completed).
    let mut settled = false;
    for _ in 0..200 {
        if !recorder.blocked().is_empty() {
            settled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Scanner over the source shard: the row was dropped by the immediate relay,
    // so it processes nothing (no double-block, no bypass).
    let scanner_metrics: &(dyn MetricsRecorder + Send + Sync) = &*recorder;
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut src_pool.get().await.unwrap(),
        scanner_metrics,
        &Some(sharded.clone()),
        &[ShardId::new(0)],
    )
    .await
    .unwrap();

    let target_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(&mut tgt_pool.get().await.unwrap())
    .await
    .expect("count target")
    .n;
    let outbox_remaining: i64 =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox")
            .get_result::<CountRow>(&mut src_pool.get().await.unwrap())
            .await
            .expect("count outbox")
            .n;
    // The fires row's outcome must have been flipped to 'admission_blocked' by the
    // relay-time block (issue #618, F-round10) — atomically with dropping the outbox
    // row. It was inserted with NULL (= fired); a NULL here would be a wrong audit.
    let fires_outcome: Option<String> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires
         WHERE source_exec_id = $1 AND trigger_id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .get_result::<OutcomeRow>(&mut src_pool.get().await.unwrap())
    .await
    .expect("read fires outcome")
    .outcome;

    set_global_admission_gate_cache(None);
    set_global_admission_metrics(None);
    install_global_router(ShardRouter::default());
    let _ = ShardedDbPool::single(build_pool(&base_url));
    drop(sharded);
    drop(src_pool);
    drop(tgt_pool);
    {
        let base_pool = build_pool(&base_url);
        if let Ok(mut admin) = base_pool.get().await {
            for name in [&s0_name, &s1_name] {
                let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await;
            }
        }
    }

    assert!(
        settled,
        "the relay-time gate block did not complete within the poll window"
    );
    assert_eq!(
        target_count, 0,
        "the relay must BLOCK on the real-queue gate — the target is NOT started"
    );
    assert_eq!(
        outbox_remaining, 0,
        "the blocked relay dropped the outbox row"
    );
    assert_eq!(
        relayed, 0,
        "the scanner had nothing to relay (row already dropped)"
    );
    let blocked = recorder.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the relay-time block is counted exactly once"
    );
    assert_eq!(blocked[0].0, "queue");
    assert!(
        recorder.bypassed().is_empty(),
        "a blocked relay must NOT also count a bypass"
    );
    assert_eq!(
        fires_outcome.as_deref(),
        Some("admission_blocked"),
        "the relay-time block must flip the fires row from NULL (fired) to \
         'admission_blocked' so the durable audit matches what actually happened"
    );
}

/// Issue #618, F-round10: the SCANNER relay-block path
/// (`enforce_completion_triggers_outbox`) must, like the immediate `spawn()` path,
/// flip the existing fires row from NULL (= fired) to `admission_blocked` when it
/// drops the outbox row under an active gate — atomically with the delete — so the
/// durable audit never reports a fired target that was actually dropped. Confirmed
/// red first: before F-round10 the scanner deleted the outbox row but left the
/// fires row NULL.
#[tokio::test]
async fn scanner_relay_block_marks_fires_row_admission_blocked() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    // A Fleet gate raised AFTER the outbox + fires rows were written (the classic
    // "gate raised while the relay is in flight" race).
    let cache = fleet_cache("scanner-block-incident");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let metrics = CapturingMetrics::default();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &metrics;

    let trigger_id = Uuid::new_v4();
    insert_trigger(&mut conn, trigger_id).await;
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();

    // Inline evaluate wrote both rows on the source shard: the fires row (outcome
    // NULL = fired) and the outbox row keyed by the same (source_exec_id, trigger_id).
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_fires
            (source_exec_id, trigger_id, fired_at, outcome)
         VALUES ($1, $2, NOW(), NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox
            (source_exec_id, trigger_id, target_shard, target_workflow_name,
             target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
         VALUES ($1, $2, 0, 'ag_target_wf', 'ct-scanner-block', '{}'::jsonb, 'default',
                 '0'::jsonb, 1048576)",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .unwrap();

    let sharded = ShardedDbPool::single(pool.clone());
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut conn,
        metrics_ref,
        &Some(sharded),
        &[ShardId::new(0)],
    )
    .await
    .unwrap();

    set_global_admission_gate_cache(None);

    // The scanner processed the row (dropped it) — a BLOCK, not a bypass.
    assert_eq!(
        relayed, 1,
        "the scanner processes the row (blocked + dropped)"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "the blocked scanner relay must NOT start the target"
    );
    let outbox_remaining: i64 =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox")
            .get_result::<CountRow>(&mut conn)
            .await
            .unwrap()
            .n;
    assert_eq!(
        outbox_remaining, 0,
        "the blocked scanner relay dropped the outbox row"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the scanner block is counted exactly once"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert!(
        metrics.bypassed().is_empty(),
        "a blocked scanner relay must NOT also count a bypass"
    );
    let fires_outcome: Option<String> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires
         WHERE source_exec_id = $1 AND trigger_id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .get_result::<OutcomeRow>(&mut conn)
    .await
    .expect("read fires outcome")
    .outcome;
    assert_eq!(
        fires_outcome.as_deref(),
        Some("admission_blocked"),
        "the scanner relay-time block must flip the fires row from NULL (fired) to \
         'admission_blocked' — atomically with dropping the outbox row"
    );
}

/// Issue #618, F-round13: an existence race mirroring F-round12, for the
/// completion-trigger cross-shard relay. A STALE outbox row — the immediate
/// `spawn()` relay started the target on the target shard but then failed to
/// delete the source outbox row — is retried by the scanner. If an admission gate
/// is raised in the meantime, the pre-F-round13 relay-time block branch would drop
/// the row, mark the fires row `admission_blocked`, and count a block WITHOUT
/// checking that the target ALREADY exists — reporting a successfully-started
/// target as dropped by the gate.
///
/// After F-round13 the relay routes through `gate_checked_start_or_load`, which
/// locks the target's existence on the target shard: the locked-live target →
/// `Started { created: false }` → DELIVERED (delete outbox + count bypass), NOT
/// blocked. The fires row stays NULL (a real fire), no block is counted, and no
/// second target run is created.
///
/// Two-DB multi-shard (source shard 0 owns the outbox row; target shard 1 owns the
/// already-started run). Confirmed red first: against pre-F-round13 code the
/// scanner blocks the stale row (target reported dropped) instead of delivering it.
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn scanner_delivers_a_stale_row_whose_target_already_exists() {
    use diesel_async::SimpleAsyncConnection;
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (base_url, _c) = setup_db().await;

    // source shard 0 (owns the outbox + fires rows), target shard 1 (already has
    // the started run).
    let s0_name = format!("ag618_stsrc_{}", Uuid::new_v4().simple());
    let s1_name = format!("ag618_sttgt_{}", Uuid::new_v4().simple());
    {
        let base_pool = build_pool(&base_url);
        let mut admin = base_pool.get().await.unwrap();
        for name in [&s0_name, &s1_name] {
            diesel::sql_query(format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await
                .expect("create shard db");
        }
    }
    let s0_url = swap_db_name(&base_url, &s0_name);
    let s1_url = swap_db_name(&base_url, &s1_name);
    for u in [&s0_url, &s1_url] {
        let p = build_pool(u);
        let mut c = p.get().await.unwrap();
        c.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate fresh shard db");
    }

    let src_pool = build_pool(&s0_url);
    let tgt_pool = build_pool(&s1_url);
    install_global_router(ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    ));
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), src_pool.clone());
    pools.insert(ShardId::new(1), tgt_pool.clone());
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    // The target ALREADY exists on shard 1 (a live RUNNING run) — as if the
    // immediate relay started it but failed to delete the source outbox row.
    {
        let mut tgt_conn = tgt_pool.get().await.unwrap();
        start_or_load_workflow_execution(
            &mut tgt_conn,
            StartWorkflowParams {
                workflow_name: "ag_target_wf",
                workflow_id: "ct-stale-delivered",
                exec_id: ExecutionId::new_for_shard(ShardId::new(1)),
                input: json!({}),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                chain_execution_timeout: None,
                max_workflow_chain_timeout_ceiling: None,
                inherited_chain_deadline_at: None,
                concurrency_key: None,
                concurrency_limit: None,
                priority: autumn_harvest::types::Priority::default(),
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
                start_source: autumn_harvest::StartSource::Api,
                start_source_ref: None,
                started_by: None,
            },
            None,
        )
        .await
        .expect("pre-create the already-started target run");
    }

    // The stale outbox row + its fires row (outcome NULL = fired) on the SOURCE.
    let outbox_id = Uuid::new_v4();
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();
    let trigger_key_id = Uuid::new_v4();
    {
        let mut src_conn = src_pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_fires
                (source_exec_id, trigger_id, fired_at, outcome)
             VALUES ($1, $2, NOW(), NULL)",
        )
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert fires row");
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_outbox
                (id, source_exec_id, trigger_id, target_shard, target_workflow_name,
                 target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
             VALUES ($1, $2, $3, 1, 'ag_target_wf', 'ct-stale-delivered', '{}'::jsonb,
                     'default', '0'::jsonb, 1048576)",
        )
        .bind::<diesel::sql_types::Uuid, _>(outbox_id)
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert stale outbox row");
    }

    // A Fleet gate that WOULD block a fresh start — raised while the stale row was
    // waiting to be retried.
    let cache = fleet_cache("stale-delivered-incident");
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let recorder = Arc::new(CapturingMetrics::default());
    let scanner_metrics: &(dyn MetricsRecorder + Send + Sync) = &*recorder;
    // The scanner reads the outbox on the SOURCE shard's connection and filters by
    // `target_shard IN assignments`; the stale row targets shard 1, so the worker
    // assignments must include shard 1 for it to be picked up.
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut src_pool.get().await.unwrap(),
        scanner_metrics,
        &Some(sharded.clone()),
        &[ShardId::new(1)],
    )
    .await
    .unwrap();

    // Read final state before tearing down.
    let target_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(&mut tgt_pool.get().await.unwrap())
    .await
    .expect("count target")
    .n;
    let outbox_remaining: i64 =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox")
            .get_result::<CountRow>(&mut src_pool.get().await.unwrap())
            .await
            .expect("count outbox")
            .n;
    let fires_outcome: Option<String> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires
         WHERE source_exec_id = $1 AND trigger_id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .get_result::<OutcomeRow>(&mut src_pool.get().await.unwrap())
    .await
    .expect("read fires outcome")
    .outcome;

    // Cleanup globals + drop the per-shard databases.
    set_global_admission_gate_cache(None);
    install_global_router(ShardRouter::default());
    let _ = ShardedDbPool::single(build_pool(&base_url));
    drop(sharded);
    drop(src_pool);
    drop(tgt_pool);
    {
        let base_pool = build_pool(&base_url);
        if let Ok(mut admin) = base_pool.get().await {
            for name in [&s0_name, &s1_name] {
                let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await;
            }
        }
    }

    assert_eq!(
        relayed, 1,
        "the scanner processes the stale row (delivered + dropped)"
    );
    assert_eq!(
        target_count, 1,
        "the target already existed and must NOT be started a second time"
    );
    assert_eq!(
        outbox_remaining, 0,
        "the delivered stale row's outbox row is dropped on the source shard"
    );
    assert!(
        recorder.blocked().is_empty(),
        "a stale row whose target ALREADY exists must NOT be counted as a block"
    );
    assert_eq!(
        recorder.bypassed(),
        vec!["completion_trigger_outbox".to_string()],
        "a delivered stale row counts the completion_trigger_outbox bypass exactly once"
    );
    assert_eq!(
        fires_outcome, None,
        "a delivered stale row leaves the fires row a real fire (NULL), never \
         'admission_blocked'"
    );
}

/// Observations from one stale-sealed-target relay case.
struct SealedCaseOutcome {
    relayed: usize,
    target_count: i64,
    outbox_remaining: i64,
    blocked_empty: bool,
    bypassed: Vec<String>,
    fires_outcome: Option<String>,
}

/// Drive the cross-shard completion-trigger scanner against a stale outbox row
/// whose deterministic target `W` was pre-created and then transitioned to
/// `seal_state` (a state the active-only existence lock excludes). Optionally
/// raise a fleet gate that WOULD block a fresh start. Creates + tears down a fresh
/// source/target DB pair. F-round15.
#[allow(clippy::too_many_lines)]
async fn run_stale_sealed_delivered_case(
    base_url: &str,
    seal_state: &str,
    with_gate: bool,
) -> SealedCaseOutcome {
    use diesel_async::SimpleAsyncConnection;

    let s0_name = format!("ag618_selsrc_{}", Uuid::new_v4().simple());
    let s1_name = format!("ag618_seltgt_{}", Uuid::new_v4().simple());
    {
        let base_pool = build_pool(base_url);
        let mut admin = base_pool.get().await.unwrap();
        for name in [&s0_name, &s1_name] {
            diesel::sql_query(format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await
                .expect("create shard db");
        }
    }
    let s0_url = swap_db_name(base_url, &s0_name);
    let s1_url = swap_db_name(base_url, &s1_name);
    for u in [&s0_url, &s1_url] {
        let p = build_pool(u);
        let mut c = p.get().await.unwrap();
        c.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate fresh shard db");
    }

    let src_pool = build_pool(&s0_url);
    let tgt_pool = build_pool(&s1_url);
    install_global_router(ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    ));
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), src_pool.clone());
    pools.insert(ShardId::new(1), tgt_pool.clone());
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    // Pre-create the target run on shard 1 (as if the immediate relay started it),
    // then SEAL it — the state the active-only existence lock excludes, simulating a
    // target that finished before the stale row was retried.
    {
        let mut tgt_conn = tgt_pool.get().await.unwrap();
        start_or_load_workflow_execution(
            &mut tgt_conn,
            StartWorkflowParams {
                workflow_name: "ag_target_wf",
                workflow_id: "ct-stale-sealed",
                exec_id: ExecutionId::new_for_shard(ShardId::new(1)),
                input: json!({}),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                chain_execution_timeout: None,
                max_workflow_chain_timeout_ceiling: None,
                inherited_chain_deadline_at: None,
                concurrency_key: None,
                concurrency_limit: None,
                priority: autumn_harvest::types::Priority::default(),
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
                start_source: autumn_harvest::StartSource::Api,
                start_source_ref: None,
                started_by: None,
            },
            None,
        )
        .await
        .expect("pre-create the already-started target run");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state = $1, completed_at = NOW() \
             WHERE workflow_name = 'ag_target_wf' AND workflow_id = 'ct-stale-sealed'",
        )
        .bind::<diesel::sql_types::Text, _>(seal_state)
        .execute(&mut tgt_conn)
        .await
        .expect("seal the pre-created target run");
    }

    // The stale outbox row + its fires row (outcome NULL = fired) on the SOURCE.
    let outbox_id = Uuid::new_v4();
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();
    let trigger_key_id = Uuid::new_v4();
    {
        let mut src_conn = src_pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_fires
                (source_exec_id, trigger_id, fired_at, outcome)
             VALUES ($1, $2, NOW(), NULL)",
        )
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert fires row");
        diesel::sql_query(
            "INSERT INTO harvest_completion_trigger_outbox
                (id, source_exec_id, trigger_id, target_shard, target_workflow_name,
                 target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
             VALUES ($1, $2, $3, 1, 'ag_target_wf', 'ct-stale-sealed', '{}'::jsonb,
                     'default', '0'::jsonb, 1048576)",
        )
        .bind::<diesel::sql_types::Uuid, _>(outbox_id)
        .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
        .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
        .execute(&mut src_conn)
        .await
        .expect("insert stale outbox row");
    }

    if with_gate {
        let cache = fleet_cache("stale-sealed-incident");
        set_global_admission_gate_cache(Some(Arc::clone(&cache)));
    } else {
        set_global_admission_gate_cache(None);
    }

    let recorder = Arc::new(CapturingMetrics::default());
    let scanner_metrics: &(dyn MetricsRecorder + Send + Sync) = &*recorder;
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut src_pool.get().await.unwrap(),
        scanner_metrics,
        &Some(sharded.clone()),
        &[ShardId::new(1)],
    )
    .await
    .unwrap();

    let target_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(&mut tgt_pool.get().await.unwrap())
    .await
    .expect("count target")
    .n;
    let outbox_remaining: i64 =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox")
            .get_result::<CountRow>(&mut src_pool.get().await.unwrap())
            .await
            .expect("count outbox")
            .n;
    let fires_outcome: Option<String> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires
         WHERE source_exec_id = $1 AND trigger_id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .get_result::<OutcomeRow>(&mut src_pool.get().await.unwrap())
    .await
    .expect("read fires outcome")
    .outcome;

    // Cleanup globals + drop the per-shard databases.
    set_global_admission_gate_cache(None);
    install_global_router(ShardRouter::default());
    let _ = ShardedDbPool::single(build_pool(base_url));
    drop(sharded);
    drop(src_pool);
    drop(tgt_pool);
    {
        let base_pool = build_pool(base_url);
        if let Ok(mut admin) = base_pool.get().await {
            for name in [&s0_name, &s1_name] {
                let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await;
            }
        }
    }

    SealedCaseOutcome {
        relayed,
        target_count,
        outbox_remaining,
        blocked_empty: recorder.blocked().is_empty(),
        bypassed: recorder.bypassed(),
        fires_outcome,
    }
}

/// F-round15: a stale cross-shard outbox row is a ONE-SHOT delivery marker. If its
/// deterministic target already ran and has since SEALED
/// (`TERMINATED`/`CONTINUED_AS_NEW`), the fire was already DELIVERED — the scanner
/// must treat it as delivered, NOT as a fresh (gated) admission. The round-13 fix
/// routed the relay through `gate_checked_start_or_load`, whose existence lock is
/// active-only, so a sealed target was invisible to it: under a matching gate it
/// BLOCKED (marking the fires row `admission_blocked`) and with no gate a sealed
/// target let `start_or_load` create a SECOND target run — both re-doing an
/// already-delivered fire. The any-state existence check in
/// `relay_gate_checked_start` closes this.
///
/// Two-DB multi-shard, three cases: `TERMINATED` + active gate, `TERMINATED` + no
/// gate, `CONTINUED_AS_NEW` + active gate. Confirmed red first: against the
/// pre-F-round15 code the gate cases mark the fires row `admission_blocked`/count a
/// block, and the no-gate case creates a second target run (`target_count == 2`).
#[tokio::test]
#[allow(clippy::similar_names)]
async fn scanner_delivers_a_stale_row_whose_target_has_sealed() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (base_url, _c) = setup_db().await;

    // Case 1: TERMINATED target under an active matching gate → delivered, not blocked.
    let terminated_gated = run_stale_sealed_delivered_case(&base_url, "TERMINATED", true).await;
    assert_eq!(
        terminated_gated.relayed, 1,
        "TERMINATED+gate: the scanner processes the stale row (delivered + dropped)"
    );
    assert_eq!(
        terminated_gated.target_count, 1,
        "TERMINATED+gate: the target already ran — no second run is created"
    );
    assert_eq!(
        terminated_gated.outbox_remaining, 0,
        "TERMINATED+gate: the delivered stale row's outbox row is dropped"
    );
    assert!(
        terminated_gated.blocked_empty,
        "TERMINATED+gate: a delivered (already-sealed) target must NOT be counted as a block"
    );
    assert_eq!(
        terminated_gated.bypassed,
        vec!["completion_trigger_outbox".to_string()],
        "TERMINATED+gate: a delivered stale row counts the bypass exactly once"
    );
    assert_eq!(
        terminated_gated.fires_outcome, None,
        "TERMINATED+gate: the fires row stays a real fire (NULL), never 'admission_blocked'"
    );

    // Case 2: TERMINATED target with NO gate → delivered, no second run (the RED
    // signal here is a SECOND target run created by start_or_load on a released
    // sealed key).
    let terminated_nogate = run_stale_sealed_delivered_case(&base_url, "TERMINATED", false).await;
    assert_eq!(
        terminated_nogate.relayed, 1,
        "TERMINATED+no-gate: the scanner processes the stale row"
    );
    assert_eq!(
        terminated_nogate.target_count, 1,
        "TERMINATED+no-gate: a sealed target must NOT be re-started — no second run"
    );
    assert_eq!(
        terminated_nogate.outbox_remaining, 0,
        "TERMINATED+no-gate: the delivered stale row's outbox row is dropped"
    );
    assert!(
        terminated_nogate.blocked_empty,
        "TERMINATED+no-gate: no gate, no block"
    );
    assert_eq!(
        terminated_nogate.bypassed,
        vec!["completion_trigger_outbox".to_string()],
        "TERMINATED+no-gate: a delivered stale row counts the bypass exactly once"
    );
    assert_eq!(terminated_nogate.fires_outcome, None);

    // Case 3: CONTINUED_AS_NEW target under an active matching gate → delivered.
    let can_gated = run_stale_sealed_delivered_case(&base_url, "CONTINUED_AS_NEW", true).await;
    assert_eq!(can_gated.relayed, 1, "CONTINUED_AS_NEW+gate: processed");
    assert_eq!(
        can_gated.target_count, 1,
        "CONTINUED_AS_NEW+gate: the target already ran — no second run"
    );
    assert_eq!(can_gated.outbox_remaining, 0);
    assert!(
        can_gated.blocked_empty,
        "CONTINUED_AS_NEW+gate: a delivered (already-sealed) target must NOT be counted as a block"
    );
    assert_eq!(
        can_gated.bypassed,
        vec!["completion_trigger_outbox".to_string()],
        "CONTINUED_AS_NEW+gate: bypass counted exactly once"
    );
    assert_eq!(
        can_gated.fires_outcome, None,
        "CONTINUED_AS_NEW+gate: the fires row stays a real fire (NULL)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// F-round12: gate_checked_start_or_load — close the webhook idempotent-retry
// TOCTOU (an existing run sealing between an unlocked existence check and the
// start would let a fresh replacement slip an active gate uncounted).
// ─────────────────────────────────────────────────────────────────────────────

/// Start a `webhook_delivery` run (RUNNING) with the given deterministic id.
async fn start_webhook_delivery(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "webhook_delivery",
            workflow_id,
            exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "webhooks",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: autumn_harvest::types::Priority::default(),
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start webhook_delivery");
    exec_id
}

/// Build `StartWorkflowParams` for a FRESH `webhook_delivery` replacement start
/// (the params the delegate passes to `gate_checked_start_or_load`).
fn webhook_replacement_params(workflow_id: &'static str) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        workflow_name: "webhook_delivery",
        workflow_id,
        exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
        input: json!({}),
        parent_id: None,
        queue_name: "webhooks",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

async fn running_webhook_delivery_count(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions
         WHERE workflow_name = 'webhook_delivery' AND state = 'RUNNING'",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count running webhook_delivery")
    .n
}

/// The idempotent-retry (non-racy) case round 9 handles: a LIVE existing run under
/// an active gate is ATTACHED (idempotent load), the gate is SKIPPED, and no fresh
/// run is created — `gate_checked_start_or_load` must not block a non-admission.
#[tokio::test]
async fn gate_checked_start_attaches_to_a_live_run_and_skips_the_gate() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let original = start_webhook_delivery(&mut conn, "webhook-delivery-live").await;
    set_global_admission_gate_cache(Some(fleet_cache("attach-incident")));

    let outcome = autumn_harvest::start_or_load_workflow_execution_with_metrics(
        &mut conn,
        webhook_replacement_params("webhook-delivery-live"),
        None,
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Ok(started) => {
            assert!(
                !started.created,
                "a live existing run must be ATTACHED (created == false), not replaced"
            );
            assert_eq!(
                started.exec_id, original,
                "the attach must return the original run's exec_id"
            );
        }
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => {
            panic!("an idempotent retry of a LIVE run must NOT be blocked by the gate")
        }
        Err(e) => panic!("unexpected start error: {e}"),
    }
    // Still exactly one RUNNING webhook_delivery run (the original) — no fresh start.
    assert_eq!(running_webhook_delivery_count(&mut conn).await, 1);
}

/// The TOCTOU the round-9 unlocked check left open, now closed: a run that is LIVE
/// when the delegate is invoked but SEALS (TERMINATED) before the start decision
/// commits. `gate_checked_start_or_load` locks the run through the decision, so it
/// observes the SEALED state and BLOCKS the fresh replacement under the active gate
/// — never a silent uncounted admission.
///
/// Choreography (the repo's `hold_execution_row_lock` pattern): a holder connection
/// takes `FOR UPDATE` on the RUNNING run (a terminate about to seal it). We spawn
/// `gate_checked_start_or_load`; its own `FOR UPDATE` existence load queues BEHIND
/// the holder. The holder then seals the row and commits, releasing the lock; the
/// spawned start's locked load now observes TERMINATED (excluded from the active
/// filter) → `locked_live = false` → the gate applies → BLOCK, no fresh start.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn gate_checked_start_blocks_a_run_that_seals_under_the_lock() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    let exec_id = start_webhook_delivery(&mut conn, "webhook-delivery-race").await;
    set_global_admission_gate_cache(Some(fleet_cache("race-incident")));

    // Holder: hold FOR UPDATE on the RUNNING run (a terminate that has locked it).
    let mut holder = pool.get().await.unwrap();
    {
        use diesel_async::SimpleAsyncConnection;
        holder.batch_execute("BEGIN").await.unwrap();
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(&mut holder)
            .await
            .unwrap();
    }

    // Spawn the gate-checked start; its locked existence load queues behind holder.
    let start_pool = pool.clone();
    let start_task = tokio::spawn(async move {
        let mut a_conn = start_pool.get().await.unwrap();
        autumn_harvest::start_or_load_workflow_execution_with_metrics(
            &mut a_conn,
            webhook_replacement_params("webhook-delivery-race"),
            None,
            Some(autumn_harvest::admission_gate::GateMode::Check),
        )
        .await
    });

    // Give the spawned start time to reach (and block on) its FOR UPDATE lock load.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Holder seals the run and commits, releasing the lock.
    {
        use diesel_async::SimpleAsyncConnection;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions
             SET state = 'TERMINATED', completed_at = NOW() WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut holder)
        .await
        .unwrap();
        holder.batch_execute("COMMIT").await.unwrap();
    }

    let outcome = start_task.await.expect("join start task");

    set_global_admission_gate_cache(None);

    // The fresh replacement (which AllowDuplicate WOULD create for a TERMINATED
    // prior) must be BLOCKED by the gate — the locked decision saw TERMINATED.
    match outcome {
        Err(autumn_harvest::HarvestError::AdmissionBlocked { reason, .. }) => {
            assert!(
                reason.contains("race-incident"),
                "the block came from the Fleet gate (reason = {reason})"
            );
        }
        Ok(started) => panic!(
            "a run sealed under the lock must BLOCK the fresh replacement, not start it \
             (created = {}, exec = {:?}) — this is the round-9 TOCTOU leaking a fresh \
             admission past the gate",
            started.created, started.exec_id
        ),
        Err(e) => panic!("unexpected start error: {e}"),
    }
    // No fresh RUNNING run was created (the original is now TERMINATED).
    assert_eq!(
        running_webhook_delivery_count(&mut conn).await,
        0,
        "no fresh webhook_delivery run slipped the gate"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// F-round18: gate REPLACEMENT starts, don't treat them as idempotent attaches.
// `gate_checked_start_or_load` must apply the gate whenever `start_or_load` will
// CREATE a new execution (a fresh admission) — including a REPLACEMENT over a
// non-sealed prior (AllowDuplicateFailedOnly over FAILED/CANCELLED; any
// TerminateIfRunning) — and skip it only for a genuine ATTACH (AllowDuplicate to a
// live OR terminal prior). The prior-round `try_load_active_execution_for_update`
// returned `Some` for these replacement priors and (as `locked_live`) wrongly
// skipped the gate, leaking a fresh execution past an active gate uncounted.
// ─────────────────────────────────────────────────────────────────────────────

fn ag_target_params(
    workflow_id: &'static str,
    reuse_policy: WorkflowIdReusePolicy,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        workflow_name: "ag_target_wf",
        workflow_id,
        exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Seed a prior `ag_target_wf` run with `workflow_id`, then force it to `state`.
async fn seed_prior_ag_target(
    conn: &mut AsyncPgConnection,
    workflow_id: &'static str,
    state: &str,
) {
    start_or_load_workflow_execution(
        conn,
        ag_target_params(workflow_id, WorkflowIdReusePolicy::AllowDuplicate),
        None,
    )
    .await
    .expect("seed prior");
    if state != "RUNNING" {
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state = $1, completed_at = NOW()
             WHERE workflow_name = 'ag_target_wf' AND workflow_id = $2",
        )
        .bind::<Text, _>(state)
        .bind::<Text, _>(workflow_id)
        .execute(conn)
        .await
        .expect("force prior state");
    }
}

/// `AllowDuplicateFailedOnly` + a FAILED prior → `start_or_load` REPLACES (fresh
/// create). Under an active Fleet gate this fresh admission must be BLOCKED, and no
/// second execution created. (Pre-fix: `locked_live == true` → gate skipped → leak.)
#[tokio::test]
async fn gate_blocks_allow_duplicate_failed_only_replacement_of_a_failed_prior() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    seed_prior_ag_target(&mut conn, "adfo-failed", "FAILED").await;
    set_global_admission_gate_cache(Some(fleet_cache("replacement-incident")));

    let outcome = autumn_harvest::start_or_load_workflow_execution_with_metrics(
        &mut conn,
        ag_target_params(
            "adfo-failed",
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        ),
        None,
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => {}
        Ok(s) => panic!(
            "AllowDuplicateFailedOnly over a FAILED prior REPLACES (fresh create) — it must be \
             gated, not skipped as an attach (created = {}, exec = {:?})",
            s.created, s.exec_id
        ),
        Err(e) => panic!("unexpected start error: {e}"),
    }
    // Exactly the one seeded (FAILED) prior remains — no fresh replacement created.
    assert_eq!(target_exec_count(&mut conn).await, 1);
}

/// `TerminateIfRunning` + a live (RUNNING) prior → cancel + REPLACE (fresh create).
/// Under an active Fleet gate this must be BLOCKED. (Pre-fix: `locked_live == true` →
/// gate skipped → leak.)
#[tokio::test]
async fn gate_blocks_terminate_if_running_replacement_of_a_live_prior() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    seed_prior_ag_target(&mut conn, "tir-live", "RUNNING").await;
    set_global_admission_gate_cache(Some(fleet_cache("terminate-incident")));

    let outcome = autumn_harvest::start_or_load_workflow_execution_with_metrics(
        &mut conn,
        ag_target_params("tir-live", WorkflowIdReusePolicy::TerminateIfRunning),
        None,
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => {}
        Ok(s) => panic!(
            "TerminateIfRunning always REPLACES (fresh create) — it must be gated \
             (created = {}, exec = {:?})",
            s.created, s.exec_id
        ),
        Err(e) => panic!("unexpected start error: {e}"),
    }
    // The gate blocked before any replacement; exactly the one seeded run remains.
    assert_eq!(target_exec_count(&mut conn).await, 1);
}

/// `AllowDuplicate` + a terminal (COMPLETED) prior → ATTACH (returns the existing
/// terminal run, created == false; NO new admission). Unlike the `SignalWithStart`
/// matrix, the standalone `start_or_load` path this primitive calls does NOT
/// escalate a terminal-prior attach to a fresh start — so the gate must be SKIPPED
/// (gating an attach that admits nothing would wrongly block an idempotent retry and
/// inflate the block counter). This pins the corrected reuse-matrix behaviour and
/// guards against re-introducing a spurious block for the webhook delegate's own
/// `AllowDuplicate` policy over a terminal prior.
#[tokio::test]
async fn gate_skips_allow_duplicate_attach_to_a_completed_prior() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    seed_prior_ag_target(&mut conn, "ad-completed", "COMPLETED").await;
    set_global_admission_gate_cache(Some(fleet_cache("attach-terminal-incident")));

    let outcome = autumn_harvest::start_or_load_workflow_execution_with_metrics(
        &mut conn,
        ag_target_params("ad-completed", WorkflowIdReusePolicy::AllowDuplicate),
        None,
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Ok(s) => {
            assert!(
                !s.created,
                "AllowDuplicate over a terminal prior ATTACHES (created == false), not a fresh create"
            );
        }
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => panic!(
            "an AllowDuplicate attach to a terminal prior admits nothing — the gate must be \
             SKIPPED, not block the idempotent retry"
        ),
        Err(e) => panic!("unexpected start error: {e}"),
    }
    // Still exactly one execution (the COMPLETED prior) — no fresh run created.
    assert_eq!(target_exec_count(&mut conn).await, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// F-round19: the immediate spawn() relay and the scanner (and multi-replica
// scanners) must be MUTUALLY EXCLUSIVE on a source outbox row. Without a claim
// they could reach DIVERGENT decisions about the same row — one starts the target,
// the other (under a just-raised gate, the first's target start not yet visible)
// blocks it — durably recording an `admission_blocked` fire for a target that ran.
// The fix claims the source row FOR UPDATE SKIP LOCKED, held across the whole relay.
// ─────────────────────────────────────────────────────────────────────────────

/// Deterministic race reproduction with the repo's lock-holding harness: a holder
/// connection holds `FOR UPDATE` on the source outbox row (simulating the immediate
/// relay having CLAIMED it and being mid-flight starting the target). Under a
/// newly-raised Fleet gate, the scanner must SKIP the claimed row — NOT block it and
/// NOT mark the fires row `admission_blocked`.
///
/// Pre-fix (RED): the scanner reads the row with a plain SELECT (no claim), decides
/// `Blocked`, and its DELETE hangs on the holder's `FOR UPDATE` lock → the scanner
/// never completes (the bounded timeout fires). Fixed (GREEN): the scanner's own
/// `FOR UPDATE SKIP LOCKED` claim skips the held row immediately → completes, with no
/// block counted, the fires row left NULL, and the outbox row still present (owned by
/// the concurrent relay).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[allow(clippy::too_many_lines)]
async fn scanner_skips_a_source_row_claimed_by_a_concurrent_relay() {
    use diesel_async::SimpleAsyncConnection;
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());

    // A pending source outbox row (target shard 0) + its fires row (NULL = fired).
    let outbox_id = Uuid::new_v4();
    let src_exec_id = ExecutionId::new_for_shard(ShardId::new(0)).as_uuid();
    let trigger_key_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_fires
            (source_exec_id, trigger_id, fired_at, outcome)
         VALUES ($1, $2, NOW(), NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox
            (id, source_exec_id, trigger_id, target_shard, target_workflow_name,
             target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
         VALUES ($1, $2, $3, 0, 'ag_target_wf', 'ct-claimed-race', '{}'::jsonb,
                 'default', '0'::jsonb, 1048576)",
    )
    .bind::<diesel::sql_types::Uuid, _>(outbox_id)
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .execute(&mut conn)
    .await
    .unwrap();

    // A Fleet gate that WOULD block a fresh start.
    set_global_admission_gate_cache(Some(fleet_cache("claimed-race-incident")));

    // Holder: claim the source outbox row FOR UPDATE (the concurrent relay owns it,
    // mid-flight). The scanner must SKIP it (SKIP LOCKED), never block it.
    let mut holder = pool.get().await.unwrap();
    holder.batch_execute("BEGIN").await.unwrap();
    diesel::sql_query("SELECT id FROM harvest_completion_trigger_outbox WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(outbox_id)
        .execute(&mut holder)
        .await
        .unwrap();

    let sharded = ShardedDbPool::single(pool.clone());
    let recorder = Arc::new(CapturingMetrics::default());

    // Run the scanner in a task with a bounded timeout.
    let scan_pool = pool.clone();
    let scan_sharded = sharded.clone();
    let scan_recorder = Arc::clone(&recorder);
    let scan = tokio::spawn(async move {
        let mut scan_conn = scan_pool.get().await.unwrap();
        let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = &*scan_recorder;
        autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
            &mut scan_conn,
            metrics_ref,
            &Some(scan_sharded),
            &[ShardId::new(0)],
        )
        .await
    });
    let scan_result = tokio::time::timeout(std::time::Duration::from_secs(8), scan).await;

    // Read state WHILE the holder still owns the row (a plain SELECT does not block on
    // FOR UPDATE): the fixed scanner has skipped; the pre-fix scanner would be hung on
    // its DELETE with the row still present and the fires row still NULL.
    let fires_outcome: Option<String> = diesel::sql_query(
        "SELECT outcome FROM harvest_completion_trigger_fires
         WHERE source_exec_id = $1 AND trigger_id = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(src_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(trigger_key_id)
    .get_result::<OutcomeRow>(&mut conn)
    .await
    .unwrap()
    .outcome;
    let outbox_remaining: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_completion_trigger_outbox WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(outbox_id)
    .get_result::<CountRow>(&mut conn)
    .await
    .unwrap()
    .n;

    // Release the holder (the concurrent relay finishes).
    holder.batch_execute("COMMIT").await.unwrap();
    set_global_admission_gate_cache(None);

    // The fixed scanner completed (skipped the claimed row) within the timeout.
    let joined = scan_result.expect(
        "the scanner must COMPLETE within the timeout — pre-fix code (no claim) decides \
         Blocked and hangs on the claimed row's DELETE, which is the race this closes",
    );
    joined
        .expect("scan task join")
        .expect("scanner returned Ok");

    assert!(
        recorder.blocked().is_empty(),
        "the scanner must NOT count a block for a source row a concurrent relay owns \
         (the target is being delivered)"
    );
    assert_eq!(
        fires_outcome, None,
        "the fires row stays a real fire (NULL) — never 'admission_blocked' for a \
         claimed/delivered target"
    );
    assert_eq!(
        outbox_remaining, 1,
        "the scanner must NOT delete a row owned by the concurrent relay — it skips it"
    );
}

// ── issue #618 F2 (PR #1014 review): signal-/update-with-start gate the fresh
// create AUTHORITATIVELY under the primitive's lock ───────────────────────────
//
// Before this change the signal-/update-with-start HTTP routes gated only via an
// UNLOCKED pre-check and passed `gate = None` into the core start — the exact
// unlocked-pre-read pattern the plain `api` start route was moved OFF (it had a
// residual seal-under-lock / gate-raised-mid-request TOCTOU). A fresh create that
// slipped that window was an UN-COUNTED admission (no `admission_blocked`, no
// bypass), violating the "zero un-counted admissions" AC. Threading
// `Some(GateMode::Check)` into the core makes the fresh-create decision gated
// under the same `FOR UPDATE` lock the plain start route uses.
//
// These tests exercise the core directly with `Some(GateMode::Check)`: before the
// fix the `_with_metrics` fns did not accept a gate argument (compile-error red,
// per the repo's #593 red→green precedent); after the fix a fresh create under an
// active gate is BLOCKED + COUNTED and no execution row is created, while a fresh
// create with no gate proceeds (positive control).

fn signal_with_start_fresh_params(workflow_id: &'static str) -> SignalWithStartParams<'static> {
    SignalWithStartParams {
        workflow_name: "ag_target_wf",
        workflow_id,
        exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        signal_name: "go",
        signal_payload: json!({}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        reject_fresh_if_debounced: false,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        workflow_info: None,
        start_source_override: None,
        start_source_ref_override: None,
    }
}

fn update_with_start_fresh_params(workflow_id: &'static str) -> UpdateWithStartParams<'static> {
    UpdateWithStartParams {
        workflow_name: "ag_target_wf",
        workflow_id,
        exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        update_id: UpdateId::new(),
        update_name: "bump".to_string(),
        update_args: json!({}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
    }
}

/// A FRESH signal-with-start create (no prior run) under an active Fleet gate is
/// BLOCKED by the core's authoritative locked gate, records exactly one
/// `record_admission_blocked`, and leaves NO execution row. Closes the
/// un-counted-admission window a `gate = None` core left open (issue #618 F2).
#[tokio::test]
async fn gate_blocks_fresh_signal_with_start_create() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(Some(fleet_cache("sws-incident")));

    let recorder = CapturingMetrics::default();
    let outcome = signal_with_start_workflow_execution_with_metrics(
        &mut conn,
        signal_with_start_fresh_params("sws-fresh-blocked"),
        Some(&recorder),
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => {}
        Ok(o) => panic!(
            "a fresh signal-with-start create under an active gate must be BLOCKED, not \
             admitted (started_fresh={}, state={})",
            o.started_fresh, o.state
        ),
        Err(e) => panic!("unexpected error: {e}"),
    }
    assert_eq!(
        recorder.blocked().len(),
        1,
        "the block must be COUNTED exactly once on harvest.admission.blocked"
    );
    assert_eq!(
        target_exec_count_named(&mut conn, "ag_target_wf").await,
        0,
        "a blocked fresh signal-with-start must roll back with NO execution row"
    );
}

/// Positive control: with NO gate active a fresh signal-with-start create
/// proceeds and no block is recorded — the gate arg does not over-block.
#[tokio::test]
async fn signal_with_start_fresh_create_proceeds_without_gate() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(None);

    let recorder = CapturingMetrics::default();
    let outcome = signal_with_start_workflow_execution_with_metrics(
        &mut conn,
        signal_with_start_fresh_params("sws-fresh-ok"),
        Some(&recorder),
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await
    .expect("no gate: fresh signal-with-start must succeed");

    assert!(outcome.started_fresh, "must be a fresh create");
    assert!(
        recorder.blocked().is_empty(),
        "no gate active: no admission_blocked must be recorded"
    );
    assert_eq!(
        target_exec_count_named(&mut conn, "ag_target_wf").await,
        1,
        "the fresh run must be created"
    );
}

/// The update-with-start twin of `gate_blocks_fresh_signal_with_start_create`:
/// a FRESH update-with-start create under an active gate is BLOCKED + COUNTED
/// with no execution row (issue #618 F2, symmetric fix).
#[tokio::test]
async fn gate_blocks_fresh_update_with_start_create() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    install_global_router(ShardRouter::default());
    set_global_admission_gate_cache(Some(fleet_cache("uws-incident")));

    let recorder = CapturingMetrics::default();
    let outcome = update_with_start_workflow_execution_with_metrics(
        &mut conn,
        update_with_start_fresh_params("uws-fresh-blocked"),
        Some(&recorder),
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    set_global_admission_gate_cache(None);

    match outcome {
        Err(autumn_harvest::HarvestError::AdmissionBlocked { .. }) => {}
        Ok(o) => panic!(
            "a fresh update-with-start create under an active gate must be BLOCKED \
             (started_fresh={}, state={})",
            o.started_fresh, o.state
        ),
        Err(e) => panic!("unexpected error: {e}"),
    }
    assert_eq!(
        recorder.blocked().len(),
        1,
        "the block must be COUNTED once"
    );
    assert_eq!(
        target_exec_count_named(&mut conn, "ag_target_wf").await,
        0,
        "a blocked fresh update-with-start must roll back with NO execution row"
    );
}
