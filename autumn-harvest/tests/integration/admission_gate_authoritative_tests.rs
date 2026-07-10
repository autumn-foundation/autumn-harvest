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
//! testcontainers Postgres is booted with `INIT_SQL`.

use std::sync::{Arc, Mutex};

use autumn_harvest::admission_gate::{
    AdmissionGate, AdmissionGateCache, AdmissionGateId, GateScope, set_global_admission_gate_cache,
};
use autumn_harvest::completion_trigger::{TerminalState, evaluate_triggers_for_execution};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
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
static TEST_SERIAL: Mutex<()> = Mutex::new(());

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    "\n",
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    // Scanner tables for the deferred-fire-producer bypass-counter test (F1/F6).
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    include_str!("../../migrations/20260706000001_harvest_start_throttle/up.sql"),
);

/// Capturing metrics recorder — records `record_admission_blocked` calls
/// (`scope_kind`, reason) so a test can assert a completion-trigger block is
/// counted identically to a direct API start.
#[derive(Default)]
struct CapturingMetrics {
    blocked: Mutex<Vec<(String, String)>>,
    fired: Mutex<Vec<(String, String)>>,
    bypassed: Mutex<Vec<String>>,
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
}

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
        .max_size(4)
        .build()
        .expect("pool build failed")
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
        // all listed tables are in INIT_SQL, so a real error should surface.
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
            trace_context: None,
            max_execution_timeout_ceiling: None,
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
        },
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
    exec_id
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

/// F3: a fail-closed / uninitialized gate cache (the transient gate-DB-blip
/// sentinel, `Uuid::nil()`) must NOT drop a completion-trigger start. It is
/// in-flight continuation of already-committed work, so evaluate PROCEEDS: the
/// target starts, no `admission_blocked` count, and the fires row is a real fire
/// (NULL outcome), not a block. A real operator gate still blocks (covered by
/// `completion_trigger_blocked_by_fleet_gate`).
#[tokio::test]
async fn completion_trigger_fail_closed_sentinel_proceeds() {
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

    // Install a fail-closed (uninitialized) cache: check() returns the sentinel.
    set_global_admission_gate_cache(Some(Arc::new(AdmissionGateCache::new_fail_closed())));

    let metrics = CapturingMetrics::default();
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("evaluate must proceed under the fail-closed sentinel");

    set_global_admission_gate_cache(None);

    // The target STARTED (not dropped) — in-flight continuation is preserved.
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "the fail-closed sentinel must NOT drop the completion-trigger start"
    );
    // No admission block was counted.
    assert!(
        metrics.blocked().is_empty(),
        "the fail-closed sentinel must not record an admission_blocked"
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
        "a sentinel-proceed is a real fire (NULL outcome), not admission_blocked"
    );
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

/// F1/F6: the four deferred-fire producers (`debounce`, `throttle`,
/// `event_batch`, and the completion-trigger cross-shard relay) each relay a
/// start that was admitted BEFORE a gate was raised — a leak past an active
/// gate. They are exempt-with-bypass-counter: with a Fleet gate active, each
/// scanner still fires (starts the target) AND increments
/// `harvest.admission.bypassed{producer}` so no admission is un-counted. This is
/// the falsifiable "zero un-counted admissions" bar for the scanner producers,
/// running Docker-backed in CI.
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

    // Raise a Fleet gate: it must NOT stop these deferred-fire scanners (their
    // start was already admitted before the gate), but each fire is counted.
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
    assert_eq!(relayed, 1, "CT cross-shard relay fires despite the gate");

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
    assert_eq!(thr, 1, "throttle scanner fires despite the gate");

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

    // Each deferred-fire producer counted its bypass — zero un-counted admissions.
    let mut bypassed = metrics.bypassed();
    bypassed.sort();
    let mut expected = vec![
        "completion_trigger_outbox".to_string(),
        "debounce".to_string(),
        "event_batch".to_string(),
        "throttle".to_string(),
    ];
    expected.sort();
    assert_eq!(bypassed, expected, "every scanner fire must count a bypass");
    // No admission was blocked (the scanners are exempt, not gated).
    assert!(
        metrics.blocked().is_empty(),
        "deferred-fire scanners are exempt, not blocked"
    );
    // All four targets started under the active gate.
    assert_eq!(target_exec_count(&mut conn).await, 4);
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
