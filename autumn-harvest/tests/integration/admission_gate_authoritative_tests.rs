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

use std::collections::BTreeMap;
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

// The COMPLETE migration set, in timestamp order (identical to `diesel migration
// run`). The CI Docker step boots a fresh Postgres seeded from ONLY this const, so
// every table/column `start_or_load_workflow_execution` and the completion-trigger
// path read MUST be present here. A hand-picked subset silently rots as the start
// path gains column reads (issue #618: `harvest_build_policies` from build_routing
// AND `legal_hold_set_at` from a much later migration were both missing from an
// earlier subset), so this suite uses the full set to be drift-proof by
// construction — the applied superset never over-constrains a test. When a new
// migration lands, regenerate this block from `ls migrations/*/` in sorted order.
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000000_harvest_sticky_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260428000000_harvest_retention_scan_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
    "\n",
    include_str!("../../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260510000000_harvest_backfill_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514000000_drop_harvest_dag_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
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
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260611000001_harvest_stalled_workflow_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000000_harvest_workflow_list_keyset_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    include_str!("../../migrations/20260624000001_harvest_non_terminal_reachability_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260627000001_harvest_payload_refs/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000000_harvest_events_history_page_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    "\n",
    include_str!("../../migrations/20260702000000_harvest_usage_report_indexes/up.sql"),
    "\n",
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    "\n",
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    include_str!("../../migrations/20260706000001_harvest_start_throttle/up.sql"),
    "\n",
    include_str!(
        "../../migrations/20260707000000_harvest_start_throttle_bucket_deferred_idx/up.sql"
    ),
    "\n",
    include_str!("../../migrations/20260708000000_harvest_start_throttle_workflow_id_idx/up.sql"),
    "\n",
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    "\n",
    include_str!("../../migrations/20260708000002_harvest_schedule_runs_slot_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260709000000_harvest_start_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260709000001_harvest_legal_hold/up.sql"),
    "\n",
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
);

/// Per-workflow schedule columns the cross-shard test's fresh DBs need. These are
/// already present in `INIT_SQL` (the full `harvest_schedules`/`schedule_id`
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
/// databases seeded from `INIT_SQL`. The full evaluate → gate → block chain for a
/// cross-shard target additionally needs `start_or_load_workflow_execution`; that
/// path is covered by the same-shard block tests above (all now backed by the
/// full start-path `INIT_SQL`, which includes `harvest_build_policies`).
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
        c.batch_execute(INIT_SQL)
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
