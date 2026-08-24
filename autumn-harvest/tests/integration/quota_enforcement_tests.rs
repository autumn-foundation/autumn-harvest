//! Per-tenant resource quota admission tests — issue #946.
//!
//! # AC coverage map
//!
//! - **AC1** (dot-path key resolver reuse, no second resolver) — every test
//!   below resolves its tenant key via a plain `"tenant_id"` expression,
//!   exercised through the SAME [`autumn_harvest::quota::resolve_quota_key`]
//!   (a one-line delegate to [`autumn_harvest::concurrency::resolve_concurrency_key`])
//!   the admission path itself calls; no test constructs a key any other way.
//! - **AC2** (≥3 independent optional caps) —
//!   [`active_executions_cap_admits_exactly_n_then_rejects_the_next`] (money
//!   test), [`history_bytes_cap_rejects_once_exceeded`],
//!   [`dead_letters_cap_rejects_once_reached`], and
//!   [`policy_with_no_caps_declared_is_a_noop`] each exercise ONE resource in
//!   isolation from the other two (a policy declaring only that resource's
//!   `with_max_*`), proving the three caps are independent, not a single
//!   combined budget.
//! - **AC3** (enforcement at admission, before `WorkflowStarted`, on every
//!   registry-aware start path) — the direct-admission tests below drive the
//!   real [`start_or_load_workflow_execution`] entry point; the
//!   `continue_as_new_*` tests drive a genuine worker end to end, proving the
//!   SAME `quota_key` resolution that governs a fresh start also governs an
//!   in-flight continuation's successor row. Batch-start, schedule
//!   tick/backfill, and debounce/throttle scanner-fire coverage is tracked
//!   separately (issue #946 Task 7) — every one of those paths funnels
//!   through the identical `start_or_load_workflow_execution_collect` choke
//!   point this file exercises directly, so enforcement there is structural,
//!   not per-call-site.
//! - **AC4** (typed error, never silent, never a `500`) — every rejection
//!   test destructures the exact
//!   [`autumn_harvest::error::HarvestError::QuotaExceeded`] shape
//!   (`workflow_name`/`key`/`resource`/`limit`/`current`), and
//!   [`rejected_start_creates_no_execution_or_task_row`] proves the
//!   rejection rolls back atomically — no phantom execution or task row
//!   survives a rejected attempt.
//! - **AC7** ("cheap by construction… never a full-table scan per
//!   admission") — exercised transitively (every admission attempt here
//!   drives the real, single-round-trip `QUOTA_USAGE_SQL` query); the query
//!   shape itself is asserted directly in `quota.rs`'s own unit tests.
//! - **AC8** (shard-local scope) — out of scope for a single-shard suite;
//!   documented in `docs/sharding.md`.
//! - **AC9** (no-policy workflow byte-for-byte unchanged, zero default
//!   overhead) — [`no_policy_workflow_is_unaffected`].
//!
//! Deliberately **not** covered here, per issue #946's own scope split: the
//! HTTP `429` mapping, the `harvest.quota.rejected` metric, and
//! `GET /admin/quotas` (Task 6, plugin-layer); the literal 10,000-start
//! success-metric load test and the batch/schedule/debounce/throttle
//! path-by-path coverage sweep (Task 7).

#![cfg(feature = "db")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use autumn_harvest::completion_trigger::{GLOBAL_WORKFLOW_METADATA, WorkflowMetadata};
use autumn_harvest::dlq::{NewDeadLetterEntry, dead_letter};
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::quota::{QuotaPolicy, QuotaResource};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{
    ExecutionId, Priority, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{WorkflowContext, WorkflowInfo};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test database")
}

/// A unique workflow-type name per test — the registry and
/// [`GLOBAL_WORKFLOW_METADATA`] are process-global, so every test needs its
/// own namespace to avoid cross-test interference.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

fn wf_meta(quota: QuotaPolicy) -> WorkflowMetadata {
    WorkflowMetadata {
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        input_schema: None,
        sla: None,
        retry_policy: None,
        quota: Some(quota),
    }
}

/// Serializes access to the process-global [`GLOBAL_WORKFLOW_METADATA`]
/// mirror across this file's tests. CI runs `linux` integration suites with
/// `--test-threads=1` (see `.github/ci/integration-suites.txt`), so this is
/// primarily a local-`cargo test`-without-that-flag safeguard, mirroring the
/// `TEST_SERIAL` convention already used by `completion_callback_tests.rs`.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// RAII installer for [`GLOBAL_WORKFLOW_METADATA`]: installs the given map,
/// and restores whatever was there before on drop — including on a mid-test
/// panic, unlike a bare manual take/restore pair.
struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install(map: HashMap<String, WorkflowMetadata>) -> Self {
        let permit = TEST_SERIAL.lock().await;
        let previous = {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            lock.take()
        };
        {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            *lock = Some(map);
        }
        Self {
            previous,
            _permit: permit,
        }
    }

    /// Convenience for the common single-workflow-type case.
    async fn install_one(workflow_name: &'static str, quota: QuotaPolicy) -> Self {
        let mut map = HashMap::new();
        map.insert(workflow_name.to_string(), wf_meta(quota));
        Self::install(map).await
    }
}

impl Drop for MetadataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
            *lock = self.previous.take();
        }
    }
}

/// Build a [`StartWorkflowParams`] with every non-essential field at its
/// production default, mirroring `cross_type_continue_as_new_tests.rs`'s
/// `start_root` literal exactly (so this stays a faithful production shape,
/// not a hand-trimmed one).
fn params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
    }
}

/// Attempt a start through the real production entry point. Returns the
/// pre-generated candidate `exec_id` alongside the outcome so a REJECTED
/// attempt can still be checked for "no row exists under this id" (the
/// `Err` variant itself carries no `exec_id`).
async fn try_start(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
) -> (
    ExecutionId,
    HarvestResult<autumn_harvest::execution::StartedWorkflowExecution>,
) {
    let exec_id = ExecutionId::new();
    let outcome = start_or_load_workflow_execution(
        conn,
        params(workflow_name, workflow_id, exec_id, input),
        None,
    )
    .await;
    (exec_id, outcome)
}

/// A fresh, uniquely-`workflow_id`'d start that must succeed — the common
/// case in every test below.
async fn start_ok(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(conn, workflow_name, &workflow_id, input).await;
    outcome.unwrap_or_else(|e| panic!("expected a successful start, got {e:?}"));
    exec_id
}

async fn count_rows(conn: &mut AsyncPgConnection, sql: &str, binds: &[&str]) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut query = diesel::sql_query(sql).into_boxed();
    for b in binds {
        query = query.bind::<diesel::sql_types::Text, _>((*b).to_string());
    }
    let row: Count = query.get_result(conn).await.expect("count rows");
    row.n
}

async fn active_count(conn: &mut AsyncPgConnection, workflow_name: &str, quota_key: &str) -> i64 {
    count_rows(
        conn,
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED')",
        &[workflow_name, quota_key],
    )
    .await
}

/// Read a single execution row's persisted `state` column -- used by the
/// `replace_execution` (issue #946 P1) regression tests below to prove a
/// REJECTED replace rolls back the whole transaction, including the seal of
/// the row being replaced (it must still read its pre-replace state, never
/// left half-sealed).
async fn row_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let row: State =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .expect("row must exist");
    row.state
}

async fn assert_no_execution_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count execution rows");
    assert_eq!(
        row.n, 0,
        "a rejected quota admission must roll back atomically -- no phantom execution row"
    );
}

async fn assert_no_task_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_task_queue WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("count task-queue rows");
    assert_eq!(
        row.n, 0,
        "a rejected quota admission must roll back atomically -- no phantom task-queue row"
    );
}

async fn mark_terminal(conn: &mut AsyncPgConnection, exec_id: ExecutionId, state: &str) {
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = $1, completed_at = NOW() WHERE id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("mark terminal");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'COMPLETED' WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("close tasks");
}

/// Assert a rejection carries the exact expected [`HarvestError::QuotaExceeded`]
/// shape. `current` is checked via a caller-supplied predicate rather than
/// exact equality where the value is implementation-detail-fragile (e.g.
/// `pg_column_size` byte counts).
fn assert_quota_exceeded(
    err: &HarvestError,
    expected_workflow_name: &str,
    expected_key: &str,
    expected_resource: QuotaResource,
    expected_limit: u64,
    current_ok: impl FnOnce(u64) -> bool,
) {
    match err {
        HarvestError::QuotaExceeded {
            workflow_name,
            key,
            resource,
            limit,
            current,
        } => {
            assert_eq!(workflow_name, expected_workflow_name);
            assert_eq!(key, expected_key);
            assert_eq!(*resource, expected_resource);
            assert_eq!(*limit, expected_limit);
            assert!(
                current_ok(*current),
                "current={current} failed the caller's predicate for resource {resource:?}"
            );
        }
        other => panic!("expected HarvestError::QuotaExceeded, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// AC2 / AC4 -- max_active_executions, the headline success-metric shape
// ---------------------------------------------------------------------------

/// The money test: a `max_active_executions = 5` policy admits exactly 5
/// concurrent starts for one key, and the 6th is rejected with the exact
/// typed error -- the small-N analogue of the issue's "10,000 starts capped
/// at exactly 100" success metric (the full-scale load test is Task 7).
#[tokio::test]
async fn active_executions_cap_admits_exactly_n_then_rejects_the_next() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_active_cap");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(5);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    for i in 0..5 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
        assert_eq!(
            active_count(&mut conn, wf, "acme").await,
            i64::from(i) + 1,
            "admission {i} must bring the active count to exactly {}",
            i + 1
        );
    }
    assert_eq!(active_count(&mut conn, wf, "acme").await, 5);

    let (rejected_id, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = outcome.expect_err("the 6th admission for a cap of 5 must be rejected");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::ActiveExecutions, 5, |c| {
        c == 5
    });

    // Capped, not merely slowed: still exactly 5, and the rejected attempt
    // left no trace of itself.
    assert_eq!(active_count(&mut conn, wf, "acme").await, 5);
    assert_no_execution_row(&mut conn, rejected_id).await;
}

/// AC4: the rejection rolls back atomically -- no phantom execution row and
/// no phantom task-queue row survive a rejected attempt.
#[tokio::test]
async fn rejected_start_creates_no_execution_or_task_row() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_no_phantom_rows");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (rejected_id, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    outcome.expect_err("the 2nd admission for a cap of 1 must be rejected");
    assert_no_execution_row(&mut conn, rejected_id).await;
    assert_no_task_row(&mut conn, rejected_id).await;
}

/// Two distinct resolved keys under one policy are independently capped.
#[tokio::test]
async fn active_executions_cap_isolates_per_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_isolate_per_key");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    // A different key is unaffected by "acme" being at its cap.
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "beta"})).await;

    let (_, acme_second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    acme_second.expect_err("acme is already at its cap of 1");

    let (_, beta_second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "beta"}),
    )
    .await;
    beta_second.expect_err("beta is already at its cap of 1");
}

/// Two different workflow TYPES resolving the same key value are
/// independently capped: accounting is keyed on `(workflow_name, quota_key)`,
/// never `quota_key` alone.
#[tokio::test]
async fn active_executions_cap_isolates_per_workflow_type() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_a = leaked("quota_type_a");
    let wf_b = leaked("quota_type_b");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let mut map = HashMap::new();
    map.insert(wf_a.to_string(), wf_meta(policy));
    map.insert(wf_b.to_string(), wf_meta(policy));
    let _guard = MetadataGuard::install(map).await;

    start_ok(&mut conn, wf_a, serde_json::json!({"tenant_id": "acme"})).await;
    // Type B, same resolved key "acme", is a DIFFERENT (workflow_name, key)
    // pair and so is unaffected by type A being at its cap.
    start_ok(&mut conn, wf_b, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, a_second) = try_start(
        &mut conn,
        wf_a,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    a_second.expect_err("type A/acme is already at its cap of 1");
}

// ---------------------------------------------------------------------------
// AC9 -- no policy, or a policy with no caps, is a byte-for-byte no-op
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_policy_workflow_is_unaffected() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // No `GLOBAL_WORKFLOW_METADATA` entry at all for this type -- the
    // process-global map may be `None`, or `Some` without this key; either
    // way `quota_policy` resolves to `None` and enforcement is skipped.
    let wf = leaked("quota_no_policy");
    for _ in 0..20 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    }
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        0,
        "no policy means no quota_key is ever stamped"
    );
}

#[tokio::test]
async fn policy_with_no_caps_declared_is_a_noop() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_no_caps");
    // `QuotaPolicy::new(key)` with zero `with_max_*` calls -- resolves a
    // key but `has_any_cap() == false`, so `check_quota` is never even
    // reached.
    let policy = QuotaPolicy::new("tenant_id");
    assert!(!policy.has_any_cap());
    let _guard = MetadataGuard::install_one(wf, policy).await;

    for _ in 0..20 {
        start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    }
    assert_eq!(active_count(&mut conn, wf, "acme").await, 20);
}

// ---------------------------------------------------------------------------
// Unresolvable key -- fail open, mirroring `concurrency_key IS NULL`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unresolvable_key_fails_open() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_unresolvable_key");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // The input has no `tenant_id` field at all -- `resolve_quota_key`
    // returns `None`, so enforcement is skipped for every one of these
    // starts regardless of the declared cap of 1.
    for _ in 0..5 {
        start_ok(&mut conn, wf, serde_json::json!({"other_field": 1})).await;
    }
}

// ---------------------------------------------------------------------------
// AC2 -- max_history_bytes, isolated from the other two caps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_bytes_cap_rejects_once_exceeded() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_history_bytes");
    // A tiny cap: the very first execution's own `WorkflowStarted` event
    // already exceeds 1 byte, so the SECOND start for the same key must be
    // rejected on `HistoryBytes` alone (active_executions/dead_letters are
    // uncapped for this policy).
    let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, second) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = second.expect_err("the 2nd start must be rejected on history_bytes");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::HistoryBytes, 1, |c| c >= 1);
}

// ---------------------------------------------------------------------------
// AC2 -- max_dead_letters, isolated from the other two caps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_letters_cap_rejects_once_reached() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_dead_letters");
    let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(3);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // A seed execution to hang the DLQ rows off of -- `dead_letter()`
    // resolves `workflow_name`/`quota_key` from this exec_id's OWN row, so
    // the seed must be of the SAME workflow type and the SAME resolved key
    // as the admission attempt below.
    let seed_id = start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    for i in 0..3 {
        dead_letter(
            &mut conn,
            &NewDeadLetterEntry {
                original_task_id: Uuid::new_v4(),
                queue_name: "default".to_string(),
                task_type: "activity".to_string(),
                workflow_exec_id: Some(seed_id.as_uuid()),
                activity_name: Some("do_thing".to_string()),
                input: serde_json::json!({"i": i}),
                error: "boom".to_string(),
                attempts: 3,
                owner: None,
                severity: None,
            },
        )
        .await
        .expect("insert dead letter");
    }

    // Rejected on the FIRST attempt -- no active-execution starts needed to
    // reach the cap, unlike the active_executions test above.
    let (_, outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let err = outcome.expect_err("3 dead letters already meets a cap of 3");
    assert_quota_exceeded(&err, wf, "acme", QuotaResource::DeadLetters, 3, |c| c == 3);
}

// ---------------------------------------------------------------------------
// A completed run frees its slot for a later start
// ---------------------------------------------------------------------------

#[tokio::test]
async fn active_executions_cap_frees_up_when_a_run_completes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_frees_on_completion");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let first = start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;

    let (_, blocked) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    blocked.expect_err("acme is at its cap of 1 while the first run is RUNNING");

    // `RUNNING` -> `COMPLETED` excludes it from the active-count filter
    // (`state IN ('RUNNING', 'PAUSED')`), freeing the slot.
    mark_terminal(&mut conn, first, "COMPLETED").await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 0);

    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "acme"})).await;
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);
}

// ---------------------------------------------------------------------------
// AC3 -- continue-as-new (in-flight continuation, not a fresh admission):
// `quota_key` propagation on the successor row. Worker-driven, mirroring
// `cross_type_continue_as_new_tests.rs`'s harness pattern exactly.
// ---------------------------------------------------------------------------

fn phase_one<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = input["next_type"].as_str().map(str::to_string);
        if let Some(target) = target {
            let target: &'static str = Box::leak(target.into_boxed_str());
            ctx.continue_as_new_as_type(target, serde_json::json!({"phase": "two"}))
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("continue_as_new_as_type suspends the run and never resolves");
        }
        ctx.continue_as_new(serde_json::json!({"phase": "two"}))
            .await
            .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new suspends the run and never resolves");
    })
}

fn phase_two<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"ran": "phase_two", "input": input})) })
}

fn wf_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "quota_enforcement_tests",
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

fn registry(infos: Vec<WorkflowInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(infos, vec![]))
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

async fn recorded_transition(url: &str, predecessor: ExecutionId) -> (ExecutionId, Option<String>) {
    load_history_from_url(url, predecessor)
        .await
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id,
                new_workflow_type,
                ..
            } => Some((*new_exec_id, new_workflow_type.clone())),
            _ => None,
        })
        .expect("predecessor history must contain WorkflowContinuedAsNew")
}

/// Run a worker until `predecessor` seals, then return the successor id and
/// the recorded target type.
async fn drive_transition(
    url: &str,
    predecessor: ExecutionId,
    reg: Arc<HandlerRegistry>,
    worker_id: &str,
) -> (ExecutionId, Option<String>) {
    let worker = build_runtime_worker(worker_id, 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    let _sealed = wait_for_execution_state(url, predecessor, "CONTINUED_AS_NEW").await;
    let transition = recorded_transition(url, predecessor).await;
    worker.shutdown();
    handle.await.expect("worker join");
    transition
}

/// Start a root execution through the real start path (not the direct
/// `try_start`/`params` helpers above, since the worker needs a genuinely
/// dispatchable task -- `quota_key` resolution is identical either way).
async fn start_root(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        params(workflow_name, workflow_id, ExecutionId::new(), input),
        None,
    )
    .await
    .expect("start root execution")
    .exec_id
}

/// Same-type `continue_as_new`: the successor carries the predecessor's
/// `quota_key` verbatim -- in-flight continuation of an already-admitted
/// run, not a fresh admission, so it never re-runs `check_quota`.
#[tokio::test]
async fn continue_as_new_same_type_propagates_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let name = leaked("quota_can_same_type");
    let workflow_id = format!("loop-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        name,
        &workflow_id,
        serde_json::json!({"phase": "one"}),
    )
    .await;

    // No `GLOBAL_WORKFLOW_METADATA` entry is installed at all for this run
    // -- `start_root` above therefore stamps `quota_key = NULL`. Stamp a
    // key directly on the predecessor's row (mirroring how
    // `same_type_continue_as_new_is_unchanged` stamps a per-start override
    // the type never declared) to prove the same-type path carries
    // whatever is ALREADY on the row verbatim, regardless of any live
    // policy.
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("acme")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    let reg = registry(vec![wf_info(name, phase_one)]);
    let (successor, recorded_type) =
        drive_transition(&url, predecessor, reg, "w-946-same-type").await;

    assert!(
        recorded_type.is_none(),
        "a same-type continuation records no target type"
    );
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key.as_deref(),
        Some("acme"),
        "same-type continuation must carry the predecessor's quota_key verbatim"
    );
}

/// Cross-type `continue_as_new_as_type`: `quota_key` is RE-RESOLVED against
/// the TARGET type's own declared policy and the new input, exercising the
/// `worker.rs` fix that reads `registry` directly (mirroring
/// `resolve_workflow_concurrency`) rather than the process-global
/// `GLOBAL_WORKFLOW_METADATA` mirror, which this test's `registry()` helper
/// never populates (it uses the raw `HandlerRegistry::new` constructor).
#[tokio::test]
async fn continue_as_new_cross_type_re_resolves_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("quota_can_cross_from");
    let phase2 = leaked("quota_can_cross_to");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2, "tenant_id": "acme"}),
    )
    .await;
    // Predecessor's row carries an unrelated key from a different policy --
    // this must NOT survive the cross-type transition.
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("stale-key")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    // Phase 2 declares its OWN quota policy directly on the `WorkflowInfo`
    // (not via `GLOBAL_WORKFLOW_METADATA`, which this registry never
    // populates) over the successor's own input shape: `{"phase": "two"}`
    // has no `tenant_id`, so resolve against a field that IS present.
    let mut target = wf_info(phase2, phase_two);
    target.quota = Some(QuotaPolicy::new("phase").with_max_active_executions(9));

    let reg = registry(vec![wf_info(phase1, phase_one), target]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-946-cross-type").await;

    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key.as_deref(),
        Some("two"),
        "the key must be re-resolved from the NEW type's policy against the new \
         input (\"phase\": \"two\" -> resolved key \"two\"), not carried from \
         the predecessor's stale row value"
    );
}

/// Cross-type transition into a type with NO declared quota policy clears
/// the key -- "presence decides", not "inherit unless overridden".
#[tokio::test]
async fn continue_as_new_cross_type_to_no_quota_workflow_clears_quota_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("quota_can_clears_from");
    let phase2 = leaked("quota_can_clears_to");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("acme")))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor quota_key");

    // Phase 2's `WorkflowInfo.quota` is `None` (the `wf_info` default).
    let reg = registry(vec![wf_info(phase1, phase_one), wf_info(phase2, phase_two)]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-946-cross-clear").await;

    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.quota_key, None,
        "a target type with no declared quota policy must clear the key, \
         never inherit the predecessor's"
    );
}

// ---------------------------------------------------------------------------
// Success metric — the issue's own runaway-tenant scenario, driven with
// genuine concurrency (not the sequential admission loop
// `active_executions_cap_admits_exactly_n_then_rejects_the_next` already
// covers above).
// ---------------------------------------------------------------------------

/// Issue #946's success metric: "tenant A submits [a burst of] starts against
/// `max_active_executions=N` while tenant B operates normally: tenant A
/// capped at exactly N active executions with 100% of overflow starts
/// receiving typed 429 [here: the typed `QuotaExceeded` `Err` the HTTP layer
/// maps to 429]; tenant B's start... success rate unchanged".
///
/// Scaled down from the issue's literal 10,000/100 for CI runtime (the
/// admission-time behaviour under concurrency does not change with scale —
/// the SQL-level advisory lock + indexed count this test exercises is the
/// same code path regardless of burst size), but driven with GENUINE
/// concurrency: every attempt races against every other attempt on its own
/// connection via `tokio::spawn`, not a sequential loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// `tenant_a_*`/`tenant_b_*` are deliberately parallel-named (the whole point
// of the test is a side-by-side comparison of the two tenants' outcomes) --
// renaming them to satisfy clippy's Levenshtein-distance heuristic would
// make the assertions below harder to read, not clearer.
#[allow(clippy::similar_names)]
async fn concurrent_runaway_tenant_is_capped_while_a_second_tenant_is_unaffected() {
    const CAP: usize = 20;
    const OVERFLOW_ATTEMPTS: usize = 60; // total burst >> cap, guarantees rejections
    const TENANT_B_ATTEMPTS: usize = 15; // a well-behaved sibling tenant, unaffected

    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf = leaked("quota_runaway");

    let policy = QuotaPolicy::new("tenant_id")
        .with_max_active_executions(u32::try_from(CAP).expect("CAP fits in u32"));
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Tenant A: a burst of concurrent starts, all sharing one quota key.
    let mut tasks = Vec::with_capacity(OVERFLOW_ATTEMPTS + TENANT_B_ATTEMPTS);
    for _ in 0..OVERFLOW_ATTEMPTS {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
            try_start(
                &mut conn,
                wf,
                &workflow_id,
                serde_json::json!({"tenant_id": "runaway-tenant"}),
            )
            .await
            .1
        }));
    }
    // Tenant B: a small, well-behaved concurrent burst on a DIFFERENT quota
    // key of the SAME workflow type, interleaved with tenant A's storm so it
    // genuinely races against the saturated key rather than running before
    // or after it.
    for _ in 0..TENANT_B_ATTEMPTS {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
            try_start(
                &mut conn,
                wf,
                &workflow_id,
                serde_json::json!({"tenant_id": "well-behaved-tenant"}),
            )
            .await
            .1
        }));
    }

    let mut tenant_a_ok = 0usize;
    let mut tenant_a_rejected = 0usize;
    let mut tenant_b_ok = 0usize;
    let mut tenant_b_rejected = 0usize;
    for (i, task) in tasks.into_iter().enumerate() {
        let outcome = task.await.expect("spawned start task must not panic");
        let is_tenant_a = i < OVERFLOW_ATTEMPTS;
        match outcome {
            Ok(_) => {
                if is_tenant_a {
                    tenant_a_ok += 1;
                } else {
                    tenant_b_ok += 1;
                }
            }
            Err(HarvestError::QuotaExceeded {
                key,
                resource,
                limit,
                ..
            }) => {
                assert_eq!(
                    resource,
                    QuotaResource::ActiveExecutions,
                    "the only declared cap is active_executions"
                );
                assert_eq!(limit, u64::try_from(CAP).expect("CAP fits in u64"));
                if is_tenant_a {
                    assert_eq!(key, "runaway-tenant");
                    tenant_a_rejected += 1;
                } else {
                    // Tenant B never has a policy of its own key saturated —
                    // if it were ever rejected it would prove cross-tenant
                    // bleed, which the assertions below independently rule
                    // out via `tenant_b_ok == TENANT_B_ATTEMPTS`.
                    tenant_b_rejected += 1;
                }
            }
            Err(e) => panic!("unexpected error kind: {e:?}"),
        }
    }

    // 100% of tenant A's overflow burst was either admitted (up to the cap)
    // or received the typed rejection — never anything else, never silently
    // dropped or a generic 500-class error.
    assert_eq!(
        tenant_a_ok + tenant_a_rejected,
        OVERFLOW_ATTEMPTS,
        "every tenant-A attempt must resolve to exactly one of admitted/rejected"
    );
    assert_eq!(
        tenant_a_ok, CAP,
        "tenant A must be capped at EXACTLY the declared limit, not fewer \
         (a false rejection under contention) and not more (a lost-update \
         race past the advisory-lock admission check)"
    );
    assert_eq!(
        tenant_a_rejected,
        OVERFLOW_ATTEMPTS - CAP,
        "every overflow start beyond the cap must receive the typed 429-mapped rejection"
    );

    // Tenant B's success rate is completely unaffected by tenant A's
    // concurrent saturation — the isolation the issue's success metric
    // requires (a different key on the same workflow type, not merely a
    // different workflow type, so this proves key-level isolation under
    // real contention, not just type-level isolation).
    assert_eq!(
        tenant_b_ok, TENANT_B_ATTEMPTS,
        "tenant B (a different quota key) must see a 100% success rate \
         while tenant A's key is saturated by a concurrent burst"
    );
    assert_eq!(tenant_b_rejected, 0);

    // The persisted state agrees with the in-flight admission decisions —
    // "capped, not merely slowed" (mirrors the sequential test's own
    // invariant, now proven to hold under genuine concurrent contention).
    assert_eq!(
        active_count(&mut conn, wf, "runaway-tenant").await,
        i64::try_from(CAP).expect("CAP fits in i64")
    );
    assert_eq!(
        active_count(&mut conn, wf, "well-behaved-tenant").await,
        i64::try_from(TENANT_B_ATTEMPTS).expect("TENANT_B_ATTEMPTS fits in i64")
    );
}

// ---------------------------------------------------------------------------
// P1 regression -- `replace_execution` is a SECOND row-creation branch
// inside `start_or_load_workflow_execution_collect`'s transaction (reached
// by `AllowDuplicateFailedOnly`/`TerminateIfRunning`/a conflict-driven
// `Terminate`), and it originally bypassed quota enforcement entirely.
// Every test above exercises only the `on_conflict_do_nothing()`
// fresh-insert branch (via `AllowDuplicate`, the default reuse policy) --
// none of them would have caught this. Each test below targets exactly one
// of the three `replace_execution` call sites and would have FAILED before
// the fix (the replace silently succeeded instead of being rejected).
// ---------------------------------------------------------------------------

/// Site 2 (`AllowDuplicateFailedOnly` over a FAILED prior): resurrecting a
/// terminal row into a fresh ACTIVE execution is a pure net **+1** to the
/// key's active population (the terminal prior contributed zero before the
/// replace, so nothing offsets the new row) -- looped across N distinct
/// `workflow_id`s, each with its own already-failed prior, this is the
/// concrete "accumulate unbounded active executions well past the declared
/// cap" vector review agent 1 identified.
#[tokio::test]
async fn replace_execution_allow_duplicate_failed_only_enforces_quota_on_resurrection() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_afo");

    // 1. No quota policy installed yet -- an unconstrained start, then a
    //    terminal failure (an ordinary completed-with-failure run).
    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "t1"}),
    )
    .await;
    outcome.expect("initial start (no policy yet) must succeed");
    mark_terminal(&mut conn, exec_id, "FAILED").await;

    // 2. NOW install a quota policy whose cap is ALREADY saturated by an
    //    unrelated, distinct-`workflow_id` execution -- so any further
    //    active admission for key "t1" (including a resurrection of the
    //    just-failed row above) must be rejected.
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "t1"})).await;
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);

    // 3. Before the P1 fix, `replace_execution` (reached here via
    //    `AllowDuplicateFailedOnly` over a FAILED prior) never called
    //    `enforce_quota_admission` at all -- this would have silently
    //    resurrected the failed row into a fresh ACTIVE execution.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "t1"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "resurrecting a FAILED row into a fresh active execution must still \
         be quota-checked, exactly like any other admission",
    );
    assert_quota_exceeded(&err, wf, "t1", QuotaResource::ActiveExecutions, 1, |c| {
        c == 1
    });

    // Rolled back atomically: no phantom row for the rejected attempt, the
    // original row is STILL FAILED (never resurrected), and the key's
    // active population is untouched.
    assert_no_execution_row(&mut conn, exec_id2).await;
    assert_eq!(row_state(&mut conn, exec_id).await, "FAILED");
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);
}

/// Site 3 (`TerminateIfRunning` over a genuinely TERMINAL prior, e.g.
/// COMPLETED): the identical bypass shape as Site 2 above, reached through
/// the OTHER reuse policy that routes a terminal-existing row into
/// `replace_execution`.
#[tokio::test]
async fn replace_execution_terminate_if_running_enforces_quota_over_a_terminal_prior() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_terminal");

    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "t1"}),
    )
    .await;
    outcome.expect("initial start (no policy yet) must succeed");
    mark_terminal(&mut conn, exec_id, "COMPLETED").await;

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;
    start_ok(&mut conn, wf, serde_json::json!({"tenant_id": "t1"})).await;
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);

    // Before the P1 fix, `replace_execution` (reached here via
    // `TerminateIfRunning` over a terminal COMPLETED prior) never called
    // `enforce_quota_admission`.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "t1"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "TerminateIfRunning over a terminal (COMPLETED) prior must still be \
         quota-checked when it creates a fresh active execution",
    );
    assert_quota_exceeded(&err, wf, "t1", QuotaResource::ActiveExecutions, 1, |c| {
        c == 1
    });

    assert_no_execution_row(&mut conn, exec_id2).await;
    assert_eq!(row_state(&mut conn, exec_id).await, "COMPLETED");
    assert_eq!(active_count(&mut conn, wf, "t1").await, 1);
}

/// Site 1 (`ActiveConflictBehavior::Terminate`, existing RUNNING/PAUSED):
/// unlike Sites 2/3, replacing an ALREADY-active row is a quota-**neutral**
/// swap for the SAME key (the old row's -1 offsets the new row's +1) -- so
/// the real bypass here is not about looping against one stable
/// `workflow_id`, but about the request's resolved key CHANGING between the
/// original start and the `TerminateIfRunning` replace call. Before the P1
/// fix this let a caller grow an ALREADY-SATURATED key's population by
/// retargeting an unrelated, still-running execution at it via
/// `TerminateIfRunning`, with zero quota check anywhere in the path.
#[tokio::test]
async fn replace_execution_terminate_if_running_enforces_the_new_requests_resolved_key() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_crosskey");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // Saturate "victim-tenant"'s cap of 1 via an unrelated, distinct
    // `workflow_id`.
    start_ok(
        &mut conn,
        wf,
        serde_json::json!({"tenant_id": "victim-tenant"}),
    )
    .await;
    assert_eq!(active_count(&mut conn, wf, "victim-tenant").await, 1);

    // A SEPARATE, still-RUNNING execution E, originally started under a
    // DIFFERENT key ("attacker-tenant") that is itself exactly at its own
    // (unrelated) cap of 1.
    let workflow_id_e = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id_e, outcome_e) = try_start(
        &mut conn,
        wf,
        &workflow_id_e,
        serde_json::json!({"tenant_id": "attacker-tenant"}),
    )
    .await;
    outcome_e.expect("E must start under its own, unsaturated key");
    assert_eq!(active_count(&mut conn, wf, "attacker-tenant").await, 1);

    // Now re-target E's SAME `workflow_id` with `TerminateIfRunning`, but
    // this request body resolves to "victim-tenant" -- the
    // ALREADY-SATURATED key. Before the P1 fix, `replace_execution`
    // (`ActiveConflictBehavior::Terminate`) never called
    // `enforce_quota_admission`, so E would have been silently sealed and
    // replaced by a fresh execution counted against "victim-tenant",
    // growing that key's population to 2 past its declared cap of 1.
    let exec_id_e2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id_e,
        exec_id_e2,
        serde_json::json!({"tenant_id": "victim-tenant"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    let err = outcome2.expect_err(
        "a TerminateIfRunning replace that resolves to an ALREADY-saturated \
         key must be rejected, exactly like a fresh admission would be",
    );
    assert_quota_exceeded(
        &err,
        wf,
        "victim-tenant",
        QuotaResource::ActiveExecutions,
        1,
        |c| c == 1,
    );

    // The rejection rolled back atomically: no phantom row for the failed
    // attempt, E is STILL RUNNING under its original key (never sealed --
    // the whole `replace_execution` call, including the seal step, rolled
    // back together with the failed quota check), and both keys' active
    // populations are untouched.
    assert_no_execution_row(&mut conn, exec_id_e2).await;
    assert_eq!(row_state(&mut conn, exec_id_e).await, "RUNNING");
    assert_eq!(active_count(&mut conn, wf, "victim-tenant").await, 1);
    assert_eq!(active_count(&mut conn, wf, "attacker-tenant").await, 1);
}

/// The fix must not over-reject: a `replace_execution` admission still
/// succeeds normally when the resolved key is well under its cap (Site 2),
/// and a quota-**neutral** same-key refresh (Site 1) succeeds even when the
/// key is already exactly at its cap, since it is a net-zero swap.
#[tokio::test]
async fn replace_execution_paths_still_succeed_when_not_over_cap() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_under_cap");

    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    // AllowDuplicateFailedOnly over a FAILED prior, well under cap.
    let wid1 = format!("wid-{}", Uuid::new_v4().simple());
    let (exec1, o1) = try_start(
        &mut conn,
        wf,
        &wid1,
        serde_json::json!({"tenant_id": "roomy"}),
    )
    .await;
    o1.expect("initial start");
    mark_terminal(&mut conn, exec1, "FAILED").await;

    let exec1b = ExecutionId::new();
    let mut p1 = params(wf, &wid1, exec1b, serde_json::json!({"tenant_id": "roomy"}));
    p1.reuse_policy = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
    start_or_load_workflow_execution(&mut conn, p1, None)
        .await
        .expect("a replace well under cap must still succeed -- the fix must not over-reject");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 1);

    // TerminateIfRunning over a RUNNING existing, bringing the key to
    // EXACTLY its cap first, then a same-key refresh at the cap boundary
    // (net-zero on active count) must still succeed.
    let wid2 = format!("wid-{}", Uuid::new_v4().simple());
    let (_exec2, o2) = try_start(
        &mut conn,
        wf,
        &wid2,
        serde_json::json!({"tenant_id": "roomy"}),
    )
    .await;
    o2.expect("initial start");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 2);

    let exec2b = ExecutionId::new();
    let mut p2 = params(wf, &wid2, exec2b, serde_json::json!({"tenant_id": "roomy"}));
    p2.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    start_or_load_workflow_execution(&mut conn, p2, None)
        .await
        .expect("a same-key refresh replace must succeed -- it is net-zero on active count");
    assert_eq!(active_count(&mut conn, wf, "roomy").await, 2);
}

/// Codex P2 (issue #946, round 1): `history_bytes`, not just
/// `active_executions`, must be exempted for the row a `TerminateIfRunning`
/// replace is about to seal -- and specifically on Site 1 (the
/// `ActiveConflictBehavior::Terminate` path over a RUNNING/PAUSED existing,
/// reached via the pre-check-cancel shortcut before the P1/P2 fix and via
/// the atomic `inline_cancel` + `replace_execution` fallthrough after it).
///
/// Before the fix, `enforce_quota_before_terminate_pre_check` only ever
/// subtracted the existing row's own contribution from
/// `usage.active_executions`, never from `usage.history_bytes` -- so a
/// same-key refresh under a tight `max_history_bytes` cap would be WRONGLY
/// REJECTED the moment the row being replaced had accumulated ANY history of
/// its own, even though that row is about to be sealed out of existence and
/// contributes nothing to the key's population going forward. The fix
/// (routing quota-governed keys through the atomic `inline_cancel` +
/// `replace_execution` path instead) closes this for free: that path seals
/// the existing row to CANCELLED *before* `enforce_quota_admission` runs, so
/// the row is excluded from EVERY resource `QUOTA_USAGE_SQL`'s `active` CTE
/// scopes by state -- `active_executions` and `history_bytes` alike -- with
/// no special-cased exemption logic required for either.
#[tokio::test]
async fn replace_execution_terminate_if_running_exempts_the_replaced_runs_own_history_bytes() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_replace_tir_history_bytes");

    // A 1-byte cap: any single row's own `WorkflowStarted` event already
    // exceeds it (mirrors `history_bytes_cap_rejects_once_exceeded`'s
    // pattern), so this key is only ever "under cap" while it has zero
    // RUNNING/PAUSED rows of its own.
    let policy = QuotaPolicy::new("tenant_id").with_max_history_bytes(1);
    let _guard = MetadataGuard::install_one(wf, policy).await;

    let workflow_id = format!("wid-{}", Uuid::new_v4().simple());
    let (exec_id, outcome) = try_start(
        &mut conn,
        wf,
        &workflow_id,
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    outcome.expect("the FIRST start for a key must succeed: usage is zero before it exists");
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);

    // Sanity check the trap is real: a FRESH, unrelated `workflow_id` under
    // the SAME key is rejected on `history_bytes` alone, proving the cap is
    // genuinely breached by the first row's own recorded history (so the
    // same-key replace below is not vacuously "under cap the whole time").
    let (_unrelated_exec, unrelated_outcome) = try_start(
        &mut conn,
        wf,
        &format!("wid-{}", Uuid::new_v4().simple()),
        serde_json::json!({"tenant_id": "acme"}),
    )
    .await;
    let unrelated_err = unrelated_outcome.expect_err(
        "an UNRELATED fresh start under the same key must be rejected on history_bytes",
    );
    assert_quota_exceeded(
        &unrelated_err,
        wf,
        "acme",
        QuotaResource::HistoryBytes,
        1,
        |c| c >= 1,
    );

    // Now replace the SAME row via `TerminateIfRunning` (Site 1 -- the
    // existing row is still RUNNING at this point). Before the P1/P2 fix,
    // this would ALSO have been wrongly rejected on `history_bytes`, since
    // the pre-check helper only exempted `active_executions`. After the
    // fix, the existing row is sealed to CANCELLED before the quota check
    // runs, so it (and its history) is excluded entirely.
    let exec_id2 = ExecutionId::new();
    let mut p = params(
        wf,
        &workflow_id,
        exec_id2,
        serde_json::json!({"tenant_id": "acme"}),
    );
    p.reuse_policy = WorkflowIdReusePolicy::TerminateIfRunning;
    let outcome2 = start_or_load_workflow_execution(&mut conn, p, None).await;

    outcome2.expect(
        "a same-key TerminateIfRunning replace must succeed: the row being \
         replaced -- and its own history -- must be excluded from the \
         history_bytes check, not just active_executions",
    );
    // `inline_cancel` appends a `WorkflowCancelled` event and sets CANCELLED
    // first, but `replace_execution` then unconditionally seals the SAME
    // row to CONTINUED_AS_NEW (its own doc comment: "existing is already
    // sealed above (CONTINUED_AS_NEW) by the time this runs") -- the final
    // observable state of the atomic `inline_cancel` + `replace_execution`
    // sequence, pre-existing and unrelated to this fix. Either state
    // excludes the row from `state IN ('RUNNING', 'PAUSED')`, which is all
    // that matters for the quota exemption this test proves.
    assert_eq!(row_state(&mut conn, exec_id).await, "CONTINUED_AS_NEW");
    assert_eq!(row_state(&mut conn, exec_id2).await, "RUNNING");
    assert_eq!(active_count(&mut conn, wf, "acme").await, 1);
}
