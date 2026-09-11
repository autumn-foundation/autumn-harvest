#![cfg(feature = "db")]
//! `quota_key` backfill reconciliation integration tests -- issue #1226.
//!
//! The migration `20260725000000_harvest_workflow_quotas` ships with no
//! backfill: an execution already `RUNNING`/`PAUSED` before its workflow
//! type declared a `QuotaPolicy` keeps `quota_key = NULL` forever, invisible
//! to `quota::load_quota_usage`. These tests exercise
//! `quota_reconcile::reconcile_quota_keys` end to end against a real
//! Postgres container:
//!
//! - A pre-upgrade active execution gets correctly backfilled.
//! - A tenant's combined pre-upgrade + post-upgrade usage becomes visible
//!   (and therefore correctly capped) once reconciliation has run.
//! - A second run is a no-op (idempotent).
//! - Terminal rows are never touched.
//! - An over-cap resolved key is left NULL rather than written.
//! - A workflow type with no declared policy is never a scan candidate,
//!   even in a mixed deployment where some other type has one.
//! - A deployment with no policy registered anywhere skips the scan.
//! - The keyset cursor advances past rows it can never resolve, so they
//!   cannot starve a resolvable row sorted behind them.

use std::collections::HashMap;
use std::sync::LazyLock;

use autumn_harvest::completion_trigger::{GLOBAL_WORKFLOW_METADATA, WorkflowMetadata};
use autumn_harvest::quota::{QuotaPolicy, check_quota, load_quota_usage};
use autumn_harvest::quota_reconcile::{reconcile_quota_keys, reconcile_quota_keys_from};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

/// Every row in this file is inserted with `shard_id = 0` (see
/// `insert_execution`). Fed straight through to `reconcile_quota_keys`'s
/// DR-fence assertion, which is a no-op unless this process has a pinned
/// generation for the shard.
const SHARD: Option<autumn_harvest::types::ShardId> = Some(autumn_harvest::types::ShardId::new(0));

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (conn, container)
}

/// A unique workflow-type name per test -- `GLOBAL_WORKFLOW_METADATA` is
/// process-global, so every test needs its own namespace.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// Insert a workflow execution in the given state with the given input JSON
/// and (possibly NULL) `quota_key`. Returns its id.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    state: &str,
    input: serde_json::Value,
    quota_key: Option<&str>,
) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         VALUES ($1, $2, $3, 0, $4, $5, $6)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Jsonb, _>(input)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(quota_key)
    .execute(conn)
    .await
    .expect("insert workflow execution");
    id
}

async fn read_quota_key(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Option<String> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let row: Row =
        diesel::sql_query("SELECT quota_key FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id)
            .get_result(conn)
            .await
            .expect("load quota_key");
    row.quota_key
}

/// Serializes access to the process-global `GLOBAL_WORKFLOW_METADATA` mirror
/// across this file's tests, mirroring `quota_enforcement_tests.rs`'s
/// identical `TEST_SERIAL` convention.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// RAII installer for `GLOBAL_WORKFLOW_METADATA`: installs the given map, and
/// restores whatever was there before on drop.
struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install_one(workflow_name: &str, quota: QuotaPolicy) -> Self {
        let permit = TEST_SERIAL.lock().await;
        let mut map = HashMap::new();
        map.insert(
            workflow_name.to_string(),
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
            },
        );
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

    /// Install with NO declared policy for any workflow type (an empty
    /// registry), used by the no-policy test.
    async fn install_empty() -> Self {
        let permit = TEST_SERIAL.lock().await;
        let previous = {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            lock.take()
        };
        {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            *lock = Some(HashMap::new());
        }
        Self {
            previous,
            _permit: permit,
        }
    }
}

impl Drop for MetadataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
            *lock = self.previous.take();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pre_upgrade_active_execution_gets_backfilled() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_backfill");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");

    assert_eq!(summary.backfilled, 1);
    assert_eq!(summary.total_scanned(), 1);
    assert_eq!(
        read_quota_key(&mut conn, exec_id).await,
        Some("acme".to_string())
    );
}

#[tokio::test]
async fn combined_pre_and_post_upgrade_usage_is_capped_after_reconciliation() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_combined");
    // Cap == the eventual TOTAL (2 pre-upgrade + 1 post-upgrade). A
    // violation therefore appears only once reconciliation makes the
    // pre-upgrade rows visible -- the exact bug issue #1226 describes.
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(3);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    // Two pre-upgrade RUNNING rows: started before the policy existed, so
    // quota_key is NULL exactly like a real rollout-window row.
    insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;
    insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;
    // One post-upgrade row: already correctly keyed by a real admission.
    insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        Some("acme"),
    )
    .await;

    let usage_before = load_quota_usage(&mut conn, workflow_name, "acme")
        .await
        .expect("usage before");
    assert_eq!(
        usage_before.active_executions, 1,
        "before reconciliation, only the already-keyed row is visible"
    );
    assert!(
        check_quota(&usage_before, &policy).is_none(),
        "usage of 1 against a cap of 3 must not violate -- the bug this test guards against"
    );

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");
    assert_eq!(summary.backfilled, 2);

    let usage_after = load_quota_usage(&mut conn, workflow_name, "acme")
        .await
        .expect("usage after");
    assert_eq!(
        usage_after.active_executions, 3,
        "after reconciliation, all three rows -- pre- and post-upgrade -- are visible"
    );
    let violation =
        check_quota(&usage_after, &policy).expect("combined usage must now violate the cap");
    assert_eq!(violation.current, 3);
    assert_eq!(violation.limit, 3);
}

#[tokio::test]
async fn second_run_is_a_no_op() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_idempotent");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "PAUSED",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let first = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("first sweep");
    assert_eq!(first.backfilled, 1);
    let key_after_first = read_quota_key(&mut conn, exec_id).await;
    assert_eq!(key_after_first, Some("acme".to_string()));

    let second = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("second sweep");
    assert_eq!(
        second,
        autumn_harvest::quota_reconcile::ReconcileSummary::default(),
        "a backfilled row no longer matches the candidate scan, so a re-run finds nothing"
    );
    assert_eq!(
        read_quota_key(&mut conn, exec_id).await,
        key_after_first,
        "the value must not change on a re-run"
    );
}

#[tokio::test]
async fn terminal_rows_are_never_touched() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_terminal");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "COMPLETED",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");

    assert_eq!(
        summary.total_scanned(),
        0,
        "a terminal row is never a candidate"
    );
    assert_eq!(read_quota_key(&mut conn, exec_id).await, None);
}

#[tokio::test]
async fn over_cap_resolved_key_is_left_null() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_over_cap");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let oversized = "x".repeat(300);
    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": oversized }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");

    assert_eq!(summary.over_cap, 1);
    assert_eq!(summary.backfilled, 0);
    assert_eq!(
        read_quota_key(&mut conn, exec_id).await,
        None,
        "an over-cap key must never reach the indexed column"
    );
}

#[tokio::test]
async fn workflow_type_with_no_declared_policy_is_never_a_candidate() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_no_policy");
    // A DIFFERENT workflow type has a policy, so the registry is
    // non-empty (not the zero-registrations early exit, covered
    // separately by `zero_registered_policies_anywhere_skips_the_scan_
    // entirely`). `workflow_name` itself is absent from the registered
    // set, so `CANDIDATE_SQL`'s `workflow_name = ANY($1)` filter excludes
    // this row from the scan entirely -- it is never fetched, not
    // fetched-then-classified `NoPolicy`. That is what stops a mixed
    // deployment from re-fetching every no-policy row's JSON input on
    // every tick forever.
    let other_workflow_name = leaked("wf_has_policy");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(other_workflow_name, policy).await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");

    assert_eq!(
        summary.total_scanned(),
        0,
        "excluded by the SQL filter, not scanned and classified NoPolicy"
    );
    assert_eq!(read_quota_key(&mut conn, exec_id).await, None);
}

#[tokio::test]
async fn zero_registered_policies_anywhere_skips_the_scan_entirely() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_no_policy_anywhere");
    let _guard = MetadataGuard::install_empty().await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 100, SHARD)
        .await
        .expect("sweep");

    assert_eq!(
        summary,
        autumn_harvest::quota_reconcile::ReconcileSummary::default(),
        "no workflow type anywhere declares a policy, so the scan itself is skipped"
    );
    assert_eq!(read_quota_key(&mut conn, exec_id).await, None);
}

#[tokio::test]
async fn zero_batch_size_disables_the_sweep() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_disabled");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let exec_id = insert_execution(
        &mut conn,
        workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    let summary = reconcile_quota_keys(&mut conn, 0, SHARD)
        .await
        .expect("sweep");

    assert_eq!(
        summary,
        autumn_harvest::quota_reconcile::ReconcileSummary::default()
    );
    assert_eq!(read_quota_key(&mut conn, exec_id).await, None);
}

#[tokio::test]
async fn batch_size_bounds_a_single_sweep_and_the_rest_finish_on_the_next_one() {
    let (mut conn, _container) = setup_db().await;
    let workflow_name = leaked("wf_batched");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(workflow_name, policy).await;

    let mut exec_ids = Vec::new();
    for _ in 0..5 {
        let id = insert_execution(
            &mut conn,
            workflow_name,
            "RUNNING",
            serde_json::json!({ "tenant_id": "acme" }),
            None,
        )
        .await;
        exec_ids.push(id);
    }

    let first = reconcile_quota_keys(&mut conn, 2, SHARD)
        .await
        .expect("first sweep");
    assert_eq!(
        first.backfilled, 2,
        "LIMIT $1 must cap one sweep to batch_size rows, not the full candidate set"
    );

    let mut backfilled_after_first = 0;
    for &id in &exec_ids {
        if read_quota_key(&mut conn, id).await.is_some() {
            backfilled_after_first += 1;
        }
    }
    assert_eq!(
        backfilled_after_first, 2,
        "exactly batch_size rows written, no more"
    );

    let second = reconcile_quota_keys(&mut conn, 2, SHARD)
        .await
        .expect("second sweep");
    assert_eq!(second.backfilled, 2);
    let third = reconcile_quota_keys(&mut conn, 2, SHARD)
        .await
        .expect("third sweep");
    assert_eq!(
        third.backfilled, 1,
        "the fifth and last row finishes on a later sweep"
    );

    for id in exec_ids {
        assert_eq!(
            read_quota_key(&mut conn, id).await,
            Some("acme".to_string())
        );
    }
}

#[tokio::test]
async fn cursor_advances_past_permanently_stuck_rows_so_a_resolvable_row_is_not_starved() {
    let (mut conn, _container) = setup_db().await;
    // `wf_stuck` has no registered policy -- every one of its rows
    // classifies `NoPolicy` and can never leave the candidate set. Row
    // `id`s are random UUIDs, so a stuck row can sort before OR after
    // the one resolvable row. A fixed `LIMIT` with no cursor could
    // therefore keep re-examining the same stuck rows forever, and
    // never reach the resolvable one. `wf_resolvable` carries the only
    // registered policy.
    let stuck_workflow_name = leaked("wf_stuck");
    let resolvable_workflow_name = leaked("wf_resolvable");
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let _guard = MetadataGuard::install_one(resolvable_workflow_name, policy).await;

    for _ in 0..3 {
        insert_execution(
            &mut conn,
            stuck_workflow_name,
            "RUNNING",
            serde_json::json!({ "tenant_id": "acme" }),
            None,
        )
        .await;
    }
    let resolvable_id = insert_execution(
        &mut conn,
        resolvable_workflow_name,
        "RUNNING",
        serde_json::json!({ "tenant_id": "acme" }),
        None,
    )
    .await;

    // 4 candidates total, batch_size 1: 4 ticks visit each exactly once
    // in one pass, regardless of id order. Run more than that. A wrapped
    // cursor (`None` once a batch returns fewer than batch_size rows)
    // then still finds the resolvable row, even if it sorted last.
    let mut cursor = None;
    for _ in 0..8 {
        let (_summary, next_cursor) = reconcile_quota_keys_from(&mut conn, 1, cursor, SHARD)
            .await
            .expect("sweep tick");
        cursor = next_cursor;
    }

    assert_eq!(
        read_quota_key(&mut conn, resolvable_id).await,
        Some("acme".to_string()),
        "the cursor must eventually reach the resolvable row despite 3 permanently stuck rows"
    );
}
