#![cfg(feature = "db")]
//! The quota limit override (issue #2408).
//!
//! These tests check the store, the table check and the admission against
//! Postgres. An admission reads the quota caps through
//! `GLOBAL_RUN_DEFAULTS`, so each enforcement test installs a cache there.
//! The tests share the serial lock of `quota_enforcement_tests`, because both
//! files change the same process globals.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16.

use std::sync::Arc;

use autumn_harvest::completion_trigger::GLOBAL_RUN_DEFAULTS;
use autumn_harvest::dlq::{NewDeadLetterEntry, dead_letter};
use autumn_harvest::error::HarvestError;
use autumn_harvest::execution::start_or_load_workflow_execution;
use autumn_harvest::quota::{QuotaLimits, QuotaLimitsPatch, QuotaPolicy, QuotaResource};
use autumn_harvest::run_defaults::{
    MAX_QUOTA_HISTORY_BYTES, RunDefaultChange, RunDefaultFields, RunDefaultPatch, RunDefaultsCache,
    get_run_default_override, load_run_default_overrides, update_run_default_override,
};
use autumn_harvest::types::ExecutionId;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state,
};
use crate::quota_enforcement_tests::{
    MetadataGuard, active_count, assert_quota_exceeded, connect, count_rows, detached_quota_child,
    detached_quota_parent, leaked, load_execution, mark_terminal, params, registry, row_state,
    serial, start_ok, start_root, try_start, wf_info,
};

/// The code policy of the enforcement tests: three active runs per tenant.
const CODE: QuotaPolicy = QuotaPolicy::new("tenant_id").with_max_active_executions(3);

/// Installs a run-default cache in [`GLOBAL_RUN_DEFAULTS`], and restores the
/// previous one on drop. The caller holds [`serial`].
struct CacheGuard {
    cache: Arc<RunDefaultsCache>,
    previous: Option<Arc<RunDefaultsCache>>,
}

impl CacheGuard {
    fn install() -> Self {
        let cache = Arc::new(RunDefaultsCache::new());
        let previous = GLOBAL_RUN_DEFAULTS
            .write()
            .expect("run-defaults lock")
            .replace(Arc::clone(&cache));
        Self { cache, previous }
    }

    /// Set the quota override of one type, as a `PATCH` write-through does.
    fn set(&self, workflow_name: &str, quota: QuotaLimits) {
        self.cache.write_through(
            workflow_name,
            RunDefaultFields {
                quota,
                ..RunDefaultFields::default()
            },
        );
    }

    /// Clear the override of one type.
    fn clear(&self, workflow_name: &str) {
        self.cache
            .write_through(workflow_name, RunDefaultFields::default());
    }
}

impl Drop for CacheGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_RUN_DEFAULTS.write() {
            *lock = self.previous.take();
        }
    }
}

const fn active(max: u32) -> QuotaLimits {
    QuotaLimits {
        max_active_executions: Some(max),
        max_history_bytes: None,
        max_dead_letters: None,
    }
}

fn tenant(name: &str) -> serde_json::Value {
    serde_json::json!({ "tenant_id": name })
}

fn wid() -> String {
    format!("wid-{}", Uuid::new_v4().simple())
}

async fn change(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    patch: RunDefaultPatch,
    clear: bool,
) -> autumn_harvest::error::HarvestResult<Option<RunDefaultFields>> {
    let change = RunDefaultChange {
        workflow_name,
        patch,
        clear,
        actor: "ops",
        reason: Some("INC-2408"),
    };
    update_run_default_override(conn, &change)
        .await
        .map(|stored| stored.map(|s| s.fields))
}

fn quota_patch(quota: QuotaLimitsPatch) -> RunDefaultPatch {
    RunDefaultPatch {
        quota,
        ..RunDefaultPatch::default()
    }
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_quota_only_override_round_trips_and_clears() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = leaked("quota_ovr_store");

    let set = QuotaLimitsPatch {
        max_active_executions: Some(Some(2)),
        max_history_bytes: Some(Some(MAX_QUOTA_HISTORY_BYTES)),
        max_dead_letters: Some(Some(u32::MAX)),
    };
    let stored = change(&mut conn, name, quota_patch(set), false)
        .await
        .expect("store")
        .expect("a row");
    let expected = QuotaLimits {
        max_active_executions: Some(2),
        max_history_bytes: Some(MAX_QUOTA_HISTORY_BYTES),
        max_dead_letters: Some(u32::MAX),
    };
    assert_eq!(stored.quota, expected);
    let loaded = get_run_default_override(&mut conn, name)
        .await
        .expect("load")
        .expect("a row");
    assert_eq!(loaded.fields.quota, expected);
    assert!(
        load_run_default_overrides(&mut conn)
            .await
            .expect("load all")
            .iter()
            .any(|o| o.workflow_name == name && o.fields.quota == expected)
    );

    // A patch that names one cap keeps the other two.
    let one = QuotaLimitsPatch {
        max_dead_letters: Some(None),
        ..QuotaLimitsPatch::default()
    };
    let stored = change(&mut conn, name, quota_patch(one), false)
        .await
        .expect("change")
        .expect("a row");
    assert_eq!(stored.quota.max_dead_letters, None);
    assert_eq!(stored.quota.max_active_executions, Some(2));

    // Clearing the override deletes the row, so the code values apply again.
    let cleared = change(&mut conn, name, RunDefaultPatch::default(), true)
        .await
        .expect("clear");
    assert_eq!(cleared, None);
    assert!(
        get_run_default_override(&mut conn, name)
            .await
            .expect("load")
            .is_none()
    );
}

#[tokio::test]
async fn a_zero_or_oversized_quota_cap_is_refused() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = leaked("quota_ovr_bounds");

    for patch in [
        QuotaLimitsPatch {
            max_active_executions: Some(Some(0)),
            ..QuotaLimitsPatch::default()
        },
        QuotaLimitsPatch {
            max_history_bytes: Some(Some(0)),
            ..QuotaLimitsPatch::default()
        },
        QuotaLimitsPatch {
            max_history_bytes: Some(Some(MAX_QUOTA_HISTORY_BYTES + 1)),
            ..QuotaLimitsPatch::default()
        },
        QuotaLimitsPatch {
            max_dead_letters: Some(Some(0)),
            ..QuotaLimitsPatch::default()
        },
    ] {
        let refused = change(&mut conn, name, quota_patch(patch), false).await;
        assert!(
            matches!(refused, Err(HarvestError::Config(_))),
            "{patch:?} is refused: {refused:?}"
        );
    }
    assert!(
        get_run_default_override(&mut conn, name)
            .await
            .expect("load")
            .is_none(),
        "a refused change stores nothing"
    );
}

#[tokio::test]
async fn the_table_check_refuses_a_cap_that_the_engine_cannot_read() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for column in [
        "quota_max_active_executions",
        "quota_max_history_bytes",
        "quota_max_dead_letters",
    ] {
        let name = leaked("quota_ovr_check");
        let sql = format!(
            "INSERT INTO harvest_run_default_overrides \
             (workflow_name, {column}, updated_by, created_at, updated_at) \
             VALUES ($1, 0, 'ops', NOW(), NOW())"
        );
        let refused = diesel::sql_query(sql)
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await;
        assert!(refused.is_err(), "{column} = 0 is refused");
        if column != "quota_max_history_bytes" {
            // A count past `u32::MAX` would fail the load of every override.
            let sql = format!(
                "INSERT INTO harvest_run_default_overrides \
                 (workflow_name, {column}, updated_by, created_at, updated_at) \
                 VALUES ($1, 4294967296, 'ops', NOW(), NOW())"
            );
            let refused = diesel::sql_query(sql)
                .bind::<diesel::sql_types::Text, _>(name)
                .execute(&mut conn)
                .await;
            assert!(refused.is_err(), "{column} past u32::MAX is refused");
        }
        let sql = format!(
            "INSERT INTO harvest_run_default_overrides \
             (workflow_name, {column}, updated_by, created_at, updated_at) \
             VALUES ($1, 1, 'ops', NOW(), NOW())"
        );
        diesel::sql_query(sql)
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("a row that sets {column} alone is valid: {e}"));
    }
}

// ---------------------------------------------------------------------------
// AC1: a lower limit refuses new work and never ends a run in flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_lower_limit_refuses_new_work_and_keeps_the_runs_in_flight() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_ovr_lower");
    let _meta = MetadataGuard::install_one(wf, CODE);
    let cache = CacheGuard::install();

    let mut live = Vec::new();
    for _ in 0..3 {
        live.push(start_ok(&mut conn, wf, tenant("acme")).await);
    }
    assert_eq!(active_count(&mut conn, wf, "acme").await, 3);

    cache.set(wf, active(2));
    let (refused_id, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    let error = outcome.expect_err("acme is over the lower limit");
    assert_quota_exceeded(
        &error,
        wf,
        "acme",
        QuotaResource::ActiveExecutions,
        2,
        |c| c == 3,
    );
    assert_eq!(
        active_count(&mut conn, wf, "acme").await,
        3,
        "no run in flight ends"
    );
    for exec_id in &live {
        assert_eq!(row_state(&mut conn, *exec_id).await, "RUNNING");
    }
    let refused = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(refused_id.as_uuid())
    .get_result::<Count>(&mut conn)
    .await
    .expect("count");
    assert_eq!(refused.n, 0, "the refused start leaves no row");

    // Another tenant under the lower limit still starts.
    start_ok(&mut conn, wf, tenant("globex")).await;

    // A run that ends frees one slot, but acme is still at the lower limit.
    mark_terminal(&mut conn, live[0], "COMPLETED").await;
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    outcome.expect_err("2 active runs meet the lower limit of 2");
    mark_terminal(&mut conn, live[1], "COMPLETED").await;
    start_ok(&mut conn, wf, tenant("acme")).await;
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A workflow retry continues a run that was admitted. A lower limit does not
/// refuse it, as the code cap does not.
#[tokio::test]
async fn a_lower_limit_never_refuses_a_workflow_retry() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_ovr_retry");
    let _meta = MetadataGuard::install_one(wf, CODE);
    let cache = CacheGuard::install();

    let first = start_ok(&mut conn, wf, tenant("acme")).await;
    cache.set(wf, active(1));
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    outcome.expect_err("a fresh start is refused at the lower limit");

    let workflow_id = wid();
    let exec_id = ExecutionId::new();
    let mut retry = params(wf, &workflow_id, exec_id, tenant("acme"));
    retry.retry_of_exec_id = Some(first.as_uuid());
    retry.workflow_attempt = 2;
    start_or_load_workflow_execution(&mut conn, retry, None)
        .await
        .expect("a retry continuation is exempt");
}

/// A run in flight reaches admission only when it spawns a child. A lower
/// cap on the child type defers the parent with a backoff. It does not fail
/// the parent. A clear restores the code cap, and the parent completes.
#[tokio::test]
async fn a_lower_limit_defers_a_child_spawn_and_never_fails_the_parent() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let parent_wf = leaked("quota_ovr_parent");
    let child_wf = leaked("quota_ovr_child");
    let code = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    let mut child_info = wf_info(child_wf, detached_quota_child);
    child_info.quota = Some(code);
    let reg = registry(vec![wf_info(parent_wf, detached_quota_parent), child_info]);

    // One live child of the tenant fits the code cap of 2. Its task row goes,
    // so the worker never ends it.
    let meta = MetadataGuard::install_one(child_wf, code);
    let blocker = start_root(&mut conn, child_wf, &wid(), tenant("acme")).await;
    drop(meta);
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(blocker.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete the blocker task row");

    // The override lowers the cap to 1. It is stored, so a cache reload of
    // the worker keeps it.
    let lower = QuotaLimitsPatch {
        max_active_executions: Some(Some(1)),
        ..QuotaLimitsPatch::default()
    };
    let stored = change(&mut conn, child_wf, quota_patch(lower), false)
        .await
        .expect("store the override")
        .expect("a row");
    reg.run_defaults().write_through(child_wf, stored);

    let parent = start_root(
        &mut conn,
        parent_wf,
        &wid(),
        serde_json::json!({ "child_type": child_wf }),
    )
    .await;
    let worker = build_runtime_worker("w-2408-child-quota", 2, 1, Arc::clone(&reg));
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let children =
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions WHERE workflow_name = $1";
    assert_eq!(
        load_execution(&mut conn, parent).await.state,
        "RUNNING",
        "the lower cap defers the parent and never fails it"
    );
    assert_eq!(
        count_rows(&mut conn, children, &[child_wf]).await,
        1,
        "the lower cap refuses the child spawn"
    );
    assert_eq!(row_state(&mut conn, blocker).await, "RUNNING");

    change(&mut conn, child_wf, RunDefaultPatch::default(), true)
        .await
        .expect("clear the override");
    reg.run_defaults()
        .write_through(child_wf, RunDefaultFields::default());
    wait_for_execution_state(&url, parent, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");
    assert_eq!(
        count_rows(&mut conn, children, &[child_wf]).await,
        2,
        "the code cap admits the child after the clear"
    );
}

// ---------------------------------------------------------------------------
// A higher limit and a clear
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_higher_limit_admits_and_a_clear_restores_the_code_limit() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_ovr_raise");
    let _meta = MetadataGuard::install_one(wf, CODE);
    let cache = CacheGuard::install();

    for _ in 0..3 {
        start_ok(&mut conn, wf, tenant("acme")).await;
    }
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    outcome.expect_err("the code limit of 3 refuses a fourth run");

    cache.set(wf, active(4));
    start_ok(&mut conn, wf, tenant("acme")).await;
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    let error = outcome.expect_err("4 runs meet the higher limit");
    assert_quota_exceeded(
        &error,
        wf,
        "acme",
        QuotaResource::ActiveExecutions,
        4,
        |c| c == 4,
    );

    cache.clear(wf);
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    let error = outcome.expect_err("the code limit applies again");
    assert_quota_exceeded(
        &error,
        wf,
        "acme",
        QuotaResource::ActiveExecutions,
        3,
        |c| c == 4,
    );
}

/// An override can set a cap that code leaves unset, on a type with another
/// admission cap.
#[tokio::test]
async fn an_override_can_add_a_dead_letter_cap() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_ovr_dlq");
    let _meta = MetadataGuard::install_one(wf, CODE);
    let cache = CacheGuard::install();

    let seed = start_ok(&mut conn, wf, tenant("acme")).await;
    dead_letter(
        &mut conn,
        &NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "activity".to_string(),
            workflow_exec_id: Some(seed.as_uuid()),
            activity_name: Some("do_thing".to_string()),
            input: serde_json::json!({}),
            error: "boom".to_string(),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("insert a dead letter");
    start_ok(&mut conn, wf, tenant("acme")).await;

    cache.set(
        wf,
        QuotaLimits {
            max_dead_letters: Some(1),
            ..QuotaLimits::default()
        },
    );
    let (_, outcome) = try_start(&mut conn, wf, &wid(), tenant("acme")).await;
    let error = outcome.expect_err("one dead letter meets the override cap");
    assert_quota_exceeded(&error, wf, "acme", QuotaResource::DeadLetters, 1, |c| {
        c == 1
    });
}

/// A type with no admission cap admits each start, with an override stored.
/// `enforce_quota_admission` returns before it reads the override, and
/// `QuotaLimits::apply` ignores one too. The unit test
/// `quota_limits_never_apply_to_a_policy_with_no_admission_cap` covers the
/// second guard.
#[tokio::test]
async fn a_type_with_no_admission_cap_admits_with_an_override_stored() {
    let _serial = serial().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let wf = leaked("quota_ovr_capless");
    let _meta = MetadataGuard::install_one(wf, QuotaPolicy::new("tenant_id"));
    let cache = CacheGuard::install();
    cache.set(wf, active(1));
    for _ in 0..3 {
        start_ok(&mut conn, wf, tenant("acme")).await;
    }
}
