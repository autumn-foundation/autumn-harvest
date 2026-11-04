#![cfg(feature = "db")]
//! The concurrency limit override (issue #2407).
//!
//! These tests check the store and the raise pass against Postgres. The raise
//! pass lifts the stored cap of the live workflow task rows of one type. A
//! live row is a waiting start or the row of a run in flight. It never lowers
//! a cap.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16.

use autumn_harvest::concurrency::ConcurrencyPolicy;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::run_defaults::{
    CapRaise, MAX_CONCURRENCY_LIMIT, RECOMPUTE_BATCH, RunDefaultChange, RunDefaultPatch,
    get_run_default_override, raise_waiting_concurrency_caps, update_run_default_override,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel::sql_types::{Integer, Nullable, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The code policy of the types in these tests.
const CODE: ConcurrencyPolicy = ConcurrencyPolicy::new("input.tenant", 1);

/// Keeps a test database alive: its container, or the shared-database lock.
struct Hold {
    _container: Option<ContainerAsync<Postgres>>,
    _shared: Option<tokio::sync::MutexGuard<'static, ()>>,
}

async fn setup_db() -> (AsyncPgConnection, Hold) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let shared = crate::run_defaults_db_tests::SHARED_DB.lock().await;
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.batch_execute("DELETE FROM harvest_run_default_overrides")
            .await
            .expect("scrub");
        let hold = Hold {
            _container: None,
            _shared: Some(shared),
        };
        return (conn, hold);
    }
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
    let hold = Hold {
        _container: Some(container),
        _shared: None,
    };
    (conn, hold)
}

/// A unique type name, so rows of another test never match.
fn type_name(stem: &str) -> String {
    format!("{stem}_{}", Uuid::new_v4().simple())
}

fn set_limit(limit: u32) -> RunDefaultPatch {
    RunDefaultPatch {
        concurrency_limit: Some(Some(limit)),
        ..RunDefaultPatch::default()
    }
}

async fn change(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    patch: RunDefaultPatch,
) -> autumn_harvest::error::HarvestResult<()> {
    let change = RunDefaultChange {
        workflow_name,
        patch,
        clear: false,
        actor: "ops",
        reason: Some("INC-1"),
    };
    update_run_default_override(conn, &change).await.map(|_| ())
}

/// Insert one run of `workflow_name` with one workflow task row.
///
/// `cap` is the stored `concurrency_cap`. `None` writes no key and no cap.
/// The row takes a queue of its own, so a claim of another test on a shared
/// database never takes it.
async fn seed(conn: &mut AsyncPgConnection, workflow_name: &str, cap: Option<u32>) -> Uuid {
    seed_with_key(conn, workflow_name, cap.map(|_| "acme"), cap).await
}

/// As [`seed`], with the key and the cap set apart.
async fn seed_with_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    key: Option<&str>,
    cap: Option<u32>,
) -> Uuid {
    let queue = format!("conc_ovr_{}", Uuid::new_v4().simple());
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({ "tenant": "acme" }).into(),
        parent_id: None,
        queue_name: &queue,
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    let mut params = EnqueueParams::new(
        queue.as_str(),
        TaskType::Workflow,
        serde_json::json!({ "tenant": "acme" }),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.concurrency_key = key.map(str::to_string);
    params.max_concurrent = cap;
    queue::enqueue(conn, &params).await.expect("enqueue");
    exec_id.as_uuid()
}

/// Mark the workflow task of a run as parked between decisions.
async fn mark_parked(conn: &mut AsyncPgConnection, exec_id: Uuid) {
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = NULL \
         WHERE workflow_exec_id = $1",
    )
    .bind::<SqlUuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("mark parked");
}

/// Mark the workflow task of a run as claimed by a worker.
async fn mark_running(conn: &mut AsyncPgConnection, exec_id: Uuid) {
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w1', started_at = NOW() \
         WHERE workflow_exec_id = $1",
    )
    .bind::<SqlUuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("mark running");
}

#[derive(diesel::QueryableByName)]
struct CapRow {
    #[diesel(sql_type = Nullable<Integer>)]
    concurrency_cap: Option<i32>,
}

async fn cap(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Option<i32> {
    diesel::sql_query("SELECT concurrency_cap FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<SqlUuid, _>(exec_id)
        .get_result::<CapRow>(conn)
        .await
        .expect("read cap")
        .concurrency_cap
}

/// AC4: a limit alone is a stored override, and clearing it removes the row.
#[tokio::test]
async fn a_limit_only_override_round_trips_and_clears() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("limit_round_trip");
    change(&mut conn, &wf, set_limit(3)).await.expect("set");
    let stored = get_run_default_override(&mut conn, &wf)
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(stored.fields.concurrency_limit, Some(3));
    assert_eq!(stored.fields.sla, None);

    change(
        &mut conn,
        &wf,
        RunDefaultPatch {
            concurrency_limit: Some(None),
            ..RunDefaultPatch::default()
        },
    )
    .await
    .expect("clear");
    assert!(
        get_run_default_override(&mut conn, &wf)
            .await
            .expect("read")
            .is_none(),
        "a merge that sets no field deletes the row"
    );
}

/// The write refuses zero and a value past the `INTEGER` cap column.
#[tokio::test]
async fn a_zero_or_oversized_limit_is_refused() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("limit_bounds");
    let zero = change(&mut conn, &wf, set_limit(0)).await;
    assert!(zero.is_err(), "zero is refused");
    let over = change(&mut conn, &wf, set_limit(MAX_CONCURRENCY_LIMIT + 1)).await;
    assert!(over.is_err(), "a value past i32::MAX is refused");
    change(&mut conn, &wf, set_limit(MAX_CONCURRENCY_LIMIT))
        .await
        .expect("the bound itself is accepted");
}

/// The table `CHECK` refuses a zero limit, so a direct write cannot store one.
#[tokio::test]
async fn the_table_check_refuses_a_zero_limit() {
    let (mut conn, _hold) = setup_db().await;
    let insert = |limit: i32| {
        format!(
            "INSERT INTO harvest_run_default_overrides \
             (workflow_name, concurrency_limit, updated_by) \
             VALUES ('check_limit_{limit}', {limit}, 'ops')"
        )
    };
    conn.batch_execute(&insert(1))
        .await
        .expect("a limit of one is stored");
    let error = conn
        .batch_execute(&insert(0))
        .await
        .expect_err("the CHECK must refuse a zero limit");
    assert!(
        error
            .to_string()
            .contains("harvest_run_default_overrides_concurrency_limit_check"),
        "the column CHECK refuses it: {error}"
    );
}

/// AC2: a raise lifts the live rows of the type, and nothing else.
///
/// A run in flight keeps one workflow task row. Between decisions the row is
/// parked, and a wake sets it back to PENDING with its stored cap. So the raise
/// lifts a RUNNING row too, or the run waits behind the old cap.
#[tokio::test]
async fn the_raise_pass_lifts_waiting_rows_only() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("raise_waiting");
    let other = type_name("raise_other");
    let waiting_a = seed(&mut conn, &wf, Some(1)).await;
    let waiting_b = seed(&mut conn, &wf, Some(1)).await;
    let running = seed(&mut conn, &wf, Some(1)).await;
    mark_running(&mut conn, running).await;
    let parked = seed(&mut conn, &wf, Some(1)).await;
    mark_parked(&mut conn, parked).await;
    let already_higher = seed(&mut conn, &wf, Some(9)).await;
    let no_key = seed(&mut conn, &wf, None).await;
    let cap_no_key = seed_with_key(&mut conn, &wf, None, Some(1)).await;
    let ended = seed(&mut conn, &wf, Some(1)).await;
    diesel::sql_query("UPDATE harvest_workflow_executions SET state = 'CANCELLED' WHERE id = $1")
        .bind::<SqlUuid, _>(ended)
        .execute(&mut conn)
        .await
        .expect("end the run");
    let other_type = seed(&mut conn, &other, Some(1)).await;
    change(&mut conn, &wf, set_limit(4)).await.expect("set");

    let raise = raise_waiting_concurrency_caps(&mut conn, None, &wf, CODE, 4)
        .await
        .expect("raise");

    assert_eq!(
        raise,
        CapRaise {
            raised: 4,
            superseded: false
        }
    );
    assert_eq!(cap(&mut conn, waiting_a).await, Some(4));
    assert_eq!(cap(&mut conn, waiting_b).await, Some(4));
    assert_eq!(cap(&mut conn, running).await, Some(4), "a claimed row");
    assert_eq!(cap(&mut conn, parked).await, Some(4), "a parked row");
    assert_eq!(
        cap(&mut conn, already_higher).await,
        Some(9),
        "a raise never lowers a cap"
    );
    assert_eq!(cap(&mut conn, no_key).await, None);
    assert_eq!(
        cap(&mut conn, cap_no_key).await,
        Some(1),
        "a row with no key is not changed"
    );
    assert_eq!(
        cap(&mut conn, ended).await,
        Some(1),
        "a row of a terminal run is not changed"
    );
    assert_eq!(
        cap(&mut conn, other_type).await,
        Some(1),
        "another type is not changed"
    );
}

/// AC1: a lower limit changes no stored row.
#[tokio::test]
async fn a_lower_limit_changes_no_row() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("lower_limit");
    let code = ConcurrencyPolicy::new("input.tenant", 5);
    let waiting = seed(&mut conn, &wf, Some(5)).await;
    let running = seed(&mut conn, &wf, Some(5)).await;
    mark_running(&mut conn, running).await;
    change(&mut conn, &wf, set_limit(2)).await.expect("set");

    let raise = raise_waiting_concurrency_caps(&mut conn, None, &wf, code, 2)
        .await
        .expect("raise");

    assert_eq!(raise.raised, 0);
    assert_eq!(cap(&mut conn, waiting).await, Some(5));
    assert_eq!(cap(&mut conn, running).await, Some(5));
}

/// A newer change stops an older raise, so two changes never undo each other.
#[tokio::test]
async fn a_newer_change_stops_the_raise() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("raise_superseded");
    let waiting = seed(&mut conn, &wf, Some(1)).await;
    change(&mut conn, &wf, set_limit(2))
        .await
        .expect("newer change");

    let raise = raise_waiting_concurrency_caps(&mut conn, None, &wf, CODE, 6)
        .await
        .expect("raise");

    assert_eq!(
        raise,
        CapRaise {
            raised: 0,
            superseded: true
        }
    );
    assert_eq!(cap(&mut conn, waiting).await, Some(1));
}

/// AC4: with no stored override, a raise to the code limit runs and is not
/// superseded.
#[tokio::test]
async fn a_cleared_override_raises_to_the_code_limit() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("raise_to_code");
    let code = ConcurrencyPolicy::new("input.tenant", 3);
    let waiting = seed(&mut conn, &wf, Some(1)).await;

    let raise = raise_waiting_concurrency_caps(&mut conn, None, &wf, code, 3)
        .await
        .expect("raise");

    assert_eq!(raise.raised, 1);
    assert_eq!(cap(&mut conn, waiting).await, Some(3));
}

/// The pass runs in batches and still reaches every waiting row.
#[tokio::test]
async fn the_raise_pass_crosses_a_batch_boundary() {
    let (mut conn, _hold) = setup_db().await;
    let wf = type_name("raise_batches");
    let rows = usize::try_from(RECOMPUTE_BATCH).expect("batch fits") + 1;
    for _ in 0..rows {
        seed(&mut conn, &wf, Some(1)).await;
    }
    change(&mut conn, &wf, set_limit(2)).await.expect("set");

    let raise = raise_waiting_concurrency_caps(&mut conn, None, &wf, CODE, 2)
        .await
        .expect("raise");

    assert_eq!(raise.raised, u64::try_from(rows).expect("fits"));
    assert!(!raise.superseded);
}
