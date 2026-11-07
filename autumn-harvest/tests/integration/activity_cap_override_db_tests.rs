#![cfg(feature = "db")]
//! The operator override of the `max_concurrent` cap of an activity type.
//!
//! These tests check the store, the table check, the rewrite of the live
//! rows and each acceptance criterion against Postgres. The claim tests
//! drive the queue directly, so each claim is exact. The worker test drives
//! a real worker, and its refresh loop loads the override from the table.
//! Each test uses fresh names and its own queue.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::activity_defaults::{
    ActivityDefaultChange, ActivityDefaultFields, ActivityDefaultOverride, ActivityDefaultPatch,
    CapRewrite, get_activity_default_override, load_activity_default_overrides,
    set_live_activity_caps, update_activity_default_override,
};
use autumn_harvest::error::HarvestResult;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::queue::{self, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{
    ActivityContext, ExecutionId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::integration_e2e::{
    build_test_pool, runtime_config, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// The cap that code declares in each test.
const CODE_CAP: u32 = 3;

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

/// A short, unique queue name.
///
/// The NOTIFY channel of a queue is a Postgres identifier of 63 bytes or
/// fewer, so the name stays short.
fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

const fn cap_patch(cap: Option<u32>) -> ActivityDefaultPatch {
    ActivityDefaultPatch {
        max_concurrent: Some(cap),
        ..empty_patch()
    }
}

const fn empty_patch() -> ActivityDefaultPatch {
    ActivityDefaultPatch {
        retry: autumn_harvest::workflow_retry::RetryLimitsPatch {
            max_attempts: None,
            initial_interval: None,
            max_interval: None,
        },
        start_to_close: None,
        heartbeat_timeout: None,
        schedule_to_start: None,
        schedule_to_close: None,
        max_concurrent: None,
    }
}

async fn change(
    conn: &mut AsyncPgConnection,
    name: &str,
    patch: ActivityDefaultPatch,
    clear: bool,
) -> HarvestResult<Option<ActivityDefaultOverride>> {
    let change = ActivityDefaultChange {
        activity_name: name,
        patch,
        clear,
        actor: "ops",
        reason: Some("protect the downstream"),
    };
    update_activity_default_override(conn, &change).await
}

/// Run the rewrite on the one shard of the test database.
async fn rewrite(conn: &mut AsyncPgConnection, name: &str, cap: u32) -> CapRewrite {
    set_live_activity_caps(conn, None, name, CODE_CAP, cap)
        .await
        .expect("rewrite")
}

/// Insert a bare execution row, so a task row has an owner.
async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'aco_owner', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue one task of `activity` under its key, as the worker does.
async fn enqueue(conn: &mut AsyncPgConnection, queue: &str, activity: &str, cap: u32) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some(activity.to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.concurrency_key = Some(activity.to_string());
    params.max_concurrent = Some(cap);
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn claim(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> Option<TaskQueueItem> {
    queue::claim_task(conn, &[queue.to_string()], worker, "", None, &[], &[])
        .await
        .expect("claim")
}

async fn row(conn: &mut AsyncPgConnection, id: Uuid) -> TaskQueueItem {
    harvest_task_queue::table
        .find(id)
        .select(TaskQueueItem::as_select())
        .first(conn)
        .await
        .expect("row")
}

async fn caps(conn: &mut AsyncPgConnection, ids: &[Uuid]) -> Vec<Option<i32>> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(row(conn, *id).await.concurrency_cap);
    }
    out
}

fn claim_of(item: &TaskQueueItem) -> TaskClaim {
    TaskClaim::new(
        item.id,
        item.worker_id.clone().expect("a worker"),
        item.attempt,
    )
}

// ---------------------------------------------------------------------------
// The store.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cap_round_trips_and_keeps_the_other_fields() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_store");

    let both = ActivityDefaultPatch {
        start_to_close: Some(Some(Duration::from_secs(90))),
        ..cap_patch(Some(2))
    };
    let stored = change(&mut conn, &name, both, false)
        .await
        .expect("set")
        .expect("a row");
    assert_eq!(stored.fields.max_concurrent, Some(2));
    assert_eq!(stored.fields.start_to_close, Some(Duration::from_secs(90)));
    assert!(
        load_activity_default_overrides(&mut conn)
            .await
            .expect("load")
            .iter()
            .any(|row| row.activity_name == name && row.fields.max_concurrent == Some(2))
    );

    let kept = change(&mut conn, &name, cap_patch(None), false)
        .await
        .expect("clear the cap")
        .expect("the timeout keeps the row");
    assert_eq!(kept.fields.max_concurrent, None);
    assert_eq!(kept.fields.start_to_close, Some(Duration::from_secs(90)));

    let only_cap = unique("aco_only");
    let stored = change(&mut conn, &only_cap, cap_patch(Some(1)), false)
        .await
        .expect("set")
        .expect("a cap alone is a row");
    assert_eq!(
        stored.fields,
        ActivityDefaultFields {
            max_concurrent: Some(1),
            ..ActivityDefaultFields::default()
        }
    );
    assert!(
        change(&mut conn, &only_cap, cap_patch(None), false)
            .await
            .expect("clear")
            .is_none(),
        "a row that sets no field is deleted"
    );
    assert!(
        get_activity_default_override(&mut conn, &only_cap)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn the_write_and_the_table_refuse_a_zero_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_zero");

    let refused = change(&mut conn, &name, cap_patch(Some(0)), false)
        .await
        .expect_err("zero");
    assert!(refused.to_string().contains("max_concurrent"), "{refused}");

    let error = diesel::sql_query(
        "INSERT INTO harvest_activity_default_overrides \
         (activity_name, max_concurrent, updated_by) VALUES ($1, 0, 'ops')",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect_err("the table refuses zero");
    assert!(
        error
            .to_string()
            .contains("harvest_activity_default_overrides_max_concurrent_check"),
        "{error}"
    );

    diesel::sql_query(
        "INSERT INTO harvest_activity_default_overrides \
         (activity_name, max_concurrent, updated_by) VALUES ($1, 2147483647, 'ops')",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect("a cap alone passes the row check");
    let stored = get_activity_default_override(&mut conn, &name)
        .await
        .expect("the bound decodes")
        .expect("a row");
    assert_eq!(stored.fields.max_concurrent, Some(2_147_483_647));
}

// ---------------------------------------------------------------------------
// The rewrite and each acceptance criterion, at the claim.
// ---------------------------------------------------------------------------

/// AC1 and AC2.
///
/// Two tasks run and two wait under the code cap of 3. The cap drops to 1.
/// No claim admits a task while the two run. Each running task keeps its
/// claim and completes. Then the claims admit one task at a time.
#[tokio::test]
async fn a_lower_cap_holds_for_each_claim_and_a_task_in_flight_runs_on() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_lower");
    let queue = unique_queue("aco-lower");

    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(enqueue(&mut conn, &queue, &name, CODE_CAP).await);
    }
    let first = claim(&mut conn, &queue, "w1").await.expect("first");
    let second = claim(&mut conn, &queue, "w1").await.expect("second");

    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    let done = rewrite(&mut conn, &name, 1).await;
    assert_eq!(
        done,
        CapRewrite {
            rewritten: 4,
            superseded: false
        },
        "the pending and the running rows take the new cap"
    );
    assert_eq!(caps(&mut conn, &ids).await, vec![Some(1); 4]);

    // AC1: two run, and the cap is 1, so no claim admits a pending task.
    assert!(
        claim(&mut conn, &queue, "w2").await.is_none(),
        "a pending task waits under the lower cap"
    );

    // AC2: each running task keeps its claim, its worker and its attempt.
    for running in [&first, &second] {
        let now = row(&mut conn, running.id).await;
        assert_eq!(now.state, "RUNNING");
        assert_eq!(now.worker_id, running.worker_id);
        assert_eq!(now.attempt, running.attempt);
        queue::complete_task(&mut conn, &claim_of(running), json!("ok"))
            .await
            .expect("a task in flight completes under its claim");
    }

    // AC1: the claims now admit one task at a time.
    let third = claim(&mut conn, &queue, "w2").await.expect("one task");
    assert!(
        claim(&mut conn, &queue, "w2").await.is_none(),
        "the lower cap holds for the next claim"
    );
    queue::complete_task(&mut conn, &claim_of(&third), json!("ok"))
        .await
        .expect("complete");
    let fourth = claim(&mut conn, &queue, "w2").await.expect("the last task");
    assert_eq!(fourth.concurrency_cap, Some(1));
}

/// A retry sets its row back to `PENDING` with its stored cap. The rewrite
/// covers a running row, so its retry waits under the new cap too.
#[tokio::test]
async fn a_retry_of_a_task_in_flight_waits_under_the_new_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_retry");
    let queue = unique_queue("aco-retry");

    enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    let failing = claim(&mut conn, &queue, "w1").await.expect("first");
    let other = claim(&mut conn, &queue, "w1").await.expect("second");

    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    rewrite(&mut conn, &name, 1).await;

    let write = queue::requeue_claimed_task_for_retry(
        &mut conn,
        &claim_of(&failing),
        chrono::Duration::zero(),
        "transient",
    )
    .await
    .expect("retry");
    assert_eq!(write, queue::ClaimWrite::Applied);
    let retried = row(&mut conn, failing.id).await;
    assert_eq!(retried.state, "PENDING");
    assert_eq!(retried.concurrency_cap, Some(1));
    assert!(
        claim(&mut conn, &queue, "w2").await.is_none(),
        "the retry waits while one task runs"
    );
    queue::complete_task(&mut conn, &claim_of(&other), json!("ok"))
        .await
        .expect("complete");
    assert_eq!(
        claim(&mut conn, &queue, "w2").await.map(|t| t.id),
        Some(failing.id)
    );
}

/// AC3: a clear restores the code cap. The claims admit three tasks again.
#[tokio::test]
async fn a_clear_restores_the_code_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_clear");
    let queue = unique_queue("aco-clear");

    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(enqueue(&mut conn, &queue, &name, CODE_CAP).await);
    }
    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    rewrite(&mut conn, &name, 1).await;
    claim(&mut conn, &queue, "w1").await.expect("one");
    assert!(claim(&mut conn, &queue, "w1").await.is_none());

    assert!(
        change(&mut conn, &name, empty_patch(), true)
            .await
            .expect("clear")
            .is_none()
    );
    let done = rewrite(&mut conn, &name, CODE_CAP).await;
    assert_eq!(done.rewritten, 4);
    assert!(!done.superseded);
    assert_eq!(caps(&mut conn, &ids).await, vec![Some(3); 4]);
    claim(&mut conn, &queue, "w1").await.expect("two");
    claim(&mut conn, &queue, "w1").await.expect("three");
    assert!(
        claim(&mut conn, &queue, "w1").await.is_none(),
        "the code cap of 3 holds"
    );
}

/// A newer change owns the rows. An older pass stops and changes nothing.
#[tokio::test]
async fn a_pass_for_a_replaced_cap_stops_and_changes_no_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_stale");
    let queue = unique_queue("aco-stale");

    let id = enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    let done = rewrite(&mut conn, &name, 2).await;
    assert_eq!(
        done,
        CapRewrite {
            rewritten: 0,
            superseded: true
        }
    );
    assert_eq!(caps(&mut conn, &[id]).await, vec![Some(3)]);
}

/// A process with a stale cache can still store the old cap. The next pass
/// rewrites that row. A row that already holds the cap is not counted.
#[tokio::test]
async fn a_settle_pass_rewrites_a_row_that_a_stale_process_wrote() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_settle");
    let queue = unique_queue("aco-settle");

    let first = enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    assert_eq!(rewrite(&mut conn, &name, 1).await.rewritten, 1);
    let stale = enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    assert_eq!(rewrite(&mut conn, &name, 1).await.rewritten, 1);
    assert_eq!(caps(&mut conn, &[first, stale]).await, vec![Some(1); 2]);
    assert_eq!(rewrite(&mut conn, &name, 1).await.rewritten, 0);
}

/// The pass changes the live rows of its activity only.
#[tokio::test]
async fn the_rewrite_changes_only_the_live_rows_of_its_activity() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("aco_scope");
    let other = unique("aco_other");
    let queue = unique_queue("aco-scope");

    let finished = enqueue(&mut conn, &queue, &name, CODE_CAP).await;
    let item = claim(&mut conn, &queue, "w1").await.expect("claim");
    queue::complete_task(&mut conn, &claim_of(&item), json!("ok"))
        .await
        .expect("complete");
    let foreign = enqueue(&mut conn, &queue, &other, CODE_CAP).await;
    let live = enqueue(&mut conn, &queue, &name, CODE_CAP).await;

    change(&mut conn, &name, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    assert_eq!(rewrite(&mut conn, &name, 1).await.rewritten, 1);
    assert_eq!(
        caps(&mut conn, &[finished, foreign, live]).await,
        vec![Some(3), Some(3), Some(1)]
    );
}

// ---------------------------------------------------------------------------
// The acceptance criteria through a worker.
// ---------------------------------------------------------------------------

const WORKER_QUEUE: &str = "q-activity-cap-override-worker";
const WORKER_STEP: &str = "aco_worker_step";
const WORKER_WF: &str = "aco_worker_wf";
/// The tasks of one run.
const FAN_OUT: usize = 6;

/// Each step waits for this flag when its input asks it to wait.
static RELEASE: AtomicBool = AtomicBool::new(false);
/// The steps that run now.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// The most steps that ran at one time since the last reset.
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn worker_step(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(now, Ordering::SeqCst);
        if input["wait"] == json!(true) {
            while !AtomicBool::load(&RELEASE, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        Ok(input)
    })
}

fn worker_step_info() -> ActivityInfo {
    ActivityInfo {
        name: WORKER_STEP,
        module: "activity_cap_override_db_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(60)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(WORKER_QUEUE),
        max_concurrent: Some(CODE_CAP),
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: worker_step,
        input_schema: None,
        output_schema: None,
    }
}

/// Schedule [`FAN_OUT`] steps at once.
fn worker_wf(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let steps = (0..FAN_OUT)
            .map(|i| {
                (
                    WORKER_STEP.to_string(),
                    json!({"i": i, "wait": input["wait"].clone()}),
                    WORKER_QUEUE.to_string(),
                )
            })
            .collect();
        ctx.execute_activity_fan_out_raw(steps)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

fn worker_wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKER_WF,
        module: "activity_cap_override_db_tests",
        handler: worker_wf,
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

async fn start(conn: &mut AsyncPgConnection, wait: bool) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams::new(
            WORKER_WF,
            &unique("aco-run"),
            ExecutionId::new(),
            json!({"wait": wait}),
            WORKER_QUEUE,
        ),
        None,
    )
    .await
    .expect("start")
    .exec_id
}

async fn activity_rows(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::activity_name.is_not_null())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("task rows")
}

/// Wait until `check` holds for the activity rows of a run.
async fn wait_for_rows(
    url: &str,
    exec_id: ExecutionId,
    what: &str,
    check: impl Fn(&[TaskQueueItem]) -> bool + Send + Sync,
) -> Vec<TaskQueueItem> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = activity_rows(url, exec_id).await;
            if check(&rows) {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

fn count(rows: &[TaskQueueItem], state: &str) -> usize {
    rows.iter().filter(|row| row.state == state).count()
}

async fn wait_for_cache(registry: &HandlerRegistry, expected: Option<ActivityDefaultFields>) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while registry.run_defaults().activity(WORKER_STEP) != expected {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the refresh loop loads the change");
}

/// AC1, AC2 and AC3 through a worker with six free activity slots.
///
/// Run one schedules six steps under the code cap of 3. Three run and wait,
/// and three are pending. The cap drops to 1. The three in flight run on and
/// complete. Then the pending steps run one at a time. Run two is scheduled
/// under the override, so its rows store the cap of 1. A clear restores the
/// cap of 3 for run three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_runs_pending_tasks_under_a_lower_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    // A shared cluster can hold rows from an earlier run of this test. A
    // stale running row would hold a slot of the key.
    change(&mut conn, WORKER_STEP, empty_patch(), true)
        .await
        .expect("scrub");
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(WORKER_QUEUE)
        .execute(&mut conn)
        .await
        .expect("scrub the queue");
    RELEASE.store(false, Ordering::SeqCst);
    IN_FLIGHT.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![worker_wf_info()],
        vec![worker_step_info()],
        autumn_harvest::context::empty_shared_state(),
        Arc::new(autumn_harvest::telemetry::TelemetryConfig::default()),
    ));
    let mut config = runtime_config("w-aco", 1, FAN_OUT, Duration::from_secs(10));
    config.queues = vec![WORKER_QUEUE.to_string()];
    let pool: DbPool = build_test_pool(&url);
    let handle = spawn_test_worker(
        Arc::new(Worker::new(config, Arc::clone(&registry)).expect("worker")),
        pool,
    );

    let first = start(&mut conn, true).await;
    let rows = wait_for_rows(&url, first, "three running steps", |rows| {
        rows.len() == FAN_OUT && count(rows, "RUNNING") == 3
    })
    .await;
    assert_eq!(count(&rows, "PENDING"), 3, "the code cap of 3 holds");
    assert!(rows.iter().all(|row| row.concurrency_cap == Some(3)));

    change(&mut conn, WORKER_STEP, cap_patch(Some(1)), false)
        .await
        .expect("lower");
    let done = rewrite(&mut conn, WORKER_STEP, 1).await;
    assert_eq!(done.rewritten, 6, "the pending and the running rows");
    wait_for_cache(
        &registry,
        Some(ActivityDefaultFields {
            max_concurrent: Some(1),
            ..ActivityDefaultFields::default()
        }),
    )
    .await;

    // AC2: the three steps in flight run on.
    let rows = activity_rows(&url, first).await;
    assert_eq!(count(&rows, "RUNNING"), 3, "no step in flight stops");
    assert_eq!(count(&rows, "PENDING"), 3);

    // Let the three in flight finish. Each later step must run alone.
    RELEASE.store(true, Ordering::SeqCst);
    wait_for_rows(&url, first, "the three steps in flight to finish", |rows| {
        count(rows, "RUNNING") == 0 && count(rows, "COMPLETED") >= 3
    })
    .await;
    PEAK.store(
        AtomicUsize::load(&IN_FLIGHT, Ordering::SeqCst),
        Ordering::SeqCst,
    );
    wait_for_execution_state_with_timeout(&url, first, "COMPLETED", Duration::from_secs(30)).await;
    assert_eq!(
        AtomicUsize::load(&PEAK, Ordering::SeqCst),
        1,
        "AC1: the pending steps run one at a time"
    );

    // A step scheduled under the override stores the new cap.
    let second = start(&mut conn, false).await;
    wait_for_execution_state_with_timeout(&url, second, "COMPLETED", Duration::from_secs(30)).await;
    let rows = activity_rows(&url, second).await;
    assert_eq!(rows.len(), FAN_OUT);
    assert!(rows.iter().all(|row| row.concurrency_cap == Some(1)));
    assert_eq!(
        AtomicUsize::load(&PEAK, Ordering::SeqCst),
        1,
        "run two runs one at a time"
    );

    // AC3: a clear restores the code cap.
    change(&mut conn, WORKER_STEP, empty_patch(), true)
        .await
        .expect("clear");
    wait_for_cache(&registry, None).await;
    RELEASE.store(false, Ordering::SeqCst);
    let third = start(&mut conn, true).await;
    let rows = wait_for_rows(&url, third, "three running steps again", |rows| {
        rows.len() == FAN_OUT && count(rows, "RUNNING") == 3
    })
    .await;
    assert!(rows.iter().all(|row| row.concurrency_cap == Some(3)));
    RELEASE.store(true, Ordering::SeqCst);
    wait_for_execution_state_with_timeout(&url, third, "COMPLETED", Duration::from_secs(30)).await;

    handle.abort();
}

// ---------------------------------------------------------------------------
// AC1 for a task that a dead-letter replay or redrive puts back.
// ---------------------------------------------------------------------------

/// A registry with one capped activity, and an override in its cache.
fn dlq_registry(name: &'static str, cap: Option<u32>) -> HandlerRegistry {
    let mut info = worker_step_info();
    info.name = name;
    let registry = HandlerRegistry::new(vec![], vec![info]);
    if let Some(cap) = cap {
        registry.run_defaults().write_through_activity(
            name,
            ActivityDefaultFields {
                max_concurrent: Some(cap),
                ..ActivityDefaultFields::default()
            },
        );
    }
    registry
}

/// A dead letter of one activity task, owned by an execution in `state`.
async fn dead_letter(conn: &mut AsyncPgConnection, name: &str, state: &str) -> Uuid {
    let exec_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, state, input) \
         VALUES ($1, 'aco_owner', $2, 0, $3, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(conn)
    .await
    .expect("insert execution");
    autumn_harvest::dlq::dead_letter(
        conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: unique_queue("aco-dlq"),
            task_type: "activity".to_string(),
            workflow_exec_id: Some(exec_id),
            activity_name: Some(name.to_string()),
            input: json!({}),
            error: "downstream refused".to_string(),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead letter")
}

/// A replayed task takes the key of its activity and the override cap. With
/// no override it takes the code cap.
#[tokio::test]
async fn a_replayed_activity_task_waits_under_the_current_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let lowered = dlq_registry("aco_dlq_lowered", Some(1));
    let entry = dead_letter(&mut conn, "aco_dlq_lowered", "RUNNING").await;
    let task = autumn_harvest::dlq::replay_dead_letter(&mut conn, entry, Some(&lowered))
        .await
        .expect("replay");
    let replayed = row(&mut conn, task).await;
    assert_eq!(replayed.concurrency_key.as_deref(), Some("aco_dlq_lowered"));
    assert_eq!(replayed.concurrency_cap, Some(1));

    let code = dlq_registry("aco_dlq_code", None);
    let entry = dead_letter(&mut conn, "aco_dlq_code", "RUNNING").await;
    let task = autumn_harvest::dlq::replay_dead_letter(&mut conn, entry, Some(&code))
        .await
        .expect("replay");
    assert_eq!(row(&mut conn, task).await.concurrency_cap, Some(3));
}

/// A redriven task takes the override cap too.
#[tokio::test]
async fn a_redriven_activity_task_waits_under_the_current_cap() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let registry = dlq_registry("aco_dlq_redrive", Some(2));
    let entry = dead_letter(&mut conn, "aco_dlq_redrive", "FAILED").await;
    let outcome = autumn_harvest::dlq::redrive_dead_letter(&mut conn, entry, Some(&registry), None)
        .await
        .expect("redrive");
    let autumn_harvest::dlq::RedriveOutcome::Redriven(task) = outcome else {
        panic!("the redrive re-enqueues the task: {outcome:?}");
    };
    let redriven = row(&mut conn, task).await;
    assert_eq!(redriven.concurrency_key.as_deref(), Some("aco_dlq_redrive"));
    assert_eq!(redriven.concurrency_cap, Some(2));
}
