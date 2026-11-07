#![cfg(feature = "db")]
//! The operator override of the activity defaults.
//!
//! These tests check the store, the table checks and each acceptance
//! criterion against Postgres. A worker drives each run. Its refresh loop
//! loads the override from the table, as in production. The store tests use
//! fresh names. Each worker test uses its own queue and fixed names, and it
//! clears its own override row first.
//!
//! The replay check needs the `testing` feature, so the module needs it too.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::activity_defaults::{
    ActivityDefaultChange, ActivityDefaultFields, ActivityDefaultPatch, MAX_ACTIVITY_TIMEOUT,
    get_activity_default_override, load_activity_default_overrides,
    update_activity_default_override,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::run_defaults::{MAX_RETRY_INTERVAL, RunDefaultsCache};
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::store;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::workflow_retry::{RetryLimits, RetryLimitsPatch};
use autumn_harvest::{
    ActivityContext, ExecutionId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_test_pool, runtime_config, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

const CODE_ATTEMPTS: u32 = 4;
const CODE_STC: Duration = Duration::from_secs(30);
const CODE_HEARTBEAT: Duration = Duration::from_secs(20);
const CODE_TO_START: Duration = Duration::from_secs(60);
const CODE_TO_CLOSE: Duration = Duration::from_secs(600);

/// Stop the retries and allow longer bounds.
const OVERRIDE: ActivityDefaultFields = ActivityDefaultFields {
    retry: RetryLimits {
        max_attempts: Some(1),
        initial_interval: Some(Duration::from_secs(2)),
        max_interval: None,
    },
    start_to_close: Some(Duration::from_secs(90)),
    heartbeat_timeout: Some(Duration::from_secs(45)),
    schedule_to_start: Some(Duration::from_secs(120)),
    schedule_to_close: Some(Duration::from_mins(20)),
};

/// A patch that sets each field of `fields`.
const fn set_all(fields: ActivityDefaultFields) -> ActivityDefaultPatch {
    ActivityDefaultPatch {
        retry: RetryLimitsPatch {
            max_attempts: Some(fields.retry.max_attempts),
            initial_interval: Some(fields.retry.initial_interval),
            max_interval: Some(fields.retry.max_interval),
        },
        start_to_close: Some(fields.start_to_close),
        heartbeat_timeout: Some(fields.heartbeat_timeout),
        schedule_to_start: Some(fields.schedule_to_start),
        schedule_to_close: Some(fields.schedule_to_close),
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn change(
    conn: &mut AsyncPgConnection,
    name: &str,
    patch: ActivityDefaultPatch,
    clear: bool,
) -> autumn_harvest::error::HarvestResult<
    Option<autumn_harvest::activity_defaults::ActivityDefaultOverride>,
> {
    let change = ActivityDefaultChange {
        activity_name: name,
        patch,
        clear,
        actor: "ops",
        reason: Some("downstream is slow"),
    };
    update_activity_default_override(conn, &change).await
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

// ---------------------------------------------------------------------------
// The store.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_change_round_trips_and_a_clear_deletes_the_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_store");

    let stored = change(&mut conn, &name, set_all(OVERRIDE), false)
        .await
        .expect("set")
        .expect("a row");
    assert_eq!(stored.fields, OVERRIDE);
    assert_eq!(stored.updated_by, "ops");
    assert_eq!(stored.reason.as_deref(), Some("downstream is slow"));
    let read = get_activity_default_override(&mut conn, &name)
        .await
        .expect("get")
        .expect("a row");
    assert_eq!(read.fields, OVERRIDE);
    assert!(
        load_activity_default_overrides(&mut conn)
            .await
            .expect("load")
            .iter()
            .any(|row| row.activity_name == name && row.fields == OVERRIDE)
    );

    let cleared = change(&mut conn, &name, ActivityDefaultPatch::default(), true)
        .await
        .expect("clear");
    assert_eq!(cleared, None, "a clear that sets nothing deletes the row");
    assert_eq!(
        get_activity_default_override(&mut conn, &name)
            .await
            .expect("get"),
        None
    );
}

#[tokio::test]
async fn a_patch_keeps_the_fields_it_does_not_name() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_merge");

    change(&mut conn, &name, set_all(OVERRIDE), false)
        .await
        .expect("set");
    let only_stc = ActivityDefaultPatch {
        start_to_close: Some(None),
        ..ActivityDefaultPatch::default()
    };
    let stored = change(&mut conn, &name, only_stc, false)
        .await
        .expect("patch")
        .expect("a row");
    assert_eq!(
        stored.fields,
        ActivityDefaultFields {
            start_to_close: None,
            ..OVERRIDE
        }
    );
    let fresh = ActivityDefaultPatch {
        heartbeat_timeout: Some(Some(Duration::from_secs(7))),
        ..ActivityDefaultPatch::default()
    };
    let stored = change(&mut conn, &name, fresh, true)
        .await
        .expect("clear and set")
        .expect("a row");
    assert_eq!(
        stored.fields,
        ActivityDefaultFields {
            heartbeat_timeout: Some(Duration::from_secs(7)),
            ..ActivityDefaultFields::default()
        },
        "a clear drops the stored fields before the patch merges"
    );
}

#[tokio::test]
async fn the_write_refuses_a_value_that_no_schedule_can_use() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_refuse");
    let refused = [
        ActivityDefaultPatch {
            start_to_close: Some(Some(Duration::ZERO)),
            ..ActivityDefaultPatch::default()
        },
        ActivityDefaultPatch {
            heartbeat_timeout: Some(Some(Duration::from_micros(500))),
            ..ActivityDefaultPatch::default()
        },
        ActivityDefaultPatch {
            schedule_to_close: Some(Some(MAX_ACTIVITY_TIMEOUT + Duration::from_secs(1))),
            ..ActivityDefaultPatch::default()
        },
        ActivityDefaultPatch {
            retry: RetryLimitsPatch {
                max_attempts: Some(Some(0)),
                ..RetryLimitsPatch::default()
            },
            ..ActivityDefaultPatch::default()
        },
        ActivityDefaultPatch {
            retry: RetryLimitsPatch {
                initial_interval: Some(Some(MAX_RETRY_INTERVAL + Duration::from_secs(1))),
                ..RetryLimitsPatch::default()
            },
            ..ActivityDefaultPatch::default()
        },
        ActivityDefaultPatch {
            retry: RetryLimitsPatch {
                initial_interval: Some(Some(Duration::from_secs(10))),
                max_interval: Some(Some(Duration::from_secs(1))),
                ..RetryLimitsPatch::default()
            },
            ..ActivityDefaultPatch::default()
        },
    ];
    for patch in refused {
        let error = change(&mut conn, &name, patch, false)
            .await
            .expect_err("refused");
        assert!(
            matches!(error, autumn_harvest::error::HarvestError::Config(_)),
            "{patch:?}: {error}"
        );
    }
    assert_eq!(
        get_activity_default_override(&mut conn, &name)
            .await
            .expect("get"),
        None,
        "a refused write stores nothing"
    );
}

/// One row that the engine cannot read would fail the load of every
/// override. So the table refuses such a row.
#[tokio::test]
async fn the_table_refuses_a_row_that_the_engine_cannot_read() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_check");
    for (columns, values) in [
        ("start_to_close_ms", "0"),
        ("heartbeat_timeout_ms", "-5"),
        ("schedule_to_close_ms", "3153600000001"),
        ("retry_max_attempts", "0"),
        ("retry_initial_interval_ms", "31536000001"),
        ("retry_initial_interval_ms, retry_max_interval_ms", "10, 5"),
        ("reason", "'no field'"),
    ] {
        let sql = format!(
            "INSERT INTO harvest_activity_default_overrides \
             (activity_name, updated_by, {columns}) VALUES ($1, 'ops', {values})"
        );
        let result = diesel::sql_query(&sql)
            .bind::<diesel::sql_types::Text, _>(&name)
            .execute(&mut conn)
            .await;
        assert!(
            matches!(
                result,
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::CheckViolation,
                    _
                ))
            ),
            "{sql} must fail a CHECK: {result:?}"
        );
    }
}

/// A row at each upper bound passes the table and decodes.
#[tokio::test]
async fn a_row_at_each_bound_loads() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_bound");
    diesel::sql_query(
        "INSERT INTO harvest_activity_default_overrides \
         (activity_name, updated_by, retry_max_attempts, retry_initial_interval_ms, \
          retry_max_interval_ms, start_to_close_ms, heartbeat_timeout_ms, \
          schedule_to_start_ms, schedule_to_close_ms) \
         VALUES ($1, 'ops', 2147483647, 31536000000, 31536000000, 3153600000000, \
                 3153600000000, 3153600000000, 3153600000000)",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect("a row at each bound");
    let stored = get_activity_default_override(&mut conn, &name)
        .await
        .expect("the row decodes")
        .expect("a row");
    assert_eq!(stored.fields.schedule_to_close, Some(MAX_ACTIVITY_TIMEOUT));
    assert_eq!(stored.fields.retry.max_interval, Some(MAX_RETRY_INTERVAL));
    change(&mut conn, &name, ActivityDefaultPatch::default(), true)
        .await
        .expect("clear");
}

/// One failed load keeps its own map, and the other map still loads.
#[tokio::test]
async fn a_failed_activity_load_keeps_its_map_and_never_blocks_the_workflows() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_isolate");
    change(&mut conn, &name, set_all(OVERRIDE), false)
        .await
        .expect("set");
    let cache = RunDefaultsCache::new();
    cache.refresh(&mut conn).await.expect("refresh");
    // A workflow override that lands after the first load.
    let workflow = unique("ado_isolate_wf");
    let sla = autumn_harvest::run_defaults::RunDefaultPatch {
        sla: Some(Some(Duration::from_secs(60))),
        ..Default::default()
    };
    autumn_harvest::run_defaults::update_run_default_override(
        &mut conn,
        &autumn_harvest::run_defaults::RunDefaultChange {
            workflow_name: &workflow,
            patch: sla,
            clear: false,
            actor: "ops",
            reason: None,
        },
    )
    .await
    .expect("a workflow override");
    assert!(cache.get(&workflow).is_none());

    // The rename rolls back with the transaction, so the table stays.
    let result: Result<(), diesel::result::Error> = Box::pin(conn.transaction(async |conn| {
        diesel::sql_query(
            "ALTER TABLE harvest_activity_default_overrides \
             RENAME TO harvest_activity_default_overrides_gone",
        )
        .execute(conn)
        .await?;
        let refreshed = cache.refresh(conn).await;
        assert!(refreshed.is_err(), "the activity load fails");
        Err(diesel::result::Error::RollbackTransaction)
    }))
    .await;
    assert!(result.is_err());
    assert_eq!(
        cache.activity(&name),
        Some(OVERRIDE),
        "the activity map stays"
    );
    assert!(
        cache.get(&workflow).is_some(),
        "the workflow map still loads"
    );
    change(&mut conn, &name, ActivityDefaultPatch::default(), true)
        .await
        .expect("clear");
    diesel::sql_query("DELETE FROM harvest_run_default_overrides WHERE workflow_name = $1")
        .bind::<diesel::sql_types::Text, _>(&workflow)
        .execute(&mut conn)
        .await
        .expect("scrub the workflow override");
}

#[tokio::test]
async fn a_refresh_loads_and_drops_the_activity_overrides() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = unique("ado_refresh");
    let cache = RunDefaultsCache::new();

    change(&mut conn, &name, set_all(OVERRIDE), false)
        .await
        .expect("set");
    cache.refresh(&mut conn).await.expect("refresh");
    assert_eq!(cache.activity(&name), Some(OVERRIDE));

    change(&mut conn, &name, ActivityDefaultPatch::default(), true)
        .await
        .expect("clear");
    cache.refresh(&mut conn).await.expect("refresh");
    assert_eq!(cache.activity(&name), None);
}

// ---------------------------------------------------------------------------
// The acceptance criteria, through a worker.
// ---------------------------------------------------------------------------

/// The first remote step waits for this.
static RELEASE: AtomicBool = AtomicBool::new(false);

/// The attempts of the first remote step. Its first attempt fails.
static FIRST_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// The queue and the activity of the remote tests. The activity info must
/// name its queue, so both are constants.
const REMOTE_QUEUE: &str = "q-activity-default-override-remote";
const REMOTE_STEP: &str = "ado_remote_step";
const REMOTE_WF: &str = "ado_remote_wf";

fn act_info(name: &'static str, queue: &'static str, handler: ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_default_override_db_tests",
        default_retry_policy: Some(RetryPolicy::fixed(CODE_ATTEMPTS, Duration::from_millis(10))),
        default_start_to_close: Some(CODE_STC),
        default_heartbeat_timeout: Some(CODE_HEARTBEAT),
        default_schedule_to_start: Some(CODE_TO_START),
        default_schedule_to_close: Some(CODE_TO_CLOSE),
        default_queue: Some(queue),
        max_concurrent: None,
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
        handler,
        input_schema: None,
        output_schema: None,
    }
}

fn wf_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "activity_default_override_db_tests",
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

/// The first step waits for [`RELEASE`], and its first attempt then fails.
/// So its retry runs after the change.
fn remote_step(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if input["wait"] == json!(true) {
            while !AtomicBool::load(&RELEASE, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if FIRST_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err("the first attempt fails".to_string());
            }
        }
        Ok(input)
    })
}

fn remote_info() -> ActivityInfo {
    act_info(REMOTE_STEP, REMOTE_QUEUE, remote_step)
}

/// Two typed calls, then a raw call. The first call waits for [`RELEASE`].
fn remote_wf(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let info = remote_info();
        let wait = input["wait"].clone();
        let _: Value = ctx
            .execute_activity(&info, json!({"step": "first", "wait": wait}))
            .await
            .map_err(|e| e.to_string())?;
        let _: Value = ctx
            .execute_activity(&info, json!({"step": "typed"}))
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw(REMOTE_STEP, json!({"step": "raw"}), REMOTE_QUEUE)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

fn registry(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        Arc::new(autumn_harvest::telemetry::TelemetryConfig::default()),
    ))
}

/// A worker with no workflow cache. Each decision replays the stored history,
/// so it replays the events before the change with the override in force.
fn worker(id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    let mut config = runtime_config(id, 1, 1, Duration::from_secs(10));
    config.queues = vec![queue.to_string()];
    config.workflow_cache_size = 0;
    config.resident_workflows = false;
    Arc::new(Worker::new(config, registry).expect("worker"))
}

async fn start(
    conn: &mut AsyncPgConnection,
    workflow: &str,
    queue: &str,
    input: Value,
) -> ExecutionId {
    let workflow_id = unique("ado-run");
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams::new(workflow, &workflow_id, ExecutionId::new(), input, queue),
        None,
    )
    .await
    .expect("start")
    .exec_id
}

/// The activity task rows of one run, in schedule order.
async fn activity_rows(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::activity_name.is_not_null())
        .order(harvest_task_queue::created_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("task rows")
}

async fn wait_for_rows(url: &str, exec_id: ExecutionId, count: usize) -> Vec<TaskQueueItem> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = activity_rows(url, exec_id).await;
            if rows.len() >= count {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the activity rows appear")
}

/// Wait until the refresh loop of the worker loads `expected`.
///
/// The test does not write the cache. A load that the loop started before
/// the write could then bring back the old snapshot.
async fn wait_for_cache(
    registry: &HandlerRegistry,
    name: &str,
    expected: Option<ActivityDefaultFields>,
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while registry.run_defaults().activity(name) != expected {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the refresh loop loads the change");
}

/// The stored values of one row: attempts, first delay, attempt bound,
/// heartbeat, wait bound and total bound.
fn stored(row: &TaskQueueItem) -> (i32, Duration, Duration, Duration, Duration, Duration) {
    let policy: RetryPolicy =
        serde_json::from_value(row.retry_policy.clone().expect("a policy")).expect("decode");
    let std = |d: Option<chrono::Duration>| d.expect("a value").to_std().expect("positive");
    let created = row.created_at.expect("created_at");
    // The worker clock sets the deadline and the database clock sets
    // `created_at`. Round to 10 seconds, which is far below the 600 s gap
    // between the code value and the override.
    let millis = (row.schedule_to_close_at.expect("a deadline") - created)
        .num_milliseconds()
        .max(0)
        .unsigned_abs();
    let to_close = Duration::from_secs((millis + 5_000) / 10_000 * 10);
    (
        row.max_attempts,
        policy.initial_interval,
        std(row.start_to_close),
        std(row.heartbeat_timeout),
        std(row.schedule_to_start),
        to_close,
    )
}

fn code_values() -> (i32, Duration, Duration, Duration, Duration, Duration) {
    (
        i32::try_from(CODE_ATTEMPTS).expect("fits"),
        Duration::from_millis(10),
        CODE_STC,
        CODE_HEARTBEAT,
        CODE_TO_START,
        CODE_TO_CLOSE,
    )
}

const fn override_values() -> (i32, Duration, Duration, Duration, Duration, Duration) {
    (
        1,
        Duration::from_secs(2),
        Duration::from_secs(90),
        Duration::from_secs(45),
        Duration::from_secs(120),
        Duration::from_mins(20),
    )
}

/// AC1, AC2 and AC4.
///
/// The first step is scheduled before the change, and it waits. The change
/// lands. The typed step and the raw step are scheduled after it. Replay of
/// that history succeeds. A clear then restores the code values for the
/// next run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activity_scheduled_after_the_change_takes_the_override() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    // A shared cluster can hold a row from an earlier run of this test.
    change(
        &mut conn,
        REMOTE_STEP,
        ActivityDefaultPatch::default(),
        true,
    )
    .await
    .expect("scrub");
    RELEASE.store(false, Ordering::SeqCst);
    FIRST_ATTEMPTS.store(0, Ordering::SeqCst);

    let registry = registry(vec![wf_info(REMOTE_WF, remote_wf)], vec![remote_info()]);
    let pool: DbPool = build_test_pool(&url);
    let handle = spawn_test_worker(
        worker("w-ado-remote", REMOTE_QUEUE, Arc::clone(&registry)),
        pool.clone(),
    );

    let exec_id = start(&mut conn, REMOTE_WF, REMOTE_QUEUE, json!({"wait": true})).await;
    let before = wait_for_rows(&url, exec_id, 1).await;
    assert_eq!(
        stored(&before[0]),
        code_values(),
        "scheduled before the change"
    );

    change(&mut conn, REMOTE_STEP, set_all(OVERRIDE), false)
        .await
        .expect("set the override");
    wait_for_cache(&registry, REMOTE_STEP, Some(OVERRIDE)).await;

    // AC1: a scheduled activity keeps its stored values.
    let first = activity_rows(&url, exec_id).await;
    assert_eq!(first.len(), 1, "the run still waits on its first step");
    assert_eq!(
        stored(&first[0]),
        code_values(),
        "a scheduled row keeps its values"
    );

    RELEASE.store(true, Ordering::SeqCst);
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    let rows = activity_rows(&url, exec_id).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        stored(&rows[0]),
        code_values(),
        "the first row never changes"
    );
    assert_eq!(
        rows[0].attempt, 2,
        "the retry of the first row runs under its stored budget, not the override of one attempt"
    );
    assert_eq!(
        stored(&rows[1]),
        override_values(),
        "a typed call takes the override"
    );
    assert_eq!(
        stored(&rows[2]),
        override_values(),
        "a raw call takes the override"
    );

    // AC2: replay of the run that spans the change succeeds.
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events;
    let report = WorkflowReplayer::new()
        .register_fn(REMOTE_WF, remote_wf as _)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a run that spans the change replays:\n{report}"
    );

    // AC4: a clear restores the code values.
    change(
        &mut conn,
        REMOTE_STEP,
        ActivityDefaultPatch::default(),
        true,
    )
    .await
    .expect("clear");
    wait_for_cache(&registry, REMOTE_STEP, None).await;
    let next = start(&mut conn, REMOTE_WF, REMOTE_QUEUE, json!({"wait": false})).await;
    wait_for_execution_state_with_timeout(&url, next, "COMPLETED", Duration::from_secs(30)).await;
    let rows = activity_rows(&url, next).await;
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(
            stored(&row),
            code_values(),
            "a clear restores the code value"
        );
    }

    handle.abort();
}

// ---------------------------------------------------------------------------
// AC3: local activities.
// ---------------------------------------------------------------------------

const LOCAL_QUEUE: &str = "q-activity-default-override-local";
const LOCAL_STEP: &str = "ado_local_step";
const LOCAL_WF: &str = "ado_local_wf";

/// The calls of [`local_step`].
static LOCAL_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Fail on the first call, then succeed.
fn local_step(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if LOCAL_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("the first call fails".to_string())
        } else {
            Ok(input)
        }
    })
}

fn local_info() -> ActivityInfo {
    let mut info = act_info(LOCAL_STEP, LOCAL_QUEUE, local_step);
    info.is_local = true;
    info.default_heartbeat_timeout = None;
    info.default_schedule_to_start = None;
    info.default_schedule_to_close = None;
    info.default_queue = None;
    info
}

fn local_wf(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let info = local_info();
        ctx.execute_local_activity::<_, Value>(&info, input)
            .await
            .map_err(|e| e.to_string())
    })
}

/// The retry `max_attempts` and `start_to_close` of each
/// `LocalActivityScheduled` of a history.
fn frozen(history: &[WorkflowEvent]) -> Vec<(Option<u32>, Option<Duration>)> {
    history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::LocalActivityScheduled {
                retry_policy,
                start_to_close_nanos,
                ..
            } => Some((
                retry_policy.as_ref().map(|p| p.max_attempts),
                start_to_close_nanos.map(Duration::from_nanos),
            )),
            _ => None,
        })
        .collect()
}

/// A frozen budget that differs from the code value and the override.
const FROZEN_ATTEMPTS: u32 = 3;

/// AC3.
///
/// The first run freezes the code values, then recovers mid-retry after the
/// change. An override of one attempt would fail it at once. It keeps its
/// frozen budget and completes. A run started after the change freezes the
/// override.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_activity_frozen_before_the_change_keeps_its_frozen_values() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    change(&mut conn, LOCAL_STEP, ActivityDefaultPatch::default(), true)
        .await
        .expect("scrub");
    LOCAL_CALLS.store(0, Ordering::SeqCst);

    // A history frozen before the change, crashed after its first failure.
    let exec_id = start(&mut conn, LOCAL_WF, LOCAL_QUEUE, json!(1)).await;
    let activity_id = autumn_harvest::types::ActivityExecId::new();
    let started = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events
        .len();
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: LOCAL_STEP.to_string(),
                input: json!(1),
                resolved: true,
                retry_policy: Some(RetryPolicy::fixed(
                    FROZEN_ATTEMPTS,
                    Duration::from_millis(10),
                )),
                start_to_close_nanos: Some(
                    u64::try_from(CODE_STC.as_nanos()).expect("fits in u64"),
                ),
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: "the first call fails".to_string(),
                attempt: 1,
            },
        ],
        i32::try_from(started).expect("fits"),
    )
    .await
    .expect("append the frozen history");
    LOCAL_CALLS.store(1, Ordering::SeqCst);

    let local_override = ActivityDefaultFields {
        retry: RetryLimits {
            max_attempts: Some(1),
            ..RetryLimits::default()
        },
        start_to_close: Some(Duration::from_secs(45)),
        ..ActivityDefaultFields::default()
    };
    change(&mut conn, LOCAL_STEP, set_all(local_override), false)
        .await
        .expect("set the override");

    let registry = registry(vec![wf_info(LOCAL_WF, local_wf)], vec![local_info()]);
    // Load the override before the worker claims the run, so the recovery
    // runs with the override in force.
    registry
        .run_defaults()
        .refresh(&mut conn)
        .await
        .expect("refresh");
    assert_eq!(
        registry.run_defaults().activity(LOCAL_STEP),
        Some(local_override)
    );
    let pool: DbPool = build_test_pool(&url);
    let handle = spawn_test_worker(
        worker("w-ado-local", LOCAL_QUEUE, Arc::clone(&registry)),
        pool.clone(),
    );

    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events;
    assert_eq!(
        frozen(&history),
        vec![(Some(FROZEN_ATTEMPTS), Some(CODE_STC))],
        "the recovery appends no new schedule and keeps the frozen values"
    );
    assert_eq!(
        AtomicUsize::load(&LOCAL_CALLS, Ordering::SeqCst),
        2,
        "the recovery retried under the frozen budget"
    );

    // A local activity scheduled after the change freezes the override.
    LOCAL_CALLS.store(1, Ordering::SeqCst);
    let next = start(&mut conn, LOCAL_WF, LOCAL_QUEUE, json!(2)).await;
    wait_for_execution_state_with_timeout(&url, next, "COMPLETED", Duration::from_secs(30)).await;
    let history = store::load_history(&mut conn, next)
        .await
        .expect("history")
        .events;
    assert_eq!(
        frozen(&history),
        vec![(Some(1), Some(Duration::from_secs(45)))]
    );

    change(&mut conn, LOCAL_STEP, ActivityDefaultPatch::default(), true)
        .await
        .expect("clear");
    handle.abort();
}
