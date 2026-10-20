#![cfg(feature = "db")]
//! A declarative `#[update]` handler on the Postgres worker (issue #2035).
//!
//! The edge appends `UpdateAdmitted` and wakes the workflow task. The worker
//! must then run the handler once and record the result. Replay must return
//! the recorded result and must not run the handler again.
//!
//! Each test uses its own update names, so two tests that share one database
//! never run each other's handler.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres. Otherwise
//! a testcontainers Postgres starts.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{
    ActivityInfo, UpdateHandlerFn, UpdateHandlerInfo, WorkflowHandlerFn, WorkflowInfo,
};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::types::UpdateId;
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{ActivityContext, ExecutionId, WorkflowContext, queue, signal, store};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_history_from_url, runtime_config,
    setup_test_database_url_or_env, spawn_test_worker, wait_for_execution_state,
};

type BodyFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type UpdateFuture = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>;

/// Reads a counter. `RunQueryDsl` is in scope, so `counter.load` would resolve to it.
fn runs(counter: &AtomicUsize) -> usize {
    AtomicUsize::load(counter, Ordering::SeqCst)
}

/// Waits for one signal, then finishes.
fn park_then_finish(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("go")
            .await
            .map_err(|error| error.to_string())?;
        Ok(json!("done"))
    })
}

/// Runs a local activity, then waits for one signal, then finishes.
fn local_then_park(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("local_probe", json!({}), None, None)
            .await
            .map_err(|error| error.to_string())?;
        ctx.wait_for_signal("go")
            .await
            .map_err(|error| error.to_string())?;
        Ok(json!("done"))
    })
}

fn local_probe(_ctx: &ActivityContext, _input: Value) -> BodyFuture<'_> {
    Box::pin(async move { Ok(json!("probed")) })
}

static PARK_RUNS: AtomicUsize = AtomicUsize::new(0);

fn bump_when_parked(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateFuture {
    PARK_RUNS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move { Ok(json!({ "bumped": input })) })
}

static TERMINAL_RUNS: AtomicUsize = AtomicUsize::new(0);

fn bump_before_finish(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateFuture {
    TERMINAL_RUNS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move { Ok(json!({ "bumped": input })) })
}

static LOCAL_RUNS: AtomicUsize = AtomicUsize::new(0);

fn bump_around_local(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateFuture {
    LOCAL_RUNS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move { Ok(json!({ "bumped": input })) })
}

static BURST_RUNS: AtomicUsize = AtomicUsize::new(0);

fn bump_in_burst(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateFuture {
    BURST_RUNS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(input)
    })
}

fn refuse(_ctx: Arc<WorkflowContext>, _input: Value) -> UpdateFuture {
    Box::pin(async move { Err("insufficient funds".to_string()) })
}

fn update_info(name: &'static str, handler: UpdateHandlerFn) -> UpdateHandlerInfo {
    UpdateHandlerInfo {
        name,
        workflow: "e2e_test_workflow",
        module: "declarative_update_tests",
        input_type_hint: "",
        output_type_hint: "",
        has_validator: false,
        handler,
        validator: None,
        mcp: false,
        description: None,
        arg_schema: None,
        response_schema: None,
    }
}

fn workflow(handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "e2e_test_workflow",
        module: "declarative_update_tests",
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

fn local_activity(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "declarative_update_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: true,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
        input_schema: None,
        output_schema: None,
    }
}

/// A registry with `body`, one local activity and the given updates.
fn registry(body: WorkflowHandlerFn, updates: Vec<UpdateHandlerInfo>) -> Arc<HandlerRegistry> {
    Arc::new(
        HandlerRegistry::new(
            vec![workflow(body)],
            vec![local_activity("local_probe", local_probe)],
        )
        .with_handler_infos(vec![], updates, vec![]),
    )
}

/// Waits until the workflow task is parked: `RUNNING` with no worker.
async fn wait_until_parked(database_url: &str, exec_id: ExecutionId) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut conn = AsyncPgConnection::establish(database_url)
                .await
                .expect("connect for task query");
            let tasks: Vec<TaskQueueItem> = harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .select(TaskQueueItem::as_select())
                .load(&mut conn)
                .await
                .expect("load tasks");
            if tasks
                .iter()
                .any(|task| task.state == "RUNNING" && task.worker_id.is_none())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the workflow task must park");
}

/// Waits for the `UpdateCompleted` or `UpdateFailed` event of `update_id`.
async fn wait_for_update_event(
    database_url: &str,
    exec_id: ExecutionId,
    update_id: UpdateId,
) -> WorkflowEvent {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let history = load_history_from_url(database_url, exec_id).await;
            if let Some(event) = history.events.iter().find(|event| {
                matches!(
                    event,
                    WorkflowEvent::UpdateCompleted { update_id: id, .. }
                        | WorkflowEvent::UpdateFailed { update_id: id, .. }
                    if *id == update_id
                )
            }) {
                return event.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker must record the update result")
}

/// The ids of the `UpdateCompleted` and `UpdateFailed` events, in history order.
fn result_ids(events: &[WorkflowEvent]) -> Vec<UpdateId> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::UpdateCompleted { update_id, .. }
            | WorkflowEvent::UpdateFailed { update_id, .. } => Some(*update_id),
            _ => None,
        })
        .collect()
}

/// The edge path: append `UpdateAdmitted`, then wake the parked task.
async fn admit(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    name: &str,
    input: Value,
) -> UpdateId {
    let update_id = UpdateId::new();
    store::admit_update_event(conn, exec_id, update_id, name.to_string(), input, None)
        .await
        .expect("admit update");
    queue::wake_workflow_task(conn, exec_id)
        .await
        .expect("wake workflow task");
    update_id
}

#[tokio::test]
async fn a_declarative_update_runs_once_while_the_workflow_waits() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;

    let updates = vec![update_info("bump_when_parked", bump_when_parked)];
    let worker = build_runtime_worker(
        "declarative-update-park",
        4,
        4,
        registry(park_then_finish, updates),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_until_parked(&database_url, exec_id).await;

    let update_id = admit(&mut conn, exec_id, "bump_when_parked", json!({ "by": 2 })).await;
    let event = wait_for_update_event(&database_url, exec_id, update_id).await;
    assert!(
        matches!(
            &event,
            WorkflowEvent::UpdateCompleted { output, .. }
                if output == &json!({ "bumped": { "by": 2 } })
        ),
        "{event:?}"
    );

    // A later signal ends the run. Each replay must return the recorded result.
    signal::send_signal(&mut conn, exec_id, "go", json!({}))
        .await
        .expect("send signal");
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(runs(&PARK_RUNS), 1, "handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    assert_eq!(result_ids(&history.events), vec![update_id]);
}

#[tokio::test]
async fn a_finishing_cycle_records_the_update_before_the_final_event() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;

    // The update and the signal both precede the first cycle. The body ends in
    // that cycle, so the result must join the terminal batch.
    let update_id = admit(&mut conn, exec_id, "bump_before_finish", json!(7)).await;
    signal::send_signal(&mut conn, exec_id, "go", json!({}))
        .await
        .expect("send signal");

    let updates = vec![update_info("bump_before_finish", bump_before_finish)];
    let worker = build_runtime_worker(
        "declarative-update-finish",
        4,
        4,
        registry(park_then_finish, updates),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(runs(&TERMINAL_RUNS), 1, "handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    let position = |wanted: fn(&WorkflowEvent) -> bool| history.events.iter().position(wanted);
    let result = position(|event| matches!(event, WorkflowEvent::UpdateCompleted { .. }))
        .expect("the update result is in history");
    let end = position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("the workflow completed");
    assert!(
        result < end,
        "result at {result} must precede the end at {end}"
    );
    assert_eq!(result_ids(&history.events), vec![update_id]);
    assert!(
        matches!(
            &history.events[result],
            WorkflowEvent::UpdateCompleted { output, .. } if output == &json!({ "bumped": 7 })
        ),
        "{:?}",
        history.events[result]
    );
}

#[tokio::test]
async fn a_failing_or_unknown_update_records_update_failed() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;

    let updates = vec![update_info("refuse_payment", refuse)];
    let worker = build_runtime_worker(
        "declarative-update-failure",
        4,
        4,
        registry(park_then_finish, updates),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_until_parked(&database_url, exec_id).await;

    let refused = admit(&mut conn, exec_id, "refuse_payment", json!({})).await;
    let unknown = admit(&mut conn, exec_id, "ghost_update", json!({})).await;
    let refused_event = wait_for_update_event(&database_url, exec_id, refused).await;
    let unknown_event = wait_for_update_event(&database_url, exec_id, unknown).await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert!(
        matches!(
            &refused_event,
            WorkflowEvent::UpdateFailed { error, .. } if error == "insufficient funds"
        ),
        "{refused_event:?}"
    );
    assert!(
        matches!(
            &unknown_event,
            WorkflowEvent::UpdateFailed { error, .. }
                if error == "update handler 'ghost_update' not found"
        ),
        "{unknown_event:?}"
    );
    let execution = crate::integration_e2e::load_execution_from_url(&database_url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a failed update keeps the run alive"
    );
}

/// A local activity makes the worker persist the update result inline and
/// re-drive the body in the same task. The re-drive must see that result.
#[tokio::test]
async fn a_local_activity_cycle_does_not_run_the_handler_twice() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;
    let update_id = admit(&mut conn, exec_id, "bump_around_local", json!(3)).await;

    let updates = vec![update_info("bump_around_local", bump_around_local)];
    let worker = build_runtime_worker(
        "declarative-update-local",
        4,
        4,
        registry(local_then_park, updates),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_for_update_event(&database_url, exec_id, update_id).await;
    wait_until_parked(&database_url, exec_id).await;

    signal::send_signal(&mut conn, exec_id, "go", json!({}))
        .await
        .expect("send signal");
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(runs(&LOCAL_RUNS), 1, "handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    assert_eq!(result_ids(&history.events), vec![update_id]);
}

/// A tight task budget closes the dispatch window after one handler. The worker
/// must wake the task, so each later cycle runs one more update.
#[tokio::test]
async fn updates_left_by_a_closed_window_run_in_later_cycles() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;
    let mut admitted = Vec::new();
    for n in 0..3 {
        admitted.push(admit(&mut conn, exec_id, "bump_in_burst", json!(n)).await);
    }

    // The effective budget is the larger of the two settings: 8 s. Half of what
    // is left is under 5 s, so the window never holds a second handler.
    let mut config = runtime_config("declarative-update-burst", 4, 4, Duration::from_secs(8));
    config.max_local_activity_start_to_close = Duration::from_secs(1);
    let updates = vec![update_info("bump_in_burst", bump_in_burst)];
    let worker = Arc::new(
        Worker::new(config, registry(park_then_finish, updates)).expect("worker should build"),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    for update_id in &admitted {
        wait_for_update_event(&database_url, exec_id, *update_id).await;
    }
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(runs(&BURST_RUNS), 3, "each handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    assert_eq!(result_ids(&history.events), admitted, "in admission order");
}

static STRAND_RUNS: AtomicUsize = AtomicUsize::new(0);

fn bump_in_strand(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateFuture {
    STRAND_RUNS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move { Ok(input) })
}

/// Runs a local activity, then waits on an activity that no worker serves. A
/// wait on an activity can stay resident, and a redundant wake resumes it warm.
///
/// No worker polls the queue, so the activity never starts. A running activity
/// would append events, and a conflicting persist would replay a handler. That
/// is the at-least-once limit, and this test must not depend on it.
fn local_then_unserved_activity(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("local_probe", json!({}), None, None)
            .await
            .map_err(|error| error.to_string())?;
        ctx.execute_activity_raw("unserved_probe", json!({}), "unserved")
            .await
            .map_err(|error| error.to_string())
    })
}

/// A registry with `body`, a local activity, an unserved activity and the updates.
fn registry_with_unserved_activity(
    body: WorkflowHandlerFn,
    updates: Vec<UpdateHandlerInfo>,
) -> Arc<HandlerRegistry> {
    let unserved = ActivityInfo {
        is_local: false,
        ..local_activity("unserved_probe", local_probe)
    };
    Arc::new(
        HandlerRegistry::new(
            vec![workflow(body)],
            vec![local_activity("local_probe", local_probe), unserved],
        )
        .with_handler_infos(vec![], updates, vec![]),
    )
}

/// A local activity makes the worker re-drive the body in the same task. Under a
/// tight budget, the first drive runs one update and the re-drive runs none. The
/// re-drive waits on an activity, so it can stay resident. A redundant wake would
/// resume it warm, and a warm cycle skips the dispatch. The cycle must go cold,
/// so the later cycles run the rest while the workflow stays parked.
#[tokio::test]
async fn updates_deferred_by_a_redrive_run_while_the_workflow_waits_on_an_activity() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "test": true })).await;
    let mut admitted = Vec::new();
    for n in 0..3 {
        admitted.push(admit(&mut conn, exec_id, "bump_in_strand", json!(n)).await);
    }

    // The effective budget is the larger of the two settings: 8 s. Half of what
    // is left is under 5 s, so the window never holds a second handler.
    let mut config = runtime_config("declarative-update-strand", 4, 4, Duration::from_secs(8));
    config.max_local_activity_start_to_close = Duration::from_secs(1);
    let updates = vec![update_info("bump_in_strand", bump_in_strand)];
    let worker = Arc::new(
        Worker::new(
            config,
            registry_with_unserved_activity(local_then_unserved_activity, updates),
        )
        .expect("worker should build"),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    for update_id in &admitted {
        wait_for_update_event(&database_url, exec_id, *update_id).await;
    }
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(runs(&STRAND_RUNS), 3, "each handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    assert_eq!(result_ids(&history.events), admitted, "in admission order");
}
