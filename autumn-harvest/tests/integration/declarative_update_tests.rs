#![cfg(feature = "db")]
//! A declarative `#[update]` handler on the Postgres worker (issue #2035).
//!
//! The edge appends `UpdateAdmitted` and wakes the workflow task. The worker
//! must then run the handler once and record the result. Replay must return
//! the recorded result and must not run the handler again.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres. Otherwise
//! a testcontainers Postgres starts.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{UpdateHandlerInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::types::UpdateId;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{ExecutionId, WorkflowContext, queue, signal, store};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

type BodyFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type UpdateFuture = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>;

/// Waits for one signal, then finishes.
fn park_then_finish(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("go")
            .await
            .map_err(|error| error.to_string())?;
        Ok(json!("done"))
    })
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

fn registry(name: &'static str, handler: autumn_harvest::info::UpdateHandlerFn) -> Arc<HandlerRegistry> {
    let update = UpdateHandlerInfo {
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
    };
    Arc::new(
        HandlerRegistry::new(vec![workflow(park_then_finish)], vec![])
            .with_handler_infos(vec![], vec![update], vec![]),
    )
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
    tokio::time::timeout(Duration::from_secs(15), async {
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

/// The edge path: append `UpdateAdmitted`, then wake the parked task.
async fn admit(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str, input: Value) -> UpdateId {
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

    let worker = build_runtime_worker(
        "declarative-update-park",
        4,
        4,
        registry("bump", bump_when_parked),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_until_parked(&database_url, exec_id).await;

    let update_id = admit(&mut conn, exec_id, "bump", json!({ "by": 2 })).await;
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

    assert_eq!(PARK_RUNS.load(Ordering::SeqCst), 1, "handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    let results = history
        .events
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::UpdateCompleted { .. }))
        .count();
    assert_eq!(results, 1, "history holds one result");
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
    let update_id = admit(&mut conn, exec_id, "bump", json!(7)).await;
    signal::send_signal(&mut conn, exec_id, "go", json!({}))
        .await
        .expect("send signal");

    let worker = build_runtime_worker(
        "declarative-update-finish",
        4,
        4,
        registry("bump", bump_before_finish),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(TERMINAL_RUNS.load(Ordering::SeqCst), 1, "handler runs once");
    let history = load_history_from_url(&database_url, exec_id).await;
    let position = |wanted: fn(&WorkflowEvent) -> bool| history.events.iter().position(wanted);
    let result = position(|event| matches!(event, WorkflowEvent::UpdateCompleted { .. }))
        .expect("the update result is in history");
    let end = position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("the workflow completed");
    assert!(result < end, "result at {result} must precede the end at {end}");
    let recorded = wait_for_update_event(&database_url, exec_id, update_id).await;
    assert!(
        matches!(&recorded, WorkflowEvent::UpdateCompleted { output, .. } if output == &json!({ "bumped": 7 })),
        "{recorded:?}"
    );
}
