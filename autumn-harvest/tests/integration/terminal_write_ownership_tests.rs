//! Deterministic race tests for issue #1184: terminal workflow writes
//! (complete/fail/pause-park) had no worker-ownership check, so a stale or
//! zombie dispatcher whose claim was reclaimed elsewhere could commit a
//! decision against a run a new owner was already driving.
//!
//! Each test follows the exact pattern established by
//! [`capability_miss_tests::a_stale_dispatcher_with_zero_emitted_commands_makes_no_terminal_decision_when_the_claim_moved`]
//! for the #1182 sibling guard: seed a claimed task, transfer the claim to
//! `"thief"` on a second connection (a committed, out-of-band claim move --
//! the same shape as a poison-pill reclaim, an operator requeue, or a
//! concurrent claim race), then call the guarded function with the ORIGINAL,
//! now-stale `worker_id`. Each must:
//!   - return `Err` naming the exact stale task id via
//!     `HarvestError::terminal_write_claim_ambiguous`,
//!   - append no terminal event to the execution's history,
//!   - leave the execution row's state untouched,
//!   - leave the task row exactly as `"thief"` left it (state and
//!     `worker_id` both unchanged).

use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{
    PreloadedFailureHistory, check_paused_and_park, fail_task_and_execution_with_history,
    persist_child_workflow_completion, persist_child_workflow_failure,
    persist_workflow_completion, persist_workflow_failure,
};
use autumn_harvest::store;

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup / seeding helpers -- mirrors `capability_miss_tests.rs`'s own
// self-contained copies (each integration-test file owns its own; there is
// no shared support module in this suite).
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: "default",
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    exec_id
}

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue_name: &str,
) -> ExecutionId {
    let exec_id = seed_execution(conn, workflow_name, input.clone()).await;
    let mut params = EnqueueParams::new(queue_name, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

/// Seed a workflow and hand-claim its task for `worker_id`, bypassing the
/// normal poll loop so the test owns the exact timing.
async fn seed_claimed_task(
    url: &str,
    queue_name: &str,
    worker_id: &str,
) -> (ExecutionId, TaskQueueItem) {
    let mut conn = connect(url).await;
    let exec_id = seed_workflow(&mut conn, "issue1184_wf", serde_json::json!({}), queue_name).await;

    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::state.eq("RUNNING"),
        harvest_task_queue::worker_id.eq(Some(worker_id)),
        harvest_task_queue::started_at.eq(Some(Utc::now())),
    ))
    .execute(&mut conn)
    .await
    .expect("claim task");

    let task = load_tasks(url, exec_id).await.into_iter().next().expect("the seeded task row");
    (exec_id, task)
}

/// Commit a claim transfer to `"thief"` on a fresh, already-committed
/// connection -- simulating a poison-pill reclaim, an operator requeue, or a
/// concurrent claim race that moved the row while the original dispatcher's
/// decision cycle was still in flight.
async fn steal_claim(url: &str, task_id: Uuid) {
    let mut mover = connect(url).await;
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = 'thief' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut mover)
        .await
        .expect("transfer the claim");
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

async fn load_tasks(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load tasks")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

/// Shared assertions for every test below: the stale dispatcher's write must
/// be a complete no-op against both the execution and the task row, whatever
/// state `"thief"` left them in.
async fn assert_thief_untouched(
    url: &str,
    exec_id: ExecutionId,
    task_id: Uuid,
    expected_exec_state: &str,
    expected_task_state: &str,
) {
    let execution = load_execution(url, exec_id).await;
    assert_eq!(
        execution.state, expected_exec_state,
        "a dispatcher that lost the claim must not alter the execution a new owner now drives"
    );
    let reloaded = load_tasks(url, exec_id)
        .await
        .into_iter()
        .find(|t| t.id == task_id)
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, expected_task_state,
        "the stolen row's state must be untouched by the stale dispatcher's write"
    );
    assert_eq!(
        reloaded.worker_id.as_deref(),
        Some("thief"),
        "the claim transfer must be untouched by the stale dispatcher's write"
    );
}

// ---------------------------------------------------------------------------
// persist_workflow_completion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_workflow_completion_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-completion", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let mut conn = connect(&url).await;
    let result = persist_workflow_completion(
        &mut conn,
        task.id,
        exec_id,
        1,
        "dispatcher-a",
        task.crash_strikes,
        serde_json::json!({"ok": true}),
        None,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a completion whose claim moved must not commit, not be swallowed")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
        "the sentinel must name the exact task whose ownership is ambiguous"
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    assert_thief_untouched(&url, exec_id, task.id, "RUNNING", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// persist_workflow_failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_workflow_failure_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-failure", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let mut conn = connect(&url).await;
    let result = persist_workflow_failure(
        &mut conn,
        task.id,
        exec_id,
        1,
        "dispatcher-a",
        task.crash_strikes,
        "boom",
        None,
        None,
        None,
        None,
        None,
        autumn_harvest::types::Priority::default(),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a failure whose claim moved must not commit, not be swallowed")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
        "the sentinel must name the exact task whose ownership is ambiguous"
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    assert_thief_untouched(&url, exec_id, task.id, "RUNNING", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// persist_child_workflow_completion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_child_workflow_completion_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-child-completion", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    // The guard runs before the parent is ever touched, so an unregistered
    // placeholder parent id is fine -- it must never be reached.
    let parent_exec_id = ExecutionId::new();

    let mut conn = connect(&url).await;
    let result = persist_child_workflow_completion(
        &mut conn,
        task.id,
        exec_id,
        1,
        "dispatcher-a",
        task.crash_strikes,
        parent_exec_id,
        serde_json::json!({"ok": true}),
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a child completion whose claim moved must not commit")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    assert_thief_untouched(&url, exec_id, task.id, "RUNNING", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// persist_child_workflow_failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_child_workflow_failure_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-child-failure", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let parent_exec_id = ExecutionId::new();

    let mut conn = connect(&url).await;
    let result = persist_child_workflow_failure(
        &mut conn,
        task.id,
        exec_id,
        1,
        "dispatcher-a",
        task.crash_strikes,
        parent_exec_id,
        "boom",
        None,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a child failure whose claim moved must not commit")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    assert_thief_untouched(&url, exec_id, task.id, "RUNNING", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// check_paused_and_park
// ---------------------------------------------------------------------------

#[tokio::test]
async fn check_paused_and_park_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-pause-park", "dispatcher-a").await;

    // An operator pause landed on the execution before the claim moved.
    {
        let mut conn = connect(&url).await;
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::state.eq("PAUSED"))
            .execute(&mut conn)
            .await
            .expect("pause execution");
    }

    steal_claim(&url, task.id).await;

    let mut conn = connect(&url).await;
    let result = check_paused_and_park(
        &mut conn,
        exec_id.as_uuid(),
        task.id,
        "dispatcher-a",
        task.crash_strikes,
        Duration::ZERO,
    )
    .await;

    assert_eq!(
        result
            .expect_err("a park whose claim moved must not commit, not be swallowed")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
        "the sentinel must name the exact task whose ownership is ambiguous"
    );

    // The task row must be untouched -- specifically, still `RUNNING` and
    // still owned by `"thief"`, never re-parked (worker_id cleared) by the
    // stale dispatcher's park attempt.
    assert_thief_untouched(&url, exec_id, task.id, "PAUSED", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// fail_task_and_execution_with_history -- the `NoExecution` branch, which
// (unlike `Loaded`) does not already run inside `persist_workflow_failure`'s
// own guarded transaction and needed its own independent guard.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_task_and_execution_with_history_makes_no_terminal_decision_when_the_claim_moved_and_has_no_execution()
 {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-no-exec", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let mut conn = connect(&url).await;
    let result = fail_task_and_execution_with_history(
        &mut conn,
        &task,
        "dispatcher-a",
        "boom",
        PreloadedFailureHistory::NoExecution,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a fail-only write whose claim moved must not commit")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
    );

    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .find(|t| t.id == task.id)
        .expect("the task row survives an undecided dispatch");
    assert_eq!(reloaded.state, "RUNNING", "must not be marked FAILED by the stale dispatcher");
    assert_eq!(reloaded.worker_id.as_deref(), Some("thief"));
}
