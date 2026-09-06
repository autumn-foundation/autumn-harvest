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

use autumn_harvest::dlq::DeadLetterReason;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{
    HandlerRegistry, PreloadedFailureHistory, WorkflowTaskPersistence, check_paused_and_park,
    fail_execution_on_error, fail_task_and_execution_with_history,
    move_workflow_to_dlq_for_history_cap, persist_child_workflow_completion,
    persist_child_workflow_failure, persist_workflow_completion, persist_workflow_continue_as_new,
    persist_workflow_failure,
};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
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

    let task = load_tasks(url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the seeded task row");
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
        &mut Vec::new(),
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
        &mut Vec::new(),
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

    // A REAL parent execution, not a dangling placeholder id: if the guard
    // were absent, this write would otherwise proceed far enough to reach
    // `wake_parent_for_child_completion`, whose own unrelated `NotFound` on a
    // nonexistent parent would roll the transaction back for the wrong
    // reason and mask the guard's absence (Codex-equivalent self-review
    // finding). A real parent lets an unguarded write actually succeed
    // end-to-end, so `.expect_err(...)` genuinely depends on the guard.
    let mut seed_conn = connect(&url).await;
    let parent_exec_id =
        seed_execution(&mut seed_conn, "issue1184_parent_wf", serde_json::json!({})).await;

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
        &mut Vec::new(),
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

    // See the identical comment in the completion test above: a real parent
    // execution, not a dangling placeholder, so an unguarded write would
    // actually succeed end-to-end instead of failing for an unrelated reason.
    let mut seed_conn = connect(&url).await;
    let parent_exec_id =
        seed_execution(&mut seed_conn, "issue1184_parent_wf", serde_json::json!({})).await;

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
        &mut Vec::new(),
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
    assert_eq!(
        reloaded.state, "RUNNING",
        "must not be marked FAILED by the stale dispatcher"
    );
    assert_eq!(reloaded.worker_id.as_deref(), Some("thief"));
}

// ---------------------------------------------------------------------------
// fail_task_and_execution_with_history -- the `Unavailable` branch (execution
// exists, but its history failed to load), which has its own independent
// guard placed before its two bare `update_workflow_execution_failed` /
// `queue::fail_task` writes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_task_and_execution_with_history_makes_no_terminal_decision_when_the_claim_moved_and_history_is_unavailable()
 {
    let (url, _container) = setup_db().await;
    let (exec_id, task) =
        seed_claimed_task(&url, "q1184-history-unavailable", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let mut conn = connect(&url).await;
    let result = fail_task_and_execution_with_history(
        &mut conn,
        &task,
        "dispatcher-a",
        "boom",
        PreloadedFailureHistory::Unavailable { exec_id },
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a fail-only write whose claim moved must not commit")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
    );

    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "must not be marked FAILED by the stale dispatcher"
    );
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .find(|t| t.id == task.id)
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, "RUNNING",
        "must not be marked FAILED by the stale dispatcher"
    );
    assert_eq!(reloaded.worker_id.as_deref(), Some("thief"));
}

// ---------------------------------------------------------------------------
// persist_workflow_continue_as_new -- a related gap of the identical shape
// found while auditing this call chain (not originally named in issue
// #1184's confirmed call-site list): the transaction that seals the
// predecessor execution (CONTINUED_AS_NEW) and completes its task row had no
// ownership check either.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_workflow_continue_as_new_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-continue-as-new", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let execution = load_execution(&url, exec_id).await;
    // Same-type continuation (`new_workflow_type: None`) needs no registered
    // workflow type at all -- `classify_continue_as_new_target` resolves it
    // to `SameType` without touching the registry or the database, so an
    // empty registry is a faithful, minimal fixture for this guard.
    let registry = HandlerRegistry::new(Vec::new(), Vec::new());
    let persistence = WorkflowTaskPersistence::new_for_test(
        &task,
        "dispatcher-a",
        exec_id,
        1,
        Duration::ZERO,
        None,
        None,
        None,
    );

    let mut conn = connect(&url).await;
    let result = persist_workflow_continue_as_new(
        &mut conn,
        &registry,
        persistence,
        &execution,
        serde_json::json!({}),
        None,
    )
    .await;

    assert_eq!(
        result
            .expect_err("a continue-as-new seal whose claim moved must not commit")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
        "the sentinel must name the exact task whose ownership is ambiguous"
    );

    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "a dispatcher that lost the claim must append no terminal event; got {history:?}"
    );
    assert_thief_untouched(&url, exec_id, task.id, "RUNNING", "RUNNING").await;
}

// ---------------------------------------------------------------------------
// SKIP LOCKED false-positive recovery -- the sibling of #1182's
// `a_dispatcher_that_still_owns_the_claim_is_released_when_skip_locked_is_a_false_positive`.
// `claim_still_held_for_update`'s `SKIP LOCKED` guard never blocks, so a
// claim that merely lost a race against an unrelated, transient lock holder
// (not a genuine transfer) reads identically to a real loss. The standalone
// release this drives into (`queue::release_terminal_workflow_claim`, wired
// through `handle_ambiguous_terminal_write_claim` in production) is a plain
// blocking UPDATE specifically so it waits out that contention and recovers
// the task instead of stranding it -- proven here the same way the #1182
// sibling proves it for `release_suspended_workflow_claim`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_dispatcher_that_still_owns_the_claim_is_released_when_skip_locked_is_a_false_positive_1184()
 {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-lock-contention", "dispatcher-a").await;

    // Stand in for a concurrent, unrelated transaction that happens to hold
    // this exact row's lock for a moment -- without touching `worker_id` or
    // `crash_strikes`, so ownership never actually changes hands.
    let mut conn_lock = connect(&url).await;
    conn_lock
        .batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    diesel::sql_query("UPDATE harvest_task_queue SET wake_requested = TRUE WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .execute(&mut conn_lock)
        .await
        .expect("hold the row's lock without changing ownership");

    // The guarded write's SKIP LOCKED probe will read the row as contended
    // (by `conn_lock`, above), not transferred. The standalone release
    // blocks on the held lock rather than skipping it, so this must run
    // concurrently with the lock hold.
    let dispatch_handle = tokio::spawn({
        let url = url.clone();
        let task = task.clone();
        async move {
            let mut conn = connect(&url).await;

            let probe_started = std::time::Instant::now();
            let ambiguous_task_id = persist_workflow_failure(
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
                &mut Vec::new(),
            )
            .await
            .expect_err("SKIP LOCKED never waits, so the row reads as contended immediately")
            .terminal_write_claim_ambiguous()
            .expect("must be the ambiguous-claim sentinel");
            let probe_elapsed = probe_started.elapsed();

            // The standalone release, exactly as `process_task` performs it
            // (via `handle_ambiguous_terminal_write_claim`) after the
            // enclosing transaction has already rolled back -- this is the
            // call that actually blocks on `conn_lock`'s held lock, never
            // the probe above.
            let released = queue::release_terminal_workflow_claim(
                &mut conn,
                ambiguous_task_id,
                "dispatcher-a",
                task.crash_strikes,
            )
            .await?;
            Ok::<_, autumn_harvest::HarvestError>((probe_elapsed, released))
        }
    });

    // Give the spawned probe time to run and reach the blocking release
    // before the lock is released.
    tokio::time::sleep(Duration::from_millis(300)).await;

    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release the held row lock");

    let (probe_elapsed, released) = dispatch_handle
        .await
        .expect("the dispatch task must not panic")
        .expect("the guarded write and its release fallback must not error");
    assert!(
        probe_elapsed < Duration::from_millis(250),
        "the ambiguity probe must never wait on the row's lock -- it took \
         {probe_elapsed:?}, but the lock was held for 300ms before this test even \
         released it, so a probe that blocked would exceed that bound"
    );
    assert!(
        released,
        "a claim that was never actually lost must be released"
    );

    // No terminal decision was made -- the run is untouched.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a false-positive ClaimLost must never terminally fail the run; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(execution.state, "RUNNING");

    // The task was released back to the pool for a fresh dispatch attempt --
    // this is the part an incomplete fix (log-and-return on the ambiguous
    // sentinel) would fail: the row would still show `state = "RUNNING"`,
    // `worker_id = Some("dispatcher-a")`, forever.
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row survives an undecided dispatch");
    assert_eq!(
        reloaded.state, "PENDING",
        "a claim that was never actually lost must be released, not stranded RUNNING"
    );
    assert_eq!(
        reloaded.worker_id, None,
        "the release must clear ownership so any capable worker can re-claim it"
    );
}

// ---------------------------------------------------------------------------
// fail_execution_on_error -- Codex review round 1, P1: this shared
// error-handling glue passed `SuspendedClaimAmbiguous` through un-failed but
// not `TerminalWriteClaimAmbiguous`, so a #1184-guarded write's ambiguity
// (e.g. from `persist_workflow_continue_as_new`'s seal transaction) fell
// through to an ordinary `fail_task_and_execution` call here -- which, if the
// transient contention that caused the ambiguity had cleared by then, would
// actually find the claim still held and commit a real `WorkflowFailed`,
// turning a blameless "no decision" into a genuine terminal failure.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_execution_on_error_passes_terminal_write_claim_ambiguous_through_unfailed() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-error-passthrough", "dispatcher-a").await;

    let mut conn = connect(&url).await;
    let result: Result<(), _> = fail_execution_on_error(
        &mut conn,
        &task,
        "dispatcher-a",
        Err(autumn_harvest::HarvestError::TerminalWriteClaimAmbiguous { task_id: task.id }),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("the ambiguous sentinel must pass through, not be swallowed")
            .terminal_write_claim_ambiguous(),
        Some(task.id),
        "must be the same sentinel, not converted into a terminal failure"
    );

    // Passing through unfailed means this function must attempt NO write at
    // all -- unlike the guarded functions elsewhere in this file, there is no
    // claim to have "moved": the task is still validly claimed the whole
    // time, and the bug this guards against is precisely that an unguarded
    // passthrough would still try (and, if the contention had cleared,
    // succeed at) failing it anyway.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "must append no terminal event; got {history:?}"
    );
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(execution.state, "RUNNING");
    let reloaded = load_tasks(&url, exec_id)
        .await
        .into_iter()
        .next()
        .expect("the task row is untouched");
    assert_eq!(reloaded.state, "RUNNING");
    assert_eq!(reloaded.worker_id.as_deref(), Some("dispatcher-a"));
}

// ---------------------------------------------------------------------------
// move_workflow_to_dlq_for_history_cap -- Codex review round 3, P1: the
// hard-cap terminal-failure/DLQ path had no ownership check at all. A stale
// dispatcher whose claim had already moved could still DLQ and terminally
// fail a run its new owner was actively driving.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn move_workflow_to_dlq_for_history_cap_makes_no_terminal_decision_when_the_claim_moved() {
    let (url, _container) = setup_db().await;
    let (exec_id, task) = seed_claimed_task(&url, "q1184-history-cap", "dispatcher-a").await;
    steal_claim(&url, task.id).await;

    let reason = DeadLetterReason::HistoryCapExceeded {
        count: 10_000,
        cap: 10_000,
        workflow_type: "issue1184_wf".to_string(),
    };

    let mut conn = connect(&url).await;
    let result = move_workflow_to_dlq_for_history_cap(
        &mut conn,
        &task,
        exec_id,
        1,
        "dispatcher-a",
        None,
        reason,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    assert_eq!(
        result
            .expect_err("a hard-cap DLQ write whose claim moved must not commit")
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
