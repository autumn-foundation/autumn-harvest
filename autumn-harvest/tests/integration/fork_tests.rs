#![cfg(feature = "db")]
#![allow(clippy::too_many_lines)]
//! Non-destructive fork of a run (issue #2000).
//!
//! A fork copies a history prefix to a new workflow id. The source stays
//! unchanged. By default, a fork takes each activity result from the source
//! record and never runs the activity again. Live effects need
//! `effects = live`. An erased source is always refused.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::fork::{
    ForkActivityOverride, ForkEffects, WorkflowForkError, WorkflowForkRequest,
    fork_workflow_execution,
};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::integration_e2e::{
    build_test_pool, runtime_config, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state,
};

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

/// Calls of `fork_charge`, keyed by the `tag` of the run.
static CHARGES: LazyLock<Mutex<HashMap<String, u32>>> = LazyLock::new(Mutex::default);

fn charges(tag: &str) -> u32 {
    CHARGES
        .lock()
        .expect("charges lock")
        .get(tag)
        .copied()
        .unwrap_or(0)
}

/// Charges `amount`, then writes a receipt for the charge.
#[workflow]
async fn fork_pay_wf(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let queue = ctx.queue_name().to_string();
    let charge = ctx
        .execute_activity_raw("fork_charge", input.clone(), &queue)
        .await
        .map_err(|e| e.to_string())?;
    let receipt = ctx
        .execute_activity_raw("fork_receipt", json!({ "charge": charge }), &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "charge": charge, "receipt": receipt }))
}

/// The side effect under test. Each call adds one to `CHARGES[tag]`.
#[activity(start_to_close = "60s")]
async fn fork_charge(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let tag = input["tag"].as_str().unwrap_or_default().to_string();
    let mut map = CHARGES.lock().map_err(|e| e.to_string())?;
    let count = map.entry(tag).or_insert(0);
    *count += 1;
    Ok(json!({ "charge_id": format!("ch-{count}"), "amount": input["amount"] }))
}

#[activity(start_to_close = "60s")]
async fn fork_receipt(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    Ok(json!({ "receipt_for": input["charge"]["charge_id"] }))
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![fork_pay_wf_info()],
        activities![fork_charge, fork_receipt],
    ))
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Running {
    fn start(queue: &str, pool: &DbPool) -> Self {
        let mut config = runtime_config(&format!("w-{queue}"), 2, 2, Duration::from_secs(10));
        config.queues = vec![queue.to_string()];
        let worker = Arc::new(Worker::new(config, registry()).expect("worker builds"));
        let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
        Self { worker, handle }
    }

    async fn stop(self) {
        self.worker.shutdown();
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("the worker must stop")
            .expect("the worker task must not panic");
    }
}

/// A queue and a tag that no earlier run used. A shared database keeps rows.
fn unique(label: &str) -> String {
    format!("q2000-{label}-{}", Uuid::new_v4().simple())
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to Postgres")
}

/// Start a `fork_pay_wf` run on `queue`.
async fn seed_run(conn: &mut AsyncPgConnection, queue: &str, input: &Value) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "fork_pay_wf",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
        parent_id: None,
        queue_name: queue,
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
        .expect("insert workflow execution");
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input.clone());
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

/// Run a source to `COMPLETED` and return it.
async fn completed_source(url: &str, pool: &DbPool, queue: &str, tag: &str) -> ExecutionId {
    let mut conn = connect(url).await;
    let source = seed_run(&mut conn, queue, &json!({ "tag": tag, "amount": 42 })).await;
    let running = Running::start(queue, pool);
    wait_for_execution_state(url, source, "COMPLETED").await;
    running.stop().await;
    assert_eq!(charges(tag), 1, "the source charges once");
    source
}

/// The stored rows of an execution and its history, for a before/after check.
async fn snapshot(url: &str, exec_id: ExecutionId) -> (WorkflowExecution, Vec<(i32, String, Value)>) {
    let mut conn = connect(url).await;
    let row = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load execution");
    let events = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select((
            harvest_events::event_id,
            harvest_events::event_type,
            harvest_events::event_data,
        ))
        .load::<(i32, String, Value)>(&mut conn)
        .await
        .expect("load events");
    (row, events)
}

fn request(effects: ForkEffects) -> WorkflowForkRequest {
    WorkflowForkRequest {
        reason: "what-if".to_string(),
        operator_id: "tester".to_string(),
        effects,
        ..WorkflowForkRequest::default()
    }
}

async fn fork(url: &str, source: ExecutionId, request: WorkflowForkRequest) -> ExecutionId {
    let mut conn = connect(url).await;
    fork_workflow_execution(&mut conn, source, request, Some(&registry()))
        .await
        .expect("fork succeeds")
        .new_exec_id
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// AC1: a completed run can be forked, and the source is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_of_a_completed_run_leaves_the_source_unchanged() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("ac1");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    let before = snapshot(&url, source).await;

    let forked = fork(&url, source, request(ForkEffects::Recorded)).await;
    let running = Running::start(&queue, &pool);
    let fork_row = wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;

    let after = snapshot(&url, source).await;
    assert_eq!(before.1, after.1, "the source history is unchanged");
    assert_eq!(before.0.state, after.0.state);
    assert_eq!(before.0.output, after.0.output);
    assert_eq!(before.0.completed_at, after.0.completed_at);
    assert_eq!(before.0.error, after.0.error);

    assert_ne!(fork_row.workflow_id, before.0.workflow_id, "a new workflow id");
    assert_eq!(fork_row.parent_id, None, "a fork is a root");
    assert_eq!(fork_row.start_source.as_deref(), Some("fork"));
    let source_ref = source.to_string();
    assert_eq!(fork_row.start_source_ref.as_deref(), Some(source_ref.as_str()));
    assert_eq!(fork_row.output, before.0.output, "the fork returns the recorded result");

    let (_, fork_events) = snapshot(&url, forked).await;
    let marker = fork_events
        .iter()
        .find(|(_, kind, _)| kind == "WorkflowForked")
        .expect("the fork history names its source");
    assert_eq!(
        marker.2["data"]["forked_from_exec_id"],
        json!(source.to_string())
    );
}

/// AC2: by default, a fork does not run an activity that the source completed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_fork_does_not_run_a_completed_activity_again() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("ac2");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    // `WorkflowForkRequest::default()` sets no effects mode.
    let forked = fork(
        &url,
        source,
        WorkflowForkRequest {
            reason: "replay".to_string(),
            ..WorkflowForkRequest::default()
        },
    )
    .await;
    let running = Running::start(&queue, &pool);
    wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 1, "the fork must not charge again");
}

/// AC3: a live effect needs `effects = live`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_fork_runs_the_activity() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("ac3");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    let forked = fork(&url, source, request(ForkEffects::Live)).await;
    let running = Running::start(&queue, &pool);
    wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 2, "a live fork runs the activity");
}

/// A recorded fork with no matching record fails closed. It never runs the
/// activity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_fork_fails_closed_without_a_record() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("closed");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    let mut changed = request(ForkEffects::Recorded);
    changed.input = Some(json!({ "tag": queue, "amount": 7 }));
    let forked = fork(&url, source, changed).await;
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, forked, "FAILED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 1, "no record, so no charge");
    let error = row.error.unwrap_or_default();
    assert!(
        error.contains("ForkEffectUnavailable") || error.contains("no recorded result"),
        "the failure names the missing record: {error}"
    );
}

/// An override replaces the result of one activity at the fork point.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_replaces_a_recorded_result() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("override");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    let mut overridden = request(ForkEffects::Recorded);
    // The new charge changes the receipt input, so no record matches it. The
    // receipt therefore needs its own override.
    overridden.activity_overrides = vec![
        ForkActivityOverride {
            activity_name: "fork_charge".to_string(),
            occurrence: 1,
            output: json!({ "charge_id": "stub", "amount": 0 }),
        },
        ForkActivityOverride {
            activity_name: "fork_receipt".to_string(),
            occurrence: 1,
            output: json!({ "receipt_for": "stub" }),
        },
    ];
    let forked = fork(&url, source, overridden).await;
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 1, "an override never runs the activity");
    let output = row.output.expect("fork output");
    assert_eq!(output["charge"]["charge_id"], json!("stub"));
    assert_eq!(output["receipt"]["receipt_for"], json!("stub"));
}

/// AC4: an erased source is always refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_refuses_an_erased_source() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("erased");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    let mut conn = connect(&url).await;
    autumn_harvest::erase::erase_workflow_payloads(&mut conn, source, "gdpr")
        .await
        .expect("erase the source");

    for effects in [ForkEffects::Recorded, ForkEffects::Live] {
        let error = fork_workflow_execution(&mut conn, source, request(effects), None)
            .await
            .expect_err("an erased source is refused");
        assert!(
            matches!(error, WorkflowForkError::ErasedSource { .. }),
            "unexpected error: {error}"
        );
    }
}

/// A running source stays running, with no new event, after a fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_of_a_running_source_leaves_it_running() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("running");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let before = snapshot(&url, source).await;

    let forked = fork(&url, source, request(ForkEffects::Recorded)).await;

    let after = snapshot(&url, source).await;
    assert_eq!(after.0.state, "RUNNING");
    assert_eq!(before.1, after.1, "the source history is unchanged");
    let fork_row = snapshot(&url, forked).await.0;
    assert_eq!(fork_row.state, "RUNNING");
}

/// A caller-chosen workflow id that a live run holds is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_refuses_a_workflow_id_in_use() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("id");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let taken = snapshot(&url, source).await.0.workflow_id;

    let mut same_id = request(ForkEffects::Recorded);
    same_id.workflow_id = Some(taken);
    let error = fork_workflow_execution(&mut conn, source, same_id, None)
        .await
        .expect_err("a workflow id in use is refused");
    assert!(
        matches!(error, WorkflowForkError::WorkflowIdInUse { .. }),
        "unexpected error: {error}"
    );
}
