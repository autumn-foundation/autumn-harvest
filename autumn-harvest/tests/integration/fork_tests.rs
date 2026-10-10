#![cfg(feature = "db")]
// Activity handlers must be `async`, even with no await.
#![allow(clippy::too_many_lines, clippy::unused_async)]
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
    fork_workflow_execution, is_recorded_fork,
};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::reset::{ResetPoint, WorkflowResetRequest, reset_workflow_execution};
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

/// Calls of `fork_lookup`, keyed by the `tag` of the run.
static LOOKUPS: LazyLock<Mutex<HashMap<String, u32>>> = LazyLock::new(Mutex::default);

fn lookups(tag: &str) -> u32 {
    LOOKUPS
        .lock()
        .expect("lookups lock")
        .get(tag)
        .copied()
        .unwrap_or(0)
}

/// Charges `amount`, then writes a receipt for the charge. With
/// `"local": true`, it first runs the local activity `fork_lookup`. With
/// `"session": true`, it charges in a worker session. With `"race": true`,
/// the charge races a receipt on a queue that no worker polls, and wins.
#[workflow]
async fn fork_pay_wf(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let queue = ctx.queue_name().to_string();
    if input["race"] == json!(true) {
        let won = ctx
            .race()
            .activity_raw("fork_charge", input.clone(), &queue)
            .activity_raw(
                "fork_receipt",
                json!({ "charge": "none" }),
                &format!("{queue}-idle"),
            )
            .run()
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!({ "winner": won.index, "value": won.value }));
    }
    if input["local"] == json!(true) {
        ctx.execute_local_activity_raw("fork_lookup", input.clone(), None, Some(30))
            .await
            .map_err(|e| e.to_string())?;
    }
    let charge = if input["session"] == json!(true) {
        let session = ctx
            .create_session(autumn_harvest::context::SessionOptions::new(&queue))
            .await
            .map_err(|e| e.to_string())?;
        let charge = session
            .execute_activity_raw("fork_charge", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?;
        session.complete().await.map_err(|e| e.to_string())?;
        charge
    } else {
        ctx.execute_activity_raw("fork_charge", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?
    };
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
    let entry = map.entry(tag).or_insert(0);
    *entry += 1;
    let count = *entry;
    drop(map);
    Ok(json!({ "charge_id": format!("ch-{count}"), "amount": input["amount"] }))
}

/// A local activity. Each call adds one to `LOOKUPS[tag]`.
#[activity(start_to_close = "60s")]
async fn fork_lookup(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let tag = input["tag"].as_str().unwrap_or_default().to_string();
    *LOOKUPS
        .lock()
        .map_err(|e| e.to_string())?
        .entry(tag)
        .or_insert(0) += 1;
    Ok(json!("found"))
}

#[activity(start_to_close = "60s")]
async fn fork_receipt(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    Ok(json!({ "receipt_for": input["charge"]["charge_id"] }))
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![fork_pay_wf_info()],
        activities![fork_charge, fork_receipt, fork_lookup],
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
        Self::start_with(queue, pool, registry())
    }

    fn start_with(queue: &str, pool: &DbPool, registry: Arc<HandlerRegistry>) -> Self {
        let mut config = runtime_config(&format!("w-{queue}"), 2, 2, Duration::from_secs(10));
        config.queues = vec![queue.to_string()];
        config.max_concurrent_sessions = 2;
        let worker = Arc::new(Worker::new(config, registry).expect("worker builds"));
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
    seed_run_with(conn, queue, input, None, None).await
}

/// Start a `fork_pay_wf` run with a carryover, through an optional offloader.
async fn seed_run_with(
    conn: &mut AsyncPgConnection,
    queue: &str,
    input: &Value,
    last_completion_result: Option<Value>,
    offloader: Option<&autumn_harvest::payload_store::PayloadOffloader>,
) -> ExecutionId {
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
    store::append_events_offloaded_with_codecs(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result,
            last_error: None,
            scheduled_time: None,
        }],
        0,
        offloader,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
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
async fn snapshot(
    url: &str,
    exec_id: ExecutionId,
) -> (WorkflowExecution, Vec<(i32, String, Value)>) {
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
    assert_eq!(
        serde_json::to_value(&before.0).expect("row json"),
        serde_json::to_value(&after.0).expect("row json"),
        "the source row is unchanged"
    );

    assert_ne!(
        fork_row.workflow_id, before.0.workflow_id,
        "a new workflow id"
    );
    assert_eq!(fork_row.parent_id, None, "a fork is a root");
    assert_eq!(fork_row.start_source.as_deref(), Some("fork"));
    let source_ref = source.to_string();
    assert_eq!(
        fork_row.start_source_ref.as_deref(),
        Some(source_ref.as_str())
    );
    assert_eq!(
        fork_row.output, before.0.output,
        "the fork returns the recorded result"
    );

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

/// A recorded fork fails before an effect that it cannot serve. The local
/// activity never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_fork_fails_before_a_local_activity() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("local");
    let source = completed_source(&url, &pool, &queue, &queue).await;

    // The new input takes the local-activity branch that the source skipped.
    let mut local = request(ForkEffects::Recorded);
    local.input = Some(json!({ "tag": queue, "amount": 42, "local": true }));
    let forked = fork(&url, source, local).await;
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, forked, "FAILED").await;
    running.stop().await;

    assert_eq!(lookups(&queue), 0, "the local activity never runs");
    assert_eq!(charges(&queue), 1);
    let error = row.error.unwrap_or_default();
    assert!(
        error.contains("RunLocalActivity"),
        "the failure names the effect: {error}"
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

/// Erasure does not reach a fork. A fork of that fork is refused, because
/// its lineage reaches the erased run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_of_a_fork_of_an_erased_source_is_refused() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("lineage");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    let first = fork(&url, source, request(ForkEffects::Recorded)).await;

    let mut conn = connect(&url).await;
    autumn_harvest::erase::erase_workflow_payloads(&mut conn, source, "gdpr")
        .await
        .expect("erase the source");
    let error = fork_workflow_execution(&mut conn, first, request(ForkEffects::Live), None)
        .await
        .expect_err("the lineage reaches an erased run");
    assert!(
        matches!(error, WorkflowForkError::ErasedSource { exec_id } if exec_id == source),
        "unexpected error: {error}"
    );
}

/// A fork lineage deeper than the scan bound is refused. The walk cannot
/// prove that no erased run lies above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_lineage_past_the_scan_bound_is_refused() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("deep");
    let mut conn = connect(&url).await;
    // Link 66 rows as forks, each of the previous one, with fork provenance.
    let mut parent = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    for _ in 0..66 {
        let child = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
        diesel::update(harvest_workflow_executions::table.find(child.as_uuid()))
            .set((
                harvest_workflow_executions::start_source.eq(Some("fork")),
                harvest_workflow_executions::start_source_ref.eq(Some(parent.to_string())),
            ))
            .execute(&mut conn)
            .await
            .expect("link the chain");
        parent = child;
    }
    let error = fork_workflow_execution(&mut conn, parent, request(ForkEffects::Recorded), None)
        .await
        .expect_err("a lineage past the bound is refused");
    assert!(
        matches!(error, WorkflowForkError::LineageTooDeep { .. }),
        "unexpected error: {error}"
    );
}

/// A fork at a later point carries the completed charge and takes the
/// receipt from the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_at_a_later_point_carries_the_prefix() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("later");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    let source_row = snapshot(&url, source).await.0;

    let mut later = request(ForkEffects::Recorded);
    later.fork_point = Some(ResetPoint::FirstActivityRun {
        activity_name: "fork_receipt".to_string(),
    });
    let mut conn = connect(&url).await;
    let result = fork_workflow_execution(&mut conn, source, later, Some(&registry()))
        .await
        .expect("fork succeeds");
    assert!(result.fork_event_id > 0, "the fork starts after the charge");
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, result.new_exec_id, "COMPLETED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 1, "the carried charge does not run again");
    assert_eq!(row.output, source_row.output);
    let (_, events) = snapshot(&url, result.new_exec_id).await;
    let carried = usize::try_from(result.fork_event_id).expect("event id") + 1;
    assert_eq!(
        events[carried].1, "WorkflowForked",
        "the marker follows the prefix"
    );
    assert!(
        events[..carried]
            .iter()
            .any(|(_, kind, _)| kind == "ActivityCompleted"),
        "the prefix carries the charge result"
    );
}

/// A recorded fork serves a member of a session that the source closed. The
/// record settles the member before the broken-session check, so the fork
/// gets the recorded charge and not `SessionBroken`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_fork_serves_a_member_of_a_closed_session() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("session");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 5, "session": true });
    let source = seed_run(&mut conn, &queue, &input).await;
    let running = Running::start(&queue, &pool);
    let source_row = wait_for_execution_state(&url, source, "COMPLETED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1, "the source charges once");

    let mut at_member = request(ForkEffects::Recorded);
    at_member.fork_point = Some(ResetPoint::FirstActivityRun {
        activity_name: "fork_charge".to_string(),
    });
    let result = fork_workflow_execution(&mut conn, source, at_member, Some(&registry()))
        .await
        .expect("fork succeeds");
    assert!(result.fork_event_id > 0, "the fork carries the session");
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, result.new_exec_id, "COMPLETED").await;
    running.stop().await;

    assert_eq!(charges(&queue), 1, "the fork never charges");
    assert_eq!(row.output, source_row.output);
}

/// A reset of a recorded fork stays a recorded fork. It never runs live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_of_a_recorded_fork_stays_recorded() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("reset");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    let forked = fork(&url, source, request(ForkEffects::Recorded)).await;

    // Event 0 is before the fork marker. The reset appends its own marker.
    let mut conn = connect(&url).await;
    let reset = reset_workflow_execution(
        &mut conn,
        forked,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "retry".to_string(),
            operator_id: "tester".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::Drop,
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        Some(&registry()),
    )
    .await
    .expect("reset the fork");
    let reset_row = snapshot(&url, reset.new_exec_id).await.0;
    assert_eq!(reset_row.start_source.as_deref(), Some("fork"));
    assert!(
        is_recorded_fork(&mut conn, &reset_row)
            .await
            .expect("read marker")
    );

    // The reset takes the record of the original source, so it completes
    // with no new charge.
    let running = Running::start(&queue, &pool);
    wait_for_execution_state(&url, reset.new_exec_id, "COMPLETED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1, "the reset fork never charges");
}

/// A reset between the markers of a recorded fork of a live fork stays
/// recorded. The carried prefix holds only the live ancestor marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_of_a_nested_recorded_fork_stays_recorded() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("nested");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    // History of the live fork: WorkflowStarted, WorkflowForked(live).
    let live = fork(&url, source, request(ForkEffects::Live)).await;
    let mut recorded_request = request(ForkEffects::Recorded);
    recorded_request.fork_point = Some(ResetPoint::EventId { event_id: 1 });
    let recorded = fork(&url, live, recorded_request).await;

    // The live fork must not run in this test, so its task never starts.
    let mut conn = connect(&url).await;
    queue::cancel_open_tasks_for_execution(&mut conn, live, "test keeps it idle")
        .await
        .expect("cancel the live fork task");

    // Event 1 is the live marker, before the recorded marker.
    let reset = reset_workflow_execution(
        &mut conn,
        recorded,
        WorkflowResetRequest {
            reset_to_event_id: Some(1),
            reset_point: None,
            reason: "retry".to_string(),
            operator_id: "tester".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::Drop,
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        Some(&registry()),
    )
    .await
    .expect("reset the nested fork");
    let reset_row = snapshot(&url, reset.new_exec_id).await.0;
    assert!(
        is_recorded_fork(&mut conn, &reset_row)
            .await
            .expect("read marker")
    );

    // The live fork never ran, so it holds no record. The reset fails closed.
    let running = Running::start(&queue, &pool);
    wait_for_execution_state(&url, reset.new_exec_id, "FAILED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1, "the reset never charges");
}

/// A reset of a live fork keeps its overrides. The stubbed charge never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_of_a_fork_keeps_its_overrides() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("reset-override");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    // History: WorkflowStarted, WorkflowForked(live), the charge override.
    let mut live = request(ForkEffects::Live);
    live.activity_overrides = vec![ForkActivityOverride {
        activity_name: "fork_charge".to_string(),
        occurrence: 1,
        output: json!({ "charge_id": "stub", "amount": 0 }),
    }];
    let forked = fork(&url, source, live).await;

    // Event 2 carries the override. The reset appends a new marker after it.
    let mut conn = connect(&url).await;
    let reset = reset_workflow_execution(
        &mut conn,
        forked,
        WorkflowResetRequest {
            reset_to_event_id: Some(2),
            reset_point: None,
            reason: "retry".to_string(),
            operator_id: "tester".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::Drop,
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        Some(&registry()),
    )
    .await
    .expect("reset the fork");

    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, reset.new_exec_id, "COMPLETED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1, "the override still stubs the charge");
    assert_eq!(
        row.output.expect("output")["charge"]["charge_id"],
        json!("stub")
    );
}

/// An in-memory blob store for the offload test.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl autumn_harvest::payload_store::PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "fork-test"
    }
    fn put(&self, bytes: &[u8]) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, String> {
        let key = format!("blob/{}", Uuid::new_v4());
        self.blobs
            .lock()
            .expect("blobs lock")
            .insert(key.clone(), bytes.to_vec());
        Box::pin(async move { Ok(key) })
    }
    fn get(&self, key: &str) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().expect("blobs lock").get(key).cloned();
        let key = key.to_string();
        Box::pin(async move {
            found.ok_or_else(|| {
                autumn_harvest::payload_store::PayloadStoreError(format!("missing {key}"))
            })
        })
    }
    fn delete(&self, key: &str) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, ()> {
        self.blobs.lock().expect("blobs lock").remove(key);
        Box::pin(async move { Ok(()) })
    }
}

/// A reset of a fork keeps a reference to each offloaded override blob, so
/// retention of the sealed fork cannot collect it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_of_a_fork_references_its_offloaded_overrides() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("reset-blob");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let offloader = Arc::new(autumn_harvest::payload_store::PayloadOffloader::new(
        Arc::new(MemStore::default()),
        64,
        Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    ));
    let offloading = Arc::new(
        HandlerRegistry::new(
            vec![fork_pay_wf_info()],
            activities![fork_charge, fork_receipt],
        )
        .with_payload_offloader(Some(offloader)),
    );

    let mut live = request(ForkEffects::Live);
    live.activity_overrides = vec![ForkActivityOverride {
        activity_name: "fork_charge".to_string(),
        occurrence: 1,
        output: json!({ "charge_id": "X".repeat(512) }),
    }];
    let forked = fork_workflow_execution(&mut conn, source, live, Some(&offloading))
        .await
        .expect("fork")
        .new_exec_id;
    let fork_refs = store::load_payload_refs(&mut conn, forked)
        .await
        .expect("refs");
    assert_eq!(fork_refs.len(), 1, "the override output is offloaded");

    let reset = reset_workflow_execution(
        &mut conn,
        forked,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "retry".to_string(),
            operator_id: "tester".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::Drop,
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        Some(&offloading),
    )
    .await
    .expect("reset the fork");
    let reset_refs = store::load_payload_refs(&mut conn, reset.new_exec_id)
        .await
        .expect("refs");
    assert_eq!(
        reset_refs.iter().map(|r| &r.blob_key).collect::<Vec<_>>(),
        fork_refs.iter().map(|r| &r.blob_key).collect::<Vec<_>>(),
        "the reset references the override blob"
    );
}

/// An input override keeps an offloaded carryover as an envelope. The fork
/// references its blob, so retention of the source cannot collect it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_input_override_keeps_an_offloaded_carryover() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("carryover");
    let mut conn = connect(&url).await;
    let offloader = Arc::new(autumn_harvest::payload_store::PayloadOffloader::new(
        Arc::new(MemStore::default()),
        64,
        Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    ));
    let carryover = json!({ "previous": "X".repeat(512) });
    let source = seed_run_with(
        &mut conn,
        &queue,
        &json!({ "n": 1 }),
        Some(carryover),
        Some(&offloader),
    )
    .await;
    let source_refs = store::load_payload_refs(&mut conn, source)
        .await
        .expect("refs");
    assert_eq!(source_refs.len(), 1, "the carryover is offloaded");
    let offloading = Arc::new(
        HandlerRegistry::new(
            vec![fork_pay_wf_info()],
            activities![fork_charge, fork_receipt],
        )
        .with_payload_offloader(Some(offloader)),
    );

    let mut what_if = request(ForkEffects::Live);
    what_if.input = Some(json!({ "n": 2 }));
    let forked = fork_workflow_execution(&mut conn, source, what_if, Some(&offloading))
        .await
        .expect("fork")
        .new_exec_id;

    let (_, source_events) = snapshot(&url, source).await;
    let (_, fork_events) = snapshot(&url, forked).await;
    let start = &fork_events[0].2;
    assert_eq!(start["data"]["input"], json!({ "n": 2 }));
    assert_eq!(
        start["data"]["last_completion_result"],
        source_events[0].2["data"]["last_completion_result"],
        "the carryover keeps its stored envelope"
    );
    let fork_refs = store::load_payload_refs(&mut conn, forked)
        .await
        .expect("refs");
    assert_eq!(
        fork_refs.iter().map(|r| &r.blob_key).collect::<Vec<_>>(),
        source_refs.iter().map(|r| &r.blob_key).collect::<Vec<_>>(),
        "the fork references the carryover blob"
    );
}

/// A live fork never reads the source history. A source blob that is gone
/// does not stop the fork from running its activities.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_fork_does_not_read_the_source_history() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("live-blob");
    let mut conn = connect(&url).await;
    let blobs = Arc::new(MemStore::default());
    let offloader = Arc::new(autumn_harvest::payload_store::PayloadOffloader::new(
        Arc::clone(&blobs) as Arc<dyn autumn_harvest::payload_store::PayloadStore>,
        64,
        Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    ));
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    // Only the fork runs. The source keeps its history.
    diesel::delete(autumn_harvest::schema::harvest_task_queue::table.filter(
        autumn_harvest::schema::harvest_task_queue::workflow_exec_id.eq(Some(source.as_uuid())),
    ))
    .execute(&mut conn)
    .await
    .expect("drop the source task");
    store::append_events_offloaded_with_codecs(
        &mut conn,
        source,
        &[WorkflowEvent::ActivityScheduled {
            activity_id: autumn_harvest::types::ActivityExecId::new(),
            name: "fork_charge".to_string(),
            input: json!({ "blob": "X".repeat(512) }),
            queue: queue.clone(),
        }],
        1,
        Some(&offloader),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("append an offloaded event");
    blobs.blobs.lock().expect("blobs lock").clear();

    let forked = fork(&url, source, request(ForkEffects::Live)).await;
    let offloading = Arc::new(
        HandlerRegistry::new(
            vec![fork_pay_wf_info()],
            activities![fork_charge, fork_receipt],
        )
        .with_payload_offloader(Some(offloader)),
    );
    let running = Running::start_with(&queue, &pool, offloading);
    wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1, "the live fork charges once");
}

/// A fork copies a prefix longer than one insert can bind. Postgres caps a
/// statement at 65,535 parameters, and each copied row binds four.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_copies_a_prefix_past_the_parameter_limit() {
    const MARKERS: usize = 16_500;
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("long");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let markers = (0..MARKERS)
        .map(|n| WorkflowEvent::MarkerRecorded {
            name: "step".to_string(),
            details: json!(n),
        })
        .collect::<Vec<_>>();
    for (n, chunk) in markers.chunks(1_000).enumerate() {
        let start = i32::try_from(1 + n * 1_000).expect("event id");
        store::append_events(&mut conn, source, chunk, start)
            .await
            .expect("append markers");
    }

    let mut at_end = request(ForkEffects::Recorded);
    at_end.fork_point = Some(ResetPoint::EventId {
        event_id: i64::try_from(MARKERS).expect("event id"),
    });
    let result = fork_workflow_execution(&mut conn, source, at_end, Some(&registry()))
        .await
        .expect("fork succeeds");
    assert_eq!(result.events_carried_over, MARKERS + 1);
    let (_, events) = snapshot(&url, result.new_exec_id).await;
    assert_eq!(events.len(), MARKERS + 2, "the prefix and the marker");
}

/// A fork is admitted under the tenant quota of its workflow type, as a start
/// is. A kept input keeps the key of the source. A new input resolves its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_is_admitted_under_the_tenant_quota() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("quota");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "tenant": queue });
    let source = seed_run(&mut conn, &queue, &input).await;
    // The start of the source admitted it under its tenant key.
    diesel::update(harvest_workflow_executions::table.find(source.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some(queue.as_str())))
        .execute(&mut conn)
        .await
        .expect("stamp the source key");
    let mut info = fork_pay_wf_info();
    info.quota =
        Some(autumn_harvest::quota::QuotaPolicy::new("tenant").with_max_active_executions(2));
    let quota_registry = Arc::new(HandlerRegistry::new(
        vec![info],
        activities![fork_charge, fork_receipt],
    ));

    let first = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&quota_registry),
    )
    .await
    .expect("the source and one fork fit the cap")
    .new_exec_id;
    let row = snapshot(&url, first).await.0;
    assert_eq!(row.quota_key.as_deref(), Some(queue.as_str()));

    let over = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&quota_registry),
    )
    .await;
    assert!(
        matches!(
            over,
            Err(WorkflowForkError::Harvest(
                autumn_harvest::error::HarvestError::QuotaExceeded { .. }
            ))
        ),
        "a third active run of the key is refused: {over:?}"
    );

    let other = format!("{queue}-other");
    let mut elsewhere = request(ForkEffects::Live);
    elsewhere.input = Some(json!({ "tag": queue, "amount": 1, "tenant": other }));
    let moved = fork_workflow_execution(&mut conn, source, elsewhere, Some(&quota_registry))
        .await
        .expect("a new input resolves its own key")
        .new_exec_id;
    let row = snapshot(&url, moved).await.0;
    assert_eq!(row.quota_key.as_deref(), Some(other.as_str()));
}

/// A source with no stored quota key, such as a reset run, resolves the key
/// from its input. A kept input then cannot skip the quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_resolves_a_quota_key_that_the_source_lacks() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("quota-null");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "tenant": queue });
    let source = seed_run(&mut conn, &queue, &input).await;
    let mut info = fork_pay_wf_info();
    info.quota =
        Some(autumn_harvest::quota::QuotaPolicy::new("tenant").with_max_active_executions(5));
    let quota_registry = Arc::new(HandlerRegistry::new(
        vec![info],
        activities![fork_charge, fork_receipt],
    ));
    let forked = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&quota_registry),
    )
    .await
    .expect("fork")
    .new_exec_id;
    let row = snapshot(&url, forked).await.0;
    assert_eq!(row.quota_key.as_deref(), Some(queue.as_str()));
}

/// A kept input resolves its quota key under the current policy. A stale key
/// that the source stored at its own start does not decide the tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_resolves_its_quota_key_under_the_current_policy() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("quota-now");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "tenant": queue });
    let source = seed_run(&mut conn, &queue, &input).await;
    diesel::update(harvest_workflow_executions::table.find(source.as_uuid()))
        .set(harvest_workflow_executions::quota_key.eq(Some("stale-tenant")))
        .execute(&mut conn)
        .await
        .expect("stamp a stale key");
    let mut info = fork_pay_wf_info();
    info.quota =
        Some(autumn_harvest::quota::QuotaPolicy::new("tenant").with_max_active_executions(5));
    let quota_registry = Arc::new(HandlerRegistry::new(
        vec![info],
        activities![fork_charge, fork_receipt],
    ));
    let forked = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&quota_registry),
    )
    .await
    .expect("fork")
    .new_exec_id;
    let row = snapshot(&url, forked).await.0;
    assert_eq!(row.quota_key.as_deref(), Some(queue.as_str()));
}

/// A fork whose lineage reaches a deleted ancestor is refused. Retention can
/// delete an erased run, so a gap cannot prove that the lineage is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_lineage_with_a_deleted_ancestor_is_refused() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("gap");
    let mut conn = connect(&url).await;
    let root = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let first = fork(&url, root, request(ForkEffects::Recorded)).await;
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(root.as_uuid())
        .execute(&mut conn)
        .await
        .expect("retain the root");

    let result = fork_workflow_execution(
        &mut conn,
        first,
        request(ForkEffects::Recorded),
        Some(&registry()),
    )
    .await;
    assert!(
        matches!(result, Err(WorkflowForkError::LineageGap { exec_id }) if exec_id == root),
        "a deleted ancestor fails closed: {result:?}"
    );
}

/// A fork is a fresh admission. An admission gate on its queue refuses it,
/// as the gate refuses a start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admission_gate_refuses_a_fork() {
    use autumn_harvest::admission_gate::{
        AdmissionGate, AdmissionGateCache, AdmissionGateId, GateScope,
        set_global_admission_gate_cache,
    };
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("gate");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let cache = Arc::new(AdmissionGateCache::new());
    cache.refresh(vec![AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope: GateScope::Queue(queue.clone()),
        reason: "incident".to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    set_global_admission_gate_cache(Some(cache));
    let result = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&registry()),
    )
    .await;
    set_global_admission_gate_cache(None);
    assert!(
        matches!(
            result,
            Err(WorkflowForkError::Harvest(
                autumn_harvest::error::HarvestError::AdmissionBlocked { .. }
            ))
        ),
        "the gate refuses the fork: {result:?}"
    );
}

/// The copied prefix counts against the history cap of the tenant quota. A
/// fork starts with that history, so the cap sees it before the insert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_copied_prefix_counts_against_the_history_quota() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("quota-bytes");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "tenant": queue });
    let source = seed_run(&mut conn, &queue, &input).await;
    let mut info = fork_pay_wf_info();
    info.quota = Some(autumn_harvest::quota::QuotaPolicy::new("tenant").with_max_history_bytes(16));
    let quota_registry = Arc::new(HandlerRegistry::new(
        vec![info],
        activities![fork_charge, fork_receipt],
    ));
    let result = fork_workflow_execution(
        &mut conn,
        source,
        request(ForkEffects::Live),
        Some(&quota_registry),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(WorkflowForkError::Harvest(
                autumn_harvest::error::HarvestError::QuotaExceeded {
                    resource: autumn_harvest::quota::QuotaResource::HistoryBytes,
                    ..
                }
            ))
        ),
        "the prefix alone exceeds the cap: {result:?}"
    );
}

/// The history cap counts the rows that the fork stores, not only the source
/// prefix. A large input override must not slip past a cap that the source
/// prefix alone fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_history_quota_counts_a_large_input_override() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("quota-override");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "tenant": queue });
    let source = seed_run(&mut conn, &queue, &input).await;
    let prefix: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(source.as_uuid()))
        .select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
            "COALESCE(SUM(pg_column_size(event_data)), 0)::BIGINT",
        ))
        .first(&mut conn)
        .await
        .expect("measure the prefix");
    let cap = u64::try_from(prefix).expect("size") + 200;
    let mut info = fork_pay_wf_info();
    info.quota =
        Some(autumn_harvest::quota::QuotaPolicy::new("tenant").with_max_history_bytes(cap));
    let quota_registry = Arc::new(HandlerRegistry::new(
        vec![info],
        activities![fork_charge, fork_receipt],
    ));
    let mut large = request(ForkEffects::Live);
    large.input = Some(json!({ "tag": queue, "tenant": queue, "note": "X".repeat(4_096) }));
    let result = fork_workflow_execution(&mut conn, source, large, Some(&quota_registry)).await;
    assert!(
        matches!(
            result,
            Err(WorkflowForkError::Harvest(
                autumn_harvest::error::HarvestError::QuotaExceeded {
                    resource: autumn_harvest::quota::QuotaResource::HistoryBytes,
                    ..
                }
            ))
        ),
        "the large input exceeds the cap: {result:?}"
    );
}

/// A held race loser still gets its loser terminal when the race resolves in
/// the fork. Every activity that the fork schedules then has an outcome, so
/// a later reset or fork point after the race stays valid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_race_loser_gets_its_terminal_in_the_fork() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("race");
    let mut conn = connect(&url).await;
    let input = json!({ "tag": queue, "amount": 1, "race": true });
    let source = seed_run(&mut conn, &queue, &input).await;
    let running = Running::start(&queue, &pool);
    let source_row = wait_for_execution_state(&url, source, "COMPLETED").await;
    running.stop().await;

    let forked = fork(&url, source, request(ForkEffects::Recorded)).await;
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, forked, "COMPLETED").await;
    running.stop().await;
    assert_eq!(row.output, source_row.output);
    assert_eq!(charges(&queue), 1, "the fork never charges");

    let (_, events) = snapshot(&url, forked).await;
    let count = |kinds: &[&str]| {
        events
            .iter()
            .filter(|(_, kind, _)| kinds.contains(&kind.as_str()))
            .count()
    };
    assert_eq!(
        count(&["ActivityScheduled"]),
        count(&["ActivityCompleted", "ActivityFailed"]),
        "every scheduled activity has a terminal: {events:#?}"
    );
}

/// The fork inserts its payload references in chunks. Postgres caps a
/// statement at 65,535 parameters, and each reference binds four.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_references_insert_past_the_parameter_limit() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("refs");
    let mut conn = connect(&url).await;
    let exec_id = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let refs = (0..16_500)
        .map(|n| autumn_harvest::payload_store::OffloadedRef {
            blob_key: format!("{queue}/blob-{n}"),
            store_id: "fork-test".to_string(),
            byte_len: 1,
        })
        .collect::<Vec<_>>();
    store::insert_payload_refs(&mut conn, exec_id, &refs)
        .await
        .expect("insert the references");
    let stored = store::load_payload_refs(&mut conn, exec_id)
        .await
        .expect("load the references");
    assert_eq!(stored.len(), refs.len());
}

/// A source erased after the fork exists serves no record. The fork fails
/// closed and never charges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_erased_after_the_fork_serves_no_record() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let queue = unique("erased-later");
    let source = completed_source(&url, &pool, &queue, &queue).await;
    let forked = fork(&url, source, request(ForkEffects::Recorded)).await;

    let mut conn = connect(&url).await;
    autumn_harvest::erase::erase_workflow_payloads(&mut conn, source, "gdpr")
        .await
        .expect("erase the source");
    let running = Running::start(&queue, &pool);
    let row = wait_for_execution_state(&url, forked, "FAILED").await;
    running.stop().await;
    assert_eq!(charges(&queue), 1);
    let error = row.error.unwrap_or_default();
    assert!(error.contains("no recorded result"), "{error}");
}

/// Only a recorded fork skips completion callbacks and triggers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_recorded_fork_suppresses_completion_notifications() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("notify");
    let mut conn = connect(&url).await;
    let source = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 1 })).await;
    let recorded = fork(&url, source, request(ForkEffects::Recorded)).await;
    let live = fork(&url, source, request(ForkEffects::Live)).await;

    for (exec_id, expected) in [(source, false), (recorded, true), (live, false)] {
        let row = snapshot(&url, exec_id).await.0;
        assert_eq!(
            is_recorded_fork(&mut conn, &row)
                .await
                .expect("read marker"),
            expected,
            "{exec_id}"
        );
    }
    let recorded_row = snapshot(&url, recorded).await.0;
    assert_eq!(recorded_row.completion_callbacks, None);
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
    let other = seed_run(&mut conn, &queue, &json!({ "tag": queue, "amount": 2 })).await;
    let taken = snapshot(&url, other).await.0.workflow_id;

    let mut other_id = request(ForkEffects::Recorded);
    other_id.workflow_id = Some(taken);
    let error = fork_workflow_execution(&mut conn, source, other_id, None)
        .await
        .expect_err("a workflow id in use is refused");
    assert!(
        matches!(error, WorkflowForkError::WorkflowIdInUse { .. }),
        "unexpected error: {error}"
    );

    // The source key routes by-id calls to the real entity, so it is refused
    // even when the source no longer holds it.
    let mut own_id = request(ForkEffects::Recorded);
    own_id.workflow_id = Some(snapshot(&url, source).await.0.workflow_id);
    let error = fork_workflow_execution(&mut conn, source, own_id, None)
        .await
        .expect_err("the source key is refused");
    assert!(
        matches!(error, WorkflowForkError::InvalidOverride { .. }),
        "unexpected error: {error}"
    );
}
