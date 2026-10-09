//! Red tests of the TLA+ trace check (issue #2003).
//!
//! Each test runs one race of a fixed protocol on a real Postgres. The
//! engine fences the stale write, so its trace is legal. The test then
//! injects the stale write with the guard from before the fix. The
//! connection names the stale claim, so the trace shows the writer.
//!
//! The trace header expects two results. The fixed spec must reject the
//! trace. The pre-fix spec must accept it. A rejection for another reason,
//! such as a malformed trace, therefore fails the check.
//!
//! These tests always record. `chaos.yml` runs
//! `scripts/check-formal-traces.sh` on the exported traces, so a check that
//! accepts an injected violation fails CI.

use std::sync::Arc;

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ActivityExecId;
use autumn_harvest::worker::{
    HandlerRegistry, append_activity_started_for_test, chaos_drive_one_workflow_task,
    finalize_activity_completion,
};
use autumn_harvest::{ExecutionId, ShardId};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

use super::tla_trace::{self, TaskTrace};
use super::{base_params, chaos_db, chaos_noop_info, connect, exec_state};

/// The activity name of the activity fixture.
const ACTIVITY: &str = "trace_red_activity";

/// The next free event id of `exec_id`.
async fn next_event_id(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct MaxRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        n: i32,
    }
    diesel::sql_query(
        "SELECT (COALESCE(MAX(event_id), -1) + 1)::int AS n FROM harvest_events \
         WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<MaxRow>(conn)
    .await
    .expect("read the last event id")
    .n
}

/// Claim the one due task of `queue_name` as `worker`.
async fn claim(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    worker: &str,
) -> autumn_harvest::models::TaskQueueItem {
    queue::claim_task(conn, &[queue_name.to_string()], worker, "", None, &[], &[])
        .await
        .expect("claim")
        .expect("a task is due")
}

/// The trace of `task` in `traces`.
fn trace_of(traces: &[TaskTrace], task: Uuid) -> &TaskTrace {
    traces
        .iter()
        .find(|t| t.task_id == task)
        .unwrap_or_else(|| panic!("no trace for task {task}"))
}

/// The last line must be the injected write. It names the stale claim while
/// the row holds a later attempt, so the fence is the only reason to reject.
fn assert_stale_last_line(trace: &TaskTrace, worker: &str, stale: i32, held: i32) {
    let last = trace.lines.last().expect("the trace has lines");
    assert_eq!(last["state"], "COMPLETED", "{last}");
    assert_eq!(last["terminal"], 1, "{last}");
    assert_eq!(last["attempt"], held, "{last}");
    assert_eq!(
        last["actor"],
        json!({ "worker": worker, "attempt": stale }),
        "{last}"
    );
}

/// `WorkflowTaskClaim`, issue #1806: a stale decision cycle closes the run.
///
/// 1. `w` claims the workflow task: attempt 1.
/// 2. The stuck-running requeue frees the row and keeps `crash_strikes`.
/// 3. `w` claims the row again: attempt 2, the same strikes.
/// 4. The stale cycle of attempt 1 runs. The engine fences it.
/// 5. The test injects the pre-#1806 persist of attempt 1. Its guard has no
///    `attempt` term, so it closes the run that attempt 2 holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::significant_drop_tightening)]
async fn trace_check_red_stale_workflow_persist() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;
    tla_trace::install_now(&mut conn).await;
    let worker = "trace-red-w";

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params("chaos_noop", "trace-red-wf", exec_id, Value::Null);
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_noop");
    let stale = claim(&mut conn, "default", worker).await;

    // The stuck-running requeue of `poison_pill::requeue_stuck_task`.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL, \
         started_at = NULL, last_heartbeat_at = NULL, scheduled_at = NOW() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(stale.id)
    .execute(&mut conn)
    .await
    .expect("stuck requeue");
    let held = claim(&mut conn, "default", worker).await;
    assert_eq!(held.id, stale.id);
    assert_eq!(held.attempt, stale.attempt + 1);
    assert_eq!(held.crash_strikes, stale.crash_strikes);

    // The real stale cycle. The fence stops its terminal write.
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));
    let stale_url = tla_trace::actor_url(&url, stale.id, worker, stale.attempt);
    let outcome =
        chaos_drive_one_workflow_task(&stale_url, registry, stale.clone(), worker.into()).await;
    // The fence finds that the row holds a later claim, so the cycle gives
    // up its terminal write.
    assert!(
        matches!(
            outcome,
            Ok(Err(HarvestError::TerminalWriteClaimAmbiguous { task_id })) if task_id == stale.id
        ),
        "the fence must stop the stale cycle: {outcome:?}"
    );
    assert_eq!(
        exec_state(&mut conn, exec_id).await,
        "RUNNING",
        "the fence must stop the stale cycle"
    );

    // Inject the persist of attempt 1 with the guard from before #1806.
    let mut injector = connect(&stale_url).await;
    let event_id = next_event_id(&mut injector, exec_id).await;
    injector
        .transaction::<(), autumn_harvest::error::HarvestError, _>(async |c| {
            autumn_harvest::store::append_events(
                c,
                exec_id,
                &[WorkflowEvent::WorkflowCompleted {
                    output: json!("stale"),
                }],
                event_id,
            )
            .await?;
            let closed = diesel::sql_query(
                "UPDATE harvest_task_queue SET state = 'COMPLETED' \
                 WHERE id = $1 AND state = 'RUNNING' AND worker_id = $2 AND crash_strikes = $3",
            )
            .bind::<diesel::sql_types::Uuid, _>(stale.id)
            .bind::<diesel::sql_types::Text, _>(worker)
            .bind::<diesel::sql_types::Integer, _>(stale.crash_strikes)
            .execute(c)
            .await
            .map_err(autumn_harvest::error::database_error)?;
            assert_eq!(closed, 1, "the pre-fix guard must match the held row");
            Ok(())
        })
        .await
        .expect("inject the stale persist");

    let traces = tla_trace::take(&mut conn).await;
    let trace = trace_of(&traces, stale.id);
    assert_eq!(trace.spec, "WorkflowTaskClaim");
    assert_stale_last_line(trace, worker, stale.attempt, held.attempt);
    tla_trace::write("red-stale-workflow-persist", &traces, |t| {
        if t.task_id == stale.id {
            tla_trace::reject_last(t, "accept")
        } else {
            tla_trace::accept()
        }
    });
    tla_trace::uninstall_unless_recording(&mut conn).await;
}

/// Seed a run with one scheduled activity on its own queue. Return the run,
/// the activity id, the queue and the activity task id.
async fn seed_activity(
    conn: &mut AsyncPgConnection,
) -> (ExecutionId, ActivityExecId, String, Uuid) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params("chaos_noop", "trace-red-act-wf", exec_id, Value::Null);
    autumn_harvest::execution::start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start the run");
    let queue_name = format!("trace-red-{}", Uuid::new_v4().simple());
    let activity_id = ActivityExecId::new();
    let event_id = next_event_id(conn, exec_id).await;
    autumn_harvest::store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: ACTIVITY.to_string(),
            input: json!({}),
            queue: queue_name.clone(),
        }],
        event_id,
    )
    .await
    .expect("schedule the activity");
    let mut params = EnqueueParams::new(&queue_name, TaskType::Activity, json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some(ACTIVITY.to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 5;
    params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue::enqueue(conn, &params).await.expect("enqueue");
    (exec_id, activity_id, queue_name, task_id)
}

/// `ActivityClaim`, issue #1789: a stale activity attempt finishes the task.
///
/// 1. `w` claims and starts the activity: attempt 1.
/// 2. `w` was never registered, so the orphan reclaimer requeues the row.
/// 3. `w` claims and starts the row again: attempt 2.
/// 4. The stale attempt 1 finishes. The engine fences it.
/// 5. The test injects the pre-#1789 finish of attempt 1. Its guard checks
///    only `state = 'RUNNING'`, so it finishes the task that attempt 2 holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::significant_drop_tightening)]
async fn trace_check_red_stale_activity_finish() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;
    tla_trace::install_now(&mut conn).await;
    let worker = "trace-red-a";
    let codecs = PayloadCodecs::default();

    let (exec_id, activity_id, queue_name, task_id) = seed_activity(&mut conn).await;
    let stale = claim(&mut conn, &queue_name, worker).await;
    assert_eq!(stale.id, task_id);
    let mut stale_conn = connect(&tla_trace::actor_url(&url, task_id, worker, stale.attempt)).await;
    let started = append_activity_started_for_test(
        &mut stale_conn,
        &stale,
        exec_id,
        ACTIVITY,
        worker,
        &codecs,
    )
    .await
    .expect("attempt 1 starts");
    assert_eq!(started, Some(activity_id));

    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut conn,
        3,
        0,
        None,
        &NoOpMetrics,
        &codecs,
    )
    .await
    .expect("reclaim");
    assert!(
        summary.requeued >= 1,
        "the reclaimer must requeue attempt 1"
    );

    let held = claim(&mut conn, &queue_name, worker).await;
    assert_eq!(held.attempt, stale.attempt + 1);
    let mut held_conn = connect(&tla_trace::actor_url(&url, task_id, worker, held.attempt)).await;
    let started =
        append_activity_started_for_test(&mut held_conn, &held, exec_id, ACTIVITY, worker, &codecs)
            .await
            .expect("attempt 2 starts");
    assert_eq!(started, Some(activity_id));

    // The real stale finish. The fence stops it.
    finalize_activity_completion(
        &mut stale_conn,
        &stale,
        exec_id,
        activity_id,
        json!("stale"),
        None,
        &codecs,
    )
    .await
    .expect("a lost claim is not an error");
    let (row_state, _) = super::task_state(&mut conn, task_id).await;
    assert_eq!(row_state, "RUNNING", "the fence must stop the stale finish");

    // Inject the finish of attempt 1 with the guard from before #1789.
    let event_id = next_event_id(&mut stale_conn, exec_id).await;
    stale_conn
        .transaction::<(), autumn_harvest::error::HarvestError, _>(async |c| {
            autumn_harvest::store::append_events(
                c,
                exec_id,
                &[WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: json!("stale"),
                }],
                event_id,
            )
            .await?;
            let finished = diesel::sql_query(
                "UPDATE harvest_task_queue SET state = 'COMPLETED' \
                 WHERE id = $1 AND state = 'RUNNING'",
            )
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .execute(c)
            .await
            .map_err(autumn_harvest::error::database_error)?;
            assert_eq!(finished, 1, "the pre-fix guard must match the held row");
            Ok(())
        })
        .await
        .expect("inject the stale finish");

    let traces = tla_trace::take(&mut conn).await;
    let trace = trace_of(&traces, task_id);
    assert_eq!(trace.spec, "ActivityClaim");
    assert_stale_last_line(trace, worker, stale.attempt, held.attempt);
    tla_trace::write("red-stale-activity-finish", &traces, |t| {
        if t.task_id == task_id {
            tla_trace::reject_last(t, "accept")
        } else {
            tla_trace::accept()
        }
    });
    tla_trace::uninstall_unless_recording(&mut conn).await;
}
