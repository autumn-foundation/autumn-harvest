//! Drain release of a claim that never started (issue #1813).
//!
//! The hold parks the dispatch task between the claim and the task start.
//! The test stops the worker in that window. The drain must give the claim
//! back at once, not leave it to lease expiry.

use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::chaos::points::WORKER_DISPATCH_BEFORE_START;
use autumn_harvest::chaos::{ChaosPlan, arm};
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use super::{base_params, chaos_db, chaos_noop_info, connect, exec_state};

/// The worker's drain budget in this test.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The workflow task row of `exec_id`: `(id, state, worker_id, attempt)`.
async fn workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (uuid::Uuid, String, Option<String>, i32) {
    use autumn_harvest::schema::harvest_task_queue::dsl;
    dsl::harvest_task_queue
        .filter(dsl::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(dsl::task_type.eq("workflow"))
        .select((dsl::id, dsl::state, dsl::worker_id, dsl::attempt))
        .first(conn)
        .await
        .expect("load workflow task")
}

/// A shutdown between the claim and the start releases the claim.
///
/// The row must be `PENDING` again with its `attempt` restored, and no
/// handler may run. The drain must also end well inside its deadline.
///
/// RED: before the fix the task starts during the drain and completes the
/// workflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// `_body` holds the shared-DB guard to the end. See `DB_BODY_SERIAL`.
#[allow(clippy::significant_drop_tightening)]
async fn chaos_repro_1813_drain_releases_a_claim_that_never_started() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params("chaos_noop", "c1813-wf", exec_id, serde_json::json!(null));
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_noop");

    let guard = arm(ChaosPlan::scripted().hold_at(WORKER_DISPATCH_BEFORE_START)).await;
    let hold = guard.hold(WORKER_DISPATCH_BEFORE_START);

    let mut config =
        crate::integration_e2e::runtime_config("c1813-worker", 2, 2, Duration::from_secs(10));
    config.shutdown_timeout = SHUTDOWN_TIMEOUT;
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));
    let worker = Arc::new(Worker::new(config, registry).expect("worker builds"));
    let pool = crate::integration_e2e::build_test_pool(&url);
    let handle = crate::integration_e2e::spawn_test_worker(Arc::clone(&worker), pool);

    tokio::time::timeout(Duration::from_secs(10), hold.reached())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the claimed task must reach the hold; {}",
                guard.diagnostics()
            )
        });
    let (task_id, state, owner, claimed_attempt) = workflow_task(&mut conn, exec_id).await;
    assert_eq!(state, "RUNNING", "the hold parks a claimed task");
    assert_eq!(owner.as_deref(), Some("c1813-worker"));

    let stop_started = Instant::now();
    worker.shutdown();
    hold.release();
    tokio::time::timeout(SHUTDOWN_TIMEOUT * 2, handle)
        .await
        .expect("the worker must stop")
        .expect("the worker task must not panic");
    let drain = stop_started.elapsed();
    let diag = guard.diagnostics();
    drop(guard);

    let (id, state, owner, attempt) = workflow_task(&mut conn, exec_id).await;
    assert_eq!(id, task_id);
    assert_eq!(
        state, "PENDING",
        "the drain must release a claim that never started; {diag}"
    );
    assert!(owner.is_none(), "a released row has no owner; {diag}");
    assert_eq!(
        attempt,
        claimed_attempt - 1,
        "a task that never started must not spend an attempt; {diag}"
    );
    assert_ne!(
        exec_state(&mut conn, exec_id).await,
        "COMPLETED",
        "no handler may run after shutdown starts; {diag}"
    );
    assert!(
        drain < SHUTDOWN_TIMEOUT,
        "the release must not wait for the drain deadline: took {drain:?}; {diag}"
    );
}
