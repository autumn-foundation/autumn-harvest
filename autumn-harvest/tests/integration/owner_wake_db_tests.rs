#![cfg(feature = "db")]
//! Each foreign writer class wakes the owner channel of the run's partition
//! (issue #2253).
//!
//! No worker runs here. A listener stands in for the owner of the run's
//! partition, and a second one for an owner of another partition. Each
//! writer class runs on a plain connection, with no decision pinned. The
//! owner must get one wake for the run, with the expected evict flag. The
//! other owner must get none.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::time::Duration;

use autumn_harvest::decision_owner::{self, DecisionPartition};
use autumn_harvest::notify::{OwnerWakeListener, OwnerWakeWait};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::Value;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env};

/// A wake is a post-commit `NOTIFY`. The sender adds at most 25 ms.
const WAKE_WAIT: Duration = Duration::from_secs(10);

/// How long the other owner waits to show that it gets no wake.
const QUIET_WAIT: Duration = Duration::from_millis(500);

#[workflow]
async fn owner_wake_db_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(input)
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

struct Fixture {
    url: String,
    pool: DbPool,
    conn: AsyncPgConnection,
    queue: String,
    _sink: autumn_harvest::notify::NotifySink,
}

impl Fixture {
    async fn new() -> (Self, Option<impl Sized>) {
        let (url, container) = setup_test_database_url_or_env().await;
        let pool: DbPool = build_test_pool(&url);
        decision_owner::enable_owner_wakes();
        let sink = autumn_harvest::notify::register_pool(&pool);
        assert!(
            sink.wait_ready(Duration::from_secs(10)).await,
            "the notify sender is ready"
        );
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (
            Self {
                url,
                pool,
                conn,
                queue: unique("owner-wake-db-q"),
                _sink: sink,
            },
            container,
        )
    }

    /// Start a run. No worker runs, so it stays at its first task.
    async fn start(&mut self) -> (ExecutionId, DecisionPartition) {
        let workflow_id = unique("owner-wake-db");
        let partition = DecisionPartition::for_workflow_id(&workflow_id);
        let client = WorkflowHandleClient::single(self.pool.clone(), self.url.clone());
        let opts = TypedStartOptions {
            queue_name: Some(self.queue.clone()),
            ..Default::default()
        };
        let exec = OwnerWakeDbFlowStub::start_with_options(
            &mut self.conn,
            &client,
            &workflow_id,
            "x".to_string(),
            opts,
        )
        .await
        .expect("start")
        .exec_id();
        (exec, partition)
    }

    /// The owner of `partition` and an owner of another partition.
    async fn listeners(
        &self,
        partition: DecisionPartition,
    ) -> (OwnerWakeListener, OwnerWakeListener) {
        let mut owner = OwnerWakeListener::connect(&self.url).await.expect("listen");
        owner.listen(partition).await.expect("listen");
        let other_partition =
            DecisionPartition::new((partition.get() + 1) % decision_owner::DECISION_PARTITIONS)
                .expect("in range");
        let mut other = OwnerWakeListener::connect(&self.url).await.expect("listen");
        other.listen(other_partition).await.expect("listen");
        (owner, other)
    }
}

/// Wait for the wake of `exec` on `owner`. Returns its evict flag.
async fn wake_of(owner: &mut OwnerWakeListener, exec: ExecutionId) -> bool {
    let deadline = tokio::time::Instant::now() + WAKE_WAIT;
    let mut evict = None;
    // Several writes in one test can each wake. Gather the wakes of `exec`
    // until the stream is quiet.
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let wait = if evict.is_some() { QUIET_WAIT } else { left };
        match owner.wait(wait).await {
            OwnerWakeWait::Woken(wake) if wake.payload.workflow_exec_id == exec.as_uuid() => {
                evict = Some(evict.unwrap_or(false) || wake.payload.evict);
            }
            OwnerWakeWait::Woken(_) => {}
            OwnerWakeWait::TimedOut | OwnerWakeWait::ChannelClosed => break,
        }
    }
    evict.unwrap_or_else(|| panic!("no owner wake for {exec}"))
}

/// The other owner got no wake for `exec`.
async fn assert_quiet(other: &mut OwnerWakeListener, exec: ExecutionId) {
    loop {
        match other.wait(QUIET_WAIT).await {
            OwnerWakeWait::Woken(wake) => assert_ne!(
                wake.payload.workflow_exec_id,
                exec.as_uuid(),
                "a non-owner got the wake"
            ),
            OwnerWakeWait::TimedOut | OwnerWakeWait::ChannelClosed => return,
        }
    }
}

/// Run `write` as a foreign writer and check the wake of `exec`.
async fn assert_wakes(
    owner: &mut OwnerWakeListener,
    other: &mut OwnerWakeListener,
    exec: ExecutionId,
    evict: bool,
) {
    assert_eq!(
        wake_of(owner, exec).await,
        evict,
        "the evict flag of the wake"
    );
    assert_quiet(other, exec).await;
}

async fn drain(listener: &mut OwnerWakeListener) {
    while let OwnerWakeWait::Woken(_) = listener.wait(Duration::from_millis(200)).await {}
}

// ---------------------------------------------------------------------------
// The writer classes
// ---------------------------------------------------------------------------

/// A start writes the first history of a run. It wakes the owner.
#[tokio::test]
async fn a_start_wakes_the_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let workflow_id = unique("owner-wake-db");
    let partition = DecisionPartition::for_workflow_id(&workflow_id);
    let (mut owner, mut other) = fx.listeners(partition).await;
    let client = WorkflowHandleClient::single(fx.pool.clone(), fx.url.clone());
    let opts = TypedStartOptions {
        queue_name: Some(fx.queue.clone()),
        ..Default::default()
    };
    let exec = OwnerWakeDbFlowStub::start_with_options(
        &mut fx.conn,
        &client,
        &workflow_id,
        "x".to_string(),
        opts,
    )
    .await
    .expect("start")
    .exec_id();
    assert_wakes(&mut owner, &mut other, exec, false).await;
}

/// A signal stages a row and re-pends the task. It wakes the owner.
#[tokio::test]
async fn a_signal_wakes_the_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    autumn_harvest::signal::send_signal(&mut fx.conn, exec, "go", Value::Null)
        .await
        .expect("signal");
    assert_wakes(&mut owner, &mut other, exec, false).await;
}

/// A cancel of a run with no open block writes its terminal. It evicts.
#[tokio::test]
async fn a_cancel_evicts() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    autumn_harvest::execution::cancel_workflow_execution(&mut fx.conn, exec, "stop", &NoOpMetrics)
        .await
        .expect("cancel");
    assert_wakes(&mut owner, &mut other, exec, true).await;
}

/// A pause and a resume change the run state. Each one evicts.
#[tokio::test]
async fn a_pause_and_a_resume_evict() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    autumn_harvest::execution::pause_workflow_execution(
        &mut fx.conn,
        exec,
        None,
        "oncall",
        &NoOpMetrics,
    )
    .await
    .expect("pause");
    assert_wakes(&mut owner, &mut other, exec, true).await;
    autumn_harvest::execution::resume_workflow_execution(
        &mut fx.conn,
        exec,
        "oncall",
        &NoOpMetrics,
    )
    .await
    .expect("resume");
    assert_wakes(&mut owner, &mut other, exec, true).await;
}

/// A terminate ends the run. It evicts.
#[tokio::test]
async fn a_terminate_evicts() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    autumn_harvest::execution::terminate_workflow_execution(
        &mut fx.conn,
        exec,
        "stop",
        &NoOpMetrics,
    )
    .await
    .expect("terminate");
    assert_wakes(&mut owner, &mut other, exec, true).await;
}

/// A reset seals the source, which evicts. The fork keeps the workflow id,
/// so its start wakes the same owner.
#[tokio::test]
async fn a_reset_evicts_the_source_and_wakes_the_fork() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    let reset = autumn_harvest::reset::reset_workflow_execution(
        &mut fx.conn,
        exec,
        autumn_harvest::reset::WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "operator reset".to_string(),
            operator_id: "oncall".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
        },
        None,
    )
    .await
    .expect("reset");
    // One read gathers the wakes of both runs.
    let mut seen = std::collections::BTreeMap::new();
    let deadline = tokio::time::Instant::now() + WAKE_WAIT;
    while seen.len() < 2 && tokio::time::Instant::now() < deadline {
        if let OwnerWakeWait::Woken(wake) = owner.wait(Duration::from_millis(200)).await {
            let evict = seen.entry(wake.payload.workflow_exec_id).or_insert(false);
            *evict |= wake.payload.evict;
        }
    }
    assert_eq!(seen.get(&exec.as_uuid()), Some(&true), "the source evicts");
    assert_eq!(
        seen.get(&reset.new_exec_id.as_uuid()),
        Some(&false),
        "the fork wakes"
    );
    assert_quiet(&mut other, exec).await;
}

/// A child terminal is written into the parent's history. It wakes the
/// owner of the parent's partition, not the child's.
#[tokio::test]
async fn a_child_terminal_wakes_the_parent_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let (parent, parent_partition) = fx.start().await;
    let (child, _child_partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(parent_partition).await;
    drain(&mut owner).await;
    let mut conn = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    conn.transaction::<_, autumn_harvest::error::HarvestError, _>(async |conn| {
        autumn_harvest::worker::wake_parent_for_child_completion(
            conn,
            parent,
            child,
            Value::String("done".into()),
        )
        .await
    })
    .await
    .expect("child terminal");
    assert_wakes(&mut owner, &mut other, parent, false).await;
}

/// A pinned decision of the child's owner writes into the parent. The wake
/// goes to the parent's owner. A plain write to the pinned partition sends
/// no wake: its writer is the owner.
#[tokio::test]
async fn a_pinned_writer_wakes_other_partitions_only() {
    let (mut fx, _container) = Fixture::new().await;
    let (parent, parent_partition) = fx.start().await;
    let (child, child_partition) = loop {
        let (child, p) = fx.start().await;
        if p != parent_partition {
            break (child, p);
        }
    };
    let (mut parent_owner, _) = fx.listeners(parent_partition).await;
    let (mut child_owner, _) = fx.listeners(child_partition).await;
    drain(&mut parent_owner).await;
    drain(&mut child_owner).await;

    // A tenure of the child's partition, pinned as a decision pins it.
    let shard = decision_owner::OwnerKey::pool_shard(None);
    let mut admin = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
             WHERE partition = $1",
        )
        .bind::<diesel::sql_types::Integer, _>(i32::from(child_partition.get())),
        &mut admin,
    )
    .await
    .expect("free the row");
    let holder = decision_owner::OwnerHolder::for_worker("child-owner");
    let decision_owner::Acquisition::Acquired(tenure) = decision_owner::acquire(
        &mut admin,
        shard,
        &holder,
        child_partition,
        &decision_owner::OwnerLeaseConfig::default(),
    )
    .await
    .expect("acquire") else {
        panic!("the free row is acquired");
    };

    let url = fx.url.clone();
    decision_owner::scope_decision(async move {
        assert!(decision_owner::pin_decision(Some(tenure)), "pinned");
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.transaction::<_, autumn_harvest::error::HarvestError, _>(async |conn| {
            autumn_harvest::store::append_single_event(
                conn,
                child,
                autumn_harvest::event::WorkflowEvent::MarkerRecorded {
                    name: "owner-wake".into(),
                    details: Value::Null,
                },
            )
            .await?;
            autumn_harvest::worker::wake_parent_for_child_completion(
                conn,
                parent,
                child,
                Value::String("done".into()),
            )
            .await
        })
        .await
        .expect("pinned write");
    })
    .await;

    assert!(
        !wake_of(&mut parent_owner, parent).await,
        "the parent wakes"
    );
    assert_quiet(&mut child_owner, child).await;
}

/// Run a SQL statement on its own connection, as a test fixture.
async fn fixture_sql(fx: &Fixture, sql: &str, exec: ExecutionId) {
    let mut conn = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(sql).bind::<diesel::sql_types::Uuid, _>(exec.as_uuid()),
        &mut conn,
    )
    .await
    .expect("fixture statement");
}

/// An external task result appends to the run and re-pends its task. It
/// wakes the owner.
#[tokio::test]
async fn an_external_task_result_wakes_the_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let token = autumn_harvest::types::ExternalActivityToken::new();
    let mut conn = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(
            "INSERT INTO harvest_external_tasks \
             (id, token, workflow_exec_id, activity_id, name, queue, state, \
              schedule_to_close_at, schedule_to_close_secs, created_at, updated_at) \
             VALUES (gen_random_uuid(), $1, $2, gen_random_uuid(), 'ext', 'q', 'PENDING', \
                     NOW() + interval '1 hour', 3600, NOW(), NOW())",
        )
        .bind::<diesel::sql_types::Uuid, _>(token.as_uuid())
        .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid()),
        &mut conn,
    )
    .await
    .expect("seed the external task");
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    assert!(
        autumn_harvest::external_task::complete_externally(
            &mut fx.conn,
            token,
            Value::String("done".into())
        )
        .await
        .expect("complete"),
        "the external task completes"
    );
    assert_wakes(&mut owner, &mut other, exec, false).await;
}

/// An update admission appends `UpdateAdmitted`, and its caller re-pends the
/// task. It wakes the owner. The test takes the edge path of each caller, as
/// `declarative_update_tests` does.
#[tokio::test]
async fn an_update_admission_wakes_the_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    autumn_harvest::store::admit_update_event(
        &mut fx.conn,
        exec,
        autumn_harvest::types::UpdateId::new(),
        "bump".to_string(),
        Value::Null,
        None,
    )
    .await
    .expect("admit");
    // Each admission caller wakes the task in its own call.
    autumn_harvest::queue::wake_workflow_task(&mut fx.conn, exec)
        .await
        .expect("wake");
    assert_wakes(&mut owner, &mut other, exec, false).await;
}

/// A redrive reactivates a failed run. It evicts.
#[tokio::test]
async fn a_reactivate_evicts() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    fixture_sql(
        &fx,
        "UPDATE harvest_workflow_executions SET state = 'FAILED' WHERE id = $1",
        exec,
    )
    .await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    autumn_harvest::execution::reactivate_failed_execution(
        &mut fx.conn,
        exec,
        uuid::Uuid::new_v4(),
        Some("redrive"),
    )
    .await
    .expect("reactivate");
    assert_wakes(&mut owner, &mut other, exec, true).await;
}

/// A fork starts a new run in its own partition. It wakes the owner of that
/// partition, not the owner of the source.
#[tokio::test]
async fn a_fork_wakes_the_owner_of_the_fork() {
    let (mut fx, _container) = Fixture::new().await;
    let (source, source_partition) = fx.start().await;
    let fork_id = loop {
        let id = unique("owner-wake-fork");
        if DecisionPartition::for_workflow_id(&id) != source_partition {
            break id;
        }
    };
    let (mut owner, mut other) = fx
        .listeners(DecisionPartition::for_workflow_id(&fork_id))
        .await;
    let (mut source_owner, _) = fx.listeners(source_partition).await;
    drain(&mut source_owner).await;
    let fork = autumn_harvest::fork::fork_workflow_execution(
        &mut fx.conn,
        source,
        autumn_harvest::fork::WorkflowForkRequest {
            workflow_id: Some(fork_id),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("fork");
    assert_wakes(&mut owner, &mut other, fork.new_exec_id, false).await;
    assert_quiet(&mut source_owner, source).await;
}

/// The poison-pill sweep re-pends an orphan of a dead worker. It wakes the
/// owner, because the dead worker can be the old owner.
#[tokio::test]
async fn a_poison_pill_requeue_wakes_the_owner() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    fixture_sql(
        &fx,
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'owner-wake-dead', \
         started_at = NOW() - interval '1 hour', attempt = 1 \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
        exec,
    )
    .await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut fx.conn,
        3,
        0,
        None,
        &NoOpMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");
    assert!(summary.requeued >= 1, "the orphan is re-queued");
    assert_wakes(&mut owner, &mut other, exec, false).await;
}

/// The poison-pill sweep quarantines a task at its strike limit and fails
/// the run. It evicts.
#[tokio::test]
async fn a_poison_pill_quarantine_evicts() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    fixture_sql(
        &fx,
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'owner-wake-dead', \
         started_at = NOW() - interval '1 hour', attempt = 1, crash_strikes = 2 \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
        exec,
    )
    .await;
    let (mut owner, mut other) = fx.listeners(partition).await;
    drain(&mut owner).await;
    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut fx.conn,
        3,
        0,
        None,
        &NoOpMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("reclaim");
    assert!(summary.quarantined >= 1, "the task is quarantined");
    assert_wakes(&mut owner, &mut other, exec, true).await;
}

/// An `UNLISTEN` stops the wakes of a partition. The owner wake task uses it
/// when a tenure ends.
#[tokio::test]
async fn an_unlisten_stops_the_wakes_of_a_partition() {
    let (mut fx, _container) = Fixture::new().await;
    let (exec, partition) = fx.start().await;
    let (mut owner, _) = fx.listeners(partition).await;
    drain(&mut owner).await;
    owner.unlisten(partition).await.expect("unlisten");
    assert!(owner.partitions().is_empty());
    autumn_harvest::signal::send_signal(&mut fx.conn, exec, "go", Value::Null)
        .await
        .expect("signal");
    assert_quiet(&mut owner, exec).await;
}
