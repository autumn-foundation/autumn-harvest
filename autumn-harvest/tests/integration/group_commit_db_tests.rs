//! Group commit of one decision partition on Postgres (issue #2256).
//!
//! `DESIGN-2256.md` section 1 gives the rules. These tests prove them on a
//! real database:
//!
//! * A failing decision rolls back alone. The rest of its batch commits.
//! * No result goes out before the batch commits.
//! * A stale batch commits nothing. A fresh decision behind it commits.
//! * The start fence holds the owner row until the batch commits.
//! * A lone decision runs with no savepoint.
//! * Two decisions of one execution never share a batch.
//! * A decision whose caller leaves before its batch starts never runs.
//! * A batch survives its leader's caller leaving.
//! * A fence error from a stage rolls the whole batch back.
//! * A failed pool checkout fails the batch.
//!
//! Each test uses its own partitions, so the tests can share one database.
#![cfg(feature = "db")]

use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnerHolder, OwnerLeaseConfig, OwnerTenure,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::group_commit::{self, GroupCommit, GroupCommitConfig, Stage};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use futures::FutureExt;
use tokio::sync::oneshot;

use crate::integration_e2e::setup_test_database_url_or_env;

/// The shard of every test execution.
const SHARD: ShardId = ShardId::new(0);

/// How long a test waits for a lane to reach a queue depth.
const DEPTH_WAIT: Duration = Duration::from_secs(10);

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("the test database must be reachable")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build the pool")
}

const fn partition(n: u16) -> DecisionPartition {
    DecisionPartition::new(n).expect("a test partition is in range")
}

fn holder(tag: &str) -> OwnerHolder {
    OwnerHolder::for_worker(&format!("group-commit-test-{tag}"))
}

/// A lane with one slot, so each decision after the first one queues.
const fn one_slot() -> GroupCommitConfig {
    GroupCommitConfig::new(1, 32).expect("a valid config")
}

/// Free the row of `p` and keep its epoch. A test fixture.
async fn free(conn: &mut AsyncPgConnection, p: DecisionPartition) {
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(p.get()))
    .execute(conn)
    .await
    .expect("free the owner row");
}

/// End the lease of `p` now. The holder and the epoch stay.
async fn lapse(conn: &mut AsyncPgConnection, p: DecisionPartition) {
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(p.get()))
    .execute(conn)
    .await
    .expect("lapse the owner row");
}

/// Free the row of `p`, then take it for `tag`.
async fn own(conn: &mut AsyncPgConnection, p: DecisionPartition, tag: &str) -> OwnerTenure {
    free(conn, p).await;
    acquire(conn, p, tag).await
}

async fn acquire(conn: &mut AsyncPgConnection, p: DecisionPartition, tag: &str) -> OwnerTenure {
    match decision_owner::acquire(conn, SHARD, &holder(tag), p, &OwnerLeaseConfig::default())
        .await
        .expect("acquire")
    {
        Acquisition::Acquired(tenure) => tenure,
        other => panic!("expected an acquisition of {p}, got {other:?}"),
    }
}

/// Insert a running execution with no events.
async fn seed_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(SHARD);
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'group_commit_test', $2, gen_random_uuid(), 0, '{}'::jsonb, \
                 'group-commit-test', 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(format!("group-commit-test-{}", uuid::Uuid::new_v4()))
    .execute(conn)
    .await
    .expect("seed an execution");
    exec_id
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    diesel::sql_query("SELECT count(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<Count>(conn)
        .await
        .expect("count the events")
        .n
}

fn marker(name: &str) -> WorkflowEvent {
    WorkflowEvent::MarkerRecorded {
        name: name.to_string(),
        details: serde_json::Value::Null,
    }
}

/// A decision that appends one event at the next event id.
fn append_one(exec_id: ExecutionId) -> Stage<()> {
    group_commit::stage(move |conn| {
        async move {
            let next = store::next_event_id_for(conn, exec_id).await?;
            store::append_events(conn, exec_id, &[marker("decided")], next).await?;
            Ok(())
        }
        .boxed()
    })
}

/// A decision that appends one event, then fails on a duplicate event id.
///
/// The first append is a partial write. Its savepoint must remove it.
fn append_then_conflict(exec_id: ExecutionId) -> Stage<()> {
    group_commit::stage(move |conn| {
        async move {
            let next = store::next_event_id_for(conn, exec_id).await?;
            store::append_events(conn, exec_id, &[marker("partial")], next).await?;
            store::append_events(conn, exec_id, &[marker("conflict")], next).await?;
            Ok(())
        }
        .boxed()
    })
}

/// A decision that tells `entered` that it runs, then waits for `release`.
fn gate(
    exec_id: ExecutionId,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) -> Stage<()> {
    group_commit::stage(move |conn| {
        async move {
            let next = store::next_event_id_for(conn, exec_id).await?;
            store::append_events(conn, exec_id, &[marker("gate")], next).await?;
            let _ = entered.send(());
            let _ = release.await;
            Ok(())
        }
        .boxed()
    })
}

/// Wait until `lanes` holds `depth` queued decisions of `tenure`.
async fn wait_for_depth<T: Send + 'static>(
    lanes: &GroupCommit<T>,
    tenure: &OwnerTenure,
    depth: usize,
) {
    tokio::time::timeout(DEPTH_WAIT, async {
        while lanes.queued(tenure) < depth {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the lane never reached a depth of {depth}"));
}

/// A blocker holds the only slot of a lane until the test releases it.
struct Blocker {
    release: oneshot::Sender<()>,
    done: tokio::task::JoinHandle<Result<(), HarvestError>>,
}

/// Start a decision that holds the only slot of `lanes`, and wait until it
/// runs. Each later decision of the lane then queues.
async fn block(
    lanes: &std::sync::Arc<GroupCommit<()>>,
    tenure: &OwnerTenure,
    exec_id: ExecutionId,
) -> Blocker {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let done = tokio::spawn({
        let lanes = std::sync::Arc::clone(lanes);
        let tenure = tenure.clone();
        async move {
            lanes
                .commit(tenure, exec_id, gate(exec_id, entered_tx, release_rx))
                .await
        }
    });
    entered_rx.await.expect("the blocker runs");
    Blocker { release, done }
}

impl Blocker {
    async fn finish(self) {
        let _ = self.release.send(());
        self.done
            .await
            .expect("the blocker task")
            .expect("the blocker commits");
    }
}

/// Queue `stage` on `lanes` in its own task.
fn submit(
    lanes: &std::sync::Arc<GroupCommit<()>>,
    tenure: &OwnerTenure,
    exec_id: ExecutionId,
    stage: Stage<()>,
) -> tokio::task::JoinHandle<Result<(), HarvestError>> {
    let lanes = std::sync::Arc::clone(lanes);
    let tenure = tenure.clone();
    tokio::spawn(async move { lanes.commit(tenure, exec_id, stage).await })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_decision_rolls_back_alone() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(470);
    let tenure = own(&mut conn, p, "isolate").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let ok_first = seed_execution(&mut conn).await;
    let failing = seed_execution(&mut conn).await;
    let ok_last = seed_execution(&mut conn).await;
    let first = submit(&lanes, &tenure, ok_first, append_one(ok_first));
    wait_for_depth(&lanes, &tenure, 1).await;
    let bad = submit(&lanes, &tenure, failing, append_then_conflict(failing));
    wait_for_depth(&lanes, &tenure, 2).await;
    let last = submit(&lanes, &tenure, ok_last, append_one(ok_last));
    wait_for_depth(&lanes, &tenure, 3).await;
    blocker.finish().await;

    first
        .await
        .expect("task")
        .expect("the first decision commits");
    let error = bad.await.expect("task").expect_err("the conflict fails");
    assert!(
        error.is_event_id_unique_violation(),
        "the failing decision keeps its own error: {error:?}"
    );
    last.await
        .expect("task")
        .expect("the last decision commits");

    assert_eq!(event_count(&mut conn, ok_first).await, 1);
    assert_eq!(
        event_count(&mut conn, failing).await,
        0,
        "the savepoint removed the partial write"
    );
    assert_eq!(event_count(&mut conn, ok_last).await, 1);
    let stats = lanes.stats();
    assert_eq!(stats.largest, 3, "the three decisions shared one batch");
    assert_eq!(stats.rolled_back, 1, "{stats:?}");
    assert_eq!(stats.failed_batches, 0, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_result_goes_out_before_the_batch_commits() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(471);
    let tenure = own(&mut conn, p, "late-ack").await;
    // A deferred unique key fails at COMMIT, after every stage passed.
    let probe = format!("group_commit_probe_{}", std::process::id());
    conn.batch_execute(&format!(
        "DROP TABLE IF EXISTS {probe}; \
         CREATE TABLE {probe} (k INT, CONSTRAINT {probe}_k UNIQUE (k) \
             DEFERRABLE INITIALLY DEFERRED)"
    ))
    .await
    .expect("create the probe table");
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let mut tasks = Vec::new();
    let mut execs = Vec::new();
    for _ in 0..2 {
        let exec_id = seed_execution(&mut conn).await;
        let insert = format!("INSERT INTO {probe} VALUES (1)");
        let stage = group_commit::stage(move |conn: &mut AsyncPgConnection| {
            async move {
                let next = store::next_event_id_for(conn, exec_id).await?;
                store::append_events(conn, exec_id, &[marker("decided")], next).await?;
                conn.batch_execute(&insert)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;
                Ok(())
            }
            .boxed()
        });
        let depth = tasks.len() + 1;
        tasks.push(submit(&lanes, &tenure, exec_id, stage));
        wait_for_depth(&lanes, &tenure, depth).await;
        execs.push(exec_id);
    }
    blocker.finish().await;

    for task in tasks {
        let error = task
            .await
            .expect("task")
            .expect_err("a passed stage gets no Ok when COMMIT fails");
        assert!(
            error.to_string().contains("group commit"),
            "the error names the batch: {error}"
        );
    }
    for exec_id in execs {
        assert_eq!(
            event_count(&mut conn, exec_id).await,
            0,
            "nothing committed"
        );
    }
    let stats = lanes.stats();
    assert_eq!(stats.failed_batches, 1, "{stats:?}");
    conn.batch_execute(&format!("DROP TABLE {probe}"))
        .await
        .expect("drop the probe table");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_batch_commits_nothing() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(472);
    let stale = own(&mut conn, p, "stale-old").await;
    lapse(&mut conn, p).await;
    let fresh = acquire(&mut conn, p, "stale-new").await;
    assert!(fresh.epoch() > stale.epoch());

    // The fresh owner holds the only slot. The stale decisions then queue
    // and form one batch.
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &fresh, blocker_exec).await;
    let mut tasks = Vec::new();
    let mut execs = Vec::new();
    let first = seed_execution(&mut conn).await;
    tasks.push(submit(&lanes, &stale, first, append_one(first)));
    wait_for_depth(&lanes, &stale, 1).await;
    execs.push(first);
    // A fresh decision between the stale ones waits for its own batch.
    let current = seed_execution(&mut conn).await;
    let fresh_task = submit(&lanes, &fresh, current, append_one(current));
    wait_for_depth(&lanes, &stale, 2).await;
    let last = seed_execution(&mut conn).await;
    tasks.push(submit(&lanes, &stale, last, append_one(last)));
    wait_for_depth(&lanes, &stale, 3).await;
    execs.push(last);
    blocker.finish().await;

    for task in tasks {
        let error = task.await.expect("task").expect_err("a stale batch fails");
        assert_eq!(
            error.ownership_lost(),
            Some((p, stale.epoch())),
            "each decision gets OwnershipLost: {error:?}"
        );
    }
    for exec_id in execs {
        assert_eq!(event_count(&mut conn, exec_id).await, 0);
    }
    fresh_task
        .await
        .expect("task")
        .expect("the fresh decision commits in its own batch");
    assert_eq!(event_count(&mut conn, current).await, 1);
    let stats = lanes.stats();
    assert_eq!(stats.largest, 2, "the stale decisions shared one batch");
    assert_eq!(
        stats.batches, 3,
        "the blocker, the stale batch, the fresh one"
    );
    assert_eq!(stats.failed_batches, 1, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fence_holds_the_owner_row_for_the_whole_batch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(473);
    let tenure = own(&mut conn, p, "hold").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    // The batch: a decision that commits, then one that waits mid-batch.
    let early = seed_execution(&mut conn).await;
    let first = submit(&lanes, &tenure, early, append_one(early));
    wait_for_depth(&lanes, &tenure, 1).await;
    let late = seed_execution(&mut conn).await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let second = submit(&lanes, &tenure, late, gate(late, entered_tx, release_rx));
    wait_for_depth(&lanes, &tenure, 2).await;
    blocker.finish().await;
    entered_rx.await.expect("the second decision runs");

    // The first decision passed its savepoint. Its fence lock still holds.
    let mut probe = connect(&url).await;
    probe
        .batch_execute("SET lock_timeout = '200ms'")
        .await
        .expect("set the probe lock timeout");
    let acquire_lock = format!(
        "SELECT epoch FROM harvest_decision_owner WHERE partition = {} FOR UPDATE",
        p.get()
    );
    let waited = probe
        .transaction::<_, diesel::result::Error, _>(async |c| c.batch_execute(&acquire_lock).await)
        .await;
    let waited = waited.expect_err("an acquisition must wait for the open batch");
    assert!(
        waited.to_string().contains("lock timeout"),
        "the acquisition lock timed out: {waited}"
    );
    let renew = format!(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() + interval '10 s' \
         WHERE partition = {}",
        p.get()
    );
    probe
        .batch_execute(&renew)
        .await
        .expect("a renewal does not wait for the batch");

    let _ = release_tx.send(());
    first
        .await
        .expect("task")
        .expect("the first decision commits");
    second
        .await
        .expect("task")
        .expect("the second decision commits");
    probe
        .transaction::<_, diesel::result::Error, _>(async |c| c.batch_execute(&acquire_lock).await)
        .await
        .expect("after the commit, the acquisition lock is free");
    assert_eq!(event_count(&mut conn, early).await, 1);
    assert_eq!(event_count(&mut conn, late).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_decision_runs_with_no_savepoint() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(474);
    let tenure = own(&mut conn, p, "lone").await;
    let lanes = GroupCommit::new(build_pool(&url), GroupCommitConfig::default());
    for _ in 0..5 {
        let exec_id = seed_execution(&mut conn).await;
        lanes
            .commit(tenure.clone(), exec_id, append_one(exec_id))
            .await
            .expect("a lone decision commits");
        assert_eq!(event_count(&mut conn, exec_id).await, 1);
    }
    let stats = lanes.stats();
    assert_eq!(stats.batches, 5, "{stats:?}");
    assert_eq!(stats.decisions, 5, "{stats:?}");
    assert_eq!(stats.grouped, 0, "no decision waited: {stats:?}");
    assert_eq!(stats.savepoints, 0, "a batch of one sends no savepoint");

    // A lone stale decision fails its fence and writes nothing.
    lapse(&mut conn, p).await;
    let _fresh = acquire(&mut conn, p, "lone-new").await;
    let exec_id = seed_execution(&mut conn).await;
    let error = lanes
        .commit(tenure.clone(), exec_id, append_one(exec_id))
        .await
        .expect_err("the stale decision fails");
    assert_eq!(error.ownership_lost(), Some((p, tenure.epoch())));
    assert_eq!(event_count(&mut conn, exec_id).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_execution_never_shares_a_batch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(475);
    let tenure = own(&mut conn, p, "one-exec").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let twice = seed_execution(&mut conn).await;
    let other = seed_execution(&mut conn).await;
    let a = submit(&lanes, &tenure, twice, append_one(twice));
    wait_for_depth(&lanes, &tenure, 1).await;
    let b = submit(&lanes, &tenure, twice, append_one(twice));
    wait_for_depth(&lanes, &tenure, 2).await;
    let c = submit(&lanes, &tenure, other, append_one(other));
    wait_for_depth(&lanes, &tenure, 3).await;
    blocker.finish().await;

    for task in [a, b, c] {
        task.await.expect("task").expect("each decision commits");
    }
    assert_eq!(
        event_count(&mut conn, twice).await,
        2,
        "the second decision read the first one's event"
    );
    assert_eq!(event_count(&mut conn, other).await, 1);
    let stats = lanes.stats();
    assert_eq!(
        stats.largest, 2,
        "one batch held one decision per execution"
    );
    assert_eq!(stats.batches, 3, "the blocker, then two batches: {stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_decision_whose_caller_leaves_before_its_batch_never_runs() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(476);
    let tenure = own(&mut conn, p, "leave").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let left = seed_execution(&mut conn).await;
    let task = submit(&lanes, &tenure, left, append_one(left));
    wait_for_depth(&lanes, &tenure, 1).await;
    task.abort();
    let _ = task.await;
    assert_eq!(lanes.queued(&tenure), 0, "the caller removed its entry");
    blocker.finish().await;

    let after = seed_execution(&mut conn).await;
    lanes
        .commit(tenure.clone(), after, append_one(after))
        .await
        .expect("the lane still works");
    assert_eq!(event_count(&mut conn, left).await, 0, "it never ran");
    assert_eq!(lanes.stats().decisions, 2, "the blocker and the last one");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_error_from_a_stage_rolls_the_batch_back() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(477);
    let tenure = own(&mut conn, p, "stage-fence").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let pinned = tenure.epoch();
    let before = seed_execution(&mut conn).await;
    let fenced = seed_execution(&mut conn).await;
    let after = seed_execution(&mut conn).await;
    let lost = group_commit::stage(move |conn: &mut AsyncPgConnection| {
        async move {
            let next = store::next_event_id_for(conn, fenced).await?;
            store::append_events(conn, fenced, &[marker("partial")], next).await?;
            Err(HarvestError::OwnershipLost {
                partition: p,
                pinned,
                current: None,
            })
        }
        .boxed()
    });
    let mut tasks = Vec::new();
    for (depth, (exec_id, stage)) in [
        (before, append_one(before)),
        (fenced, lost),
        (after, append_one(after)),
    ]
    .into_iter()
    .enumerate()
    {
        tasks.push(submit(&lanes, &tenure, exec_id, stage));
        wait_for_depth(&lanes, &tenure, depth + 1).await;
    }
    blocker.finish().await;

    for task in tasks {
        let error = task
            .await
            .expect("task")
            .expect_err("the batch rolled back");
        assert_eq!(
            error.ownership_lost(),
            Some((p, pinned)),
            "each decision keeps the fence variant: {error:?}"
        );
    }
    for exec_id in [before, fenced, after] {
        assert_eq!(
            event_count(&mut conn, exec_id).await,
            0,
            "nothing committed"
        );
    }
    assert_eq!(lanes.stats().failed_batches, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_survives_its_leader_caller_leaving() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(478);
    let tenure = own(&mut conn, p, "leader-leaves").await;
    let lanes = std::sync::Arc::new(GroupCommit::new(build_pool(&url), one_slot()));
    let blocker_exec = seed_execution(&mut conn).await;
    let blocker = block(&lanes, &tenure, blocker_exec).await;

    let leader = seed_execution(&mut conn).await;
    let leader_task = submit(&lanes, &tenure, leader, append_one(leader));
    wait_for_depth(&lanes, &tenure, 1).await;
    let follower = seed_execution(&mut conn).await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let follower_task = submit(
        &lanes,
        &tenure,
        follower,
        gate(follower, entered_tx, release_rx),
    );
    wait_for_depth(&lanes, &tenure, 2).await;
    blocker.finish().await;
    entered_rx.await.expect("the batch runs");

    // The caller of the first decision leaves while its batch runs.
    leader_task.abort();
    let _ = leader_task.await;
    let _ = release_tx.send(());
    follower_task
        .await
        .expect("task")
        .expect("the follower still gets its result");
    assert_eq!(
        event_count(&mut conn, follower).await,
        1,
        "the batch committed"
    );
    assert_eq!(
        event_count(&mut conn, leader).await,
        1,
        "a started batch is not cancelled"
    );
    assert_eq!(lanes.stats().largest, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_pool_checkout_fails_the_batch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(479);
    let tenure = own(&mut conn, p, "checkout").await;
    // No server listens on port 1, so each checkout fails.
    let lanes = GroupCommit::new(
        build_pool("postgres://postgres@127.0.0.1:1/none"),
        GroupCommitConfig::default(),
    );
    let exec_id = seed_execution(&mut conn).await;
    let error = lanes
        .commit(tenure, exec_id, append_one(exec_id))
        .await
        .expect_err("the checkout fails");
    assert!(
        error
            .to_string()
            .contains("group commit: the pool checkout failed"),
        "{error}"
    );
    assert_eq!(event_count(&mut conn, exec_id).await, 0);
    assert_eq!(lanes.stats().failed_batches, 1);
}
