//! The owner lease and the decision commit fence on Postgres (issue #2251).
//!
//! ADR 0009 gives the rules. These tests prove them on a real database:
//!
//! * Each acquisition adds 1 to the epoch, also for the same holder.
//! * A renewal changes the lease end only. It never changes the epoch.
//! * A stale owner's commit fails with `OwnershipLost` after a takeover.
//! * A fenced commit leaves no event row and no other write.
//! * A write outside a decision takes no owner row.
//! * A statement on many owner rows locks them in ascending order.
//! * A decision takes the owner row before the execution row.
//!
//! Each test uses its own partitions, so the tests can share one database.
#![cfg(feature = "db")]
#![allow(clippy::items_after_statements)]

use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DECISION_PARTITIONS, DecisionPartition, LeaseWrite, OwnerEpoch, OwnerHolder,
    OwnerLeaseConfig, OwnerTenure,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::integration_e2e::setup_test_database_url_or_env;

/// The shard of every test execution.
const SHARD: ShardId = ShardId::new(0);

/// A short lock wait, so a test that must not wait fails fast.
const SHORT_WAIT: Duration = Duration::from_millis(200);

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("the test database must be reachable")
}

const fn partition(n: u16) -> DecisionPartition {
    DecisionPartition::new(n).expect("a test partition is in range")
}

fn holder(tag: &str) -> OwnerHolder {
    OwnerHolder::for_worker(&format!("owner-test-{tag}"))
}

fn config() -> OwnerLeaseConfig {
    OwnerLeaseConfig::default().with_lock_timeout(SHORT_WAIT)
}

#[derive(QueryableByName)]
struct OwnerRow {
    #[diesel(sql_type = Nullable<Text>)]
    holder: Option<String>,
    #[diesel(sql_type = BigInt)]
    epoch: i64,
    #[diesel(sql_type = Bool)]
    live: bool,
}

/// The owner row of `p`, read with no lock.
async fn row(conn: &mut AsyncPgConnection, p: DecisionPartition) -> OwnerRow {
    diesel::sql_query(
        "SELECT holder, epoch, lease_until > clock_timestamp() AS live \
         FROM harvest_decision_owner WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(p.get()))
    .get_result(conn)
    .await
    .expect("the migration seeds every owner row")
}

/// Free the row of `p` and keep its epoch. A test fixture, not engine code.
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

/// End the lease of `p` now, as the clock does. The holder and epoch stay.
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

async fn acquire(
    conn: &mut AsyncPgConnection,
    h: &OwnerHolder,
    p: DecisionPartition,
) -> OwnerTenure {
    match decision_owner::acquire(conn, SHARD, h, p, &config())
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
         VALUES ($1, 'owner_test', $2, gen_random_uuid(), 0, '{}'::jsonb, 'owner-test', \
                 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(format!("owner-test-{}", uuid::Uuid::new_v4()))
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
        .expect("count events")
        .n
}

/// Connect as `app`, so [`wait_for_lock_wait`] can find the session.
async fn connect_as(url: &str, app: &str) -> AsyncPgConnection {
    let mut conn = connect(url).await;
    conn.batch_execute(&format!("SET application_name = '{app}'"))
        .await
        .expect("name the session");
    conn
}

#[derive(QueryableByName)]
struct Waiting {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// Wait until the session named `app` waits for a lock.
async fn wait_for_lock_wait(url: &str, app: &str) {
    let mut conn = connect(url).await;
    for _ in 0..200 {
        let waiting: Waiting = diesel::sql_query(
            "SELECT count(*) AS n FROM pg_stat_activity \
             WHERE application_name = $1 AND wait_event_type = 'Lock'",
        )
        .bind::<Text, _>(app)
        .get_result(&mut conn)
        .await
        .expect("read pg_stat_activity");
        if waiting.n > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{app} never waited for a lock");
}

/// The lease end of `p`, in seconds since the epoch.
async fn lease_end(conn: &mut AsyncPgConnection, p: DecisionPartition) -> f64 {
    #[derive(QueryableByName)]
    struct End {
        #[diesel(sql_type = diesel::sql_types::Double)]
        t: f64,
    }
    diesel::sql_query(
        "SELECT extract(epoch FROM lease_until)::float8 AS t \
         FROM harvest_decision_owner WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(p.get()))
    .get_result::<End>(conn)
    .await
    .expect("read the lease end")
    .t
}

fn signal(n: u32) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: "owner-test".to_string(),
        payload: serde_json::json!(n),
    }
}

/// Run `$body` as a decision of `$tenure`, in one transaction. The
/// transaction starts with the decision fence, as the worker's does.
///
/// A macro keeps the closure type concrete, so a spawned decision is `Send`.
macro_rules! decide {
    ($conn:expr, $tenure:expr, $body:expr) => {
        decision_owner::scope_decision(async {
            let _ = decision_owner::pin_decision($tenure);
            $conn
                .transaction::<_, HarvestError, _>(async |conn| {
                    autumn_harvest::replication::assert_decision_fence(conn).await?;
                    ($body)(conn).await
                })
                .await
        })
    };
}

#[tokio::test]
async fn the_migration_seeds_every_partition() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    #[derive(QueryableByName)]
    struct Seeded {
        #[diesel(sql_type = BigInt)]
        n: i64,
        #[diesel(sql_type = Integer)]
        lo: i32,
        #[diesel(sql_type = Integer)]
        hi: i32,
    }
    let seeded: Seeded = diesel::sql_query(
        "SELECT count(*) AS n, min(partition) AS lo, max(partition) AS hi \
         FROM harvest_decision_owner",
    )
    .get_result(&mut conn)
    .await
    .expect("read the owner rows");
    assert_eq!(seeded.n, i64::from(DECISION_PARTITIONS));
    assert_eq!(seeded.lo, 0);
    assert_eq!(seeded.hi, i32::from(DECISION_PARTITIONS) - 1);
}

/// Red test 3 of the issue: an acquisition bumps the epoch.
#[tokio::test]
async fn an_acquisition_bumps_the_epoch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(401);
    free(&mut conn, p).await;
    let before = row(&mut conn, p).await.epoch;

    let a = holder("bump-a");
    let tenure = acquire(&mut conn, &a, p).await;
    assert_eq!(tenure.epoch(), OwnerEpoch::new(before + 1));
    assert_eq!(tenure.partition(), p);
    let after = row(&mut conn, p).await;
    assert_eq!(after.epoch, before + 1);
    assert_eq!(after.holder.as_deref(), Some(a.as_str()));
    assert!(after.live, "a new lease is live");
}

/// ADR 0009 decision 5: a holder that takes back its own lapsed row gets a
/// new epoch. The scanner lease keeps the epoch here. The owner lease must not.
#[tokio::test]
async fn the_same_holder_gets_a_new_epoch_on_each_acquisition() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(402);
    free(&mut conn, p).await;
    let a = holder("again");

    let first = acquire(&mut conn, &a, p).await;
    lapse(&mut conn, p).await;
    let second = acquire(&mut conn, &a, p).await;
    assert_eq!(second.epoch(), first.epoch().next().expect("no overflow"));

    // A release keeps the epoch. The next acquisition still adds 1.
    let released =
        decision_owner::release_all(&mut conn, SHARD, std::slice::from_ref(&second), SHORT_WAIT)
            .await
            .expect("release");
    assert_eq!(released, LeaseWrite::done(vec![p], vec![]));
    let freed = row(&mut conn, p).await;
    assert_eq!(freed.epoch, second.epoch().get());
    assert_eq!(
        freed.holder.as_deref(),
        Some(a.as_str()),
        "a release keeps the holder"
    );
    assert!(!freed.live, "a release ends the lease");
    let third = acquire(&mut conn, &a, p).await;
    assert_eq!(third.epoch(), second.epoch().next().expect("no overflow"));
}

#[tokio::test]
async fn a_live_lease_is_held_against_another_holder() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(403);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("live-a"), p).await;

    let other = decision_owner::acquire(&mut conn, SHARD, &holder("live-b"), p, &config())
        .await
        .expect("acquire");
    assert_eq!(other, Acquisition::Held);
    assert_eq!(row(&mut conn, p).await.epoch, tenure.epoch().get());
}

/// Red test 2 of the issue: a renewal does not bump the epoch. It moves the
/// lease end.
#[tokio::test]
async fn a_renewal_keeps_the_epoch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(404);
    let q = partition(405);
    free(&mut conn, p).await;
    free(&mut conn, q).await;
    let a = holder("renew");
    let tp = acquire(&mut conn, &a, p).await;
    let tq = acquire(&mut conn, &a, q).await;

    for _ in 0..3 {
        let before = lease_end(&mut conn, p).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let renewed =
            decision_owner::renew_all(&mut conn, SHARD, &[tq.clone(), tp.clone()], SHORT_WAIT)
                .await
                .expect("renew");
        assert_eq!(renewed, LeaseWrite::done(vec![p, q], vec![]));
        assert!(
            lease_end(&mut conn, p).await > before,
            "the lease end moves"
        );
    }
    assert_eq!(row(&mut conn, p).await.epoch, tp.epoch().get());
    assert_eq!(row(&mut conn, q).await.epoch, tq.epoch().get());
    assert!(row(&mut conn, p).await.live);
}

/// ADR 0009 decision 5: a renewal never waits for a commit in flight. A
/// second session holds the engine fence of a live tenure.
#[tokio::test]
async fn a_renewal_does_not_wait_for_a_held_fence() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(417);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("renew-held"), p).await;

    let mut commit = connect(&url).await;
    commit.batch_execute("BEGIN").await.expect("begin");
    decision_owner::fence(&mut commit, &tenure)
        .await
        .expect("the owner holds its fence");
    let renewed =
        decision_owner::renew_all(&mut conn, SHARD, std::slice::from_ref(&tenure), SHORT_WAIT)
            .await
            .expect("renew");
    assert_eq!(renewed, LeaseWrite::done(vec![p], vec![]));
    commit.batch_execute("COMMIT").await.expect("commit");
}
/// ADR 0009 decision 5: a renewal requires a live lease. A lapsed holder
/// acquires again, at a new epoch.
#[tokio::test]
async fn a_renewal_of_a_lapsed_or_moved_lease_is_lost() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(406);
    let q = partition(407);
    free(&mut conn, p).await;
    free(&mut conn, q).await;
    let a = holder("lost-a");
    let tp = acquire(&mut conn, &a, p).await;
    let tq = acquire(&mut conn, &a, q).await;
    lapse(&mut conn, p).await;
    lapse(&mut conn, q).await;
    let _taken = acquire(&mut conn, &holder("lost-b"), q).await;

    let renewed = decision_owner::renew_all(&mut conn, SHARD, &[tp, tq], SHORT_WAIT)
        .await
        .expect("renew");
    assert_eq!(renewed, LeaseWrite::done(vec![], vec![p, q]));
    assert!(
        !row(&mut conn, p).await.live,
        "a lost renewal changes nothing"
    );
}

/// Red test 1 of the issue: a stale owner's commit fails after a takeover.
#[tokio::test]
async fn a_stale_owners_commit_is_rejected_after_a_takeover() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(408);
    free(&mut conn, p).await;
    let stale = acquire(&mut conn, &holder("stale-a"), p).await;
    lapse(&mut conn, p).await;
    let fresh = acquire(&mut conn, &holder("stale-b"), p).await;

    let error = decide!(
        conn,
        Some(stale.clone()),
        async |_conn: &mut AsyncPgConnection| Ok(())
    )
    .await
    .expect_err("the stale owner is fenced");
    match error {
        HarvestError::OwnershipLost {
            partition,
            pinned,
            current,
        } => {
            assert_eq!(partition, p);
            assert_eq!(pinned, stale.epoch());
            assert_eq!(current, Some(fresh.epoch()));
        }
        other => panic!("expected OwnershipLost, got {other:?}"),
    }
    assert!(
        error_is_ownership_lost(
            &decide!(conn, Some(stale), async |_conn: &mut AsyncPgConnection| Ok(
                ()
            ))
            .await
        ),
        "a second try at the stale epoch is fenced again"
    );
    decide!(conn, Some(fresh), async |_conn: &mut AsyncPgConnection| Ok(
        ()
    ))
    .await
    .expect("the new owner commits");
}

/// The fence compares the epoch: the same holder that took its lapsed row
/// back cannot commit at its old epoch.
#[tokio::test]
async fn the_same_holder_cannot_commit_at_its_old_epoch() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(418);
    free(&mut conn, p).await;
    let a = holder("same");
    let old = acquire(&mut conn, &a, p).await;
    lapse(&mut conn, p).await;
    let new = acquire(&mut conn, &a, p).await;

    let error = decide!(
        conn,
        Some(old.clone()),
        async |_conn: &mut AsyncPgConnection| Ok(())
    )
    .await
    .expect_err("the old tenure is fenced");
    assert!(
        matches!(
            error,
            HarvestError::OwnershipLost { pinned, current: Some(current), .. }
                if pinned == old.epoch() && current == new.epoch()
        ),
        "{error:?}"
    );
}

/// A release ends the lease and keeps holder and epoch, as a lapse does.
/// The released tenure still passes until another worker acquires the row.
/// Then the fence refuses it.
#[tokio::test]
async fn a_released_tenure_is_fenced_by_the_next_acquisition() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(419);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("released"), p).await;
    decision_owner::release_all(&mut conn, SHARD, std::slice::from_ref(&tenure), SHORT_WAIT)
        .await
        .expect("release");
    decide!(
        conn,
        Some(tenure.clone()),
        async |_conn: &mut AsyncPgConnection| Ok(())
    )
    .await
    .expect("no other owner exists yet");

    let next = acquire(&mut conn, &holder("next"), p).await;
    let error = decide!(
        conn,
        Some(tenure.clone()),
        async |_conn: &mut AsyncPgConnection| Ok(())
    )
    .await
    .expect_err("the released tenure is fenced");
    assert!(
        matches!(
            error,
            HarvestError::OwnershipLost { current: Some(current), .. } if current == next.epoch()
        ),
        "{error:?}"
    );
}

/// The fence waits at most 250 ms. A short hold, as a failed acquisition
/// recheck takes, lets a healthy owner pass. A long hold is a takeover in
/// flight, so the fence fails closed.
#[tokio::test]
async fn the_fence_waits_briefly_then_fails_closed() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(420);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("bounded"), p).await;

    for (hold_ms, passes) in [(60_u64, true), (2_000, false)] {
        let url2 = url.clone();
        let blocker = tokio::spawn(async move {
            let mut b = connect(&url2).await;
            b.batch_execute(&format!(
                "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} FOR UPDATE; \
                 SELECT pg_sleep({}); ROLLBACK",
                p.get(),
                f64::from(u32::try_from(hold_ms).expect("small")) / 1000.0
            ))
            .await
            .expect("hold the row");
        });
        // Wait until the blocker holds this row: a NOWAIT probe then fails.
        let mut probe = connect(&url).await;
        for _ in 0..400 {
            let held = probe
                .batch_execute(&format!(
                    "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} \
                     FOR KEY SHARE NOWAIT; ROLLBACK",
                    p.get()
                ))
                .await
                .is_err();
            if held {
                let _ = probe.batch_execute("ROLLBACK").await;
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let result = decide!(
            conn,
            Some(tenure.clone()),
            async |_conn: &mut AsyncPgConnection| Ok(())
        )
        .await;
        assert_eq!(result.is_ok(), passes, "a hold of {hold_ms} ms: {result:?}");
        if !passes {
            assert!(
                matches!(
                    result,
                    Err(HarvestError::OwnershipLost { current: None, .. })
                ),
                "a lock miss fails closed: {result:?}"
            );
        }
        blocker.await.expect("join");
    }
}

fn error_is_ownership_lost<T>(result: &Result<T, HarvestError>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|e| e.ownership_lost().is_some())
}

/// Red test 4 of the issue: a fenced commit leaves no partial rows.
#[tokio::test]
async fn a_fenced_commit_leaves_no_event_rows() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(409);
    free(&mut conn, p).await;
    let exec_id = seed_execution(&mut conn).await;
    let owner = acquire(&mut conn, &holder("partial-a"), p).await;

    // The owner commits two events.
    decide!(
        conn,
        Some(owner.clone()),
        async |conn: &mut AsyncPgConnection| {
            let next = store::next_event_id_for(conn, exec_id).await?;
            store::append_events(conn, exec_id, &[signal(0), signal(1)], next).await
        }
    )
    .await
    .expect("the owner commits");
    assert_eq!(event_count(&mut conn, exec_id).await, 2);

    // A takeover. The old owner's next decision skips the start fence. The
    // late fence in the event insert stops it, and the whole transaction
    // rolls back.
    lapse(&mut conn, p).await;
    let _new = acquire(&mut conn, &holder("partial-b"), p).await;
    let result = decision_owner::scope_decision(async {
        let _ = decision_owner::pin_decision(Some(owner.clone()));
        conn.transaction::<(), HarvestError, _>(async |conn| {
            // No explicit fence at the start: the late fence must still hold.
            let next = store::next_event_id_for(conn, exec_id).await?;
            diesel::sql_query(
                "UPDATE harvest_workflow_executions SET memo = '{\"stale\": true}'::jsonb \
                 WHERE id = $1",
            )
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(autumn_harvest::error::database_error)?;
            store::append_events(conn, exec_id, &[signal(2), signal(3)], next).await?;
            Ok(())
        })
        .await
    })
    .await;
    assert!(error_is_ownership_lost(&result), "{result:?}");
    assert_eq!(
        event_count(&mut conn, exec_id).await,
        2,
        "no event row lands"
    );

    #[derive(QueryableByName)]
    struct Memo {
        #[diesel(sql_type = Bool)]
        stale: bool,
    }
    let memo: Memo = diesel::sql_query(
        "SELECT memo IS NOT NULL AND memo ? 'stale' AS stale \
         FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("read the memo");
    assert!(
        !memo.stale,
        "the earlier write of the transaction rolls back too"
    );
}

/// ADR 0009 decision 6: a foreign writer takes no owner row. A write with no
/// pinned tenure holds no lock on the owner table.
#[tokio::test]
async fn a_write_outside_a_decision_takes_no_owner_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let exec_id = seed_execution(&mut conn).await;
    let result = conn
        .transaction::<i64, HarvestError, _>(async |conn| {
            store::append_single_event(conn, exec_id, signal(9)).await?;
            let held: Count = diesel::sql_query(
                "SELECT count(*) AS n FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
                 WHERE c.relname = 'harvest_decision_owner' AND l.pid = pg_backend_pid()",
            )
            .get_result(conn)
            .await
            .map_err(autumn_harvest::error::database_error)?;
            Ok(held.n)
        })
        .await
        .expect("a foreign append");
    assert_eq!(result, 0, "the foreign append locks no owner row");
    assert_eq!(event_count(&mut conn, exec_id).await, 1);
}
/// The fence sets an idle timeout of the TTL, so a frozen commit cannot
/// hold the row for ever (ADR 0009 decision 8).
#[tokio::test]
async fn the_fence_bounds_an_idle_commit_by_the_ttl() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(410);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("idle"), p).await;

    #[derive(QueryableByName)]
    struct Setting {
        #[diesel(sql_type = Text)]
        v: String,
    }
    let v = decide!(conn, Some(tenure), async |conn: &mut AsyncPgConnection| {
        diesel::sql_query("SELECT current_setting('idle_in_transaction_session_timeout') AS v")
            .get_result::<Setting>(conn)
            .await
            .map_err(autumn_harvest::error::database_error)
    })
    .await
    .expect("read the setting")
    .v;
    assert_eq!(v, "10s");
}

/// An offload upload inside a decision widens the idle bound, then
/// restores the TTL.
#[tokio::test]
async fn an_upload_widens_the_idle_bound_for_its_duration() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(421);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("upload"), p).await;

    #[derive(QueryableByName)]
    struct Setting {
        #[diesel(sql_type = Text)]
        v: String,
    }
    let read = "SELECT current_setting('idle_in_transaction_session_timeout') AS v";
    let seen = decide!(conn, Some(tenure), async |conn: &mut AsyncPgConnection| {
        let mut seen = Vec::new();
        for step in 0..3 {
            match step {
                1 => decision_owner::start_upload_allowance(conn).await?,
                2 => decision_owner::end_upload_allowance(conn).await?,
                _ => {}
            }
            let v: Setting = diesel::sql_query(read)
                .get_result(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            seen.push(v.v);
        }
        Ok(seen)
    })
    .await
    .expect("decide");
    assert_eq!(seen, ["10s", "5min", "10s"]);
}

/// ADR 0009 decision 6: a statement on many owner rows locks them in
/// ascending partition order, whatever order its input has.
///
/// A second session holds one row. While the renewal waits for it, a
/// `NOWAIT` probe shows which other row the renewal holds already.
#[tokio::test]
async fn a_multi_row_renewal_locks_rows_in_ascending_order() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let lo = partition(411);
    let hi = partition(412);
    free(&mut conn, lo).await;
    free(&mut conn, hi).await;
    let a = holder("order");
    let t_lo = acquire(&mut conn, &a, lo).await;
    let t_hi = acquire(&mut conn, &a, hi).await;

    for (held, other, other_locked) in [(hi, lo, true), (lo, hi, false)] {
        let mut blocker = connect(&url).await;
        blocker
            .batch_execute(&format!(
                "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} FOR UPDATE",
                held.get()
            ))
            .await
            .expect("hold one row");
        let url2 = url.clone();
        // The input lists the high row first.
        let tenures = vec![t_hi.clone(), t_lo.clone()];
        let renewal = tokio::spawn(async move {
            let mut c = connect_as(&url2, "owner-order-renewal").await;
            decision_owner::renew_all(&mut c, SHARD, &tenures, Duration::from_secs(5)).await
        });
        wait_for_lock_wait(&url, "owner-order-renewal").await;
        let mut probe = connect(&url).await;
        let probed = probe
            .batch_execute(&format!(
                "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} \
                 FOR UPDATE NOWAIT",
                other.get()
            ))
            .await;
        assert_eq!(
            probed.is_err(),
            other_locked,
            "while the renewal waits for {held}, it holds {other}: {other_locked}"
        );
        probe
            .batch_execute("ROLLBACK")
            .await
            .expect("end the probe");
        blocker.batch_execute("ROLLBACK").await.expect("release");
        let renewed = renewal.await.expect("join").expect("renew");
        assert_eq!(renewed, LeaseWrite::done(vec![lo, hi], vec![]));
    }
}

/// The lock-order table row of the owner row: a decision takes the owner
/// row before the execution row.
///
/// A second session holds the execution row. While the decision waits for
/// it, a probe shows that the decision holds the owner row already.
#[tokio::test]
async fn a_decision_takes_the_owner_row_before_the_execution_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(413);
    free(&mut conn, p).await;
    let exec_id = seed_execution(&mut conn).await;
    let tenure = acquire(&mut conn, &holder("lock-order"), p).await;

    let mut blocker = connect(&url).await;
    blocker
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM harvest_workflow_executions WHERE id = '{}' FOR UPDATE",
            exec_id.as_uuid()
        ))
        .await
        .expect("hold the execution row");
    let url2 = url.clone();
    let decision = tokio::spawn(async move {
        let mut c = connect_as(&url2, "owner-lock-order").await;
        decide!(c, Some(tenure), async |conn: &mut AsyncPgConnection| {
            store::next_event_id_for(conn, exec_id).await
        })
        .await
    });
    wait_for_lock_wait(&url, "owner-lock-order").await;
    let mut probe = connect(&url).await;
    let probed = probe
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} \
             FOR UPDATE NOWAIT",
            p.get()
        ))
        .await;
    assert!(
        probed.is_err(),
        "the decision holds the owner row while it waits for the execution row"
    );
    probe
        .batch_execute("ROLLBACK")
        .await
        .expect("end the probe");
    blocker.batch_execute("ROLLBACK").await.expect("release");
    assert_eq!(decision.await.expect("join").expect("decide"), 0);
}

/// An acquisition waits for a commit in flight, then sees that the lease is
/// gone or still live. A lock miss is `Busy`, never an error.
#[tokio::test]
async fn an_acquisition_that_meets_a_commit_lock_is_busy() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(414);
    free(&mut conn, p).await;
    let owner = acquire(&mut conn, &holder("busy-a"), p).await;
    lapse(&mut conn, p).await;

    let mut commit = connect(&url).await;
    commit
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = {} FOR KEY SHARE",
            p.get()
        ))
        .await
        .expect("hold the commit lock");
    let busy = decision_owner::acquire(&mut conn, SHARD, &holder("busy-b"), p, &config())
        .await
        .expect("acquire");
    assert_eq!(busy, Acquisition::Busy);
    assert_eq!(
        row(&mut conn, p).await.epoch,
        owner.epoch().get(),
        "nothing changed"
    );
    commit.batch_execute("ROLLBACK").await.expect("release");
}

/// The epoch guard: the epoch never falls and never jumps by more than 1.
#[tokio::test]
async fn the_epoch_guard_refuses_a_fall_and_a_jump() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(415);
    free(&mut conn, p).await;
    let _ = acquire(&mut conn, &holder("guard"), p).await;
    for sql in [
        "UPDATE harvest_decision_owner SET epoch = epoch - 1 WHERE partition = $1",
        "UPDATE harvest_decision_owner SET epoch = epoch + 2 WHERE partition = $1",
        "UPDATE harvest_decision_owner SET partition = 499 WHERE partition = $1",
        "UPDATE harvest_decision_owner SET holder = 'someone-else' WHERE partition = $1",
    ] {
        let result = diesel::sql_query(sql)
            .bind::<Integer, _>(i32::from(p.get()))
            .execute(&mut conn)
            .await;
        assert!(result.is_err(), "the guard refuses: {sql}");
    }
}

#[tokio::test]
async fn a_fence_of_a_missing_row_fails_closed() {
    assert!(DecisionPartition::new(DECISION_PARTITIONS).is_none());
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(416);
    free(&mut conn, p).await;
    let tenure = acquire(&mut conn, &holder("closed"), p).await;
    // Delete the row in a transaction that rolls back, and fence there.
    let result = conn
        .transaction::<(), HarvestError, _>(async |conn| {
            diesel::sql_query("DELETE FROM harvest_decision_owner WHERE partition = $1")
                .bind::<Integer, _>(i32::from(p.get()))
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            decision_owner::fence(conn, &tenure).await
        })
        .await;
    match result {
        Err(HarvestError::OwnershipLost { current: None, .. }) => {}
        other => panic!("a missing row fences: {other:?}"),
    }
}

/// The fence calls of this backend that are not yet flushed to the
/// cumulative statistics.
async fn fence_calls(conn: &mut AsyncPgConnection) -> Result<i64, HarvestError> {
    #[derive(QueryableByName)]
    struct Calls {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let calls: Calls = diesel::sql_query(
        "SELECT COALESCE(SUM(calls), 0)::bigint AS n FROM pg_stat_xact_user_functions \
         WHERE funcname = 'harvest_decision_owner_fence'",
    )
    .get_result(conn)
    .await
    .map_err(autumn_harvest::error::database_error)?;
    Ok(calls.n)
}

/// One decision transaction, shaped as the worker persist: a start fence,
/// then a nested write with its own fence and an event insert. `scoped`
/// marks the rest of the transaction as fenced (issue #2252). Returns the
/// fence calls that the transaction made.
async fn persist_shaped(
    conn: &mut AsyncPgConnection,
    owner: &OwnerTenure,
    exec_id: ExecutionId,
    n: u32,
    scoped: bool,
) -> Result<i64, HarvestError> {
    decision_owner::scope_decision(async {
        let _ = decision_owner::pin_decision(Some(owner.clone()));
        conn.transaction::<_, HarvestError, _>(async |conn| {
            // A backend can hold unflushed counts of an earlier transaction,
            // so count the difference.
            let before = fence_calls(conn).await?;
            autumn_harvest::replication::assert_decision_fence(conn).await?;
            let nested = async |conn: &mut AsyncPgConnection| {
                conn.transaction::<_, HarvestError, _>(async |conn| {
                    autumn_harvest::replication::assert_decision_fence(conn).await?;
                    let next = store::next_event_id_for(conn, exec_id).await?;
                    store::append_events(conn, exec_id, &[signal(n)], next).await
                })
                .await
            };
            if scoped {
                let id = decision_owner::conn_id(conn);
                decision_owner::fenced_scope(id, nested(conn)).await?;
            } else {
                nested(conn).await?;
            }
            Ok(fence_calls(conn).await? - before)
        })
        .await
    })
    .await
}

/// Inside one transaction, the first fence holds its row lock until the end.
/// So a scoped transaction fences once. A transaction with no scope still
/// fences each nested write and its event insert (issue #2252).
#[tokio::test]
async fn a_fenced_scope_sends_one_fence_per_transaction() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("SET track_functions = 'all'")
        .await
        .expect("count function calls");
    let p = partition(431);
    free(&mut conn, p).await;
    let exec_id = seed_execution(&mut conn).await;
    let owner = acquire(&mut conn, &holder("scoped"), p).await;

    let unscoped = persist_shaped(&mut conn, &owner, exec_id, 0, false)
        .await
        .expect("the owner commits");
    let scoped = persist_shaped(&mut conn, &owner, exec_id, 1, true)
        .await
        .expect("the owner commits");

    assert_eq!(
        unscoped, 3,
        "start, nested write and event insert each fence"
    );
    assert_eq!(scoped, 1, "the start fence covers the whole transaction");
    assert_eq!(event_count(&mut conn, exec_id).await, 2);
}

/// The scope ends with its transaction. A later transaction on the same
/// connection that skips the start fence still meets the late fence.
#[tokio::test]
async fn a_fenced_scope_ends_with_its_transaction() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let p = partition(432);
    free(&mut conn, p).await;
    let exec_id = seed_execution(&mut conn).await;
    let owner = acquire(&mut conn, &holder("scope-end-a"), p).await;
    persist_shaped(&mut conn, &owner, exec_id, 0, true)
        .await
        .expect("the owner commits");

    lapse(&mut conn, p).await;
    let _new = acquire(&mut conn, &holder("scope-end-b"), p).await;
    let result = decision_owner::scope_decision(async {
        let _ = decision_owner::pin_decision(Some(owner.clone()));
        conn.transaction::<(), HarvestError, _>(async |conn| {
            let next = store::next_event_id_for(conn, exec_id).await?;
            store::append_events(conn, exec_id, &[signal(1)], next).await?;
            Ok(())
        })
        .await
    })
    .await;
    assert!(error_is_ownership_lost(&result), "{result:?}");
    assert_eq!(event_count(&mut conn, exec_id).await, 1);
}
