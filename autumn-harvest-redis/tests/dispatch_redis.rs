//! Redis-backed tests for [`autumn_harvest_redis::RedisDispatch`] (issue #1312).
//!
//! The dispatch channel carries references to `harvest_task_queue` rows.
//! Postgres stays the source of truth, so every case here asserts channel
//! behaviour only: dedupe, reschedule, batch read, release, recovery.
//!
//! Set `HARVEST_REDIS_TEST_URL` to run against a live Redis. Without it the
//! helper starts a `testcontainers` Redis, and skips the case when Docker is
//! not available. Each fixture uses a unique key prefix, so cases that share
//! one Redis never collide.

use std::time::{Duration, Instant};

use autumn_harvest::dispatch::{DispatchHint, DispatchLease, TaskDispatch};
use autumn_harvest_redis::{
    RedisDispatch, RedisDispatchConfig, dispatch_delayed_key, dispatch_marker_key,
    dispatch_stream_key,
};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use uuid::Uuid;

/// A live Redis instance, a dispatch channel on it, and a raw connection.
///
/// The raw connection reads the key space directly, so a case can assert on
/// the stream, the pending entries list and the dedupe markers.
struct Fixture {
    _container: Option<testcontainers::ContainerAsync<Redis>>,
    dispatch: RedisDispatch,
    raw: redis::aio::ConnectionManager,
    prefix: String,
}

impl Fixture {
    fn stream_key(&self, queue: &str) -> String {
        dispatch_stream_key(&self.prefix, queue)
    }

    fn marker_key(&self, queue: &str, task_id: Uuid) -> String {
        dispatch_marker_key(&self.prefix, queue, &task_id.to_string())
    }

    async fn stream_len(&self, queue: &str) -> i64 {
        let mut conn = self.raw.clone();
        conn.xlen(self.stream_key(queue)).await.expect("xlen")
    }

    async fn marker_exists(&self, queue: &str, task_id: Uuid) -> bool {
        let mut conn = self.raw.clone();
        conn.exists(self.marker_key(queue, task_id))
            .await
            .expect("exists")
    }

    async fn pending_count(&self, queue: &str) -> usize {
        let mut conn = self.raw.clone();
        let reply: redis::streams::StreamPendingReply = conn
            .xpending(self.stream_key(queue), "harvest_workers")
            .await
            .expect("xpending");
        match reply {
            redis::streams::StreamPendingReply::Empty => 0,
            redis::streams::StreamPendingReply::Data(data) => data.count,
        }
    }

    async fn destroy_group(&self, queue: &str) {
        let mut conn = self.raw.clone();
        let _: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(self.stream_key(queue))
            .arg("harvest_workers")
            .query_async(&mut conn)
            .await
            .expect("xgroup destroy");
    }
}

/// Build a fixture, or return `None` so the case skips cleanly.
async fn try_start(visibility_timeout: Duration) -> Option<Fixture> {
    let prefix = format!("t_{}", Uuid::new_v4().simple());
    let config = RedisDispatchConfig {
        key_prefix: prefix.clone(),
        visibility_timeout,
        ..RedisDispatchConfig::default()
    };

    let (url, container) = if let Ok(url) = std::env::var("HARVEST_REDIS_TEST_URL") {
        (url, None)
    } else {
        let container = match Redis::default().start().await {
            Ok(container) => container,
            Err(err) => {
                eprintln!("skipping: docker unavailable: {err}");
                return None;
            }
        };
        let host = container.get_host().await.ok()?;
        let port = container.get_host_port_ipv4(REDIS_PORT).await.ok()?;
        (format!("redis://{host}:{port}"), Some(container))
    };

    let dispatch = match RedisDispatch::connect(&url, config).await {
        Ok(dispatch) => dispatch,
        Err(err) => {
            eprintln!("skipping: redis unreachable: {err}");
            return None;
        }
    };
    let client = redis::Client::open(url).ok()?;
    let raw = redis::aio::ConnectionManager::new(client).await.ok()?;
    Some(Fixture {
        _container: container,
        dispatch,
        raw,
        prefix,
    })
}

fn hint(queue: &str, task_id: Uuid, scheduled_at: DateTime<Utc>) -> DispatchHint {
    DispatchHint {
        task_id,
        queue_name: queue.to_string(),
        scheduled_at,
        priority: 0,
        shard: None,
        kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
    }
}

/// Read once with a short wait and return the leases.
async fn read(fixture: &Fixture, queues: &[String], max: usize) -> Vec<DispatchLease> {
    fixture
        .dispatch
        .next(queues, "consumer-1", max, Duration::from_millis(50))
        .await
        .expect("next")
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_then_next_round_trips_a_hint() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["default".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("default", task_id, Utc::now())])
        .await
        .expect("publish");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1, "one published hint must yield one lease");
    assert_eq!(leases[0].task_id, task_id);
    assert_eq!(leases[0].queue_name, "default");
    assert_eq!(leases[0].redeliveries, 0);
    assert!(
        !leases[0].handle.is_empty(),
        "handle is the stream entry id"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_publish_is_a_no_op() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["default".to_string()];
    let task_id = Uuid::new_v4();
    let hint = hint("default", task_id, Utc::now());

    let batch = std::slice::from_ref(&hint);
    fixture.dispatch.publish(batch).await.expect("one");
    fixture.dispatch.publish(batch).await.expect("two");
    fixture.dispatch.publish(batch).await.expect("three");

    assert_eq!(
        fixture.stream_len("default").await,
        1,
        "the dedupe marker must hold the stream at one entry"
    );
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_future_hint_is_invisible_until_due() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["delayed".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint(
            "delayed",
            task_id,
            Utc::now() + chrono::Duration::milliseconds(400),
        )])
        .await
        .expect("publish");

    assert!(
        read(&fixture, &queues, 10).await.is_empty(),
        "a hint that is not due must not be delivered"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1, "a due hint must be promoted and delivered");
    assert_eq!(leases[0].task_id, task_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_earlier_due_time_moves_a_delayed_entry_forward() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["timers".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint(
            "timers",
            task_id,
            Utc::now() + chrono::Duration::seconds(3600),
        )])
        .await
        .expect("park");
    assert!(read(&fixture, &queues, 10).await.is_empty());

    // A signal arrives for the parked row. The second hint is due now, so the
    // entry moves out of the delayed set and onto the stream.
    fixture
        .dispatch
        .publish(&[hint("timers", task_id, Utc::now())])
        .await
        .expect("move forward");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(
        leases.len(),
        1,
        "an earlier due time must override the park"
    );
    assert_eq!(leases[0].task_id, task_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_moves_a_held_entry_later() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["timers".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint(
            "timers",
            task_id,
            Utc::now() + chrono::Duration::milliseconds(300),
        )])
        .await
        .expect("park");
    // A retry moves the row's `scheduled_at` later. Contract C1 keys on that
    // value, so the parked reference moves with it.
    fixture
        .dispatch
        .publish(&[hint(
            "timers",
            task_id,
            Utc::now() + chrono::Duration::seconds(3600),
        )])
        .await
        .expect("later");

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        read(&fixture, &queues, 10).await.is_empty(),
        "the later due time must hold the entry back"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconcile_republish_leaves_a_backed_off_entry_alone() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["gated".to_string()];
    let task_id = Uuid::new_v4();
    // The row's due time. Every republish of this row carries it again.
    let scheduled_at = Utc::now();

    fixture
        .dispatch
        .publish(&[hint("gated", task_id, scheduled_at)])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    fixture
        .dispatch
        .release(&leases[0], Duration::from_millis(600))
        .await
        .expect("release");

    // The reconcile sweep republishes the same row with the same
    // `scheduled_at`. Contract C1 makes that a no-op.
    fixture
        .dispatch
        .publish(&[hint("gated", task_id, scheduled_at)])
        .await
        .expect("republish");
    assert!(
        read(&fixture, &queues, 10).await.is_empty(),
        "a republish must not cut the backoff short"
    );

    tokio::time::sleep(Duration::from_millis(700)).await;
    let again = read(&fixture, &queues, 10).await;
    assert_eq!(again.len(), 1, "the entry returns once the backoff elapses");
    assert_eq!(
        again[0].redeliveries, 1,
        "a republish must not reset the redelivery count"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wake_moves_a_backed_off_entry_forward() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["waking".to_string()];
    let task_id = Uuid::new_v4();
    let scheduled_at = Utc::now();

    fixture
        .dispatch
        .publish(&[hint("waking", task_id, scheduled_at)])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    fixture
        .dispatch
        .release(&leases[0], Duration::from_secs(3600))
        .await
        .expect("release");
    assert!(read(&fixture, &queues, 10).await.is_empty());

    // A signal moves the row's `scheduled_at`, so the reference moves too.
    fixture
        .dispatch
        .publish(&[hint("waking", task_id, Utc::now())])
        .await
        .expect("wake");

    let again = read(&fixture, &queues, 10).await;
    assert_eq!(again.len(), 1, "a new due time must cut the backoff short");
    assert_eq!(again[0].task_id, task_id);
    assert_eq!(
        again[0].redeliveries, 0,
        "a moved reference restarts its redelivery count"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_read_spans_two_queues() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["alpha".to_string(), "beta".to_string()];
    let alpha = Uuid::new_v4();
    let beta = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[
            hint("alpha", alpha, Utc::now()),
            hint("beta", beta, Utc::now()),
        ])
        .await
        .expect("publish");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 2, "one read must cover every served queue");
    let mut names: Vec<&str> = leases.iter().map(|l| l.queue_name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["alpha", "beta"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn release_with_delay_hides_the_entry_and_counts_a_redelivery() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["gated".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("gated", task_id, Utc::now())])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);

    fixture
        .dispatch
        .release(&leases[0], Duration::from_millis(400))
        .await
        .expect("release");
    assert!(
        read(&fixture, &queues, 10).await.is_empty(),
        "a released entry must stay hidden until its delay elapses"
    );
    assert_eq!(
        fixture.pending_count("gated").await,
        0,
        "release must clear the pending entries list"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    let again = read(&fixture, &queues, 10).await;
    assert_eq!(again.len(), 1, "the entry returns after the delay");
    assert_eq!(again[0].task_id, task_id);
    assert_eq!(again[0].redeliveries, 1, "release counts one redelivery");
}

/// One lease's delay overflowing `chrono::Duration` must not skip its
/// siblings' release in the same batch (Codex review, issue #1429
/// follow-up).
///
/// `release_many_inner` used to build every entry's due time with a `?` on
/// the `chrono::Duration::from_std` conversion. One out-of-range delay
/// then aborted the whole call before `requeue_batch` ever ran, and every
/// other, unrelated lease in the batch stayed pending until visibility
/// recovery. This drives a batch with one ordinary lease and one whose delay
/// `chrono::Duration` cannot represent, and asserts the ordinary lease
/// still gets released.
#[tokio::test(flavor = "multi_thread")]
async fn a_leases_invalid_release_delay_does_not_block_its_siblings() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["healthy".to_string(), "overflow".to_string()];
    let healthy_task = Uuid::new_v4();
    let overflow_task = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("healthy", healthy_task, Utc::now())])
        .await
        .expect("publish healthy");
    fixture
        .dispatch
        .publish(&[hint("overflow", overflow_task, Utc::now())])
        .await
        .expect("publish overflow");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 2);
    let healthy_lease = leases
        .iter()
        .find(|lease| lease.task_id == healthy_task)
        .cloned()
        .expect("healthy lease");
    let overflow_lease = leases
        .iter()
        .find(|lease| lease.task_id == overflow_task)
        .cloned()
        .expect("overflow lease");

    let result = fixture
        .dispatch
        .release_many(&[
            (healthy_lease, Duration::from_millis(50)),
            (overflow_lease, Duration::MAX),
        ])
        .await;
    assert!(
        result.is_err(),
        "an out-of-range delay must still surface as an error"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let again = read(&fixture, &["healthy".to_string()], 10).await;
    assert_eq!(
        again.len(),
        1,
        "the healthy lease's release must not be skipped by its sibling's bad delay"
    );
    assert_eq!(again[0].task_id, healthy_task);
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_deletes_the_marker_so_a_republish_is_delivered() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["acked".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("acked", task_id, Utc::now())])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    assert!(
        fixture.marker_exists("acked", task_id).await,
        "publish sets a marker"
    );

    fixture.dispatch.ack(&leases[0]).await.expect("ack");
    assert!(
        !fixture.marker_exists("acked", task_id).await,
        "ack must delete the marker"
    );
    assert_eq!(
        fixture.stream_len("acked").await,
        0,
        "ack deletes the entry"
    );
    assert_eq!(fixture.pending_count("acked").await, 0);

    fixture
        .dispatch
        .publish(&[hint("acked", task_id, Utc::now())])
        .await
        .expect("republish");
    let again = read(&fixture, &queues, 10).await;
    assert_eq!(again.len(), 1, "a republish after ack is delivered again");
    assert_eq!(again[0].task_id, task_id);
}

/// Acking a stale lease must not delete a marker a fresher entry now owns
/// (Codex review, issue #1429).
///
/// A batched worker defers a lease's ack until after its task has already
/// been spawned. A fast-completing task can re-pend and republish the same
/// task id before that stale lease is acked. That overwrites the marker to
/// name the fresh entry. Acking the stale lease afterward must not delete
/// that marker. Doing so would leave the fresh entry undetected by a later
/// publish's own intact check, producing a duplicate.
#[tokio::test(flavor = "multi_thread")]
async fn acking_a_stale_lease_leaves_a_fresher_entrys_marker_intact() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["acked".to_string()];
    let task_id = Uuid::new_v4();
    let stale_due = Utc::now();

    fixture
        .dispatch
        .publish(&[hint("acked", task_id, stale_due)])
        .await
        .expect("publish");
    let stale = read(&fixture, &queues, 10).await;
    assert_eq!(stale.len(), 1);

    // Simulates the fast-completing task's re-pend: republishing before the
    // stale lease above is acked, at a due time distinct from the first
    // publish. The old marker then reads as not intact, so this writes a
    // fresh entry and overwrites the marker to name it.
    let fresh_due = stale_due + chrono::Duration::milliseconds(1);
    fixture
        .dispatch
        .publish(&[hint("acked", task_id, fresh_due)])
        .await
        .expect("republish before the stale lease is acked");
    let fresh = read(&fixture, &queues, 10).await;
    assert_eq!(fresh.len(), 1, "the republish delivers a fresh entry");
    assert_ne!(
        fresh[0].handle, stale[0].handle,
        "the fresh lease must be a different stream entry from the stale one"
    );

    fixture
        .dispatch
        .ack(&stale[0])
        .await
        .expect("ack the stale lease");
    assert!(
        fixture.marker_exists("acked", task_id).await,
        "acking the stale lease must not delete the fresh entry's marker"
    );

    // A republish at the fresh entry's own due time now reads the marker as
    // intact and skips, proving it still guards against a duplicate.
    fixture
        .dispatch
        .publish(&[hint("acked", task_id, fresh_due)])
        .await
        .expect("publish at the fresh entry's due time");
    assert_eq!(
        fixture.stream_len("acked").await,
        1,
        "the fresh entry's marker must have prevented a duplicate"
    );
}

/// Recovering a stale, unacked lease must not clobber a fresher entry's
/// marker (Codex review, issue #1429).
///
/// Same republish-before-settling setup as
/// `acking_a_stale_lease_leaves_a_fresher_entrys_marker_intact`, but the
/// stale lease's deferred ack fails outright instead of merely running
/// late. It is then never acked at all. It sits in the pending entries
/// list until visibility recovery claims it, long after the fresh entry
/// already claimed the marker. Recovery must see that fresher marker and
/// skip both the duplicate publish and the overwrite, exactly like a stale
/// ack must. The fresh entry is left undelivered rather than read. It then
/// never enters the pending entries list itself, so only the stale lease
/// is there for recovery to find.
#[tokio::test(flavor = "multi_thread")]
async fn recovering_a_stale_lease_leaves_a_fresher_entrys_marker_intact() {
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["recovered-stale".to_string()];
    let task_id = Uuid::new_v4();
    let stale_due = Utc::now();

    fixture
        .dispatch
        .publish(&[hint("recovered-stale", task_id, stale_due)])
        .await
        .expect("publish");
    let stale = read(&fixture, &queues, 10).await;
    assert_eq!(stale.len(), 1);

    // Simulates the fast-completing task's re-pend: republishing before the
    // stale lease is ever settled, at a due time distinct from the first
    // publish. The old marker then reads as not intact, so this writes a
    // fresh entry and overwrites the marker to name it. Left undelivered
    // here, so it never enters the pending entries list itself -- only the
    // stale lease read above does. Recovery's `XPENDING` scan must see
    // just that one idle entry.
    let fresh_due = stale_due + chrono::Duration::milliseconds(1);
    fixture
        .dispatch
        .publish(&[hint("recovered-stale", task_id, fresh_due)])
        .await
        .expect("republish before the stale lease is settled");
    assert_eq!(
        fixture.stream_len("recovered-stale").await,
        2,
        "the republish must add a fresh entry alongside the still-pending stale one"
    );

    // The stale lease's own deferred ack never runs -- it sits pending
    // until visibility recovery claims it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let counts = fixture.dispatch.maintain(&queues).await.expect("maintain");
    assert_eq!(counts.recovered, 1, "the stale entry is still cleared out");
    assert_eq!(fixture.pending_count("recovered-stale").await, 0);

    assert_eq!(
        fixture.stream_len("recovered-stale").await,
        1,
        "recovering the stale lease must not add a duplicate entry"
    );
    assert!(
        fixture.marker_exists("recovered-stale", task_id).await,
        "recovering the stale lease must not delete the fresh entry's marker"
    );

    // A republish at the fresh entry's own due time now reads the marker
    // as intact and skips. That proves it still names the fresh entry, not
    // a recovery-created duplicate.
    fixture
        .dispatch
        .publish(&[hint("recovered-stale", task_id, fresh_due)])
        .await
        .expect("publish at the fresh entry's due time");
    assert_eq!(
        fixture.stream_len("recovered-stale").await,
        1,
        "the fresh entry's marker must have prevented a duplicate"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn maintain_recovers_an_unacked_lease_after_the_visibility_timeout() {
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["crashed".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("crashed", task_id, Utc::now())])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    // The consumer "crashes": the lease is neither acked nor released.
    assert_eq!(fixture.pending_count("crashed").await, 1);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let counts = fixture.dispatch.maintain(&queues).await.expect("maintain");
    assert_eq!(counts.recovered, 1, "an idle entry must be recovered");
    assert_eq!(fixture.pending_count("crashed").await, 0);

    let again = read(&fixture, &queues, 10).await;
    assert_eq!(again.len(), 1, "a recovered entry is delivered again");
    assert_eq!(again[0].task_id, task_id);
    assert_eq!(again[0].redeliveries, 1, "recovery counts one redelivery");
}

#[tokio::test(flavor = "multi_thread")]
async fn maintain_recovers_unacked_leases_across_several_queues_in_one_pass() {
    // Issue #1429: recovery pipelines its `XPENDING` scan across every queue
    // in one round trip. This pins that a multi-queue pass still recovers
    // every queue's idle entries correctly, not only a single queue's.
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["crashed-a".to_string(), "crashed-b".to_string()];
    let task_a = Uuid::new_v4();
    let task_b = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("crashed-a", task_a, Utc::now())])
        .await
        .expect("publish a");
    fixture
        .dispatch
        .publish(&[hint("crashed-b", task_b, Utc::now())])
        .await
        .expect("publish b");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 2);
    // Both consumers "crash": neither lease is acked nor released.
    assert_eq!(fixture.pending_count("crashed-a").await, 1);
    assert_eq!(fixture.pending_count("crashed-b").await, 1);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let counts = fixture.dispatch.maintain(&queues).await.expect("maintain");
    assert_eq!(
        counts.recovered, 2,
        "both queues' idle entries must recover"
    );
    assert_eq!(fixture.pending_count("crashed-a").await, 0);
    assert_eq!(fixture.pending_count("crashed-b").await, 0);

    let again = read(&fixture, &queues, 10).await;
    let recovered_ids: std::collections::HashSet<Uuid> =
        again.iter().map(|lease| lease.task_id).collect();
    assert_eq!(again.len(), 2, "both recovered entries are delivered again");
    assert!(recovered_ids.contains(&task_a));
    assert!(recovered_ids.contains(&task_b));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_queues_broken_pending_scan_does_not_block_its_siblings_recovery() {
    // Issue #1429 review: `recover_queues` reads its pipelined `XPENDING`
    // scan through `req_packed_commands` instead of `Pipeline::query_async`.
    // One queue's reply erroring does not collapse the whole call into a
    // single `Err`. This pins that behavior end to end. A queue whose
    // stream key has the wrong type for `XPENDING` must not stop a sibling
    // queue's idle entry from recovering in the same pass.
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["healthy".to_string(), "broken".to_string()];
    let healthy_task = Uuid::new_v4();
    let broken_task = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("healthy", healthy_task, Utc::now())])
        .await
        .expect("publish healthy");
    fixture
        .dispatch
        .publish(&[hint("broken", broken_task, Utc::now())])
        .await
        .expect("publish broken");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 2);
    assert_eq!(fixture.pending_count("healthy").await, 1);
    assert_eq!(fixture.pending_count("broken").await, 1);

    // Overwrite "broken"'s stream key with a plain string, after the
    // consumer group already claimed its entry above. `XPENDING` against a
    // wrong-typed key fails with a `WRONGTYPE` error. That is the same
    // class of per-queue failure a wrong-typed key or an ACL denial would
    // produce in production.
    let mut raw = fixture.raw.clone();
    let _: () = redis::cmd("SET")
        .arg(fixture.stream_key("broken"))
        .arg("not-a-stream")
        .query_async(&mut raw)
        .await
        .expect("corrupt the broken queue's stream key");

    tokio::time::sleep(Duration::from_millis(400)).await;
    let counts = fixture.dispatch.maintain(&queues).await.expect(
        "maintain must still succeed: the broken queue's XPENDING failure is logged and \
         skipped, not propagated",
    );
    assert_eq!(
        counts.recovered, 1,
        "only the healthy queue's idle entry recovers"
    );
    assert_eq!(fixture.pending_count("healthy").await, 0);

    let again = read(&fixture, &["healthy".to_string()], 10).await;
    assert_eq!(
        again.len(),
        1,
        "the healthy queue's entry is delivered again"
    );
    assert_eq!(again[0].task_id, healthy_task);
    assert_eq!(again[0].redeliveries, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_read_returns_early_when_an_entry_arrives() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["blocking".to_string()];
    let task_id = Uuid::new_v4();

    // Ensure the group exists before the blocking read starts.
    assert!(read(&fixture, &queues, 10).await.is_empty());

    let publisher = fixture.dispatch.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        publisher
            .publish(&[hint("blocking", task_id, Utc::now())])
            .await
            .expect("publish");
    });

    let started = Instant::now();
    let leases = fixture
        .dispatch
        .next(&queues, "consumer-1", 10, Duration::from_secs(5))
        .await
        .expect("next");
    let elapsed = started.elapsed();
    handle.await.expect("publisher");

    assert_eq!(leases.len(), 1, "the blocking read must wake on the XADD");
    assert!(
        elapsed < Duration::from_secs(2),
        "the read must return on arrival, not on the wait deadline (elapsed {elapsed:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_queue_blocking_read_still_delivers_an_arrival_on_either_queue() {
    // Issue #1429, Codex review: a multi-queue read no longer combines every
    // queue's stream into one blocking `XREADGROUP` (that crossed Redis
    // Cluster slots). It blocks on only the queue this call's own rotation
    // leads with, and reads every other queue non-blocking. A caller's own
    // poll loop calls `next` repeatedly. It still picks up an arrival on any
    // queue within a small bounded number of calls, regardless of which
    // queue led this call's rotation.
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["multi-a".to_string(), "multi-b".to_string()];
    // Prime both consumer groups so neither read hits NOGROUP mid-test.
    assert!(read(&fixture, &queues, 10).await.is_empty());

    let task_id = Uuid::new_v4();
    let publisher = fixture.dispatch.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        publisher
            .publish(&[hint("multi-b", task_id, Utc::now())])
            .await
            .expect("publish");
    });

    let started = Instant::now();
    let mut leases = Vec::new();
    while leases.is_empty() && started.elapsed() < Duration::from_secs(5) {
        leases = fixture
            .dispatch
            .next(&queues, "consumer-1", 10, Duration::from_millis(300))
            .await
            .expect("next");
    }
    handle.await.expect("publisher");

    assert_eq!(
        leases.len(),
        1,
        "the arrival on either queue must still be delivered"
    );
    assert_eq!(leases[0].task_id, task_id);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "delivery must stay within a small bounded number of poll calls (elapsed {:?})",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_consumer_group_self_heals() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["healing".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("healing", task_id, Utc::now())])
        .await
        .expect("publish");
    fixture.destroy_group("healing").await;

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(
        leases.len(),
        1,
        "a read must recreate the group and still see the live entry"
    );
    assert_eq!(leases[0].task_id, task_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_across_two_queues_returns_at_most_the_requested_count() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["left".to_string(), "right".to_string()];
    let mut published = Vec::new();
    for queue in &queues {
        for _ in 0..2 {
            let task_id = Uuid::new_v4();
            published.push(task_id);
            fixture
                .dispatch
                .publish(&[hint(queue, task_id, Utc::now())])
                .await
                .expect("publish");
        }
    }

    // Each queue's `COUNT` is sized from the batch's remaining capacity
    // (issue #1429). This read still honours the cap of three: the first
    // queue visited takes up to all three, leaving only what is left for
    // the second. Whichever queue that leaves an entry unclaimed, it stays
    // in its stream for a later read to pick up.
    let leases = read(&fixture, &queues, 3).await;
    assert_eq!(leases.len(), 3, "the read must honour the caller's cap");
    assert_eq!(
        fixture.pending_count("left").await + fixture.pending_count("right").await,
        3,
        "a requeued surplus must not stay in the pending entries list"
    );

    let rest = read(&fixture, &queues, 10).await;
    assert_eq!(rest.len(), 1, "the surplus must be deliverable again");
    let mut seen: Vec<Uuid> = leases
        .iter()
        .chain(rest.iter())
        .map(|lease| lease.task_id)
        .collect();
    seen.sort_unstable();
    published.sort_unstable();
    assert_eq!(seen, published, "every reference must be delivered once");
}

/// A single busy queue's read is sized from the full remaining batch
/// capacity, not an equal `max / queue_count` split (Codex review, issue
/// #1429).
///
/// Before this fix, `per_stream_count` divided the cap evenly across every
/// configured queue, regardless of which ones actually had work. A worker
/// serving many mostly idle queues would then throttle its one busy queue
/// to `max / N` per read. Filling one batch needed roughly `N` such reads,
/// each visiting every queue: about `N²` commands for what one full-budget
/// read now does. This also means a same-call surplus across queues, the
/// kind `a_capped_read_favors_the_higher_priority_candidates` used to
/// exercise, no longer normally arises. The busy queue processed first now
/// consumes the whole cap before a second queue is ever visited. The
/// priority sort-and-split in `next_inner` stays as a defensive
/// fallback regardless.
#[tokio::test(flavor = "multi_thread")]
async fn a_busy_queue_gets_the_full_budget_among_idle_peers() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec![
        "busy".to_string(),
        "idle-a".to_string(),
        "idle-b".to_string(),
        "idle-c".to_string(),
    ];
    let mut published = Vec::new();
    for _ in 0..4 {
        let task_id = Uuid::new_v4();
        published.push(task_id);
        fixture
            .dispatch
            .publish(&[hint("busy", task_id, Utc::now())])
            .await
            .expect("publish");
    }

    // Four idle peer queues no longer throttle "busy"'s own read to
    // `4 / 4 == 1` per call. One call now delivers all four.
    let leases = read(&fixture, &queues, 4).await;
    assert_eq!(
        leases.len(),
        4,
        "the busy queue's read must not be capped by its idle peers"
    );
    let mut delivered: Vec<Uuid> = leases.iter().map(|lease| lease.task_id).collect();
    delivered.sort_unstable();
    published.sort_unstable();
    assert_eq!(delivered, published);
}

/// A persistently broken queue must surface as an error, not a quiet
/// success, even while a healthy sibling queue keeps delivering (Codex
/// review, issue #1429).
///
/// `read_across_queues` used to swallow a failing queue's error whenever
/// any other queue in the same pass returned an entry. Consider a queue
/// whose stream key is the wrong Redis type: `WRONGTYPE`, which
/// `read_with_heal` cannot self-heal the way it heals `NOGROUP`. It would
/// then never surface its own failure as long as a busy sibling kept
/// every pass "ready". The
/// caller's `enter_degraded` fallback never engaged, so the broken queue
/// went undrained except by the much slower reconcile sweep.
#[tokio::test(flavor = "multi_thread")]
async fn a_persistently_broken_queue_is_not_masked_by_a_healthy_sibling() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["healthy".to_string(), "broken".to_string()];

    fixture
        .dispatch
        .publish(&[hint("healthy", Uuid::new_v4(), Utc::now())])
        .await
        .expect("publish");

    // Corrupt "broken"'s stream key to a non-stream type. `XREADGROUP` on
    // it then fails `WRONGTYPE`, which `read_with_heal` cannot self-heal
    // (only `NOGROUP` is healed).
    let mut raw = fixture.raw.clone();
    let _: () = redis::cmd("SET")
        .arg(fixture.stream_key("broken"))
        .arg("not-a-stream")
        .query_async(&mut raw)
        .await
        .expect("corrupt the broken queue's stream key");

    let result = fixture
        .dispatch
        .next(&queues, "consumer-1", 10, Duration::from_millis(50))
        .await;
    assert!(
        result.is_err(),
        "a persistently broken queue must surface as an error, not a quiet \
         success, even though the healthy queue had a ready entry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_queue_is_served_when_the_read_cap_is_one() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["first".to_string(), "second".to_string()];
    for queue in &queues {
        fixture
            .dispatch
            .publish(&[hint(queue, Uuid::new_v4(), Utc::now())])
            .await
            .expect("publish");
    }

    // One lease per read, and the rotation must reach both queues.
    let mut served = Vec::new();
    for _ in 0..2 {
        let leases = read(&fixture, &queues, 1).await;
        assert_eq!(leases.len(), 1, "a cap of one must deliver one lease");
        served.push(leases[0].queue_name.clone());
    }
    served.sort();
    assert_eq!(
        served,
        vec!["first".to_string(), "second".to_string()],
        "the queue order must rotate so no queue starves"
    );
}

/// An entry published on a non-leader queue must not wait out the leader's
/// full blocking timeout (Codex review, issue #1429).
///
/// `read_across_queues` blocks each queue's stream on its own single-key
/// call. A multi-key `XREADGROUP` across several queues' hash-tagged
/// streams would cross Redis Cluster slots. Blocking the whole `wait` on
/// only the rotation's leader has a cost, though. An entry can land on a
/// different queue right after that queue's own non-blocking scan. It then
/// sits unseen until the leader's timeout expires. The fix caps each
/// queue's blocking slice and cycles the rotation across `wait`'s budget.
/// A sibling queue's arrival then surfaces within about one slice, instead
/// of the whole wait.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_leader_queue_is_not_starved_behind_the_leaders_full_wait() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    // First call's rotation offset is 0, so `ordered` keeps this order:
    // "first" leads, "second" does not.
    let queues = vec!["first".to_string(), "second".to_string()];
    let wait = Duration::from_millis(600);

    let dispatch = fixture.dispatch.clone();
    let read_queues = queues.clone();
    let handle = tokio::spawn(async move {
        let started = Instant::now();
        let leases = dispatch
            .next(&read_queues, "consumer-1", 1, wait)
            .await
            .expect("next");
        (started.elapsed(), leases)
    });

    // Give the read time to start blocking on "first" before publishing to
    // "second", so this exercises the rotation rather than the initial
    // non-blocking pass.
    tokio::time::sleep(Duration::from_millis(50)).await;
    fixture
        .dispatch
        .publish(&[hint("second", Uuid::new_v4(), Utc::now())])
        .await
        .expect("publish");

    let (elapsed, leases) = handle.await.expect("join");
    assert_eq!(
        leases.len(),
        1,
        "the read must find the entry published on the non-leader queue"
    );
    assert_eq!(leases[0].queue_name, "second");
    assert!(
        elapsed < wait / 2,
        "a non-leader queue's arrival must not wait out the leader's full \
         block; elapsed was {elapsed:?} against a {wait:?} wait"
    );
}

/// A queue near the tail of a long rotation must still get a blocking
/// look within the same `wait` (Codex review, issue #1429). That must
/// hold however many queues are configured.
///
/// A fixed per-queue blocking slice does not fit a lap with more queues
/// than `wait / QUEUE_BLOCK_SLICE`. The deadline passes before the
/// rotation reaches a queue near the tail. The rotation always restarts
/// at 0 on the next call. That queue would then never get a blocking
/// slice at all, not just a delayed one. `read_across_queues` instead
/// divides the budget still left by the queues still left in the lap. So
/// every queue gets one blocking look inside the same `wait`.
#[tokio::test(flavor = "multi_thread")]
async fn a_queue_near_the_tail_of_a_long_rotation_still_gets_a_blocking_look() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    // 20 queues at a fixed 200ms slice each would need 4s to reach the
    // last one, more than this test's 3s wait. First call's rotation
    // offset is 0, so `ordered` keeps this declaration order.
    let queues: Vec<String> = (0..20).map(|i| format!("tail-{i}")).collect();
    let wait = Duration::from_secs(3);
    let tail_queue = queues.last().cloned().expect("at least one queue");

    let dispatch = fixture.dispatch.clone();
    let read_queues = queues.clone();
    let handle = tokio::spawn(async move {
        let started = Instant::now();
        let leases = dispatch
            .next(&read_queues, "consumer-1", 1, wait)
            .await
            .expect("next");
        (started.elapsed(), leases)
    });

    // Give the read time to start blocking before publishing to the tail
    // queue, so this exercises the rotation rather than the initial
    // non-blocking pass.
    tokio::time::sleep(Duration::from_millis(50)).await;
    fixture
        .dispatch
        .publish(&[hint(&tail_queue, Uuid::new_v4(), Utc::now())])
        .await
        .expect("publish");

    let (elapsed, leases) = handle.await.expect("join");
    assert_eq!(
        leases.len(),
        1,
        "the read must find the entry published on the tail queue"
    );
    assert_eq!(leases[0].queue_name, tail_queue);
    // `wait` only bounds the blocking phase. The initial non-blocking pass
    // over all 20 queues runs before that budget starts, adding its own
    // round-trip time. So this leaves it a margin on top of `wait`.
    let generous_bound = wait + Duration::from_secs(1);
    assert!(
        elapsed < generous_bound,
        "a tail queue's arrival must surface within the same wait, not a \
         later call; elapsed was {elapsed:?} against a {generous_bound:?} bound"
    );
}

/// An address that accepts no connection, so `connect` has to time out.
///
/// Some environments answer with a reset at once. The case tolerates that:
/// it asserts an error and a bound on the wait, not a wait.
#[tokio::test(flavor = "multi_thread")]
async fn connect_to_an_unreachable_address_fails_fast() {
    let started = Instant::now();
    let result =
        RedisDispatch::connect("redis://10.255.255.1:6379", RedisDispatchConfig::default()).await;
    let elapsed = started.elapsed();
    assert!(result.is_err(), "an unreachable address must not connect");
    assert!(
        elapsed < Duration::from_secs(10),
        "connect must fail inside its timeout (elapsed {elapsed:?})"
    );
}

/// An entry the channel cannot read must leave the pending entries list
/// (issue #1312).
///
/// `XREADGROUP` puts every delivered entry in the pending entries list. A
/// worker that only drops an unreadable entry leaves it there for good. The
/// recovery pass then claims it on every sweep and leaves it pending again.
/// `XPENDING` reads a fixed window, so enough such entries hide every
/// legitimate abandoned lease below them.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_entry_leaves_the_pending_list() {
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["malformed".to_string()];
    let task_id = Uuid::new_v4();

    // A real publish creates the consumer group the hand-written entries need.
    fixture
        .dispatch
        .publish(&[hint("malformed", task_id, Utc::now())])
        .await
        .expect("publish");

    let key = fixture.stream_key("malformed");
    let mut conn = fixture.raw.clone();
    let _: String = redis::cmd("XADD")
        .arg(&key)
        .arg("*")
        .arg("other")
        .arg("no payload field here")
        .query_async(&mut conn)
        .await
        .expect("xadd an entry with no payload field");
    let _: String = redis::cmd("XADD")
        .arg(&key)
        .arg("*")
        .arg("payload")
        .arg("{ this is not json")
        .query_async(&mut conn)
        .await
        .expect("xadd an entry with an unreadable payload");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1, "only the legitimate entry yields a lease");
    assert_eq!(leases[0].task_id, task_id);

    assert_eq!(
        fixture.pending_count("malformed").await,
        1,
        "a malformed entry must be acknowledged, so only the live lease is pending"
    );
    assert_eq!(
        fixture.stream_len("malformed").await,
        1,
        "a malformed entry must be deleted, and nothing else with it"
    );

    // The legitimate lease is abandoned. Recovery must still reach it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let counts = fixture.dispatch.maintain(&queues).await.expect("maintain");
    assert_eq!(counts.recovered, 1, "the abandoned lease must be recovered");
    assert_eq!(
        fixture.pending_count("malformed").await,
        0,
        "no entry may be left pending after the recovery pass"
    );
}

/// Malformed entries spanning two queues in one multi-queue read must both
/// get cleaned up (Codex review, issue #1429).
///
/// `discard_entries` used to build one atomic `MULTI`/`EXEC` pipe over
/// every malformed entry a read collected, across every queue involved.
/// Each queue's stream carries its own hash tag. That pipe would then fail
/// `CROSSSLOT` on a real Cluster the moment two queues both contributed a
/// malformed entry to the same multi-queue read. It now groups by stream
/// first, mirroring `ack_many_inner`/`requeue_batch`. This pins the
/// functional behavior that split preserves. Every queue's malformed
/// entries still get cleaned up, not just the first one a `HashMap`
/// happens to visit.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_entries_across_two_queues_are_both_discarded() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["malformed-a".to_string(), "malformed-b".to_string()];
    let task_a = Uuid::new_v4();
    let task_b = Uuid::new_v4();

    // A real publish on each queue creates the consumer group the
    // hand-written entries need.
    fixture
        .dispatch
        .publish(&[hint("malformed-a", task_a, Utc::now())])
        .await
        .expect("publish a");
    fixture
        .dispatch
        .publish(&[hint("malformed-b", task_b, Utc::now())])
        .await
        .expect("publish b");

    let mut conn = fixture.raw.clone();
    for queue in &queues {
        let key = fixture.stream_key(queue);
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("*")
            .arg("other")
            .arg("no payload field here")
            .query_async(&mut conn)
            .await
            .expect("xadd an entry with no payload field");
    }

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 2, "only the legitimate entries yield leases");

    assert_eq!(
        fixture.pending_count("malformed-a").await,
        1,
        "queue a's malformed entry must be acknowledged"
    );
    assert_eq!(
        fixture.pending_count("malformed-b").await,
        1,
        "queue b's malformed entry must be acknowledged too, not just queue a's"
    );
    assert_eq!(fixture.stream_len("malformed-a").await, 1);
    assert_eq!(fixture.stream_len("malformed-b").await, 1);
}

/// The recovery pass discards an entry it cannot read, rather than leaving it
/// pending for the next pass (issue #1312).
#[tokio::test(flavor = "multi_thread")]
async fn the_recovery_pass_discards_a_malformed_entry() {
    let Some(fixture) = try_start(Duration::from_millis(300)).await else {
        return;
    };
    let queues = vec!["recover_malformed".to_string()];
    let task_id = Uuid::new_v4();

    fixture
        .dispatch
        .publish(&[hint("recover_malformed", task_id, Utc::now())])
        .await
        .expect("publish");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    fixture.dispatch.ack(&leases[0]).await.expect("ack");

    // A malformed entry that reaches the pending list without going through
    // `next`. A peer on an older build is one way to get one.
    let key = fixture.stream_key("recover_malformed");
    let mut conn = fixture.raw.clone();
    let _: String = redis::cmd("XADD")
        .arg(&key)
        .arg("*")
        .arg("payload")
        .arg("{ this is not json")
        .query_async(&mut conn)
        .await
        .expect("xadd an entry with an unreadable payload");
    let _: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("harvest_workers")
        .arg("peer")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg(&key)
        .arg(">")
        .query_async(&mut conn)
        .await
        .expect("peer read");
    assert_eq!(fixture.pending_count("recover_malformed").await, 1);

    tokio::time::sleep(Duration::from_millis(400)).await;
    fixture.dispatch.maintain(&queues).await.expect("maintain");
    assert_eq!(
        fixture.pending_count("recover_malformed").await,
        0,
        "the recovery pass must discard an entry it cannot read"
    );
    assert_eq!(
        fixture.stream_len("recover_malformed").await,
        0,
        "the discarded entry must leave the stream"
    );
}

/// A reference keeps the pool it needs across the stream (issue #1312).
#[tokio::test(flavor = "multi_thread")]
async fn a_lease_carries_the_kind_of_its_hint() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["typed".to_string()];
    let task_id = Uuid::new_v4();

    let mut typed = hint("typed", task_id, Utc::now());
    typed.kind = Some(autumn_harvest::dispatch::DispatchKind::Activity);
    fixture.dispatch.publish(&[typed]).await.expect("publish");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(leases.len(), 1);
    assert_eq!(
        leases[0].kind,
        Some(autumn_harvest::dispatch::DispatchKind::Activity),
        "the lease must name the pool the reference needs"
    );
}

/// The marker must not outlive the reference it stands for (issue #1312).
///
/// A key eviction, an external `XTRIM` or an operator deleting the stream can
/// take the entry and leave the marker. Every republish then refreshed the
/// marker TTL, so the marker lived for good and the row stayed `PENDING` for
/// good.
#[tokio::test(flavor = "multi_thread")]
async fn a_republish_restores_a_stream_entry_that_vanished() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["vanished".to_string()];
    let task_id = Uuid::new_v4();
    let due = Utc::now();

    fixture
        .dispatch
        .publish(&[hint("vanished", task_id, due)])
        .await
        .expect("publish");
    assert_eq!(fixture.stream_len("vanished").await, 1);

    // The entry goes; the marker stays.
    let mut conn = fixture.raw.clone();
    let ids: Vec<String> = redis::cmd("XRANGE")
        .arg(fixture.stream_key("vanished"))
        .arg("-")
        .arg("+")
        .query_async::<Vec<(String, Vec<String>)>>(&mut conn)
        .await
        .expect("xrange")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let _: i64 = redis::cmd("XDEL")
        .arg(fixture.stream_key("vanished"))
        .arg(&ids[0])
        .query_async(&mut conn)
        .await
        .expect("xdel");
    assert_eq!(fixture.stream_len("vanished").await, 0);
    assert!(
        fixture.marker_exists("vanished", task_id).await,
        "the case needs the marker to outlive the entry"
    );

    // The reconcile sweep republishes the same hint.
    fixture
        .dispatch
        .publish(&[hint("vanished", task_id, due)])
        .await
        .expect("republish");

    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(
        leases.len(),
        1,
        "a republish must restore a reference the marker can no longer account for"
    );
    assert_eq!(leases[0].task_id, task_id);
}

/// The same rule for a parked reference (issue #1312).
#[tokio::test(flavor = "multi_thread")]
async fn a_republish_restores_a_parked_reference_that_vanished() {
    let Some(fixture) = try_start(Duration::from_secs(60)).await else {
        return;
    };
    let queues = vec!["parked".to_string()];
    let task_id = Uuid::new_v4();
    let due = Utc::now() + chrono::Duration::milliseconds(400);

    fixture
        .dispatch
        .publish(&[hint("parked", task_id, due)])
        .await
        .expect("publish");

    // The parked reference goes; the marker stays.
    let mut conn = fixture.raw.clone();
    let _: i64 = redis::cmd("ZREM")
        .arg(dispatch_delayed_key(&fixture.prefix, "parked"))
        .arg(task_id.to_string())
        .query_async(&mut conn)
        .await
        .expect("zrem");
    assert!(
        fixture.marker_exists("parked", task_id).await,
        "the case needs the marker to outlive the parked reference"
    );

    fixture
        .dispatch
        .publish(&[hint("parked", task_id, due)])
        .await
        .expect("republish");

    tokio::time::sleep(Duration::from_millis(500)).await;
    fixture.dispatch.maintain(&queues).await.expect("maintain");
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(
        leases.len(),
        1,
        "a republish must restore a parked reference the marker can no longer account for"
    );
    assert_eq!(leases[0].task_id, task_id);
}
