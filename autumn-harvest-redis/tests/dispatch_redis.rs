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
use autumn_harvest_redis::{RedisDispatch, RedisDispatchConfig};
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
        format!("{}:dispatch:{queue}", self.prefix)
    }

    fn marker_key(&self, task_id: Uuid) -> String {
        format!("{}:dispatch:marker:{task_id}", self.prefix)
    }

    async fn stream_len(&self, queue: &str) -> i64 {
        let mut conn = self.raw.clone();
        conn.xlen(self.stream_key(queue)).await.expect("xlen")
    }

    async fn marker_exists(&self, task_id: Uuid) -> bool {
        let mut conn = self.raw.clone();
        conn.exists(self.marker_key(task_id)).await.expect("exists")
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
        fixture.marker_exists(task_id).await,
        "publish sets a marker"
    );

    fixture.dispatch.ack(&leases[0]).await.expect("ack");
    assert!(
        !fixture.marker_exists(task_id).await,
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

    // `COUNT` bounds one stream, so a read over two queues can return four
    // entries for a caller that asked for three. The surplus goes back.
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
/// (issue #1312 review round 1, finding F4).
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

/// The recovery pass discards an entry it cannot read, rather than leaving it
/// pending for the next pass (issue #1312 review round 1, finding F4).
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

/// A reference keeps the pool it needs across the stream (issue #1312 review
/// round 1, finding F7).
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

/// The marker must not outlive the reference it stands for (issue #1312
/// review round 1, finding F8).
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
        fixture.marker_exists(task_id).await,
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

/// The same rule for a parked reference (issue #1312 review round 1, F8).
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
        .arg(format!("{}:dispatch:parked:delayed", fixture.prefix))
        .arg(task_id.to_string())
        .query_async(&mut conn)
        .await
        .expect("zrem");
    assert!(
        fixture.marker_exists(task_id).await,
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
