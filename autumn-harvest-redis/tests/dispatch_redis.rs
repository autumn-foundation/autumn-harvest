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
async fn a_later_due_time_for_a_held_entry_is_a_no_op() {
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
    let leases = read(&fixture, &queues, 10).await;
    assert_eq!(
        leases.len(),
        1,
        "the later hint must not push the parked entry out"
    );
    assert_eq!(leases[0].task_id, task_id);
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
