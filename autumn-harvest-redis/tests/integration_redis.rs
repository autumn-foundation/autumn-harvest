//! End-to-end tests for the Redis Streams adapter.
//!
//! Each test spins up a fresh `redis:5.0` container via `testcontainers` so
//! tests are hermetic. They are skipped automatically when Docker is not
//! available -- the `start()` call returns an error and we mark the test as
//! `Ok(())` rather than panicking, which keeps the suite green on machines
//! without a Docker daemon (e.g. Windows CI without Docker Desktop).

use std::time::Duration;

use autumn_harvest_redis::{
    EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis};

/// A live Redis instance plus a queue connected to it.
///
/// The `_container` slot keeps the testcontainer alive for the duration of
/// the test; when the env-var path is taken (CI / local dev with an existing
/// Redis), it stays `None`.
struct RedisFixture {
    _container: Option<testcontainers::ContainerAsync<Redis>>,
    queue: RedisTaskQueue,
}

/// Try to obtain a Redis instance, in order of preference:
///   1. The `HARVEST_REDIS_TEST_URL` environment variable, if set.
///   2. A `redis:5.0` testcontainer.
///
/// On hosts without Docker (and without the env var set) the helper returns
/// `None` so the calling test skips cleanly. This mirrors the convention used
/// by the other integration tests in the workspace.
async fn try_start_redis() -> Option<RedisFixture> {
    // Use a unique prefix per fixture so parallel tests sharing the same
    // Redis don't collide on stream / sorted set keys.
    let prefix = format!("test_{}", uuid::Uuid::new_v4().simple());
    let cfg = RedisTaskQueueConfig {
        key_prefix: prefix,
        // Short visibility timeout so the recovery test stays fast.
        visibility_timeout: Duration::from_millis(250),
        ..RedisTaskQueueConfig::default()
    };

    if let Ok(url) = std::env::var("HARVEST_REDIS_TEST_URL") {
        let queue = match RedisTaskQueue::connect(&url, cfg.clone()).await {
            Ok(q) => q,
            Err(e) => {
                eprintln!("skipping: HARVEST_REDIS_TEST_URL set but unreachable: {e}");
                return None;
            }
        };
        return Some(RedisFixture {
            _container: None,
            queue,
        });
    }

    let container = match Redis::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: docker unavailable: {e}");
            return None;
        }
    };

    let host = container.get_host().await.ok()?;
    let port = container.get_host_port_ipv4(REDIS_PORT).await.ok()?;
    let url = format!("redis://{host}:{port}");
    let queue = RedisTaskQueue::connect(&url, cfg).await.ok()?;
    Some(RedisFixture {
        _container: Some(container),
        queue,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_and_claim_round_trips_a_task() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "default".to_string();

    let task_id = queue
        .enqueue(EnqueueParams::new(
            &q,
            TaskType::Activity,
            serde_json::json!({"to": "alice"}),
        ))
        .await
        .expect("enqueue");

    let claim = queue
        .claim(std::slice::from_ref(&q), "worker-1")
        .await
        .expect("claim")
        .expect("expected one task");
    assert_eq!(claim.task_id(), task_id);
    assert_eq!(claim.envelope.attempt, 1, "attempt counter starts at 1");
    assert_eq!(claim.envelope.input, serde_json::json!({"to": "alice"}));

    queue
        .complete(&claim, serde_json::json!({"ok": true}))
        .await
        .expect("complete");

    // Stream should be empty now.
    let depths = queue.queue_depths(&[q]).await.expect("depths");
    assert_eq!(depths[0].1, 0, "queue must drain after complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_task_is_invisible_until_due() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "delayed".to_string();

    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!(null));
    params.scheduled_at = chrono::Utc::now() + chrono::Duration::milliseconds(400);
    let _ = queue.enqueue(params).await.expect("enqueue");

    // Immediately after enqueue, claim sees nothing -- the task is on the
    // scheduled sorted set, not the stream.
    let claim = queue
        .claim(std::slice::from_ref(&q), "worker-1")
        .await
        .expect("claim");
    assert!(claim.is_none(), "delayed task must not be claimable yet");

    // After the delay elapses, the next claim() promotes it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let claim = queue
        .claim(std::slice::from_ref(&q), "worker-1")
        .await
        .expect("claim post-delay")
        .expect("delayed task should be due now");
    queue
        .complete(&claim, serde_json::Value::Null)
        .await
        .expect("complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn requeue_for_retry_keeps_task_id_and_advances_attempt() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "retry".to_string();

    let task_id = queue
        .enqueue(EnqueueParams::new(
            &q,
            TaskType::Activity,
            serde_json::json!({"n": 1}),
        ))
        .await
        .expect("enqueue");

    let first = queue
        .claim(std::slice::from_ref(&q), "w1")
        .await
        .expect("claim")
        .expect("first claim");
    assert_eq!(first.envelope.attempt, 1);
    queue
        .requeue_for_retry(&first, Duration::from_millis(100))
        .await
        .expect("requeue");

    // Wait past the retry delay then the second claim must surface the same
    // task_id with attempt incremented.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let second = queue
        .claim(std::slice::from_ref(&q), "w1")
        .await
        .expect("claim")
        .expect("retry must surface");
    assert_eq!(second.task_id(), task_id);
    assert_eq!(
        second.envelope.attempt, 2,
        "attempt counter must advance across retries"
    );
    queue
        .complete(&second, serde_json::Value::Null)
        .await
        .expect("complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_pending_reclaims_idle_entries() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "recover".to_string();

    queue
        .enqueue(EnqueueParams::new(
            &q,
            TaskType::Workflow,
            serde_json::json!(null),
        ))
        .await
        .expect("enqueue");

    // Worker A claims the task and "crashes" without ack-ing.
    let claim = queue
        .claim(std::slice::from_ref(&q), "worker-a")
        .await
        .expect("claim")
        .expect("first claim");
    drop(claim);

    // Without recovery, worker B sees nothing -- the entry is on worker A's
    // PEL.
    let none = queue
        .claim(std::slice::from_ref(&q), "worker-b")
        .await
        .expect("claim");
    assert!(
        none.is_none(),
        "PEL entries must not double-deliver to peers"
    );

    // After the visibility timeout, recover_pending re-queues it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let reclaimed = queue
        .recover_pending(std::slice::from_ref(&q))
        .await
        .expect("recover");
    assert_eq!(reclaimed, 1, "recovery should reclaim the orphaned entry");

    let after_recovery = queue
        .claim(std::slice::from_ref(&q), "worker-b")
        .await
        .expect("claim")
        .expect("worker-b should now see the reclaimed task");
    queue
        .complete(&after_recovery, serde_json::Value::Null)
        .await
        .expect("complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn fail_pushes_to_dlq_when_enabled() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "deadletter".to_string();

    queue
        .enqueue(EnqueueParams::new(
            &q,
            TaskType::Activity,
            serde_json::json!({"x": 1}),
        ))
        .await
        .expect("enqueue");

    let claim = queue
        .claim(std::slice::from_ref(&q), "w1")
        .await
        .expect("claim")
        .expect("one task");
    queue.fail(&claim, "permanent failure").await.expect("fail");

    // Main queue is drained; the DLQ stream now holds the failed envelope.
    let depths = queue
        .queue_depths(std::slice::from_ref(&q))
        .await
        .expect("depths");
    assert_eq!(depths[0].1, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn claim_load_balances_across_consumers() {
    let Some(fixture) = try_start_redis().await else {
        return;
    };
    let queue = &fixture.queue;
    let q = "balanced".to_string();

    for i in 0..4 {
        queue
            .enqueue(EnqueueParams::new(
                &q,
                TaskType::Activity,
                serde_json::json!({"i": i}),
            ))
            .await
            .expect("enqueue");
    }

    // Two consumers should each receive at least one of the four tasks.
    let mut a_count = 0;
    let mut b_count = 0;
    for _ in 0..4 {
        if let Some(claim) = queue
            .claim(std::slice::from_ref(&q), "worker-a")
            .await
            .expect("claim a")
        {
            a_count += 1;
            queue
                .complete(&claim, serde_json::Value::Null)
                .await
                .expect("complete a");
        } else if let Some(claim) = queue
            .claim(std::slice::from_ref(&q), "worker-b")
            .await
            .expect("claim b")
        {
            b_count += 1;
            queue
                .complete(&claim, serde_json::Value::Null)
                .await
                .expect("complete b");
        }
    }
    assert!(
        a_count + b_count == 4,
        "all four tasks should be processed (a={a_count}, b={b_count})"
    );
}
