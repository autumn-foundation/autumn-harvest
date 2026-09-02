// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It exercises
// autumn-harvest-redis's existing public API only; it does not modify or
// depend on any workspace crate other than that one.
//
// Measures sustained enqueue -> claim -> complete round trips/sec through
// RedisTaskQueue, at a swept worker-concurrency count, against a local Redis.
// All workers share one queue name (the pre-registered condition), each with
// its own consumer identity, so the number reflects real consumer-group
// contention on a shared stream rather than N independent streams.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest_redis::{EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType};
use tokio::sync::Barrier;

const QUEUE_NAME: &str = "bench-shared";

async fn run_worker(
    queue: RedisTaskQueue,
    worker_id: String,
    start_gate: Arc<Barrier>,
    duration: Duration,
    counter: Arc<AtomicU64>,
) {
    // All workers connect and reach this point before any of them starts
    // enqueuing, so the measured window and the concurrency it reports are
    // the same window for every worker -- no worker is running solo while
    // later ones are still connecting.
    start_gate.wait().await;
    let deadline = Instant::now() + duration;

    let payload = serde_json::json!({"to": "alice", "n": 1});
    loop {
        if Instant::now() >= deadline {
            return;
        }
        let params = EnqueueParams::new(QUEUE_NAME, TaskType::Activity, payload.clone());
        if queue.enqueue(params).await.is_err() {
            continue;
        }
        loop {
            match queue.claim(std::slice::from_ref(&QUEUE_NAME.to_string()), &worker_id).await {
                Ok(Some(claimed)) => {
                    // Only a successful ack is a completed round trip. A
                    // failed complete() leaves the entry pending in Redis --
                    // counting it anyway would let transient failures inflate
                    // the reported throughput.
                    if queue.complete(&claimed, serde_json::json!({"ok": true})).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    break;
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
                Err(_) => break,
            }
        }
    }
}

async fn sweep_one(concurrency: usize, duration: Duration, redis_url: &str) -> f64 {
    let mut handles = Vec::with_capacity(concurrency);
    let counter = Arc::new(AtomicU64::new(0));
    // +1: main also waits at the gate, so the timer starts only once every
    // worker has a live connection and is parked on `wait()`.
    let start_gate = Arc::new(Barrier::new(concurrency + 1));

    for i in 0..concurrency {
        let cfg = RedisTaskQueueConfig {
            key_prefix: "bench".to_string(),
            ..RedisTaskQueueConfig::default()
        };
        let queue = RedisTaskQueue::connect(redis_url, cfg)
            .await
            .expect("connect to local redis");
        let worker_id = format!("bench-worker-{i}");
        let counter = counter.clone();
        let start_gate = start_gate.clone();
        handles.push(tokio::spawn(async move {
            run_worker(queue, worker_id, start_gate, duration, counter).await;
        }));
    }

    start_gate.wait().await;
    for h in handles {
        let _ = h.await;
    }

    let total = counter.load(Ordering::Relaxed);
    total as f64 / duration.as_secs_f64()
}

/// Drain a pre-seeded, static backlog with claim-only workers -- no worker
/// enqueues anything once the seed is in place. This mirrors
/// `docs/performance.md`'s own methodology ("N real claim_task() calls
/// draining the full backlog") instead of the steady-state loop above, which
/// keeps the queue near-empty by construction and is therefore not a fair
/// comparison against a Postgres number that is specifically about draining
/// a seeded backlog.
async fn run_drain_worker(
    queue: RedisTaskQueue,
    worker_id: String,
    start_gate: Arc<Barrier>,
    backlog: u64,
    completed: Arc<AtomicU64>,
    ceiling: Duration,
) {
    start_gate.wait().await;
    let deadline = Instant::now() + ceiling;
    loop {
        if completed.load(Ordering::Relaxed) >= backlog || Instant::now() >= deadline {
            return;
        }
        match queue.claim(std::slice::from_ref(&QUEUE_NAME.to_string()), &worker_id).await {
            Ok(Some(claimed)) => {
                if queue.complete(&claimed, serde_json::json!({"ok": true})).await.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(None) => {
                if completed.load(Ordering::Relaxed) >= backlog {
                    return;
                }
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            Err(_) => return,
        }
    }
}

/// Returns (claims_per_sec, drained_before_ceiling).
async fn backlog_drain_scenario(
    backlog: u64,
    concurrency: usize,
    ceiling: Duration,
    redis_url: &str,
) -> (f64, bool) {
    let seed_cfg = RedisTaskQueueConfig {
        key_prefix: "bench".to_string(),
        ..RedisTaskQueueConfig::default()
    };
    let seeder = RedisTaskQueue::connect(redis_url, seed_cfg)
        .await
        .expect("connect to local redis for seeding");
    let payload = serde_json::json!({"to": "alice", "n": 1});
    for _ in 0..backlog {
        let params = EnqueueParams::new(QUEUE_NAME, TaskType::Activity, payload.clone());
        seeder.enqueue(params).await.expect("seed enqueue");
    }

    let completed = Arc::new(AtomicU64::new(0));
    let start_gate = Arc::new(Barrier::new(concurrency + 1));
    let mut handles = Vec::with_capacity(concurrency);

    for i in 0..concurrency {
        let cfg = RedisTaskQueueConfig {
            key_prefix: "bench".to_string(),
            ..RedisTaskQueueConfig::default()
        };
        let queue = RedisTaskQueue::connect(redis_url, cfg)
            .await
            .expect("connect to local redis");
        let worker_id = format!("bench-drain-worker-{i}");
        let completed = completed.clone();
        let start_gate = start_gate.clone();
        handles.push(tokio::spawn(async move {
            run_drain_worker(queue, worker_id, start_gate, backlog, completed, ceiling).await;
        }));
    }

    start_gate.wait().await;
    let started = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let elapsed = started.elapsed();

    let drained = completed.load(Ordering::Relaxed);
    let drained_before_ceiling = drained >= backlog;
    (drained as f64 / elapsed.as_secs_f64(), drained_before_ceiling)
}

#[tokio::main]
async fn main() {
    let redis_url = env::var("BENCH_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let duration = Duration::from_secs(
        env::var("BENCH_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10),
    );

    println!("apparatus: autumn-harvest-redis standalone throughput bench (non-production)");
    println!("redis_url={redis_url} duration_per_cell={duration:?} queue=shared({QUEUE_NAME})");
    println!("concurrency,ops_completed_per_sec");

    for concurrency in [1usize, 4, 8, 16, 32, 64] {
        let ops_per_sec = sweep_one(concurrency, duration, &redis_url).await;
        println!("{concurrency},{ops_per_sec:.2}");
    }

    // Matched-workload comparison: drain a static 1,000-row backlog with 8
    // claim-only workers, the same shape as docs/performance.md's headline
    // Postgres cell (1,000 backlog, 8 concurrent claimers).
    let (claims_per_sec, drained) =
        backlog_drain_scenario(1_000, 8, Duration::from_secs(60), &redis_url).await;
    println!(
        "backlog_drain,backlog=1000,claimers=8,claims_per_sec={claims_per_sec:.2},drained_before_ceiling={drained}"
    );
}
