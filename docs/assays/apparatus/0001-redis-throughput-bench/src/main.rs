// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It exercises
// autumn-harvest-redis's existing public API only; it does not modify or
// depend on any workspace crate other than that one.
//
// Measures sustained enqueue -> claim -> complete round trips/sec through
// RedisTaskQueue, at a swept worker-concurrency count, against a local Redis.
// All workers share one queue name, the pre-registered condition. Each worker
// has its own consumer identity. So the number reflects consumer-group
// contention on one shared stream, not N independent streams.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest_redis::{EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType};
use tokio::sync::Barrier;

const QUEUE_NAME: &str = "bench-shared";

/// Returns a fresh key prefix per scenario invocation. A rerun or the next
/// cell never inherits a leftover stream, consumer group or un-acked entry.
/// With a fixed prefix, stale state bleeds from one measurement into the
/// next. For example, a steady-state cell can hit its deadline with an
/// unclaimed entry on the stream. The next cell, or the backlog-drain
/// scenario, then sees a false "seed exactly N" precondition. Mirrors the
/// crate's own integration tests
/// (`format!("test_{}", uuid::Uuid::new_v4().simple())`), without a `uuid`
/// dependency.
fn unique_prefix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_nanos();
    format!("bench-{}-{nanos}-{n}", std::process::id())
}

async fn run_worker(
    queue: RedisTaskQueue,
    worker_id: String,
    start_gate: Arc<Barrier>,
    duration: Duration,
    counter: Arc<AtomicU64>,
) {
    // All workers connect before any worker enqueues. So every worker
    // measures the same window, and no worker runs solo while later ones
    // still connect.
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
    let prefix = unique_prefix();
    let mut handles = Vec::with_capacity(concurrency);
    let counter = Arc::new(AtomicU64::new(0));
    // +1: main also waits at the gate, so the timer starts only once every
    // worker has a live connection and is parked on `wait()`.
    let start_gate = Arc::new(Barrier::new(concurrency + 1));

    for i in 0..concurrency {
        let cfg = RedisTaskQueueConfig {
            key_prefix: prefix.clone(),
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

/// Drain a pre-seeded, static backlog with claim-only workers. No worker
/// enqueues once the seed is in place. This is closer to
/// `docs/performance.md`'s methodology than the steady-state loop, which
/// keeps the queue near-empty. It is still NOT a matched reproduction of
/// that harness. `claim_bench_support.rs` spreads its backlog across 4
/// queues and caps claims at `backlog / MAX_DRAIN_FRACTION` (= 5). Its
/// backlog stays at 80-100% of the seeded depth. This function uses one
/// queue and drains it to zero. Assay #1's report, "post-review correction
/// #2", says why this gap stays: a true match must reuse or port that
/// harness's scenario shape. The numbers are exploratory and are never
/// compared to the Postgres control by a ratio.
/// Returns this worker's own (start, end) instants. `Barrier` does not
/// guarantee the order in which released tasks resume. This scenario is short
/// (~80ms observed). So a timestamp that `main` takes after *its own*
/// `wait()` can lag a worker that already resumed and claims. That lag shrinks the
/// denominator and inflates the rate. Each worker times itself instead. The
/// caller folds all windows into `min(start)..max(end)`, as the harness of
/// `docs/performance.md` does.
async fn run_drain_worker(
    queue: RedisTaskQueue,
    worker_id: String,
    start_gate: Arc<Barrier>,
    backlog: u64,
    completed: Arc<AtomicU64>,
    ceiling: Duration,
) -> (Instant, Instant) {
    start_gate.wait().await;
    let my_start = Instant::now();
    let deadline = my_start + ceiling;
    loop {
        if completed.load(Ordering::Relaxed) >= backlog || Instant::now() >= deadline {
            break;
        }
        match queue.claim(std::slice::from_ref(&QUEUE_NAME.to_string()), &worker_id).await {
            Ok(Some(claimed)) => {
                if queue.complete(&claimed, serde_json::json!({"ok": true})).await.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(None) => {
                if completed.load(Ordering::Relaxed) >= backlog {
                    break;
                }
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            Err(_) => break,
        }
    }
    (my_start, Instant::now())
}

/// Returns (claims_per_sec, drained_before_ceiling).
async fn backlog_drain_scenario(
    backlog: u64,
    concurrency: usize,
    ceiling: Duration,
    redis_url: &str,
) -> (f64, bool) {
    let prefix = unique_prefix();
    let seed_cfg = RedisTaskQueueConfig {
        key_prefix: prefix.clone(),
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
            key_prefix: prefix.clone(),
            ..RedisTaskQueueConfig::default()
        };
        let queue = RedisTaskQueue::connect(redis_url, cfg)
            .await
            .expect("connect to local redis");
        let worker_id = format!("bench-drain-worker-{i}");
        let completed = completed.clone();
        let start_gate = start_gate.clone();
        handles.push(tokio::spawn(async move {
            run_drain_worker(queue, worker_id, start_gate, backlog, completed, ceiling).await
        }));
    }

    start_gate.wait().await;
    let mut window: Option<(Instant, Instant)> = None;
    for h in handles {
        if let Ok((start, end)) = h.await {
            window = Some(match window {
                None => (start, end),
                Some((min_start, max_end)) => (min_start.min(start), max_end.max(end)),
            });
        }
    }
    let (min_start, max_end) = window.expect("at least one worker");
    let elapsed = max_end.duration_since(min_start);

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

    // Exploratory only. This is NOT a matched reproduction of the harness of
    // docs/performance.md (see run_drain_worker and assay #1's report,
    // "post-review correction #2"). That harness spreads its backlog across
    // 4 queues and claims only backlog/5, so depth holds at 80-100%. This
    // drains one queue to zero. Derive no ratio against the Postgres control.
    let (claims_per_sec, drained) =
        backlog_drain_scenario(1_000, 8, Duration::from_secs(60), &redis_url).await;
    println!(
        "backlog_drain,backlog=1000,claimers=8,claims_per_sec={claims_per_sec:.2},drained_before_ceiling={drained}"
    );
}
