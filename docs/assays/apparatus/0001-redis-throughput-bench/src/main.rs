// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It exercises
// autumn-harvest-redis's existing public API only; it does not modify or
// depend on any workspace crate other than that one.
//
// Measures sustained enqueue -> claim -> complete round trips/sec through
// RedisTaskQueue, at a swept worker-concurrency count, against a local Redis.
// Each worker owns a dedicated queue name so the number reflects server-side
// capacity rather than consumer-group fairness across workers.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest_redis::{EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType};

async fn run_worker(queue: RedisTaskQueue, queue_name: String, deadline: Instant, counter: Arc<AtomicU64>) {
    let payload = serde_json::json!({"to": "alice", "n": 1});
    loop {
        if Instant::now() >= deadline {
            return;
        }
        let params = EnqueueParams::new(&queue_name, TaskType::Activity, payload.clone());
        if queue.enqueue(params).await.is_err() {
            continue;
        }
        loop {
            match queue.claim(std::slice::from_ref(&queue_name), "bench-worker").await {
                Ok(Some(claimed)) => {
                    let _ = queue.complete(&claimed, serde_json::json!({"ok": true})).await;
                    counter.fetch_add(1, Ordering::Relaxed);
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
    let deadline = Instant::now() + duration;

    for i in 0..concurrency {
        let cfg = RedisTaskQueueConfig {
            key_prefix: "bench".to_string(),
            ..RedisTaskQueueConfig::default()
        };
        let queue = RedisTaskQueue::connect(redis_url, cfg)
            .await
            .expect("connect to local redis");
        let queue_name = format!("bench-worker-{i}");
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            run_worker(queue, queue_name, deadline, counter).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let total = counter.load(Ordering::Relaxed);
    total as f64 / duration.as_secs_f64()
}

#[tokio::main]
async fn main() {
    let redis_url = env::var("BENCH_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let duration = Duration::from_secs(
        env::var("BENCH_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10),
    );

    println!("apparatus: autumn-harvest-redis standalone throughput bench (non-production)");
    println!("redis_url={redis_url} duration_per_cell={duration:?}");
    println!("concurrency,ops_completed_per_sec");

    for concurrency in [1usize, 4, 8, 16, 32, 64] {
        let ops_per_sec = sweep_one(concurrency, duration, &redis_url).await;
        println!("{concurrency},{ops_per_sec:.2}");
    }
}
