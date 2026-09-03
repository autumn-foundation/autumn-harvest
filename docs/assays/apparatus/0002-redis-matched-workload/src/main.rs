// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It exercises
// autumn-harvest-redis's existing public API only; it does not modify or
// depend on any workspace crate other than that one.
//
// Reproduces docs/performance.md's "Claim latency vs backlog depth" scenario
// shape against RedisTaskQueue instead of Postgres: 8 concurrent claimers,
// backlog spread round-robin across 4 queues, a bounded-fraction claim-only
// draw (never more than a fifth of the seeded backlog), split exactly across
// claimers. The scenario constants and helper functions below are ported by
// value from `autumn-harvest/tests/integration/claim_bench_support.rs`
// (`measured_claims_for`, `claims_for_claimer`, `measured_window`), not
// re-derived from the docs prose, so this apparatus measures the same shape
// docs/performance.md's table does. See
// ../../0002-redis-matched-workload-vs-postgres.md for the pre-registration
// this apparatus was built to answer.

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest_redis::{
    EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType,
};
use tokio::sync::Barrier;

/// Ported from `claim_bench_support.rs::MAX_DRAIN_FRACTION`.
const MAX_DRAIN_FRACTION: usize = 5;
/// Ported from `claim_bench_support.rs::MAX_MEASURED_CLAIMS`.
const MAX_MEASURED_CLAIMS: usize = 800;

const QUEUES: usize = 4;
const CLAIMERS: usize = 8;

/// Ported verbatim from `claim_bench_support.rs::measured_claims_for`.
const fn measured_claims_for(backlog: usize) -> usize {
    let by_fraction = backlog / MAX_DRAIN_FRACTION;
    let capped = if by_fraction > MAX_MEASURED_CLAIMS {
        MAX_MEASURED_CLAIMS
    } else {
        by_fraction
    };
    if capped == 0 { 1 } else { capped }
}

/// Ported verbatim from `claim_bench_support.rs::claims_for_claimer`.
const fn claims_for_claimer(total_ops: usize, claimers: usize, index: usize) -> usize {
    if claimers == 0 {
        return 0;
    }
    let base = total_ops / claimers;
    let remainder = total_ops % claimers;
    if index < remainder { base + 1 } else { base }
}

const WARMUP_DIVISOR: usize = 10;

/// Ported verbatim from `claim_bench_support.rs::warmup_claims_for`.
const fn warmup_claims_for(collected: usize) -> usize {
    collected / WARMUP_DIVISOR
}

/// Ported verbatim (in spirit) from `claim_bench_support.rs::measured_window`:
/// earliest resume to latest finish across every claimer's own clock.
fn measured_window(spans: &[(Instant, Instant)]) -> f64 {
    let Some(start) = spans.iter().map(|&(s, _)| s).min() else {
        return 0.0;
    };
    let end = spans.iter().map(|&(_, e)| e).max().unwrap_or(start).max(start);
    end.duration_since(start).as_secs_f64()
}

fn unique_prefix(backlog: usize) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_nanos();
    format!("matchbench-{backlog}-{}-{nanos}", std::process::id())
}

fn queue_names(prefix: &str) -> Vec<String> {
    (0..QUEUES).map(|q| format!("{prefix}-q-{q}")).collect()
}

/// Seed `backlog` rows round-robin across `QUEUES` queues, mirroring
/// `claim_bench_support.rs::seed_backlog`'s `queue_name = prefix-q-(i % queues)`
/// assignment. Empty JSON input, matching the Postgres seed's `'{}'::jsonb`.
async fn seed_backlog(redis_url: &str, prefix: &str, backlog: usize) {
    let cfg = RedisTaskQueueConfig {
        key_prefix: prefix.to_string(),
        ..RedisTaskQueueConfig::default()
    };
    let seeder = RedisTaskQueue::connect(redis_url, cfg)
        .await
        .expect("connect to local redis for seeding");
    let names = queue_names(prefix);
    let payload = serde_json::json!({});
    for i in 0..backlog {
        let queue_name = &names[i % QUEUES];
        let params = EnqueueParams::new(queue_name.clone(), TaskType::Activity, payload.clone());
        seeder.enqueue(params).await.expect("seed enqueue");
    }
}

struct ClaimerOutcome {
    /// (latency_ms, claimed_a_task) per attempted claim, in order.
    observed: Vec<(f64, bool)>,
    span: (Instant, Instant),
}

/// One claimer: `per_claimer` claim-only attempts against all `QUEUES` queue
/// names, no enqueue and no complete/ack during the measured phase (matches
/// what `docs/performance.md`'s table itself measures: `claim_task` alone).
async fn run_claimer(
    queue: RedisTaskQueue,
    worker_id: String,
    queues: Arc<Vec<String>>,
    start_gate: Arc<Barrier>,
    per_claimer: usize,
    deadline: Instant,
) -> ClaimerOutcome {
    start_gate.wait().await;
    let my_start = Instant::now();
    let mut observed = Vec::with_capacity(per_claimer);
    for _ in 0..per_claimer {
        if Instant::now() >= deadline {
            break;
        }
        let call_start = Instant::now();
        match queue.claim(&queues, &worker_id).await {
            Ok(Some(_claimed)) => {
                let ms = call_start.elapsed().as_secs_f64() * 1000.0;
                observed.push((ms, true));
            }
            Ok(None) => {
                let ms = call_start.elapsed().as_secs_f64() * 1000.0;
                observed.push((ms, false));
            }
            Err(_) => break,
        }
    }
    ClaimerOutcome { observed, span: (my_start, Instant::now()) }
}

struct CellReport {
    backlog: usize,
    /// Post-warmup sample count (the "n" column in docs/performance.md's table).
    n: usize,
    /// Post-warmup calls that returned a task.
    claimed: usize,
    /// Post-warmup calls that returned `None`.
    empty: usize,
    /// Every successful claim, warmup included — the throughput numerator
    /// (ported from `ClaimerOutcome::total_claimed`'s definition).
    total_claimed: usize,
    wall_secs: f64,
    claims_per_sec: f64,
    truncated: bool,
    p50_ms: f64,
    p99_ms: f64,
}

fn percentile(mut xs: Vec<f64>, p: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((xs.len() as f64) * p).ceil() as usize;
    let idx = idx.saturating_sub(1).min(xs.len() - 1);
    xs[idx]
}

async fn run_cell(backlog: usize, redis_url: &str, scenario_budget: Duration) -> CellReport {
    let prefix = unique_prefix(backlog);
    seed_backlog(redis_url, &prefix, backlog).await;

    let total_ops = measured_claims_for(backlog);
    let queues = Arc::new(queue_names(&prefix));
    let start_gate = Arc::new(Barrier::new(CLAIMERS + 1));
    let deadline = Instant::now() + scenario_budget;

    let mut handles = Vec::with_capacity(CLAIMERS);
    for c in 0..CLAIMERS {
        let per_claimer = claims_for_claimer(total_ops, CLAIMERS, c);
        let cfg = RedisTaskQueueConfig {
            key_prefix: prefix.clone(),
            ..RedisTaskQueueConfig::default()
        };
        let queue = RedisTaskQueue::connect(redis_url, cfg)
            .await
            .expect("connect to local redis");
        let worker_id = format!("matchbench-worker-{c}");
        let queues = Arc::clone(&queues);
        let start_gate = Arc::clone(&start_gate);
        handles.push(tokio::spawn(async move {
            run_claimer(queue, worker_id, queues, start_gate, per_claimer, deadline).await
        }));
    }

    start_gate.wait().await;

    // Mirrors `ClaimerOutcome::from_observed`: per claimer, the head
    // `collected / WARMUP_DIVISOR` observations are warmup and are dropped
    // from the latency samples and the post-warmup claimed/empty counts, but
    // `total_claimed` (the throughput numerator) counts every successful
    // claim including warmup, because `wall_secs` starts at the first
    // warmup call too.
    let mut spans = Vec::with_capacity(CLAIMERS);
    let mut samples = Vec::with_capacity(total_ops);
    let mut claimed = 0usize;
    let mut empty = 0usize;
    let mut total_claimed = 0usize;
    let mut total_planned = 0usize;
    let mut total_collected = 0usize;
    for h in handles {
        let outcome = h.await.expect("claimer task panicked");
        spans.push(outcome.span);
        let collected = outcome.observed.len();
        total_collected += collected;
        total_claimed += outcome.observed.iter().filter(|(_, got)| *got).count();
        let warmup = warmup_claims_for(collected);
        for (ms, got) in outcome.observed.into_iter().skip(warmup) {
            samples.push(ms);
            if got {
                claimed += 1;
            } else {
                empty += 1;
            }
        }
        total_planned += 1;
    }
    let _ = total_planned;

    let wall_secs = measured_window(&spans);
    let truncated = total_collected < total_ops;
    let claims_per_sec = if wall_secs <= 0.0 { 0.0 } else { total_claimed as f64 / wall_secs };

    CellReport {
        backlog,
        n: samples.len(),
        claimed,
        empty,
        total_claimed,
        wall_secs,
        claims_per_sec,
        truncated,
        p50_ms: percentile(samples.clone(), 0.50),
        p99_ms: percentile(samples, 0.99),
    }
}

#[tokio::main]
async fn main() {
    let redis_url = env::var("BENCH_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let scenario_secs: u64 = env::var("BENCH_SCENARIO_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let scenario_budget = Duration::from_secs(scenario_secs);

    let backlogs: Vec<usize> = env::var("BENCH_BACKLOGS")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .filter(|v: &Vec<usize>| !v.is_empty())
        .unwrap_or_else(|| vec![1_000, 10_000, 100_000]);

    println!("apparatus: autumn-harvest-redis matched-workload bench vs docs/performance.md (non-production)");
    println!(
        "redis_url={redis_url} claimers={CLAIMERS} queues={QUEUES} scenario_budget={scenario_budget:?}"
    );
    println!("backlog,n,claimed,empty,total_claimed,wall_secs,claims_per_sec,p50_ms,p99_ms,truncated");

    for backlog in backlogs {
        let r = run_cell(backlog, &redis_url, scenario_budget).await;
        println!(
            "{},{},{},{},{},{:.3},{:.2},{:.3},{:.3},{}",
            r.backlog,
            r.n,
            r.claimed,
            r.empty,
            r.total_claimed,
            r.wall_secs,
            r.claims_per_sec,
            r.p50_ms,
            r.p99_ms,
            r.truncated
        );
    }
}
