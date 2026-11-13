//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `queue_coverage::partition_uncovered_and_paused`. That is the
//! pure, in-memory per-shard core of `GET /admin/queue-coverage` (issue
//! #774): the fleet-wide "which queues have pending work but zero live
//! pollers" deploy smoke-check.
//!
//! Wall-clock timing is not admissible evidence on this (shared-vCPU)
//! machine. Every number this harness produces is evidence of a
//! deterministic instruction count (`valgrind --tool=callgrind`) or an
//! allocation count/bytes figure (`valgrind --tool=dhat`).
//!
//! Callgrind counts are not bit-for-bit reproducible here, although every
//! queue and worker name is a fixed string (PR #1554). The default
//! `RandomState` of `std::collections::HashSet` reseeds every process, so
//! hash *values* differ run to run. Fixed keys only avoid the *additional*
//! variance of a `Uuid`-keyed map, which `dlq_aggregate_profile.rs` and
//! `run_chain_profile.rs` document.
//! On this machine, five callgrind runs of the indexed path spread over
//! 23,780,878-23,784,259 instructions (about 0.014%). Hash values do not
//! change the dhat allocation count or bytes, and repeated runs gave
//! identical figures. The "Measurement" section of
//! `docs/performance-queue-coverage.md` has the full before/after range.
//!
//! # Workload
//!
//! `observe_shard` calls this function once per inspected shard. The first
//! argument is every `PENDING`-backlog queue name on that shard
//! (`pending`). The second is every worker whose heartbeat is fresh and
//! status is `Active`/`Draining` on that shard's own connection
//! (`workers`). The third is the shard's paused-queue set.
//! For each pending queue, the pre-fix implementation asked
//! `workers.iter().any(|w| worker_covers_queue(w, &demand.queue_name,
//! shard_id))`. `worker_covers_queue` itself scans that worker's own
//! `queues` JSON array looking for a match. That is an O(pending queues x
//! workers x queues-per-worker) nested scan, run on every single request to
//! this endpoint.
//!
//! A large multi-tenant deployment stresses this shape. It has many
//! queues, one or a few per tenant, and a large fleet where each worker
//! polls a handful of queues. This harness builds
//! `QUEUE_COVERAGE_PROFILE_PENDING` (default 2 000) distinct pending queue
//! names and `QUEUE_COVERAGE_PROFILE_WORKERS` (default 1 000) workers. Each
//! worker polls `QUEUE_COVERAGE_PROFILE_QUEUES_PER_WORKER` (default 10)
//! queues from a "covered" sub-range. `QUEUE_COVERAGE_PROFILE_UNCOVERED`
//! (default 150) queue names have no worker. They are the typo'd or
//! undeployed queues that this endpoint exists to surface. Some covered and
//! some uncovered queue names are paused, so the paused-exclusion branch
//! runs too. This matches a real mixed fleet.
//!
//! An uncovered queue forces the pre-fix `.any()` scan over every worker,
//! because no covering worker short-circuits it. That is the worst case for
//! the O(n x m) shape. The `queue_coverage` module doc names it as the case
//! this endpoint detects, not an adversarial edge case.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
//!   --bench queue_coverage_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_coverage_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `QUEUE_COVERAGE_PROFILE_REPS` (default 1) repeats the whole build+call
//! cycle. One rep suffices for an input this size, as in
//! `dlq_aggregate_profile.rs` and `run_chain_profile.rs`.

use std::collections::BTreeSet;

use autumn_harvest::models::HarvestWorker;
use autumn_harvest::queue::PendingQueueDemand;
use autumn_harvest::workers::{WorkerHealth, WorkerRow, WorkerStatus};
use autumn_harvest_plugin::queue_coverage::partition_uncovered_and_paused;
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

/// Reads `key` as a `usize`. Only an *absent* variable gives `default`. A
/// malformed value panics.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

fn queue_name(i: usize) -> String {
    format!("tenant-{i:05}-jobs")
}

fn build_pending(num_pending: usize) -> Vec<PendingQueueDemand> {
    (0..num_pending)
        .map(|i| PendingQueueDemand {
            queue_name: queue_name(i),
            pending_count: 10,
            sample_task_ids: vec![Uuid::new_v4(), Uuid::new_v4()],
            sample_execution_ids: vec![Uuid::new_v4()],
        })
        .collect()
}

/// `num_workers` workers, each polling `queues_per_worker` queue names from
/// `0..covered_pool`, never from the uncovered range. An empty
/// `shard_assignments` array is the legacy single-shard registration. It
/// covers every shard, so the fixture suits the `shard_id = 0` call.
fn build_workers(
    num_workers: usize,
    covered_pool: usize,
    queues_per_worker: usize,
) -> Vec<WorkerRow> {
    (0..num_workers)
        .map(|i| {
            // Blocks of `queues_per_worker` names tile `0..covered_pool`
            // exactly; the caller asserts that. There are at least as many
            // workers as blocks, so every covered name has a worker.
            let start = (i * queues_per_worker) % covered_pool;
            let queues: Vec<String> = (0..queues_per_worker)
                .map(|j| queue_name((start + j) % covered_pool))
                .collect();
            WorkerRow {
                worker: HarvestWorker {
                    worker_id: format!("worker-{i:04}"),
                    started_at: at(0),
                    last_heartbeat_at: at(0),
                    queues: serde_json::json!(queues),
                    shard_assignments: serde_json::json!([]),
                    max_concurrency: 10,
                    in_flight_count: 0,
                    host: "host".to_string(),
                    version: None,
                    status: WorkerStatus::Active.as_str().to_string(),
                    drain_deadline_at: None,
                    build_id: String::new(),
                    deployment_name: None,
                    labels: serde_json::json!({}),
                    max_concurrent_sessions: 0,
                    in_use_sessions: 0,
                },
                health: WorkerHealth::Healthy,
                active_task_ids: Vec::new(),
            }
        })
        .collect()
}

fn main() {
    let num_pending = env_usize("QUEUE_COVERAGE_PROFILE_PENDING", 2_000);
    let num_workers = env_usize("QUEUE_COVERAGE_PROFILE_WORKERS", 1_000);
    let queues_per_worker = env_usize("QUEUE_COVERAGE_PROFILE_QUEUES_PER_WORKER", 10);
    let uncovered = env_usize("QUEUE_COVERAGE_PROFILE_UNCOVERED", 150);
    let reps = env_usize("QUEUE_COVERAGE_PROFILE_REPS", 1);

    assert!(
        num_pending > uncovered,
        "QUEUE_COVERAGE_PROFILE_PENDING ({num_pending}) must exceed \
         QUEUE_COVERAGE_PROFILE_UNCOVERED ({uncovered})"
    );
    assert!(
        queues_per_worker > 0,
        "QUEUE_COVERAGE_PROFILE_QUEUES_PER_WORKER must be at least 1"
    );
    // reps=0 measures only argument parsing, which can pass for a fast run.
    assert!(
        reps > 0,
        "QUEUE_COVERAGE_PROFILE_REPS must be at least 1, got 0"
    );

    let covered_pool = num_pending - uncovered;
    assert_eq!(
        covered_pool % queues_per_worker,
        0,
        "QUEUE_COVERAGE_PROFILE_PENDING - QUEUE_COVERAGE_PROFILE_UNCOVERED ({covered_pool}) must \
         be an exact multiple of QUEUE_COVERAGE_PROFILE_QUEUES_PER_WORKER ({queues_per_worker}) \
         so the covered range tiles evenly and every covered queue is genuinely covered"
    );
    assert!(
        num_workers >= covered_pool / queues_per_worker,
        "QUEUE_COVERAGE_PROFILE_WORKERS ({num_workers}) must be at least enough to cover every \
         block in the covered range ({} blocks)",
        covered_pool / queues_per_worker
    );

    // Three paused names are covered and two are uncovered. So each rep
    // runs both the "paused and covered" branch, which excludes silently,
    // and the `paused_uncovered` branch.
    let paused_indices = [50usize, 500, 1_000, covered_pool + 10, covered_pool + 40];
    let paused: BTreeSet<String> = paused_indices
        .into_iter()
        .filter(|i| *i < num_pending)
        .map(queue_name)
        .collect();
    // Count from the deduplicated `paused` set, not from `paused_indices`.
    // A custom size can make two paused indices collide. A raw count would
    // then disagree with the correct output of the function.
    let uncovered_range_names: BTreeSet<String> =
        (covered_pool..num_pending).map(queue_name).collect();
    let paused_in_uncovered_range = paused.intersection(&uncovered_range_names).count();
    let expected_uncovered_rows = uncovered - paused_in_uncovered_range;

    let mut total_uncovered_rows = 0usize;
    let mut total_paused_uncovered = 0usize;
    for _ in 0..reps {
        let pending = build_pending(num_pending);
        let workers = build_workers(num_workers, covered_pool, queues_per_worker);
        let (rows, paused_uncovered) =
            partition_uncovered_and_paused(pending, &workers, &paused, 0);
        // Self-check, so a later change cannot measure a wrong result: the
        // uncovered range, less its paused names, is uncovered on every rep.
        assert_eq!(
            rows.len(),
            expected_uncovered_rows,
            "expected {expected_uncovered_rows} uncovered rows from the reserved-uncovered range"
        );
        assert_eq!(
            paused_uncovered.len(),
            paused_in_uncovered_range,
            "expected {paused_in_uncovered_range} paused-and-uncovered queue names"
        );
        total_uncovered_rows += rows.len();
        total_paused_uncovered += paused_uncovered.len();
        std::hint::black_box((&rows, &paused_uncovered));
    }

    println!(
        "queue_coverage_profile: pending={num_pending} workers={num_workers} \
         queues_per_worker={queues_per_worker} uncovered_reserved={uncovered} reps={reps} \
         total_uncovered_rows={total_uncovered_rows} total_paused_uncovered={total_paused_uncovered}"
    );
}
