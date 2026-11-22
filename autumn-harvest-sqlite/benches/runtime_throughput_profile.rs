//! Deterministic profiling harness for the throughput shape of
//! `SqliteRuntime`: many short runs against ONE open database.
//!
//! `runtime_drive_profile.rs` drives one long run on a fresh database. Its
//! cost is the history reload, and the one-time schema setup hides the cost
//! of each statement. This harness opens the database once and drives `M`
//! runs of `k` activities each. So the difference between two values of `M`
//! is the steady cost of one run, with the setup cancelled.
//!
//! It is `harness = false` with its own `main()`, as the drive harness is.
//! Run it under `valgrind --tool=callgrind` for instruction counts. On a file
//! database, run it under `strace -f -c -e trace=fsync,fdatasync` to count
//! the syncs of a run. Both counts are deterministic.
//!
//! The harness prints a wall-clock rate and latency percentiles too. Wall
//! clock varies on a shared machine, so treat those lines as context only.
//!
//! # Inputs
//!
//! - `SQLITE_THROUGHPUT_WORKFLOWS` (default `200`): the number of runs.
//! - `SQLITE_THROUGHPUT_ACTIVITIES` (default `3`): the activities of a run.
//! - `SQLITE_THROUGHPUT_DB` (default `memory`): `memory`, or a directory
//!   that receives a new database file.
//! - `SQLITE_THROUGHPUT_MODE` (default `blocked`): `blocked` starts a run and
//!   drives it with `run_until_blocked`. `idle` starts every run, then drives
//!   the fleet with `run_until_idle`.
//!
//! See `docs/performance-sqlite-hot-path.md` for the measured results.

use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
use serde_json::{Value, json};

/// The fixed activity names of one run, in call order.
const ACTIVITY_NAMES: [&str; 4] = ["reserve", "charge", "ship", "notify"];

/// An order line item, the payload shape of `runtime_drive_profile.rs`.
fn activity_payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer_id": i % 4096,
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
        "items": [
            {"sku": format!("SKU-{}", i % 512), "qty": 1 + (i % 5), "unit_cents": 999},
            {"sku": format!("SKU-{}", (i + 7) % 512), "qty": 1 + ((i + 3) % 5), "unit_cents": 1499},
        ],
        "status": "processing",
    })
}

#[workflow]
async fn order_workflow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let n: usize = usize::try_from(n).unwrap_or(0);
    let mut last = Value::Null;
    for i in 0..n {
        let name = ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()];
        last = ctx
            .execute_activity_raw(name, activity_payload(i), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(last)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn open_engine(db: &str) -> (SqliteRuntime, Option<std::path::PathBuf>) {
    if db == "memory" {
        let engine = SqliteRuntime::open_in_memory().expect("open in-memory sqlite db");
        return (engine, None);
    }
    let dir = std::path::PathBuf::from(db);
    std::fs::create_dir_all(&dir).expect("create the database directory");
    let path = dir.join(format!("throughput-{}.db", std::process::id()));
    let engine = SqliteRuntime::open(&path).expect("open the sqlite db file");
    (engine, Some(path))
}

fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = (sorted.len() * per_mille / 1000).min(sorted.len() - 1);
    sorted[index]
}

fn main() {
    let workflows = env_usize("SQLITE_THROUGHPUT_WORKFLOWS", 200);
    let activities = env_usize("SQLITE_THROUGHPUT_ACTIVITIES", 3);
    let db = std::env::var("SQLITE_THROUGHPUT_DB").unwrap_or_else(|_| "memory".to_string());
    let mode = std::env::var("SQLITE_THROUGHPUT_MODE").unwrap_or_else(|_| "blocked".to_string());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");

    let (mut engine, path) = open_engine(&db);
    engine.register_workflow(&order_workflow_info());
    for name in ACTIVITY_NAMES {
        engine.register_activity_raw(name, ActivitySpec::new(1, |input: Value| Ok(input)));
    }
    let input = json!(i64::try_from(activities).unwrap_or(i64::MAX));

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(workflows);
    if mode == "idle" {
        let execs: Vec<_> = (0..workflows)
            .map(|_| {
                engine
                    .start_workflow("order_workflow", input.clone())
                    .expect("start_workflow")
            })
            .collect();
        rt.block_on(engine.run_until_idle())
            .expect("run_until_idle");
        for exec in execs {
            let outcome = engine.outcome(exec).expect("outcome");
            assert!(matches!(outcome, ExecutionOutcome::Completed(_)));
        }
    } else {
        for _ in 0..workflows {
            let one = Instant::now();
            let exec = engine
                .start_workflow("order_workflow", input.clone())
                .expect("start_workflow");
            let state = rt
                .block_on(engine.run_until_blocked(exec))
                .expect("run_until_blocked");
            assert!(
                matches!(state, RunState::Completed(_)),
                "a run must complete for a profile run to be meaningful: {state:?}"
            );
            latencies.push(one.elapsed());
        }
    }
    let elapsed = started.elapsed();
    std::hint::black_box(&engine);
    drop(engine);
    if let Some(path) = path {
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let mut file = path.clone().into_os_string();
            file.push(suffix);
            let _ = std::fs::remove_file(file);
        }
    }

    latencies.sort_unstable();
    let rate = workflows as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    println!(
        "sqlite_runtime_throughput_profile: db={} mode={mode} workflows={workflows} \
         activities={activities}",
        if db == "memory" { "memory" } else { "file" },
    );
    println!(
        "wall_clock (context only): elapsed={elapsed:?} runs_per_sec={rate:.0} \
         p50={:?} p99={:?}",
        percentile(&latencies, 500),
        percentile(&latencies, 990),
    );
}
