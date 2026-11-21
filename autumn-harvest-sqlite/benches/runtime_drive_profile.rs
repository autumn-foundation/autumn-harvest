//! Deterministic instruction/allocation-count profiling harness for
//! `run_until_blocked`, the decision-cycle driver of `SqliteRuntime`.
//!
//! It is the SQLite-backend analogue of `autumn-harvest/benches/replay_profile.rs`
//! (the replay budget of issue #135). It measures a full *drive*: repeated
//! replay, persist, and inline activity execution across N decision cycles. It
//! does not measure one `replay_from_events` pass over a pre-built history.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs.
//!
//! `harness = false` and its own `main()` match the shape of
//! `autumn-harvest/benches/replay_profile.rs`. So the compiled artifact is a
//! plain executable that a profiler can run directly.
//!
//! # Workload
//!
//! `sequential_workflow(ctx, n)` calls `ctx.execute_activity_raw` `n` times in
//! sequence. Each call has a distinct `format!("activity_{i}")` name. A
//! realistic ~230-byte JSON payload (the `activity_payload` shape of
//! `replay_profile_support.rs`) is both the scheduled input and the output.
//! The name formatting and the payload shape mirror
//! `replay_profile_support.rs::sequential_workflow` exactly. So this is the
//! SAME workload that issue #135 budgets for the Postgres replay path. Here it
//! runs through `SqliteRuntime::run_until_blocked` end-to-end (start -> N
//! decision cycles -> completion). Each decision cycle re-loads and re-replays
//! the ENTIRE history so far. This backend has no warm-cache / delta-load path.
//! So this is the realistic full-run cost, not a single-cycle slice.
//!
//! `SQLITE_RUNTIME_PROFILE_N` (default `100`) sets the activity count.
//! `SQLITE_RUNTIME_PROFILE_REPS` (default `1`) repeats the whole
//! register+start+drive cycle against a fresh in-memory database each time.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-sqlite --bench runtime_drive_profile \
//!   --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable')
//!
//! # Instruction counts:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
//!   --callgrind-out-file=callgrind.out "$BIN"
//! callgrind_annotate --threshold=100 callgrind.out
//!
//! # Allocation counts/bytes -- `--num-callers=30` is REQUIRED: the default
//! # depth (12) truncates `serde_json::Value::deserialize`'s recursive
//! # descent before it reaches the real Rust caller for a large share of
//! # allocations (see `docs/performance-sqlite-runtime-drive.md`'s
//! # "Allocation-site attribution" Methodology note for the measured effect
//! # of getting this wrong):
//! valgrind --tool=dhat --dhat-out-file=dhat.json --num-callers=30 "$BIN"
//! ```

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
use serde_json::{Value, json};

/// A representative per-activity input/output record -- an order line item.
/// Mirrors `autumn-harvest/benches/replay_profile_support.rs::activity_payload`
/// exactly, so the two harnesses measure comparable per-event payload cost.
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

// The per-iteration `format!("activity_{i}")` name mirrors
// `autumn-harvest/benches/replay_profile_support.rs::sequential_workflow`
// exactly. That workload re-formats a distinct name on every replay pass too.
// A constant name would understate the "author code re-executes on every
// replay cycle" cost that issue #135 compares against.
#[workflow]
async fn sequential_workflow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let n: usize = usize::try_from(n).unwrap_or(0);
    let mut last = Value::Null;
    for i in 0..n {
        let name = format!("activity_{i}");
        last = ctx
            .execute_activity_raw(&name, activity_payload(i), "default")
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

fn main() {
    let n = env_usize("SQLITE_RUNTIME_PROFILE_N", 100);
    let reps = env_usize("SQLITE_RUNTIME_PROFILE_REPS", 1);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");

    let mut total_activities = 0usize;
    for _ in 0..reps {
        let mut engine = SqliteRuntime::open_in_memory().expect("open in-memory sqlite db");
        engine.register_workflow(&sequential_workflow_info());
        // One registration per `activity_{i}` name, matching the workflow's
        // per-iteration `format!("activity_{i}")` call (see the workflow doc
        // comment above for why the name varies per iteration).
        for i in 0..n {
            engine.register_activity_raw(
                format!("activity_{i}"),
                ActivitySpec::new(1, |input: Value| Ok(input)),
            );
        }
        let exec = engine
            .start_workflow(
                "sequential_workflow",
                json!(i64::try_from(n).unwrap_or(i64::MAX)),
            )
            .expect("start_workflow");

        let state = rt
            .block_on(engine.run_until_blocked(exec))
            .expect("run_until_blocked");
        assert!(
            matches!(state, RunState::Completed(_)),
            "workflow must complete for a profile run to be meaningful: {state:?}"
        );
        let outcome = engine.outcome(exec).expect("outcome");
        assert!(matches!(outcome, ExecutionOutcome::Completed(_)));
        total_activities += n;
        std::hint::black_box(&engine);
    }

    println!(
        "sqlite_runtime_drive_profile: n={n} reps={reps} total_activities_driven={total_activities}"
    );
}
