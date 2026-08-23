//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `SqliteRuntime`'s end-to-end decision-cycle driver
//! (`run_until_blocked`) -- the SQLite-backend analogue of
//! `autumn-harvest/benches/replay_profile.rs` (issue #135's replay budget),
//! but measuring a full *drive* (repeated replay + persist + inline
//! activity execution across N decision cycles), not one single
//! `replay_from_events` pass over a pre-built history.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs.
//!
//! `harness = false` + its own `main()` -- same shape as
//! `autumn-harvest/benches/replay_profile.rs` -- so the compiled artifact is
//! a plain executable a profiler can be pointed at directly.
//!
//! # Workload
//!
//! `sequential_workflow(ctx, n)` calls `ctx.execute_activity_raw` `n` times,
//! sequentially, each with a distinct `format!("activity_{i}")` name and a
//! realistic ~230-byte JSON payload (the same `activity_payload` shape
//! `replay_profile_support.rs` uses) as both the scheduled input and the
//! completed output. Both the per-iteration name formatting and the payload
//! shape deliberately mirror `replay_profile_support.rs::sequential_workflow`
//! exactly, so this is the SAME workload issue #135 budgets for the Postgres
//! replay path, driven through `SqliteRuntime::run_until_blocked` end-to-end
//! (start -> N decision cycles -> completion) rather than through a single
//! `replay_from_events` call over a pre-built history. Each decision cycle
//! re-loads and re-replays the ENTIRE history accumulated so far (this
//! backend has no warm-cache / delta-load path), so this is the realistic
//! full-run cost, not a single-cycle slice.
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
//! # Allocation counts/bytes:
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
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

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-loop-only body reads fine here; kept explicit for clarity.
//
// The per-iteration `format!("activity_{i}")` name (rather than a constant
// string) deliberately mirrors
// `autumn-harvest/benches/replay_profile_support.rs::sequential_workflow`
// exactly -- a constant name would understate the "author code re-executes
// on every replay cycle" cost this harness is explicitly compared against
// (issue #135), since the real comparison workload re-formats a distinct
// name string on every replay pass too.
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
            .start_workflow("sequential_workflow", json!(n as i64))
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
