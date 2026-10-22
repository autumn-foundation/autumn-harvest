//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for the recorded-fork activity resolution that
//! `fork::serve_recorded_activities` runs on every decision of a fork
//! (issue #2000).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A source run fans out `FORK_RESOLVE_PROFILE_N` activities of one name in a
//! single decision, and each completes with a realistic record. The fork
//! repeats that decision under `ForkEffects::Recorded`. The harness then
//! resolves the whole scheduled batch through the public
//! `resolve_activity` and `settle_holds` entry points, in the same order
//! `serve_recorded_activities` does. The histories are built once, outside
//! the measured loop. `FORK_RESOLVE_PROFILE_REPS` (default 1) repeats the
//! resolution.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest \
//!   --bench fork_resolve_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_resolve_profile") | .executable')
//! FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::fork::{ForkEffects, ForkResolution, resolve_activity, settle_holds};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use serde_json::{Value, json};

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}")),
        Err(_) => default,
    }
}

fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer_id": i % 4096,
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
        "items": [{ "sku": format!("sku-{}", i % 512), "qty": 1 + i % 5 }],
    })
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn scheduled(id: ActivityExecId, i: usize) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: "charge".to_string(),
        input: payload(i),
        queue: "default".to_string(),
    }
}

fn main() {
    let n = env_usize("FORK_RESOLVE_PROFILE_N", 1_000);
    let reps = env_usize("FORK_RESOLVE_PROFILE_REPS", 1);

    let source_ids: Vec<_> = (0..n).map(|_| ActivityExecId::new()).collect();
    let fork_ids: Vec<_> = (0..n).map(|_| ActivityExecId::new()).collect();

    let mut source = vec![started()];
    source.extend(
        source_ids
            .iter()
            .enumerate()
            .map(|(i, id)| scheduled(*id, i)),
    );
    source.extend(
        source_ids
            .iter()
            .enumerate()
            .map(|(i, id)| WorkflowEvent::ActivityCompleted {
                activity_id: *id,
                output: payload(i),
            }),
    );
    source.push(WorkflowEvent::WorkflowCompleted {
        output: json!("done"),
    });

    let mut fork = vec![
        started(),
        WorkflowEvent::WorkflowForked {
            forked_from_exec_id: ExecutionId::new(),
            fork_event_id: 0,
            effects: ForkEffects::Recorded,
            reason: "profile".to_string(),
            operator_id: "profile".to_string(),
        },
    ];
    fork.extend(fork_ids.iter().enumerate().map(|(i, id)| scheduled(*id, i)));

    let mut served = 0_usize;
    for _ in 0..reps {
        let batch = fork_ids
            .iter()
            .map(|id| (*id, resolve_activity(&fork, &source, *id)))
            .collect();
        served += settle_holds(&fork, &source, batch)
            .iter()
            .filter(|(_, resolution)| matches!(resolution, ForkResolution::Serve(_)))
            .count();
    }
    assert_eq!(served, n * reps, "every fan-out activity must be served");
    println!("fork_resolve_profile: n={n} reps={reps} served={served}");
}
