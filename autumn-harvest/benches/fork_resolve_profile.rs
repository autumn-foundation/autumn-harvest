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
//! A fork decision schedules `FORK_RESOLVE_PROFILE_N` activities of one name.
//! The harness resolves the whole batch through the public
//! `resolve_activity` and `settle_holds` entry points, in the same order
//! `serve_recorded_activities` does. `FORK_RESOLVE_PROFILE_SHAPE` picks the
//! source and fork histories:
//!
//! - `serve` (default): the source fans out the batch in one decision, and
//!   each activity completes with a realistic record. Every activity is
//!   served from its record.
//! - `hold`: the source runs `race(join_all(batch), timer)`, and the timer
//!   wins. Each activity has the race-loser terminal. The fork repeats the
//!   race, and its timer is still pending. Every activity is held.
//! - `scope`: a signal cancels a cancellation scope that holds the batch.
//!   Each activity has the scope-cancel terminal. No sibling wins, so the
//!   fork cannot prove a winner. Every activity fails closed as unavailable.
//! - `override`: as `serve`, and the fork also carries one override for each
//!   activity, up to `MAX_ACTIVITY_OVERRIDES`. Every covered activity is
//!   served from its override.
//! - `miss`: as `serve`, but the fork schedules each activity with another
//!   currency. No record matches, so every activity fails as unavailable.
//!
//! The histories are built once, outside the measured loop. So the inclusive
//! cost of the two entry points leaves the setup out.
//! `FORK_RESOLVE_PROFILE_REPS` (default 1) repeats the resolution.
//!
//! The `serve` shape keeps random activity ids, so its total stays
//! comparable with `docs/performance-fork-scheduled-occurrence.md`. The
//! other shapes use fixed ids.
//!
//! An edit of this file can move the `serve` total by a fraction of a
//! percent. LLVM then inlines the setup and drop code of the harness in
//! another way. To compare two builds, use the inclusive cost of the entry
//! points. See `docs/performance-fork-resolve-shapes.md`.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest \
//!   --bench fork_resolve_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_resolve_profile") | .executable')
//! FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! FORK_RESOLVE_PROFILE_SHAPE=hold FORK_RESOLVE_PROFILE_N=400 valgrind \
//!   --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::fork::{ForkEffects, ForkResolution, resolve_activity, settle_holds};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Serve,
    Hold,
    Scope,
    Override,
    Miss,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("FORK_RESOLVE_PROFILE_SHAPE") else {
            return Self::Serve;
        };
        match value.as_str() {
            "serve" => Self::Serve,
            "hold" => Self::Hold,
            "scope" => Self::Scope,
            "override" => Self::Override,
            "miss" => Self::Miss,
            other => panic!(
                "FORK_RESOLVE_PROFILE_SHAPE must be serve, hold, scope, override or miss, \
                 got {other:?}"
            ),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::Hold => "hold",
            Self::Scope => "scope",
            Self::Override => "override",
            Self::Miss => "miss",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
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

/// The shapes other than `serve`, with their own builders and checks.
///
/// The `serve` code in `main` builds the workload that
/// `docs/performance-fork-scheduled-occurrence.md` measured, kept as it was.
/// Only its check is stricter: it accepts a completion with the fork id, not
/// any served event. This module holds the rest, so the new code does not
/// touch that workload.
mod shapes {
    use autumn_harvest::context::LoserCancelReason;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::fork::{
        ERROR_TYPE_FORK_EFFECT_UNAVAILABLE, ForkEffects, ForkResolution, MAX_ACTIVITY_OVERRIDES,
        resolve_activity, settle_holds,
    };
    use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{Shape, payload};

    /// The first fixed id of a source activity.
    const SOURCE_ID_BASE: u128 = 0x5000_0000_0000_0000_0000_0000_0000_0000;

    /// The first fixed id of a fork activity.
    const FORK_ID_BASE: u128 = 0xf000_0000_0000_0000_0000_0000_0000_0000;

    /// The timer that races the batch in the `hold` shape.
    const RACE_TIMER: &str = "deadline";

    /// The source and fork histories of one shape, and the fork batch.
    struct Workload {
        source: Vec<WorkflowEvent>,
        fork: Vec<WorkflowEvent>,
        fork_ids: Vec<ActivityExecId>,
    }

    /// The input that a `miss` fork schedules. Only the currency differs.
    fn changed_payload(i: usize) -> Value {
        json!({
            "order_id": format!("order-{i:08}"),
            "customer_id": i % 4096,
            "amount_cents": (i as u64 * 137) % 1_000_000,
            "currency": "EUR",
            "items": [{ "sku": format!("sku-{}", i % 512), "qty": 1 + i % 5 }],
        })
    }

    /// The output that the `override` fork sets for activity `i`.
    fn override_output(i: usize) -> Value {
        json!({
            "order_id": format!("order-{i:08}"),
            "status": "overridden",
            "amount_cents": 0,
        })
    }

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: json!({}),
            timestamp: chrono::DateTime::from_timestamp(1_790_000_000, 0)
                .expect("a fixed timestamp is in range"),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(id: ActivityExecId, input: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "charge".to_string(),
            input,
            queue: "default".to_string(),
        }
    }

    fn marker() -> WorkflowEvent {
        WorkflowEvent::WorkflowForked {
            forked_from_exec_id: ExecutionId::from_uuid(Uuid::from_u128(1)),
            fork_event_id: 0,
            effects: ForkEffects::Recorded,
            reason: "profile".to_string(),
            operator_id: "profile".to_string(),
        }
    }

    /// The terminal that `worker::apply_race_loser_cancellations` writes.
    fn loser(id: ActivityExecId, reason: LoserCancelReason) -> WorkflowEvent {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: reason.message().to_string(),
            attempt: 1,
            error_type: "Error".to_string(),
            non_retryable: true,
            details: None,
        }
    }

    fn race_timer() -> WorkflowEvent {
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(RACE_TIMER),
            duration_secs: 30,
        }
    }

    fn fixed_ids(base: u128, n: usize) -> Vec<ActivityExecId> {
        (0..n)
            .map(|i| ActivityExecId::from_uuid(Uuid::from_u128(base + i as u128)))
            .collect()
    }

    /// The scheduled events of one decision, one per id, with `input(i)`.
    fn batch(
        ids: &[ActivityExecId],
        input: fn(usize) -> Value,
    ) -> impl Iterator<Item = WorkflowEvent> + '_ {
        ids.iter()
            .enumerate()
            .map(move |(i, id)| scheduled(*id, input(i)))
    }

    /// A source that schedules the batch in one decision. Each activity
    /// completes with its input as output.
    fn completed_source(source_ids: &[ActivityExecId]) -> Vec<WorkflowEvent> {
        let mut source = vec![started()];
        source.extend(batch(source_ids, payload));
        source.extend(source_ids.iter().enumerate().map(|(i, id)| {
            WorkflowEvent::ActivityCompleted {
                activity_id: *id,
                output: payload(i),
            }
        }));
        source.push(WorkflowEvent::WorkflowCompleted {
            output: json!("done"),
        });
        source
    }

    /// A fork prefix, the given overrides, then the batch with `input(i)`.
    fn fork_history(
        overrides: Vec<WorkflowEvent>,
        fork_ids: &[ActivityExecId],
        input: fn(usize) -> Value,
    ) -> Vec<WorkflowEvent> {
        let mut fork = vec![started(), marker()];
        fork.extend(overrides);
        fork.extend(batch(fork_ids, input));
        fork
    }

    /// The source runs `race(join_all(batch), timer)`, and the timer fires
    /// first. One decision schedules the batch and then the timer.
    fn hold_workload(n: usize) -> Workload {
        let source_ids = fixed_ids(SOURCE_ID_BASE, n);
        let fork_ids = fixed_ids(FORK_ID_BASE, n);
        let mut source = vec![started()];
        source.extend(batch(&source_ids, payload));
        source.push(race_timer());
        source.push(WorkflowEvent::TimerFired {
            timer_id: TimerId::new(RACE_TIMER),
        });
        source.extend(
            source_ids
                .iter()
                .map(|id| loser(*id, LoserCancelReason::RaceLoser)),
        );
        source.push(WorkflowEvent::WorkflowCompleted {
            output: json!("timed out"),
        });
        let mut fork = fork_history(Vec::new(), &fork_ids, payload);
        fork.push(race_timer());
        Workload {
            source,
            fork,
            fork_ids,
        }
    }

    /// A signal makes the source cancel the scope that holds the batch. The
    /// cancel records its marker, then one scope-cancel terminal per member.
    fn scope_workload(n: usize) -> Workload {
        let source_ids = fixed_ids(SOURCE_ID_BASE, n);
        let fork_ids = fixed_ids(FORK_ID_BASE, n);
        let mut source = vec![started()];
        source.extend(batch(&source_ids, payload));
        source.push(WorkflowEvent::SignalReceived {
            signal_name: "cancel".to_string(),
            payload: json!({}),
        });
        source.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:0".to_string(),
            details: json!({
                "reason": "cancellation scope cancelled",
                "horizon": n + 2,
            }),
        });
        source.extend(
            source_ids
                .iter()
                .map(|id| loser(*id, LoserCancelReason::ScopeCancelled)),
        );
        source.push(WorkflowEvent::WorkflowCompleted {
            output: json!("cancelled"),
        });
        Workload {
            source,
            fork: fork_history(Vec::new(), &fork_ids, payload),
            fork_ids,
        }
    }

    /// The fork request sets one override per activity, in occurrence order.
    fn override_workload(n: usize) -> Workload {
        let source_ids = fixed_ids(SOURCE_ID_BASE, n);
        let fork_ids = fixed_ids(FORK_ID_BASE, n);
        let overrides = (0..n.min(MAX_ACTIVITY_OVERRIDES))
            .map(|i| WorkflowEvent::ForkActivityResultOverridden {
                activity_name: "charge".to_string(),
                occurrence: u32::try_from(i + 1).expect("an occurrence fits in u32"),
                output: override_output(i),
            })
            .collect();
        Workload {
            source: completed_source(&source_ids),
            fork: fork_history(overrides, &fork_ids, payload),
            fork_ids,
        }
    }

    fn miss_workload(n: usize) -> Workload {
        let source_ids = fixed_ids(SOURCE_ID_BASE, n);
        let fork_ids = fixed_ids(FORK_ID_BASE, n);
        Workload {
            source: completed_source(&source_ids),
            fork: fork_history(Vec::new(), &fork_ids, changed_payload),
            fork_ids,
        }
    }

    fn is_unavailable(id: ActivityExecId, resolution: &ForkResolution) -> bool {
        matches!(
            resolution,
            ForkResolution::Serve(WorkflowEvent::ActivityFailed {
                activity_id,
                error_type,
                non_retryable: true,
                ..
            }) if *activity_id == id && error_type == ERROR_TYPE_FORK_EFFECT_UNAVAILABLE
        )
    }

    /// Whether `resolution` serves the output that the `override` fork sets
    /// for activity `i`. An activity past the cap takes its record.
    fn served_by_override(i: usize, id: ActivityExecId, resolution: &ForkResolution) -> bool {
        let ForkResolution::Serve(WorkflowEvent::ActivityCompleted {
            activity_id,
            output,
        }) = resolution
        else {
            return false;
        };
        let from_override = output
            .get("status")
            .is_some_and(|status| status == "overridden");
        *activity_id == id
            && output["order_id"] == format!("order-{i:08}")
            && from_override == (i < MAX_ACTIVITY_OVERRIDES)
    }

    /// Assert the outcome that `shape` expects for every activity of one
    /// batch. It returns the number of activities that settled as expected.
    fn check(shape: Shape, held: usize, settled: &[(ActivityExecId, ForkResolution)]) -> usize {
        let n = settled.len();
        match shape {
            Shape::Serve => unreachable!("main runs the serve shape"),
            Shape::Hold => {
                assert_eq!(held, n, "resolve_activity must hold every race loser");
                settled
                    .iter()
                    .filter(|(_, resolution)| matches!(resolution, ForkResolution::Hold))
                    .count()
            }
            Shape::Override => settled
                .iter()
                .enumerate()
                .filter(|(i, (id, resolution))| served_by_override(*i, *id, resolution))
                .count(),
            Shape::Scope | Shape::Miss => {
                if shape == Shape::Scope {
                    assert_eq!(held, n, "resolve_activity must hold every scope member");
                }
                settled
                    .iter()
                    .filter(|(id, resolution)| is_unavailable(*id, resolution))
                    .count()
            }
        }
    }

    /// Build the histories of `shape`, then resolve its batch `reps` times.
    /// It returns the number of activities that settled as expected.
    pub fn run(shape: Shape, n: usize, reps: usize) -> usize {
        let Workload {
            source,
            fork,
            fork_ids,
        } = match shape {
            Shape::Serve => unreachable!("main runs the serve shape"),
            Shape::Hold => hold_workload(n),
            Shape::Scope => scope_workload(n),
            Shape::Override => override_workload(n),
            Shape::Miss => miss_workload(n),
        };
        let mut settled_as_expected = 0_usize;
        for _ in 0..reps {
            let batch: Vec<_> = fork_ids
                .iter()
                .map(|id| (*id, resolve_activity(&fork, &source, *id)))
                .collect();
            let held = batch
                .iter()
                .filter(|(_, resolution)| matches!(resolution, ForkResolution::Hold))
                .count();
            let settled = settle_holds(&fork, &source, batch);
            settled_as_expected += check(shape, held, &settled);
        }
        settled_as_expected
    }
}

fn main() {
    let shape = Shape::from_env();
    let n = env_usize("FORK_RESOLVE_PROFILE_N", 1_000);
    let reps = env_usize("FORK_RESOLVE_PROFILE_REPS", 1);
    if shape != Shape::Serve {
        let settled = shapes::run(shape, n, reps);
        assert_eq!(
            settled,
            n * reps,
            "every activity of the {} shape must settle as the shape expects",
            shape.name()
        );
        println!(
            "fork_resolve_profile: shape={} n={n} reps={reps} settled={settled}",
            shape.name()
        );
        return;
    }

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
            .filter(|(id, resolution)| {
                matches!(
                    resolution,
                    ForkResolution::Serve(WorkflowEvent::ActivityCompleted { activity_id, .. })
                        if activity_id == id
                )
            })
            .count();
    }
    assert_eq!(
        served,
        n * reps,
        "every fan-out activity must be served from its record"
    );
    println!("fork_resolve_profile: shape=serve n={n} reps={reps} settled={served}");
}
