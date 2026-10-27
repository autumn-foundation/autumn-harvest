//! Deterministic (non-criterion) instruction-count and allocation profiling
//! harness for the pure checks that one fork request runs before it writes a
//! row (issue #2000).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! The harness builds one completed source history of exactly
//! `FORK_VALIDATE_PROFILE_N` events in memory. It is a loop of sequential
//! activities. Each activity has a schedule, a start and a completion. A timer
//! follows every eighth activity, and a signal follows every fifth. Signals
//! pad the history to its exact length. `WorkflowCompleted` ends it.
//!
//! `FORK_VALIDATE_PROFILE_SHAPE` picks the fork point:
//!
//! - `last` (default): `ResetPoint::LastWorkflowTask`. It resolves to the
//!   event just before `WorkflowCompleted`.
//! - `first`: `ResetPoint::FirstActivityRun` for the name of the first
//!   activity. It resolves to event 0.
//! - `explicit`: `ResetPoint::EventId` at the activity completion nearest to
//!   the middle of the history.
//!
//! `FORK_VALIDATE_PROFILE_OVERRIDES` (default 0, at most
//! `MAX_ACTIVITY_OVERRIDES`) sets that many activity overrides. Each override
//! names an activity occurrence after the carried prefix. In the `first` and
//! `explicit` shapes, it names a scheduled activity of the suffix while the
//! suffix has one.
//!
//! `check_fork` runs `reset::resolve_reset_point` and then
//! `fork::validate_fork`, in the order of `fork_in_transaction`. The history
//! and the request are built once, outside `check_fork`.
//! `FORK_VALIDATE_PROFILE_REPS` (default 1) repeats `check_fork`.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest \
//!   --bench fork_validate_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_validate_profile") | .executable')
//! FORK_VALIDATE_PROFILE_N=16500 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! FORK_VALIDATE_PROFILE_N=16500 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::collections::HashMap;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::fork::{
    ForkActivityOverride, MAX_ACTIVITY_OVERRIDES, WorkflowForkRequest, validate_fork,
};
use autumn_harvest::reset::{ResetPlan, ResetPoint, resolve_reset_point};
use autumn_harvest::types::{ActivityExecId, TimerId, WorkerId};
use serde_json::{Value, json};

/// Activity names, used in turn by the activity loop.
const NAMES: [&str; 4] = ["charge", "reserve", "ship", "notify"];

/// The fixed start time of the source run, so no input reads the clock.
const START_SECS: i64 = 1_790_000_000;

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
    })
}

const fn activity_id(i: usize) -> ActivityExecId {
    ActivityExecId::from_uuid(uuid::Uuid::from_u128(i as u128 + 1))
}

const fn has_timer(i: usize) -> bool {
    i % 8 == 7
}

const fn has_signal(i: usize) -> bool {
    i % 5 == 4
}

/// The number of events that activity `i` adds to the history.
const fn step_len(i: usize) -> usize {
    3 + if has_timer(i) { 2 } else { 0 } + if has_signal(i) { 1 } else { 0 }
}

fn signal(step: usize) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: "nudge".to_string(),
        payload: json!({ "step": step }),
    }
}

/// The source history of exactly `len` events, and the index of each
/// activity completion.
fn history(len: usize) -> (Vec<WorkflowEvent>, Vec<usize>) {
    let mut events = Vec::with_capacity(len);
    events.push(WorkflowEvent::WorkflowStarted {
        input: payload(0),
        timestamp: chrono::DateTime::from_timestamp(START_SECS, 0).expect("fixed start time"),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });
    let mut completions = Vec::new();
    let mut step = 0;
    // One slot stays free for `WorkflowCompleted`.
    while events.len() + step_len(step) < len {
        let id = activity_id(step);
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: NAMES[step % NAMES.len()].to_string(),
            input: payload(step),
            queue: "default".to_string(),
        });
        events.push(WorkflowEvent::ActivityStarted {
            activity_id: id,
            worker_id: WorkerId::new("worker-1"),
        });
        completions.push(events.len());
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: payload(step),
        });
        if has_timer(step) {
            let timer_id = TimerId::new(format!("timer-{step}"));
            events.push(WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            });
            events.push(WorkflowEvent::TimerFired { timer_id });
        }
        if has_signal(step) {
            events.push(signal(step));
        }
        step += 1;
    }
    while events.len() + 1 < len {
        events.push(signal(step));
    }
    events.push(WorkflowEvent::WorkflowCompleted {
        output: json!({ "steps": step }),
    });
    assert_eq!(events.len(), len, "the history has the requested length");
    (events, completions)
}

/// The fork point of `shape`, and the event id that it must resolve to.
fn fork_point(shape: &str, len: usize, completions: &[usize]) -> (ResetPoint, usize) {
    match shape {
        "last" => (ResetPoint::LastWorkflowTask, len - 2),
        "first" => (
            ResetPoint::FirstActivityRun {
                activity_name: NAMES[0].to_string(),
            },
            0,
        ),
        "explicit" => {
            let middle = *completions
                .iter()
                .min_by_key(|index| index.abs_diff(len / 2))
                .expect("the history holds an activity");
            let event_id = i64::try_from(middle).expect("event id fits i64");
            (ResetPoint::EventId { event_id }, middle)
        }
        other => {
            panic!("FORK_VALIDATE_PROFILE_SHAPE must be last, first or explicit, got {other:?}")
        }
    }
}

/// `count` overrides, each after the carried occurrences of its name.
fn overrides(prefix: &[WorkflowEvent], count: usize) -> Vec<ForkActivityOverride> {
    let mut carried = HashMap::<&str, u32>::new();
    for event in prefix {
        if let WorkflowEvent::ActivityScheduled { name, .. } = event {
            *carried.entry(name.as_str()).or_default() += 1;
        }
    }
    (0..count)
        .map(|k| {
            let name = NAMES[k % NAMES.len()];
            let later = u32::try_from(k / NAMES.len()).expect("occurrence fits u32");
            ForkActivityOverride {
                activity_name: name.to_string(),
                occurrence: carried.get(name).copied().unwrap_or(0) + later + 1,
                output: json!({ "stub": k }),
            }
        })
        .collect()
}

/// The pure checks of `fork_in_transaction`, in its order.
#[inline(never)]
fn check_fork(events: &[WorkflowEvent], request: &WorkflowForkRequest) -> (i64, ResetPlan) {
    let fork_event_id = request.fork_point.as_ref().map_or(0, |point| {
        resolve_reset_point(events, point)
            .unwrap_or_else(|reason| panic!("the fork point must resolve, got {reason:?}"))
    });
    let plan = validate_fork(events, fork_event_id, request)
        .unwrap_or_else(|error| panic!("validate_fork must accept the fork, got {error}"));
    (fork_event_id, plan)
}

fn main() {
    let len = env_usize("FORK_VALIDATE_PROFILE_N", 16_500);
    let reps = env_usize("FORK_VALIDATE_PROFILE_REPS", 1);
    let count = env_usize("FORK_VALIDATE_PROFILE_OVERRIDES", 0);
    let shape = std::env::var("FORK_VALIDATE_PROFILE_SHAPE").unwrap_or_else(|_| "last".into());
    assert!(
        len >= 5,
        "FORK_VALIDATE_PROFILE_N must be at least 5, got {len}"
    );
    assert!(
        count <= MAX_ACTIVITY_OVERRIDES,
        "FORK_VALIDATE_PROFILE_OVERRIDES must be at most {MAX_ACTIVITY_OVERRIDES}, got {count}"
    );

    let (events, completions) = history(len);
    let (point, expected) = fork_point(&shape, len, &completions);
    let expected_id = i64::try_from(expected).expect("event id fits i64");
    let request = WorkflowForkRequest {
        fork_point: Some(point),
        reason: "profile".to_string(),
        operator_id: "profile".to_string(),
        activity_overrides: overrides(&events[..=expected], count),
        ..WorkflowForkRequest::default()
    };

    for _ in 0..reps {
        let (resolved, plan) = check_fork(std::hint::black_box(&events), &request);
        assert_eq!(resolved, expected_id, "the {shape} point resolves");
        assert_eq!(
            plan.reset_to_event_id, expected_id,
            "the plan keeps the point"
        );
        assert_eq!(
            plan.events_carried_over,
            expected + 1,
            "the plan carries the prefix up to the point"
        );
        assert!(
            plan.unresolved_side_effects.is_empty(),
            "the point is a clean boundary"
        );
        std::hint::black_box(plan);
    }
    println!(
        "fork_validate_profile: shape={shape} n={len} overrides={count} reps={reps} \
         resolved={expected_id} carried={}",
        expected + 1
    );
}
