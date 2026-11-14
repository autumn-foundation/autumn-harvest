//! Non-criterion instruction/allocation-count profiling harness for
//! `autumn_harvest::timeline::derive_timeline`.
//!
//! It is the pure, read-only projection behind `GET /workflows/{id}/timeline`
//! (issue #739). Wall-clock timing is not admissible evidence on this
//! shared-vCPU machine, so this binary does not use `cargo bench` / criterion
//! timing. It runs under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes). That is a direct
//! single-process measurement, but **not** bit-for-bit reproducible run-to-run.
//! This harness generates fresh random activity/child UUIDs on each run. This
//! file's `AccKey`-keyed lookup map mirrors the internal map of
//! `derive_timeline`. Both use the randomly-seeded `RandomState` of
//! `std::collections::HashMap`, so hash values and probe patterns vary between
//! runs. Measured spread: two callgrind runs each of the pre-fix and post-fix
//! binaries (see the #1372 commits) varied by ~0.1-0.2% run-to-run. That is
//! over two orders of magnitude below the delta this harness detects. dhat
//! allocation counts/bytes do not depend on hash values. Repeated runs
//! confirmed them byte-identical.
//!
//! # Workload
//!
//! The `autumn-harvest-plugin` timeline handler loads an execution's full event
//! history (`crate::store::load_timestamped_history`, uncapped). It calls
//! `derive_timeline` exactly once per request. This harness reproduces the
//! shape the module docs single out: a long-running, wide-fan-out workflow
//! near completion. It has regular activities (some retried once before
//! success), local activities, external handoffs, durable timers, and child
//! workflows. Most are closed and a handful are still open. An operator's
//! "where did this run spend its time?" request targets this shape. The
//! harness derives the fixed history `TIMELINE_PROFILE_REPS` times. That
//! mirrors an operator who reloads the timeline, or a polling dashboard.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench timeline_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="timeline_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `TIMELINE_PROFILE_N` (default `400`) sets the number of regular activities
//! per simulated execution. The other categories scale off it.
//! `TIMELINE_PROFILE_REPS` (default `1_500`, minimum `100`) sets how many
//! times the fixed history is derived into a timeline. Callgrind's default
//! `--collect-atstart=yes` also captures the one-time sanity-check derivation
//! in `main`. The floor keeps that derivation under ~1% of the collected
//! instruction count.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
use autumn_harvest::{TimelineEventRow, derive_timeline};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}

/// A representative per-activity input/output record — small enough to be
/// typical, large enough that a `Value` clone is not free.
fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
    })
}

const ACTIVITY_NAMES: [&str; 4] = [
    "charge_card",
    "send_receipt",
    "notify_partner",
    "sync_inventory",
];
const QUEUES: [&str; 4] = ["payments", "email", "webhooks", "inventory"];

const fn row(timestamp: DateTime<Utc>, event: WorkflowEvent) -> TimelineEventRow {
    TimelineEventRow { timestamp, event }
}

/// Builds the fixed history that this harness derives repeatedly.
///
/// It holds `n` regular activities. 9 of every 10 are closed, and 1 of every 5
/// closed ones retries once before success. Scaled local activities, external
/// handoffs, timers, and children follow the same 9:1 closed:open ratio. That
/// is a fan-out workflow near completion with a few things still open, the
/// shape an operator's timeline request targets.
///
/// Returns the rows and the timestamp of the last generated event. Callers
/// must derive the profiling clock (`now`) from it, not from a fixed offset
/// from `start` (#1372). Otherwise a large `TIMELINE_PROFILE_N` makes the
/// history outlast a hardcoded horizon. Its events are then in the "future"
/// of the clock that the timeline is derived against.
#[allow(clippy::too_many_lines, reason = "one straight-line event list")]
fn build_workload(n: usize, start: DateTime<Utc>) -> (Vec<TimelineEventRow>, DateTime<Utc>) {
    let mut rows: Vec<TimelineEventRow> = Vec::new();
    let mut t = start;
    let mut tick = || {
        t += chrono::Duration::milliseconds(50);
        t
    };
    let worker = WorkerId::new("worker-a");

    // Regular activities: 9 of every 10 close out (1 of every 5 of those
    // retries once first); the rest stay open (scheduled, never started).
    // The retry selector counts CLOSED activities, not `i` (#1372). Picking on
    // `i % 5` directly would make every retry candidate (i % 5 == 4) land on an
    // index that is ALSO open. That index is i % 10 == 9, since 9 % 5 == 4 and
    // the period-10 open cycle never shifts that residue. Retries would then
    // fire on 1 in 10 regular activities. The documented rate is 1 in 5 of
    // the CLOSED ones.
    let mut closed_count: usize = 0;
    for i in 0..n {
        let activity_id = ActivityExecId::new();
        let name = ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()].to_string();
        let queue = QUEUES[i % QUEUES.len()].to_string();
        rows.push(row(
            tick(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: name.clone(),
                input: payload(i),
                queue,
            },
        ));
        if i % 10 != 9 {
            let retries = closed_count % 5 == 4;
            closed_count += 1;
            if retries {
                // Retried once: started, failed, restarted, then completed.
                rows.push(row(
                    tick(),
                    WorkflowEvent::ActivityStarted {
                        activity_id,
                        worker_id: worker.clone(),
                    },
                ));
                rows.push(row(
                    tick(),
                    WorkflowEvent::ActivityFailed {
                        activity_id,
                        error: "transient".to_string(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        non_retryable: false,
                        details: None,
                    },
                ));
            }
            rows.push(row(
                tick(),
                WorkflowEvent::ActivityStarted {
                    activity_id,
                    worker_id: worker.clone(),
                },
            ));
            rows.push(row(
                tick(),
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: payload(i),
                },
            ));
        }
    }

    // Local activities: n/4 of them, same 9:1 ratio.
    let local_n = (n / 4).max(1);
    for i in 0..local_n {
        let activity_id = ActivityExecId::new();
        let name = format!("local_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        rows.push(row(
            tick(),
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: name.clone(),
                input: payload(i),
                resolved: true,
                retry_policy: None,
                start_to_close_nanos: None,
            },
        ));
        if i % 10 != 9 {
            rows.push(row(
                tick(),
                WorkflowEvent::LocalActivityCompleted {
                    activity_id,
                    output: payload(i),
                },
            ));
        }
    }

    // External-handoff activities: n/10 of them, same 9:1 ratio.
    let external_n = (n / 10).max(1);
    for i in 0..external_n {
        let activity_id = ActivityExecId::new();
        let token = autumn_harvest::types::ExternalActivityToken::new();
        let name = format!("external_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        rows.push(row(
            tick(),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: name.clone(),
                input: payload(i),
                queue: QUEUES[i % QUEUES.len()].to_string(),
                schedule_to_close_secs: 3600,
            },
        ));
        if i % 10 != 9 {
            rows.push(row(
                tick(),
                WorkflowEvent::ActivityCompletedExternally {
                    activity_id,
                    token,
                    output: payload(i),
                },
            ));
        }
    }

    // Durable timers: n/10 of them, same 9:1 ratio.
    let timer_n = (n / 10).max(1);
    for i in 0..timer_n {
        let timer_id = TimerId::new(format!("timer-{i}"));
        rows.push(row(
            tick(),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
        ));
        if i % 10 != 9 {
            rows.push(row(tick(), WorkflowEvent::TimerFired { timer_id }));
        }
    }

    // Child workflows: n/20 of them, same 9:1 ratio.
    let child_n = (n / 20).max(1);
    for i in 0..child_n {
        let child_id = ExecutionId::new();
        let workflow_name = format!("child_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        rows.push(row(
            tick(),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: workflow_name.clone(),
                input: payload(i),
            },
        ));
        if i % 10 != 9 {
            rows.push(row(
                tick(),
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id,
                    output: payload(i),
                },
            ));
        }
    }

    (rows, t)
}

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

fn validate_workload_params(n: usize, reps: usize) {
    // Callgrind's `--collect-atstart` defaults to `yes` (see the Running
    // section above). So the one-time, unmeasured-in-INTENT sanity
    // `derive_timeline` call in `main` below IS captured in the collected
    // instruction count. No in-process way excludes it without a
    // client-request crate, and this repo has no dependency on one. At
    // reps=1 that one call is roughly HALF of the counted work, silently
    // diluting the measured delta (#1372). At least 100 reps keep the fixed
    // cost of the sanity call under ~1% of the total. That is an order of
    // magnitude below the impact floor (>=5% Ir). It is also below this
    // harness's measured run-to-run noise (~0.1-0.2%, see the header). So it
    // can never be mistaken for signal.
    assert!(
        reps >= 100,
        "TIMELINE_PROFILE_REPS must be at least 100 (so the one-time, \
         always-collected sanity-check derive_timeline call before the \
         measured loop stays under ~1% of the total instruction count), \
         got {reps}"
    );
    // The smallest category is children at n/20. Below n=200, child_n < 10,
    // so its 9:1 ratio (i % 10 == 9) can never land on an open item (#1372).
    // The fixture would then silently measure an all-closed category, not the
    // documented 9:1 closed:open mix in every category.
    assert!(
        n >= 200,
        "TIMELINE_PROFILE_N must be at least 200 (so even n/20 reaches the \
         10 items needed for every category's 9:1 closed:open ratio to \
         include an open item), got {n}"
    );
}

fn main() {
    let n = env_usize("TIMELINE_PROFILE_N", 400);
    let reps = env_usize("TIMELINE_PROFILE_REPS", 1_500);
    validate_workload_params(n, reps);
    let start = now();

    let (rows, last_event_ts) = build_workload(n, start);
    // Always after every generated event, however large TIMELINE_PROFILE_N
    // gets -- see build_workload's doc comment.
    let now_ts = last_event_ts + chrono::Duration::hours(1);

    // Sanity-check the fixture once, unmeasured. The timeline must contain
    // both open and closed steps in every category. Otherwise this harness
    // silently profiles a degenerate (all-closed, or missing-category) history.
    let sanity = derive_timeline(
        &rows,
        start,
        now_ts,
        None,
        "exec-sanity".to_string(),
        "wf-sanity".to_string(),
        "order_fulfillment".to_string(),
        "RUNNING".to_string(),
    );
    assert!(
        sanity.steps.len() >= n,
        "fixture bug: fewer timeline steps ({}) than regular activities ({n})",
        sanity.steps.len()
    );
    for kind in [
        autumn_harvest::StepKind::Activity,
        autumn_harvest::StepKind::LocalActivity,
        autumn_harvest::StepKind::Timer,
        autumn_harvest::StepKind::ChildWorkflow,
    ] {
        let (open, closed) = sanity.steps.iter().filter(|s| s.step_kind == kind).fold(
            (0u32, 0u32),
            |(open, closed), s| {
                if s.outcome == autumn_harvest::StepOutcome::Pending {
                    (open + 1, closed)
                } else {
                    (open, closed + 1)
                }
            },
        );
        assert!(
            open > 0 && closed > 0,
            "fixture bug: {kind:?} has no {} steps (open={open}, closed={closed}) -- \
             TIMELINE_PROFILE_N is too small for every category's 9:1 closed:open \
             ratio to include both",
            if open == 0 { "open" } else { "closed" }
        );
    }

    // BTreeMap, not HashMap: the measured loop below reads this tally.
    // HashMap's default RandomState hasher is seeded per process. Its
    // instructions would leak into the profiled count. They would add MORE
    // run-to-run variance on top of the internal hashing of derive_timeline
    // (see this file's header). BTreeMap is Ord-keyed with no hashing, so it
    // does not make that variance worse.
    let mut kind_tally: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    let mut total_steps: u64 = 0;
    let mut total_busy_ms: i64 = 0;
    let mut total_wait_ms: i64 = 0;

    for _ in 0..reps {
        // Mirrors the real handler: one `derive_timeline` call per served
        // request, over the SAME loaded history.
        let timeline = derive_timeline(
            std::hint::black_box(&rows),
            start,
            now_ts,
            None,
            "exec-1".to_string(),
            "wf-1".to_string(),
            "order_fulfillment".to_string(),
            "RUNNING".to_string(),
        );
        total_steps += timeline.steps.len() as u64;
        total_busy_ms += timeline.rollup.busy_ms;
        total_wait_ms += timeline.rollup.wait_ms;
        for step in &timeline.steps {
            *kind_tally.entry(step.step_kind.as_str()).or_insert(0) += 1;
        }
    }

    let kinds: Vec<(&str, u64)> = kind_tally.into_iter().collect();
    println!(
        "timeline_profile: n={n} reps={reps} rows={} calls={reps} total_steps={total_steps} \
         total_busy_ms={total_busy_ms} total_wait_ms={total_wait_ms} kinds={kinds:?}",
        rows.len(),
    );
}
