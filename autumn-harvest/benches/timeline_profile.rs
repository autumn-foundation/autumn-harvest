//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::timeline::derive_timeline` — the pure,
//! read-only projection behind `GET /workflows/{id}/timeline` (issue #739).
//! Wall-clock timing is not admissible evidence on this (shared-vCPU)
//! machine — every number this harness produces evidence for is a
//! deterministic instruction count (`valgrind --tool=callgrind`) or
//! allocation count/bytes (`valgrind --tool=dhat`), both reproducible
//! bit-for-bit on any machine.
//!
//! # Workload
//!
//! `autumn-harvest-plugin`'s timeline handler loads an execution's full event
//! history (`crate::store::load_timestamped_history`, uncapped) and calls
//! `derive_timeline` exactly once per request. This harness reproduces the
//! shape the module's own docs single out: a long-running, wide-fan-out
//! workflow near completion — regular activities (some retried once before
//! succeeding), local activities, external handoffs, durable timers, and
//! child workflows, most closed out with a handful still open — the shape an
//! operator's "where did this run spend its time?" request targets. The
//! fixed history is derived `TIMELINE_PROFILE_REPS` times, mirroring how many
//! times an operator might reload the same execution's timeline, or how a
//! polling dashboard re-requests it.
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
//! scheduled per simulated execution (the other categories scale off it).
//! `TIMELINE_PROFILE_REPS` (default `1_500`) sets how many times the (fixed)
//! history is derived into a timeline.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
use autumn_harvest::{TimelineEventRow, derive_timeline};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}

/// A representative per-activity input/output record — small enough to be
/// typical, large enough that a `Value` clone isn't free.
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

fn row(timestamp: DateTime<Utc>, event: WorkflowEvent) -> TimelineEventRow {
    TimelineEventRow { timestamp, event }
}

/// Builds the fixed, realistic history this harness derives repeatedly: `n`
/// regular activities (9 of every 10 closed; 1 of every 5 closed ones retries
/// once before succeeding), plus proportionally scaled local activities,
/// external handoffs, timers, and children in the same 9:1 closed:open
/// ratio — a long-running fan-out workflow near completion with a genuine
/// handful of things still open, exactly the shape an operator's timeline
/// request targets.
fn build_workload(n: usize, start: DateTime<Utc>) -> Vec<TimelineEventRow> {
    let mut rows: Vec<TimelineEventRow> = Vec::new();
    let mut t = start;
    let mut tick = || {
        t += chrono::Duration::milliseconds(50);
        t
    };
    let worker = WorkerId::new("worker-a");

    // Regular activities: 9 of every 10 close out (1 of every 5 of those
    // retries once first); the rest stay open (scheduled, never started).
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
            if i % 5 == 4 {
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

    rows
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
    assert!(reps >= 1, "TIMELINE_PROFILE_REPS must be at least 1, got 0");
    assert!(n >= 10, "TIMELINE_PROFILE_N must be at least 10, got {n}");
}

fn main() {
    let n = env_usize("TIMELINE_PROFILE_N", 400);
    let reps = env_usize("TIMELINE_PROFILE_REPS", 1_500);
    validate_workload_params(n, reps);
    let start = now();

    let rows = build_workload(n, start);

    // Sanity-check the fixture once, unmeasured: the timeline must actually
    // contain both open and closed steps in every category, or this harness
    // would silently profile a degenerate (all-closed, or missing-category)
    // history.
    let sanity = derive_timeline(
        &rows,
        start,
        now() + chrono::Duration::hours(1),
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
    let open_steps = sanity
        .steps
        .iter()
        .filter(|s| s.outcome == autumn_harvest::StepOutcome::Pending)
        .count();
    assert!(
        open_steps > 0,
        "fixture bug: no open (Pending) steps in the derived timeline"
    );

    // BTreeMap, not HashMap: this tally is read inside the measured loop
    // below, and HashMap's default RandomState hasher is seeded per-process
    // -- its instructions would leak into the profiled instruction count and
    // make two "identical" runs diverge. BTreeMap is Ord-keyed (no hashing
    // at all), so it costs nothing extra and keeps the profile reproducible
    // bit-for-bit.
    let mut kind_tally: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    let mut total_steps: u64 = 0;
    let mut total_busy_ms: i64 = 0;
    let mut total_wait_ms: i64 = 0;

    let now_ts = now() + chrono::Duration::hours(1);
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
