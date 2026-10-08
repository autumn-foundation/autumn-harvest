//! Non-criterion instruction/allocation-count profiling harness for
//! `autumn_harvest::timeline::derive_timeline` — the pure, read-only
//! projection behind `GET /workflows/{id}/timeline` (issue #739). Wall-clock
//! timing is not admissible evidence on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is
//! driven directly under `valgrind --tool=callgrind` (instruction counts)
//! and `valgrind --tool=dhat` (allocation counts/bytes) instead -- a direct
//! single-process measurement, but **not** bit-for-bit reproducible
//! run-to-run: this harness generates fresh random activity/child UUIDs on
//! every run, and both this file's own `AccKey`-keyed lookup map (mirroring
//! `derive_timeline`'s internal one) and `derive_timeline` itself use
//! `std::collections::HashMap`'s randomly-seeded `RandomState`, so hash
//! values and probe patterns vary between runs. Measured spread: two
//! callgrind runs each of the pre-fix and post-fix binaries (see #1372's
//! commits) varied by ~0.1-0.2% run-to-run, over two orders of magnitude
//! below the delta this harness was built to detect. dhat's allocation
//! counts/bytes are unaffected (allocation *count* doesn't depend on hash
//! values) and were confirmed byte-identical across repeated runs.
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
//! `TIMELINE_PROFILE_REPS` (default `1_500`, minimum `100`) sets how many
//! times the (fixed) history is derived into a timeline. The floor keeps the
//! one-time sanity-check derivation below `main` (also captured by
//! Callgrind's default `--collect-atstart=yes`) under ~1% of the total
//! collected instruction count.

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
///
/// Returns the rows alongside the timestamp of the last generated event:
/// callers must derive the profiling clock (`now`) from it rather than a
/// fixed offset from `start`, or a large enough `TIMELINE_PROFILE_N` makes
/// the fixed history outlast a hardcoded horizon and produces "future"
/// events relative to the clock the timeline is derived against (Codex
/// review on #1372).
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
    // The retry selector counts CLOSED activities, not `i` -- picking on `i
    // % 5` directly would make every retry candidate (i % 5 == 4) land on an
    // index that's ALSO open (i % 10 == 9, since 9 % 5 == 4 and the period-10
    // open cycle never shifts that residue), so retries would fire on 1 in 10
    // regular activities instead of the documented 1 in 5 of the CLOSED ones
    // (Codex review on #1372).
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
    // section above), so the one-time, unmeasured-in-INTENT sanity
    // `derive_timeline` call in `main` below IS captured in the collected
    // instruction count -- there is no in-process way to exclude it without
    // a client-request crate this repo hasn't taken a dependency on. At
    // reps=1 that one call is roughly HALF of the counted work, silently
    // diluting the measured delta (Codex review on #1372). Requiring at
    // least 100 reps keeps the sanity call's fixed one-call cost under ~1%
    // of the total -- an order of magnitude below both the impact floor
    // (>=5% Ir) and this harness's own measured run-to-run noise
    // (~0.1-0.2%, see the header) -- so it can never be mistaken for signal.
    assert!(
        reps >= 100,
        "TIMELINE_PROFILE_REPS must be at least 100 (so the one-time, \
         always-collected sanity-check derive_timeline call before the \
         measured loop stays under ~1% of the total instruction count), \
         got {reps}"
    );
    // The smallest category is children at n/20. Below n=200, child_n < 10,
    // so its 9:1 ratio (i % 10 == 9) can never land on an open item -- the
    // fixture would silently measure a workload with an all-closed category
    // rather than the documented 9:1 closed:open mix in every category (Codex
    // review on #1372).
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

    // Sanity-check the fixture once, unmeasured: the timeline must actually
    // contain both open and closed steps in every category, or this harness
    // would silently profile a degenerate (all-closed, or missing-category)
    // history.
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

    // BTreeMap, not HashMap: this tally is read inside the measured loop
    // below, and HashMap's default RandomState hasher is seeded per-process
    // -- its instructions would leak into the profiled instruction count and
    // add MORE run-to-run variance on top of what derive_timeline's own
    // internal hashing already contributes (see this file's header).
    // BTreeMap is Ord-keyed (no hashing at all), so it costs nothing extra
    // and doesn't make that existing variance any worse.
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
