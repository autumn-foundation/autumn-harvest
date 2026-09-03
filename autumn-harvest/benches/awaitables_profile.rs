//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::awaitables::project_awaitables` — the pure,
//! read-only projection behind every "what is this execution parked on right
//! now?" management-API endpoint (issue #615): `/workflows/{id}/awaitables`,
//! and the awaitables sub-report folded into `/workflows/{id}/diagnose` and
//! `/workflows/{id}/replay-diagnosis`. Wall-clock timing is not admissible
//! evidence on this (shared-vCPU) machine — every number this harness
//! produces evidence for is a deterministic instruction count
//! (`valgrind --tool=callgrind`) or allocation count/bytes
//! (`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.
//!
//! # Workload
//!
//! `autumn-harvest-plugin/src/api.rs`'s awaitables handler loads the FULL
//! event history for the target execution (`rows`, uncapped) and calls
//! `project_awaitables(&rows, wait_set, AWAITABLE_CATEGORY_CAP)` exactly
//! once per request. `project_awaitables` always runs `build_history_index`
//! — a single O(n) pass over every row — regardless of wait-set source, then
//! projects either the drained replay command buffer
//! (`WaitSetInput::Replayed`, the common "handler registered, replay
//! succeeded, workflow suspended" happy path used by
//! `QueryReplayOutcome::Suspended`) or a best-effort history-only scan
//! (`WaitSetInput::HistoryOnly`, the degraded-replay fallback).
//!
//! This harness reproduces the realistic shape the module's own docs single
//! out: a long-running, wide-fan-out workflow near completion — most of its
//! scheduled work (activities, local activities, external handoffs, timers,
//! children) already closed out, a handful still genuinely open. That is
//! exactly the traffic an operator's `/awaitables` request is for: a
//! workflow that has been running long enough to accumulate a large history
//! is also the one worth asking "what's it still waiting on?" about. Both
//! `WaitSetInput` modes are exercised every rep, mirroring how the same
//! fixed history is diagnosed from both angles in production (a `/diagnose`
//! call folds in an awaitables-shaped sub-check; the plugin degrades to
//! history-only whenever replay is unavailable).
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench awaitables_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="awaitables_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `AWAITABLES_PROFILE_N` (default `400`) sets the number of regular
//! activities scheduled per simulated execution (the other categories scale
//! off it). `AWAITABLES_PROFILE_REPS` (default `1_500`) sets how many times
//! the (fixed) history is projected in each mode — i.e. how many simulated
//! `/awaitables`-shaped requests are served.

use autumn_harvest::awaitables::{AWAITABLE_CATEGORY_CAP, WaitSetInput, project_awaitables};
use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalActivityToken, TimerId};
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

/// Builds the fixed, realistic history + drained-command wait-set this
/// harness projects repeatedly: `n` regular activities (9 of every 10
/// closed), plus proportionally scaled local activities, external handoffs,
/// timers, and children in the same 9:1 closed:open ratio — a long-running
/// fan-out workflow near completion with a genuine handful of things still
/// open, exactly the shape an operator's diagnostic request targets.
fn build_workload(
    n: usize,
    start: DateTime<Utc>,
) -> (Vec<(DateTime<Utc>, WorkflowEvent)>, Vec<WorkflowCommand>) {
    let mut rows: Vec<(DateTime<Utc>, WorkflowEvent)> = Vec::new();
    let mut commands: Vec<WorkflowCommand> = Vec::new();
    let mut t = start;
    let mut tick = || {
        t += chrono::Duration::milliseconds(50);
        t
    };

    // Regular activities: 9 of every 10 close out; the rest stay open and
    // surface as WaitForActivity in the drained wait-set.
    for i in 0..n {
        let activity_id = ActivityExecId::new();
        let name = ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()].to_string();
        let queue = QUEUES[i % QUEUES.len()].to_string();
        rows.push((
            tick(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: name.clone(),
                input: payload(i),
                queue,
            },
        ));
        if i % 10 == 9 {
            let (result_tx, _rx) = tokio::sync::oneshot::channel();
            commands.push(WorkflowCommand::WaitForActivity {
                activity_id,
                result_tx,
            });
        } else {
            rows.push((
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
        rows.push((
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
        if i % 10 == 9 {
            let (result_tx, _rx) = tokio::sync::oneshot::channel();
            commands.push(WorkflowCommand::RunLocalActivity {
                activity_id,
                name: name.clone(),
                input: payload(i),
                start_to_close: None,
                retry_policy: None,
                result_tx,
                already_scheduled: true,
                failed_attempts: 0,
                last_error: None,
            });
        } else {
            rows.push((
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
        let token = ExternalActivityToken::new();
        let name = format!("external_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        rows.push((
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
        if i % 10 == 9 {
            let (result_tx, _rx) = tokio::sync::oneshot::channel();
            commands.push(WorkflowCommand::ScheduleExternalActivity {
                activity_id,
                token,
                name: name.clone(),
                input: payload(i),
                queue: QUEUES[i % QUEUES.len()].to_string(),
                schedule_to_close_secs: 3600,
                result_tx,
            });
        } else {
            rows.push((
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
        rows.push((
            tick(),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
        ));
        if i % 10 == 9 {
            let (result_tx, _rx) = tokio::sync::oneshot::channel();
            commands.push(WorkflowCommand::StartTimer {
                timer_id,
                duration_secs: 300,
                result_tx,
            });
        } else {
            rows.push((tick(), WorkflowEvent::TimerFired { timer_id }));
        }
    }

    // Child workflows: n/20 of them, same 9:1 ratio.
    let child_n = (n / 20).max(1);
    for i in 0..child_n {
        let child_id = ExecutionId::new();
        let workflow_name = format!("child_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        rows.push((
            tick(),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: workflow_name.clone(),
                input: payload(i),
            },
        ));
        if i % 10 == 9 {
            let (result_tx, _rx) = tokio::sync::oneshot::channel();
            commands.push(WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name: workflow_name.clone(),
                input: payload(i),
                result_tx,
            });
        } else {
            rows.push((
                tick(),
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id,
                    output: payload(i),
                },
            ));
        }
    }

    (rows, commands)
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
    assert!(reps >= 1, "AWAITABLES_PROFILE_REPS must be at least 1, got 0");
    assert!(n >= 10, "AWAITABLES_PROFILE_N must be at least 10, got {n}");
}

fn main() {
    let n = env_usize("AWAITABLES_PROFILE_N", 400);
    let reps = env_usize("AWAITABLES_PROFILE_REPS", 1_500);
    validate_workload_params(n, reps);
    let start = now();

    let (rows, commands) = build_workload(n, start);

    // Sanity-check the fixture once, unmeasured: both modes must actually
    // find the planted open awaitables, or this harness would silently
    // profile an all-closed (degenerate) history.
    let replayed_sanity = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );
    assert!(
        !replayed_sanity.awaitables.is_empty(),
        "fixture bug: replayed projection found no open awaitables"
    );
    let history_only_sanity = project_awaitables(
        &rows,
        WaitSetInput::HistoryOnly {
            fire_eligible_timers: None,
        },
        AWAITABLE_CATEGORY_CAP,
    );
    assert!(
        !history_only_sanity.awaitables.is_empty(),
        "fixture bug: history-only projection found no open awaitables"
    );

    // BTreeMap, not HashMap: this tally is read inside the measured loop
    // below, and HashMap's default RandomState hasher is seeded per-process
    // -- its instructions would leak into the profiled instruction count and
    // make two "identical" runs diverge. BTreeMap is Ord-keyed (no hashing
    // at all), so it costs nothing extra and keeps the profile reproducible
    // bit-for-bit.
    let mut kind_tally: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    let mut total_awaitables: u64 = 0;

    for _ in 0..reps {
        // Mirrors the real handler: the replayed (happy-path) projection and
        // the history-only (degraded-fallback) projection are both real
        // production call shapes over the SAME loaded history, so both are
        // measured every rep.
        let replayed = project_awaitables(
            std::hint::black_box(&rows),
            WaitSetInput::Replayed {
                commands: std::hint::black_box(&commands),
            },
            AWAITABLE_CATEGORY_CAP,
        );
        let history_only = project_awaitables(
            std::hint::black_box(&rows),
            WaitSetInput::HistoryOnly {
                fire_eligible_timers: None,
            },
            AWAITABLE_CATEGORY_CAP,
        );
        total_awaitables += replayed.awaitables.len() as u64;
        total_awaitables += history_only.awaitables.len() as u64;
        for a in &replayed.awaitables {
            *kind_tally.entry(a.kind.as_str()).or_insert(0) += 1;
        }
        for a in &history_only.awaitables {
            *kind_tally.entry(a.kind.as_str()).or_insert(0) += 1;
        }
    }

    let kinds: Vec<(&str, u64)> = kind_tally.into_iter().collect();
    println!(
        "awaitables_profile: n={n} reps={reps} rows={} commands={} calls={} \
         total_awaitables={total_awaitables} kinds={kinds:?}",
        rows.len(),
        commands.len(),
        reps * 2
    );
}
