//! Per-execution timeline read model (issue #739).
//!
//! A **read-only** projection of a workflow execution's recorded history into an
//! ordered list of orchestration *steps* (activities, local activities, timers,
//! child workflows, signal waits, side effects) plus a duration rollup. It is
//! derived **purely** from `harvest_events` — no new events are appended, no
//! state is recomputed, and the workflow function is never re-run. It answers
//! the operator question "where did this run spend its time?".
//!
//! The core of this module is the pure function [`derive_timeline`], which maps
//! an ordered slice of `(row timestamp, WorkflowEvent)` pairs (plus the
//! execution's start time and clock context) to a [`Timeline`] document. It is
//! `db`-free and unit-tested against hand-built history fixtures; the `db`-gated
//! loader [`crate::store::load_timestamped_history`] supplies the rows at
//! runtime and the `autumn-harvest-plugin` management API serves the result over
//! `GET /workflows/{id}/timeline`.
//!
//! ## Why timestamps live on the row, not the event
//!
//! [`WorkflowEvent`] variants do not carry a per-event wall-clock timestamp
//! (only `WorkflowStarted { timestamp }`). The authoritative timestamp is on the
//! `harvest_events` row. [`TimelineEventRow`] pairs each event with its row
//! timestamp so the derivation can reason about durations.
//!
//! ## Step reconstruction
//!
//! Each step is reconstructed by pairing a *scheduling* event with its *terminal*
//! event, keyed by the relevant id:
//!
//! | kind | key | scheduled by | ended by | wait/exec split |
//! |---|---|---|---|---|
//! | activity | `activity_id` | `ActivityScheduled` | `ActivityCompleted`/`ActivityFailed`/`ActivityTimedOut` | present ⇔ an `ActivityStarted` was recorded |
//! | local_activity | `activity_id` | `LocalActivityScheduled` | `LocalActivityCompleted`/`LocalActivityExhausted` | never (no claim event) |
//! | timer | `timer_id` | `TimerStarted` | `TimerFired` | never |
//! | child_workflow | `child_id` | `ChildWorkflowStarted` | `ChildWorkflowCompleted`/`ChildWorkflowFailed` | never (parent records no child-start ts) |
//! | side_effect | — | `SideEffectRecorded` (point) | same event | never |
//! | signal_wait | — | previous event's ts | `SignalReceived` | never |
//!
//! ### `attempt` derivation
//!
//! A retried activity does **not** append a fresh `ActivityScheduled` — there is
//! exactly one per `activity_id`, reused across attempts, with a new
//! `ActivityStarted`/`ActivityFailed { attempt }` per attempt. So `attempt` is
//! the **maximum** `attempt` field seen on `ActivityFailed` (resp.
//! `LocalActivityFailed`/`LocalActivityExhausted`) events for the id, defaulting
//! to `1` for a first-try completion. `ActivityTimedOut` carries no `attempt`
//! field, so a timed-out step reports the max prior failed attempt (or `1`).
//!
//! ### signal_wait caveat
//!
//! A signal-wait step's duration is measured from the **immediately preceding**
//! recorded history event to the signal's arrival; history records no explicit
//! "started waiting" marker, so if the workflow was concurrently running other
//! work this window may overlap it. A running execution parked on a
//! **never-arriving** signal produces **no** step (there is no pending-signal
//! event to reconstruct from).
//!
//! ### child_workflow caveat
//!
//! The parent's history has no "child running" timestamp, so the wait/exec split
//! is never available for child_workflow steps — only `total_ms` is reported.
//!
//! ## Rollup attribution
//!
//! `busy_ms` and `wait_ms` are attributed per step by kind (see
//! [`TimelineRollup`]). Their sum need **not** equal `total_wall_clock_ms` —
//! suspension and orchestration gaps between steps are unattributed and expected.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::event::WorkflowEvent;

/// One recorded history event paired with its `harvest_events` row timestamp.
///
/// [`WorkflowEvent`] does not carry a per-event timestamp; the authoritative
/// wall-clock instant lives on the row. The `db`-gated loader
/// [`crate::store::load_timestamped_history`] produces these; the pure
/// [`derive_timeline`] consumes an ordered slice of them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEventRow {
    /// The `harvest_events.timestamp` of the row that stored this event.
    pub timestamp: DateTime<Utc>,
    /// The recorded workflow event.
    pub event: WorkflowEvent,
}

/// The kind of orchestration unit a [`TimelineStep`] represents. Bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// A regular (queued) activity.
    Activity,
    /// A local activity (inline on the workflow worker).
    LocalActivity,
    /// A durable timer.
    Timer,
    /// A child workflow.
    ChildWorkflow,
    /// A wait for an external signal, measured to the signal's arrival.
    SignalWait,
    /// A deterministic side-effect capture (a point in time).
    SideEffect,
}

impl StepKind {
    /// Stable `snake_case` label, used as the [`TimelineRollup::step_count_by_kind`]
    /// map key and for serialization.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Activity => "activity",
            Self::LocalActivity => "local_activity",
            Self::Timer => "timer",
            Self::ChildWorkflow => "child_workflow",
            Self::SignalWait => "signal_wait",
            Self::SideEffect => "side_effect",
        }
    }
}

/// The terminal (or in-flight) outcome of a [`TimelineStep`]. Bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed,
    /// Exceeded a timeout.
    TimedOut,
    /// Explicitly cancelled (reserved; the core derivation does not currently
    /// emit this — no per-step "cancelled" terminal event exists — but the plugin
    /// may surface it in future).
    Cancelled,
    /// A timer that elapsed.
    Fired,
    /// Still open at the time the timeline was derived.
    Pending,
}

/// A single reconstructed orchestration unit on the timeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimelineStep {
    /// Which kind of orchestration unit this is.
    pub step_kind: StepKind,
    /// The unit's name (activity/child-workflow/signal name, timer id); `None`
    /// where not applicable (e.g. an unnamed side effect).
    pub name: Option<String>,
    /// When the unit was scheduled/started, from the scheduling event's row.
    pub scheduled_at: DateTime<Utc>,
    /// When the unit reached a terminal state; `None` while still open.
    pub ended_at: Option<DateTime<Utc>>,
    /// `ended_at - scheduled_at` in ms (to "now" for open steps), clamped `>= 0`.
    pub total_ms: i64,
    /// Queue-wait split (`start_ts - scheduled_at`); `Some` only when a start
    /// timestamp was recorded (regular activities). Paired with [`Self::exec_ms`].
    pub wait_ms: Option<i64>,
    /// Execution split (`ended_at - start_ts`); `Some` only when a start
    /// timestamp was recorded. `None` → only [`Self::total_ms`] is meaningful.
    pub exec_ms: Option<i64>,
    /// The unit's outcome.
    pub outcome: StepOutcome,
    /// Final attempt number for kinds that retry (activity, `local_activity`);
    /// `None` for kinds that don't.
    pub attempt: Option<i32>,
}

/// The single slowest step by `total_ms`, surfaced in the rollup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlowestStep {
    /// The slowest step's name (if any).
    pub name: Option<String>,
    /// The slowest step's kind.
    pub step_kind: StepKind,
    /// The slowest step's total duration in ms.
    pub total_ms: i64,
}

/// Aggregate duration statistics across all steps of an execution.
///
/// `busy_ms + wait_ms` need **not** equal `total_wall_clock_ms` — gaps between
/// steps (suspension, orchestration) are unattributed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimelineRollup {
    /// `clock_end - started_at` in ms, where `clock_end` is the terminal instant
    /// or "now" for a running execution.
    pub total_wall_clock_ms: i64,
    /// Sum of per-step busy contributions (activity exec,
    /// `local`/`child`/`side_effect` totals).
    pub busy_ms: i64,
    /// Sum of per-step wait contributions (activity queue wait, timer + signal
    /// totals).
    pub wait_ms: i64,
    /// The single slowest step, or `None` when there are no steps.
    pub slowest_step: Option<SlowestStep>,
    /// Count of steps per kind, keyed by the `snake_case` kind name.
    pub step_count_by_kind: BTreeMap<String, usize>,
}

/// The full per-execution timeline document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Timeline {
    /// The execution id (stringified).
    pub exec_id: String,
    /// The logical workflow id.
    pub workflow_id: String,
    /// The registered workflow type name.
    pub workflow_name: String,
    /// The execution's current state (e.g. `RUNNING`, `COMPLETED`).
    pub state: String,
    /// The ordered steps (by `scheduled_at`, stable by first appearance).
    pub steps: Vec<TimelineStep>,
    /// Aggregate duration rollup.
    pub rollup: TimelineRollup,
}

/// Derive a [`Timeline`] purely from an execution's recorded history.
///
/// `rows` must be the execution's events in append (event-id) order, each paired
/// with its row timestamp. `started_at` is the execution's start instant,
/// `completed_at` its terminal instant (or `None` if still running), and `now`
/// the current instant used to measure open steps. The remaining arguments are
/// echoed verbatim into the [`Timeline`] header.
///
/// This function never appends events, never touches the database, and is
/// deterministic given its inputs.
// The header arguments (`exec_id`/`workflow_id`/`workflow_name`/`state`) are
// echoed verbatim into the document and are part of the plugin-facing contract,
// so the arity is intentional.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn derive_timeline(
    rows: &[TimelineEventRow],
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    exec_id: String,
    workflow_id: String,
    workflow_name: String,
    state: String,
) -> Timeline {
    let clock_end = completed_at.unwrap_or(now);

    // Finalize each accumulator into a `TimelineStep`, then order by
    // `scheduled_at` with a stable tie-break on first-appearance index.
    let mut steps: Vec<(usize, TimelineStep)> = build_accs(rows, started_at)
        .into_iter()
        .map(|acc| (acc.first_index, acc.finalize(clock_end)))
        .collect();
    steps.sort_by(|(ia, a), (ib, b)| a.scheduled_at.cmp(&b.scheduled_at).then(ia.cmp(ib)));
    let steps: Vec<TimelineStep> = steps.into_iter().map(|(_, s)| s).collect();

    let rollup = compute_rollup(&steps, clock_end, started_at);

    Timeline {
        exec_id,
        workflow_id,
        workflow_name,
        state,
        steps,
        rollup,
    }
}

/// Scan the ordered history into one [`Acc`] per orchestration unit.
///
/// This is a wide dispatch match over the event variants that map to timeline
/// steps; all other events are ignored.
#[allow(clippy::too_many_lines)]
fn build_accs(rows: &[TimelineEventRow], started_at: DateTime<Utc>) -> Vec<Acc> {
    // `keyed` maps a stable key → index into `accs`; `accs` preserves first
    // appearance so the final ordering can break `scheduled_at` ties stably.
    let mut accs: Vec<Acc> = Vec::new();
    let mut keyed: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // Timestamp of the previous recorded row, for signal-wait measurement.
    let mut prev_ts: Option<DateTime<Utc>> = None;

    for (index, TimelineEventRow { timestamp, event }) in rows.iter().enumerate() {
        let ts = *timestamp;
        match event {
            // ── regular activity ─────────────────────────────────────
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } => ensure_scheduled(&mut accs, &mut keyed, format!("act:{activity_id}"), || {
                Acc::scheduled(
                    StepKind::Activity,
                    Some(name.clone()),
                    ts,
                    index,
                    true,
                    true,
                )
            }),
            WorkflowEvent::ActivityStarted { activity_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("act:{activity_id}")) {
                    // A (re)start reopens the step: the last attempt's start ts
                    // wins and any prior failure's terminal marking is cleared.
                    acc.start_ts = Some(ts);
                    acc.ended_at = None;
                    acc.outcome = None;
                }
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("act:{activity_id}")) {
                    acc.close(ts, StepOutcome::Completed);
                }
            }
            WorkflowEvent::ActivityFailed {
                activity_id,
                attempt,
                ..
            } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("act:{activity_id}")) {
                    acc.record_attempt(*attempt);
                    acc.close(ts, StepOutcome::Failed);
                }
            }
            WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("act:{activity_id}")) {
                    acc.close(ts, StepOutcome::TimedOut);
                }
            }

            // ── local activity ───────────────────────────────────────
            WorkflowEvent::LocalActivityScheduled {
                activity_id, name, ..
            } => ensure_scheduled(&mut accs, &mut keyed, format!("la:{activity_id}"), || {
                Acc::scheduled(
                    StepKind::LocalActivity,
                    Some(name.clone()),
                    ts,
                    index,
                    false,
                    true,
                )
            }),
            WorkflowEvent::LocalActivityCompleted { activity_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("la:{activity_id}")) {
                    acc.close(ts, StepOutcome::Completed);
                }
            }
            // A retryable `LocalActivityFailed` and the terminal
            // `LocalActivityExhausted` are handled identically: bump the attempt
            // counter and mark failed (last-writer-wins — a later `Completed`
            // overwrites a mid-retry failure).
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                attempt,
                ..
            }
            | WorkflowEvent::LocalActivityExhausted {
                activity_id,
                attempt,
                ..
            } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("la:{activity_id}")) {
                    acc.record_attempt(*attempt);
                    acc.close(ts, StepOutcome::Failed);
                }
            }

            // ── timer ────────────────────────────────────────────────
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                ensure_scheduled(&mut accs, &mut keyed, format!("timer:{timer_id}"), || {
                    Acc::scheduled(
                        StepKind::Timer,
                        Some(timer_id.to_string()),
                        ts,
                        index,
                        false,
                        false,
                    )
                });
            }
            WorkflowEvent::TimerFired { timer_id } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("timer:{timer_id}")) {
                    acc.close(ts, StepOutcome::Fired);
                }
            }

            // ── child workflow ───────────────────────────────────────
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => ensure_scheduled(&mut accs, &mut keyed, format!("child:{child_id}"), || {
                Acc::scheduled(
                    StepKind::ChildWorkflow,
                    Some(workflow_name.clone()),
                    ts,
                    index,
                    false,
                    false,
                )
            }),
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("child:{child_id}")) {
                    acc.close(ts, StepOutcome::Completed);
                }
            }
            WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                if let Some(acc) = lookup_mut(&keyed, &mut accs, &format!("child:{child_id}")) {
                    acc.close(ts, StepOutcome::Failed);
                }
            }

            // ── side effect (a point in time) ────────────────────────
            WorkflowEvent::SideEffectRecorded { name, .. } => {
                let mut acc =
                    Acc::scheduled(StepKind::SideEffect, name.clone(), ts, index, false, false);
                acc.close(ts, StepOutcome::Completed);
                accs.push(acc);
            }

            // ── signal wait (measured from the previous recorded event) ─
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                let scheduled_at = prev_ts.unwrap_or(started_at);
                let mut acc = Acc::scheduled(
                    StepKind::SignalWait,
                    Some(signal_name.clone()),
                    scheduled_at,
                    index,
                    false,
                    false,
                );
                acc.close(ts, StepOutcome::Completed);
                accs.push(acc);
            }

            // All other events (lifecycle, markers, updates, external
            // signals/cancels, heartbeats, detached-child, reset, redrive,
            // external activities, …) do not map to a timeline step.
            _ => {}
        }

        prev_ts = Some(ts);
    }

    accs
}

/// Aggregate the finalized steps into a [`TimelineRollup`].
fn compute_rollup(
    steps: &[TimelineStep],
    clock_end: DateTime<Utc>,
    started_at: DateTime<Utc>,
) -> TimelineRollup {
    let mut busy_ms: i64 = 0;
    let mut wait_ms: i64 = 0;
    let mut slowest: Option<SlowestStep> = None;
    let mut step_count_by_kind: BTreeMap<String, usize> = BTreeMap::new();

    for step in steps {
        *step_count_by_kind
            .entry(step.step_kind.as_str().to_string())
            .or_insert(0) += 1;

        match (step.step_kind, step.wait_ms, step.exec_ms) {
            (StepKind::Activity, Some(w), Some(e)) => {
                wait_ms += w;
                busy_ms += e;
            }
            (StepKind::Timer | StepKind::SignalWait, _, _) => wait_ms += step.total_ms,
            _ => busy_ms += step.total_ms,
        }

        if slowest.as_ref().is_none_or(|s| step.total_ms > s.total_ms) {
            slowest = Some(SlowestStep {
                name: step.name.clone(),
                step_kind: step.step_kind,
                total_ms: step.total_ms,
            });
        }
    }

    TimelineRollup {
        total_wall_clock_ms: (clock_end - started_at).num_milliseconds().max(0),
        busy_ms,
        wait_ms,
        slowest_step: slowest,
        step_count_by_kind,
    }
}

/// Non-negative milliseconds from `a` to `b` (clamped at 0 to tolerate clock
/// skew / out-of-order timestamps).
fn ms_between(a: DateTime<Utc>, b: DateTime<Utc>) -> i64 {
    (b - a).num_milliseconds().max(0)
}

/// Insert a freshly-scheduled accumulator under `key` iff none exists yet,
/// recording its index so terminal events can pair back to it.
fn ensure_scheduled(
    accs: &mut Vec<Acc>,
    keyed: &mut std::collections::HashMap<String, usize>,
    key: String,
    make: impl FnOnce() -> Acc,
) {
    if let std::collections::hash_map::Entry::Vacant(entry) = keyed.entry(key) {
        entry.insert(accs.len());
        accs.push(make());
    }
}

/// Look up a mutable accumulator by key, if present.
fn lookup_mut<'a>(
    keyed: &std::collections::HashMap<String, usize>,
    accs: &'a mut [Acc],
    key: &str,
) -> Option<&'a mut Acc> {
    keyed.get(key).map(|&i| &mut accs[i])
}

/// Internal per-unit accumulator built while scanning history.
struct Acc {
    step_kind: StepKind,
    name: Option<String>,
    scheduled_at: DateTime<Utc>,
    first_index: usize,
    /// Only meaningful for regular activities: the last recorded start instant.
    start_ts: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    outcome: Option<StepOutcome>,
    max_failed_attempt: Option<i32>,
    /// Whether the wait/exec split can ever apply (regular activities only).
    split_applicable: bool,
    /// Whether this kind reports an `attempt` (activities / local activities).
    retrying: bool,
}

impl Acc {
    const fn scheduled(
        step_kind: StepKind,
        name: Option<String>,
        scheduled_at: DateTime<Utc>,
        first_index: usize,
        split_applicable: bool,
        retrying: bool,
    ) -> Self {
        Self {
            step_kind,
            name,
            scheduled_at,
            first_index,
            start_ts: None,
            ended_at: None,
            outcome: None,
            max_failed_attempt: None,
            split_applicable,
            retrying,
        }
    }

    fn record_attempt(&mut self, attempt: u32) {
        let attempt = i32::try_from(attempt).unwrap_or(i32::MAX);
        self.max_failed_attempt = Some(self.max_failed_attempt.map_or(attempt, |m| m.max(attempt)));
    }

    /// Mark this step terminated (last-writer-wins).
    const fn close(&mut self, ended_at: DateTime<Utc>, outcome: StepOutcome) {
        self.ended_at = Some(ended_at);
        self.outcome = Some(outcome);
    }

    fn finalize(self, clock_end: DateTime<Utc>) -> TimelineStep {
        let (ended_at, outcome) = match self.outcome {
            Some(o) => (self.ended_at, o),
            None => (None, StepOutcome::Pending),
        };
        let endpoint = ended_at.unwrap_or(clock_end);
        let total_ms = ms_between(self.scheduled_at, endpoint);

        let (wait_ms, exec_ms) = if self.split_applicable {
            if let Some(start) = self.start_ts {
                (
                    Some(ms_between(self.scheduled_at, start)),
                    Some(ms_between(start, endpoint)),
                )
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let attempt = if self.retrying {
            Some(self.max_failed_attempt.unwrap_or(1))
        } else {
            None
        };

        TimelineStep {
            step_kind: self.step_kind,
            name: self.name,
            scheduled_at: self.scheduled_at,
            ended_at,
            total_ms,
            wait_ms,
            exec_ms,
            outcome,
            attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SideEffectKind;
    use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
    use chrono::{Duration, TimeZone};

    /// Base instant; step timestamps are `base + ms`.
    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn at(ms: i64) -> DateTime<Utc> {
        base() + Duration::milliseconds(ms)
    }

    fn row(ms: i64, event: WorkflowEvent) -> TimelineEventRow {
        TimelineEventRow {
            timestamp: at(ms),
            event,
        }
    }

    fn started(ms: i64) -> TimelineEventRow {
        row(
            ms,
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: at(ms),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
        )
    }

    fn find(steps: &[TimelineStep], kind: StepKind) -> &TimelineStep {
        steps
            .iter()
            .find(|s| s.step_kind == kind)
            .unwrap_or_else(|| panic!("no {kind:?} step"))
    }

    fn derive(rows: &[TimelineEventRow], completed_ms: Option<i64>, now_ms: i64) -> Timeline {
        derive_timeline(
            rows,
            at(0),
            at(now_ms),
            completed_ms.map(at),
            "exec".to_string(),
            "wf-1".to_string(),
            "onboarding".to_string(),
            "COMPLETED".to_string(),
        )
    }

    // ── Test 1: a completed run with activity + timer + child + side_effect ──

    #[test]
    #[allow(clippy::too_many_lines)]
    fn completed_run_ordering_math_split_slowest_rollup() {
        let a1 = ActivityExecId::new();
        let t1 = TimerId::new("t1");
        let c1 = ExecutionId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "send_email".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                30,
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                50,
                WorkflowEvent::TimerStarted {
                    timer_id: t1.clone(),
                    duration_secs: 5,
                },
            ),
            row(
                60,
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: c1,
                    workflow_name: "report".into(),
                    input: serde_json::Value::Null,
                },
            ),
            row(
                100,
                WorkflowEvent::ActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
            row(
                120,
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: c1,
                    output: serde_json::Value::Null,
                },
            ),
            row(
                130,
                WorkflowEvent::SideEffectRecorded {
                    kind: SideEffectKind::Now,
                    name: None,
                    value: serde_json::Value::Null,
                },
            ),
            row(200, WorkflowEvent::TimerFired { timer_id: t1 }),
        ];
        let tl = derive(&rows, Some(210), 210);

        // Ordering by scheduled_at: activity(10), timer(50), child(60), side_effect(130).
        assert_eq!(tl.steps.len(), 4);
        assert_eq!(tl.steps[0].step_kind, StepKind::Activity);
        assert_eq!(tl.steps[1].step_kind, StepKind::Timer);
        assert_eq!(tl.steps[2].step_kind, StepKind::ChildWorkflow);
        assert_eq!(tl.steps[3].step_kind, StepKind::SideEffect);

        // Activity: total 90, wait 20, exec 70, completed, attempt 1.
        let act = &tl.steps[0];
        assert_eq!(act.total_ms, 90);
        assert_eq!(act.wait_ms, Some(20));
        assert_eq!(act.exec_ms, Some(70));
        assert_eq!(act.outcome, StepOutcome::Completed);
        assert_eq!(act.attempt, Some(1));
        assert_eq!(act.ended_at, Some(at(100)));

        // Timer: total 150, fired, ended 200, split null.
        let tm = &tl.steps[1];
        assert_eq!(tm.total_ms, 150);
        assert_eq!(tm.outcome, StepOutcome::Fired);
        assert_eq!(tm.ended_at, Some(at(200)));
        assert_eq!(tm.wait_ms, None);
        assert_eq!(tm.exec_ms, None);
        assert_eq!(tm.name.as_deref(), Some("t1"));

        // Child: total 60, split ALWAYS null, completed.
        let ch = &tl.steps[2];
        assert_eq!(ch.total_ms, 60);
        assert_eq!(ch.wait_ms, None);
        assert_eq!(ch.exec_ms, None);
        assert_eq!(ch.outcome, StepOutcome::Completed);
        assert_eq!(ch.name.as_deref(), Some("report"));

        // Side effect: point step, total 0, completed.
        let se = &tl.steps[3];
        assert_eq!(se.total_ms, 0);
        assert_eq!(se.outcome, StepOutcome::Completed);
        assert_eq!(se.ended_at, Some(at(130)));

        // slowest = timer (150).
        let slow = tl.rollup.slowest_step.as_ref().unwrap();
        assert_eq!(slow.step_kind, StepKind::Timer);
        assert_eq!(slow.total_ms, 150);

        // step_count_by_kind.
        assert_eq!(tl.rollup.step_count_by_kind.get("activity"), Some(&1));
        assert_eq!(tl.rollup.step_count_by_kind.get("timer"), Some(&1));
        assert_eq!(tl.rollup.step_count_by_kind.get("child_workflow"), Some(&1));
        assert_eq!(tl.rollup.step_count_by_kind.get("side_effect"), Some(&1));

        // busy = exec(70) + child(60) + side_effect(0) = 130.
        assert_eq!(tl.rollup.busy_ms, 130);
        // wait = act wait(20) + timer(150) = 170.
        assert_eq!(tl.rollup.wait_ms, 170);
        // total wall clock = 210 - 0.
        assert_eq!(tl.rollup.total_wall_clock_ms, 210);
    }

    // ── Test 2: activity with NO ActivityStarted → split null ──

    #[test]
    fn activity_without_start_has_no_split() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "x".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                100,
                WorkflowEvent::ActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
        ];
        let tl = derive(&rows, Some(100), 100);
        let act = find(&tl.steps, StepKind::Activity);
        assert_eq!(act.total_ms, 90);
        assert_eq!(act.wait_ms, None);
        assert_eq!(act.exec_ms, None);
        assert_eq!(act.outcome, StepOutcome::Completed);
        // Rollup: no split → busy = total.
        assert_eq!(tl.rollup.busy_ms, 90);
        assert_eq!(tl.rollup.wait_ms, 0);
    }

    // ── Test 3: in-flight run → pending, measured to now ──

    #[test]
    fn in_flight_activity_is_pending_measured_to_now() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "x".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
        ];
        let tl = derive(&rows, None, 500);
        let act = find(&tl.steps, StepKind::Activity);
        assert_eq!(act.outcome, StepOutcome::Pending);
        assert_eq!(act.ended_at, None);
        assert_eq!(act.total_ms, 490);
        // total wall clock measured to now.
        assert_eq!(tl.rollup.total_wall_clock_ms, 500);
    }

    // ── Test 4: retried activity → attempt = max failed attempt ──

    #[test]
    fn retried_activity_reports_max_attempt_and_final_split() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "flaky".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                15,
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                20,
                WorkflowEvent::ActivityFailed {
                    activity_id: a1,
                    error: "boom".into(),
                    attempt: 1,
                    error_type: "Error".into(),
                    non_retryable: false,
                    details: None,
                },
            ),
            row(
                25,
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                30,
                WorkflowEvent::ActivityFailed {
                    activity_id: a1,
                    error: "boom".into(),
                    attempt: 2,
                    error_type: "Error".into(),
                    non_retryable: false,
                    details: None,
                },
            ),
            row(
                35,
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                50,
                WorkflowEvent::ActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
        ];
        let tl = derive(&rows, Some(60), 60);
        let act = find(&tl.steps, StepKind::Activity);
        assert_eq!(act.attempt, Some(2), "max failed attempt");
        assert_eq!(act.outcome, StepOutcome::Completed);
        // final attempt's start @ 35: exec = 50-35 = 15, wait = 35-10 = 25, total = 40.
        assert_eq!(act.exec_ms, Some(15));
        assert_eq!(act.wait_ms, Some(25));
        assert_eq!(act.total_ms, 40);
    }

    // ── Test 5: signal_wait measured from previous event, counted as wait ──

    #[test]
    fn signal_wait_measured_from_previous_event() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "x".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                12,
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                40,
                WorkflowEvent::ActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
            row(
                100,
                WorkflowEvent::SignalReceived {
                    signal_name: "approval".into(),
                    payload: serde_json::Value::Null,
                },
            ),
        ];
        let tl = derive(&rows, Some(110), 110);
        let sig = find(&tl.steps, StepKind::SignalWait);
        assert_eq!(sig.name.as_deref(), Some("approval"));
        assert_eq!(sig.scheduled_at, at(40), "measured from prev event ts");
        assert_eq!(sig.ended_at, Some(at(100)));
        assert_eq!(sig.total_ms, 60);
        assert_eq!(sig.outcome, StepOutcome::Completed);
        assert_eq!(sig.wait_ms, None);
        assert_eq!(sig.exec_ms, None);
        // signal_wait counts toward wait_ms.
        assert!(
            tl.rollup.wait_ms >= 60,
            "signal wait must contribute to wait_ms, got {}",
            tl.rollup.wait_ms
        );
    }

    #[test]
    fn signal_wait_as_first_event_measured_from_started_at() {
        let rows = vec![
            started(0),
            row(
                80,
                WorkflowEvent::SignalReceived {
                    signal_name: "kick".into(),
                    payload: serde_json::Value::Null,
                },
            ),
        ];
        // Only WorkflowStarted precedes the signal → measured from started_at (0).
        let tl = derive(&rows, Some(90), 90);
        let sig = find(&tl.steps, StepKind::SignalWait);
        assert_eq!(sig.scheduled_at, at(0));
        assert_eq!(sig.total_ms, 80);
    }

    // ── Test 6: empty history (only WorkflowStarted) ──

    #[test]
    fn empty_history_has_no_steps() {
        let rows = vec![started(0)];
        let tl = derive(&rows, Some(50), 50);
        assert!(tl.steps.is_empty());
        assert!(tl.rollup.slowest_step.is_none());
        assert_eq!(tl.rollup.busy_ms, 0);
        assert_eq!(tl.rollup.wait_ms, 0);
        assert_eq!(tl.rollup.total_wall_clock_ms, 50);
        assert!(tl.rollup.step_count_by_kind.is_empty());
    }

    // ── Test 7: negative/out-of-order timestamps clamp to 0 without panic ──

    #[test]
    fn out_of_order_timestamps_clamp_and_do_not_panic() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                100,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "x".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                50, // started BEFORE scheduled (clock skew)
                WorkflowEvent::ActivityStarted {
                    activity_id: a1,
                    worker_id: WorkerId::new("w"),
                },
            ),
            row(
                40, // completed before started
                WorkflowEvent::ActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
        ];
        let tl = derive(&rows, Some(40), 40);
        let act = find(&tl.steps, StepKind::Activity);
        assert_eq!(act.total_ms, 0, "clamped");
        assert_eq!(act.wait_ms, Some(0), "clamped");
        assert_eq!(act.exec_ms, Some(0), "clamped");
        assert!(tl.rollup.busy_ms >= 0);
        assert!(tl.rollup.wait_ms >= 0);
    }

    // ── local activity: failed/exhausted terminal, attempt derivation ──

    #[test]
    fn local_activity_exhausted_is_failed_with_attempt() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::LocalActivityScheduled {
                    activity_id: a1,
                    name: "checksum".into(),
                    input: serde_json::Value::Null,
                },
            ),
            row(
                20,
                WorkflowEvent::LocalActivityFailed {
                    activity_id: a1,
                    error: "boom".into(),
                    attempt: 1,
                },
            ),
            row(
                30,
                WorkflowEvent::LocalActivityFailed {
                    activity_id: a1,
                    error: "boom".into(),
                    attempt: 2,
                },
            ),
            row(
                40,
                WorkflowEvent::LocalActivityExhausted {
                    activity_id: a1,
                    error: "boom".into(),
                    attempt: 3,
                },
            ),
        ];
        let tl = derive(&rows, Some(50), 50);
        let la = find(&tl.steps, StepKind::LocalActivity);
        assert_eq!(la.outcome, StepOutcome::Failed);
        assert_eq!(la.attempt, Some(3));
        assert_eq!(la.total_ms, 30); // 40 - 10
        assert_eq!(la.wait_ms, None);
        assert_eq!(la.exec_ms, None);
        // local contributes total to busy.
        assert_eq!(tl.rollup.busy_ms, 30);
    }

    #[test]
    fn local_activity_completed_first_try_attempt_one() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::LocalActivityScheduled {
                    activity_id: a1,
                    name: "checksum".into(),
                    input: serde_json::Value::Null,
                },
            ),
            row(
                25,
                WorkflowEvent::LocalActivityCompleted {
                    activity_id: a1,
                    output: serde_json::Value::Null,
                },
            ),
        ];
        let tl = derive(&rows, Some(30), 30);
        let la = find(&tl.steps, StepKind::LocalActivity);
        assert_eq!(la.outcome, StepOutcome::Completed);
        assert_eq!(la.attempt, Some(1));
        assert_eq!(la.total_ms, 15);
    }

    // ── activity timed out ──

    #[test]
    fn activity_timed_out_outcome() {
        let a1 = ActivityExecId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ActivityScheduled {
                    activity_id: a1,
                    name: "slow".into(),
                    input: serde_json::Value::Null,
                    queue: "default".into(),
                },
            ),
            row(
                200,
                WorkflowEvent::ActivityTimedOut {
                    activity_id: a1,
                    timeout_type: crate::error::TimeoutType::StartToClose,
                },
            ),
        ];
        let tl = derive(&rows, Some(200), 200);
        let act = find(&tl.steps, StepKind::Activity);
        assert_eq!(act.outcome, StepOutcome::TimedOut);
        assert_eq!(act.attempt, Some(1));
        assert_eq!(act.total_ms, 190);
    }

    // ── child workflow failed ──

    #[test]
    fn child_workflow_failed_outcome_no_split() {
        let c1 = ExecutionId::new();
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: c1,
                    workflow_name: "sub".into(),
                    input: serde_json::Value::Null,
                },
            ),
            row(
                90,
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: c1,
                    error: "nope".into(),
                },
            ),
        ];
        let tl = derive(&rows, Some(90), 90);
        let ch = find(&tl.steps, StepKind::ChildWorkflow);
        assert_eq!(ch.outcome, StepOutcome::Failed);
        assert_eq!(ch.wait_ms, None);
        assert_eq!(ch.exec_ms, None);
        assert_eq!(ch.total_ms, 80);
        assert_eq!(ch.attempt, None);
    }

    // ── in-flight timer stays pending ──

    #[test]
    fn unfired_timer_is_pending() {
        let t1 = TimerId::new("nap");
        let rows = vec![
            started(0),
            row(
                10,
                WorkflowEvent::TimerStarted {
                    timer_id: t1,
                    duration_secs: 3600,
                },
            ),
        ];
        let tl = derive(&rows, None, 1000);
        let tm = find(&tl.steps, StepKind::Timer);
        assert_eq!(tm.outcome, StepOutcome::Pending);
        assert_eq!(tm.ended_at, None);
        assert_eq!(tm.total_ms, 990);
    }

    // ── serde round-trip of the public document ──

    #[test]
    fn timeline_serializes_snake_case() {
        let tl = derive(&[started(0)], Some(50), 50);
        let json = serde_json::to_value(&tl).unwrap();
        assert!(json.get("rollup").is_some());
        // enum snake_case check via a constructed step doc.
        let step = TimelineStep {
            step_kind: StepKind::LocalActivity,
            name: Some("x".into()),
            scheduled_at: at(0),
            ended_at: None,
            total_ms: 1,
            wait_ms: None,
            exec_ms: None,
            outcome: StepOutcome::TimedOut,
            attempt: Some(2),
        };
        let sj = serde_json::to_value(&step).unwrap();
        assert_eq!(sj["step_kind"], "local_activity");
        assert_eq!(sj["outcome"], "timed_out");
        let back: TimelineStep = serde_json::from_value(sj).unwrap();
        assert_eq!(back, step);
    }
}
