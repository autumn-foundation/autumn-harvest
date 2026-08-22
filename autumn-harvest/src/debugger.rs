//! Time-travel replay debugger for recorded workflow histories (issue #949).
//!
//! # What this is
//!
//! A **stepping and diffing API over the existing replay machinery**. Given a
//! recorded history (typically an exported
//! [`HistorySnapshot`](crate::testing::HistorySnapshot)) it produces a
//! [`ReplayTrace`]: one [`DebugStep`] per consumed event, each carrying the
//! event index and type, the workflow's pending [`WorkflowCommand`]s at that
//! point, the open awaitables (with the event index that opened each), the
//! resolved version- and patch-gate values, and the recorded side-effect and
//! marker values.
//!
//! # How it steps
//!
//! Replay is deterministic, so "stepping" is **prefix replay**: step `k` is a
//! fresh replay of `events[0..=k]`. This is exactly the mechanism issue #949
//! specifies ("re-replay from the top to that index — replay is deterministic,
//! so 'backward' is re-run-forward") and it needs **no engine hook**.
//!
//! The load-bearing insight: on a replay, replay-significant commands are only
//! emitted at the **live frontier**. So replaying a prefix yields precisely
//! *"what does this code do next, given history so far"* — a per-step frontier
//! command batch, and the ideal diff signal.
//!
//! # Read-only by construction (AC5)
//!
//! No new [`WorkflowEvent`] variant, no migration, no engine-runtime change.
//! Every primitive used here was already `pub`:
//! [`WorkflowContext::for_replay_canary_with_state`],
//! [`crate::executor::drive_query_replay`],
//! [`WorkflowContext::drain_commands`]. Nothing is persisted, no activity runs,
//! and no database is touched.
//!
//! Prefix replay uses **canary** (frontier-tolerant) mode rather than strict:
//! a prefix of a history is exactly an in-flight execution parked at its
//! frontier, which strict replay would report as a spurious divergence.
//!
//! # Cost
//!
//! Prefix replay is O(N²) in history length. [`ReplayDebugger::max_steps`]
//! caps it (default [`DEFAULT_MAX_STEPS`]) and the resulting trace sets
//! [`ReplayTrace::truncated`]. The handler-free
//! [`ReplayTrace::from_history`] projection is always O(N) and needs no
//! registered code at all.
//!
//! # Serialization is a display surface, not a round-trip format
//!
//! The snapshot types derive [`serde::Serialize`] but deliberately **not**
//! `Deserialize`: they exist so `--format json` can be piped into `jq`, not so
//! a trace can be persisted and read back. Several fields
//! ([`CommandSnapshot::summary`], the rendered `event_type`) are *renderings*
//! of engine types rather than the types themselves, so a round-trip would be
//! lossy in ways a `Deserialize` impl would silently paper over. The
//! round-trippable artefact is the **history** ([`HistorySnapshot`]) — feed
//! that back in and rebuild the trace.
//!
//! # Relationship to issue #614
//!
//! `POST /workflows/{id}/replay-diagnosis` (#614) is the **one-shot,
//! server-side** answer for a single live execution: "does this run diverge
//! under the code deployed right now?". This debugger is the **local,
//! interactive** counterpart: it works offline on an exported history, steps
//! through it, and diffs *two* code registrations against each other. They
//! complement rather than duplicate: #614 tells you *that* a run diverges;
//! this tells you *where*, step by step, and *how the two builds differ*.
//!
//! # Gates at a truncated frontier
//!
//! Because a step replays `events[0..=k]`, an **intermediate** step models an
//! execution parked at that truncated frontier. A [`ctx.patched`] /
//! [`ctx.version`] gate reached there is therefore, correctly, *newly* patched:
//! it records its marker and returns `true`, exactly as a live execution
//! reaching that point for the first time would.
//!
//! So a gated build legitimately differs from an ungated one at intermediate
//! steps even when it replays the *full* recorded history perfectly. Two
//! questions, two checks:
//!
//! * "Where do these two builds first behave differently?" —
//!   [`diff_traces`]`(a, b).divergence`.
//! * "Does this build replay this history **cleanly**?" — no step has
//!   `divergence.is_some()`.
//!
//! Use the second to confirm a fix. See `docs/replay-debugger.md`.
//!
//! [`ctx.patched`]: crate::context::WorkflowContext::patched
//! [`ctx.version`]: crate::context::WorkflowContext::version

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::time::Duration;

use serde_json::Value;

pub use crate::awaitables::AwaitableKind;
use crate::context::{
    SharedState, WorkflowCommand, WorkflowContext, WorkflowHistoryPolicy, empty_shared_state,
};
use crate::event::WorkflowEvent;
use crate::executor::{QueryReplayOutcome, drive_query_replay_async};
use crate::info::WorkflowHandlerFn;
use crate::testing::HistorySnapshot;
use crate::types::ExecutionId;

/// Default cap on the number of prefix replays a single trace performs.
///
/// Prefix replay is O(N²); this bounds a pathological history to a bounded
/// amount of work. A capped trace sets [`ReplayTrace::truncated`].
pub const DEFAULT_MAX_STEPS: usize = 500;

/// Default per-step drive budget. A spinning workflow (a `yield_now` loop)
/// yields [`StepOutcome::TimedOut`] for that step rather than hanging the
/// debugger.
pub const DEFAULT_STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// Marker-name prefix recorded by `ctx.version(change_id, ..)`.
///
/// The engine builds the full name with [`crate::replay::version_marker_name`],
/// which returns a `String` rather than exposing a prefix — so this constant is
/// a *duplicate* of that convention, and `marker_prefixes_match_the_engine`
/// below fails the build if the two ever drift apart.
const VERSION_MARKER_PREFIX: &str = "version:";
/// Marker-name prefix recorded by `ctx.patched(patch_id)`.
///
/// Drift-guarded against [`crate::replay::patch_marker_name`] — see
/// [`VERSION_MARKER_PREFIX`].
const PATCH_MARKER_PREFIX: &str = "patch:";
/// Placeholder for a divergence side the engine did not populate.
const UNKNOWN: &str = "<unknown>";

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot types
// ─────────────────────────────────────────────────────────────────────────────

/// One pending [`WorkflowCommand`], rendered for display and comparison.
///
/// Deliberately a *rendered* projection rather than the command itself:
/// [`WorkflowCommand`] carries oneshot result channels that are neither
/// `Clone`, `PartialEq`, nor `Serialize`.
///
/// Display and identity are **separated on purpose**. `summary` is a short,
/// human-readable one-liner for a terminal; `payload` is the command's exact
/// input value. Diffing compares the whole struct, so a divergence buried deep
/// inside a large payload is still caught exactly — truncating the payload into
/// `summary` and comparing only that would silently hide it, which on a
/// divergence-finding tool is the worst possible failure mode.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CommandSnapshot {
    /// The command's variant name, e.g. `"ScheduleActivity"`.
    pub kind: &'static str,
    /// A one-line human-readable rendering, e.g. `"step_a -> default"`.
    pub summary: String,
    /// The command's exact input/payload value, where it carries one.
    ///
    /// Compared verbatim when diffing, so a changed activity input is detected
    /// at the step where the code first emits it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl fmt::Display for CommandSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.summary.is_empty() {
            write!(f, "{}", self.kind)
        } else {
            write!(f, "{}({})", self.kind, self.summary)
        }
    }
}

/// One awaitable the execution has opened and not yet closed, as of a step.
///
/// Carries the **event index that opened it** — the projection in
/// [`crate::awaitables`] is timestamp-based and operates on rows a
/// [`HistorySnapshot`] does not carry, so this is a debugger-owned projection
/// that reuses only that module's [`AwaitableKind`] vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OpenAwaitable {
    /// The awaitable's category.
    pub kind: AwaitableKind,
    /// Handler / signal / mutex name where one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Stable identifier where one exists (activity exec id, timer id, ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The 0-based index of the history event that opened this awaitable.
    pub opened_at: usize,
}

/// A recorded deterministic side effect (`ctx.system_now`, `ctx.new_uuid`,
/// `ctx.random_*`, `ctx.side_effect`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SideEffectSnapshot {
    /// The bounded `SideEffectKind` label (`"now"` / `"uuid"` / `"random"` /
    /// `"custom"`).
    pub kind: &'static str,
    /// The author-supplied dedup key for `ctx.side_effect`; `None` for built-ins.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The recorded value, verbatim — including a codec or offload envelope
    /// (AC4: envelopes display as envelopes, they are never decoded here).
    pub value: Value,
    /// The 0-based index of the recording event.
    pub event_index: usize,
}

/// A recorded `MarkerRecorded` event.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MarkerSnapshot {
    /// The marker name, verbatim (`"fan_out:1"`, `"race_winner:2"`, ...).
    pub name: String,
    /// The recorded details value.
    pub details: Value,
    /// The 0-based index of the recording event.
    pub event_index: usize,
}

/// Where the workflow stopped when this step's prefix was replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StepOutcome {
    /// No prefix replay was performed (the handler-free
    /// [`ReplayTrace::from_history`] projection).
    NotReplayed,
    /// The workflow parked on a durable command — the normal case for a step
    /// whose prefix ends mid-execution.
    Suspended,
    /// The workflow function returned (completed or failed).
    ReachedTerminal,
    /// The per-step drive budget elapsed — a spinning workflow.
    TimedOut,
    /// The workflow handler panicked (contained; the debugger survives).
    Panicked,
}

impl StepOutcome {
    /// The stable `snake_case` label for this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotReplayed => "not_replayed",
            Self::Suspended => "suspended",
            Self::ReachedTerminal => "reached_terminal",
            Self::TimedOut => "timed_out",
            Self::Panicked => "panicked",
        }
    }
}

/// A non-determinism divergence surfaced while replaying a step's prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StepDivergence {
    /// What the recorded history expected at this point.
    pub expected: String,
    /// What the registered code did instead.
    pub actual: String,
    /// The history event index the engine reported the divergence at.
    pub event_index: usize,
}

/// The debugger's view of the execution after consuming `events[0..=index]`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DebugStep {
    /// 0-based index of the history event this step consumed.
    pub index: usize,
    /// The consumed event's `type_name()`.
    pub event_type: &'static str,
    /// Where the workflow stopped when this prefix was replayed.
    pub outcome: StepOutcome,
    /// The workflow's pending commands at this point — its *frontier*.
    ///
    /// Empty for a handler-free trace ([`ReplayTrace::from_history`]).
    pub commands: Vec<CommandSnapshot>,
    /// Awaitables opened and not yet closed, in the order they were opened.
    pub open_awaitables: Vec<OpenAwaitable>,
    /// Resolved `ctx.version(change_id, ..)` gate values seen so far.
    pub version_gates: BTreeMap<String, i64>,
    /// `ctx.patched(patch_id)` gates recorded so far.
    pub patch_gates: BTreeSet<String>,
    /// Recorded deterministic side effects seen so far.
    pub side_effects: Vec<SideEffectSnapshot>,
    /// Recorded markers seen so far (excluding the version/patch gates above,
    /// which are surfaced in their own resolved form).
    pub markers: Vec<MarkerSnapshot>,
    /// The payload this event resolved, when it carries one (an activity or
    /// child output, a signal payload, a marker's details).
    ///
    /// AC4: surfaced **verbatim**. A codec or offload envelope displays as the
    /// envelope; the debugger never decodes and never errors on one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_payload: Option<Value>,
    /// The event's complete semantic content with per-run identity normalized
    /// out — the key two fixture histories are diffed on.
    ///
    /// `resolved_payload` above is a *display* projection; this is the
    /// comparison key, and it is exhaustive by construction (it is the
    /// serialized event), so a `WorkflowEvent` field this debugger has never
    /// heard of still participates in the diff. See [`normalized_event_facts`].
    pub event_facts: Value,
    /// The signal name, when this step consumed a `SignalReceived` event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
    /// A non-determinism divergence detected while replaying this prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub divergence: Option<StepDivergence>,
}

/// The full stepped trace of one recorded history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReplayTrace {
    /// The workflow type name the history belongs to.
    pub workflow_name: String,
    /// The execution the history was recorded for.
    pub execution_id: ExecutionId,
    /// One step per consumed event, in order.
    pub steps: Vec<DebugStep>,
    /// The number of events in the source history (may exceed `steps.len()`).
    pub total_events: usize,
    /// `true` when `steps` was capped by [`ReplayDebugger::max_steps`].
    pub truncated: bool,
}

impl ReplayTrace {
    /// Build the **handler-free** projection: everything AC1 asks for except
    /// the per-step pending commands (which require registered workflow code).
    ///
    /// Pure and O(N) — no replay, no handler, no I/O. This is the always-
    /// available "explain this history" view, and the substrate the
    /// command-carrying [`ReplayDebugger`] traces are built on.
    #[must_use]
    pub fn from_history(
        workflow_name: impl Into<String>,
        execution_id: ExecutionId,
        events: &[WorkflowEvent],
    ) -> Self {
        Self::from_history_capped(workflow_name, execution_id, events, usize::MAX)
    }

    /// [`from_history`](Self::from_history) with an explicit step cap.
    #[must_use]
    pub fn from_history_capped(
        workflow_name: impl Into<String>,
        execution_id: ExecutionId,
        events: &[WorkflowEvent],
        max_steps: usize,
    ) -> Self {
        let total_events = events.len();
        let limit = max_steps.min(total_events);
        let mut acc = HistoryAccumulator::default();
        let mut steps = Vec::with_capacity(limit);

        for (index, event) in events.iter().take(limit).enumerate() {
            acc.apply(index, event);
            steps.push(DebugStep {
                index,
                event_type: event.type_name(),
                outcome: StepOutcome::NotReplayed,
                commands: Vec::new(),
                open_awaitables: acc.open_awaitables(),
                version_gates: acc.version_gates.clone(),
                patch_gates: acc.patch_gates.clone(),
                side_effects: acc.side_effects.clone(),
                markers: acc.markers.clone(),
                resolved_payload: resolved_payload(event),
                event_facts: normalized_event_facts(event),
                signal_name: signal_name_of(event),
                divergence: None,
            });
        }

        Self {
            workflow_name: workflow_name.into(),
            execution_id,
            steps,
            total_events,
            truncated: limit < total_events,
        }
    }

    /// Find the first step at or after `from` that satisfies `breakpoint`.
    ///
    /// Returns the step index, or `None` when no step matches — the
    /// "run-to-breakpoint" primitive of AC2.
    #[must_use]
    pub fn find_breakpoint(&self, breakpoint: &Breakpoint, from: usize) -> Option<usize> {
        self.steps
            .iter()
            .skip(from)
            .find(|step| breakpoint.matches(step))
            .map(|step| step.index)
    }

    /// The step at `index`, if the trace reaches that far.
    #[must_use]
    pub fn step(&self, index: usize) -> Option<&DebugStep> {
        self.steps.get(index)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Breakpoints (AC2)
// ─────────────────────────────────────────────────────────────────────────────

/// A run-to-breakpoint predicate over an already-built [`DebugStep`].
///
/// Pure: matching never re-runs the replay, so a breakpoint is cheap to
/// re-evaluate and a debugger session can scan forward and backward freely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Breakpoint {
    /// Break on a history event whose `type_name()` equals this string.
    EventType(String),
    /// Break at exactly this event index.
    EventIndex(usize),
    /// Break when this activity is scheduled — either by the recorded event
    /// (`ActivityScheduled` / `LocalActivityScheduled`) or by a pending
    /// `ScheduleActivity` / `RunLocalActivity` command at the frontier.
    ActivityName(String),
    /// Break when this signal is received, or when the workflow parks waiting
    /// for it.
    SignalName(String),
    /// Break at the first step whose prefix replay reported a divergence.
    FirstDivergence,
}

impl Breakpoint {
    /// Does `step` satisfy this breakpoint?
    #[must_use]
    pub fn matches(&self, step: &DebugStep) -> bool {
        match self {
            Self::EventType(t) => step.event_type == t,
            Self::EventIndex(i) => step.index == *i,
            Self::ActivityName(name) => step_names_activity(step, name),
            Self::SignalName(name) => step_names_signal(step, name),
            Self::FirstDivergence => step.divergence.is_some(),
        }
    }
}

/// `true` when the step's own event, its open awaitables, or its frontier
/// commands name this activity.
fn step_names_activity(step: &DebugStep, name: &str) -> bool {
    let opened_here = step.open_awaitables.iter().any(|a| {
        a.opened_at == step.index
            && a.kind == AwaitableKind::Activity
            && a.name.as_deref() == Some(name)
    });
    opened_here
        || step.commands.iter().any(|c| {
            matches!(
                c.kind,
                "ScheduleActivity" | "RunLocalActivity" | "ScheduleExternalActivity"
            )
            // Only the *first* segment: `ScheduleActivity`'s summary is
            // `"{name} -> {queue}"`, so matching any segment would make
            // `ActivityName("payments")` hit every activity on the `payments`
            // queue — a false breakpoint on an unrelated step.
            && first_segment(&c.summary) == name
        })
}

/// `true` when the step's own event, or its frontier commands, name this signal.
fn step_names_signal(step: &DebugStep, name: &str) -> bool {
    // A `SignalReceived` event opens no awaitable, so it is matched via the
    // step's own `signal_name` projection rather than the open set.
    let received_here = step.signal_name.as_deref() == Some(name);
    let waiting = step
        .commands
        .iter()
        // Exact, not segment-wise: a `WaitForSignal` summary *is* the signal
        // name with no qualifier appended (unlike `ScheduleActivity`'s
        // `"{name} -> {queue}"`), and a signal name may legitimately contain
        // spaces — nothing in the engine or the `POST .../signal/{name}` route
        // restricts it to one token. Splitting on whitespace made
        // `SignalName("order approved")` unmatchable at exactly the step a
        // breakpoint is for: a waiting frontier, where no `SignalReceived`
        // exists yet to match instead.
        .any(|c| c.kind == "WaitForSignal" && c.summary == name);
    received_here || waiting
}

/// The signal name, when `event` is a `SignalReceived`.
fn signal_name_of(event: &WorkflowEvent) -> Option<String> {
    match event {
        WorkflowEvent::SignalReceived { signal_name, .. } => Some(signal_name.clone()),
        _ => None,
    }
}

/// The leading segment of a rendered command summary — the command's own
/// name, before any ` -> queue` or ` (…)` qualifier.
fn first_segment(summary: &str) -> &str {
    summary
        .split([' ', ',', '(', ')', '='])
        .next()
        .unwrap_or(summary)
}

// ─────────────────────────────────────────────────────────────────────────────
// Diff mode (AC3)
// ─────────────────────────────────────────────────────────────────────────────

/// How two traces first differ.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[non_exhaustive]
pub enum DiffKind {
    /// The two *histories* themselves differ at this step.
    ///
    /// Every field of a [`DebugStep`] except `commands`/`outcome` is derived
    /// from the recorded history alone, so a difference here means the two
    /// traces are not replaying the same recording. This is the signal that
    /// drives the **two fixture histories** arm of AC3 (and the only signal
    /// available when neither side has registered workflow code, since a
    /// handler-free trace emits no commands at all).
    HistoryFacts {
        /// Which derived field differs, e.g. `"event_type"` or
        /// `"open_awaitables"`.
        field: &'static str,
    },
    /// Both sides emitted commands, but a specific command differs.
    CommandMismatch {
        /// The 0-based index within the step's command list.
        command_index: usize,
    },
    /// The two sides emitted a different *number* of commands at this step.
    CommandCount {
        /// How many commands the left trace emitted.
        left: usize,
        /// How many commands the right trace emitted.
        right: usize,
    },
    /// The two sides stopped in different places (e.g. one completed, the
    /// other suspended).
    Outcome {
        /// The left trace's outcome.
        left: StepOutcome,
        /// The right trace's outcome.
        right: StepOutcome,
    },
    /// Exactly one side recorded a replay divergence at this step.
    ///
    /// A divergence is a *code-vs-history* fact (one build's code no longer
    /// matches the recording), so it is reported as its own kind rather than
    /// folded into [`DiffKind::HistoryFacts`] — the two recordings are
    /// identical here; the two *builds* are not.
    Divergence {
        /// The left trace's divergence, if it had one.
        #[serde(skip_serializing_if = "Option::is_none")]
        left: Option<StepDivergence>,
        /// The right trace's divergence, if it had one.
        #[serde(skip_serializing_if = "Option::is_none")]
        right: Option<StepDivergence>,
    },
    /// The two traces belong to **different workflow types**.
    ///
    /// `workflow_name` is an execution-*row* column — it appears in no
    /// `WorkflowEvent` (not even `WorkflowStarted`), so `event_facts` cannot
    /// see it. It also selects the handler that owns and interprets the
    /// history, which makes it the one piece of metadata that must agree
    /// before comparing anything else: two recordings of *different workflows*
    /// are not "in agreement" no matter how similar their event arrays look.
    ///
    /// `execution_id` is deliberately **not** compared alongside it. Two
    /// independent recordings always carry different execution ids, so
    /// comparing it would fabricate a divergence on every fixture-vs-fixture
    /// diff — the same reason it is normalized out of `event_facts`.
    WorkflowName {
        /// The left trace's workflow type.
        left: String,
        /// The right trace's workflow type.
        right: String,
    },
    /// One trace ran out of steps before the other.
    TraceLength {
        /// The left trace's step count.
        left: usize,
        /// The right trace's step count.
        right: usize,
        /// `true` when at least one side was capped by `max_steps`, so the
        /// length difference is a **cap artefact** rather than a property of
        /// the two traces. Diffing traces built with different caps is a
        /// caller error; this field makes it self-diagnosing instead of
        /// silently reporting a divergence that does not exist.
        capped: bool,
    },
}

/// The first point at which two traces diverge, with both sides' snapshots.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TraceDivergence {
    /// The step index at which the two traces first differ.
    pub step_index: usize,
    /// How they differ.
    pub kind: DiffKind,
    /// The left trace's snapshot at this step. `None` only for
    /// [`DiffKind::TraceLength`] when the left trace is the shorter one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<DebugStep>,
    /// The right trace's snapshot at this step. `None` only for
    /// [`DiffKind::TraceLength`] when the right trace is the shorter one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<DebugStep>,
}

/// The result of diffing two traces of the same history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TraceDiff {
    /// The first divergence, or `None` when the two traces agree throughout.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub divergence: Option<TraceDivergence>,
    /// How many steps the left trace produced.
    pub left_steps: usize,
    /// How many steps the right trace produced.
    pub right_steps: usize,
    /// `true` when either input trace was capped by
    /// [`ReplayDebugger::max_steps`], so the comparison examined only a
    /// **prefix** of the two histories.
    ///
    /// This is why callers must ask [`TraceDiff::is_clean`] rather than
    /// testing `divergence.is_none()`: on a capped trace a `None` divergence
    /// means "no difference in the part we looked at", which is emphatically
    /// not the same claim.
    pub truncated: bool,
}

impl TraceDiff {
    /// `true` only when the two traces were compared **in full** and agree.
    ///
    /// `divergence.is_none()` alone is **not** the clean signal. When both
    /// traces hit the same `max_steps` cap their step counts are equal, so the
    /// [`DiffKind::TraceLength`] check — the only one carrying a `capped`
    /// flag — never fires, and a difference living past the cap is reported as
    /// silence. A CI gate reading `divergence.is_none()` would treat an
    /// unexamined suffix as a pass; this method refuses to.
    ///
    /// Raise `max_steps` (or drop the cap) to turn an inconclusive result into
    /// a conclusive one.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.divergence.is_none() && !self.truncated
    }
}

/// Diff two traces of the same history and report the **first** divergent
/// command (AC3).
///
/// The comparison walks steps in order and stops at the first difference, so a
/// divergence cascade (one root cause producing dozens of downstream
/// differences) reports exactly one actionable result.
///
/// Read the verdict with [`TraceDiff::is_clean`], **not** with
/// `divergence.is_none()`: when both traces were capped by `max_steps` only a
/// prefix was compared, and a difference past the cap is indistinguishable
/// from agreement.
#[must_use]
pub fn diff_traces(left: &ReplayTrace, right: &ReplayTrace) -> TraceDiff {
    let truncated = left.truncated || right.truncated;
    let finish = |divergence| TraceDiff {
        divergence,
        left_steps: left.steps.len(),
        right_steps: right.steps.len(),
        truncated,
    };

    // Before any step: the two recordings must belong to the same workflow
    // type. `workflow_name` lives on the execution row, not in any
    // `WorkflowEvent`, so `event_facts` is structurally blind to it — two
    // recordings of *different* workflows with similarly-shaped event arrays
    // would otherwise walk every step and report agreement. Same principle as
    // `step_diff_kind`'s history-facts-first ordering, one level up: if the
    // recordings are not of the same thing, comparing what the code did next
    // is apples to oranges.
    if left.workflow_name != right.workflow_name {
        return finish(Some(TraceDivergence {
            step_index: 0,
            kind: DiffKind::WorkflowName {
                left: left.workflow_name.clone(),
                right: right.workflow_name.clone(),
            },
            // Trace-level, not step-level: the names are carried by the kind
            // itself, and pinning a step here would imply the difference lives
            // in that step's contents when it does not.
            left: None,
            right: None,
        }));
    }

    let shared = left.steps.len().min(right.steps.len());

    for i in 0..shared {
        let l = &left.steps[i];
        let r = &right.steps[i];

        if let Some(kind) = step_diff_kind(l, r) {
            return finish(Some(TraceDivergence {
                step_index: i,
                kind,
                left: Some(l.clone()),
                right: Some(r.clone()),
            }));
        }
    }

    finish(
        (left.steps.len() != right.steps.len()).then(|| TraceDivergence {
            step_index: shared,
            kind: DiffKind::TraceLength {
                left: left.steps.len(),
                right: right.steps.len(),
                capped: truncated,
            },
            left: left.steps.get(shared).cloned(),
            right: right.steps.get(shared).cloned(),
        }),
    )
}

/// How a single pair of steps differs, if at all.
///
/// Comparison order is deliberate:
///
/// 1. **History facts** — if the two recordings already differ here, comparing
///    "what did the code do next" across them is apples to oranges, and this is
///    the only signal a handler-free trace carries (it emits no commands).
/// 2. **Divergence** — one side's code no longer matches the recording. That
///    *is* the root cause; every later difference at this step is downstream
///    of it.
/// 3. **Commands** — the actionable root cause when both sides replay the
///    *same* history against different code.
/// 4. **Outcome** — usually a downstream consequence of (3), so it is reported
///    last.
fn step_diff_kind(l: &DebugStep, r: &DebugStep) -> Option<DiffKind> {
    if let Some(field) = history_fact_diff(l, r) {
        return Some(DiffKind::HistoryFacts { field });
    }

    if l.divergence != r.divergence {
        return Some(DiffKind::Divergence {
            left: l.divergence.clone(),
            right: r.divergence.clone(),
        });
    }

    let shared_cmds = l.commands.len().min(r.commands.len());
    for c in 0..shared_cmds {
        if l.commands[c] != r.commands[c] {
            return Some(DiffKind::CommandMismatch { command_index: c });
        }
    }
    if l.commands.len() != r.commands.len() {
        return Some(DiffKind::CommandCount {
            left: l.commands.len(),
            right: r.commands.len(),
        });
    }
    if l.outcome != r.outcome {
        return Some(DiffKind::Outcome {
            left: l.outcome,
            right: r.outcome,
        });
    }
    None
}

/// Compare the history-derived half of two steps, returning the name of the
/// first field that differs.
///
/// # Per-run identity is normalized out
///
/// Two *independent recordings* of the same logical scenario necessarily differ
/// on values that are freshly minted per run: activity/child execution UUIDs,
/// and the recorded values of the non-deterministic side-effect kinds
/// (`Now`/`Uuid`/`Random`, issue #384). Comparing those verbatim would make the
/// "two fixture histories" arm of AC3 report a divergence at the first activity
/// of every real pair — pure noise that would bury the signal.
///
/// So this comparator normalizes them away and compares the **semantic** shape:
/// event type, signal name, resolved payload, awaitable kind/name/opening
/// index, version and patch gates, marker names and details, and the value of
/// `Custom` side effects (which is application data, not engine identity).
///
/// This normalization cannot hide a code-caused divergence:
///
/// * In the **one history, two code registrations** arm (AC3's primary case)
///   both sides replay byte-identical events, so every normalized-away field is
///   equal by construction and the divergence surfaces through `commands`.
/// * In the **two fixture histories** arm, a change that actually matters —
///   a renamed or reordered activity, a different input, a new marker, a
///   changed version gate — moves a field that *is* compared.
///
/// # Exhaustiveness
///
/// The destructure below binds every [`DebugStep`] field, so adding a field
/// without classifying it as history-derived (here) or code-derived (in
/// [`step_diff_kind`]) is a compile error rather than a silent false "clean".
fn history_fact_diff(l: &DebugStep, r: &DebugStep) -> Option<&'static str> {
    let DebugStep {
        index: _,
        event_type: l_event_type,
        // `outcome` and `commands` are code-derived; `step_diff_kind` owns them.
        outcome: _,
        commands: _,
        open_awaitables: l_open,
        version_gates: l_versions,
        patch_gates: l_patches,
        side_effects: l_side_effects,
        markers: l_markers,
        // A display projection; `event_facts` is the comparison key.
        resolved_payload: _,
        event_facts: l_facts,
        signal_name: l_signal,
        // Code-derived: a divergence is a code-vs-history mismatch, not a
        // property of the recording. `step_diff_kind` owns it.
        divergence: _,
    } = l;

    if l_event_type != &r.event_type {
        return Some("event_type");
    }
    if l_signal != &r.signal_name {
        return Some("signal_name");
    }
    if awaitable_shapes(l_open) != awaitable_shapes(&r.open_awaitables) {
        return Some("open_awaitables");
    }
    if l_versions != &r.version_gates {
        return Some("version_gates");
    }
    if l_patches != &r.patch_gates {
        return Some("patch_gates");
    }
    if side_effect_shapes(l_side_effects) != side_effect_shapes(&r.side_effects) {
        return Some("side_effects");
    }
    if l_markers != &r.markers {
        return Some("markers");
    }
    // Last, as the completeness backstop. The projections above are *curated*:
    // each names a specific field, which is what makes the report legible to a
    // human ("open_awaitables differ" beats "the events differ"). But a curated
    // list is exactly the thing that silently rots as `WorkflowEvent` grows —
    // the hole Codex found, where a semantic field nobody projected made two
    // genuinely different histories compare EQUAL. `event_facts` is the whole
    // serialized event with only per-run identity normalized away, so it is
    // exhaustive by construction and catches anything the curated checks miss.
    // Ordering it last keeps the good diagnostics and closes the hole.
    if l_facts != &r.event_facts {
        return Some("event_facts");
    }
    None
}

/// Replace every UUID-shaped token in a display label with the per-run marker.
///
/// An awaitable's `name` is a *display* string that can embed engine identity —
/// `ExternalSignalRequested` renders `"{signal_name} -> {target}"`, and an
/// `ExternalTarget::ExecutionId` target is a per-run UUID. Dropping the
/// separate `id` field is therefore not enough: without this, two independent
/// recordings of one "signal a sibling workflow" scenario diff as different.
/// That is noise rather than a false clean, but noise on a divergence-finding
/// tool buries the signal it exists to surface.
///
/// A business-id target (`ExternalTarget::WorkflowId`) contains no UUID and so
/// still compares verbatim.
fn normalize_label_ids(label: &str) -> String {
    label
        .split(' ')
        .map(|token| {
            if uuid::Uuid::parse_str(token).is_ok() {
                "<per-run>"
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Project open awaitables onto their comparable shape, dropping the per-run
/// execution UUID — both the separate `id` field and any UUID embedded in the
/// display `name` (see [`history_fact_diff`] and [`normalize_label_ids`]).
fn awaitable_shapes(awaitables: &[OpenAwaitable]) -> Vec<(AwaitableKind, Option<String>, usize)> {
    awaitables
        .iter()
        // `AwaitableKind::Signal` and `AwaitableKind::Mutex` are synthesized
        // from frontier `WaitForSignal`/`AcquireMutex` commands, so they are
        // *code*-derived, not recorded history facts. Comparing them here
        // would report a code difference as "the recorded histories differ";
        // `step_diff_kind`'s command comparison owns them and sees the same
        // signal name / mutex key.
        .filter(|a| !matches!(a.kind, AwaitableKind::Signal | AwaitableKind::Mutex))
        .map(|a| {
            (
                a.kind,
                a.name.as_deref().map(normalize_label_ids),
                a.opened_at,
            )
        })
        .collect()
}

/// Project side effects onto their comparable shape. The recorded `value` is
/// compared only for `Custom` side effects — the `Now`/`Uuid`/`Random` kinds
/// are freshly drawn per run by design (see [`history_fact_diff`]).
fn side_effect_shapes(
    effects: &[SideEffectSnapshot],
) -> Vec<(&'static str, Option<&str>, usize, Option<&Value>)> {
    effects
        .iter()
        .map(|e| {
            let value = (e.kind == "custom").then_some(&e.value);
            (e.kind, e.name.as_deref(), e.event_index, value)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// Whether an exported history document declares redacted payloads.
///
/// `HistoryExportDocument` carries `payload_policy` at the top level but
/// [`HistorySnapshot`] does not model it, so this reads the raw JSON — the same
/// approach `harvest history sample`'s own redaction warning takes.
#[must_use]
pub fn is_redacted_export(json: &str) -> bool {
    serde_json::from_str::<Value>(json)
        .is_ok_and(|doc| doc.get("payload_policy").and_then(Value::as_str) == Some("redacted"))
}

/// Why a debugger trace could not be produced.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DebugError {
    /// The snapshot names a workflow type with no registered handler.
    #[error(
        "workflow '{name}' is not registered in this debugger; call register_fn(\"{name}\", …)"
    )]
    HandlerNotRegistered {
        /// The unregistered workflow type name.
        name: String,
    },
    /// The supplied JSON was not a valid `HistorySnapshot`.
    #[error("could not parse history snapshot JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// The exported history has redacted payloads.
    ///
    /// Redaction rewrites every `input` / `output` / `payload` field in place,
    /// so replaying against it makes the very first payload-bearing activity
    /// report a divergence that is an artefact of the export, not of the code.
    /// Refused rather than reported, because a divergence-finding tool must
    /// never invent one.
    #[error(
        "this history was exported with redacted payloads, so every payload-bearing \
         activity would report a fabricated divergence; re-export with \
         `harvest history export <ID> --payload-policy full`"
    )]
    RedactedHistory,
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplayDebugger
// ─────────────────────────────────────────────────────────────────────────────

/// Builds command-carrying [`ReplayTrace`]s by prefix-replaying a history
/// against registered workflow code.
///
/// ```rust,no_run
/// # use autumn_harvest::debugger::ReplayDebugger;
/// # use autumn_harvest::context::WorkflowContext;
/// # use serde_json::Value;
/// # use std::pin::Pin;
/// # fn my_workflow<'a>(ctx: &'a WorkflowContext, input: Value)
/// #   -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>
/// # { Box::pin(async move { Ok(input) }) }
/// # async fn example(exported_json: &str) -> Result<(), Box<dyn std::error::Error>> {
/// let trace = ReplayDebugger::new()
///     .register_fn("my_workflow", my_workflow)
///     .trace_json(exported_json)
///     .await?;
///
/// for step in &trace.steps {
///     println!("{:>4}  {:<24}  {:?}", step.index, step.event_type, step.commands);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ReplayDebugger {
    handlers: HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    max_steps: usize,
    step_timeout: Duration,
    history_policy: WorkflowHistoryPolicy,
    build_id: Option<String>,
}

impl fmt::Debug for ReplayDebugger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.handlers.keys().map(String::as_str).collect();
        names.sort_unstable();
        f.debug_struct("ReplayDebugger")
            .field("handlers", &names)
            .field("max_steps", &self.max_steps)
            .field("step_timeout", &self.step_timeout)
            .finish_non_exhaustive()
    }
}

impl Default for ReplayDebugger {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayDebugger {
    /// A debugger with no registered handlers and library defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            state: empty_shared_state(),
            max_steps: DEFAULT_MAX_STEPS,
            step_timeout: DEFAULT_STEP_TIMEOUT,
            history_policy: WorkflowHistoryPolicy::default(),
            build_id: None,
        }
    }

    /// Register a workflow handler by type name.
    #[must_use]
    pub fn register_fn(mut self, name: impl Into<String>, handler: WorkflowHandlerFn) -> Self {
        self.handlers.insert(name.into(), handler);
        self
    }

    /// Inject shared application state, required when workflow code calls
    /// `ctx.state::<T>()`.
    #[must_use]
    pub fn state(mut self, state: SharedState) -> Self {
        self.state = state;
        self
    }

    /// Cap the number of prefix replays (default [`DEFAULT_MAX_STEPS`]).
    ///
    /// A value of `0` is clamped to `1`: a trace of a non-empty history always
    /// produces at least one step.
    #[must_use]
    pub fn max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps.max(1);
        self
    }

    /// Set the per-step drive budget (default [`DEFAULT_STEP_TIMEOUT`]).
    #[must_use]
    pub const fn step_timeout(mut self, timeout: Duration) -> Self {
        self.step_timeout = timeout;
        self
    }

    /// Set the candidate build id seen by `ctx.info().build_id` during replay.
    #[must_use]
    pub fn build_id(mut self, build_id: Option<String>) -> Self {
        self.build_id = build_id;
        self
    }

    /// Set the history policy seen by `ctx.should_continue_as_new()`.
    #[must_use]
    pub const fn history_policy(mut self, policy: WorkflowHistoryPolicy) -> Self {
        self.history_policy = policy;
        self
    }

    /// Trace an exported [`HistorySnapshot`] JSON document.
    ///
    /// AC4: this is the offline entry point — a production history exported via
    /// `harvest history export` is debugged locally with zero database access.
    ///
    /// # Errors
    ///
    /// [`DebugError::Json`] when the document is not a valid snapshot, or
    /// [`DebugError::HandlerNotRegistered`] when its `workflow_name` has no
    /// registered handler.
    pub async fn trace_json(&self, json: &str) -> Result<ReplayTrace, DebugError> {
        if is_redacted_export(json) {
            return Err(DebugError::RedactedHistory);
        }
        let snapshot: HistorySnapshot = serde_json::from_str(json)?;
        self.trace_snapshot(snapshot).await
    }

    /// Trace a [`HistorySnapshot`] by prefix-replaying it step by step.
    ///
    /// # Errors
    ///
    /// [`DebugError::HandlerNotRegistered`] when the snapshot's `workflow_name`
    /// has no registered handler.
    pub async fn trace_snapshot(
        &self,
        snapshot: HistorySnapshot,
    ) -> Result<ReplayTrace, DebugError> {
        let Some(&handler) = self.handlers.get(&snapshot.workflow_name) else {
            return Err(DebugError::HandlerNotRegistered {
                name: snapshot.workflow_name,
            });
        };

        // The handler-free projection supplies every AC1 field except the
        // per-step pending commands; prefix replay fills those in.
        let mut trace = ReplayTrace::from_history_capped(
            snapshot.workflow_name.clone(),
            snapshot.execution_id,
            &snapshot.events,
            self.max_steps,
        );

        let input = workflow_input(&snapshot.events);
        for step in &mut trace.steps {
            let prefix = snapshot.events[..=step.index].to_vec();
            let replayed = self
                .replay_prefix(&snapshot, handler, input.clone(), prefix)
                .await;
            step.outcome = replayed.outcome;
            step.divergence = replayed.divergence;
            // AC1 requires signal waits among the open awaitables, but some
            // parks have no *opening event* — they exist only as a frontier
            // command. A `wait_for_signal` park emits `WaitForSignal`, and a
            // `ctx.mutex(k).acquire()` park emits `AcquireMutex` (its waiter
            // row lives in `harvest_mutex_waiters`; only the *grant* records a
            // `MutexGranted` event, issue #691). Synthesize both here, where
            // the commands are known, so "why is this run parked?" is
            // answerable for either and `history_fact_diff`'s
            // `open_awaitables` comparison can see a changed name. The
            // command → kind mapping mirrors `awaitables::…` so the debugger
            // and the operator-facing open-awaitables report agree.
            for command in &replayed.commands {
                let kind = match command.kind {
                    "WaitForSignal" => AwaitableKind::Signal,
                    "AcquireMutex" => AwaitableKind::Mutex,
                    _ => continue,
                };
                step.open_awaitables.push(OpenAwaitable {
                    kind,
                    name: Some(command.summary.clone()),
                    id: None,
                    opened_at: step.index,
                });
            }
            step.commands = replayed.commands;
        }

        Ok(trace)
    }

    /// Replay one prefix and collect the frontier commands, the stop reason,
    /// and any divergence.
    ///
    /// Uses **canary** (frontier-tolerant) replay: a prefix is exactly an
    /// in-flight execution parked at its frontier, which strict replay would
    /// report as a spurious divergence.
    async fn replay_prefix(
        &self,
        snapshot: &HistorySnapshot,
        handler: WorkflowHandlerFn,
        input: Value,
        prefix: Vec<WorkflowEvent>,
    ) -> PrefixReplay {
        let mut ctx = WorkflowContext::for_replay_canary_with_state(
            snapshot.execution_id,
            prefix,
            self.state.clone(),
        )
        .with_workflow_name(snapshot.workflow_name.clone())
        .with_build_id(self.build_id.clone())
        // Every replay input the snapshot carries. `HistorySnapshot` documents
        // these as replay inputs precisely because they are *row* columns that
        // live in no `WorkflowEvent`, so a replay path that drops them makes a
        // workflow branching on `ctx.info().parent_execution_id`, reading its
        // execution deadline, or calling the deadline-sensitive
        // `should_continue_as_new()` emit commands from defaults — a fabricated
        // divergence on an otherwise valid production export. Threading them
        // here keeps this new replay path aligned with the worker, the
        // replayer, and the query-hydration paths (issues #698, #772).
        .with_parent_execution_id(snapshot.parent_execution_id)
        .with_execution_timeout(snapshot.execution_timeout)
        .with_deadline(snapshot.deadline_at)
        // Configured via `ReplayDebugger::history_policy`; without this the
        // setter is dead and `should_continue_as_new()` always sees the
        // default threshold and deadline fraction.
        .with_history_policy(self.history_policy);

        if let Some(workflow_id) = snapshot.workflow_id.clone() {
            ctx = ctx.with_workflow_id(workflow_id);
        }
        if let Some(queue) = snapshot.queue_name.clone() {
            ctx = ctx.with_queue_name(queue);
        }
        if let Some(headers) = snapshot.context_headers.clone() {
            ctx = ctx.with_context_headers(headers);
        }

        let outcome = drive_query_replay_async(&ctx, handler, input, self.step_timeout).await;

        // `drive_query_replay_async` deliberately discards the handler's
        // `Ok`/`Err`, so a non-determinism error is read back from the context's
        // structured slots.
        //
        // Both slots are drained **unconditionally**: `record_deferred_nd`
        // populates `nd_details` with empty `expected`/`actual` *and*
        // `deferred_nd_error` with the real message, so a lazy `.or_else` chain
        // would never reach the message and every deferred divergence — the
        // whole `SideEffectDrift` class — would report `<unknown>`.
        let deferred = ctx.take_deferred_nd_error();
        let position = ctx.replay_position();
        let divergence = ctx
            .take_nd_details()
            .map(|d| StepDivergence {
                expected: d
                    .expected
                    .unwrap_or_else(|| "<deterministic replay>".to_string()),
                actual: d
                    .actual
                    .or_else(|| deferred.clone())
                    .unwrap_or_else(|| UNKNOWN.to_string()),
                event_index: d
                    .event_index
                    .and_then(|i| usize::try_from(i).ok())
                    .unwrap_or(position),
            })
            .or_else(|| {
                deferred.map(|message| StepDivergence {
                    expected: "<deterministic replay>".to_string(),
                    actual: message,
                    event_index: position,
                })
            })
            // The engine's own runners apply one more check *after* the handler
            // returns: a workflow that completes while recorded history is still
            // unconsumed dropped work the recording did (`run_workflow_canary`,
            // `run_strict_with_ctx`). Without this the debugger is strictly
            // weaker than canary — a build that never calls an activity the
            // history records replays 100% "clean", which is the worst possible
            // answer for the single-trace "does this build replay cleanly?"
            // check. Gated on `ReachedTerminal` to match the engine, which
            // applies it only on the handler's `Ok` arm.
            .or_else(|| {
                (outcome == QueryReplayOutcome::ReachedTerminal
                    && ctx.history_has_unconsumed_events())
                .then(|| StepDivergence {
                    expected: "<end of history>".to_string(),
                    actual: "<workflow returned early>".to_string(),
                    event_index: ctx.replay_position(),
                })
            });

        PrefixReplay {
            outcome: match outcome {
                QueryReplayOutcome::ReachedTerminal => StepOutcome::ReachedTerminal,
                QueryReplayOutcome::Suspended => StepOutcome::Suspended,
                QueryReplayOutcome::TimedOut => StepOutcome::TimedOut,
                QueryReplayOutcome::Panicked => StepOutcome::Panicked,
            },
            commands: ctx.drain_commands().iter().map(render_command).collect(),
            divergence,
        }
    }
}

/// The per-prefix replay result folded into a [`DebugStep`].
struct PrefixReplay {
    outcome: StepOutcome,
    commands: Vec<CommandSnapshot>,
    divergence: Option<StepDivergence>,
}

// ─────────────────────────────────────────────────────────────────────────────
// History accumulator (the pure O(N) projection)
// ─────────────────────────────────────────────────────────────────────────────

/// Running state while walking a history: which awaitables are open, which
/// gates resolved, which side effects and markers were recorded.
#[derive(Default)]
struct HistoryAccumulator {
    /// Open awaitables keyed by `(kind, id)`, preserving open order via the
    /// stored `opened_at`.
    open: Vec<OpenAwaitable>,
    version_gates: BTreeMap<String, i64>,
    patch_gates: BTreeSet<String>,
    side_effects: Vec<SideEffectSnapshot>,
    markers: Vec<MarkerSnapshot>,
}

impl HistoryAccumulator {
    fn open_awaitables(&self) -> Vec<OpenAwaitable> {
        self.open.clone()
    }

    fn push_open(
        &mut self,
        kind: AwaitableKind,
        id: Option<String>,
        name: Option<String>,
        at: usize,
    ) {
        self.open.push(OpenAwaitable {
            kind,
            name,
            id,
            opened_at: at,
        });
    }

    fn close(&mut self, kind: AwaitableKind, id: &str) {
        if let Some(pos) = self
            .open
            .iter()
            .position(|a| a.kind == kind && a.id.as_deref() == Some(id))
        {
            self.open.remove(pos);
        }
    }

    /// Fold one event into the running projection.
    fn apply(&mut self, index: usize, event: &WorkflowEvent) {
        match event {
            // `ActivityAwaitingExternal` is included deliberately: an
            // external-activity handoff emits *only* that event — there is no
            // preceding `ActivityScheduled` — so without it the activity never
            // opens at all and the run looks idle.
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            }
            | WorkflowEvent::LocalActivityScheduled {
                activity_id, name, ..
            }
            | WorkflowEvent::ActivityAwaitingExternal {
                activity_id, name, ..
            } => self.push_open(
                AwaitableKind::Activity,
                Some(activity_id.to_string()),
                Some(name.clone()),
                index,
            ),
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
            | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
            | WorkflowEvent::ActivityFailedExternally { activity_id, .. }
            | WorkflowEvent::LocalActivityCompleted { activity_id, .. }
            | WorkflowEvent::LocalActivityExhausted { activity_id, .. } => {
                self.close(AwaitableKind::Activity, &activity_id.to_string());
            }

            WorkflowEvent::TimerStarted { timer_id, .. } => self.push_open(
                AwaitableKind::Timer,
                Some(timer_id.to_string()),
                None,
                index,
            ),
            WorkflowEvent::TimerFired { timer_id }
            | WorkflowEvent::TimerCancelled { timer_id, .. } => {
                self.close(AwaitableKind::Timer, &timer_id.to_string());
            }

            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => self.push_open(
                AwaitableKind::ChildWorkflow,
                Some(child_id.to_string()),
                Some(workflow_name.clone()),
                index,
            ),
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                self.close(AwaitableKind::ChildWorkflow, &child_id.to_string());
            }

            WorkflowEvent::UpdateAdmitted {
                update_id, name, ..
            } => self.push_open(
                AwaitableKind::Update,
                Some(update_id.to_string()),
                Some(name.clone()),
                index,
            ),
            WorkflowEvent::UpdateCompleted { update_id, .. }
            | WorkflowEvent::UpdateFailed { update_id, .. } => {
                self.close(AwaitableKind::Update, &update_id.to_string());
            }

            other => {
                if !self.apply_external(index, other) {
                    self.apply_record(index, other);
                }
            }
        }
    }

    /// Fold a cross-workflow signal / cancel / await handoff into the
    /// projection. Returns `true` when `event` was one.
    fn apply_external(&mut self, index: usize, event: &WorkflowEvent) -> bool {
        match event {
            WorkflowEvent::ExternalAwaitRequested { await_id, target } => self.push_open(
                AwaitableKind::ExternalWorkflow,
                Some(await_id.to_string()),
                Some(target.to_string()),
                index,
            ),
            WorkflowEvent::ExternalAwaitResolved { await_id, .. }
            | WorkflowEvent::ExternalAwaitFailed { await_id, .. } => {
                self.close(AwaitableKind::ExternalWorkflow, &await_id.to_string());
            }

            // Cross-workflow signal (#244) and cancel (#492) handoffs. A run
            // parked on one of these is the single most common "why is this
            // stuck?" question, so they must open an awaitable — otherwise the
            // park is invisible here *and* to `history_fact_diff`'s
            // `open_awaitables` comparison.
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name,
                ..
            } => self.push_open(
                AwaitableKind::ExternalWorkflow,
                Some(signal_id.to_string()),
                Some(format!(
                    "{signal_name} -> {}",
                    external_target_label(target)
                )),
                index,
            ),
            WorkflowEvent::ExternalSignalDelivered { signal_id }
            | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                self.close(AwaitableKind::ExternalWorkflow, &signal_id.to_string());
            }
            WorkflowEvent::ExternalCancelRequested { cancel_id, target } => self.push_open(
                AwaitableKind::ExternalWorkflow,
                Some(cancel_id.to_string()),
                Some(format!("cancel -> {}", external_target_label(target))),
                index,
            ),
            WorkflowEvent::ExternalCancelDelivered { cancel_id }
            | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                self.close(AwaitableKind::ExternalWorkflow, &cancel_id.to_string());
            }
            _ => return false,
        }
        true
    }

    /// Fold a marker or recorded side effect into the projection.
    fn apply_record(&mut self, index: usize, event: &WorkflowEvent) {
        match event {
            WorkflowEvent::MarkerRecorded { name, details } => {
                self.apply_marker(index, name, details);
            }

            WorkflowEvent::SideEffectRecorded { kind, name, value } => {
                self.side_effects.push(SideEffectSnapshot {
                    kind: kind.as_str(),
                    name: name.clone(),
                    value: value.clone(),
                    event_index: index,
                });
            }

            _ => {}
        }
    }

    /// Route a marker into the resolved version/patch gate maps, or record it
    /// verbatim.
    fn apply_marker(&mut self, index: usize, name: &str, details: &Value) {
        if let Some(change_id) = name.strip_prefix(VERSION_MARKER_PREFIX) {
            // `ctx.version()` records the resolved version as a JSON number.
            // A malformed marker falls through to the verbatim list rather
            // than being silently swallowed.
            if let Some(version) = details.as_i64() {
                self.version_gates.insert(change_id.to_string(), version);
                return;
            }
        } else if let Some(patch_id) = name.strip_prefix(PATCH_MARKER_PREFIX) {
            self.patch_gates.insert(patch_id.to_string());
            return;
        }

        self.markers.push(MarkerSnapshot {
            name: name.to_string(),
            details: details.clone(),
            event_index: index,
        });
    }
}

/// The payload this event resolved, when it carries one.
///
/// AC4: returned **verbatim** — a codec or offload envelope is surfaced as the
/// envelope. The debugger never decodes and never errors on one.
fn resolved_payload(event: &WorkflowEvent) -> Option<Value> {
    match event {
        WorkflowEvent::ActivityCompleted { output, .. }
        | WorkflowEvent::ActivityCompletedExternally { output, .. }
        | WorkflowEvent::LocalActivityCompleted { output, .. }
        | WorkflowEvent::ChildWorkflowCompleted { output, .. }
        | WorkflowEvent::WorkflowCompleted { output } => Some(output.clone()),
        WorkflowEvent::SignalReceived { payload, .. } => Some(payload.clone()),
        WorkflowEvent::SideEffectRecorded { value, .. } => Some(value.clone()),
        WorkflowEvent::MarkerRecorded { details, .. } => Some(details.clone()),
        WorkflowEvent::ActivityScheduled { input, .. }
        | WorkflowEvent::LocalActivityScheduled { input, .. }
        | WorkflowEvent::ChildWorkflowStarted { input, .. }
        | WorkflowEvent::WorkflowStarted { input, .. } => Some(input.clone()),
        _ => None,
    }
}

/// The event's **complete semantic content**, with per-run identity normalized
/// out — the key `history_fact_diff` compares two fixture histories on.
///
/// # Why the whole event rather than a projection
///
/// Top-level `data` field names the engine mints fresh on every run.
///
/// Derived mechanically from `WorkflowEvent`'s own declared field types. A
/// field belongs here when the engine mints its value fresh on every run:
///
/// * a UUID newtype — `ActivityExecId`, `ExecutionId`, `UpdateId`,
///   `ExternalSignalId`, `ExternalAwaitId`, `ExternalCancelId`,
///   `ExternalActivityToken`, `Uuid`;
/// * `WorkerId`, which is `String`-backed but whose value
///   `WorkerRuntimeConfig::from` always mints as a fresh `Uuid::new_v4()` —
///   there is no `WorkerConfig::with_worker_id`. Two independent recordings
///   therefore differ at the *first* `ActivityStarted`, which would bury every
///   later semantic difference behind a divergence that is pure per-run noise.
///
/// `WorkerId` is exactly why the value guard (does it parse as a UUID?) is
/// retained *alongside* the name match: `WorkerRuntimeConfig::worker_id` is a
/// `pub` field, so an embedder may set a stable human label
/// (`WorkerId::new("worker-eu-1")`). A label does not parse as a UUID and so
/// still compares verbatim — engine-minted identity is normalized, an
/// operator's chosen name is preserved.
///
/// Deliberately **not** here, despite being UUID-shaped in practice:
///
/// * `idempotency_key` — a caller-chosen exactly-once key (#521). Semantic.
/// * `timer_id` — a `String` newtype whose value is author-chosen
///   (`ctx.timer("escalate", …)`), so it must compare verbatim.
///
/// `debugger_tests::engine_minted_id_fields_match_the_event_enum` re-derives
/// this set from `event.rs` at test time, so an engine-minted field added to
/// `WorkflowEvent` later fails until it is classified here.
const ENGINE_MINTED_ID_FIELDS: &[&str] = &[
    "activity_id",
    "await_id",
    "cancel_id",
    "child_id",
    "dead_letter_id",
    "new_exec_id",
    "reset_from_exec_id",
    "reset_to_exec_id",
    "retry_exec_id",
    "signal_id",
    "target",
    "token",
    "update_id",
    "worker_id",
];

/// The first cut compared a hand-written [`resolved_payload`] projection, whose
/// `_ => None` arm silently swallowed every variant it did not enumerate. Two
/// histories differing only in a `TimerStarted { duration_secs }` (30 vs 60) or
/// an `ActivityFailed { error }` therefore compared **equal**, and
/// `harvest debug diff` exited `0` — a false green on a documented CI gate, and
/// on a divergence-finding tool the worst possible failure. Serializing the
/// event makes the comparison exhaustive **by construction**: a `WorkflowEvent`
/// variant added later carries its fields into the diff with no edit here, so
/// the hole cannot reopen.
///
/// # What is normalized away, and why only at the top level
///
/// Two *independent recordings* of the same scenario necessarily differ on
/// values minted per run. Those are stripped from the top level of the event's
/// `data` object only:
///
/// * a UUID value in one of the [`ENGINE_MINTED_ID_FIELDS`] — matched by
///   **field name**, not by "looks like a UUID";
/// * `timestamp`;
/// * the recorded `value` of a non-deterministic side-effect draw
///   (`Now`/`Uuid`/`Random`, issue #384) — its `kind` and `name` still compare.
///
/// Field-name matching is load-bearing. A purely value-based test ("normalize
/// anything that parses as a UUID") also eats
/// `ExternalSignalRequested { idempotency_key }`, which is a caller-chosen
/// exactly-once key (#521) — very commonly a UUID, and *semantic*: two
/// histories differing only in it are genuinely different, and normalizing it
/// would report them as equal.
///
/// Top-level *only* is load-bearing for the same reason: engine identity lives
/// there, while application data lives nested under
/// `input`/`output`/`payload`/`details`, where a UUID is real business data a
/// diff must not discard.
fn normalized_event_facts(event: &WorkflowEvent) -> Value {
    /// Stand-in for a stripped per-run value. A single shared marker keeps two
    /// recordings comparing equal on the field while leaving it visibly
    /// present, rather than deleting the key (which would make "absent" and
    /// "normalized" indistinguishable).
    const NORMALIZED: &str = "<per-run>";

    let Ok(mut value) = serde_json::to_value(event) else {
        // `WorkflowEvent` is the persisted wire type, so this is unreachable in
        // practice; degrade to "no facts" rather than panicking a debugger.
        return Value::Null;
    };
    let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
        return value;
    };

    let nondeterministic_draw = data
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind != "Custom");

    for (key, slot) in data.iter_mut() {
        // The UUID parse is kept *in addition to* the name match, not instead
        // of it: `target` is `ExecutionId` on some variants and
        // `ExternalTarget` on others, and the latter's business-id form
        // serializes as an object that must compare verbatim.
        let is_per_run = key == "timestamp"
            || (nondeterministic_draw && key == "value")
            || (ENGINE_MINTED_ID_FIELDS.contains(&key.as_str())
                && slot
                    .as_str()
                    .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok()));
        if is_per_run {
            *slot = Value::String(NORMALIZED.to_string());
        }
    }
    value
}

/// The workflow's input, read from the recorded `WorkflowStarted` event.
fn workflow_input(events: &[WorkflowEvent]) -> Value {
    events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowStarted { input, .. } => Some(input.clone()),
            _ => None,
        })
        .unwrap_or(Value::Null)
}

/// Project a [`WorkflowCommand`] onto its display + identity snapshot.
///
/// **Deliberately exhaustive — no `_` arm**, here and in [`command_payload`].
/// A catch-all would collapse two genuinely different commands onto the same
/// [`CommandSnapshot`], which compare *equal* and would therefore silently hide
/// a divergence — the worst failure mode a divergence-finding tool can have.
/// Adding a `WorkflowCommand` variant must be a compile error here, not a false
/// "clean".
fn render_command(command: &WorkflowCommand) -> CommandSnapshot {
    let payload = command_payload(command);
    let (kind, summary) = match command {
        WorkflowCommand::ScheduleActivity { name, queue, .. } => {
            ("ScheduleActivity", format!("{name} -> {queue}"))
        }
        WorkflowCommand::WaitForActivity { activity_id, .. } => {
            ("WaitForActivity", activity_id.to_string())
        }
        WorkflowCommand::RunLocalActivity { name, .. } => ("RunLocalActivity", name.clone()),
        WorkflowCommand::StartTimer {
            timer_id,
            duration_secs,
            ..
        } => ("StartTimer", format!("{timer_id} {duration_secs}s")),
        WorkflowCommand::ArmTimer {
            timer_id,
            duration_secs,
            ..
        } => ("ArmTimer", format!("{timer_id} {duration_secs}s")),
        WorkflowCommand::CancelTimer { timer_id } => ("CancelTimer", timer_id.to_string()),
        WorkflowCommand::WaitForSignal { signal_name, .. } => {
            ("WaitForSignal", signal_name.clone())
        }
        WorkflowCommand::StartChildWorkflow { workflow_name, .. } => {
            ("StartChildWorkflow", workflow_name.clone())
        }
        WorkflowCommand::SpawnDetachedChildWorkflow { workflow_name, .. } => {
            ("SpawnDetachedChildWorkflow", workflow_name.clone())
        }
        WorkflowCommand::RecordMarker { name, .. } => ("RecordMarker", name.clone()),
        WorkflowCommand::RecordSideEffect { kind, name, .. } => (
            "RecordSideEffect",
            name.clone().unwrap_or_else(|| kind.as_str().to_string()),
        ),
        WorkflowCommand::ScheduleExternalActivity { name, .. } => {
            ("ScheduleExternalActivity", name.clone())
        }
        WorkflowCommand::AcquireMutex { key, .. } => ("AcquireMutex", key.clone()),
        WorkflowCommand::ReleaseMutex { key, .. } => ("ReleaseMutex", key.clone()),
        WorkflowCommand::ContinueAsNew {
            new_workflow_type, ..
        } => (
            "ContinueAsNew",
            new_workflow_type.clone().unwrap_or_default(),
        ),
        WorkflowCommand::SignalExternalWorkflow {
            target,
            signal_name,
            ..
        } => (
            "SignalExternalWorkflow",
            format!("{signal_name} -> {}", external_target_label(target)),
        ),
        WorkflowCommand::RequestCancelExternalWorkflow { target, .. } => (
            "RequestCancelExternalWorkflow",
            external_target_label(target),
        ),
        WorkflowCommand::AwaitExternalWorkflow { target, .. } => {
            ("AwaitExternalWorkflow", target.to_string())
        }
        // Bookkeeping commands. Listed explicitly rather than swept up by a
        // `_` arm — see this function's doc comment. Their identity lives in
        // `command_payload` where it is a whole value rather than a label.
        WorkflowCommand::Complete { .. } => ("Complete", String::new()),
        WorkflowCommand::Fail { error } => ("Fail", error.clone()),
        WorkflowCommand::RecordUpdateResult { update_id, .. } => {
            ("RecordUpdateResult", update_id.to_string())
        }
        WorkflowCommand::UpsertSearchAttributes { .. } => ("UpsertSearchAttributes", String::new()),
        WorkflowCommand::SetCurrentDetails { .. } => ("SetCurrentDetails", String::new()),
        WorkflowCommand::PublishProgress { seq, .. } => ("PublishProgress", seq.to_string()),
        WorkflowCommand::RecordLog { seq, level, .. } => {
            ("RecordLog", format!("{seq} {}", level.as_str()))
        }
        WorkflowCommand::CancelRaceLosers {
            activities,
            children,
            timers,
        } => (
            "CancelRaceLosers",
            format!(
                "{}a {}c {}t",
                activities.len(),
                children.len(),
                timers.len()
            ),
        ),
    };

    CommandSnapshot {
        kind,
        summary,
        payload,
    }
}

/// The command's exact semantic content, where it carries any.
///
/// This is the **identity** half of a [`CommandSnapshot`]: it is never
/// truncated, so a divergence deep inside a large payload is caught exactly.
///
/// # Every semantic field, not just the input
///
/// A dispatch command carries more intent than its input: the queue it routes
/// to, its timeout and retry overrides, a detached child's close policy, a
/// signal's idempotency key. Projecting only `input` made two builds that
/// differ *only* in routing or timeout compare **equal**, so `diff_traces`
/// reported them clean — a false "no divergence" on the exact class of change
/// (a re-queued or re-timed activity) this tool exists to catch.
///
/// What is deliberately **excluded** is per-run identity that carries no code
/// intent and would differ between any two runs: freshly-minted
/// `activity_id` / `child_id` / `signal_id` / `cancel_id` / `await_id`
/// execution ids, the external-activity `token`, a mutex `lock_seq` fencing
/// token, and the replay-bookkeeping flags (`already_scheduled`,
/// `already_requested`, `failed_attempts`, `last_error`) that are recovered
/// from history rather than chosen by the code.
fn command_payload(command: &WorkflowCommand) -> Option<Value> {
    match command {
        // Every semantic dispatch field; ids and channels excluded (see above).
        WorkflowCommand::ScheduleActivity {
            input,
            queue,
            retry_policy_override,
            start_to_close_override,
            schedule_to_start_override,
            session_id,
            session_worker_id,
            ..
        } => Some(serde_json::json!({
            "input": input,
            "queue": queue,
            "retry_policy": retry_policy_override,
            // Full precision, NOT `as_secs()`: a caller passes a raw
            // `Duration`, so 100ms and 900ms would both collapse to 0 and two
            // genuinely different builds would compare equal. `Duration` is
            // `Serialize` (`{secs, nanos}`), so this is exact.
            "start_to_close": start_to_close_override,
            "schedule_to_start": schedule_to_start_override,
            "session_id": session_id.as_ref().map(ToString::to_string),
            "session_worker_id": session_worker_id.as_ref().map(ToString::to_string),
        })),
        WorkflowCommand::ScheduleExternalActivity {
            input,
            queue,
            schedule_to_close_secs,
            ..
        } => Some(serde_json::json!({
            "input": input,
            "queue": queue,
            "schedule_to_close_secs": schedule_to_close_secs,
        })),
        WorkflowCommand::RunLocalActivity {
            input,
            start_to_close,
            retry_policy,
            ..
        } => Some(serde_json::json!({
            "input": input,
            // Full precision — a local activity legitimately carries a
            // sub-second timeout (see `ScheduleActivity` above).
            "start_to_close": start_to_close,
            "retry_policy": retry_policy,
        })),
        WorkflowCommand::SpawnDetachedChildWorkflow {
            input,
            parent_close_policy,
            ..
        } => Some(serde_json::json!({
            "input": input,
            "parent_close_policy": parent_close_policy,
        })),
        WorkflowCommand::SignalExternalWorkflow {
            payload,
            idempotency_key,
            ..
        } => Some(serde_json::json!({
            "payload": payload,
            "idempotency_key": idempotency_key,
        })),
        WorkflowCommand::ArmTimer { for_await, .. } => {
            Some(serde_json::json!({ "for_await": for_await }))
        }
        WorkflowCommand::StartChildWorkflow { input, .. }
        | WorkflowCommand::ContinueAsNew { input, .. } => Some(input.clone()),
        WorkflowCommand::RecordMarker { details, .. } => Some(details.clone()),
        WorkflowCommand::RecordSideEffect { value, .. } => Some(value.clone()),
        WorkflowCommand::PublishProgress { chunk: payload, .. }
        | WorkflowCommand::Complete { output: payload } => Some(payload.clone()),
        WorkflowCommand::UpsertSearchAttributes { patch, .. } => serde_json::to_value(patch).ok(),
        WorkflowCommand::SetCurrentDetails {
            value,
            explicit_clear,
        } => Some(serde_json::json!({ "value": value, "explicit_clear": explicit_clear })),
        WorkflowCommand::Fail { error } | WorkflowCommand::RecordLog { message: error, .. } => {
            Some(Value::String(error.clone()))
        }
        WorkflowCommand::RecordUpdateResult { result, .. } => Some(match result {
            Ok(value) => serde_json::json!({ "ok": value }),
            Err(reason) => serde_json::json!({ "err": reason }),
        }),
        WorkflowCommand::CancelRaceLosers {
            activities,
            children,
            timers,
        } => Some(serde_json::json!({
            "activities": activities.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "children": children.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "timers": timers.iter().map(ToString::to_string).collect::<Vec<_>>(),
        })),
        // Commands whose whole identity already fits in the summary label:
        // there is no second value that two otherwise-identical commands could
        // differ on, so `None` here cannot hide a divergence.
        WorkflowCommand::WaitForActivity { .. }
        | WorkflowCommand::StartTimer { .. }
        | WorkflowCommand::CancelTimer { .. }
        | WorkflowCommand::WaitForSignal { .. }
        | WorkflowCommand::AcquireMutex { .. }
        | WorkflowCommand::ReleaseMutex { .. }
        | WorkflowCommand::RequestCancelExternalWorkflow { .. }
        | WorkflowCommand::AwaitExternalWorkflow { .. } => None,
    }
}

/// A short, stable label for an [`ExternalTarget`].
///
/// Rendered rather than `Debug`-formatted so the summary stays readable and
/// stable across `ExternalTarget`'s own internal changes.
fn external_target_label(target: &crate::types::ExternalTarget) -> String {
    match target {
        crate::types::ExternalTarget::ExecutionId(id) => id.to_string(),
        crate::types::ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        } => format!("{workflow_name}/{workflow_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{ENGINE_MINTED_ID_FIELDS, PATCH_MARKER_PREFIX, VERSION_MARKER_PREFIX};
    use crate::replay::{patch_marker_name, version_marker_name};

    /// The field types `WorkflowEvent` uses for values the engine mints fresh
    /// on every run.
    ///
    /// Mostly UUID newtypes. `WorkerId` is the exception that proves the rule:
    /// it wraps a `String`, but `WorkerRuntimeConfig::from` always mints its
    /// value as a fresh `Uuid::new_v4()` and no builder overrides it, so by
    /// *value* it is engine identity even though by *type* it is not. Keying
    /// this list on the declared type is a mechanical proxy for the real
    /// predicate ("is this minted per run?"), and `WorkerId` is where the
    /// proxy needs an explicit entry rather than a type-shape inference.
    const ENGINE_MINTED_FIELD_TYPES: &[&str] = &[
        "ActivityExecId",
        "ExecutionId",
        "ExternalActivityToken",
        "ExternalAwaitId",
        "ExternalCancelId",
        "ExternalSignalId",
        "UpdateId",
        "Uuid",
        "WorkerId",
    ];

    /// `normalized_event_facts` strips per-run identity by **field name**, and
    /// that list is hand-written — so an engine-minted field added to
    /// `WorkflowEvent` later would silently start being compared verbatim,
    /// making two independent recordings of one scenario diff as different
    /// (noise that buries the signal — the `worker_id` case Codex found in
    /// round 3). Re-derive the set from `event.rs` itself so the omission
    /// fails here instead.
    ///
    /// The inverse direction matters just as much: a field the engine does
    /// *not* mint must never be in the list, because normalizing a semantic
    /// field is a false clean — the `idempotency_key` (#521) case Codex found
    /// in round 2.
    #[test]
    fn engine_minted_id_fields_match_the_event_enum() {
        let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/event.rs"))
            .expect("event.rs is in this crate");
        let start = source
            .find("pub enum WorkflowEvent")
            .expect("the event enum is declared");
        let body = &source[start..];
        let end = body.find("\n}\n").expect("the enum body terminates");
        let body = &body[..end];

        let mut derived: Vec<&str> = Vec::new();
        for line in body.lines() {
            let trimmed = line.trim();
            // Struct-variant fields are `name: Type,` — skip doc comments,
            // attributes, and variant headers.
            let Some((name, ty)) = trimmed.strip_suffix(',').and_then(|f| f.split_once(": "))
            else {
                continue;
            };
            if !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                continue;
            }
            // `Option<T>` never holds engine identity in this enum, so the bare
            // type is the whole test.
            if ENGINE_MINTED_FIELD_TYPES.contains(&ty.trim()) && !derived.contains(&name) {
                derived.push(name);
            }
        }
        derived.sort_unstable();
        assert!(
            !derived.is_empty(),
            "the field scan found nothing — event.rs's shape changed and this \
             guard is no longer guarding anything"
        );

        let mut declared: Vec<&str> = ENGINE_MINTED_ID_FIELDS.to_vec();
        declared.sort_unstable();
        assert_eq!(
            derived, declared,
            "ENGINE_MINTED_ID_FIELDS drifted from WorkflowEvent's engine-minted \
             fields. A field only in `derived` is being compared verbatim (two \
             recordings of one scenario will diff as different); a field only \
             in `declared` is being normalized away (a semantic difference \
             will report a false clean)."
        );
    }

    /// The debugger classifies a `MarkerRecorded` as a version or patch gate by
    /// `strip_prefix`, but the engine *builds* those names with the helpers in
    /// `replay.rs`. Nothing structurally ties the two together, so if the
    /// engine ever renames a prefix the debugger would silently reclassify
    /// every gate as an ordinary marker — a false "clean" in the gate columns.
    /// This is the guard that makes that a compile-time-ish failure instead.
    #[test]
    fn marker_prefixes_match_the_engine() {
        assert!(
            version_marker_name("billing-v2").starts_with(VERSION_MARKER_PREFIX),
            "version marker prefix drifted: engine builds {:?}, debugger strips {VERSION_MARKER_PREFIX:?}",
            version_marker_name("billing-v2"),
        );
        assert!(
            patch_marker_name("fraud-check-v1").starts_with(PATCH_MARKER_PREFIX),
            "patch marker prefix drifted: engine builds {:?}, debugger strips {PATCH_MARKER_PREFIX:?}",
            patch_marker_name("fraud-check-v1"),
        );
        // And the strip must recover the id verbatim, not just match a prefix.
        assert_eq!(
            version_marker_name("billing-v2").strip_prefix(VERSION_MARKER_PREFIX),
            Some("billing-v2"),
        );
        assert_eq!(
            patch_marker_name("fraud-check-v1").strip_prefix(PATCH_MARKER_PREFIX),
            Some("fraud-check-v1"),
        );
    }
}
