//! Replay engine for deterministic workflow re-execution.
//!
//! The [`HistoryMatcher`] walks through previously recorded [`WorkflowEvent`]s
//! during replay, matching each workflow command against history to return
//! already-computed results instead of re-executing side effects.
//!
//! This is the brain of the durable execution model: when a workflow function
//! calls `execute_activity("send_email", ...)`, the matcher checks whether
//! history already contains a completed result for that activity and returns
//! it directly, avoiding duplicate side effects.

use serde_json::Value;
use std::collections::{HashSet, VecDeque};

use crate::error::TimeoutType;
use crate::event::{SideEffectKind, WorkflowEvent};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalSignalId, ParentClosePolicy,
    UpdateId,
};

/// Result of matching a workflow command against the event history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryMatch {
    /// History contains a completed result for this command.
    Matched {
        /// The JSON result value returned by the matched event.
        output: Value,
    },
    /// History contains a failure for this command.
    Failed {
        /// Human-readable failure message.
        error: String,
        /// Attempt number for the failed action.
        attempt: u32,
        /// Stable, low-cardinality error-type class recorded with the failure
        /// (issue #227 / #369), e.g. `"CircuitOpen"`. `"Error"` for legacy
        /// `Err(String)` failures. Carried so the consumer can build a typed
        /// [`HarvestError::ActivityFailed`] without parsing the message.
        error_type: String,
        /// Optional structured details recorded with a typed failure (e.g.
        /// `retry_after_secs` / `forced` for `CircuitOpen`). `None` otherwise.
        details: Option<Value>,
    },
    /// History contains a timeout for this command.
    TimedOut {
        /// The type of timeout that occurred.
        timeout_type: TimeoutType,
    },
    /// Cursor is past the end of history — this is a new command.
    ActivityInProgress {
        /// The activity execution ID already recorded in history.
        activity_id: ActivityExecId,
    },
    NoMatch,
    /// The command does not match what was recorded at this position,
    /// indicating non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
        /// The event index where the divergence occurred.
        event_index: Option<i32>,
    },
    /// History shows an external activity was scheduled but no terminal event
    /// (completed/failed/timed-out) exists yet. The workflow should re-emit the
    /// schedule command with the same token and activity ID (idempotent at the
    /// worker level) and then suspend until the external completion arrives.
    AwaitingExternalCompletion {
        /// The activity execution ID already recorded in history.
        activity_id: ActivityExecId,
        /// The token already recorded in history. Must be reused to stay idempotent.
        token: ExternalActivityToken,
    },
    /// History shows a child workflow was started but no terminal event
    /// (completed/failed) exists yet.  This occurs when the parent wakes after
    /// one of several parallel children completes while others are still
    /// running.  The workflow should re-emit a `StartChildWorkflow` command
    /// carrying the **existing** `child_id` from history (idempotent at the
    /// worker level) and then suspend until the child's terminal event arrives.
    ChildInProgress {
        /// The child execution ID already recorded in history. Must be reused.
        child_id: ExecutionId,
    },
    /// History has a `LocalActivityScheduled` event but no `LocalActivityCompleted`
    /// event yet.  This covers two cases:
    ///
    /// 1. **Crash before first run** — the worker appended `LocalActivityScheduled`
    ///    then crashed before executing the handler. `failed_attempts` is 0.
    ///
    /// 2. **Crash between retries** — the worker recorded one or more
    ///    `LocalActivityFailed` events and then crashed before the next attempt.
    ///    `failed_attempts` reflects how many failures are already durable.
    ///
    ///    This case is also returned when all retry attempts have already been
    ///    recorded (`failed_attempts >= max_attempts`). In that situation the
    ///    worker compares `failed_attempts` against its retry policy and returns
    ///    `last_error` immediately without executing the handler.
    ///
    /// The caller must re-execute the local activity using the **same**
    /// `activity_id` so that the derived idempotency key is unchanged across
    /// the crash.
    LocalActivityInProgress {
        /// The `ActivityExecId` already recorded in history. Must be reused.
        activity_id: ActivityExecId,
        /// How many `LocalActivityFailed` events are already durable for this
        /// invocation. The worker starts its retry loop from `failed_attempts + 1`.
        failed_attempts: u32,
        /// Error from the last recorded `LocalActivityFailed`, if any.
        /// Returned by the worker when `failed_attempts >= max_attempts`.
        last_error: Option<String>,
    },
    /// History has an `ExternalSignalRequested` event but no terminal event
    /// (`ExternalSignalDelivered` or `ExternalSignalFailed`) yet.
    ///
    /// This occurs when the worker crashed after appending the request event
    /// but before recording the delivery outcome. The caller must re-attempt
    /// delivery using the **same** `signal_id` and the **same** `payload` so
    /// the idempotency key is unchanged, and must NOT append a second
    /// `ExternalSignalRequested` event.
    ExternalSignalInProgress {
        /// The `ExternalSignalId` already recorded in history. Must be reused.
        signal_id: ExternalSignalId,
        /// The payload stored in the durable `ExternalSignalRequested` event.
        /// The worker must re-send this exact payload rather than the current
        /// argument value, so that target receives consistent data if the
        /// workflow code changed the payload expression between crash and recovery.
        payload: serde_json::Value,
    },
    /// History contains an `ExternalSignalFailed` terminal event for a
    /// `signal_external_workflow` call.  Carries the original `signal_id` from
    /// history so the replayed error matches the durable event exactly.
    ExternalSignalFailed {
        /// The `ExternalSignalId` recorded in the originating `ExternalSignalRequested` event.
        signal_id: ExternalSignalId,
        /// The machine-readable reason code from history.
        reason_code: String,
    },
    /// History contains a `ChildWorkflowSpawnedDetached` event for a detached
    /// spawn.  Carries the `child_id` recorded in that event so the workflow
    /// function gets back the same [`ExecutionId`] it got on the first run.
    DetachedChildSpawned {
        /// The `ExecutionId` recorded in the `ChildWorkflowSpawnedDetached` event.
        child_id: ExecutionId,
    },
}

/// Result of matching a signal-vs-timer race against the event history
/// (issue #476: `WorkflowContext::receive_signal_timeout`).
///
/// The race is resolved **deterministically by recorded history order**:
/// whichever of `SignalReceived` or `TimerFired` appears first in history
/// wins on every replay, regardless of wall-clock timing on the replaying
/// worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalOrTimerMatch {
    /// The signal was recorded before the deadline timer fired (or before the
    /// timer was ever started). Carries the recorded signal payload.
    SignalWon {
        /// The JSON payload from the winning `SignalReceived` event.
        payload: Value,
    },
    /// The deadline timer fired before the signal arrived. No signal payload
    /// is consumed — a later delivery remains observable by a subsequent
    /// signal wait.
    TimerWon,
    /// `TimerStarted` is recorded but neither `TimerFired` nor a matching
    /// `SignalReceived` exists yet. The caller should re-emit the race
    /// commands (the worker dedupes the durable timer row by `timer_id`)
    /// and suspend again.
    InProgress,
    /// Cursor is past the end of history — this is the first live execution
    /// of the race.
    NoMatch,
    /// The recorded history does not match the requested race, indicating
    /// non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
        /// The event index where the divergence occurred.
        event_index: Option<i32>,
    },
}

/// Terminal outcome for an early-drained external signal.
#[derive(Debug, Clone)]
enum StashedSignalTerminal {
    Delivered,
    Failed(String),
}

/// An `ExternalSignalRequested` event that was drained early by
/// [`HistoryMatcher::drain_early_signals`] before the cursor reached the
/// normal matching position.  Stored so that [`HistoryMatcher::match_external_signal`]
/// can return the correct result regardless of where in history the signal
/// pair falls relative to other durable events.
#[derive(Debug, Clone)]
struct StashedExternalSignal {
    signal_id: ExternalSignalId,
    target: ExecutionId,
    signal_name: String,
    /// Durable payload from the recorded `ExternalSignalRequested` event.
    /// Carried through so that crash-recovery re-dispatch uses the same
    /// payload that was originally sent, not whatever the workflow currently
    /// passes as an argument.
    payload: serde_json::Value,
    terminal: Option<StashedSignalTerminal>,
}

/// Walks through recorded workflow events during replay, matching
/// commands against what was previously recorded.
///
/// The cursor advances through events sequentially. During replay
/// (`is_replaying() == true`), each workflow command must match the
/// corresponding event in history. Once the cursor reaches the end
/// of history, new commands produce [`HistoryMatch::NoMatch`] and
/// will be executed for real.
pub struct HistoryMatcher {
    events: Vec<WorkflowEvent>,
    cursor: usize,
    consumed_out_of_order_events: HashSet<usize>,
    consumed_signal_events: HashSet<usize>,
    pending_signals: VecDeque<(String, Value, usize)>,
    /// External signals drained before their natural cursor position,
    /// e.g. when signal events appear before `ActivityScheduled` or
    /// `TimerStarted` events in a mixed-batch history.
    pending_external_signals: Vec<StashedExternalSignal>,
    /// Indices of events that are transparent to command-dispatch replay and
    /// therefore pre-marked consumed (issue #383: `WorkflowExecutionPaused` /
    /// `WorkflowExecutionResumed`). These are pure operator-lifecycle no-ops:
    /// they have no workflow-command counterpart and must never flag
    /// non-determinism, so every scan loop skips them via [`Self::is_consumed`].
    transparent_events: HashSet<usize>,
    /// Event indices of the exact `SignalReceived` events that lost a
    /// signal-or-deadline race (issue #476): for each race whose `TimerFired`
    /// precedes a matching signal, the **first** such signal event is the
    /// loser. A late loser is a normal production occurrence on the timeout
    /// branch — the workflow may intentionally never consume it — so
    /// [`Self::has_non_lifecycle_unconsumed`] must not report that specific
    /// event as early-completion non-determinism. Any other unconsumed signal
    /// (a second same-name delivery, or the loser's exemption spent because a
    /// later wait consumed it) still flags. The excused signal stays
    /// deliverable to any subsequent signal wait (the exemption only
    /// suppresses the completed-history check, never consumption).
    late_race_signal_events: HashSet<usize>,
}

impl HistoryMatcher {
    /// Create a new matcher from a list of recorded events.
    #[must_use]
    pub fn new(events: Vec<WorkflowEvent>) -> Self {
        // Pre-mark pause/resume events as consumed so they are transparent to
        // every cursor-based scan (issue #383). They carry no workflow command,
        // so settling them up front keeps the matcher's scan loops unchanged.
        let transparent_events = events
            .iter()
            .enumerate()
            .filter(|(_, e)| Self::is_pause_lifecycle_event(e))
            .map(|(i, _)| i)
            .collect();
        Self {
            events,
            cursor: 0,
            consumed_out_of_order_events: HashSet::new(),
            consumed_signal_events: HashSet::new(),
            pending_signals: VecDeque::new(),
            pending_external_signals: Vec::new(),
            transparent_events,
            late_race_signal_events: HashSet::new(),
        }
    }

    /// Returns `true` for operator pause/resume lifecycle events (issue #383),
    /// which are transparent no-ops for command-dispatch replay.
    const fn is_pause_lifecycle_event(event: &WorkflowEvent) -> bool {
        matches!(
            event,
            WorkflowEvent::WorkflowExecutionPaused { .. }
                | WorkflowEvent::WorkflowExecutionResumed { .. }
        )
    }

    /// Returns `true` if the event at `index` has already been consumed out-of-order.
    fn is_consumed(&self, index: usize) -> bool {
        self.consumed_out_of_order_events.contains(&index)
            || self.consumed_signal_events.contains(&index)
            || self.transparent_events.contains(&index)
    }

    fn stash_signal(&mut self, cursor: usize, signal_name: String, payload: Value) {
        self.consumed_signal_events.insert(cursor);
        // Carry the source event index so the late-race exemption (issue
        // #476) can follow the exact losing event into the stash.
        self.pending_signals
            .push_back((signal_name, payload, cursor));
    }

    fn stash_external_signal_request(
        &mut self,
        cursor: usize,
        signal_id: ExternalSignalId,
        target: ExecutionId,
        signal_name: String,
        payload: Value,
    ) {
        self.pending_external_signals.push(StashedExternalSignal {
            signal_id,
            target,
            signal_name,
            payload,
            terminal: None,
        });
        self.consumed_signal_events.insert(cursor);
    }

    fn stash_external_signal_terminal(
        &mut self,
        cursor: usize,
        signal_id: ExternalSignalId,
        terminal: StashedSignalTerminal,
    ) {
        if let Some(pending) = self
            .pending_external_signals
            .iter_mut()
            .find(|pending| pending.signal_id == signal_id)
        {
            pending.terminal = Some(terminal);
        }
        self.consumed_signal_events.insert(cursor);
    }

    /// Returns `true` for events transparent to main workflow command replay.
    ///
    /// Update events are transparent to the main workflow replay sequence —
    /// they are skipped during activity/timer/signal/child-workflow matching
    /// and are consumed by the `match_update` / `drain_admitted_updates` APIs.
    /// `WorkflowResetFork` is informational and likewise has no workflow
    /// command counterpart.
    const fn is_update_event(event: &WorkflowEvent) -> bool {
        matches!(
            event,
            WorkflowEvent::UpdateAdmitted { .. }
                | WorkflowEvent::UpdateCompleted { .. }
                | WorkflowEvent::UpdateFailed { .. }
                | WorkflowEvent::WorkflowResetFork { .. }
        )
    }

    /// Format the `actual` field for a [`HistoryMatch::Diverged`] result.
    ///
    /// When the unexpected event is a `MarkerRecorded` its name is included so
    /// the replayer's `classify_kind` can recognise `"MarkerRecorded(version:…)"`
    /// and return [`crate::testing::NonDeterminismKind::VersionMarkerMismatch`]
    /// instead of the generic command-level mismatch kind.
    fn actual_event_name(event: &WorkflowEvent) -> String {
        match event {
            WorkflowEvent::MarkerRecorded { name, .. } => format!("MarkerRecorded({name})"),
            WorkflowEvent::SideEffectRecorded { kind, name, .. } => name.as_ref().map_or_else(
                || format!("SideEffectRecorded({})", kind.as_str()),
                |n| format!("SideEffectRecorded({}:{n})", kind.as_str()),
            ),
            other => other.type_name().to_string(),
        }
    }

    /// Prepares for matching by advancing past consumed events and draining early signals.
    /// Returns `true` if there are still events to replay.
    #[allow(clippy::too_many_lines)]
    fn scan_activity_terminal(
        &mut self,
        activity_id: ActivityExecId,
        mut scan_cursor: usize,
    ) -> HistoryMatch {
        let mut first_interleaved_command = None;

        // Scan forward for Completed or Failed with matching activity_id,
        // skipping Started, Heartbeat, and other intermediate events.
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ActivityCompleted {
                    activity_id: id,
                    output,
                } if *id == activity_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityFailed {
                    activity_id: id,
                    error,
                    attempt,
                    error_type,
                    details,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
                        error_type: error_type.clone(),
                        details: details.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Skip heartbeats and started events for this activity.
                WorkflowEvent::ActivityHeartbeat {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityStarted {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                // Other activities may be scheduled and complete while this
                // activity is still running. Keep their scheduled event as the
                // next replay cursor, but scan past it to find this activity's
                // terminal event.
                WorkflowEvent::ActivityScheduled {
                    activity_id: id, ..
                } if *id != activity_id => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ActivityCompleted {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityFailed {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityTimedOut {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityHeartbeat {
                    activity_id: id, ..
                }
                | WorkflowEvent::ActivityStarted {
                    activity_id: id, ..
                } if *id != activity_id => {
                    scan_cursor += 1;
                }
                // Child workflows can run concurrently with activities.
                // Preserve replay by scanning past interleaved child starts.
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Signals can arrive at any time; stash them for later
                // wait_for_signal calls and continue scanning.
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    let signal_name = signal_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, signal_name, payload);
                    scan_cursor += 1;
                }
                // ExternalSignal event triplets can be interleaved with an
                // in-flight activity (e.g. tokio::join!(signal, activity)).
                // Stash them so match_external_signal can find them later.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                } => {
                    let stashed = StashedExternalSignal {
                        signal_id: *signal_id,
                        target: *target,
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                        terminal: None,
                    };
                    self.pending_external_signals.push(stashed);
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Update events are transparent to the activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // Fan-out markers (and any other MarkerRecorded) can be
                // interleaved when a fan-out runs concurrently with this
                // activity (e.g. via tokio::join!). Track as an interleaved
                // command so the cursor returns to it after matching the
                // terminal event. Deterministic side-effect captures (issue
                // #384) are cursor-ordered like markers and treated the same.
                WorkflowEvent::MarkerRecorded { .. } | WorkflowEvent::SideEffectRecorded { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Any other event type is unexpected mid-activity
                _ => break,
            }
        }

        // We found the Scheduled event but no terminal event — treat as
        // incomplete history (the activity was scheduled but never finished).
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ActivityInProgress { activity_id }
    }

    #[allow(clippy::too_many_lines)]
    fn scan_local_activity_terminal(
        &mut self,
        activity_id: ActivityExecId,
        mut scan_cursor: usize,
    ) -> HistoryMatch {
        let mut failed_attempts: u32 = 0;
        let mut last_error: Option<String> = None;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::LocalActivityCompleted {
                    activity_id: id,
                    output,
                } if *id == activity_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Terminal: all retries exhausted. This event is always
                // authoritative regardless of the current retry policy.
                WorkflowEvent::LocalActivityExhausted {
                    activity_id: id,
                    error,
                    attempt,
                } if *id == activity_id => {
                    // Local activities carry no typed failure payload (and may
                    // not declare a circuit breaker), so the error-type is the
                    // legacy "Error" fallback with no structured details.
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
                        error_type: "Error".to_string(),
                        details: None,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::LocalActivityFailed {
                    activity_id: id,
                    error,
                    attempt: _,
                } if *id == activity_id => {
                    failed_attempts += 1;
                    last_error = Some(error.clone());
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Signals can be ingested while a local activity is retrying
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    let signal_name = signal_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, signal_name, payload);
                    scan_cursor += 1;
                }
                // ExternalSignal events can be interleaved before the local
                // activity's terminal event when a crash recovery case writes
                // signal events first (the RunLocalActivity + SignalExternalWorkflow
                // mixed batch).  Stash them so match_external_signal can find them
                // after the local activity resolves.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                } => {
                    let stashed = StashedExternalSignal {
                        signal_id: *signal_id,
                        target: *target,
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                        terminal: None,
                    };
                    self.pending_external_signals.push(stashed);
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // Fan-out markers and deterministic side-effect captures (issue
                // #384) can be interleaved during concurrent execution.
                WorkflowEvent::MarkerRecorded { .. } | WorkflowEvent::SideEffectRecorded { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // No LocalActivityCompleted or LocalActivityExhausted found. The worker
        // either crashed before the first attempt or between retry attempts.
        // Return InProgress so the worker can resume from the right attempt.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        } else if failed_attempts > 0 {
            // Advance the cursor past the recorded failure events so the next
            // match picks up from the right position on the next worker call.
            self.cursor = scan_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::LocalActivityInProgress {
            activity_id,
            failed_attempts,
            last_error,
        }
    }

    fn prepare_match(&mut self) -> bool {
        self.advance_to_next_unconsumed_event();
        self.drain_early_signals();
        self.scan_ahead_for_external_signal_terminals();
        self.is_replaying()
    }

    /// Returns `true` if the cursor is still within the recorded history
    /// (cursor-based check only, does not inspect the early-drain stash).
    ///
    /// Used internally by `prepare_match` and other cursor-advancing methods.
    /// For the user-visible `ctx.is_replaying()` check, use
    /// [`has_buffered_history`](Self::has_buffered_history) instead.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        let mut cursor = self.cursor;
        while self.is_consumed(cursor) {
            cursor += 1;
        }
        cursor < self.events.len()
    }

    /// Returns `true` if there is any un-replayed history — either cursor-based
    /// events or signals/external-signals buffered in the early-drain stash.
    ///
    /// Use this for the user-visible `ctx.is_replaying()` check.  Stashed entries
    /// represent recorded history that `drain_early_signals` moved out of the
    /// cursor path for out-of-order matching; they are still "history" from the
    /// workflow's perspective even though the cursor is past them.
    #[must_use]
    pub fn has_buffered_history(&self) -> bool {
        self.is_replaying()
            || !self.pending_signals.is_empty()
            || !self.pending_external_signals.is_empty()
    }

    /// Number of events loaded into this replay matcher.
    #[must_use]
    pub fn event_count(&self) -> u64 {
        u64::try_from(self.events.len()).unwrap_or(u64::MAX)
    }

    /// Returns `true` if there are unconsumed events that are not terminal
    /// lifecycle events (`WorkflowCompleted`, `WorkflowFailed`,
    /// `WorkflowCancelled`, `WorkflowContinuedAsNew`), or if there are buffered
    /// signals that were never delivered via `wait_for_signal`.
    ///
    /// Used by [`crate::context::WorkflowContext::history_has_unconsumed_events`] to avoid
    /// false non-determinism reports when replaying full histories that include
    /// a terminal event appended after workflow completion.
    ///
    /// The pending-signal check is necessary because early `SignalReceived`
    /// events are moved into `consumed_signal_events` when buffered, so they
    /// are invisible to the cursor-based check.  If new code removes a
    /// `wait_for_signal` call, the buffered signal would be silently ignored
    /// without this additional check.
    #[must_use]
    pub fn has_non_lifecycle_unconsumed(&self) -> bool {
        // The exact SignalReceived events that lost a signal-or-deadline race
        // (issue #476) are excused: the timeout branch may intentionally never
        // consume them. Any other unconsumed signal still flags — including a
        // second same-name delivery when the loser was consumed by a later
        // wait (the exemption travels with the losing event, not the name).
        let mut cursor = self.cursor;
        while cursor < self.events.len() {
            if !self.is_consumed(cursor)
                && !self.events[cursor].is_terminal_lifecycle()
                && !Self::is_update_event(&self.events[cursor])
                && !self.late_race_signal_events.contains(&cursor)
            {
                return true;
            }
            cursor += 1;
        }
        // Signals buffered early (via drain_early_signals) that were never
        // consumed by wait_for_signal represent unconsumed history, except
        // for the exact events excused by a lost race.
        if self
            .pending_signals
            .iter()
            .any(|(_, _, idx)| !self.late_race_signal_events.contains(idx))
        {
            return true;
        }
        // External signals drained early that were never consumed by
        // signal_external_workflow represent unconsumed history.
        !self.pending_external_signals.is_empty()
    }

    /// Current cursor position in the event list.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.cursor
    }

    /// Advance the cursor by one, skipping the current event.
    ///
    /// Use this to skip lifecycle events like `WorkflowStarted` that
    /// don't correspond to a workflow command.
    pub fn advance(&mut self) {
        if self.cursor < self.events.len() {
            self.cursor += 1;
            self.advance_to_next_unconsumed_event();
        }
    }

    fn advance_to_next_unconsumed_event(&mut self) {
        while self.cursor < self.events.len() && self.is_consumed(self.cursor) {
            self.cursor += 1;
        }
    }

    /// Drain any `SignalReceived` and update events at the current cursor.
    ///
    /// Signals can be ingested into history at any point (when the worker
    /// picks up a task), so they may appear before an `ActivityScheduled`,
    /// `TimerStarted`, or `ChildWorkflowStarted` event even though the
    /// workflow code only calls `wait_for_signal` later.  This helper
    /// buffers those early signals so the normal matcher methods do not
    /// mis-report them as non-determinism.
    ///
    /// Update events (`UpdateAdmitted`, `UpdateCompleted`, `UpdateFailed`) are
    /// also transparent to the main workflow replay sequence and are consumed
    /// here so they don't cause spurious `Diverged` results in `prepare_match`.
    fn drain_early_signals(&mut self) {
        while self.cursor < self.events.len() {
            match &self.events[self.cursor] {
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    let signal_name = signal_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(self.cursor, signal_name, payload);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                // Drain ExternalSignal event pairs so they can be matched by
                // match_external_signal regardless of where they fall in history
                // relative to ActivityScheduled / TimerStarted events (mixed batches).
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                } => {
                    let stashed = StashedExternalSignal {
                        signal_id: *signal_id,
                        target: *target,
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                        terminal: None,
                    };
                    self.pending_external_signals.push(stashed);
                    self.consumed_signal_events.insert(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                    }
                    self.consumed_signal_events.insert(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                ev if Self::is_update_event(ev) => {
                    // Consume the update event so it doesn't block the cursor.
                    self.consumed_signal_events.insert(self.cursor);
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                }
                _ => break,
            }
        }
    }

    /// Scan remaining unconsumed history events to resolve and consume
    /// terminal events (delivered or failed) for any in-progress stashed signals.
    /// This ensures we resolve signals that are finished but blocked by subsequent
    /// un-fired timers or un-executed activities (mixed batch).
    fn scan_ahead_for_external_signal_terminals(&mut self) {
        if self.pending_external_signals.is_empty() {
            return;
        }

        let mut scan_cursor = self.cursor;
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                        self.consumed_signal_events.insert(scan_cursor);
                    }
                }
                _ => {}
            }
            scan_cursor += 1;
        }
    }

    /// Advance the cursor after a terminal event, respecting any interleaved
    /// command that needs to remain at the cursor for later replay.
    ///
    /// If `first_interleaved_command` is set, the terminal event is marked
    /// consumed and the cursor is rewound so the matching command API can pick
    /// it up. Otherwise the cursor advances past the terminal event normally.
    fn settle_terminal(
        &mut self,
        terminal_cursor: usize,
        first_interleaved_command: Option<usize>,
        result: HistoryMatch,
    ) -> HistoryMatch {
        if let Some(command_cursor) = first_interleaved_command {
            self.consumed_out_of_order_events.insert(terminal_cursor);
            // Also mark any ActivityStarted/Heartbeat events for the settled
            // activity that appear between the interleaved command and the
            // terminal. Without this, after the cursor rewinds to the
            // interleaved command and the marker/fan-out branch is replayed,
            // those progress events remain unconsumed and the next workflow
            // command match diverges against them.
            let activity_id = match &self.events[terminal_cursor] {
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityFailed { activity_id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
                _ => None,
            };
            if let Some(aid) = activity_id {
                for idx in command_cursor..terminal_cursor {
                    match &self.events[idx] {
                        WorkflowEvent::ActivityStarted {
                            activity_id: id, ..
                        }
                        | WorkflowEvent::ActivityHeartbeat {
                            activity_id: id, ..
                        } if *id == aid => {
                            self.consumed_out_of_order_events.insert(idx);
                        }
                        _ => {}
                    }
                }
            }
            self.cursor = command_cursor;
        } else {
            self.cursor = terminal_cursor + 1;
        }
        self.advance_to_next_unconsumed_event();
        result
    }

    /// Match an `execute_activity` command against history.
    ///
    /// Expects `ActivityScheduled { name }` at the current cursor position,
    /// then scans forward for `ActivityCompleted` or `ActivityFailed` with
    /// the same `activity_id`, skipping heartbeat and started events.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if a completed result is found
    /// - [`HistoryMatch::Failed`] if a failure is found
    /// - [`HistoryMatch::TimedOut`] if a timeout is found
    /// - [`HistoryMatch::NoMatch`] if past end of history
    /// - [`HistoryMatch::Diverged`] if the event at cursor is not the expected activity
    #[allow(clippy::too_many_lines)]
    pub fn match_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        // Expect ActivityScheduled at cursor
        let WorkflowEvent::ActivityScheduled {
            activity_id,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let activity_id = *activity_id;

        // Verify activity name matches
        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: format!("ActivityScheduled({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past the Scheduled event
        self.cursor += 1;
        self.scan_activity_terminal(activity_id, self.cursor)
    }

    /// Like [`match_activity`](Self::match_activity) but also verifies the input payload.
    ///
    /// Used by the [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to detect
    /// non-determinism caused by changing an activity's input arguments across deployments.
    #[allow(clippy::too_many_lines)]
    pub fn match_activity_strict(&mut self, activity_name: &str, input: &Value) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        // Extract fields as owned values so the immutable borrow ends before cursor mutation.
        let result = match &self.events[self.cursor] {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: recorded_name,
                input: recorded_input,
                ..
            } => {
                if recorded_name != activity_name {
                    return HistoryMatch::Diverged {
                        expected: format!("ActivityScheduled({activity_name})"),
                        actual: format!("ActivityScheduled({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("ActivityScheduled({activity_name}, input={input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };
        let activity_id = match result {
            Ok(id) => id,
            Err(diverged) => return diverged,
        };

        self.cursor += 1;
        self.scan_activity_terminal(activity_id, self.cursor)
    }

    /// Match an `execute_activity_external` command against history.
    ///
    /// Expects `ActivityAwaitingExternal { name }` at the current cursor, then
    /// scans forward for a terminal event (`ActivityCompletedExternally`,
    /// `ActivityFailedExternally`, or `ActivityTimedOut`) with the same
    /// `activity_id`. `ActivityExternalDeadlineExtended` events are skipped.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] when the external system completed the activity
    /// - [`HistoryMatch::Failed`] when the external system failed the activity
    /// - [`HistoryMatch::TimedOut`] when the schedule-to-close clock expired
    /// - [`HistoryMatch::AwaitingExternalCompletion`] when scheduled but no terminal yet
    /// - [`HistoryMatch::NoMatch`] when past end of history (first-time scheduling)
    /// - [`HistoryMatch::Diverged`] when history has a different event at this position
    #[allow(clippy::too_many_lines)]
    pub fn match_external_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ActivityAwaitingExternal({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityAwaitingExternal({activity_name})"),
                actual: format!("ActivityAwaitingExternal({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let activity_id = *activity_id;
        let token = *token;

        // Advance past the ActivityAwaitingExternal event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ActivityCompletedExternally {
                    activity_id: id,
                    output,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Matched {
                        output: output.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityFailedExternally {
                    activity_id: id,
                    error,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        details: None,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Deadline-extended events are informational; skip them.
                WorkflowEvent::ActivityExternalDeadlineExtended {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                // Signals can arrive while an external activity is pending.
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    let signal_name = signal_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, signal_name, payload);
                    scan_cursor += 1;
                }
                // A second ActivityAwaitingExternal for the same activity can
                // appear when a workflow is woken by a signal while still
                // awaiting external completion: the worker re-runs
                // persist_scheduled_external_activity, but record_external_task
                // is idempotent (ON CONFLICT DO NOTHING).  Skip the duplicate.
                WorkflowEvent::ActivityAwaitingExternal {
                    activity_id: id, ..
                } if *id == activity_id => {
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Update events are transparent to the external activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // ExternalSignal event triplets can be interleaved when the
                // workflow sends a concurrent external signal while awaiting
                // external activity completion (e.g. tokio::join! with
                // signal_external_workflow).  Stash them so
                // match_external_signal can find them after the activity
                // resolves, rather than breaking the scan prematurely.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                } => {
                    let stashed = StashedExternalSignal {
                        signal_id: *signal_id,
                        target: *target,
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                        terminal: None,
                    };
                    self.pending_external_signals.push(stashed);
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // Awaiting event exists in history but no terminal found yet.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::AwaitingExternalCompletion { activity_id, token }
    }

    /// Match a `signal_external_workflow` command against history.
    ///
    /// Expects `ExternalSignalRequested { target, signal_name }` at the current
    /// cursor, then scans forward for `ExternalSignalDelivered` or
    /// `ExternalSignalFailed` with the same `signal_id`.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] (output = `null`) when the signal was delivered
    /// - [`HistoryMatch::ExternalSignalFailed`] when `ExternalSignalFailed` is found in history
    /// - [`HistoryMatch::ExternalSignalInProgress`] when `ExternalSignalRequested`
    ///   exists but no terminal event yet (crash recovery path)
    /// - [`HistoryMatch::NoMatch`] when past end of history (first-time call)
    /// - [`HistoryMatch::Diverged`] when a different event is at this position
    #[allow(clippy::too_many_lines)]
    pub fn match_external_signal(
        &mut self,
        target: ExecutionId,
        signal_name: &str,
    ) -> HistoryMatch {
        // Helper: drain a matching entry from the stash and return its result.
        let try_stash = |pending: &mut Vec<StashedExternalSignal>| {
            let pos = pending
                .iter()
                .position(|p| p.target == target && p.signal_name == signal_name)?;
            let stashed = pending.remove(pos);
            Some(match stashed.terminal {
                Some(StashedSignalTerminal::Delivered) => HistoryMatch::Matched {
                    output: serde_json::Value::Null,
                },
                Some(StashedSignalTerminal::Failed(reason_code)) => {
                    HistoryMatch::ExternalSignalFailed {
                        signal_id: stashed.signal_id,
                        reason_code,
                    }
                }
                None => HistoryMatch::ExternalSignalInProgress {
                    signal_id: stashed.signal_id,
                    payload: stashed.payload,
                },
            })
        };

        // prepare_match calls drain_early_signals which eagerly stashes any
        // ExternalSignal events sitting at the current cursor. This ensures
        // terminal events at the current cursor are paired with their start
        // events before we check the stash.
        // Track the stash size so we can distinguish "no history at all"
        // from "history had a different signal here" after the drain.
        let stash_size_before = self.pending_external_signals.len();
        let has_history = self.prepare_match();

        // Check the stash (which now includes any newly drained events from
        // the current cursor position, as well as events stashed by prior
        // prepare_match calls).
        //
        // Note: stash-based matching is position-independent — if two concurrent
        // signal_external calls share a drain batch, swapping their call order in
        // the workflow code will not be detected as non-determinism.  This is an
        // accepted trade-off: concurrent signals have no authoritative ordering in
        // history; sequential calls (each in their own execution cycle) always reach
        // cursor-based matching below and DO detect reordering.
        if let Some(result) = try_stash(&mut self.pending_external_signals) {
            return result;
        }
        if !has_history {
            // If prepare_match drained ExternalSignal events (stash grew) and
            // none matched, history recorded a *different* signal at this
            // position — report non-determinism instead of silently issuing a
            // new live signal.
            if self.pending_external_signals.len() > stash_size_before {
                let actual = &self.pending_external_signals[stash_size_before];
                return HistoryMatch::Diverged {
                    expected: format!(
                        "ExternalSignalRequested(target={target}, signal={signal_name})"
                    ),
                    actual: format!(
                        "ExternalSignalRequested(target={}, signal={})",
                        actual.target, actual.signal_name
                    ),

                    event_index: i32::try_from(self.cursor).ok(),
                };
            }
            return HistoryMatch::NoMatch;
        }

        // Cursor-based path: the ExternalSignalRequested event is ahead of the
        // current position and was not yet reached by drain_early_signals.
        // Sequential signal_external calls (not concurrent) always arrive here,
        // preserving ordering guarantees — a swapped call order returns Diverged.
        let result = match &self.events[self.cursor] {
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: recorded_target,
                signal_name: recorded_name,
                payload: recorded_payload,
            } => {
                if *recorded_target != target {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ExternalSignalRequested(target={target}, signal={signal_name})"
                        ),
                        actual: format!(
                            "ExternalSignalRequested(target={recorded_target}, signal={recorded_name})"
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_name != signal_name {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ExternalSignalRequested(target={target}, signal={signal_name})"
                        ),
                        actual: format!(
                            "ExternalSignalRequested(target={target}, signal={recorded_name})"
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok((*signal_id, recorded_payload.clone()))
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("ExternalSignalRequested(target={target}, signal={signal_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };

        let (signal_id, recorded_payload) = match result {
            Ok(pair) => pair,
            Err(diverged) => return diverged,
        };

        // Advance past the ExternalSignalRequested event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalDelivered { signal_id: id } if *id == signal_id => {
                    let result = HistoryMatch::Matched {
                        output: serde_json::Value::Null,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id: id,
                    reason_code,
                } if *id == signal_id => {
                    let reason_code = reason_code.clone();
                    let result = HistoryMatch::ExternalSignalFailed {
                        signal_id,
                        reason_code,
                    };
                    return self.settle_terminal(scan_cursor, first_interleaved_command, result);
                }
                // Signals can arrive while the external signal delivery is in-flight.
                WorkflowEvent::SignalReceived {
                    signal_name: sn,
                    payload,
                } => {
                    let sn = sn.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, sn, payload);
                    scan_cursor += 1;
                }
                // Update events are transparent to the external signal scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // ExternalSignalRequested found in history but no terminal event yet.
        // Worker crashed between recording the request and the delivery outcome.
        // Return the durable payload so the caller re-sends exactly what was
        // originally recorded, regardless of any code changes since the crash.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        HistoryMatch::ExternalSignalInProgress {
            signal_id,
            payload: recorded_payload,
        }
    }

    /// Peek forward to determine if `TimerStarted` for the requested ID is the next active deterministic event in history.
    #[must_use]
    pub fn is_timer_started_next(&self, timer_id: &str) -> bool {
        let mut idx = self.cursor;
        while idx < self.events.len() {
            if self.is_consumed(idx) {
                idx += 1;
                continue;
            }
            match &self.events[idx] {
                WorkflowEvent::SignalReceived { .. } => {
                    idx += 1;
                }
                ev if Self::is_update_event(ev) => {
                    idx += 1;
                }
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    idx += 1;
                }
                WorkflowEvent::TimerStarted { timer_id: id, .. } => {
                    return id.as_str() == timer_id;
                }
                _ => return false,
            }
        }
        false
    }

    /// Match a timer command against history.
    ///
    /// Expects `TimerStarted { timer_id }` at cursor, then scans for
    /// `TimerFired` with the same `timer_id`.
    pub fn match_timer(&mut self, timer_id: &str) -> HistoryMatch {
        self.match_timer_strict(timer_id, None)
    }

    /// Match a timer command against history, strictly checking duration if provided.
    #[allow(clippy::too_many_lines)]
    pub fn match_timer_strict(
        &mut self,
        timer_id: &str,
        expected_duration: Option<u64>,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_id.as_str() != timer_id {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if let Some(expected) = expected_duration
            && *recorded_duration != expected
        {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected}s)"),
                actual: format!("TimerStarted({recorded_id}, duration={recorded_duration}s)"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past TimerStarted
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        // Scan forward for TimerFired, skipping consumed child terminals and signals.
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            if let WorkflowEvent::TimerFired { timer_id: id } = &self.events[scan_cursor]
                && id.as_str() == timer_id
            {
                let result = HistoryMatch::Matched {
                    output: Value::Null,
                };
                return self.settle_terminal(scan_cursor, first_interleaved_command, result);
            }

            if matches!(
                self.events[scan_cursor],
                WorkflowEvent::ChildWorkflowStarted { .. }
                    | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
            ) {
                first_interleaved_command.get_or_insert(scan_cursor);
                scan_cursor += 1;
                continue;
            }

            // Signals can arrive while a timer is pending; stash them for
            // later wait_for_signal calls and continue scanning.
            if let WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } = &self.events[scan_cursor]
            {
                let signal_name = signal_name.clone();
                let payload = payload.clone();
                self.stash_signal(scan_cursor, signal_name, payload);
                scan_cursor += 1;
                continue;
            }

            // ExternalSignal event triplets can be interleaved with an in-flight
            // timer (e.g. tokio::join!(signal_external, sleep)). Stash them so
            // match_external_signal can find them after the timer resolves.
            match &self.events[scan_cursor] {
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                } => {
                    let stashed = StashedExternalSignal {
                        signal_id: *signal_id,
                        target: *target,
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                        terminal: None,
                    };
                    self.pending_external_signals.push(stashed);
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                    continue;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    let id = *signal_id;
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Delivered);
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                    continue;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    let id = *signal_id;
                    let code = reason_code.clone();
                    if let Some(p) = self
                        .pending_external_signals
                        .iter_mut()
                        .find(|p| p.signal_id == id)
                    {
                        p.terminal = Some(StashedSignalTerminal::Failed(code));
                    }
                    self.consumed_signal_events.insert(scan_cursor);
                    scan_cursor += 1;
                    continue;
                }
                _ => {}
            }

            // Update events are transparent to the timer scan.
            if Self::is_update_event(&self.events[scan_cursor]) {
                scan_cursor += 1;
                continue;
            }

            break;
        }

        // Timer was started but never fired — incomplete history
        HistoryMatch::NoMatch
    }

    /// Match a signal wait command against history.
    ///
    /// Expects `SignalReceived { signal_name }` at the current cursor.
    pub fn match_signal(&mut self, signal_name: &str) -> HistoryMatch {
        if let Some(index) = self
            .pending_signals
            .iter()
            .position(|(name, _, _)| name == signal_name)
            && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
        {
            return HistoryMatch::Matched { output: payload };
        }

        self.advance_to_next_unconsumed_event();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;
        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } if recorded_name == signal_name => {
                    let output = payload.clone();
                    self.consumed_signal_events.insert(scan_cursor);
                    self.cursor =
                        first_interleaved_command.unwrap_or_else(|| scan_cursor.saturating_add(1));
                    self.advance_to_next_unconsumed_event();

                    return HistoryMatch::Matched { output };
                }
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } => {
                    let recorded_name = recorded_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, recorded_name, payload);
                    scan_cursor += 1;
                }
                ev if Self::is_update_event(ev) => {
                    // Update events are transparent to signal scanning.
                    scan_cursor += 1;
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // ExternalSignal event triplets can appear before SignalReceived
                // when a mixed batch (e.g. tokio::join!(wait_for_signal, signal_external))
                // wrote signal events first.  Stash them for later match_external_signal.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name: sn,
                    payload,
                } => {
                    self.stash_external_signal_request(
                        scan_cursor,
                        *signal_id,
                        *target,
                        sn.clone(),
                        payload.clone(),
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Delivered,
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Failed(reason_code.clone()),
                    );
                    scan_cursor += 1;
                }
                other => {
                    if first_interleaved_command.is_some() {
                        return HistoryMatch::NoMatch;
                    }
                    return HistoryMatch::Diverged {
                        expected: format!("SignalReceived({signal_name})"),
                        actual: Self::actual_event_name(other),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
            }
        }

        HistoryMatch::NoMatch
    }

    /// Settle the bookkeeping for a signal-branch win of a signal-or-deadline
    /// race (issue #476): consume the winning `SignalReceived` event, consume
    /// the stray `TimerFired` of the race timer if it is already recorded (if
    /// the timer fires only after this replay cycle, the next full replay
    /// re-runs the same scan and consumes it then), and settle the cursor on
    /// the first interleaved sibling command, or just past the winning signal.
    fn settle_race_signal_won(
        &mut self,
        signal_pos: usize,
        first_interleaved_command: Option<usize>,
        timer_id: &str,
    ) {
        self.consumed_signal_events.insert(signal_pos);
        let mut fired_scan = signal_pos + 1;
        while fired_scan < self.events.len() {
            if !self.is_consumed(fired_scan)
                && let WorkflowEvent::TimerFired { timer_id: id } = &self.events[fired_scan]
                && id.as_str() == timer_id
            {
                self.consumed_out_of_order_events.insert(fired_scan);
                break;
            }
            fired_scan += 1;
        }
        self.cursor = first_interleaved_command.unwrap_or_else(|| signal_pos.saturating_add(1));
        self.advance_to_next_unconsumed_event();
    }

    /// Match a signal-vs-deadline race against history (issue #476).
    ///
    /// The race composes the existing `TimerStarted`/`TimerFired` and
    /// `SignalReceived` events — no new event variant. The winner is the
    /// resolution event that appears **first in recorded history**:
    ///
    /// - A `SignalReceived { signal_name }` before `TimerFired { timer_id }`
    ///   (or a signal that was stashed/recorded before the race even started)
    ///   → [`SignalOrTimerMatch::SignalWon`]. A stray `TimerFired` for the
    ///   race's timer that lands later in history is marked consumed so
    ///   subsequent matches do not diverge against it.
    /// - A `TimerFired { timer_id }` before any matching signal
    ///   → [`SignalOrTimerMatch::TimerWon`]. A matching signal recorded
    ///   *after* the fire is **not** consumed: it stays observable by a
    ///   subsequent signal wait.
    ///
    /// Non-matching signals and external-signal triplets encountered during
    /// the scan are stashed exactly like in [`Self::match_timer_strict`].
    #[allow(clippy::too_many_lines)]
    pub fn match_signal_or_timer(
        &mut self,
        signal_name: &str,
        timer_id: &str,
        expected_duration: Option<u64>,
    ) -> SignalOrTimerMatch {
        let replaying = self.prepare_match();

        // A signal stashed by an earlier scan — or drained by prepare_match
        // just now — whose recorded position precedes this race point arrived
        // before the race started: the signal wins and the timer was never
        // started on the matching live run. Stashed signals recorded at or
        // after the race point must NOT short-circuit here — history order
        // decides, so they compete at their recorded index during the scan
        // below.
        let race_pos = self.cursor;
        if let Some(index) = self
            .pending_signals
            .iter()
            .position(|(name, _, idx)| name == signal_name && *idx < race_pos)
            && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
        {
            return SignalOrTimerMatch::SignalWon { payload };
        }

        if !replaying {
            return SignalOrTimerMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            duration_secs: recorded_duration,
        } = &self.events[self.cursor]
        else {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_id.as_str() != timer_id {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        if let Some(expected) = expected_duration
            && *recorded_duration != expected
        {
            return SignalOrTimerMatch::Diverged {
                expected: format!("TimerStarted({timer_id}, duration={expected}s)"),
                actual: format!("TimerStarted({recorded_id}, duration={recorded_duration}s)"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past TimerStarted, then scan for the first resolution event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_command = None;

        while scan_cursor < self.events.len() {
            if self.is_consumed(scan_cursor) {
                // A matching signal stashed by a sibling scan (consumed but
                // still undelivered in the pending buffer) competes at its
                // recorded position: if it precedes the race's TimerFired in
                // history, the signal branch wins.
                if let Some(index) = self
                    .pending_signals
                    .iter()
                    .position(|(name, _, idx)| *idx == scan_cursor && name == signal_name)
                    && let Some((_name, payload, _idx)) = self.pending_signals.remove(index)
                {
                    self.settle_race_signal_won(scan_cursor, first_interleaved_command, timer_id);
                    return SignalOrTimerMatch::SignalWon { payload };
                }
                scan_cursor += 1;
                continue;
            }

            match &self.events[scan_cursor] {
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } if recorded_name == signal_name => {
                    let payload = payload.clone();
                    self.settle_race_signal_won(scan_cursor, first_interleaved_command, timer_id);
                    return SignalOrTimerMatch::SignalWon { payload };
                }

                WorkflowEvent::TimerFired { timer_id: id } if id.as_str() == timer_id => {
                    // The first matching signal recorded after this fire is
                    // the exact event that lost the race. It stays deliverable
                    // to a later signal wait, but if the timeout branch
                    // intentionally ignores it, the completed history must
                    // still pass the strict unconsumed check. Each race claims
                    // a distinct loser, so skip indices already claimed.
                    let loser_index = (scan_cursor + 1..self.events.len()).find(|idx| {
                        !self.late_race_signal_events.contains(idx)
                            && matches!(
                                &self.events[*idx],
                                WorkflowEvent::SignalReceived { signal_name: n, .. } if n == signal_name
                            )
                    });
                    if let Some(idx) = loser_index {
                        self.late_race_signal_events.insert(idx);
                    }
                    if let Some(command_cursor) = first_interleaved_command {
                        self.consumed_out_of_order_events.insert(scan_cursor);
                        self.cursor = command_cursor;
                    } else {
                        self.cursor = scan_cursor + 1;
                    }
                    self.advance_to_next_unconsumed_event();
                    return SignalOrTimerMatch::TimerWon;
                }

                // Other signals can arrive while the race is pending; stash
                // them for later signal waits and continue scanning.
                WorkflowEvent::SignalReceived {
                    signal_name: recorded_name,
                    payload,
                } => {
                    let recorded_name = recorded_name.clone();
                    let payload = payload.clone();
                    self.stash_signal(scan_cursor, recorded_name, payload);
                    scan_cursor += 1;
                }

                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }

                // Concurrent commands (tokio::join! siblings) can interleave
                // with the pending race. Keep the first one as the next replay
                // cursor — mirroring scan_activity_terminal — and scan past it
                // so a resolution event recorded later is still found.
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::TimerStarted { .. } => {
                    first_interleaved_command.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }

                // Progress and terminal events of those concurrent siblings
                // (and fires of foreign timers) are transparent to the race
                // scan — their own matchers consume them after the rewind.
                WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityHeartbeat { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::TimerFired { .. } => {
                    scan_cursor += 1;
                }

                // ExternalSignal event triplets can be interleaved with the
                // pending race; stash them for later match_external_signal.
                WorkflowEvent::ExternalSignalRequested {
                    signal_id,
                    target,
                    signal_name: sn,
                    payload,
                } => {
                    self.stash_external_signal_request(
                        scan_cursor,
                        *signal_id,
                        *target,
                        sn.clone(),
                        payload.clone(),
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Delivered,
                    );
                    scan_cursor += 1;
                }
                WorkflowEvent::ExternalSignalFailed {
                    signal_id,
                    reason_code,
                } => {
                    self.stash_external_signal_terminal(
                        scan_cursor,
                        *signal_id,
                        StashedSignalTerminal::Failed(reason_code.clone()),
                    );
                    scan_cursor += 1;
                }

                _ => break,
            }
        }

        // Timer started but neither resolution event is recorded yet. Rewind
        // to the first interleaved command so a concurrent sibling's matcher
        // still finds its own events.
        if let Some(command_cursor) = first_interleaved_command {
            self.cursor = command_cursor;
            self.advance_to_next_unconsumed_event();
        }
        SignalOrTimerMatch::InProgress
    }

    /// Match a continue-as-new command against history.
    ///
    /// Expects `WorkflowContinuedAsNew { input }` at the current cursor.
    pub fn match_continue_as_new(&mut self, input: &Value) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::WorkflowContinuedAsNew {
            input: recorded_input,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("WorkflowContinuedAsNew({input})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };

        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("WorkflowContinuedAsNewInput({input})"),
                actual: format!("WorkflowContinuedAsNewInput({recorded_input})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let output = recorded_input.clone();
        self.cursor += 1;
        self.advance_to_next_unconsumed_event();
        HistoryMatch::Matched { output }
    }

    /// Match a child workflow command against history.
    ///
    /// Expects `ChildWorkflowStarted { workflow_name }` at cursor, then scans for
    /// a terminal `ChildWorkflowCompleted` or `ChildWorkflowFailed` with the same
    /// `child_id`.
    pub fn match_child_workflow(&mut self, workflow_name: &str, input: &Value) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let start_cursor = self.cursor;
        let WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: recorded_name,
            input: recorded_input,
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let child_id = *child_id;

        if recorded_name != workflow_name {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: format!("ChildWorkflowStarted({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }
        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowInput({input})"),
                actual: format!("ChildWorkflowInput({recorded_input})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        let mut scan_cursor = self.cursor + 1;

        while scan_cursor < self.events.len() {
            match &self.events[scan_cursor] {
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: id,
                    output,
                } if *id == child_id => {
                    let output = output.clone();
                    self.consumed_out_of_order_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Matched { output };
                }
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: id,
                    error,
                } if *id == child_id => {
                    let error = error.clone();
                    self.consumed_out_of_order_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Failed {
                        error,
                        attempt: 1,
                        error_type: "Error".to_string(),
                        details: None,
                    };
                }
                _ => scan_cursor += 1,
            }
        }

        // The start event was found and name+input matched, but the terminal
        // hasn't arrived yet.  This is the normal state when the parent wakes
        // after one of several parallel children completes while this child is
        // still running.  Return ChildInProgress so the caller can re-emit a
        // StartChildWorkflow command with the existing child_id rather than
        // treating the incomplete history as a non-determinism error.
        self.cursor = start_cursor + 1;
        self.advance_to_next_unconsumed_event();
        HistoryMatch::ChildInProgress { child_id }
    }

    /// Match a detached child workflow spawn against history.
    ///
    /// Expects `ChildWorkflowSpawnedDetached { workflow_name, input,
    /// parent_close_policy }` at the current cursor position. Returns the
    /// recorded `child_id` so the workflow function gets back the same
    /// [`ExecutionId`] across replay cycles.
    ///
    /// Returns:
    /// - `DetachedChildSpawned { child_id }` when the event matches
    /// - `NoMatch` when the cursor is past the end of history (new live spawn)
    /// - `Diverged` when the event at cursor is not the expected spawn event
    pub fn match_detached_child_spawn(
        &mut self,
        workflow_name: &str,
        input: &Value,
        parent_close_policy: ParentClosePolicy,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: recorded_name,
                input: recorded_input,
                parent_close_policy: recorded_policy,
            } => {
                if recorded_name != workflow_name {
                    return HistoryMatch::Diverged {
                        expected: format!("ChildWorkflowSpawnedDetached({workflow_name})"),
                        actual: format!("ChildWorkflowSpawnedDetached({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!("DetachedChildWorkflowInput({input})"),
                        actual: format!("DetachedChildWorkflowInput({recorded_input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if *recorded_policy != parent_close_policy {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "DetachedChildWorkflowPolicy({})",
                            parent_close_policy.as_str()
                        ),
                        actual: format!(
                            "DetachedChildWorkflowPolicy({})",
                            recorded_policy.as_str()
                        ),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                let child_id = *child_id;
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::DetachedChildSpawned { child_id }
            }
            other => HistoryMatch::Diverged {
                expected: format!("ChildWorkflowSpawnedDetached({workflow_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Match a version gate against history.
    ///
    /// Looks for a `MarkerRecorded { name: "version:{change_id}" }` at
    /// the current cursor position.
    ///
    /// Returns:
    /// - The recorded version if a matching marker is found
    /// - `min_version` if no marker exists (old workflow before versioning)
    /// - `max_version` if past end of history (new code path)
    #[must_use]
    pub fn match_side_effect(&mut self, side_effect_id: &str) -> HistoryMatch {
        self.match_side_effect_event(SideEffectKind::Custom, Some(side_effect_id))
    }

    /// Match a deterministic side-effect capture against history (issue #384).
    ///
    /// All of the `WorkflowContext` deterministic primitives — `system_now()`,
    /// `new_uuid()`, `random_*()`, and `side_effect()` — lower onto a single
    /// [`WorkflowEvent::SideEffectRecorded`] variant and match through this
    /// method in command (cursor) order. The recorded `value` is returned via
    /// [`HistoryMatch::Matched`].
    ///
    /// `kind` distinguishes the built-in helper; `name` is `Some` only for the
    /// author-named `side_effect()` path. A mismatch in either the `kind` or the
    /// `name` at the current cursor surfaces as [`HistoryMatch::Diverged`] with
    /// the same diagnostic quality as the activity-mismatch path.
    ///
    /// **Backward compatibility:** the pre-#384 `side_effect()` implementation
    /// lowered onto `MarkerRecorded { name: "side_effect:{id}" }`. For the
    /// `Custom` + named case this method also accepts that legacy marker so
    /// in-flight executions recorded under the old engine replay unchanged.
    ///
    /// Uses `prepare_match` so `drain_early_signals` skips `ExternalSignal`
    /// events that may have been written before this capture in a mixed batch.
    #[must_use]
    pub fn match_side_effect_event(
        &mut self,
        kind: SideEffectKind,
        name: Option<&str>,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let expected = name.map_or_else(
            || format!("SideEffectRecorded({})", kind.as_str()),
            |n| format!("SideEffectRecorded({}:{n})", kind.as_str()),
        );

        // Legacy compatibility: the old side_effect() lowered to a MarkerRecorded
        // with name "side_effect:{id}". Only the Custom+named path ever produced it.
        let legacy_marker_name = name
            .filter(|_| kind == SideEffectKind::Custom)
            .map(|n| format!("side_effect:{n}"));

        match &self.events[self.cursor] {
            WorkflowEvent::SideEffectRecorded {
                kind: recorded_kind,
                name: recorded_name,
                value,
            } if *recorded_kind == kind && recorded_name.as_deref() == name => {
                let output = value.clone();
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched { output }
            }
            WorkflowEvent::MarkerRecorded {
                name: marker_name,
                details,
            } if legacy_marker_name.as_deref() == Some(marker_name.as_str()) => {
                let output = details.clone();
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched { output }
            }
            other => HistoryMatch::Diverged {
                expected,
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Match a local activity command against history.
    ///
    /// Expects `LocalActivityScheduled { name }` at the current cursor, then
    /// scans forward for `LocalActivityCompleted` (returns [`HistoryMatch::Matched`])
    /// or exhausts `LocalActivityFailed` events and returns the last failure
    /// (returns [`HistoryMatch::Failed`]).
    ///
    /// Intermediate `LocalActivityFailed` events (when the handler was retried
    /// inline) are skipped; only the final outcome is surfaced to the workflow.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if the activity eventually succeeded
    /// - [`HistoryMatch::Failed`] if retries were exhausted (last recorded failure)
    /// - [`HistoryMatch::NoMatch`] if past end of history (first-time execution)
    /// - [`HistoryMatch::Diverged`] if history has a different event at this position
    pub fn match_local_activity(&mut self, activity_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::LocalActivityScheduled {
            activity_id,
            name: recorded_name,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),

                event_index: i32::try_from(self.cursor).ok(),
            };
        };
        let activity_id = *activity_id;

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: format!("LocalActivityScheduled({recorded_name})"),

                event_index: i32::try_from(self.cursor).ok(),
            };
        }

        // Advance past LocalActivityScheduled
        self.cursor += 1;
        self.scan_local_activity_terminal(activity_id, self.cursor)
    }

    /// Like [`match_local_activity`](Self::match_local_activity) but also verifies the input payload.
    ///
    /// Used by the [`WorkflowReplayer`](crate::testing::WorkflowReplayer) in strict replay mode.
    pub fn match_local_activity_strict(
        &mut self,
        activity_name: &str,
        input: &Value,
    ) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let result = match &self.events[self.cursor] {
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: recorded_name,
                input: recorded_input,
            } => {
                if recorded_name != activity_name {
                    return HistoryMatch::Diverged {
                        expected: format!("LocalActivityScheduled({activity_name})"),
                        actual: format!("LocalActivityScheduled({recorded_name})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "LocalActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("LocalActivityScheduled({activity_name}, input={input})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            }),
        };
        let activity_id = match result {
            Ok(id) => id,
            Err(diverged) => return diverged,
        };

        self.cursor += 1;
        match self.scan_local_activity_terminal(activity_id, self.cursor) {
            HistoryMatch::LocalActivityInProgress {
                failed_attempts: 0, ..
            } => HistoryMatch::NoMatch,
            other => other,
        }
    }

    /// Versioning mechanism for safe workflow code changes.
    ///
    /// Checks the recorded history for a version marker. If the marker is present
    /// in history, returns the recorded version. If not in history (first execution
    /// or unversioned branch), records `max_version` as a marker and returns it.
    ///
    /// This ensures that existing non-deterministic executions continue correctly,
    /// while new executions start on the new `max_version` path.
    pub fn match_version(&mut self, change_id: &str, min_version: u32, max_version: u32) -> u32 {
        self.advance_to_next_unconsumed_event();
        let marker_name = format!("version:{change_id}");

        // Check BEFORE draining: if already past cursor-based history, this is
        // a genuinely new code path → record max_version.
        if !self.is_replaying() {
            return max_version;
        }

        // Now safe to drain ExternalSignal events that may precede this marker
        // in a mixed batch (e.g. tokio::join!(ctx.version(...), signal_external)).
        self.drain_early_signals();

        // After draining: if cursor is past end (only stashed ExternalSignal
        // events were the remaining history), this is an unversioned position —
        // return min_version so existing executions stay on the old branch
        // instead of recording a new marker and jumping to max_version.
        if !self.is_replaying() {
            return min_version;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let version = details
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(min_version);
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                version
            }
            // No marker at current position — old workflow that didn't have
            // this version gate. Don't advance cursor.
            _ => min_version,
        }
    }

    // ── Fan-out / parallel activities (issue #359) ───────────────────────────

    /// Match the count marker for a fan-out group against history.
    ///
    /// On the **first live execution** (past end of history) returns
    /// [`HistoryMatch::NoMatch`] so the caller can emit a `RecordMarker`
    /// command for replay.
    ///
    /// During **replay** expects a `MarkerRecorded { name: "fan_out:{seq}", … }`
    /// event at the current cursor. Returns:
    ///
    /// - [`HistoryMatch::Matched`] — marker found and recorded count equals `count`.
    /// - [`HistoryMatch::Diverged`] — recorded count differs from `count`
    ///   (non-deterministic collection resize detected), or a different event
    ///   type was at this cursor position.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution).
    pub fn match_fan_out_marker(&mut self, seq: u32, count: usize) -> HistoryMatch {
        let marker_name = format!("fan_out:{seq}");
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let recorded_count = details
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(0);
                if recorded_count == count {
                    self.cursor += 1;
                    self.advance_to_next_unconsumed_event();
                    HistoryMatch::Matched {
                        output: serde_json::json!(count),
                    }
                } else {
                    HistoryMatch::Diverged {
                        expected: format!("fan_out:{seq}(count={recorded_count})"),
                        actual: format!("fan_out:{seq}(count={count})"),

                        event_index: i32::try_from(self.cursor).ok(),
                    }
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),

                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    /// Match a named `MarkerRecorded` event at the current cursor position.
    ///
    /// Used by `WorkflowContext::dag_skip_marker` to record the condition-skip
    /// decision deterministically so replay always selects the identical branch.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] — the event at cursor is `MarkerRecorded`
    ///   with the exact `name`.
    /// - [`HistoryMatch::Diverged`] — a different event (or a marker with a
    ///   different name) is at the cursor; this indicates the condition predicate
    ///   returned a different value than during the original run — a
    ///   non-determinism violation.
    /// - [`HistoryMatch::NoMatch`] — past end of history (live execution).
    pub fn match_named_marker(&mut self, marker_name: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }
        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, .. } if name == marker_name => {
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched {
                    output: serde_json::Value::Null,
                }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),
                event_index: i32::try_from(self.cursor).ok(),
            },
        }
    }

    // ── Update primitive (issue #140) ─────────────────────────────────────

    /// Look up the recorded result for a specific update by `update_id`.
    ///
    /// Unlike the cursor-based activity/timer/signal matching methods, this
    /// performs a full-history scan keyed by `update_id`. It does **not**
    /// advance the cursor — update events are managed independently of the
    /// main workflow replay sequence.
    ///
    /// Returns:
    /// - [`HistoryMatch::Matched`] if `UpdateCompleted` with the given ID is found.
    /// - [`HistoryMatch::Failed`] if `UpdateFailed` with the given ID is found.
    /// - [`HistoryMatch::NoMatch`] if only `UpdateAdmitted` exists (in-flight) or
    ///   if the ID is entirely unknown.
    #[must_use]
    pub fn match_update(&self, update_id: UpdateId) -> HistoryMatch {
        for event in &self.events {
            match event {
                WorkflowEvent::UpdateCompleted {
                    update_id: id,
                    output,
                } if *id == update_id => {
                    return HistoryMatch::Matched {
                        output: output.clone(),
                    };
                }
                WorkflowEvent::UpdateFailed {
                    update_id: id,
                    error,
                } if *id == update_id => {
                    return HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        details: None,
                    };
                }
                _ => {}
            }
        }
        HistoryMatch::NoMatch
    }

    /// Return all `UpdateAdmitted` events that do not have a paired
    /// `UpdateCompleted` or `UpdateFailed` event in history.
    ///
    /// The worker calls this on restart to discover in-flight updates that
    /// must be re-dispatched to their registered handlers.
    ///
    /// Returns a `Vec` of `(update_id, handler_name, input)` tuples.
    #[must_use]
    pub fn drain_admitted_updates(&self) -> Vec<(UpdateId, String, Value)> {
        // Collect IDs of completed/failed updates.
        let mut resolved: std::collections::HashSet<UpdateId> = std::collections::HashSet::new();
        for event in &self.events {
            match event {
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => {
                    resolved.insert(*update_id);
                }
                _ => {}
            }
        }

        // Return admitted updates that are not yet resolved.
        self.events
            .iter()
            .filter_map(|event| {
                if let WorkflowEvent::UpdateAdmitted {
                    update_id,
                    name,
                    input,
                    ..
                } = event
                    && !resolved.contains(update_id)
                {
                    return Some((*update_id, name.clone(), input.clone()));
                }
                None
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TimeoutType;
    use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};
    use chrono::Utc;

    /// Helper: build a minimal activity lifecycle (Scheduled -> Completed).
    fn activity_completed_events(
        name: &str,
        output: Value,
    ) -> (ActivityExecId, Vec<WorkflowEvent>) {
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: name.into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output,
            },
        ];
        (id, events)
    }

    /// Helper: build an activity lifecycle with failure.
    fn activity_failed_events(
        name: &str,
        error: &str,
        attempt: u32,
    ) -> (ActivityExecId, Vec<WorkflowEvent>) {
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: name.into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: id,
                error: error.into(),
                attempt,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
        ];
        (id, events)
    }

    #[test]
    fn matcher_replays_completed_activity() {
        let output = serde_json::json!({"email_id": "msg-001"});
        let (_id, events) = activity_completed_events("send_email", output.clone());

        let mut matcher = HistoryMatcher::new(events);
        assert!(matcher.is_replaying());
        assert_eq!(matcher.position(), 0);

        let result = matcher.match_activity("send_email");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_replays_failed_activity() {
        let (_id, events) = activity_failed_events("send_email", "SMTP connection refused", 3);

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "SMTP connection refused".into(),
                attempt: 3,
                error_type: "Error".into(),
                details: None,
            }
        );
    }

    #[test]
    fn matcher_replays_timed_out_activity() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: TimeoutType::Heartbeat,
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert_eq!(
            result,
            HistoryMatch::TimedOut {
                timeout_type: TimeoutType::Heartbeat,
            }
        );
    }

    #[test]
    fn matcher_returns_no_match_at_end_of_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        assert!(!matcher.is_replaying());
        assert_eq!(matcher.position(), 0);

        let result = matcher.match_activity("send_email");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_detects_non_determinism_wrong_event_type() {
        // History has a TimerStarted where we expect ActivityScheduled
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t1"),
            duration_secs: 60,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));

        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("send_email"));
            assert!(actual.contains("TimerStarted"));
        }
    }

    #[test]
    fn matcher_detects_non_determinism_wrong_activity_name() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "charge_payment".into(),
            input: Value::Null,
            queue: "default".into(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("send_email");

        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("send_email"));
            assert!(actual.contains("charge_payment"));
        }
    }

    #[test]
    fn matcher_skips_heartbeats_during_replay() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"rows": 1000});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: id,
                worker_id: WorkerId::new("worker-1"),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 25}),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 50}),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::json!({"progress": 75}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("import_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        // Cursor should be past all 6 events
        assert_eq!(matcher.position(), 6);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_replays_timer() {
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("cooldown"),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_timer("cooldown");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_timer_no_match_at_end() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_timer("t1");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_timer_detects_divergence() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "foo".into(),
            input: Value::Null,
            queue: "default".into(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_timer("t1");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
    }

    #[test]
    fn matcher_replays_continue_as_new() {
        let payload = serde_json::json!({"phase": "next"});
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: payload.clone(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&payload);
        assert_eq!(result, HistoryMatch::Matched { output: payload });
        assert_eq!(matcher.position(), 1);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_continue_as_new_input_mismatch_diverges() {
        let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
            new_exec_id: ExecutionId::new(),
            input: serde_json::json!({"phase": "next"}),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&serde_json::json!({"phase": "later"}));
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_continue_as_new_wrong_event_diverges() {
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t1"),
            duration_secs: 60,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_continue_as_new(&serde_json::json!({"phase": "next"}));
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_replays_version_marker() {
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "version:billing_v2".into(),
            details: serde_json::json!(2),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 2);
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_version_returns_min_for_old_workflow() {
        // Old workflow has a different event at this position — no marker
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let mut matcher = HistoryMatcher::new(events);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 1);
        // Cursor should NOT advance — the event isn't consumed
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_version_returns_max_past_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let version = matcher.match_version("billing_v2", 1, 3);
        assert_eq!(version, 3);
    }

    #[test]
    fn matcher_replays_child_workflow_completion() {
        let child_id = crate::types::ExecutionId::new();
        let output = serde_json::json!({"result": "ok"});
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 42}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: output.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &serde_json::json!({"id": 42}));
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_replays_child_workflow_failure() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowFailed {
                child_id,
                error: "child failed".into(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &Value::Null);
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "child failed".into(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
            }
        );
    }

    #[test]
    fn matcher_child_workflow_without_terminal_returns_child_in_progress() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".into(),
            input: Value::Null,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &Value::Null);
        assert!(
            matches!(result, HistoryMatch::ChildInProgress { child_id: id } if id == child_id),
            "should be ChildInProgress with the known child_id, got {result:?}"
        );
        // Cursor advances past the ChildWorkflowStarted event so subsequent
        // commands (e.g. other parallel children) can be matched correctly.
        assert_eq!(
            matcher.position(),
            1,
            "cursor must advance past started event"
        );
    }

    #[test]
    fn matcher_child_workflow_scans_past_interleaved_events() {
        let child_a = crate::types::ExecutionId::new();
        let child_b = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 1}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": 2}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &serde_json::json!({"id": 1}));
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_child_workflow_input_mismatch_diverges() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"sku":"book"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let result =
            matcher.match_child_workflow("process_order", &serde_json::json!({"sku":"magazine"}));
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(matcher.position(), 0);
    }

    #[test]
    fn matcher_child_workflow_keeps_interleaved_starts_replayable() {
        let child_a = crate::types::ExecutionId::new();
        let child_b = crate::types::ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "B"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_b,
                output: serde_json::json!({"id": "B", "ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let a = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert_eq!(
            a,
            HistoryMatch::Matched {
                output: serde_json::json!({"id": "A", "ok": true}),
            }
        );
        // Cursor should stay at Started(B), not advance past it.
        assert_eq!(matcher.position(), 1);

        let b = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"B"}));
        assert_eq!(
            b,
            HistoryMatch::Matched {
                output: serde_json::json!({"id": "B", "ok": true}),
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_child_workflow_keeps_interleaved_activity_replayable() {
        let child_a = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        // Cursor should remain at the interleaved activity schedule.
        assert_eq!(matcher.position(), 1);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        // The consumed child terminal event is skipped automatically.
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_activity_scan_skips_consumed_interleaved_child_terminal() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id": "A"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id": "A", "ok": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 1);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_activity_scan_skips_interleaved_child_start() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!({"sent": true}),
            }
        );
        // Cursor stays at interleaved child start so child replay remains deterministic.
        assert_eq!(matcher.position(), 1);
    }

    #[test]
    fn matcher_activity_replay_preserves_interleaved_child_start_for_later_child_match() {
        let child_id = crate::types::ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent": true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id":"A","ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let activity = matcher.match_activity("send_email");
        assert!(matches!(activity, HistoryMatch::Matched { .. }));
        // Cursor should stay on ChildWorkflowStarted for later child replay.
        assert_eq!(matcher.position(), 1);

        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_timer_scan_skips_consumed_interleaved_child_terminal() {
        let child_id = crate::types::ExecutionId::new();
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 1);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_timer_replay_preserves_interleaved_child_start_for_later_child_match() {
        let child_id = crate::types::ExecutionId::new();
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::TimerFired { timer_id },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id":"A","ok": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        let timer = matcher.match_timer("cooldown");
        assert!(matches!(timer, HistoryMatch::Matched { .. }));
        // Cursor should stay on ChildWorkflowStarted for later child replay.
        assert_eq!(matcher.position(), 1);

        let child = matcher.match_child_workflow("process_order", &serde_json::json!({"id":"A"}));
        assert!(matches!(child, HistoryMatch::Matched { .. }));
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn advance_skips_current_event() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"done": true}),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);
        assert_eq!(matcher.position(), 0);

        matcher.advance();
        assert_eq!(matcher.position(), 1);

        matcher.advance();
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());

        // Advance past end is a no-op
        matcher.advance();
        assert_eq!(matcher.position(), 2);
    }

    #[test]
    fn matcher_replays_multiple_activities_in_sequence() {
        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let output1 = serde_json::json!({"email_id": "msg-001"});
        let output2 = serde_json::json!({"charge_id": "ch-999"});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id1,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id1,
                output: output1.clone(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id2,
                name: "charge_payment".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id2,
                output: output2.clone(),
            },
        ];

        let mut matcher = HistoryMatcher::new(events);

        let r1 = matcher.match_activity("send_email");
        assert_eq!(r1, HistoryMatch::Matched { output: output1 });

        let r2 = matcher.match_activity("charge_payment");
        assert_eq!(r2, HistoryMatch::Matched { output: output2 });

        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_rewinds_to_later_sibling_when_earlier_activity_is_in_progress() {
        let earlier_id = ActivityExecId::new();
        let later_id = ActivityExecId::new();
        let later_output = serde_json::json!({"done": "later"});

        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: earlier_id,
                name: "slow_task".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id: earlier_id,
                worker_id: WorkerId::new("worker-a"),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: later_id,
                name: "fast_task".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: later_id,
                output: later_output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_activity("slow_task"),
            HistoryMatch::ActivityInProgress {
                activity_id: earlier_id
            }
        );
        assert_eq!(
            matcher.position(),
            2,
            "in-progress replay must rewind to the first interleaved sibling command"
        );

        assert_eq!(
            matcher.match_activity("fast_task"),
            HistoryMatch::Matched {
                output: later_output
            }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_treats_reset_fork_marker_as_informational() {
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id: ExecutionId::new(),
                reset_to_event_id: 0,
                reason: "bad deploy".into(),
                operator_id: "oncall".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "resume_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_activity("resume_work");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_timer_started_peek_skips_detached_spawn() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "condition_sidecar".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("condition-timeout"),
                duration_secs: 30,
            },
        ];
        let matcher = HistoryMatcher::new(events);

        assert!(
            matcher.is_timer_started_next("condition-timeout"),
            "timer peek should skip an unconsumed detached-spawn event"
        );
    }

    #[test]
    fn matcher_replays_signal_payload() {
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approved".into(),
            payload: serde_json::json!({"ok": true}),
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal("approved");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
    }

    #[test]
    fn matcher_skips_unrelated_signals_while_waiting() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_signal("approved");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );
        assert_eq!(
            matcher.position(),
            2,
            "cursor should advance beyond matched signal to avoid stale divergences"
        );
    }

    #[test]
    fn matcher_preserves_unrelated_signal_for_later_wait() {
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let approved = matcher.match_signal("approved");
        assert_eq!(
            approved,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true}),
            }
        );

        let cancel = matcher.match_signal("cancel");
        assert_eq!(
            cancel,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "manual"}),
            }
        );
    }

    #[test]
    fn matcher_allows_non_signal_command_after_out_of_order_signal_match() {
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 5,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let approved = matcher.match_signal("approved");
        assert!(matches!(approved, HistoryMatch::Matched { .. }));

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
    }

    #[test]
    fn matcher_activity_skips_signal_ingested_before_activity_scheduled() {
        // A signal arrives before the workflow runs its first activity.
        // ingest_pending_signals would place SignalReceived at position 0
        // (the first position after WorkflowStarted is skipped).
        // match_activity must buffer the early signal and still find
        // ActivityScheduled + ActivityCompleted.
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"email_id": "msg-001"});
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let activity = matcher.match_activity("send_email");
        assert_eq!(
            activity,
            HistoryMatch::Matched { output },
            "match_activity should skip the early signal and replay the activity"
        );

        // The buffered signal must be deliverable via a subsequent match_signal call.
        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during match_activity should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_timer_skips_signal_ingested_before_timer_started() {
        // Same scenario as above but for a timer: signal arrives before the
        // timer is started, so it sits before TimerStarted in history.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer should skip the early signal and replay the timer"
        );

        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during match_timer should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_timer_skips_signal_interleaved_between_started_and_fired() {
        // A signal is recorded while the timer is pending (between TimerStarted
        // and TimerFired in history). match_timer must skip it and still find
        // TimerFired.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer should skip signals between TimerStarted and TimerFired"
        );

        let signal = matcher.match_signal("approved");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            },
            "signal buffered during timer scan should be returned by match_signal"
        );
    }

    // ── Pause/Resume replay transparency (issue #383) ─────────────────────

    #[test]
    fn matcher_timer_skips_pause_resume_between_started_and_fired() {
        // The operator paused while the workflow was waiting on a timer, then
        // resumed; the timer fired on resume. The pause/resume pair is recorded
        // between TimerStarted and TimerFired. match_timer must treat them as
        // no-ops and still find TimerFired — replay determinism is unchanged.
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: Some("incident".into()),
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let timer = matcher.match_timer("cooldown");
        assert_eq!(
            timer,
            HistoryMatch::Matched {
                output: Value::Null
            },
            "match_timer must skip pause/resume events and still find TimerFired"
        );
    }

    #[test]
    fn matcher_pause_resume_are_not_unconsumed_history() {
        // A trailing pause/resume pair (e.g. paused-and-resumed with no pending
        // work) must not be reported as unconsumed history, otherwise the
        // executor would flag spurious non-determinism / never-settle.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: None,
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        matcher.advance(); // skip WorkflowStarted
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "pause/resume events must be transparent to unconsumed-history checks"
        );
    }

    #[test]
    fn matcher_activity_skips_pause_resume_between_scheduled_and_completed() {
        // Pause/resume recorded while an activity was in flight.
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::WorkflowExecutionPaused {
                paused_at: chrono::Utc::now(),
                reason: None,
                actor: "oncall".into(),
            },
            WorkflowEvent::WorkflowExecutionResumed {
                resumed_at: chrono::Utc::now(),
                actor: "oncall".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"charged": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_activity("charge");
        assert_eq!(
            result,
            HistoryMatch::Matched {
                output: serde_json::json!({"charged": true})
            },
            "match_activity must skip pause/resume and find the recorded completion"
        );
    }

    // ── Local activity tests ──────────────────────────────────────────────

    #[test]
    fn matcher_replays_completed_local_activity() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"formatted": "hello"});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 2);
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_local_activity_with_recorded_failures_returns_in_progress() {
        // The replay engine does not know max_attempts, so it always returns
        // LocalActivityInProgress when there is no completion event — even if
        // all retries may be exhausted. The worker checks max_attempts and
        // either returns last_error immediately or runs the next attempt.
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "still failing".into(),
                attempt: 2,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(
            matches!(
                &result,
                HistoryMatch::LocalActivityInProgress {
                    activity_id: rid,
                    failed_attempts: 2,
                    last_error: Some(e),
                } if *rid == id && e == "still failing"
            ),
            "expected LocalActivityInProgress with 2 failed attempts, got {result:?}"
        );
    }

    #[test]
    fn matcher_local_activity_exhausted_event_returns_failed() {
        // LocalActivityExhausted is the authoritative terminal marker. Replay
        // must return Failed regardless of any current retry-policy value.
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityExhausted {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(
            result,
            HistoryMatch::Failed {
                error: "transient".into(),
                attempt: 1,
                error_type: "Error".into(),
                details: None,
            }
        );
    }

    #[test]
    fn matcher_local_activity_skips_intermediate_failures_and_returns_completion() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "still transient".into(),
                attempt: 2,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });
        assert_eq!(matcher.position(), 4);
    }

    #[test]
    fn matcher_local_activity_no_match_at_end_of_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::NoMatch);
    }

    #[test]
    fn matcher_local_activity_detects_divergence_wrong_event_type() {
        let timer_id = TimerId::new("t1");
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 10,
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("format_data"));
            assert!(actual.contains("TimerStarted"));
        }
    }

    #[test]
    fn matcher_local_activity_detects_divergence_wrong_name() {
        let id = ActivityExecId::new();
        let events = vec![WorkflowEvent::LocalActivityScheduled {
            activity_id: id,
            name: "other_activity".into(),
            input: Value::Null,
        }];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        if let HistoryMatch::Diverged {
            expected, actual, ..
        } = result
        {
            assert!(expected.contains("format_data"));
            assert!(actual.contains("other_activity"));
        }
    }

    #[test]
    fn matcher_local_activity_stashes_signals_during_retry_scan() {
        let id = ActivityExecId::new();
        let output = serde_json::json!({"ok": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "transient".into(),
                attempt: 1,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "abort".into(),
                payload: serde_json::json!({"reason": "user"}),
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_local_activity("format_data");
        assert_eq!(result, HistoryMatch::Matched { output });

        // Signal buffered during scan must be deliverable later.
        let signal = matcher.match_signal("abort");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "user"})
            }
        );
    }

    #[test]
    fn matcher_replays_sequential_local_and_regular_activities() {
        let local_id = ActivityExecId::new();
        let regular_id = ActivityExecId::new();
        let local_out = serde_json::json!("formatted");
        let regular_out = serde_json::json!({"sent": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id: local_id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: local_id,
                output: local_out.clone(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: regular_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: regular_id,
                output: regular_out.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let r1 = matcher.match_local_activity("format_data");
        assert_eq!(r1, HistoryMatch::Matched { output: local_out });

        let r2 = matcher.match_activity("send_email");
        assert_eq!(
            r2,
            HistoryMatch::Matched {
                output: regular_out
            }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_activity_skips_signal_interleaved_between_scheduled_and_completed() {
        // A signal arrives while an activity is running (between ActivityScheduled
        // and ActivityCompleted in history). match_activity must skip it and find
        // ActivityCompleted.
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"rows": 100});
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let activity = matcher.match_activity("import_data");
        assert_eq!(
            activity,
            HistoryMatch::Matched { output },
            "match_activity should skip signals interleaved during activity execution"
        );

        let signal = matcher.match_signal("cancel");
        assert_eq!(
            signal,
            HistoryMatch::Matched {
                output: serde_json::json!({"reason": "manual"})
            },
            "signal buffered during activity scan should be returned by match_signal"
        );
    }

    #[test]
    fn matcher_activity_preserves_detached_spawn_interleaved_before_completion() {
        let activity_id = ActivityExecId::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"rows": 100});
        let events = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "import_data".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_activity("import_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
        assert!(!matcher.is_replaying());
    }

    #[test]
    fn matcher_timer_preserves_detached_spawn_interleaved_before_fire() {
        let timer_id = TimerId::new("cooldown");
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_timer("cooldown"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_local_activity_preserves_detached_spawn_interleaved_before_completion() {
        let activity_id = ActivityExecId::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"formatted": true});
        let events = vec![
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_local_activity("format_data"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_signal_wait_preserves_detached_spawn_before_later_signal() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_signal("approved"),
            HistoryMatch::Matched {
                output: serde_json::json!({"ok": true})
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_external_activity_preserves_detached_spawn_before_external_completion() {
        let activity_id = ActivityExecId::new();
        let token = ExternalActivityToken::new();
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"accepted": true});
        let events = vec![
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "ship_order".into(),
                input: Value::Null,
                queue: "fulfillment".into(),
                schedule_to_close_secs: 60,
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ActivityCompletedExternally {
                activity_id,
                token,
                output: output.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_external_activity("ship_order"),
            HistoryMatch::Matched { output }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    #[test]
    fn matcher_external_signal_preserves_detached_spawn_before_delivery() {
        let signal_id = ExternalSignalId::new();
        let target = ExecutionId::new();
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "poke".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name: "monitor".into(),
                input: Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];
        let mut matcher = HistoryMatcher::new(events);

        assert_eq!(
            matcher.match_external_signal(target, "poke"),
            HistoryMatch::Matched {
                output: Value::Null
            }
        );
        assert_eq!(
            matcher
                .match_detached_child_spawn("monitor", &Value::Null, ParentClosePolicy::Abandon,),
            HistoryMatch::DetachedChildSpawned { child_id }
        );
    }

    // ── match_signal_or_timer (issue #476) ────────────────────────────────

    #[test]
    fn signal_or_timer_signal_wins_when_recorded_before_timer_fired() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_signal_win_consumes_stray_timer_fired() {
        // The durable timer fires after the signal already won. The stray
        // TimerFired must be consumed so subsequent matches do not diverge.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert!(
            !matcher.is_replaying(),
            "stray TimerFired must be consumed after the signal wins"
        );
    }

    #[test]
    fn signal_or_timer_timer_wins_when_fired_first() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);
    }

    #[test]
    fn signal_or_timer_timer_win_does_not_consume_late_signal() {
        // The signal arrives after the timer already fired: the timer wins and
        // the late signal stays observable for a subsequent match_signal.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let late = matcher.match_signal("approval");
        assert_eq!(
            late,
            HistoryMatch::Matched {
                output: serde_json::json!({"approved": true})
            },
            "a signal that lost the race must remain observable later"
        );
    }

    #[test]
    fn signal_or_timer_signal_wins_when_received_before_race_started() {
        // The signal was ingested before the race point — no timer was ever
        // started on the corresponding live run.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: serde_json::json!({"approved": false}),
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": false})
            }
        );
    }

    #[test]
    fn signal_or_timer_no_match_on_empty_history() {
        let mut matcher = HistoryMatcher::new(vec![]);
        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert_eq!(result, SignalOrTimerMatch::NoMatch);
    }

    #[test]
    fn signal_or_timer_in_progress_when_neither_resolution_recorded() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::InProgress);
    }

    #[test]
    fn signal_or_timer_diverges_on_unrelated_event() {
        let events = vec![WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: Value::Null,
            queue: "default".into(),
        }];
        let mut matcher = HistoryMatcher::new(events);

        let result =
            matcher.match_signal_or_timer("approval", "__signal_timeout:1:approval", Some(300));
        assert!(matches!(result, SignalOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn signal_or_timer_diverges_on_duration_change() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(600));
        assert!(matches!(result, SignalOrTimerMatch::Diverged { .. }));
    }

    #[test]
    fn signal_or_timer_stashes_non_matching_signals_during_scan() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "other".into(),
                payload: serde_json::json!(1),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let other = matcher.match_signal("other");
        assert_eq!(
            other,
            HistoryMatch::Matched {
                output: serde_json::json!(1)
            },
            "non-matching signals scanned during the race must be stashed"
        );
    }

    #[test]
    fn signal_or_timer_signal_wins_across_interleaved_activity() {
        // tokio::join!(receive_signal_timeout, execute_activity): the sibling
        // activity's Scheduled event is interleaved between TimerStarted and
        // the race resolution. The scan must skip it (tracking it as the next
        // replay cursor) instead of reporting the race as still in progress.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );

        // The cursor must rewind to the interleaved command so the join
        // sibling can match its own activity.
        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_timer_wins_across_interleaved_activity() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_in_progress_rewinds_to_interleaved_command() {
        // The race is unresolved but a concurrent activity already has events
        // in history: InProgress must leave the cursor on the interleaved
        // command so the sibling matcher still finds it.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "audit_log".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("logged"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::InProgress);

        let activity = matcher.match_activity("audit_log");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("logged")
            }
        );
    }

    #[test]
    fn signal_or_timer_scans_past_interleaved_bookkeeping_and_sibling_timer() {
        // Markers, side effects, and a sibling timer from concurrent branches
        // must not hide the race resolution.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let sibling_timer = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::MarkerRecorded {
                name: "fan_out:1".into(),
                details: Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: sibling_timer.clone(),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired {
                timer_id: sibling_timer,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(
            result,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_timer_win_late_signal_is_not_flagged_unconsumed() {
        // Timeout branch with a late approval that the workflow intentionally
        // ignores (the documented auto-reject case): the leftover signal must
        // not be reported as early-completion non-determinism by the strict
        // replay check.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a late signal that lost the race must not flag the completed history"
        );
    }

    #[test]
    fn signal_or_timer_timer_win_late_signal_exemption_survives_stashing() {
        // After the timeout branch, a subsequent matcher scan stashes the late
        // signal into the pending buffer. The exemption must still apply.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "notify".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        // The next command's prepare_match drains the late signal into the
        // pending stash before matching the activity.
        let activity = matcher.match_activity("notify");
        assert_eq!(
            activity,
            HistoryMatch::Matched {
                output: serde_json::json!("sent")
            }
        );

        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "a stashed late race signal must not flag the completed history"
        );
    }

    #[test]
    fn late_race_exemption_is_scoped_to_one_occurrence() {
        // One race loss excuses exactly one unconsumed signal of that name. A
        // second same-name signal whose wait was removed/skipped must still be
        // reported as non-determinism.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "only one approval lost the race — the second unconsumed approval must flag"
        );
    }

    #[test]
    fn late_race_exemption_is_voided_when_loser_is_consumed() {
        // The exemption tracks the exact event that lost the race. If a later
        // wait consumes that signal, the exemption is spent with it: a second
        // same-name unconsumed signal (e.g. from a removed/skipped wait) must
        // still be reported as non-determinism.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"n": 2}),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
        assert_eq!(result, SignalOrTimerMatch::TimerWon);

        // The wait consumes the front-most approval — the one that lost the
        // race — so the exemption travels with it.
        let consumed = matcher.match_signal("approval");
        assert_eq!(
            consumed,
            HistoryMatch::Matched {
                output: serde_json::json!({"n": 1})
            }
        );

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "the second approval is not excused once the race loser was consumed"
        );
    }

    #[test]
    fn unraced_unconsumed_signal_still_flags_history() {
        // The late-race exemption must not weaken the existing removed-wait
        // detection for signals that never lost a race.
        let events = vec![WorkflowEvent::SignalReceived {
            signal_name: "approval".into(),
            payload: serde_json::json!({"approved": true}),
        }];
        let matcher = HistoryMatcher::new(events);

        assert!(
            matcher.has_non_lifecycle_unconsumed(),
            "an unconsumed signal with no race must still be flagged"
        );
    }

    #[test]
    fn signal_or_timer_stashed_signal_after_fire_does_not_win() {
        // A sibling race's scan stashes an "approval" that was recorded AFTER
        // this race's TimerFired. The stash must not override recorded history
        // order: the timer won, and the late approval stays deliverable.
        let r1 = TimerId::new("__signal_timeout:1:other");
        let r2 = TimerId::new("__signal_timeout:2:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: r1.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerStarted {
                timer_id: r2.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: r2.clone(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: r1.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        // Race 1 ("other") scans across the whole history, stashing the
        // non-matching "approval" recorded at index 3.
        let first = matcher.match_signal_or_timer("other", r1.as_str(), Some(300));
        assert_eq!(first, SignalOrTimerMatch::TimerWon);

        // Race 2 ("approval"): its TimerFired (index 2) precedes the stashed
        // approval (index 3) — the timer won by history order.
        let second = matcher.match_signal_or_timer("approval", r2.as_str(), Some(300));
        assert_eq!(second, SignalOrTimerMatch::TimerWon);

        // The late approval remains deliverable to a subsequent wait.
        let late = matcher.match_signal("approval");
        assert_eq!(
            late,
            HistoryMatch::Matched {
                output: serde_json::json!({"approved": true})
            }
        );
    }

    #[test]
    fn signal_or_timer_stashed_signal_before_fire_still_wins() {
        // The mirror case: the stashed approval was recorded BEFORE this
        // race's TimerFired, so the signal branch wins even though a sibling
        // scan moved the event into the pending stash.
        let r1 = TimerId::new("__signal_timeout:1:other");
        let r2 = TimerId::new("__signal_timeout:2:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: r1.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerStarted {
                timer_id: r2.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: r2.clone(),
            },
            WorkflowEvent::TimerFired {
                timer_id: r1.clone(),
            },
        ];
        let mut matcher = HistoryMatcher::new(events);

        let first = matcher.match_signal_or_timer("other", r1.as_str(), Some(300));
        assert_eq!(first, SignalOrTimerMatch::TimerWon);

        let second = matcher.match_signal_or_timer("approval", r2.as_str(), Some(300));
        assert_eq!(
            second,
            SignalOrTimerMatch::SignalWon {
                payload: serde_json::json!({"approved": true})
            }
        );
        assert!(
            !matcher.has_non_lifecycle_unconsumed(),
            "winning signal and both timer events must all be settled"
        );
    }

    #[test]
    fn signal_or_timer_replays_same_branch_when_both_events_exist() {
        // Whichever event was recorded first wins on every replay regardless
        // of wall-clock timing on the replaying worker.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let events = vec![
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            },
        ];

        for _ in 0..3 {
            let mut matcher = HistoryMatcher::new(events.clone());
            let result = matcher.match_signal_or_timer("approval", timer_id.as_str(), Some(300));
            assert_eq!(
                result,
                SignalOrTimerMatch::SignalWon {
                    payload: serde_json::json!({"approved": true})
                }
            );
        }
    }
}
