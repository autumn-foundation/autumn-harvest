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
use crate::event::WorkflowEvent;
use crate::types::{ActivityExecId, ExecutionId, ExternalActivityToken, UpdateId};

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
        /// String representation of the failure.
        error: String,
        /// Attempt number for the failed action.
        attempt: u32,
    },
    /// History contains a timeout for this command.
    TimedOut {
        /// The type of timeout that occurred.
        timeout_type: TimeoutType,
    },
    /// Cursor is past the end of history — this is a new command.
    NoMatch,
    /// The command does not match what was recorded at this position,
    /// indicating non-determinism in the workflow code.
    Diverged {
        /// What the history matcher expected to find based on recorded events.
        expected: String,
        /// What the workflow actually requested.
        actual: String,
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
    pending_signals: VecDeque<(String, Value)>,
}

impl HistoryMatcher {
    /// Create a new matcher from a list of recorded events.
    #[must_use]
    pub fn new(events: Vec<WorkflowEvent>) -> Self {
        Self {
            events,
            cursor: 0,
            consumed_out_of_order_events: HashSet::new(),
            consumed_signal_events: HashSet::new(),
            pending_signals: VecDeque::new(),
        }
    }

    /// Returns `true` if the event at `index` has already been consumed out-of-order.
    fn is_consumed(&self, index: usize) -> bool {
        self.consumed_out_of_order_events.contains(&index)
            || self.consumed_signal_events.contains(&index)
    }

    fn stash_signal(&mut self, cursor: usize, signal_name: String, payload: Value) {
        self.consumed_signal_events.insert(cursor);
        self.pending_signals.push_back((signal_name, payload));
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
            other => other.type_name().to_string(),
        }
    }

    /// Prepares for matching by advancing past consumed events and draining early signals.
    /// Returns `true` if there are still events to replay.
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
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
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
                WorkflowEvent::ChildWorkflowStarted { .. } => {
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
                // Update events are transparent to the activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                // Any other event type is unexpected mid-activity
                _ => break,
            }
        }

        // We found the Scheduled event but no terminal event — treat as
        // incomplete history (the activity was scheduled but never finished).
        HistoryMatch::NoMatch
    }

    fn scan_local_activity_terminal(
        &mut self,
        activity_id: ActivityExecId,
        mut scan_cursor: usize,
    ) -> HistoryMatch {
        let mut failed_attempts: u32 = 0;
        let mut last_error: Option<String> = None;

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
                    let output = output.clone();
                    self.cursor = scan_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Matched { output };
                }
                // Terminal: all retries exhausted. This event is always
                // authoritative regardless of the current retry policy.
                WorkflowEvent::LocalActivityExhausted {
                    activity_id: id,
                    error,
                    attempt,
                } if *id == activity_id => {
                    let error = error.clone();
                    let attempt = *attempt;
                    self.cursor = scan_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Failed { error, attempt };
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
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // No LocalActivityCompleted or LocalActivityExhausted found. The worker
        // either crashed before the first attempt or between retry attempts.
        // Return InProgress so the worker can resume from the right attempt.
        if failed_attempts > 0 {
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
        self.is_replaying()
    }

    /// Returns `true` if the cursor is still within the recorded history.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        let mut cursor = self.cursor;
        while self.is_consumed(cursor) {
            cursor += 1;
        }
        cursor < self.events.len()
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
        let mut cursor = self.cursor;
        while cursor < self.events.len() {
            if !self.is_consumed(cursor)
                && !self.events[cursor].is_terminal_lifecycle()
                && !Self::is_update_event(&self.events[cursor])
            {
                return true;
            }
            cursor += 1;
        }
        // Signals buffered early (via drain_early_signals) that were never
        // consumed by wait_for_signal represent unconsumed history.
        !self.pending_signals.is_empty()
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
            };
        };
        let activity_id = *activity_id;

        // Verify activity name matches
        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: format!("ActivityScheduled({recorded_name})"),
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
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "ActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("ActivityScheduled({activity_name}, input={input})"),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),
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
            };
        };

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityAwaitingExternal({activity_name})"),
                actual: format!("ActivityAwaitingExternal({recorded_name})"),
            };
        }

        let activity_id = *activity_id;
        let token = *token;

        // Advance past the ActivityAwaitingExternal event.
        self.cursor += 1;
        let mut scan_cursor = self.cursor;

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
                    self.cursor = scan_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return result;
                }
                WorkflowEvent::ActivityFailedExternally {
                    activity_id: id,
                    error,
                    ..
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: 1,
                    };
                    self.cursor = scan_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return result;
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    self.cursor = scan_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return result;
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
                // Update events are transparent to the external activity scan.
                ev if Self::is_update_event(ev) => {
                    scan_cursor += 1;
                }
                _ => break,
            }
        }

        // Awaiting event exists in history but no terminal found yet.
        HistoryMatch::AwaitingExternalCompletion { activity_id, token }
    }

    /// Match a timer command against history.
    ///
    /// Expects `TimerStarted { timer_id }` at cursor, then scans for
    /// `TimerFired` with the same `timer_id`.
    pub fn match_timer(&mut self, timer_id: &str) -> HistoryMatch {
        if !self.prepare_match() {
            return HistoryMatch::NoMatch;
        }

        let WorkflowEvent::TimerStarted {
            timer_id: recorded_id,
            ..
        } = &self.events[self.cursor]
        else {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: Self::actual_event_name(&self.events[self.cursor]),
            };
        };

        if recorded_id.as_str() != timer_id {
            return HistoryMatch::Diverged {
                expected: format!("TimerStarted({timer_id})"),
                actual: format!("TimerStarted({recorded_id})"),
            };
        }

        // Advance past TimerStarted
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_child_start = None;

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
                return self.settle_terminal(scan_cursor, first_interleaved_child_start, result);
            }

            if matches!(
                self.events[scan_cursor],
                WorkflowEvent::ChildWorkflowStarted { .. }
            ) {
                first_interleaved_child_start.get_or_insert(scan_cursor);
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
            .position(|(name, _)| name == signal_name)
            && let Some((_name, payload)) = self.pending_signals.remove(index)
        {
            return HistoryMatch::Matched { output: payload };
        }

        self.advance_to_next_unconsumed_event();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let mut scan_cursor = self.cursor;
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
                    self.cursor = scan_cursor.saturating_add(1);
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
                other => {
                    return HistoryMatch::Diverged {
                        expected: format!("SignalReceived({signal_name})"),
                        actual: Self::actual_event_name(other),
                    };
                }
            }
        }

        HistoryMatch::NoMatch
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
            };
        };

        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("WorkflowContinuedAsNewInput({input})"),
                actual: format!("WorkflowContinuedAsNewInput({recorded_input})"),
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
            };
        };
        let child_id = *child_id;

        if recorded_name != workflow_name {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowStarted({workflow_name})"),
                actual: format!("ChildWorkflowStarted({recorded_name})"),
            };
        }
        if recorded_input != input {
            return HistoryMatch::Diverged {
                expected: format!("ChildWorkflowInput({input})"),
                actual: format!("ChildWorkflowInput({recorded_input})"),
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
                    return HistoryMatch::Failed { error, attempt: 1 };
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
        self.advance_to_next_unconsumed_event();
        let marker_name = format!("side_effect:{side_effect_id}");

        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        match &self.events[self.cursor] {
            WorkflowEvent::MarkerRecorded { name, details } if *name == marker_name => {
                let output = details.clone();
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
                HistoryMatch::Matched { output }
            }
            other => HistoryMatch::Diverged {
                expected: format!("MarkerRecorded({marker_name})"),
                actual: Self::actual_event_name(other),
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
            };
        };
        let activity_id = *activity_id;

        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: format!("LocalActivityScheduled({recorded_name})"),
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
                    };
                }
                if recorded_input != input {
                    return HistoryMatch::Diverged {
                        expected: format!(
                            "LocalActivityScheduled({activity_name}, input={recorded_input})"
                        ),
                        actual: format!("LocalActivityScheduled({activity_name}, input={input})"),
                    };
                }
                Ok(*activity_id)
            }
            other => Err(HistoryMatch::Diverged {
                expected: format!("LocalActivityScheduled({activity_name})"),
                actual: Self::actual_event_name(other),
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

        if !self.is_replaying() {
            return max_version;
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

        if let HistoryMatch::Diverged { expected, actual } = result {
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
        if let HistoryMatch::Diverged { expected, actual } = result {
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
        if let HistoryMatch::Diverged { expected, actual } = result {
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
        if let HistoryMatch::Diverged { expected, actual } = result {
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
}
