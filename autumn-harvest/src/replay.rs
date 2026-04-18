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

/// Result of matching a workflow command against the event history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryMatch {
    /// History contains a completed result for this command.
    Matched { output: Value },
    /// History contains a failure for this command.
    Failed { error: String, attempt: u32 },
    /// History contains a timeout for this command.
    TimedOut { timeout_type: TimeoutType },
    /// Cursor is past the end of history — this is a new command.
    NoMatch,
    /// The command does not match what was recorded at this position,
    /// indicating non-determinism in the workflow code.
    Diverged { expected: String, actual: String },
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
    consumed_child_terminal_events: HashSet<usize>,
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
            consumed_child_terminal_events: HashSet::new(),
            consumed_signal_events: HashSet::new(),
            pending_signals: VecDeque::new(),
        }
    }

    /// Returns `true` if the cursor is still within the recorded history.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        let mut cursor = self.cursor;
        while self.consumed_child_terminal_events.contains(&cursor)
            || self.consumed_signal_events.contains(&cursor)
        {
            cursor += 1;
        }
        cursor < self.events.len()
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
        while (self.consumed_child_terminal_events.contains(&self.cursor)
            || self.consumed_signal_events.contains(&self.cursor))
            && self.cursor < self.events.len()
        {
            self.cursor += 1;
        }
    }

    /// Drain any `SignalReceived` events at the current cursor into
    /// `pending_signals`.
    ///
    /// Signals can be ingested into history at any point (when the worker
    /// picks up a task), so they may appear before an `ActivityScheduled`,
    /// `TimerStarted`, or `ChildWorkflowStarted` event even though the
    /// workflow code only calls `wait_for_signal` later.  This helper
    /// buffers those early signals so the normal matcher methods do not
    /// mis-report them as non-determinism.
    fn drain_early_signals(&mut self) {
        while self.cursor < self.events.len() {
            if let WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } = &self.events[self.cursor]
            {
                self.consumed_signal_events.insert(self.cursor);
                self.pending_signals
                    .push_back((signal_name.clone(), payload.clone()));
                self.cursor += 1;
                self.advance_to_next_unconsumed_event();
            } else {
                break;
            }
        }
    }

    /// Advance the cursor after a terminal event, respecting any interleaved
    /// child workflow start that needs to remain at the cursor for later replay.
    ///
    /// If `first_interleaved_child_start` is set, the terminal event is marked
    /// consumed and the cursor is rewound to the child start position so
    /// `match_child_workflow` can pick it up. Otherwise the cursor advances
    /// past the terminal event normally.
    fn settle_terminal(
        &mut self,
        terminal_cursor: usize,
        first_interleaved_child_start: Option<usize>,
        result: HistoryMatch,
    ) -> HistoryMatch {
        if let Some(child_start_cursor) = first_interleaved_child_start {
            self.consumed_child_terminal_events.insert(terminal_cursor);
            self.cursor = child_start_cursor;
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
    pub fn match_activity(&mut self, activity_name: &str) -> HistoryMatch {
        self.advance_to_next_unconsumed_event();
        // Signals may have been ingested before the activity was scheduled;
        // buffer them so they can be consumed by a later wait_for_signal call.
        self.drain_early_signals();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        // Expect ActivityScheduled at cursor
        let scheduled_event = &self.events[self.cursor];
        let (activity_id, recorded_name) = match scheduled_event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } => (*activity_id, name.as_str()),
            other => {
                return HistoryMatch::Diverged {
                    expected: format!("ActivityScheduled({activity_name})"),
                    actual: other.type_name().to_string(),
                };
            }
        };

        // Verify activity name matches
        if recorded_name != activity_name {
            return HistoryMatch::Diverged {
                expected: format!("ActivityScheduled({activity_name})"),
                actual: format!("ActivityScheduled({recorded_name})"),
            };
        }

        // Advance past the Scheduled event
        self.cursor += 1;
        let mut scan_cursor = self.cursor;
        let mut first_interleaved_child_start = None;

        // Scan forward for Completed or Failed with matching activity_id,
        // skipping Started, Heartbeat, and other intermediate events.
        while scan_cursor < self.events.len() {
            if self.consumed_child_terminal_events.contains(&scan_cursor)
                || self.consumed_signal_events.contains(&scan_cursor)
            {
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
                    return self.settle_terminal(
                        scan_cursor,
                        first_interleaved_child_start,
                        result,
                    );
                }
                WorkflowEvent::ActivityFailed {
                    activity_id: id,
                    error,
                    attempt,
                } if *id == activity_id => {
                    let result = HistoryMatch::Failed {
                        error: error.clone(),
                        attempt: *attempt,
                    };
                    return self.settle_terminal(
                        scan_cursor,
                        first_interleaved_child_start,
                        result,
                    );
                }
                WorkflowEvent::ActivityTimedOut {
                    activity_id: id,
                    timeout_type,
                } if *id == activity_id => {
                    let result = HistoryMatch::TimedOut {
                        timeout_type: timeout_type.clone(),
                    };
                    return self.settle_terminal(
                        scan_cursor,
                        first_interleaved_child_start,
                        result,
                    );
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
                // Child workflows can run concurrently with activities.
                // Preserve replay by scanning past interleaved child starts.
                WorkflowEvent::ChildWorkflowStarted { .. } => {
                    first_interleaved_child_start.get_or_insert(scan_cursor);
                    scan_cursor += 1;
                }
                // Signals can arrive at any time; stash them for later
                // wait_for_signal calls and continue scanning.
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    self.consumed_signal_events.insert(scan_cursor);
                    self.pending_signals
                        .push_back((signal_name.clone(), payload.clone()));
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

    /// Match a timer command against history.
    ///
    /// Expects `TimerStarted { timer_id }` at cursor, then scans for
    /// `TimerFired` with the same `timer_id`.
    pub fn match_timer(&mut self, timer_id: &str) -> HistoryMatch {
        self.advance_to_next_unconsumed_event();
        // Signals may have been ingested before the timer was started;
        // buffer them so they can be consumed by a later wait_for_signal call.
        self.drain_early_signals();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let started_event = &self.events[self.cursor];
        let recorded_id = match started_event {
            WorkflowEvent::TimerStarted { timer_id: id, .. } => id.as_str(),
            other => {
                return HistoryMatch::Diverged {
                    expected: format!("TimerStarted({timer_id})"),
                    actual: other.type_name().to_string(),
                };
            }
        };

        if recorded_id != timer_id {
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
            if self.consumed_child_terminal_events.contains(&scan_cursor)
                || self.consumed_signal_events.contains(&scan_cursor)
            {
                scan_cursor += 1;
                continue;
            }

            if let WorkflowEvent::TimerFired { timer_id: id } = &self.events[scan_cursor] {
                if id.as_str() == timer_id {
                    let result = HistoryMatch::Matched {
                        output: Value::Null,
                    };
                    return self.settle_terminal(
                        scan_cursor,
                        first_interleaved_child_start,
                        result,
                    );
                }
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
                self.consumed_signal_events.insert(scan_cursor);
                self.pending_signals
                    .push_back((signal_name.clone(), payload.clone()));
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
        {
            if let Some((_name, payload)) = self.pending_signals.remove(index) {
                return HistoryMatch::Matched { output: payload };
            }
        }

        self.advance_to_next_unconsumed_event();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let mut scan_cursor = self.cursor;
        while scan_cursor < self.events.len() {
            if self.consumed_child_terminal_events.contains(&scan_cursor)
                || self.consumed_signal_events.contains(&scan_cursor)
            {
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
                    self.consumed_signal_events.insert(scan_cursor);
                    self.pending_signals
                        .push_back((recorded_name.clone(), payload.clone()));
                    scan_cursor += 1;
                }
                other => {
                    return HistoryMatch::Diverged {
                        expected: format!("SignalReceived({signal_name})"),
                        actual: other.type_name().to_string(),
                    };
                }
            }
        }

        HistoryMatch::NoMatch
    }

    /// Match a child workflow command against history.
    ///
    /// Expects `ChildWorkflowStarted { workflow_name }` at cursor, then scans for
    /// a terminal `ChildWorkflowCompleted` or `ChildWorkflowFailed` with the same
    /// `child_id`.
    pub fn match_child_workflow(&mut self, workflow_name: &str, input: &Value) -> HistoryMatch {
        self.advance_to_next_unconsumed_event();
        // Signals may have been ingested before the child workflow was started;
        // buffer them so they can be consumed by a later wait_for_signal call.
        self.drain_early_signals();
        if !self.is_replaying() {
            return HistoryMatch::NoMatch;
        }

        let start_cursor = self.cursor;
        let started_event = &self.events[self.cursor];
        let (child_id, recorded_name, recorded_input) = match started_event {
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                input,
            } => (*child_id, workflow_name.as_str(), input),
            other => {
                return HistoryMatch::Diverged {
                    expected: format!("ChildWorkflowStarted({workflow_name})"),
                    actual: other.type_name().to_string(),
                };
            }
        };

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
                    self.consumed_child_terminal_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Matched { output };
                }
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: id,
                    error,
                } if *id == child_id => {
                    let error = error.clone();
                    self.consumed_child_terminal_events.insert(scan_cursor);
                    self.cursor = start_cursor + 1;
                    self.advance_to_next_unconsumed_event();
                    return HistoryMatch::Failed { error, attempt: 1 };
                }
                _ => scan_cursor += 1,
            }
        }

        self.cursor = start_cursor;
        HistoryMatch::Diverged {
            expected: format!("ChildWorkflowTerminal({workflow_name})"),
            actual: "EndOfHistory".to_string(),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TimeoutType;
    use crate::types::{ActivityExecId, TimerId, WorkerId};
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
    fn matcher_child_workflow_without_terminal_diverges_and_preserves_cursor() {
        let child_id = crate::types::ExecutionId::new();
        let events = vec![WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".into(),
            input: Value::Null,
        }];

        let mut matcher = HistoryMatcher::new(events);
        let result = matcher.match_child_workflow("process_order", &Value::Null);
        assert!(matches!(result, HistoryMatch::Diverged { .. }));
        assert_eq!(
            matcher.position(),
            0,
            "cursor must not consume started event"
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
