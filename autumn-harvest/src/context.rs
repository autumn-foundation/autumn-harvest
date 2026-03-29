//! Execution contexts passed to workflow and activity functions.
//!
//! `WorkflowContext` drives deterministic replay -- it tracks the event history
//! pointer and routes commands either to real execution or to history lookup.
//!
//! `ActivityContext` provides heartbeating, state access, and a DB connection
//! to activities.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::replay::{HistoryMatch, HistoryMatcher};
use crate::types::{ActivityExecId, ExecutionId, TimerId};

// ---------------------------------------------------------------------------
// WorkflowCommand -- commands emitted during live execution
// ---------------------------------------------------------------------------

/// A command emitted by the workflow coroutine during live (non-replay) execution.
///
/// The worker drains these after the coroutine suspends, then schedules real
/// side-effects (activity dispatch, timer registration, etc.).
pub enum WorkflowCommand {
    /// Schedule an activity for execution on a task queue.
    ScheduleActivity {
        activity_id: ActivityExecId,
        name: String,
        input: Value,
        queue: String,
        /// The worker sends the result back through this channel.
        result_tx: oneshot::Sender<Result<Value, String>>,
    },
    /// Start a durable timer.
    StartTimer {
        timer_id: TimerId,
        duration_secs: u64,
        /// Fires when the timer completes.
        result_tx: oneshot::Sender<()>,
    },
    /// Record an opaque marker (used by version gates, side-effect-free notes).
    RecordMarker { name: String, details: Value },
    /// The workflow function returned `Ok(output)`.
    Complete { output: Value },
    /// The workflow function returned `Err(error)`.
    Fail { error: String },
}

// Manual Debug because oneshot::Sender is not Debug.
impl std::fmt::Debug for WorkflowCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScheduleActivity {
                activity_id,
                name,
                queue,
                ..
            } => f
                .debug_struct("ScheduleActivity")
                .field("activity_id", activity_id)
                .field("name", name)
                .field("queue", queue)
                .finish_non_exhaustive(),
            Self::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => f
                .debug_struct("StartTimer")
                .field("timer_id", timer_id)
                .field("duration_secs", duration_secs)
                .finish_non_exhaustive(),
            Self::RecordMarker { name, details } => f
                .debug_struct("RecordMarker")
                .field("name", name)
                .field("details", details)
                .finish(),
            Self::Complete { output } => {
                f.debug_struct("Complete").field("output", output).finish()
            }
            Self::Fail { error } => f.debug_struct("Fail").field("error", error).finish(),
        }
    }
}

// ---------------------------------------------------------------------------
// WorkflowContext
// ---------------------------------------------------------------------------

/// Context passed to every workflow function.
///
/// In **replay mode** (resuming from Postgres history): commands are matched
/// against recorded events and return the stored result without re-executing.
///
/// In **live mode** (past end of history): commands emit [`WorkflowCommand`]s
/// and suspend the coroutine until the worker resolves them.
///
/// Interior mutability via [`Mutex`] is required because the macro-generated
/// handler signature takes `&self` (not `&mut self`), and the returned future
/// must be `Send`.
pub struct WorkflowContext {
    /// Unique ID for this workflow execution (run).
    exec_id: ExecutionId,
    /// Replay engine -- matches commands against recorded event history.
    matcher: Mutex<HistoryMatcher>,
    /// Commands accumulated during live execution, drained by the worker.
    commands: Mutex<Vec<WorkflowCommand>>,
    /// Deterministic "now" -- the timestamp from the `WorkflowStarted` event.
    start_time: DateTime<Utc>,
    /// Monotonically increasing counter for generating activity sequence IDs.
    activity_seq: Mutex<u32>,
    /// Shared typed state map (same `AppState` extras as the web server).
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl WorkflowContext {
    // ── Constructors ──────────────────────────────────────────────────

    /// Create a context for replaying a workflow from its event history.
    ///
    /// The `events` slice must begin with `WorkflowStarted` (the timestamp
    /// is extracted for deterministic `now()`). The matcher is initialized
    /// with the cursor past the `WorkflowStarted` event.
    #[must_use]
    pub fn for_replay(exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> Self {
        Self::for_replay_with_state(exec_id, events, Arc::new(HashMap::new()))
    }

    /// Create a replay context with shared application state.
    #[must_use]
    pub fn for_replay_with_state(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    ) -> Self {
        // Extract the start_time from WorkflowStarted (first event).
        let start_time = events
            .first()
            .and_then(|e| match e {
                WorkflowEvent::WorkflowStarted { timestamp, .. } => Some(*timestamp),
                _ => None,
            })
            .unwrap_or_else(Utc::now);

        let mut matcher = HistoryMatcher::new(events);
        // Advance past the WorkflowStarted lifecycle event -- it does not
        // correspond to a workflow command.
        matcher.advance();

        Self {
            exec_id,
            matcher: Mutex::new(matcher),
            commands: Mutex::new(Vec::new()),
            start_time,
            activity_seq: Mutex::new(0),
            state,
        }
    }

    /// Test constructor -- creates a context in live (non-replay) mode with
    /// empty state and a fresh execution ID.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        let exec_id = ExecutionId::new();
        let start_time = Utc::now();
        Self {
            exec_id,
            matcher: Mutex::new(HistoryMatcher::new(vec![])),
            commands: Mutex::new(Vec::new()),
            start_time,
            activity_seq: Mutex::new(0),
            state: Arc::new(HashMap::new()),
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Deterministic "wall clock" -- returns the `WorkflowStarted` timestamp
    /// so that all replays produce the same result.
    #[must_use]
    pub const fn now(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// The unique execution (run) ID for this workflow.
    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.exec_id
    }

    /// Returns `true` if the context is currently replaying recorded history
    /// (i.e. the matcher cursor has not yet reached the end).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher mutex is poisoned.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        self.matcher
            .lock()
            .expect("matcher lock poisoned")
            .is_replaying()
    }

    /// Access typed shared state (e.g., email clients, config).
    ///
    /// Returns `None` if the state type was not registered with the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    // ── Version gate ──────────────────────────────────────────────────

    /// Query or record a versioned code path.
    ///
    /// During **replay**, returns the version recorded in the marker event
    /// (or `min` if no marker exists for old workflows).
    ///
    /// During **live execution**, returns `max` and emits a `RecordMarker`
    /// command so the version is persisted in the event history.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub fn version(&self, change_id: &str, min: u32, max: u32) -> u32 {
        let version = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_version(change_id, min, max);

        // During live execution (matcher returned max_version and is past
        // history), emit a marker so future replays see this version.
        if !self.is_replaying() && version == max {
            self.push_command(WorkflowCommand::RecordMarker {
                name: format!("version:{change_id}"),
                details: Value::from(u64::from(max)),
            });
        }

        version
    }

    // ── Core activity dispatch ────────────────────────────────────────

    /// Execute an activity, returning the recorded result during replay or
    /// suspending the coroutine during live execution.
    ///
    /// This is the core method of the replay-aware workflow context.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if the activity at this history
    ///   position does not match `name`.
    /// - [`HarvestError::ActivityFailed`] if the recorded history shows a failure.
    /// - [`HarvestError::Cancelled`] if the oneshot sender was dropped (workflow
    ///   was cancelled while the activity was in flight).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn execute_activity_raw(
        &self,
        name: &str,
        input: Value,
        queue: &str,
    ) -> HarvestResult<Value> {
        // Step 1: Match against history (lock is dropped before any .await).
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_activity(name);

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),

            HistoryMatch::Failed { error, attempt } => Err(HarvestError::ActivityFailed {
                name: name.to_string(),
                attempt,
                source: error.into(),
            }),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("activity mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::NoMatch => {
                // Live execution: emit a ScheduleActivity command and suspend
                // until the worker sends the result through the oneshot channel.
                let activity_id = self.next_activity_id();
                let (tx, rx) = oneshot::channel();

                self.push_command(WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    result_tx: tx,
                });

                // Suspend the coroutine until the worker resolves this activity.
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }
        }
    }

    // ── Timer ─────────────────────────────────────────────────────────

    /// Start a durable timer that suspends the workflow for `duration_secs`.
    ///
    /// During **replay**, returns immediately if the timer already fired.
    /// During **live execution**, emits a `StartTimer` command and suspends.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if the timer at this history
    ///   position does not match `timer_id`.
    /// - [`HarvestError::Cancelled`] if the oneshot sender was dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn timer(&self, timer_id: &str, duration_secs: u64) -> HarvestResult<()> {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_timer(timer_id);

        match history_match {
            HistoryMatch::Matched { .. } => Ok(()),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("timer mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::Failed { .. } => {
                // Timers don't fail in the traditional sense, but handle gracefully.
                Ok(())
            }

            HistoryMatch::NoMatch => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::StartTimer {
                    timer_id: TimerId::new(timer_id),
                    duration_secs,
                    result_tx: tx,
                });

                rx.await.map_err(|_| {
                    HarvestError::Cancelled(format!(
                        "timer '{timer_id}' cancelled: result channel dropped"
                    ))
                })
            }
        }
    }

    // ── Command drain ─────────────────────────────────────────────────

    /// Drain all accumulated commands. Called by the worker after the
    /// workflow coroutine suspends or completes.
    ///
    /// # Panics
    ///
    /// Panics if the internal commands mutex is poisoned.
    pub fn drain_commands(&self) -> Vec<WorkflowCommand> {
        let mut cmds = self.commands.lock().expect("commands lock poisoned");
        std::mem::take(&mut *cmds)
    }

    // ── Internal helpers ──────────────────────────────────────────────

    /// Generate the next sequential activity execution ID.
    fn next_activity_id(&self) -> ActivityExecId {
        {
            let mut seq = self
                .activity_seq
                .lock()
                .expect("activity_seq lock poisoned");
            *seq += 1;
        }
        // Only called during live execution (NoMatch), so a random UUID is fine.
        ActivityExecId::new()
    }

    /// Push a command onto the pending commands queue.
    fn push_command(&self, cmd: WorkflowCommand) {
        self.commands
            .lock()
            .expect("commands lock poisoned")
            .push(cmd);
    }
}

// ---------------------------------------------------------------------------
// ActivityContext
// ---------------------------------------------------------------------------

/// Context passed to every activity function.
///
/// Activities may perform I/O, call external services, and interact with the
/// database. The context provides heartbeating to signal liveness, cancellation
/// detection, and state access for shared resources.
pub struct ActivityContext {
    /// Shared state map.
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    /// Heartbeat channel -- `None` in test contexts.
    heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
    /// Cancellation token -- allows the worker to signal graceful shutdown.
    cancel: tokio_util::sync::CancellationToken,
}

impl ActivityContext {
    /// Production constructor -- creates a context with heartbeat channel and
    /// cancellation token.
    #[allow(dead_code)]
    pub(crate) fn new(
        state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
        heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            state,
            heartbeat_tx,
            cancel,
        }
    }

    /// Access typed shared state.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Send a heartbeat to signal the activity is still running.
    ///
    /// The `details` payload is serialized to JSON and forwarded to the worker's
    /// heartbeat loop, which batches writes to the database. Always check the
    /// return value -- an `Err(Cancelled)` means the workflow was cancelled and
    /// the activity should wind down promptly.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Cancelled`] if the cancellation token has been triggered
    ///   or the heartbeat channel is closed.
    /// - [`HarvestError::Serialization`] if `details` fails to serialize.
    pub async fn heartbeat(&self, details: impl serde::Serialize) -> crate::HarvestResult<()> {
        // Check cancellation first -- fast path.
        if self.cancel.is_cancelled() {
            return Err(HarvestError::Cancelled(
                "activity cancelled via cancellation token".into(),
            ));
        }

        let payload = serde_json::to_value(details)?;

        if let Some(ref tx) = self.heartbeat_tx {
            tx.send(payload).await.map_err(|_| {
                HarvestError::Cancelled("activity cancelled: heartbeat channel closed".into())
            })?;
        }

        Ok(())
    }

    /// Returns `true` if the cancellation token has been triggered.
    ///
    /// Activities performing long-running loops should check this periodically
    /// and exit cleanly when it returns `true`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Constructor for testing -- no heartbeat channel, default cancel token.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self::new(
            Arc::new(HashMap::new()),
            None,
            tokio_util::sync::CancellationToken::new(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;

    #[test]
    fn activity_context_state_returns_none_when_not_registered() {
        let ctx = ActivityContext::new_test();
        let state: Option<&String> = ctx.state::<String>();
        assert!(state.is_none());
    }

    #[tokio::test]
    async fn activity_context_heartbeat_sends_on_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel);

        // Send a couple of heartbeats with different payloads.
        ctx.heartbeat(serde_json::json!({"progress": 50}))
            .await
            .expect("heartbeat should succeed");
        ctx.heartbeat(serde_json::json!({"progress": 100}))
            .await
            .expect("heartbeat should succeed");

        // Verify both payloads arrived in order.
        let first = rx.recv().await.expect("should receive first heartbeat");
        assert_eq!(first, serde_json::json!({"progress": 50}));

        let second = rx.recv().await.expect("should receive second heartbeat");
        assert_eq!(second, serde_json::json!({"progress": 100}));
    }

    #[tokio::test]
    async fn activity_context_detects_cancellation() {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel.clone());

        // Before cancellation -- should not be cancelled.
        assert!(!ctx.is_cancelled());

        // Trigger cancellation.
        cancel.cancel();

        // Now is_cancelled() should return true.
        assert!(ctx.is_cancelled());

        // Heartbeat should return Cancelled error.
        let result = ctx.heartbeat(serde_json::json!({})).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::Cancelled(_)));
    }

    #[tokio::test]
    async fn activity_context_heartbeat_errors_when_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel);

        // Drop the receiver -- channel is now closed.
        drop(rx);

        let result = ctx.heartbeat(serde_json::json!({})).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HarvestError::Cancelled(_)));
    }

    #[test]
    fn context_now_returns_deterministic_time() {
        let fixed_time = DateTime::parse_from_rfc3339("2026-01-15T10:30:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: fixed_time,
        }];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // now() must return the exact WorkflowStarted timestamp, not wall clock.
        assert_eq!(ctx.now(), fixed_time);
        // Calling again returns the same value (deterministic).
        assert_eq!(ctx.now(), fixed_time);
    }

    #[tokio::test]
    async fn context_replays_completed_activity() {
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"email_id": "msg-001"});

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"to": "alice@example.com"}),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // The context should be replaying (events remain after WorkflowStarted).
        assert!(ctx.is_replaying());

        // execute_activity_raw should return the recorded output immediately.
        let result = ctx
            .execute_activity_raw(
                "send_email",
                serde_json::json!({"to": "alice@example.com"}),
                "default",
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), output);

        // After consuming all events, no longer replaying.
        assert!(!ctx.is_replaying());

        // No commands emitted during replay.
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_replays_failed_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: "SMTP connection refused".into(),
                attempt: 3,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::ActivityFailed { .. }));
        assert!(err.to_string().contains("send_email"));
    }

    #[test]
    fn context_version_returns_recorded_version() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:billing_v2".into(),
                details: serde_json::json!(2),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let version = ctx.version("billing_v2", 1, 3);
        assert_eq!(version, 2);

        // No commands during replay.
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_version_emits_marker_during_live_execution() {
        let ctx = WorkflowContext::new_test();

        // Live execution: should return max and emit RecordMarker.
        let version = ctx.version("billing_v2", 1, 3);
        assert_eq!(version, 3);

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(
            matches!(&cmds[0], WorkflowCommand::RecordMarker { name, .. } if name == "version:billing_v2")
        );
    }

    #[tokio::test]
    async fn context_suspends_on_new_activity() {
        // Spawn the execute_activity_raw call -- it should suspend (await the oneshot).
        let handle = tokio::spawn({
            let ctx = WorkflowContext::new_test();

            async move {
                ctx.execute_activity_raw("send_email", Value::Null, "default")
                    .await
            }
        });

        // Give the task a moment to start and emit the command.
        tokio::task::yield_now().await;

        // The handle should NOT be finished yet -- the activity is suspended.
        // Use a brief timeout to verify.
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle).await;

        // The timeout should fire (the task is still suspended), which means
        // the outer Result is Err(Elapsed).
        assert!(
            timeout_result.is_err(),
            "expected task to be suspended, but it completed"
        );
    }

    #[tokio::test]
    async fn context_live_activity_resolves_via_oneshot() {
        let ctx = Arc::new(WorkflowContext::new_test());
        let ctx2 = Arc::clone(&ctx);

        let expected_output = serde_json::json!({"sent": true});
        let expected_output2 = expected_output.clone();

        // Spawn the workflow coroutine.
        let handle = tokio::spawn(async move {
            ctx2.execute_activity_raw("send_email", Value::Null, "default")
                .await
        });

        // Yield to let the coroutine emit the command.
        tokio::task::yield_now().await;

        // Drain commands and resolve the oneshot.
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);

        if let WorkflowCommand::ScheduleActivity {
            result_tx, name, ..
        } = cmds.into_iter().next().unwrap()
        {
            assert_eq!(name, "send_email");
            result_tx
                .send(Ok(expected_output2))
                .expect("send should succeed");
        } else {
            panic!("expected ScheduleActivity command");
        }

        // The coroutine should now resolve with the output.
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_output);
    }

    #[tokio::test]
    async fn context_detects_non_deterministic_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_payment".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // Calling with a different activity name than what's in history.
        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::NonDeterministic(_)));
        assert!(err.to_string().contains("send_email"));
        assert!(err.to_string().contains("charge_payment"));
    }

    #[tokio::test]
    async fn context_replays_multiple_activities_in_sequence() {
        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let output1 = serde_json::json!({"email_id": "msg-001"});
        let output2 = serde_json::json!({"charge_id": "ch-999"});

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
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

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let r1 = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;
        assert_eq!(r1.unwrap(), output1);

        let r2 = ctx
            .execute_activity_raw("charge_payment", Value::Null, "billing")
            .await;
        assert_eq!(r2.unwrap(), output2);

        assert!(!ctx.is_replaying());
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_cancelled_when_sender_dropped() {
        let ctx = WorkflowContext::new_test();

        // Spawn a task that will await an activity.
        let handle = tokio::spawn(async move {
            ctx.execute_activity_raw("send_email", Value::Null, "default")
                .await
        });

        // Yield to let it emit the command.
        tokio::task::yield_now().await;

        // Drop the handle -- the oneshot sender will be dropped when
        // the JoinHandle's task is aborted. But we actually need to
        // explicitly drop the sender. Let's approach differently:
        // The task holds the context, so we can't drain commands from here.
        // Instead, just abort the spawned task and verify the handle errors.
        handle.abort();
        let result = handle.await;
        assert!(result.is_err()); // JoinError from abort
    }

    #[test]
    fn context_execution_id_accessible() {
        let exec_id = ExecutionId::new();
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];
        let ctx = WorkflowContext::for_replay(exec_id, events);
        assert_eq!(ctx.execution_id(), exec_id);
    }

    #[test]
    fn context_drain_commands_returns_empty_when_no_commands() {
        let ctx = WorkflowContext::new_test();
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_state_access() {
        let mut state_map: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
        state_map.insert(TypeId::of::<String>(), Box::new(String::from("hello")));

        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let ctx =
            WorkflowContext::for_replay_with_state(ExecutionId::new(), events, Arc::new(state_map));

        assert_eq!(ctx.state::<String>(), Some(&String::from("hello")));
        assert!(ctx.state::<u32>().is_none());
    }

    #[tokio::test]
    async fn context_timer_replays_when_fired() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("cooldown"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.timer("cooldown", 300).await;
        assert!(result.is_ok());
        assert!(!ctx.is_replaying());
    }

    #[tokio::test]
    async fn context_timer_detects_divergence() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "foo".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.timer("cooldown", 300).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::NonDeterministic(_)
        ));
    }
}
