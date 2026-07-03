//! Workflow simulator for local, determinism-safe testing.
//!
//! Provides a fast, in-memory execution environment for workflows that doesn't
//! require Postgres or worker pools. This allows testing workflow logic,
//! branching, and state manipulation instantly.

use std::collections::HashMap;

use serde_json::Value;

use crate::context::{SharedState, WorkflowCommand, empty_shared_state};
use crate::event::WorkflowEvent;
use crate::executor::{WorkflowOutcome, run_workflow_with_state};
use crate::info::WorkflowHandlerFn;
use crate::policy::RetryPolicy;
use crate::types::{ActivityExecId, ExecutionId, WorkerId};

/// The final outcome of a simulated workflow execution.
#[derive(Debug, Clone)]
pub struct SimulatorResult {
    /// The return value of the workflow (or an error string).
    pub final_output: Result<Value, String>,
    /// The complete list of events generated during the simulation.
    pub history: Vec<WorkflowEvent>,
}

/// A synchronous mock function for an activity.
pub type ActivityMockFn = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// An attempt-aware synchronous mock function for an activity (issue #541).
///
/// Receives the activity input and the 1-based worker retry attempt number,
/// enabling per-attempt behavior (e.g. fail attempts 1-2, succeed on 3)
/// without needing external mutable state in the test.
pub type ActivityMockFnWithAttempt = Box<dyn Fn(Value, u32) -> Result<Value, String> + Send + Sync>;

/// A registered activity mock: either attempt-oblivious or attempt-aware.
enum ActivityMock {
    Simple(ActivityMockFn),
    WithAttempt(ActivityMockFnWithAttempt),
}

impl ActivityMock {
    fn call(&self, input: Value, attempt: u32) -> Result<Value, String> {
        match self {
            Self::Simple(f) => f(input),
            Self::WithAttempt(f) => f(input, attempt),
        }
    }
}

/// In-memory simulator for executing workflows locally without a database.
///
/// The simulator runs the workflow function iteratively. When the workflow
/// suspends (e.g., requests an activity execution), the simulator intercepts
/// the command, executes a registered mock (or returns `Value::Null` by default),
/// appends the corresponding events to the history, and resumes the workflow.
pub struct WorkflowSimulator {
    handler: WorkflowHandlerFn,
    state: SharedState,
    activity_mocks: HashMap<String, ActivityMock>,
    /// Per-activity-name retry policy explicitly registered on the simulator
    /// (via [`with_retry_policy`](Self::with_retry_policy)). Takes priority
    /// over a policy resolved from [`with_activity_info`](Self::with_activity_info),
    /// but is itself overridden by a `retry_policy_override` the workflow
    /// code supplies at the call site (e.g. `execute_activity_with_opts`).
    retry_policy_overrides: HashMap<String, RetryPolicy>,
    /// Per-activity-name retry policy resolved from a registered
    /// [`ActivityInfo`](crate::info::ActivityInfo)'s `default_retry_policy`.
    activity_info_retry_policies: HashMap<String, RetryPolicy>,
    child_workflow_mocks: HashMap<String, ActivityMockFn>,
    signals_to_send: HashMap<String, std::collections::VecDeque<Value>>,
}

impl WorkflowSimulator {
    /// Create a new simulator for the given workflow handler.
    #[must_use]
    pub fn new(handler: WorkflowHandlerFn) -> Self {
        Self {
            handler,
            state: empty_shared_state(),
            activity_mocks: HashMap::new(),
            retry_policy_overrides: HashMap::new(),
            activity_info_retry_policies: HashMap::new(),
            child_workflow_mocks: HashMap::new(),
            signals_to_send: HashMap::new(),
        }
    }

    /// Provide shared state to the workflow context.
    #[must_use]
    pub fn with_state(mut self, state: SharedState) -> Self {
        self.state = state;
        self
    }

    /// Register a mock implementation for an activity by name.
    ///
    /// When the workflow schedules this activity, the mock is called
    /// synchronously with the activity input. If the activity's resolved
    /// [`RetryPolicy`] allows further attempts and the mock returns `Err`,
    /// the simulator re-invokes the same mock for the next attempt — see
    /// [`mock_activity_with_attempt`](Self::mock_activity_with_attempt) for a
    /// mock that can observe which attempt it is being called for.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.activity_mocks
            .insert(name.to_string(), ActivityMock::Simple(Box::new(mock)));
        self
    }

    /// Register an attempt-aware mock implementation for an activity by name
    /// (issue #541 AC7).
    ///
    /// The mock receives the 1-based worker retry attempt number alongside
    /// the input, so tests can express "fail attempts 1-2, succeed on 3"
    /// directly in the closure body instead of via external mutable state.
    #[must_use]
    pub fn mock_activity_with_attempt<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn(Value, u32) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.activity_mocks
            .insert(name.to_string(), ActivityMock::WithAttempt(Box::new(mock)));
        self
    }

    /// Register the activity's [`ActivityInfo`](crate::info::ActivityInfo) so
    /// the simulator resolves its `default_retry_policy` the same way the
    /// live worker resolves retries from the registered handler (issue #541
    /// AC3). Only `name` and `default_retry_policy` are consulted.
    #[must_use]
    pub fn with_activity_info(mut self, info: &crate::info::ActivityInfo) -> Self {
        if let Some(policy) = info.default_retry_policy.clone() {
            self.activity_info_retry_policies
                .insert(info.name.to_string(), policy);
        }
        self
    }

    /// Explicit per-activity retry policy override (issue #541 AC3).
    ///
    /// Takes priority over a policy resolved from
    /// [`with_activity_info`](Self::with_activity_info), letting a test
    /// override the retry behavior of one activity without needing its full
    /// registered `ActivityInfo`.
    #[must_use]
    pub fn with_retry_policy(mut self, name: &str, policy: RetryPolicy) -> Self {
        self.retry_policy_overrides.insert(name.to_string(), policy);
        self
    }

    /// Resolve the effective retry policy for a `ScheduleActivity` command:
    /// the call-site override (from `execute_activity_with_opts`) wins,
    /// then the explicit per-mock override, then the `ActivityInfo` default.
    /// `None` means no policy is configured at all — the live worker's
    /// `queue::EnqueueParams::new()` default of 3 attempts (flat 1s delay)
    /// applies, not a single attempt; see the `max_attempts` comment at the
    /// `ScheduleActivity` call site.
    fn resolve_retry_policy(
        &self,
        name: &str,
        call_site_override: Option<RetryPolicy>,
    ) -> Option<RetryPolicy> {
        call_site_override
            .or_else(|| self.retry_policy_overrides.get(name).cloned())
            .or_else(|| self.activity_info_retry_policies.get(name).cloned())
    }

    /// Register a mock implementation for a child workflow by name.
    #[must_use]
    pub fn mock_child_workflow<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.child_workflow_mocks
            .insert(name.to_string(), Box::new(mock));
        self
    }

    /// Register a signal to send when requested by `wait_for_signal`.
    #[must_use]
    pub fn send_signal(mut self, name: &str, payload: Value) -> Self {
        self.signals_to_send
            .entry(name.to_string())
            .or_default()
            .push_back(payload);
        self
    }

    /// Run the workflow to completion using the provided input.
    ///
    /// # Panics
    ///
    /// Panics if the workflow deadlocks (e.g., suspends without emitting any
    /// progressable commands like mocked activities, child workflows, or awaited signals).
    #[allow(clippy::too_many_lines)]
    pub async fn run(mut self, input: Value) -> SimulatorResult {
        let exec_id = ExecutionId::new();
        let mut history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        loop {
            let (outcome, _pending, _span) = run_workflow_with_state(
                exec_id,
                history.clone(),
                self.handler,
                input.clone(),
                self.state.clone(),
                None,
            )
            .await;

            match outcome {
                WorkflowOutcome::Completed { output } => {
                    history.push(WorkflowEvent::WorkflowCompleted {
                        output: output.clone(),
                    });
                    return SimulatorResult {
                        final_output: Ok(output),
                        history,
                    };
                }
                WorkflowOutcome::Failed { error, .. } => {
                    history.push(WorkflowEvent::WorkflowFailed {
                        error: error.clone(),
                    });
                    return SimulatorResult {
                        final_output: Err(error),
                        history,
                    };
                }
                WorkflowOutcome::ContinuedAsNew { input: cont_input } => {
                    // Sim-stop: simulate the seal-and-restart by recording the
                    // terminal marker and feeding the new input back into the
                    // top of the loop. The simulator runs in-process, so this
                    // is a tail call rather than a fresh queue task.
                    history.push(WorkflowEvent::WorkflowContinuedAsNew {
                        new_exec_id: ExecutionId::new(),
                        input: cont_input.clone(),
                    });
                    return SimulatorResult {
                        final_output: Ok(cont_input),
                        history,
                    };
                }
                WorkflowOutcome::Suspended { commands } => {
                    let advanced = self.process_simulator_commands(commands, &mut history);
                    assert!(
                        advanced,
                        "Simulator deadlock: workflow suspended but no progressable commands were emitted (e.g. waiting on unmocked signal)."
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn process_simulator_commands(
        &mut self,
        commands: Vec<WorkflowCommand>,
        history: &mut Vec<WorkflowEvent>,
    ) -> bool {
        let mut advanced = false;

        for cmd in commands {
            match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name,
                    input: activity_input,
                    queue,
                    retry_policy_override,
                    ..
                } => {
                    history.push(WorkflowEvent::ActivityScheduled {
                        activity_id,
                        name: name.clone(),
                        input: activity_input.clone(),
                        queue,
                    });
                    history.push(WorkflowEvent::ActivityStarted {
                        activity_id,
                        worker_id: WorkerId::new("sim-worker"),
                    });

                    let retry_policy = self.resolve_retry_policy(&name, retry_policy_override);
                    // Mirrors the live worker's regular (queue-dispatched)
                    // activity default: `queue::EnqueueParams::new()` sets
                    // `max_attempts = 3` and is only lowered/raised when a
                    // retry policy actually resolves (`worker.rs`'s
                    // `effective_retry` gate); with no policy at all,
                    // `next_retry_delay` retries up to that default with a
                    // flat 1s delay instead of giving up after one attempt.
                    let max_attempts = retry_policy.as_ref().map_or(3, |p| p.max_attempts.max(1));
                    let mock = self.activity_mocks.get(&name);

                    let mut attempt: u32 = 1;
                    loop {
                        let mock_res = mock
                            .map_or(Ok(Value::Null), |m| m.call(activity_input.clone(), attempt));

                        match mock_res {
                            Ok(out) => {
                                history.push(WorkflowEvent::ActivityCompleted {
                                    activity_id,
                                    output: out,
                                });
                                break;
                            }
                            Err(raw_error) => {
                                // Single parse recovers both the persisted
                                // `ActivityFailure` and the retry-termination
                                // decision (issue #227), shared with the live
                                // worker via `crate::failure::classify_activity_error`.
                                let (failure, non_retryable_by_policy) =
                                    crate::failure::classify_activity_error(
                                        &raw_error,
                                        retry_policy.as_ref(),
                                    );
                                let terminal = attempt >= max_attempts || non_retryable_by_policy;

                                // Non-terminal attempts get a synthetic
                                // activity_id distinct from the real one:
                                // production never persists an event for a
                                // requeued retry, and `HistoryMatcher::
                                // scan_activity_terminal` returns the *first*
                                // `ActivityFailed` matching `activity_id`, so
                                // reusing the real id would shadow the
                                // eventual terminal event. A distinct id is
                                // silently skipped by that scan (unrelated-
                                // activity event) — purely observational
                                // (issue #541 AC1/AC2), never affecting
                                // control flow. Caveat: a history containing
                                // these events must not be fed into id-keyed
                                // tooling that expects every `ActivityFailed`
                                // to have a matching `ActivityScheduled` (e.g.
                                // `test_generator.rs`'s mock generator, or
                                // `analyzer.rs`'s `ExcessiveRetriesRule`,
                                // which would misreport the activity name for
                                // these events).
                                let event_id = if terminal {
                                    activity_id
                                } else {
                                    ActivityExecId::new()
                                };
                                history.push(WorkflowEvent::ActivityFailed {
                                    activity_id: event_id,
                                    error: failure.message,
                                    attempt,
                                    error_type: failure.error_type,
                                    non_retryable: failure.non_retryable,
                                    details: failure.details,
                                });

                                if terminal {
                                    break;
                                }
                                attempt += 1;
                            }
                        }
                    }
                    advanced = true;
                }
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => {
                    history.push(WorkflowEvent::TimerStarted {
                        timer_id: timer_id.clone(),
                        duration_secs,
                    });
                    history.push(WorkflowEvent::TimerFired { timer_id });
                    advanced = true;
                }
                WorkflowCommand::StartChildWorkflow {
                    child_id,
                    workflow_name,
                    input,
                    ..
                } => {
                    history.push(WorkflowEvent::ChildWorkflowStarted {
                        child_id,
                        workflow_name: workflow_name.clone(),
                        input: input.clone(),
                    });

                    let mock_res = self
                        .child_workflow_mocks
                        .get(&workflow_name)
                        .map_or(Ok(Value::Null), |mock| mock(input));

                    match mock_res {
                        Ok(out) => {
                            history.push(WorkflowEvent::ChildWorkflowCompleted {
                                child_id,
                                output: out,
                            });
                        }
                        Err(err) => {
                            history.push(WorkflowEvent::ChildWorkflowFailed {
                                child_id,
                                error: err,
                            });
                        }
                    }
                    advanced = true;
                }
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    if let Some(payload) = self
                        .signals_to_send
                        .get_mut(&signal_name)
                        .and_then(std::collections::VecDeque::pop_front)
                    {
                        history.push(WorkflowEvent::SignalReceived {
                            signal_name: signal_name.clone(),
                            payload: payload.clone(),
                        });
                        advanced = true;
                    }
                }
                WorkflowCommand::RecordMarker { name, details } => {
                    history.push(WorkflowEvent::MarkerRecorded { name, details });
                    advanced = true;
                }
                WorkflowCommand::RecordSideEffect { kind, name, value } => {
                    history.push(WorkflowEvent::SideEffectRecorded { kind, name, value });
                    advanced = true;
                }
                WorkflowCommand::CancelRaceLosers {
                    activities,
                    children,
                    timers: _,
                } => {
                    // Mirrors WorkflowTestEnv's handling (testing.rs): a still-open
                    // losing branch gets a synthetic terminal so no future iteration
                    // loops on an in-progress match; timers carry no history footprint
                    // in the simulator (no `harvest_timers` table to clean up).
                    for activity_id in activities {
                        history.push(WorkflowEvent::ActivityFailed {
                            activity_id,
                            error: "lost race to a sibling branch".to_string(),
                            attempt: 1,
                            error_type: "Error".to_string(),
                            non_retryable: true,
                            details: None,
                        });
                    }
                    for child_id in children {
                        history.push(WorkflowEvent::ChildWorkflowFailed {
                            child_id,
                            error: "lost race to a sibling branch".to_string(),
                        });
                    }
                    advanced = true;
                }
                _ => {
                    // Commands like WaitForSignal, StartChildWorkflow are not yet fully supported.
                    // Complete and Fail are handled on the next loop iteration.
                }
            }
        }
        advanced
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    fn dummy_workflow_handler(
        ctx: &crate::context::WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            let res1 = ctx
                .execute_activity_raw("step1", input.clone(), "default")
                .await
                .map_err(|e| e.to_string())?;
            let res2 = ctx
                .execute_activity_raw("step2", res1, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({ "final": res2 }))
        })
    }

    #[tokio::test]
    async fn test_simulator_basic_flow() {
        let sim = WorkflowSimulator::new(dummy_workflow_handler)
            .mock_activity("step1", |val| Ok(serde_json::json!({ "s1": val })))
            .mock_activity("step2", |val| Ok(serde_json::json!({ "s2": val })));

        let res = sim.run(serde_json::json!("init")).await;

        let final_output = res
            .final_output
            .expect("workflow should complete successfully");
        assert_eq!(
            final_output,
            serde_json::json!({ "final": { "s2": { "s1": "init" } } })
        );

        assert!(res.history.len() > 3, "history should record the events");
    }

    fn dummy_workflow_with_child_and_signal(
        ctx: &crate::context::WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            let signal_val = ctx
                .wait_for_signal("test_signal")
                .await
                .map_err(|e| e.to_string())?;
            let child_res = ctx
                .spawn_child_workflow_raw("my_child", signal_val)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({ "final": child_res }))
        })
    }

    #[tokio::test]
    async fn test_simulator_child_and_signal() {
        let sim = WorkflowSimulator::new(dummy_workflow_with_child_and_signal)
            .send_signal("test_signal", serde_json::json!("signal_data"))
            .mock_child_workflow("my_child", |val| Ok(serde_json::json!({ "child": val })));

        let res = sim.run(serde_json::json!("init")).await;
        let final_output = res
            .final_output
            .expect("workflow should complete successfully");
        assert_eq!(
            final_output,
            serde_json::json!({ "final": { "child": "signal_data" } })
        );
    }
    fn dummy_workflow_with_multiple_signals(
        ctx: &crate::context::WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            let s1 = ctx
                .wait_for_signal("test_signal")
                .await
                .map_err(|e| e.to_string())?;
            let s2 = ctx
                .wait_for_signal("test_signal")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!([s1, s2]))
        })
    }

    #[tokio::test]
    async fn test_simulator_multiple_signals() {
        let sim = WorkflowSimulator::new(dummy_workflow_with_multiple_signals)
            .send_signal("test_signal", serde_json::json!("sig1"))
            .send_signal("test_signal", serde_json::json!("sig2"));

        let res = sim.run(serde_json::json!("init")).await;
        let final_output = res
            .final_output
            .expect("workflow should complete successfully");
        assert_eq!(final_output, serde_json::json!(["sig1", "sig2"]));
    }

    // -----------------------------------------------------------------
    // Issue #541: retry-policy-faithful simulator (RED phase)
    // -----------------------------------------------------------------

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::failure::{ActivityFailure, IntoActivityErrorString};
    use crate::info::ActivityInfo;
    use crate::policy::RetryPolicy;
    use crate::saga::Saga;

    fn test_activity_info(name: &'static str, retry: Option<RetryPolicy>) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "simulator::tests",
            default_retry_policy: retry,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn single_activity_workflow(
        ctx: &crate::context::WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            let out = ctx
                .execute_activity_raw("flaky", input, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(out)
        })
    }

    /// AC1 + AC2: a mock that fails on attempt 1 but succeeds on attempt 2 is
    /// re-invoked by the simulator; the workflow observes the eventual
    /// success, and the recorded history contains an `ActivityFailed` event
    /// with the correct (non-hardcoded) attempt number before the terminal
    /// `ActivityCompleted`.
    #[tokio::test]
    async fn test_simulator_retries_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = calls.clone();

        let sim = WorkflowSimulator::new(single_activity_workflow)
            .with_retry_policy(
                "flaky",
                RetryPolicy::fixed(3, std::time::Duration::from_millis(1)),
            )
            .mock_activity("flaky", move |_input| {
                let n = calls_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    Err("transient".to_string())
                } else {
                    Ok(serde_json::json!({ "ok": true }))
                }
            });

        let res = sim.run(serde_json::json!("input")).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "mock invoked exactly twice"
        );
        assert_eq!(
            res.final_output,
            Ok(serde_json::json!({ "ok": true })),
            "workflow observes eventual success"
        );

        let failed_idx = res
            .history
            .iter()
            .position(|e| matches!(e, WorkflowEvent::ActivityFailed { attempt: 1, .. }))
            .expect("history must contain an ActivityFailed{attempt: 1} event");
        let completed_idx = res
            .history
            .iter()
            .position(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
            .expect("history must contain an ActivityCompleted event");
        assert!(
            failed_idx < completed_idx,
            "ActivityFailed{{attempt: 1}} must precede ActivityCompleted"
        );
    }

    /// AC3: mock invocation count equals the resolved `RetryPolicy::max_attempts`
    /// and the terminal error surfaces to the workflow body after exhaustion.
    #[tokio::test]
    async fn test_simulator_retry_exhaustion_matches_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = calls.clone();

        let info = test_activity_info(
            "flaky",
            Some(RetryPolicy::fixed(3, std::time::Duration::from_millis(1))),
        );

        let sim = WorkflowSimulator::new(single_activity_workflow)
            .with_activity_info(&info)
            .mock_activity("flaky", move |_input| {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Err("permanently broken".to_string())
            });

        let res = sim.run(serde_json::json!("input")).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "mock invoked exactly max_attempts times"
        );
        let err = res.final_output.expect_err("workflow must observe failure");
        assert!(
            err.contains("permanently broken"),
            "terminal error message must surface to the workflow body: {err}"
        );

        let terminal_attempts: Vec<u32> = res
            .history
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::ActivityFailed { attempt, .. } => Some(*attempt),
                _ => None,
            })
            .collect();
        assert!(
            terminal_attempts.contains(&3),
            "the terminal ActivityFailed must carry attempt 3, got {terminal_attempts:?}"
        );
    }

    /// Regression test (code review, issue #541): when NO retry policy
    /// resolves at all (no `with_activity_info`, no `with_retry_policy`, no
    /// call-site override), the simulator must match the live worker's
    /// regular-activity default of 3 attempts — `queue::EnqueueParams::new()`
    /// defaults `max_attempts` to 3, and `next_retry_delay` retries up to
    /// that default with a flat 1s delay when no policy is configured — not
    /// give up after a single attempt.
    #[tokio::test]
    async fn test_simulator_default_no_policy_matches_production_three_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = calls.clone();

        let sim = WorkflowSimulator::new(single_activity_workflow).mock_activity(
            "flaky",
            move |_input| {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Err("always fails".to_string())
            },
        );

        let res = sim.run(serde_json::json!("input")).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "with no retry policy configured anywhere, the simulator must retry \
             up to the live worker's un-configured default of 3 attempts"
        );
        assert!(res.final_output.is_err());
    }

    /// AC4: a non-retryable typed failure stops the retry loop after attempt 1
    /// even though `max_attempts` is much higher; the mock is never re-invoked.
    #[tokio::test]
    async fn test_simulator_non_retryable_failure_stops_after_one_attempt() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = calls.clone();

        let sim = WorkflowSimulator::new(single_activity_workflow)
            .with_retry_policy(
                "flaky",
                RetryPolicy::fixed(5, std::time::Duration::from_millis(1)),
            )
            .mock_activity("flaky", move |_input| {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Err(
                    ActivityFailure::non_retryable("InvalidInput", "bad request")
                        .into_error_payload(),
                )
            });

        let res = sim.run(serde_json::json!("input")).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-retryable failure must not be retried"
        );
        let err = res.final_output.expect_err("workflow must observe failure");
        assert!(err.contains("bad request"), "message must surface: {err}");
    }

    /// AC7: the mock can observe the current attempt number via the
    /// attempt-aware mock builder.
    #[tokio::test]
    async fn test_simulator_attempt_aware_mock_sees_attempt_number() {
        let seen_attempts = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let seen_clone = seen_attempts.clone();

        let sim = WorkflowSimulator::new(single_activity_workflow)
            .with_retry_policy(
                "flaky",
                RetryPolicy::fixed(3, std::time::Duration::from_millis(1)),
            )
            .mock_activity_with_attempt("flaky", move |_input, attempt| {
                seen_clone.lock().unwrap().push(attempt);
                if attempt < 3 {
                    Err("not yet".to_string())
                } else {
                    Ok(serde_json::json!("done"))
                }
            });

        let res = sim.run(serde_json::json!("input")).await;

        assert_eq!(res.final_output, Ok(serde_json::json!("done")));
        assert_eq!(*seen_attempts.lock().unwrap(), vec![1, 2, 3]);
    }

    /// AC5: a workflow using `Saga` observes retry exhaustion of a later step
    /// and drives LIFO compensation, all without a database.
    #[tokio::test]
    async fn test_simulator_saga_compensates_on_retry_exhaustion() {
        fn saga_workflow(
            ctx: &crate::context::WorkflowContext,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            Box::pin(async move {
                let mut saga = Saga::new(ctx);

                let reserved: Value = saga
                    .step(
                        || async {
                            ctx.execute_activity_raw("reserve", Value::Null, "default")
                                .await
                        },
                        |_reserved| async {
                            ctx.execute_activity_raw("release", Value::Null, "default")
                                .await
                                .map(|_| ())
                        },
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                let charge_result = saga
                    .step(
                        || async {
                            ctx.execute_activity_raw("charge", reserved.clone(), "default")
                                .await
                        },
                        |_| async { Ok::<(), crate::error::HarvestError>(()) },
                    )
                    .await;

                charge_result.map_err(|e| e.to_string())
            })
        }

        let released = Arc::new(AtomicU32::new(0));
        let released_clone = released.clone();
        let charge_calls = Arc::new(AtomicU32::new(0));
        let charge_calls_clone = charge_calls.clone();

        let sim = WorkflowSimulator::new(saga_workflow)
            .with_retry_policy(
                "charge",
                RetryPolicy::fixed(2, std::time::Duration::from_millis(1)),
            )
            .mock_activity("reserve", |_input| Ok(serde_json::json!({ "rsv": "abc" })))
            .mock_activity("release", move |_input| {
                released_clone.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            })
            .mock_activity("charge", move |_input| {
                charge_calls_clone.fetch_add(1, Ordering::SeqCst);
                Err("card_declined".to_string())
            });

        let res = sim.run(serde_json::json!(null)).await;

        assert_eq!(
            charge_calls.load(Ordering::SeqCst),
            2,
            "charge must be retried up to max_attempts before exhausting"
        );
        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "saga compensation (release) must run exactly once after exhaustion"
        );
        assert!(res.final_output.is_err(), "workflow must fail overall");
    }

    /// AC6: retries must not perform any real-time sleep. The executor's
    /// suspension-detection window (`SUSPENSION_TIMEOUT`, `executor.rs`) is a
    /// fixed, pre-existing per-suspension cost unrelated to this issue — the
    /// whole retry loop for one `ScheduleActivity` command resolves inside a
    /// *single* suspension, so that fixed cost is paid exactly once
    /// regardless of `max_attempts`. This test isolates the retry-specific
    /// cost by asserting elapsed time does not scale with `max_attempts`:
    /// a policy configured with a 10-second initial backoff and 50 attempts
    /// must run in essentially the same wall-clock time as one with 2
    /// attempts, proving no attempt actually sleeps for its configured delay.
    #[tokio::test]
    async fn test_simulator_retries_are_logical_only_no_real_sleep() {
        async fn run_with_max_attempts(max_attempts: u32) -> std::time::Duration {
            let sim = WorkflowSimulator::new(single_activity_workflow)
                .with_retry_policy(
                    "flaky",
                    RetryPolicy::exponential(max_attempts, std::time::Duration::from_secs(10)),
                )
                .mock_activity("flaky", |_input| Err("always fails".to_string()));

            let started = std::time::Instant::now();
            let res = sim.run(serde_json::json!("input")).await;
            assert!(res.final_output.is_err());
            started.elapsed()
        }

        let few_attempts = run_with_max_attempts(2).await;
        let many_attempts = run_with_max_attempts(50).await;

        assert!(
            many_attempts.abs_diff(few_attempts) < std::time::Duration::from_millis(100),
            "50 attempts ({many_attempts:?}) took far longer than 2 attempts \
             ({few_attempts:?}); retries must be logical-only, not real-time sleeps"
        );
    }

    /// AC3: an explicit per-mock `with_retry_policy` override takes priority
    /// over a policy resolved from a registered `ActivityInfo`.
    #[tokio::test]
    async fn test_simulator_per_mock_retry_override_beats_activity_info() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = calls.clone();

        let info = test_activity_info(
            "flaky",
            Some(RetryPolicy::fixed(5, std::time::Duration::from_millis(1))),
        );

        let sim = WorkflowSimulator::new(single_activity_workflow)
            .with_activity_info(&info)
            .with_retry_policy(
                "flaky",
                RetryPolicy::fixed(2, std::time::Duration::from_millis(1)),
            )
            .mock_activity("flaky", move |_input| {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Err("nope".to_string())
            });

        let _ = sim.run(serde_json::json!("input")).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "explicit override (max_attempts=2) must win over ActivityInfo default (5)"
        );
    }
}
