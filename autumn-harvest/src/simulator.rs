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
use crate::types::{ExecutionId, WorkerId};

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

/// In-memory simulator for executing workflows locally without a database.
///
/// The simulator runs the workflow function iteratively. When the workflow
/// suspends (e.g., requests an activity execution), the simulator intercepts
/// the command, executes a registered mock (or returns `Value::Null` by default),
/// appends the corresponding events to the history, and resumes the workflow.
pub struct WorkflowSimulator {
    handler: WorkflowHandlerFn,
    state: SharedState,
    activity_mocks: HashMap<String, ActivityMockFn>,
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
    /// synchronously with the activity input.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.activity_mocks.insert(name.to_string(), Box::new(mock));
        self
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
                WorkflowOutcome::Failed { error } => {
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

                    let mock_res = self
                        .activity_mocks
                        .get(&name)
                        .map_or(Ok(Value::Null), |mock| mock(activity_input));

                    match mock_res {
                        Ok(out) => {
                            history.push(WorkflowEvent::ActivityCompleted {
                                activity_id,
                                output: out,
                            });
                        }
                        Err(err) => {
                            history.push(WorkflowEvent::ActivityFailed {
                                activity_id,
                                error: err,
                                attempt: 1,
                                error_type: "Error".into(),
                                non_retryable: false,
                                details: None,
                            });
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
}
