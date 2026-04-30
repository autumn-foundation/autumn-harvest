import re

with open('autumn-harvest/src/simulator.rs', 'r') as f:
    original = f.read()

def replace_once(s, search, replacement):
    idx = s.find(search)
    if idx == -1:
        raise Exception(f"Could not find: {search[:50]}...")
    return s[:idx] + replacement + s[idx+len(search):]

# 1. Struct
s1 = """pub struct WorkflowSimulator {
    handler: WorkflowHandlerFn,
    state: SharedState,
    activity_mocks: HashMap<String, ActivityMockFn>,
}"""
r1 = """pub struct WorkflowSimulator {
    handler: WorkflowHandlerFn,
    state: SharedState,
    activity_mocks: HashMap<String, ActivityMockFn>,
    child_workflow_mocks: HashMap<String, ActivityMockFn>,
    signals_to_send: HashMap<String, Value>,
}"""
content = replace_once(original, s1, r1)

# 2. new()
s2 = """pub fn new(handler: WorkflowHandlerFn) -> Self {
        Self {
            handler,
            state: empty_shared_state(),
            activity_mocks: HashMap::new(),
        }
    }"""
r2 = """pub fn new(handler: WorkflowHandlerFn) -> Self {
        Self {
            handler,
            state: empty_shared_state(),
            activity_mocks: HashMap::new(),
            child_workflow_mocks: HashMap::new(),
            signals_to_send: HashMap::new(),
        }
    }"""
content = replace_once(content, s2, r2)

# 3. Builder methods before run()
s3 = """    /// Run the workflow to completion using the provided input.
    ///
    /// # Panics
    ///
    /// Panics if the workflow deadlocks (e.g., suspends without emitting any
    /// mockable commands).
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, input: Value) -> SimulatorResult {"""
r3 = """    /// Register a mock implementation for a child workflow by name.
    #[must_use]
    pub fn mock_child_workflow<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.child_workflow_mocks.insert(name.to_string(), Box::new(mock));
        self
    }

    /// Register a signal to send when requested by `wait_for_signal`.
    #[must_use]
    pub fn send_signal(mut self, name: &str, payload: Value) -> Self {
        self.signals_to_send.insert(name.to_string(), payload);
        self
    }

    /// Run the workflow to completion using the provided input.
    ///
    /// # Panics
    ///
    /// Panics if the workflow deadlocks (e.g., suspends without emitting any
    /// progressable commands like mocked activities, child workflows, or awaited signals).
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, input: Value) -> SimulatorResult {"""
content = replace_once(content, s3, r3)

# 4. Match arms inside run()
s4 = """                            WorkflowCommand::RecordMarker { name, details } => {"""
r4 = """                            WorkflowCommand::StartChildWorkflow {
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
                            WorkflowCommand::WaitForSignal {
                                signal_name,
                                ..
                            } => {
                                if let Some(payload) = self.signals_to_send.get(&signal_name) {
                                    history.push(WorkflowEvent::SignalReceived {
                                        signal_name: signal_name.clone(),
                                        payload: payload.clone(),
                                    });
                                    advanced = true;
                                }
                            }
                            WorkflowCommand::RecordMarker { name, details } => {"""
content = replace_once(content, s4, r4)

# 5. Add the test at the very end.
s6 = """    #[tokio::test]
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
}
"""
r6 = """    #[tokio::test]
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
        let final_output = res.final_output.expect("workflow should complete successfully");
        assert_eq!(final_output, serde_json::json!({ "final": { "child": "signal_data" } }));
    }
}
"""
content = replace_once(content, s6, r6)

with open('autumn-harvest/src/simulator.rs', 'w') as f:
    f.write(content)
