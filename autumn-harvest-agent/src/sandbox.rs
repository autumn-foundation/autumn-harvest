//! A tool that runs code in a sandbox.
//!
//! [`SandboxTool`] gives an agent one tool, `run_code` by default. The model
//! sends a [`SandboxRequest`] as the tool input: a runtime, the code, and
//! optional files, stdin, arguments, environment and output paths. The tool
//! returns the [`SandboxOutcome`] as JSON. A non-zero exit is a result, not a
//! tool error, so the model can read stderr and fix its code.
//!
//! The tool runs inside the tool-call activity of the agent loop. History
//! records the outcome, so replay never runs the code again.
//!
//! [`SandboxRequest`]: autumn_harvest::sandbox::SandboxRequest
//! [`SandboxOutcome`]: autumn_harvest::sandbox::SandboxOutcome

use std::sync::Arc;

use autumn_harvest::sandbox::{NetworkPolicy, SandboxError, SandboxRequest, SandboxRuntime};

use crate::error::{AgentError, ErrorKind};
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolContext, ToolEffect};

/// The default tool name.
pub const DEFAULT_TOOL_NAME: &str = "run_code";

/// A tool that runs code on a [`SandboxRuntime`].
#[derive(Debug, Clone)]
pub struct SandboxTool {
    runtime: Arc<SandboxRuntime>,
    name: String,
    description: String,
}

impl SandboxTool {
    /// A `run_code` tool on `runtime`.
    #[must_use]
    pub fn new(runtime: Arc<SandboxRuntime>) -> Self {
        let _ = runtime;
        todo!("SandboxTool::new")
    }

    /// Rename the tool.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Wrap the tool in an `Arc` for a harness.
    #[must_use]
    pub fn shared(self) -> Arc<dyn Tool> {
        Arc::new(self)
    }
}

/// Map a sandbox error to a tool error.
///
/// A denial and a protocol violation are tool errors that the model can
/// read. An unavailable backend and a cancel are retryable. A configuration
/// error is a configuration error.
#[must_use]
pub fn tool_error(err: &SandboxError) -> AgentError {
    let _ = (err, ErrorKind::Tool);
    todo!("tool_error")
}

impl Tool for SandboxTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> serde_json::Value {
        todo!("SandboxTool::input_schema")
    }

    fn execute<'a>(
        &'a self,
        input: serde_json::Value,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        let _ = (input, ctx, std::marker::PhantomData::<SandboxRequest>);
        todo!("SandboxTool::execute")
    }

    /// No network means that the code changes only its own sandbox.
    fn effect(&self) -> ToolEffect {
        let _ = NetworkPolicy::Deny;
        todo!("SandboxTool::effect")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use autumn_harvest::sandbox::{
        BoxFuture as SandboxFuture, IsolationLevel, RunStatus, SandboxBackend, SandboxJob,
        SandboxOutcome, SandboxPolicy,
    };
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::message::RunId;

    /// A backend that echoes the code as stdout and exits with code 1.
    #[derive(Default)]
    struct EchoBackend {
        seen: Mutex<Vec<SandboxJob>>,
    }

    impl SandboxBackend for EchoBackend {
        fn isolation(&self) -> IsolationLevel {
            IsolationLevel::MicroVm
        }

        fn execute(
            &self,
            job: SandboxJob,
            _cancel: CancellationToken,
        ) -> SandboxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            let code = String::from_utf8_lossy(&job.plan.files[0].content).into_owned();
            self.seen.lock().unwrap().push(job);
            Box::pin(async move {
                Ok(SandboxOutcome {
                    status: RunStatus::Exited { code: 1 },
                    stdout: code,
                    stderr: "boom".into(),
                    stdout_bytes: 0,
                    stderr_bytes: 4,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    outputs: Vec::new(),
                    skipped_outputs: Vec::new(),
                    duration_ms: 1,
                    isolation: IsolationLevel::Process,
                    image: None,
                })
            })
        }
    }

    fn tool(policy: SandboxPolicy) -> SandboxTool {
        let runtime = SandboxRuntime::new(Arc::new(EchoBackend::default()), policy).unwrap();
        SandboxTool::new(Arc::new(runtime))
    }

    fn ctx() -> ToolContext {
        ToolContext {
            run_id: RunId::new("run-1"),
            call_id: "call-1".into(),
            session_id: None,
            step: 0,
        }
    }

    #[test]
    fn the_tool_is_named_run_code_and_describes_itself() {
        let tool = tool(SandboxPolicy::default());
        assert_eq!(tool.name(), "run_code");
        assert!(tool.description().contains("python3"), "{}", tool.description());
        assert_eq!(tool.with_name("exec").name(), "exec");
    }

    #[test]
    fn the_schema_lists_the_runtimes_and_requires_runtime() {
        let schema = tool(SandboxPolicy::default()).input_schema();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["runtime"]));
        assert_eq!(
            schema["properties"]["runtime"]["enum"],
            json!(["bash", "node", "python3", "sh"])
        );
        assert_eq!(schema["additionalProperties"], false);
        for field in ["code", "args", "stdin", "files", "env", "outputs", "limits"] {
            assert!(schema["properties"][field].is_object(), "{field} missing");
        }
    }

    #[test]
    fn every_schema_field_is_a_request_field() {
        // The schema must not offer a field that the request rejects.
        let schema = tool(SandboxPolicy::default()).input_schema();
        let mut sample = serde_json::Map::new();
        for (field, spec) in schema["properties"].as_object().unwrap() {
            let value = match spec["type"].as_str().unwrap() {
                "string" => json!("python3"),
                "array" => json!([]),
                "object" => json!({}),
                other => panic!("unexpected type {other}"),
            };
            sample.insert(field.clone(), value);
        }
        serde_json::from_value::<SandboxRequest>(serde_json::Value::Object(sample)).unwrap();
    }

    #[test]
    fn the_effect_follows_the_network_policy() {
        assert_eq!(tool(SandboxPolicy::default()).effect(), ToolEffect::Internal);
        let open = SandboxPolicy::default().with_network(NetworkPolicy::Allow);
        assert_eq!(tool(open).effect(), ToolEffect::External);
    }

    #[tokio::test]
    async fn a_failing_program_is_a_result_not_a_tool_error() {
        let tool = tool(SandboxPolicy::default());
        let out = tool
            .execute(json!({"runtime": "python3", "code": "raise SystemExit(1)"}), &ctx())
            .await
            .unwrap();
        assert_eq!(out["status"], json!({"kind": "exited", "code": 1}));
        assert_eq!(out["stdout"], "raise SystemExit(1)");
        assert_eq!(out["stderr"], "boom");
    }

    #[tokio::test]
    async fn a_bad_request_is_a_tool_error_that_the_model_can_read() {
        let tool = tool(SandboxPolicy::default());
        let err = tool
            .execute(json!({"runtime": "cobol", "code": "x"}), &ctx())
            .await
            .expect_err("unknown runtime");
        assert_eq!(err.kind(), ErrorKind::Tool);
        assert!(err.message().contains("cobol"));
        let err = tool
            .execute(json!({"runtime": "sh", "unknown": 1}), &ctx())
            .await
            .expect_err("unknown field");
        assert_eq!(err.kind(), ErrorKind::Tool);
    }

    #[test]
    fn errors_map_to_tool_kinds() {
        assert_eq!(tool_error(&SandboxError::Denied("x".into())).kind(), ErrorKind::Tool);
        assert_eq!(tool_error(&SandboxError::Protocol("x".into())).kind(), ErrorKind::Tool);
        assert_eq!(tool_error(&SandboxError::Config("x".into())).kind(), ErrorKind::Config);
        assert!(tool_error(&SandboxError::Unavailable("x".into())).kind().is_retryable());
        assert!(tool_error(&SandboxError::Cancelled).kind().is_retryable());
    }
}
