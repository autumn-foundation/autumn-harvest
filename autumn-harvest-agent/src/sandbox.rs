//! A tool that runs code in a sandbox.
//!
//! [`SandboxTool`] gives an agent one tool, `run_code` by default. The model
//! sends a [`SandboxRequest`] as the tool input: a runtime, the code, and
//! optional files, stdin, arguments, environment and output paths. The tool
//! returns a bounded view of the [`SandboxOutcome`] as JSON. A non-zero exit
//! is a result, not a tool error, so the model can read stderr and fix its
//! code.
//!
//! The harness cuts a long tool result. So the view keeps a head and a tail
//! of each stream, and a short part of each output file. The status and
//! stderr then stay in the part that the model reads.
//!
//! The tool runs inside the tool-call activity of the agent loop. History
//! records the outcome, so replay never runs the code again.
//!
//! [`SandboxRequest`]: autumn_harvest::sandbox::SandboxRequest
//! [`SandboxOutcome`]: autumn_harvest::sandbox::SandboxOutcome

use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::sandbox::{
    IDEMPOTENCY_KEY_ENV, IsolationLevel, NetworkPolicy, RunOptions, SandboxError, SandboxOutcome,
    SandboxRequest, SandboxRuntime,
};
use tokio_util::sync::CancellationToken;

use crate::error::{AgentError, ErrorKind};
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolContext, ToolEffect};

/// The default tool name.
pub const DEFAULT_TOOL_NAME: &str = "run_code";

/// The JSON characters of each stream in the view of the model.
pub const VIEW_STREAM_CHARS: usize = 2000;

/// The JSON characters of all output entries together in the view of the
/// model: paths, skip reasons, file text and structure.
pub const VIEW_OUTPUTS_CHARS: usize = 1500;

/// The JSON characters of each path and each skip reason in the view.
const VIEW_LABEL_CHARS: usize = 80;

/// Room for the JSON structure of one output entry, with a 20-digit byte
/// count and the comma after it.
const VIEW_ENTRY_CHARS: usize = 72;

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
        let policy = runtime.policy();
        let runtimes: Vec<&str> = policy.runtimes.keys().map(String::as_str).collect();
        let network = match policy.network {
            NetworkPolicy::Deny => "has no network",
            NetworkPolicy::Allow => "has network access",
        };
        let description = format!(
            "Run code in a new, isolated sandbox and get its exit status, stdout, stderr and \
             output files. Runtimes: {}. The sandbox {network}. Each call starts clean. A \
             non-zero exit is a normal result: read stderr and fix the code. The wall clock \
             limit is {} ms by default and {} ms at most.",
            runtimes.join(", "),
            policy.default_limits.wall_clock_ms,
            policy.max_limits.wall_clock_ms,
        );
        Self {
            runtime,
            name: DEFAULT_TOOL_NAME.to_owned(),
            description,
        }
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
/// A denial and a protocol violation have the `Tool` kind. An unavailable
/// backend and a cancel have the `Unavailable` kind. A configuration error
/// has the `Config` kind. The harness gives each error to the model as text,
/// and the model can call the tool again.
#[must_use]
pub fn tool_error(err: &SandboxError) -> AgentError {
    let kind = match err {
        SandboxError::Denied(_) | SandboxError::Protocol(_) => ErrorKind::Tool,
        SandboxError::Config(_) => ErrorKind::Config,
        SandboxError::Unavailable(_) | SandboxError::Cancelled => ErrorKind::Unavailable,
    };
    AgentError::new(kind, err.to_string())
}

impl Tool for SandboxTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> serde_json::Value {
        let runtimes: Vec<&str> = self
            .runtime
            .policy()
            .runtimes
            .keys()
            .map(String::as_str)
            .collect();
        serde_json::json!({
            "type": "object",
            "properties": {
                "runtime": {"type": "string", "enum": runtimes, "description": "The language runtime."},
                "code": {"type": "string", "description": "The program."},
                "args": {"type": "array", "items": {"type": "string"}, "description": "Arguments after the program file."},
                "stdin": {"type": "string", "description": "Text for stdin."},
                "files": {
                    "type": "array",
                    "description": "Input files, relative to the work directory. Set text or base64.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "text": {"type": "string"},
                            "base64": {"type": "string"},
                            "executable": {"type": "boolean"}
                        },
                        "required": ["path"],
                        "additionalProperties": false
                    }
                },
                "env": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Environment variables."},
                "outputs": {"type": "array", "items": {"type": "string"}, "description": "Relative paths of files to return."},
                "limits": {
                    "type": "object",
                    "properties": {
                        "timeout_ms": {"type": "integer", "minimum": 1},
                        "cpu_seconds": {"type": "integer", "minimum": 1},
                        "memory_mib": {"type": "integer", "minimum": 1},
                        "max_output_bytes": {"type": "integer", "minimum": 1}
                    },
                    "additionalProperties": false
                }
            },
            "required": ["runtime"],
            "additionalProperties": false
        })
    }

    fn execute<'a>(
        &'a self,
        input: serde_json::Value,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        Box::pin(async move {
            let request: SandboxRequest = serde_json::from_value(input).map_err(|e| {
                AgentError::new(
                    ErrorKind::Tool,
                    format!("the input is not a valid request: {e}"),
                )
            })?;
            // The run, the step and the call id form an idempotency key.
            let mut engine_env = BTreeMap::new();
            engine_env.insert(
                IDEMPOTENCY_KEY_ENV.to_owned(),
                format!("{}:{}:{}", ctx.run_id.as_str(), ctx.step, ctx.call_id),
            );
            let options = RunOptions {
                engine_env,
                time_left: None,
            };
            let outcome = self
                .runtime
                .run_with(&request, options, &CancellationToken::new())
                .await
                .map_err(|e| tool_error(&e))?;
            Ok(model_view(&outcome))
        })
    }

    /// Code in a microVM with no network changes only its own sandbox. Code
    /// in a host process can change host files. Code with a network acts
    /// outside the app.
    fn effect(&self) -> ToolEffect {
        match self.runtime.policy().network {
            NetworkPolicy::Allow => ToolEffect::External,
            NetworkPolicy::Deny if self.runtime.isolation() >= IsolationLevel::MicroVm => {
                ToolEffect::Internal
            }
            NetworkPolicy::Deny => ToolEffect::Write,
        }
    }
}

/// The bounded view of `outcome` that the model reads.
///
/// The harness sorts the keys and cuts the end, and `outputs` sorts before
/// `status`. So the output entries share [`VIEW_OUTPUTS_CHARS`]: each path
/// and skip reason is cut, and UTF-8 file text shares the rest. An entry that
/// does not fit is only counted, in `outputs_more` or `skipped_more`. Each
/// stream keeps a head and a tail of [`VIEW_STREAM_CHARS`]. The byte counts
/// stay exact.
///
/// Each size counts the characters of the JSON text, because the harness
/// cuts that text. JSON writes a control character as six characters.
#[must_use]
pub fn model_view(outcome: &SandboxOutcome) -> serde_json::Value {
    let mut budget = VIEW_OUTPUTS_CHARS;
    let mut outputs = Vec::new();
    let mut outputs_more = 0usize;
    let entries = outcome.outputs.len() + outcome.skipped_outputs.len();
    for (index, file) in outcome.outputs.iter().enumerate() {
        let path = cut(&file.path, VIEW_LABEL_CHARS);
        let cost = json_chars(&path) + VIEW_ENTRY_CHARS;
        if cost > budget {
            outputs_more += 1;
            continue;
        }
        budget -= cost;
        let entry = std::str::from_utf8(&file.content).map_or_else(
            |_| serde_json::json!({"path": path, "bytes": file.content.len(), "binary": true}),
            |text| {
                // Share the rest evenly, so one large file leaves room for
                // the entries after it.
                let shown = cut(text, budget / (entries - index));
                budget = budget.saturating_sub(json_chars(&shown));
                serde_json::json!({
                    "path": path,
                    "bytes": file.content.len(),
                    "text": shown,
                    "truncated": shown != text,
                })
            },
        );
        outputs.push(entry);
    }
    let mut skipped = Vec::new();
    let mut skipped_more = 0usize;
    for skip in &outcome.skipped_outputs {
        let path = cut(&skip.path, VIEW_LABEL_CHARS);
        let reason = cut(&skip.reason, VIEW_LABEL_CHARS);
        let cost = json_chars(&path) + json_chars(&reason) + VIEW_ENTRY_CHARS;
        if cost > budget {
            skipped_more += 1;
            continue;
        }
        budget -= cost;
        skipped.push(serde_json::json!({"path": path, "reason": reason}));
    }
    let stdout = cut(&outcome.stdout, VIEW_STREAM_CHARS);
    let stderr = cut(&outcome.stderr, VIEW_STREAM_CHARS);
    let mut view = serde_json::json!({
        "status": outcome.status,
        "stdout_truncated": outcome.stdout_truncated || stdout != outcome.stdout,
        "stderr_truncated": outcome.stderr_truncated || stderr != outcome.stderr,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_bytes": outcome.stdout_bytes,
        "stderr_bytes": outcome.stderr_bytes,
        "outputs": outputs,
        "skipped_outputs": skipped,
        "duration_ms": outcome.duration_ms,
    });
    for (key, more) in [
        ("outputs_more", outputs_more),
        ("skipped_more", skipped_more),
    ] {
        if more > 0 {
            view[key] = serde_json::json!(more);
        }
    }
    view
}

/// `text` in at most `max` characters of JSON: the head and the tail around
/// a marker. When the marker does not fit, only a head stays.
fn cut(text: &str, max: usize) -> String {
    if json_chars(text) <= max {
        return text.to_owned();
    }
    let count = text.chars().count();
    // The marker never names more characters than the text holds.
    let Some(keep) = max.checked_sub(json_chars(&cut_marker(count))) else {
        return head_within(text, max).to_owned();
    };
    let head = head_within(text, keep.div_ceil(2));
    let tail = tail_within(&text[head.len()..], keep - json_chars(head));
    let dropped = count - head.chars().count() - tail.chars().count();
    format!("{head}{}{tail}", cut_marker(dropped))
}

/// The marker for `dropped` characters that [`cut`] removed.
fn cut_marker(dropped: usize) -> String {
    format!("\n[... {dropped} characters cut ...]\n")
}

/// The longest start of `text` that fits `max` characters of JSON.
fn head_within(text: &str, max: usize) -> &str {
    let mut used = 0;
    for (at, c) in text.char_indices() {
        used += json_char_len(c);
        if used > max {
            return &text[..at];
        }
    }
    text
}

/// The longest end of `text` that fits `max` characters of JSON.
fn tail_within(text: &str, max: usize) -> &str {
    let mut used = 0;
    for (at, c) in text.char_indices().rev() {
        used += json_char_len(c);
        if used > max {
            return &text[at + c.len_utf8()..];
        }
    }
    text
}

/// The characters of JSON for `text` inside a string, without the quotes.
fn json_chars(text: &str) -> usize {
    text.chars().map(json_char_len).sum()
}

/// The characters of JSON for `c` inside a string. `serde_json` escapes the
/// quote, the backslash and each control character, and nothing else.
const fn json_char_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        '\0'..='\u{1f}' => 6,
        _ => 1,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use autumn_harvest::sandbox::runtime::BoxFuture as SandboxFuture;
    use autumn_harvest::sandbox::{
        OutputFile, RunStatus, SandboxBackend, SandboxJob, SandboxPolicy, SkippedOutput,
    };
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::message::RunId;

    /// A backend that echoes the code as stdout and exits with code 1.
    #[derive(Default)]
    struct EchoBackend {
        seen: Mutex<Vec<SandboxJob>>,
        process: bool,
    }

    impl SandboxBackend for EchoBackend {
        fn isolation(&self) -> IsolationLevel {
            if self.process {
                IsolationLevel::Process
            } else {
                IsolationLevel::MicroVm
            }
        }

        fn supports_network(&self) -> bool {
            true
        }

        fn execute(
            &self,
            job: SandboxJob,
            _cancel: CancellationToken,
        ) -> SandboxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            let code = String::from_utf8_lossy(&job.plan.files[0].content).into_owned();
            let bytes = u64::try_from(code.len()).unwrap();
            self.seen.lock().unwrap().push(job);
            Box::pin(async move {
                Ok(SandboxOutcome {
                    status: RunStatus::Exited { code: 1 },
                    stdout: code,
                    stderr: "boom".into(),
                    stdout_bytes: bytes,
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
        assert!(
            tool.description().contains("python3"),
            "{}",
            tool.description()
        );
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
        // The schema must not offer a field that the request rejects, at the
        // top level or inside a file or the limits.
        let schema = tool(SandboxPolicy::default()).input_schema();
        let sample_of = |spec: &serde_json::Value| -> serde_json::Value {
            match spec["type"].as_str().unwrap() {
                "string" => json!("python3"),
                "integer" => json!(1),
                "boolean" => json!(true),
                "array" => json!([]),
                "object" => json!({}),
                other => panic!("unexpected type {other}"),
            }
        };
        let object_of = |properties: &serde_json::Value| -> serde_json::Value {
            let mut object = serde_json::Map::new();
            for (field, spec) in properties.as_object().unwrap() {
                object.insert(field.clone(), sample_of(spec));
            }
            serde_json::Value::Object(object)
        };
        let mut sample = object_of(&schema["properties"]);
        let file = object_of(&schema["properties"]["files"]["items"]["properties"]);
        sample["files"] = json!([file]);
        sample["limits"] = object_of(&schema["properties"]["limits"]["properties"]);
        assert_eq!(sample["limits"].as_object().unwrap().len(), 4);
        assert_eq!(sample["files"][0].as_object().unwrap().len(), 4);
        serde_json::from_value::<SandboxRequest>(sample).unwrap();
    }

    #[test]
    fn the_effect_follows_the_network_and_the_isolation() {
        assert_eq!(
            tool(SandboxPolicy::default()).effect(),
            ToolEffect::Internal
        );
        let open = SandboxPolicy::default().with_network(NetworkPolicy::Allow);
        assert_eq!(tool(open).effect(), ToolEffect::External);
        let backend = Arc::new(EchoBackend {
            process: true,
            ..EchoBackend::default()
        });
        let runtime = SandboxRuntime::new(backend, SandboxPolicy::for_development()).unwrap();
        assert_eq!(
            SandboxTool::new(Arc::new(runtime)).effect(),
            ToolEffect::Write,
            "a host process can change host files"
        );
    }

    #[test]
    fn many_long_output_paths_do_not_push_the_status_out_of_the_cut() {
        let mut outcome = SandboxOutcome {
            status: RunStatus::Exited { code: 1 },
            stdout: "o".repeat(50_000),
            stderr: "Traceback: boom".into(),
            stdout_bytes: 50_000,
            stderr_bytes: 15,
            stdout_truncated: false,
            stderr_truncated: false,
            outputs: Vec::new(),
            skipped_outputs: Vec::new(),
            duration_ms: 5,
            isolation: IsolationLevel::MicroVm,
            image: None,
        };
        for i in 0..16 {
            outcome.outputs.push(OutputFile {
                path: format!("{i:02}/{}", "p".repeat(500)),
                content: vec![b'x'; 300],
            });
            outcome.skipped_outputs.push(SkippedOutput {
                path: format!("{i:02}/{}", "q".repeat(500)),
                reason: "r".repeat(256),
            });
        }
        let text = model_view(&outcome).to_string();
        assert!(text.len() < 8000, "{} bytes", text.len());
        let status = text.find("\"status\"").unwrap();
        let stderr = text.find("Traceback: boom").unwrap();
        assert!(
            status < 3000 && stderr < 3500,
            "status at {status}, stderr at {stderr}"
        );
        assert!(text.contains("\"outputs_more\""), "{text}");
    }

    #[test]
    fn escaped_characters_do_not_push_the_status_out_of_the_cut() {
        // JSON writes each of these control characters as six characters.
        let junk = |n: usize| "\u{1}".repeat(n);
        let mut outcome = SandboxOutcome {
            status: RunStatus::Exited { code: 1 },
            stdout: junk(10_000),
            stderr: format!("{}Traceback: boom", junk(10_000)),
            stdout_bytes: 10_000,
            stderr_bytes: 10_015,
            stdout_truncated: false,
            stderr_truncated: false,
            outputs: Vec::new(),
            skipped_outputs: Vec::new(),
            duration_ms: 5,
            isolation: IsolationLevel::MicroVm,
            image: None,
        };
        for i in 0..8 {
            outcome.outputs.push(OutputFile {
                path: format!("{i}{}", junk(100)),
                content: junk(1500).into_bytes(),
            });
            outcome.skipped_outputs.push(SkippedOutput {
                path: format!("{i}{}", "\"".repeat(100)),
                reason: junk(256),
            });
        }
        let text = model_view(&outcome).to_string();
        let chars = text.chars().count();
        assert!(
            chars <= crate::harness::DEFAULT_TOOL_OUTPUT_LIMIT,
            "{chars} characters"
        );
        let status = text.find("\"status\"").unwrap();
        let stderr = text.find("Traceback: boom").unwrap();
        assert!(
            status < 3000 && stderr < 5000,
            "status at {status}, stderr at {stderr}"
        );
    }

    #[test]
    fn a_small_share_never_overdraws_the_budget() {
        // The last entries get a share smaller than the cut marker. They
        // must not wrap the budget and let later entries in.
        let mut outcome = SandboxOutcome {
            status: RunStatus::Exited { code: 0 },
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            outputs: Vec::new(),
            skipped_outputs: Vec::new(),
            duration_ms: 5,
            isolation: IsolationLevel::MicroVm,
            image: None,
        };
        for i in 0..40 {
            outcome.outputs.push(OutputFile {
                path: format!("{i:02}"),
                content: vec![b'x'; 5000],
            });
        }
        let view = model_view(&outcome);
        let outputs = serde_json::to_string(&view["outputs"]).unwrap();
        assert!(
            outputs.chars().count() <= VIEW_OUTPUTS_CHARS + VIEW_ENTRY_CHARS,
            "{} characters",
            outputs.chars().count()
        );
    }

    #[test]
    fn the_json_size_matches_serde_json() {
        let probes = (0u32..0x80)
            .chain([0x7f, 0xe9, 0x2028, 0x2029, 0xfffd, 0x1_f600])
            .filter_map(char::from_u32);
        for c in probes {
            let written = serde_json::to_string(&c.to_string()).unwrap();
            assert_eq!(json_char_len(c), written.chars().count() - 2, "{c:?}");
        }
    }

    #[test]
    fn a_cut_never_exceeds_its_json_size() {
        let text = "a\u{1}\"\u{e9}\n\u{1f600}".repeat(200);
        for max in 0..400 {
            let shown = cut(&text, max);
            let written = serde_json::to_string(&shown).unwrap().chars().count() - 2;
            assert!(written <= max, "max {max}: {written}");
        }
        assert_eq!(cut("short", 5), "short");
    }

    #[test]
    fn the_entry_room_holds_the_largest_entry_structure() {
        for entry in [
            json!({"path": "", "bytes": u64::MAX, "text": "", "truncated": false}),
            json!({"path": "", "bytes": u64::MAX, "binary": true}),
            json!({"path": "", "reason": ""}),
        ] {
            // One more character for the comma between entries.
            let chars = entry.to_string().chars().count() + 1;
            assert!(chars <= VIEW_ENTRY_CHARS, "{entry}: {chars}");
        }
    }

    #[test]
    fn the_view_keeps_the_status_and_stderr_within_the_harness_cut() {
        let outcome = SandboxOutcome {
            status: RunStatus::Exited { code: 2 },
            stdout: "o".repeat(50_000),
            stderr: format!("{}Traceback: the end", "e".repeat(50_000)),
            stdout_bytes: 50_000,
            stderr_bytes: 50_018,
            stdout_truncated: false,
            stderr_truncated: false,
            outputs: vec![
                OutputFile {
                    path: "out/big.txt".into(),
                    content: "x".repeat(20_000).into_bytes(),
                },
                OutputFile {
                    path: "out/blob.bin".into(),
                    content: vec![0xff; 100],
                },
            ],
            skipped_outputs: Vec::new(),
            duration_ms: 5,
            isolation: IsolationLevel::MicroVm,
            image: None,
        };
        let view = model_view(&outcome);
        let text = view.to_string();
        assert!(text.len() < 8000, "{} bytes", text.len());
        assert_eq!(view["status"], json!({"kind": "exited", "code": 2}));
        assert!(
            view["stderr"]
                .as_str()
                .unwrap()
                .ends_with("Traceback: the end")
        );
        assert_eq!(view["stderr_truncated"], true);
        assert_eq!(view["stdout_bytes"], 50_000);
        assert_eq!(view["outputs"][0]["bytes"], 20_000);
        assert_eq!(view["outputs"][0]["truncated"], true);
        assert_eq!(
            view["outputs"][1],
            json!({"path": "out/blob.bin", "bytes": 100, "binary": true})
        );
    }

    #[tokio::test]
    async fn a_failing_program_is_a_result_not_a_tool_error() {
        let tool = tool(SandboxPolicy::default());
        let out = tool
            .execute(
                json!({"runtime": "python3", "code": "raise SystemExit(1)"}),
                &ctx(),
            )
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

    #[tokio::test]
    async fn the_code_gets_an_idempotency_key_from_the_call() {
        let backend = Arc::new(EchoBackend::default());
        let runtime = SandboxRuntime::new(backend.clone(), SandboxPolicy::default()).unwrap();
        let tool = SandboxTool::new(Arc::new(runtime));
        tool.execute(json!({"runtime": "sh", "code": "true"}), &ctx())
            .await
            .unwrap();
        let jobs = backend.seen.lock().unwrap();
        assert_eq!(
            jobs[0]
                .plan
                .env
                .get(IDEMPOTENCY_KEY_ENV)
                .map(String::as_str),
            Some("run-1:0:call-1")
        );
    }

    #[test]
    fn errors_map_to_tool_kinds() {
        assert_eq!(
            tool_error(&SandboxError::Denied("x".into())).kind(),
            ErrorKind::Tool
        );
        assert_eq!(
            tool_error(&SandboxError::Protocol("x".into())).kind(),
            ErrorKind::Tool
        );
        assert_eq!(
            tool_error(&SandboxError::Config("x".into())).kind(),
            ErrorKind::Config
        );
        assert!(
            tool_error(&SandboxError::Unavailable("x".into()))
                .kind()
                .is_retryable()
        );
        assert!(tool_error(&SandboxError::Cancelled).kind().is_retryable());
    }
}
