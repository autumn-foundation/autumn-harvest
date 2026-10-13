//! What an agent asks a sandbox to run, and the checked plan that a backend
//! runs.
//!
//! A [`SandboxRequest`] is untrusted input. [`SandboxPolicy::plan`] checks it
//! against the policy and turns it into an [`ExecPlan`]. The guest checks the
//! plan again with [`ExecPlan::validate`] before it writes a file, because the
//! guest must not trust the host process more than it needs to.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::error::SandboxError;
use super::policy::{NetworkPolicy, SandboxLimits, SandboxPolicy};

/// The most bytes in one path of a file or of an output.
pub const MAX_PATH_BYTES: usize = 512;

/// The most components in one path.
pub const MAX_PATH_DEPTH: usize = 16;

/// The prefix of the environment names that the engine sets.
pub const RESERVED_ENV_PREFIX: &str = "HARVEST_";

/// The environment names that the executor sets for each run.
pub const EXECUTOR_ENV: [&str; 5] = ["HOME", "PATH", "PWD", "TMPDIR", "LANG"];

/// One run of code, as an agent asks for it.
///
/// Unknown fields are an error, so a misspelled field fails loudly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRequest {
    /// The runtime name, such as `python3`. The policy maps it to a command.
    pub runtime: String,
    /// The code. The executor writes it to the file that the runtime names.
    #[serde(default)]
    pub code: String,
    /// Arguments after the code file.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Text for stdin. `None` gives an empty stdin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    /// More files for the work directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<SandboxFile>,
    /// Environment variables. The code gets no host environment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Paths of files to return after the run, relative to the work
    /// directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<String>,
    /// Lower limits than the policy defaults, or higher up to the ceilings.
    #[serde(default, skip_serializing_if = "RequestedLimits::is_empty")]
    pub limits: RequestedLimits,
}

/// One input file. Set exactly one of `text` and `base64`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxFile {
    /// The path, relative to the work directory.
    pub path: String,
    /// The content as UTF-8 text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// The content as standard base64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
    /// Make the file executable.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub executable: bool,
}

/// The limits that a request asks for. `None` takes the policy default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestedLimits {
    /// Wall-clock time, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// CPU time, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_seconds: Option<u64>,
    /// Memory, in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// Bytes of stdout, and separately of stderr, to keep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
}

impl RequestedLimits {
    /// Whether the request asks for no limit.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.timeout_ms.is_none()
            && self.cpu_seconds.is_none()
            && self.memory_mib.is_none()
            && self.max_output_bytes.is_none()
    }
}

impl SandboxRequest {
    /// A request that runs `code` with `runtime`.
    #[must_use]
    pub fn new(runtime: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            runtime: runtime.into(),
            code: code.into(),
            ..Self::default()
        }
    }

    /// Add an argument.
    #[must_use]
    pub fn with_arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Set stdin.
    #[must_use]
    pub fn with_stdin(mut self, stdin: impl Into<String>) -> Self {
        self.stdin = Some(stdin.into());
        self
    }

    /// Add a text file.
    #[must_use]
    pub fn with_file(mut self, path: impl Into<String>, text: impl Into<String>) -> Self {
        self.files.push(SandboxFile {
            path: path.into(),
            text: Some(text.into()),
            ..SandboxFile::default()
        });
        self
    }

    /// Add an environment variable.
    #[must_use]
    pub fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    /// Ask for a file back after the run.
    #[must_use]
    pub fn with_output(mut self, path: impl Into<String>) -> Self {
        self.outputs.push(path.into());
        self
    }

    /// Ask for a wall-clock limit.
    #[must_use]
    pub const fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.limits.timeout_ms = Some(timeout_ms);
        self
    }
}

/// One file of a plan, with its decoded content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFile {
    /// The checked relative path.
    pub path: String,
    /// The content.
    #[serde(with = "super::b64")]
    pub content: Vec<u8>,
    /// Make the file executable.
    pub executable: bool,
}

/// A request after the policy checked it. A backend runs a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecPlan {
    /// The command line. `argv[0]` is the command.
    pub argv: Vec<String>,
    /// The files for the work directory, the code file included.
    pub files: Vec<PlanFile>,
    /// The stdin bytes.
    #[serde(with = "super::b64")]
    pub stdin: Vec<u8>,
    /// The environment of the code, the executor variables excluded.
    pub env: BTreeMap<String, String>,
    /// The `PATH` that the code sees.
    pub path_env: String,
    /// The hard limits of the run.
    pub limits: SandboxLimits,
    /// The files to return after the run.
    pub outputs: Vec<String>,
    /// Bytes of one output file to return.
    pub max_output_file_bytes: u64,
    /// Bytes of all output files to return.
    pub max_output_files_total_bytes: u64,
    /// Network access for the code.
    pub network: NetworkPolicy,
}

impl ExecPlan {
    /// Check the parts of a plan that the guest depends on for its own
    /// safety: the paths, the environment names and the limits.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Denied`] for the first defect found.
    pub fn validate(&self) -> Result<(), SandboxError> {
        let _ = self;
        todo!("ExecPlan::validate")
    }
}

/// Check that `path` is a safe relative POSIX path.
///
/// Each component must be a normal name: no empty component, no `.`, no
/// `..`, no NUL byte and no leading `/`. The check splits on `/` by hand, so
/// it gives the same answer on every host OS.
///
/// # Errors
///
/// Returns [`SandboxError::Denied`] that names the defect.
pub fn validate_relative_path(path: &str) -> Result<(), SandboxError> {
    let _ = path;
    todo!("validate_relative_path")
}

/// Check that `name` is a valid environment name that a request can set.
///
/// # Errors
///
/// Returns [`SandboxError::Denied`] that names the defect.
pub fn validate_env_name(name: &str) -> Result<(), SandboxError> {
    let _ = name;
    todo!("validate_env_name")
}

impl SandboxPolicy {
    /// Check `request` against this policy and build the plan to run.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Denied`] for the first rule that the request
    /// breaks. The message names the rule and the limit, so an agent can
    /// correct its request.
    pub fn plan(&self, request: &SandboxRequest) -> Result<ExecPlan, SandboxError> {
        let _ = (self, request, BTreeSet::<String>::new());
        todo!("SandboxPolicy::plan")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sandbox::policy::{DEFAULT_LIMITS, MAX_LIMITS, RuntimeSpec};

    fn denied(result: Result<ExecPlan, SandboxError>) -> String {
        match result {
            Err(SandboxError::Denied(message)) => message,
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[test]
    fn a_python_request_becomes_a_plan_with_the_code_file() {
        let policy = SandboxPolicy::default();
        let request = SandboxRequest::new("python3", "print(1)").with_arg("x");
        let plan = policy.plan(&request).unwrap();
        assert_eq!(plan.argv, ["python3", "-I", "-B", "-u", "main.py", "x"]);
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.files[0].path, "main.py");
        assert_eq!(plan.files[0].content, b"print(1)");
        assert_eq!(plan.limits, DEFAULT_LIMITS);
        assert_eq!(plan.network, NetworkPolicy::Deny);
        assert!(plan.stdin.is_empty());
    }

    #[test]
    fn an_unknown_runtime_is_denied_and_names_the_allowed_ones() {
        let message = denied(SandboxPolicy::default().plan(&SandboxRequest::new("cobol", "")));
        assert!(message.contains("cobol"), "{message}");
        assert!(message.contains("python3"), "{message}");
    }

    #[test]
    fn a_limit_over_the_ceiling_is_denied_and_names_the_ceiling() {
        let policy = SandboxPolicy::default();
        let request =
            SandboxRequest::new("sh", "true").with_timeout_ms(MAX_LIMITS.wall_clock_ms + 1);
        let message = denied(policy.plan(&request));
        assert!(message.contains("timeout_ms"), "{message}");
        assert!(message.contains(&MAX_LIMITS.wall_clock_ms.to_string()), "{message}");
    }

    #[test]
    fn a_zero_limit_is_denied() {
        let request = SandboxRequest::new("sh", "true").with_timeout_ms(0);
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn requested_limits_replace_the_defaults() {
        let mut request = SandboxRequest::new("sh", "true").with_timeout_ms(1_500);
        request.limits.memory_mib = Some(64);
        request.limits.cpu_seconds = Some(2);
        request.limits.max_output_bytes = Some(10);
        let plan = SandboxPolicy::default().plan(&request).unwrap();
        assert_eq!(plan.limits.wall_clock_ms, 1_500);
        assert_eq!(plan.limits.memory_bytes, 64 * 1024 * 1024);
        assert_eq!(plan.limits.cpu_seconds, 2);
        assert_eq!(plan.limits.max_output_bytes, 10);
        assert_eq!(plan.limits.max_processes, DEFAULT_LIMITS.max_processes);
    }

    #[test]
    fn traversal_and_absolute_paths_are_denied() {
        for path in [
            "../x", "a/../../x", "/etc/passwd", "a//b", "a/./b", ".", "", "a/", "a\0b",
        ] {
            assert!(
                matches!(validate_relative_path(path), Err(SandboxError::Denied(_))),
                "{path:?} must be denied"
            );
        }
        for path in ["a", "a/b/c.txt", ".hidden", "dir.d/x..y"] {
            assert!(validate_relative_path(path).is_ok(), "{path:?} must pass");
        }
        let deep = vec!["d"; MAX_PATH_DEPTH + 1].join("/");
        assert!(validate_relative_path(&deep).is_err());
        assert!(validate_relative_path(&"a".repeat(MAX_PATH_BYTES + 1)).is_err());
    }

    #[test]
    fn a_file_path_with_traversal_is_denied_in_a_request() {
        let request = SandboxRequest::new("sh", "true").with_file("../../etc/cron.d/x", "boom");
        let message = denied(SandboxPolicy::default().plan(&request));
        assert!(message.contains("../../etc/cron.d/x"), "{message}");
    }

    #[test]
    fn a_file_needs_exactly_one_content_field() {
        let mut request = SandboxRequest::new("sh", "true");
        request.files.push(SandboxFile {
            path: "a".into(),
            ..SandboxFile::default()
        });
        denied(SandboxPolicy::default().plan(&request));
        request.files[0].text = Some("t".into());
        request.files[0].base64 = Some("dA==".into());
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn base64_files_are_decoded_and_bad_base64_is_denied() {
        let mut request = SandboxRequest::new("sh", "true");
        request.files.push(SandboxFile {
            path: "bin/tool".into(),
            base64: Some("AAEC".into()),
            executable: true,
            ..SandboxFile::default()
        });
        let plan = SandboxPolicy::default().plan(&request).unwrap();
        let file = plan.files.iter().find(|f| f.path == "bin/tool").unwrap();
        assert_eq!(file.content, [0, 1, 2]);
        assert!(file.executable);
        request.files[0].base64 = Some("not base64!".into());
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn a_file_that_shadows_the_code_file_or_repeats_a_path_is_denied() {
        let request = SandboxRequest::new("python3", "print(1)").with_file("main.py", "x");
        denied(SandboxPolicy::default().plan(&request));
        let request = SandboxRequest::new("sh", "true")
            .with_file("a", "1")
            .with_file("a", "2");
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn a_file_under_a_file_path_is_denied() {
        let request = SandboxRequest::new("sh", "true")
            .with_file("a", "1")
            .with_file("a/b", "2");
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn oversized_input_is_denied() {
        let mut policy = SandboxPolicy::default();
        policy.sizes.max_input_bytes = 10;
        let request = SandboxRequest::new("sh", "echo 0123456789");
        let message = denied(policy.plan(&request));
        assert!(message.contains("10"), "{message}");
        let request = SandboxRequest::new("sh", "").with_stdin("0123456789ab");
        denied(policy.plan(&request));
    }

    #[test]
    fn too_many_files_args_env_or_outputs_are_denied() {
        let mut policy = SandboxPolicy::default();
        policy.sizes.max_files = 2;
        policy.sizes.max_args = 1;
        policy.sizes.max_env = 1;
        policy.sizes.max_outputs = 1;
        let files = SandboxRequest::new("sh", "")
            .with_file("a", "")
            .with_file("b", "");
        denied(policy.plan(&files));
        let args = SandboxRequest::new("sh", "").with_arg("1").with_arg("2");
        denied(policy.plan(&args));
        let env = SandboxRequest::new("sh", "")
            .with_env("A", "1")
            .with_env("B", "2");
        denied(policy.plan(&env));
        let outputs = SandboxRequest::new("sh", "")
            .with_output("a")
            .with_output("b");
        denied(policy.plan(&outputs));
    }

    #[test]
    fn env_names_are_checked_and_reserved_names_are_denied() {
        for name in ["A", "_x", "PY_PATH2"] {
            assert!(validate_env_name(name).is_ok(), "{name}");
        }
        for name in [
            "", "1A", "A-B", "A B", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "HOME", "PATH",
            "HARVEST_IDEMPOTENCY_KEY",
        ] {
            assert!(validate_env_name(name).is_err(), "{name:?} must be denied");
        }
        let request = SandboxRequest::new("sh", "").with_env("LD_PRELOAD", "/x.so");
        denied(SandboxPolicy::default().plan(&request));
        let request = SandboxRequest::new("sh", "").with_env("A", "a\0b");
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn a_nul_byte_in_an_argument_is_denied() {
        let request = SandboxRequest::new("sh", "").with_arg("a\0b");
        denied(SandboxPolicy::default().plan(&request));
    }

    #[test]
    fn output_paths_are_checked_and_unique() {
        let request = SandboxRequest::new("sh", "").with_output("../secret");
        denied(SandboxPolicy::default().plan(&request));
        let request = SandboxRequest::new("sh", "")
            .with_output("out.txt")
            .with_output("out.txt");
        denied(SandboxPolicy::default().plan(&request));
        let request = SandboxRequest::new("sh", "").with_output("out/a.csv");
        let plan = SandboxPolicy::default().plan(&request).unwrap();
        assert_eq!(plan.outputs, ["out/a.csv"]);
    }

    #[test]
    fn the_plan_carries_the_policy_path_and_network() {
        let policy = SandboxPolicy::default().with_network(NetworkPolicy::Allow);
        let plan = policy.plan(&SandboxRequest::new("sh", "")).unwrap();
        assert_eq!(plan.network, NetworkPolicy::Allow);
        assert_eq!(plan.path_env, policy.path_env);
    }

    #[test]
    fn unknown_request_fields_are_rejected_by_serde() {
        let json = serde_json::json!({"runtime": "sh", "code": "true", "timeout": 5});
        assert!(serde_json::from_value::<SandboxRequest>(json).is_err());
        let json = serde_json::json!({"runtime": "sh", "limits": {"timeout_ms": 5}});
        let request: SandboxRequest = serde_json::from_value(json).unwrap();
        assert_eq!(request.limits.timeout_ms, Some(5));
        assert_eq!(request.code, "");
    }

    #[test]
    fn a_plan_round_trips_through_json_with_binary_content() {
        let mut request = SandboxRequest::new("sh", "true").with_stdin("in");
        request.files.push(SandboxFile {
            path: "blob".into(),
            base64: Some("/wD+".into()),
            ..SandboxFile::default()
        });
        let plan = SandboxPolicy::default().plan(&request).unwrap();
        let json = serde_json::to_string(&plan).unwrap();
        let back: ExecPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(back, plan);
    }

    #[test]
    fn plan_validation_rejects_what_the_policy_would_reject() {
        let plan = SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", "true"))
            .unwrap();
        assert!(plan.validate().is_ok());
        let mut bad = plan.clone();
        bad.files[0].path = "../x".into();
        assert!(bad.validate().is_err());
        let mut bad = plan.clone();
        bad.outputs.push("/etc/shadow".into());
        assert!(bad.validate().is_err());
        let mut bad = plan.clone();
        bad.env.insert("LD_PRELOAD".into(), "x".into());
        assert!(bad.validate().is_err());
        let mut bad = plan.clone();
        bad.argv.clear();
        assert!(bad.validate().is_err());
        let mut bad = plan;
        bad.limits.wall_clock_ms = 0;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn a_custom_runtime_runs_its_own_command() {
        let policy = SandboxPolicy::default().with_runtime("ruby", RuntimeSpec::new(&["ruby"], "main.rb"));
        let plan = policy.plan(&SandboxRequest::new("ruby", "puts 1")).unwrap();
        assert_eq!(plan.argv, ["ruby", "main.rb"]);
    }
}
