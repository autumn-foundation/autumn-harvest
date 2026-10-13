//! What an agent asks a sandbox to run, and the checked plan that a backend
//! runs.
//!
//! A [`SandboxRequest`] is untrusted input. [`SandboxPolicy::plan`] checks it
//! against the policy and turns it into an [`ExecPlan`]. The guest checks the
//! plan again with [`ExecPlan::validate`] before it writes a file. The guest
//! must not trust the host process more than it needs to.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::error::SandboxError;
use super::policy::{NetworkPolicy, RuntimeSpec, SandboxLimits, SandboxPolicy};

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
        if self.argv.is_empty() || self.argv[0].is_empty() {
            return Err(SandboxError::Denied("the plan has no command".into()));
        }
        for arg in &self.argv {
            check_no_nul("argument", arg)?;
        }
        let mut paths = BTreeSet::new();
        for file in &self.files {
            validate_relative_path(&file.path)?;
            if !paths.insert(file.path.as_str()) {
                return Err(SandboxError::Denied(format!(
                    "file path {:?} appears twice",
                    file.path
                )));
            }
        }
        check_no_nested_files(&paths)?;
        for output in &self.outputs {
            validate_relative_path(output)?;
        }
        for (name, value) in &self.env {
            // The engine adds `HARVEST_*` variables after the request check,
            // so the plan check admits that prefix.
            if !name.starts_with(RESERVED_ENV_PREFIX) {
                validate_env_name(name)?;
            } else if name.len() > 128
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(SandboxError::Denied(format!(
                    "engine variable name {name:?} is not valid"
                )));
            }
            check_no_nul("environment value", value)?;
        }
        check_no_nul("PATH", &self.path_env)?;
        let limits = &self.limits;
        let zero = [
            ("wall_clock_ms", limits.wall_clock_ms),
            ("cpu_seconds", limits.cpu_seconds),
            ("memory_bytes", limits.memory_bytes),
            ("max_processes", u64::from(limits.max_processes)),
            ("max_file_bytes", limits.max_file_bytes),
            ("max_open_files", u64::from(limits.max_open_files)),
        ]
        .into_iter()
        .find(|(_, value)| *value == 0);
        if let Some((name, _)) = zero {
            return Err(SandboxError::Denied(format!(
                "limit {name} must not be zero"
            )));
        }
        Ok(())
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
    let deny = |why: &str| Err(SandboxError::Denied(format!("path {path:?} {why}")));
    if path.is_empty() {
        return deny("is empty");
    }
    if path.len() > MAX_PATH_BYTES {
        return deny(&format!("is longer than {MAX_PATH_BYTES} bytes"));
    }
    if path.contains('\0') {
        return deny("holds a NUL byte");
    }
    if path.starts_with('/') {
        return deny("is absolute; use a path relative to the work directory");
    }
    let mut depth = 0;
    for component in path.split('/') {
        depth += 1;
        match component {
            "" => return deny("has an empty component"),
            "." | ".." => return deny("has a '.' or '..' component"),
            _ => {}
        }
    }
    if depth > MAX_PATH_DEPTH {
        return deny(&format!("is deeper than {MAX_PATH_DEPTH} components"));
    }
    Ok(())
}

/// The content of one request file: exactly one of `text` and `base64`.
fn decode_file(file: &SandboxFile) -> Result<Vec<u8>, SandboxError> {
    match (&file.text, &file.base64) {
        (Some(text), None) => Ok(text.clone().into_bytes()),
        (None, Some(encoded)) => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|e| {
                    SandboxError::Denied(format!("file {:?} is not valid base64: {e}", file.path))
                })
        }
        _ => Err(SandboxError::Denied(format!(
            "file {:?} must set exactly one of `text` and `base64`",
            file.path
        ))),
    }
}

/// Deny `value` when it holds a NUL byte. `execve` cannot pass one.
fn check_no_nul(what: &str, value: &str) -> Result<(), SandboxError> {
    if value.contains('\0') {
        return Err(SandboxError::Denied(format!("{what} holds a NUL byte")));
    }
    Ok(())
}

/// Deny a file whose path is a directory of another file, such as `a` and
/// `a/b`. The executor could not create both.
fn check_no_nested_files(paths: &BTreeSet<&str>) -> Result<(), SandboxError> {
    for path in paths {
        let mut prefix = String::new();
        for component in path.split('/') {
            if !prefix.is_empty() && paths.contains(prefix.as_str()) {
                return Err(SandboxError::Denied(format!(
                    "file path {path:?} is inside the file {prefix:?}"
                )));
            }
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
        }
    }
    Ok(())
}

/// Check that `name` is a valid environment name that a request can set.
///
/// # Errors
///
/// Returns [`SandboxError::Denied`] that names the defect.
pub fn validate_env_name(name: &str) -> Result<(), SandboxError> {
    let deny = |why: &str| {
        Err(SandboxError::Denied(format!(
            "environment name {name:?} {why}"
        )))
    };
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return deny("is empty");
    };
    if name.len() > 128 {
        return deny("is longer than 128 bytes");
    }
    if !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return deny("must match [A-Za-z_][A-Za-z0-9_]*");
    }
    if name.starts_with("LD_") || name.starts_with("DYLD_") {
        return deny("is a loader variable");
    }
    if name.starts_with(RESERVED_ENV_PREFIX) || EXECUTOR_ENV.contains(&name) {
        return deny("is reserved");
    }
    Ok(())
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
        let Some(spec) = self.runtimes.get(&request.runtime) else {
            let allowed: Vec<&str> = self.runtimes.keys().map(String::as_str).collect();
            return Err(SandboxError::Denied(format!(
                "runtime {:?} is not allowed; allowed runtimes: {}",
                request.runtime,
                allowed.join(", ")
            )));
        };
        let limits = self.resolve_limits(&request.limits)?;
        let (files, stdin) = self.plan_input(spec, request)?;
        self.check_counts(request)?;
        let mut argv = spec.argv.clone();
        argv.push(spec.file_name.clone());
        argv.extend(request.args.iter().cloned());
        let plan = ExecPlan {
            argv,
            files,
            stdin,
            env: request.env.clone(),
            path_env: self.path_env.clone(),
            limits,
            outputs: request.outputs.clone(),
            max_output_file_bytes: self.sizes.max_output_file_bytes,
            max_output_files_total_bytes: self.sizes.max_output_files_total_bytes,
            network: self.network,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Decode the code, the files and stdin, and check their sizes. The code
    /// file comes first.
    fn plan_input(
        &self,
        spec: &RuntimeSpec,
        request: &SandboxRequest,
    ) -> Result<(Vec<PlanFile>, Vec<u8>), SandboxError> {
        let sizes = &self.sizes;
        if request.files.len().saturating_add(1) > sizes.max_files {
            return Err(SandboxError::Denied(format!(
                "{} files exceed the limit of {} (the code file counts as one)",
                request.files.len() + 1,
                sizes.max_files
            )));
        }
        let mut files = Vec::with_capacity(request.files.len() + 1);
        files.push(PlanFile {
            path: spec.file_name.clone(),
            content: request.code.clone().into_bytes(),
            executable: false,
        });
        for file in &request.files {
            validate_relative_path(&file.path)?;
            files.push(PlanFile {
                path: file.path.clone(),
                content: decode_file(file)?,
                executable: file.executable,
            });
        }
        let stdin = request.stdin.clone().unwrap_or_default().into_bytes();
        let input_bytes = files
            .iter()
            .map(|f| f.content.len())
            .sum::<usize>()
            .saturating_add(stdin.len());
        if u64::try_from(input_bytes).unwrap_or(u64::MAX) > sizes.max_input_bytes {
            return Err(SandboxError::Denied(format!(
                "the code, files and stdin hold {input_bytes} bytes, over the limit of {}",
                sizes.max_input_bytes
            )));
        }
        Ok((files, stdin))
    }

    /// Check the counts and sizes of the arguments, the environment and the
    /// outputs.
    fn check_counts(&self, request: &SandboxRequest) -> Result<(), SandboxError> {
        let sizes = &self.sizes;
        let over = |what: &str, count: usize, max: usize| {
            Err(SandboxError::Denied(format!(
                "{count} {what} exceed the limit of {max}"
            )))
        };
        if request.args.len() > sizes.max_args {
            return over("arguments", request.args.len(), sizes.max_args);
        }
        let arg_bytes: usize = request.args.iter().map(String::len).sum();
        if arg_bytes > sizes.max_arg_bytes {
            return over("argument bytes", arg_bytes, sizes.max_arg_bytes);
        }
        if request.env.len() > sizes.max_env {
            return over("environment variables", request.env.len(), sizes.max_env);
        }
        let env_bytes: usize = request.env.iter().map(|(k, v)| k.len() + v.len()).sum();
        if env_bytes > sizes.max_env_bytes {
            return over("environment bytes", env_bytes, sizes.max_env_bytes);
        }
        if request.outputs.len() > sizes.max_outputs {
            return over("outputs", request.outputs.len(), sizes.max_outputs);
        }
        let mut seen = BTreeSet::new();
        for output in &request.outputs {
            validate_relative_path(output)?;
            if !seen.insert(output.as_str()) {
                return Err(SandboxError::Denied(format!(
                    "output {output:?} appears twice"
                )));
            }
        }
        Ok(())
    }

    /// Resolve the requested limits against the defaults and the ceilings.
    fn resolve_limits(&self, requested: &RequestedLimits) -> Result<SandboxLimits, SandboxError> {
        let pick = |name: &str, asked: Option<u64>, default: u64, max: u64, scale: u64| {
            let Some(value) = asked else {
                return Ok(default);
            };
            if value == 0 {
                return Err(SandboxError::Denied(format!(
                    "limit {name} must not be zero"
                )));
            }
            let scaled = value.saturating_mul(scale);
            if scaled > max {
                return Err(SandboxError::Denied(format!(
                    "limit {name} {value} exceeds the policy maximum {}",
                    max / scale
                )));
            }
            Ok(scaled)
        };
        let d = &self.default_limits;
        let m = &self.max_limits;
        Ok(SandboxLimits {
            wall_clock_ms: pick(
                "timeout_ms",
                requested.timeout_ms,
                d.wall_clock_ms,
                m.wall_clock_ms,
                1,
            )?,
            cpu_seconds: pick(
                "cpu_seconds",
                requested.cpu_seconds,
                d.cpu_seconds,
                m.cpu_seconds,
                1,
            )?,
            memory_bytes: pick(
                "memory_mib",
                requested.memory_mib,
                d.memory_bytes,
                m.memory_bytes,
                1024 * 1024,
            )?,
            max_output_bytes: pick(
                "max_output_bytes",
                requested.max_output_bytes,
                d.max_output_bytes,
                m.max_output_bytes,
                1,
            )?,
            ..*d
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sandbox::policy::{DEFAULT_LIMITS, MAX_LIMITS};

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
        assert_eq!(plan.stdin, b"");
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
        assert!(
            message.contains(&MAX_LIMITS.wall_clock_ms.to_string()),
            "{message}"
        );
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
            "../x",
            "a/../../x",
            "/etc/passwd",
            "a//b",
            "a/./b",
            ".",
            "",
            "a/",
            "a\0b",
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
            "",
            "1A",
            "A-B",
            "A B",
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
            "HOME",
            "PATH",
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
        let policy =
            SandboxPolicy::default().with_runtime("ruby", RuntimeSpec::new(&["ruby"], "main.rb"));
        let plan = policy.plan(&SandboxRequest::new("ruby", "puts 1")).unwrap();
        assert_eq!(plan.argv, ["ruby", "main.rb"]);
    }
}
