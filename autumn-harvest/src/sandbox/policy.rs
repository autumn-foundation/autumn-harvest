//! The operator policy that bounds each sandbox run.
//!
//! The operator writes the policy. The agent writes the request. A request
//! can ask for less than the policy allows, never more. A value over a policy
//! ceiling is a non-retryable denial, not a silent clamp, so the agent sees
//! the limit and can correct its request.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::error::SandboxError;

/// How strongly a backend isolates code from the host.
///
/// The order is significant: `Process < MicroVm`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// A Linux process with resource limits and namespaces. It shares the
    /// host kernel. It is not a boundary for hostile code.
    Process,
    /// A Firecracker microVM with its own guest kernel.
    MicroVm,
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Process => "process",
            Self::MicroVm => "micro_vm",
        })
    }
}

/// Network access for sandboxed code.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    /// No network. This is the default.
    #[default]
    Deny,
    /// The network that the operator configured on the backend.
    Allow,
}

/// The resource limits of one run.
///
/// Each field is a hard limit that the backend enforces. The fields are plain
/// integers, so the limits cross the guest protocol as plain JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxLimits {
    /// Wall-clock time of the run, in milliseconds.
    pub wall_clock_ms: u64,
    /// CPU time of the run, in seconds.
    pub cpu_seconds: u64,
    /// Memory of the code, in bytes.
    pub memory_bytes: u64,
    /// Processes and threads that the code can have at one time.
    pub max_processes: u32,
    /// Size of one file that the code writes, in bytes.
    pub max_file_bytes: u64,
    /// Open file descriptors of one process.
    pub max_open_files: u32,
    /// Bytes of stdout, and separately of stderr, that the outcome keeps.
    pub max_output_bytes: u64,
}

impl SandboxLimits {
    /// The wall-clock limit as a [`Duration`].
    #[must_use]
    pub const fn wall_clock(&self) -> Duration {
        Duration::from_millis(self.wall_clock_ms)
    }
}

/// The limits that a request gets when it asks for none.
pub const DEFAULT_LIMITS: SandboxLimits = SandboxLimits {
    wall_clock_ms: 30_000,
    cpu_seconds: 30,
    memory_bytes: 512 * 1024 * 1024,
    max_processes: 64,
    max_file_bytes: 64 * 1024 * 1024,
    max_open_files: 256,
    max_output_bytes: 256 * 1024,
};

/// The highest limits that a request can ask for under the default policy.
pub const MAX_LIMITS: SandboxLimits = SandboxLimits {
    wall_clock_ms: 300_000,
    cpu_seconds: 300,
    memory_bytes: 2 * 1024 * 1024 * 1024,
    max_processes: 256,
    max_file_bytes: 256 * 1024 * 1024,
    max_open_files: 1024,
    max_output_bytes: 1024 * 1024,
};

/// How to run the code of one runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSpec {
    /// The command and its fixed arguments. The code file and the request
    /// arguments follow them.
    pub argv: Vec<String>,
    /// The name of the file that holds the code, in the work directory.
    pub file_name: String,
}

impl RuntimeSpec {
    /// A runtime that runs `file_name` with `argv`.
    #[must_use]
    pub fn new(argv: &[&str], file_name: &str) -> Self {
        Self {
            argv: argv.iter().map(|a| (*a).to_owned()).collect(),
            file_name: file_name.to_owned(),
        }
    }
}

/// The size bounds of a request and of its outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizeBounds {
    /// Files in a request, the code file included.
    pub max_files: usize,
    /// Bytes of the code, the files and stdin together.
    pub max_input_bytes: u64,
    /// Arguments in a request.
    pub max_args: usize,
    /// Bytes of all arguments together.
    pub max_arg_bytes: usize,
    /// Environment variables in a request.
    pub max_env: usize,
    /// Bytes of all environment names and values together.
    pub max_env_bytes: usize,
    /// Output files that a request can ask for.
    pub max_outputs: usize,
    /// Bytes of one output file that the outcome can return.
    pub max_output_file_bytes: u64,
    /// Bytes of all output files that the outcome can return.
    pub max_output_files_total_bytes: u64,
}

/// The default size bounds.
pub const DEFAULT_SIZE_BOUNDS: SizeBounds = SizeBounds {
    max_files: 64,
    max_input_bytes: 1024 * 1024,
    max_args: 64,
    max_arg_bytes: 64 * 1024,
    max_env: 64,
    max_env_bytes: 64 * 1024,
    max_outputs: 16,
    max_output_file_bytes: 256 * 1024,
    max_output_files_total_bytes: 512 * 1024,
};

/// The operator policy for one sandbox runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPolicy {
    /// The runtimes that a request can name, by name.
    pub runtimes: BTreeMap<String, RuntimeSpec>,
    /// The limits that a request gets when it asks for none.
    pub default_limits: SandboxLimits,
    /// The highest limits that a request can ask for.
    pub max_limits: SandboxLimits,
    /// The size bounds of a request and of its outcome.
    pub sizes: SizeBounds,
    /// Network access for the code.
    pub network: NetworkPolicy,
    /// The weakest isolation that this policy accepts from a backend.
    pub min_isolation: IsolationLevel,
    /// The `PATH` that the code sees.
    pub path_env: String,
}

impl Default for SandboxPolicy {
    /// The production default: microVM isolation, no network, and the
    /// `python3`, `node`, `bash` and `sh` runtimes.
    fn default() -> Self {
        Self {
            runtimes: default_runtimes(),
            default_limits: DEFAULT_LIMITS,
            max_limits: MAX_LIMITS,
            sizes: DEFAULT_SIZE_BOUNDS,
            network: NetworkPolicy::Deny,
            min_isolation: IsolationLevel::MicroVm,
            path_env: "/usr/local/bin:/usr/bin:/bin".to_owned(),
        }
    }
}

/// The built-in runtimes.
///
/// `python3 -I` ignores the `PYTHON*` variables and the user site directory.
/// `-B` writes no bytecode files, and `-u` keeps the output unbuffered, so a
/// killed run keeps the output that it printed.
fn default_runtimes() -> BTreeMap<String, RuntimeSpec> {
    let mut runtimes = BTreeMap::new();
    runtimes.insert(
        "python3".to_owned(),
        RuntimeSpec::new(&["python3", "-I", "-B", "-u"], "main.py"),
    );
    runtimes.insert("node".to_owned(), RuntimeSpec::new(&["node"], "main.js"));
    runtimes.insert(
        "bash".to_owned(),
        RuntimeSpec::new(&["bash", "--noprofile", "--norc"], "main.sh"),
    );
    runtimes.insert("sh".to_owned(), RuntimeSpec::new(&["sh"], "main.sh"));
    runtimes
}

impl SandboxPolicy {
    /// The default policy, with process isolation allowed.
    ///
    /// Use it for development and tests only. A process sandbox shares the
    /// host kernel, so it does not contain hostile code.
    #[must_use]
    pub fn for_development() -> Self {
        Self {
            min_isolation: IsolationLevel::Process,
            ..Self::default()
        }
    }

    /// Add or replace a runtime.
    #[must_use]
    pub fn with_runtime(mut self, name: impl Into<String>, spec: RuntimeSpec) -> Self {
        self.runtimes.insert(name.into(), spec);
        self
    }

    /// Set the network policy.
    #[must_use]
    pub const fn with_network(mut self, network: NetworkPolicy) -> Self {
        self.network = network;
        self
    }

    /// Set the default and the highest limits.
    #[must_use]
    pub const fn with_limits(mut self, default_limits: SandboxLimits, max: SandboxLimits) -> Self {
        self.default_limits = default_limits;
        self.max_limits = max;
        self
    }

    /// Set the size bounds.
    #[must_use]
    pub const fn with_sizes(mut self, sizes: SizeBounds) -> Self {
        self.sizes = sizes;
        self
    }

    /// Check that the policy is consistent.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] for one of these defects:
    ///
    /// - a default limit is over its ceiling, or a limit is zero;
    /// - no runtime exists;
    /// - a runtime name or spec is not valid.
    pub fn validate(&self) -> Result<(), SandboxError> {
        if self.runtimes.is_empty() {
            return Err(SandboxError::Config("the policy names no runtime".into()));
        }
        for (name, spec) in &self.runtimes {
            if !is_valid_runtime_name(name) {
                return Err(SandboxError::Config(format!(
                    "runtime name {name:?} is not 1 to 32 bytes of a-z, 0-9, '_' or '-'"
                )));
            }
            if spec.argv.is_empty() || spec.argv.iter().any(|a| a.is_empty() || a.contains('\0')) {
                return Err(SandboxError::Config(format!(
                    "runtime {name:?} needs a command, and no argument may be empty or hold NUL"
                )));
            }
            super::request::validate_relative_path(&spec.file_name).map_err(|e| {
                SandboxError::Config(format!("runtime {name:?} has a bad file name: {e}"))
            })?;
        }
        for (field, default, max) in limit_pairs(&self.default_limits, &self.max_limits) {
            if default == 0 || max == 0 {
                return Err(SandboxError::Config(format!(
                    "limit {field} must not be zero"
                )));
            }
            if default > max {
                return Err(SandboxError::Config(format!(
                    "default {field} {default} exceeds its ceiling {max}"
                )));
            }
        }
        if self.path_env.contains('\0') {
            return Err(SandboxError::Config("the PATH value holds NUL".into()));
        }
        // Each request and each outcome must fit one guest frame. Otherwise
        // an accepted request fails late, as a retryable fault.
        let frame = u64::try_from(super::protocol::MAX_FRAME_BYTES).unwrap_or(u64::MAX);
        for (what, bound) in [
            ("request", self.max_request_json_bytes()),
            ("outcome", self.max_outcome_json_bytes()),
        ] {
            if bound > frame {
                return Err(SandboxError::Config(format!(
                    "the size bounds allow a {what} of {bound} bytes, over the {frame}-byte guest \
                     frame; lower the bounds"
                )));
            }
        }
        Ok(())
    }

    /// The most bytes that the JSON of one outcome can have.
    ///
    /// JSON escapes one byte of text into at most six bytes. Base64 makes
    /// three bytes into four. The sum bounds the outcome, so an activity can
    /// raise its result cap to fit it.
    #[must_use]
    pub fn max_outcome_json_bytes(&self) -> u64 {
        // Each kept stream byte can decode to three bytes of U+FFFD, and each
        // of those can escape to six bytes of JSON.
        let streams = self.max_limits.max_output_bytes.saturating_mul(2 * 3 * 6);
        let files = self
            .sizes
            .max_output_file_bytes
            .max(self.sizes.max_output_files_total_bytes)
            .div_ceil(3)
            .saturating_mul(4);
        // Each output appears once, returned or skipped, with an escaped
        // path, an escaped skip reason and its JSON structure.
        let reason = u64::try_from(super::outcome::MAX_SKIP_REASON_BYTES).unwrap_or(u64::MAX);
        let per_output = path_json_bytes()
            .saturating_add(reason.saturating_mul(6))
            .saturating_add(ITEM_JSON_BYTES);
        let outputs = count(self.sizes.max_outputs).saturating_mul(per_output);
        streams
            .saturating_add(files)
            .saturating_add(outputs)
            .saturating_add(OUTCOME_ENVELOPE_BYTES)
    }

    /// The most bytes that the JSON of one request can have.
    #[must_use]
    pub fn max_request_json_bytes(&self) -> u64 {
        // Text escapes to at most six bytes a byte, and base64 to 4/3, so six
        // times the input covers both. Arguments and variables escape too.
        let input = self.sizes.max_input_bytes.saturating_mul(6);
        let args = u64::try_from(self.sizes.max_arg_bytes).unwrap_or(u64::MAX);
        let env = u64::try_from(self.sizes.max_env_bytes).unwrap_or(u64::MAX);
        // Each file and each output adds an escaped path. Each item of any
        // list adds its JSON structure.
        let paths = count(self.sizes.max_files)
            .saturating_add(count(self.sizes.max_outputs))
            .saturating_mul(path_json_bytes());
        let items = count(self.sizes.max_files)
            .saturating_add(count(self.sizes.max_outputs))
            .saturating_add(count(self.sizes.max_args))
            .saturating_add(count(self.sizes.max_env))
            .saturating_mul(ITEM_JSON_BYTES);
        input
            .saturating_add(args.saturating_add(env).saturating_mul(6))
            .saturating_add(paths)
            .saturating_add(items)
            .saturating_add(OUTCOME_ENVELOPE_BYTES)
    }
}

/// Check that `name` is a valid runtime name: 1 to 32 bytes of `a-z`, `0-9`,
/// `_` or `-`.
#[must_use]
pub fn is_valid_runtime_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// Room for the fixed fields of an outcome or a request, in bytes.
const OUTCOME_ENVELOPE_BYTES: u64 = 64 * 1024;

/// Room for the JSON structure of one list item: keys, quotes, braces and
/// commas.
const ITEM_JSON_BYTES: u64 = 64;

/// The most bytes of one escaped path in JSON.
fn path_json_bytes() -> u64 {
    u64::try_from(super::request::MAX_PATH_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_mul(6)
}

/// A count as `u64`.
fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Each limit as `(name, default, ceiling)`.
fn limit_pairs(default: &SandboxLimits, max: &SandboxLimits) -> [(&'static str, u64, u64); 7] {
    [
        ("wall_clock_ms", default.wall_clock_ms, max.wall_clock_ms),
        ("cpu_seconds", default.cpu_seconds, max.cpu_seconds),
        ("memory_bytes", default.memory_bytes, max.memory_bytes),
        (
            "max_processes",
            u64::from(default.max_processes),
            u64::from(max.max_processes),
        ),
        ("max_file_bytes", default.max_file_bytes, max.max_file_bytes),
        (
            "max_open_files",
            u64::from(default.max_open_files),
            u64::from(max.max_open_files),
        ),
        (
            "max_output_bytes",
            default.max_output_bytes,
            max.max_output_bytes,
        ),
    ]
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn the_default_policy_requires_a_microvm_and_denies_the_network() {
        let policy = SandboxPolicy::default();
        assert_eq!(policy.min_isolation, IsolationLevel::MicroVm);
        assert_eq!(policy.network, NetworkPolicy::Deny);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn the_development_policy_allows_process_isolation() {
        let policy = SandboxPolicy::for_development();
        assert_eq!(policy.min_isolation, IsolationLevel::Process);
        assert_eq!(policy.network, NetworkPolicy::Deny);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn the_default_policy_has_the_four_builtin_runtimes() {
        let policy = SandboxPolicy::default();
        let names: Vec<&str> = policy.runtimes.keys().map(String::as_str).collect();
        assert_eq!(names, ["bash", "node", "python3", "sh"]);
        assert_eq!(policy.runtimes["python3"].file_name, "main.py");
    }

    #[test]
    fn microvm_isolation_is_stronger_than_process_isolation() {
        assert!(IsolationLevel::Process < IsolationLevel::MicroVm);
    }

    #[test]
    fn a_default_limit_over_its_ceiling_is_a_config_error() {
        let mut low = MAX_LIMITS;
        low.wall_clock_ms = 1_000;
        let policy = SandboxPolicy::default().with_limits(DEFAULT_LIMITS, low);
        let err = policy
            .validate()
            .expect_err("the default exceeds the ceiling");
        assert!(matches!(err, SandboxError::Config(ref m) if m.contains("wall_clock_ms")));
    }

    #[test]
    fn bounds_that_cannot_fit_a_guest_frame_are_a_config_error() {
        let mut sizes = DEFAULT_SIZE_BOUNDS;
        sizes.max_input_bytes = 64 * 1024 * 1024;
        let policy = SandboxPolicy::default().with_sizes(sizes);
        assert!(
            matches!(policy.validate(), Err(SandboxError::Config(ref m)) if m.contains("frame")),
            "a request"
        );
        let mut max = MAX_LIMITS;
        max.max_output_bytes = 4 * 1024 * 1024;
        let policy = SandboxPolicy::default().with_limits(DEFAULT_LIMITS, max);
        assert!(
            matches!(policy.validate(), Err(SandboxError::Config(ref m)) if m.contains("frame")),
            "an outcome"
        );
        SandboxPolicy::default().validate().unwrap();
    }

    #[test]
    fn a_zero_limit_is_a_config_error() {
        let mut zero = DEFAULT_LIMITS;
        zero.memory_bytes = 0;
        let policy = SandboxPolicy::default().with_limits(zero, MAX_LIMITS);
        assert!(matches!(policy.validate(), Err(SandboxError::Config(_))));
    }

    #[test]
    fn a_policy_without_runtimes_is_a_config_error() {
        let mut policy = SandboxPolicy::default();
        policy.runtimes.clear();
        assert!(matches!(policy.validate(), Err(SandboxError::Config(_))));
    }

    #[test]
    fn a_runtime_with_an_unsafe_file_name_is_a_config_error() {
        let policy =
            SandboxPolicy::default().with_runtime("evil", RuntimeSpec::new(&["sh"], "../main.sh"));
        assert!(matches!(policy.validate(), Err(SandboxError::Config(_))));
        let policy = SandboxPolicy::default().with_runtime("empty", RuntimeSpec::new(&[], "a.sh"));
        assert!(matches!(policy.validate(), Err(SandboxError::Config(_))));
    }

    #[test]
    fn runtime_names_are_short_lowercase_tokens() {
        assert!(is_valid_runtime_name("python3"));
        assert!(is_valid_runtime_name("go-1_22"));
        assert!(!is_valid_runtime_name(""));
        assert!(!is_valid_runtime_name("Python"));
        assert!(!is_valid_runtime_name("py thon"));
        assert!(!is_valid_runtime_name(&"a".repeat(33)));
        let policy =
            SandboxPolicy::default().with_runtime("Bad Name", RuntimeSpec::new(&["sh"], "a"));
        assert!(matches!(policy.validate(), Err(SandboxError::Config(_))));
    }

    #[test]
    fn the_outcome_bound_covers_escaped_streams_and_encoded_files() {
        let policy = SandboxPolicy::default();
        let streams = 2 * 6 * policy.max_limits.max_output_bytes;
        let files = policy.sizes.max_output_files_total_bytes.div_ceil(3) * 4;
        assert!(policy.max_outcome_json_bytes() >= streams + files);
    }

    #[test]
    fn the_bounds_count_each_path_and_each_item() {
        let base = SandboxPolicy::default();
        let mut sizes = DEFAULT_SIZE_BOUNDS;
        sizes.max_outputs += 1;
        let more = SandboxPolicy::default().with_sizes(sizes);
        let path = u64::try_from(crate::sandbox::request::MAX_PATH_BYTES).unwrap();
        assert!(more.max_request_json_bytes() >= base.max_request_json_bytes() + 6 * path);
        assert!(more.max_outcome_json_bytes() >= base.max_outcome_json_bytes() + 6 * path);
        // Many outputs with tiny byte budgets still cannot fit a frame.
        let mut sizes = DEFAULT_SIZE_BOUNDS;
        sizes.max_outputs = 200_000;
        sizes.max_output_file_bytes = 1;
        sizes.max_output_files_total_bytes = 1;
        let policy = SandboxPolicy::default().with_sizes(sizes);
        assert!(
            matches!(policy.validate(), Err(SandboxError::Config(ref m)) if m.contains("frame")),
            "{:?}",
            policy.validate()
        );
    }

    #[test]
    fn the_request_bound_covers_escaped_input() {
        let policy = SandboxPolicy::default();
        assert!(policy.max_request_json_bytes() >= 6 * policy.sizes.max_input_bytes);
    }

    #[test]
    fn limits_round_trip_through_json() {
        let json = serde_json::to_value(DEFAULT_LIMITS).unwrap();
        assert_eq!(json["wall_clock_ms"], 30_000);
        let back: SandboxLimits = serde_json::from_value(json).unwrap();
        assert_eq!(back, DEFAULT_LIMITS);
        assert_eq!(DEFAULT_LIMITS.wall_clock(), Duration::from_secs(30));
    }
}
