//! What a sandbox run produced.
//!
//! The outcome is data. A non-zero exit, a signal and a timeout are all
//! outcomes, so the activity completes and history records them. The agent
//! reads stderr and fixes its code. Replay reads the recorded outcome and
//! never runs the code again.

use serde::{Deserialize, Serialize};

use super::error::SandboxError;
use super::policy::IsolationLevel;
use super::request::ExecPlan;

/// How the code ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunStatus {
    /// The main process exited with this code.
    Exited {
        /// The exit code.
        code: i32,
    },
    /// A signal ended the main process. `24` is `SIGXCPU`, the CPU limit.
    /// `25` is `SIGXFSZ`, the file size limit. `9` is `SIGKILL`.
    Signaled {
        /// The signal number.
        signal: i32,
    },
    /// The run reached its wall-clock limit, and the executor killed it.
    TimedOut,
}

/// One output file that the run returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputFile {
    /// The path that the request asked for.
    pub path: String,
    /// The content.
    #[serde(with = "super::b64")]
    pub content: Vec<u8>,
}

/// One output file that the run does not return, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkippedOutput {
    /// The path that the request asked for.
    pub path: String,
    /// Why the file is not in the outcome.
    pub reason: String,
}

/// The result of one sandbox run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxOutcome {
    /// How the code ended.
    pub status: RunStatus,
    /// The kept stdout text. Invalid UTF-8 becomes U+FFFD.
    pub stdout: String,
    /// The kept stderr text. Invalid UTF-8 becomes U+FFFD.
    pub stderr: String,
    /// All bytes that the code wrote to stdout.
    pub stdout_bytes: u64,
    /// All bytes that the code wrote to stderr.
    pub stderr_bytes: u64,
    /// The outcome keeps the head and the tail of stdout, and drops the
    /// middle.
    pub stdout_truncated: bool,
    /// The outcome keeps the head and the tail of stderr, and drops the
    /// middle.
    pub stderr_truncated: bool,
    /// The output files that the run returns.
    pub outputs: Vec<OutputFile>,
    /// The output files that the run does not return.
    pub skipped_outputs: Vec<SkippedOutput>,
    /// The wall-clock time of the run, in milliseconds.
    pub duration_ms: u64,
    /// The isolation of the backend. The host sets it, never the guest.
    pub isolation: IsolationLevel,
    /// The guest image that ran the code, when the operator names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

impl SandboxOutcome {
    /// Whether the code exited with code 0.
    #[must_use]
    pub const fn success(&self) -> bool {
        matches!(self.status, RunStatus::Exited { code: 0 })
    }

    /// Check that an outcome from a guest respects the bounds of `plan`.
    ///
    /// The guest runs untrusted code, so the host checks each bound again
    /// before it records the outcome.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Protocol`] for the first broken bound.
    pub fn check_bounds(&self, plan: &ExecPlan) -> Result<(), SandboxError> {
        let _ = plan;
        todo!("SandboxOutcome::check_bounds")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use crate::sandbox::policy::SandboxPolicy;
    use crate::sandbox::request::SandboxRequest;

    pub(crate) fn outcome(status: RunStatus) -> SandboxOutcome {
        SandboxOutcome {
            status,
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            outputs: Vec::new(),
            skipped_outputs: Vec::new(),
            duration_ms: 1,
            isolation: IsolationLevel::Process,
            image: None,
        }
    }

    fn plan_with_output(path: &str) -> ExecPlan {
        SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", "true").with_output(path))
            .unwrap()
    }

    #[test]
    fn only_exit_code_zero_is_success() {
        assert!(outcome(RunStatus::Exited { code: 0 }).success());
        assert!(!outcome(RunStatus::Exited { code: 1 }).success());
        assert!(!outcome(RunStatus::Signaled { signal: 9 }).success());
        assert!(!outcome(RunStatus::TimedOut).success());
    }

    #[test]
    fn the_status_serializes_with_a_kind_tag() {
        let json = serde_json::to_value(RunStatus::Exited { code: 3 }).unwrap();
        assert_eq!(json, serde_json::json!({"kind": "exited", "code": 3}));
        let json = serde_json::to_value(RunStatus::TimedOut).unwrap();
        assert_eq!(json, serde_json::json!({"kind": "timed_out"}));
    }

    #[test]
    fn an_outcome_round_trips_through_json() {
        let mut out = outcome(RunStatus::Signaled { signal: 24 });
        out.outputs.push(OutputFile {
            path: "a.bin".into(),
            content: vec![0, 255],
        });
        let back: SandboxOutcome =
            serde_json::from_str(&serde_json::to_string(&out).unwrap()).unwrap();
        assert_eq!(back, out);
    }

    #[test]
    fn an_outcome_inside_the_bounds_passes() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.stdout = "x".repeat(usize::try_from(plan.limits.max_output_bytes).unwrap());
        out.outputs.push(OutputFile {
            path: "out.txt".into(),
            content: b"ok".to_vec(),
        });
        assert!(out.check_bounds(&plan).is_ok());
    }

    #[test]
    fn oversized_streams_break_the_bounds() {
        let plan = plan_with_output("out.txt");
        let max = usize::try_from(plan.limits.max_output_bytes).unwrap();
        let mut out = outcome(RunStatus::Exited { code: 0 });
        // Lossy decoding can turn one byte into three, so 3x is the bound.
        out.stderr = "x".repeat(3 * max + 64 + 1);
        assert!(matches!(out.check_bounds(&plan), Err(SandboxError::Protocol(_))));
    }

    #[test]
    fn an_output_that_the_plan_did_not_ask_for_breaks_the_bounds() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(OutputFile {
            path: "/etc/shadow".into(),
            content: b"root".to_vec(),
        });
        assert!(matches!(out.check_bounds(&plan), Err(SandboxError::Protocol(_))));
    }

    #[test]
    fn an_oversized_output_file_breaks_the_bounds() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        let too_big = usize::try_from(plan.max_output_file_bytes).unwrap() + 1;
        out.outputs.push(OutputFile {
            path: "out.txt".into(),
            content: vec![0; too_big],
        });
        assert!(matches!(out.check_bounds(&plan), Err(SandboxError::Protocol(_))));
    }

    #[test]
    fn a_repeated_output_breaks_the_bounds() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        for _ in 0..2 {
            out.outputs.push(OutputFile {
                path: "out.txt".into(),
                content: b"1".to_vec(),
            });
        }
        assert!(matches!(out.check_bounds(&plan), Err(SandboxError::Protocol(_))));
    }
}
