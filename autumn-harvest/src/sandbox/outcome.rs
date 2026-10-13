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

/// The highest Linux signal number, `SIGRTMAX`.
pub const MAX_SIGNAL: i32 = 64;

/// The longest reason of a skipped output, in bytes.
pub const MAX_SKIP_REASON_BYTES: usize = 256;

/// How the code ended.
///
/// The decoder reads the fields in any order, because Postgres `jsonb`
/// sorts the keys. It skips an unknown field without a copy, as the guest
/// message decoder does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

/// Whether the byte count and the truncation flag of a stream match its
/// text, for a capture that keeps `cap` bytes.
///
/// A whole stream keeps each byte. A valid byte stays one byte of text, and
/// an invalid run of one to three bytes becomes one U+FFFD of three bytes.
/// So the text has between one and three bytes for each counted byte.
///
/// A cut stream counts more bytes than the cap and holds the truncation
/// marker. The capture keeps `cap` bytes and drops at most three at each side
/// of the cut. So the text also holds at least `cap - 6` bytes besides the
/// marker.
fn stream_counts_match(text: &str, bytes: u64, truncated: bool, cap: u64) -> bool {
    const SEAM_LOSS: u64 = 6;
    if truncated {
        let marker = super::capture::TRUNCATION_MARKER;
        let kept = u64::try_from(text.len().saturating_sub(marker.len())).unwrap_or(u64::MAX);
        return bytes > cap && text.contains(marker) && kept >= cap.saturating_sub(SEAM_LOSS);
    }
    let len = u64::try_from(text.len()).unwrap_or(u64::MAX);
    bytes <= cap && bytes <= len && len <= bytes.saturating_mul(3)
}

/// A guest string for an error message: quoted, escaped, and cut to 64
/// characters. A guest can send any bytes, and the message goes to history.
#[must_use]
pub fn shown(text: &str) -> String {
    const SHOWN_CHARS: usize = 64;
    if text.chars().count() <= SHOWN_CHARS {
        return format!("{text:?}");
    }
    let head: String = text.chars().take(SHOWN_CHARS).collect();
    format!("{head:?}...")
}

/// The fields of a run status.
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum StatusField {
    Kind,
    Code,
    Signal,
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for RunStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(RunStatusVisitor)
    }
}

/// Reads a run status field by field.
struct RunStatusVisitor;

impl<'de> serde::de::Visitor<'de> for RunStatusVisitor {
    type Value = RunStatus;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a run status object")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<RunStatus, A::Error> {
        use super::protocol::put;
        use serde::de::Error as _;
        let (mut kind, mut code, mut signal) = (None::<String>, None, None);
        while let Some(field) = map.next_key::<StatusField>()? {
            match field {
                StatusField::Kind => put(&mut kind, map.next_value()?, "kind")?,
                StatusField::Code => put(&mut code, map.next_value()?, "code")?,
                StatusField::Signal => put(&mut signal, map.next_value()?, "signal")?,
                StatusField::Other => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        match (kind.as_deref(), code, signal) {
            (Some("exited"), Some(code), None) => Ok(RunStatus::Exited { code }),
            (Some("signaled"), None, Some(signal)) => Ok(RunStatus::Signaled { signal }),
            (Some("timed_out"), None, None) => Ok(RunStatus::TimedOut),
            (None, ..) => Err(A::Error::missing_field("kind")),
            (Some(kind @ ("exited" | "signaled" | "timed_out")), ..) => Err(A::Error::custom(
                format!("a {kind} status has the wrong fields"),
            )),
            (Some(other), ..) => Err(A::Error::unknown_variant(
                other,
                &["exited", "signaled", "timed_out"],
            )),
        }
    }
}

/// One output file that the run returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFile {
    /// The path that the request asked for.
    pub path: String,
    /// The content.
    #[serde(with = "super::b64")]
    pub content: Vec<u8>,
}

/// One output file that the run does not return, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedOutput {
    /// The path that the request asked for.
    pub path: String,
    /// Why the file is not in the outcome.
    pub reason: String,
}

/// The result of one sandbox run.
///
/// History keeps it as the activity result. A decoder skips an unknown
/// field, so a later release can add a field while an older worker still
/// reads the result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Check that Linux can report the status.
    ///
    /// Linux exit codes are 0 to 255, and signals are 1 to 64. The executor
    /// reports signal 0 when the status names neither.
    fn check_status(&self) -> Result<(), SandboxError> {
        match self.status {
            RunStatus::Exited { code } if !(0..=255).contains(&code) => Err(
                SandboxError::Protocol(format!("exit code {code} is not 0 to 255")),
            ),
            RunStatus::Signaled { signal } if !(0..=MAX_SIGNAL).contains(&signal) => Err(
                SandboxError::Protocol(format!("signal {signal} is not 0 to {MAX_SIGNAL}")),
            ),
            _ => Ok(()),
        }
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
        let violation = |what: String| Err(SandboxError::Protocol(what));
        self.check_status()?;
        // Lossy decoding turns one invalid byte into three bytes of U+FFFD,
        // and the capture adds one truncation marker.
        let kept = usize::try_from(plan.limits.max_output_bytes).unwrap_or(usize::MAX);
        let stream_cap = kept
            .saturating_mul(3)
            .saturating_add(super::capture::TRUNCATION_MARKER.len());
        let streams = [
            (
                "stdout",
                &self.stdout,
                self.stdout_bytes,
                self.stdout_truncated,
            ),
            (
                "stderr",
                &self.stderr,
                self.stderr_bytes,
                self.stderr_truncated,
            ),
        ];
        for (name, text, bytes, truncated) in streams {
            if text.len() > stream_cap {
                return violation(format!(
                    "{name} holds {} bytes, over the bound of {stream_cap}",
                    text.len()
                ));
            }
            if text.contains('\0') {
                return violation(format!("{name} holds a NUL byte"));
            }
            if !stream_counts_match(text, bytes, truncated, plan.limits.max_output_bytes) {
                return violation(format!(
                    "{name} reports {bytes} bytes and truncated={truncated}, which its text \
                     does not match"
                ));
            }
        }
        let per_file = usize::try_from(plan.max_output_file_bytes).unwrap_or(usize::MAX);
        let total_cap = usize::try_from(plan.max_output_files_total_bytes).unwrap_or(usize::MAX);
        let mut total = 0usize;
        // Each requested output must appear once: returned or skipped. A set
        // keeps the check fast for a plan with many outputs.
        let requested: std::collections::BTreeSet<&str> =
            plan.outputs.iter().map(String::as_str).collect();
        let mut seen = std::collections::BTreeSet::new();
        for output in &self.outputs {
            if !requested.contains(output.path.as_str()) {
                return violation(format!("output {} was not asked for", shown(&output.path)));
            }
            if !seen.insert(output.path.as_str()) {
                return violation(format!("output {:?} appears twice", output.path));
            }
            if output.content.len() > per_file {
                return violation(format!(
                    "output {:?} holds {} bytes, over the bound of {per_file}",
                    output.path,
                    output.content.len()
                ));
            }
            total = total.saturating_add(output.content.len());
        }
        if total > total_cap {
            return violation(format!(
                "outputs hold {total} bytes, over the bound of {total_cap}"
            ));
        }
        for skipped in &self.skipped_outputs {
            if !requested.contains(skipped.path.as_str()) {
                return violation(format!(
                    "skipped output {} was not asked for",
                    shown(&skipped.path)
                ));
            }
            if skipped.reason.len() > MAX_SKIP_REASON_BYTES {
                return violation(format!(
                    "skipped output {:?} has a long reason",
                    skipped.path
                ));
            }
            if skipped.reason.contains('\0') {
                return violation(format!(
                    "skipped output {:?} has a NUL byte in its reason",
                    skipped.path
                ));
            }
            if !seen.insert(skipped.path.as_str()) {
                return violation(format!("output {:?} appears twice", skipped.path));
            }
        }
        if seen.len() != plan.outputs.len() {
            return violation(format!(
                "the outcome omits {} requested output(s)",
                plan.outputs.len() - seen.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use crate::sandbox::policy::SandboxPolicy;
    use crate::sandbox::request::SandboxRequest;

    pub fn outcome(status: RunStatus) -> SandboxOutcome {
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
        out.stdout_bytes = plan.limits.max_output_bytes;
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
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(_))
        ));
    }

    #[test]
    fn a_nul_in_a_skip_reason_breaks_the_bounds() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.skipped_outputs.push(SkippedOutput {
            path: "out.txt".into(),
            reason: "gone\0".into(),
        });
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(ref m)) if m.contains("NUL")
        ));
    }

    #[test]
    fn a_long_guest_path_is_cut_in_the_error() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(OutputFile {
            path: "x".repeat(100_000),
            content: Vec::new(),
        });
        let err = out.check_bounds(&plan).unwrap_err().to_string();
        assert!(err.len() < 200, "{} bytes", err.len());
    }

    #[test]
    fn an_output_that_the_plan_did_not_ask_for_breaks_the_bounds() {
        let plan = plan_with_output("out.txt");
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(OutputFile {
            path: "/etc/shadow".into(),
            content: b"root".to_vec(),
        });
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(_))
        ));
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
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(_))
        ));
    }

    #[test]
    fn impossible_exit_codes_and_signals_break_the_bounds() {
        let plan = SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", "true"))
            .unwrap();
        for status in [
            RunStatus::Exited { code: -1 },
            RunStatus::Exited { code: 256 },
            RunStatus::Signaled { signal: -9 },
            RunStatus::Signaled { signal: 65 },
        ] {
            assert!(
                matches!(
                    outcome(status).check_bounds(&plan),
                    Err(SandboxError::Protocol(_))
                ),
                "{status:?}"
            );
        }
        for status in [
            RunStatus::Exited { code: 255 },
            RunStatus::Signaled { signal: 0 },
            RunStatus::Signaled { signal: 64 },
            RunStatus::TimedOut,
        ] {
            assert!(outcome(status).check_bounds(&plan).is_ok(), "{status:?}");
        }
    }

    #[test]
    fn stream_counts_that_contradict_the_text_break_the_bounds() {
        let plan = plan_with_output("out.txt");
        let with = |text: &str, bytes: u64, truncated: bool| {
            let mut out = outcome(RunStatus::Exited { code: 0 });
            out.skipped_outputs.push(SkippedOutput {
                path: "out.txt".into(),
                reason: "missing".into(),
            });
            out.stdout = text.into();
            out.stdout_bytes = bytes;
            out.stdout_truncated = truncated;
            out.check_bounds(&plan)
        };
        let cap = plan.limits.max_output_bytes;
        assert!(with("hi\n", 3, false).is_ok());
        assert!(with("\u{fffd}", 1, false).is_ok(), "one invalid byte");
        let real = captured(cap, &"x".repeat(usize::try_from(cap).unwrap() + 10));
        assert!(with(&real.text, real.bytes, true).is_ok());
        let marker = format!("a{}b", crate::sandbox::capture::TRUNCATION_MARKER);
        for (text, bytes, truncated) in [
            ("hi\n", 0, false),
            ("hi\n", 99, false),
            ("", cap + 1, false),
            ("hi\n", cap + 1, true),
            (real.text.as_str(), cap, true),
            (marker.as_str(), cap + 10, true),
        ] {
            assert!(
                matches!(with(text, bytes, truncated), Err(SandboxError::Protocol(_))),
                "{text:?} {bytes} {truncated}"
            );
        }
    }

    /// The text that a real capture of `cap` bytes makes of `stream`.
    fn captured(cap: u64, stream: &str) -> crate::sandbox::capture::CapturedText {
        let mut capture =
            crate::sandbox::capture::StreamCapture::new(usize::try_from(cap).unwrap());
        capture.push(stream.as_bytes());
        capture.finish()
    }

    #[test]
    fn a_truncated_stream_holds_the_bytes_that_the_capture_kept() {
        let mut plan = plan_with_output("out.txt");
        let with = |plan: &ExecPlan, text: &str, bytes: u64| {
            let mut out = outcome(RunStatus::Exited { code: 0 });
            out.skipped_outputs.push(SkippedOutput {
                path: "out.txt".into(),
                reason: "missing".into(),
            });
            out.stderr = text.into();
            out.stderr_bytes = bytes;
            out.stderr_truncated = true;
            out.check_bounds(plan)
        };
        // An odd cap splits a character at each seam, and the capture drops
        // both parts. The bound allows that loss.
        for cap in [7, 9, 11, 64] {
            plan.limits.max_output_bytes = cap;
            for stream in ["\u{e9}".repeat(40), "\u{1f600}".repeat(40)] {
                let real = captured(cap, &stream);
                assert!(real.truncated);
                assert!(
                    with(&plan, &real.text, real.bytes).is_ok(),
                    "{cap} {real:?}"
                );
                let marker_only = crate::sandbox::capture::TRUNCATION_MARKER;
                assert!(
                    matches!(
                        with(&plan, marker_only, real.bytes),
                        Err(SandboxError::Protocol(_))
                    ),
                    "{cap}: a bare marker hides the kept bytes"
                );
            }
        }
    }

    #[test]
    fn each_requested_output_appears_exactly_once() {
        let plan = SandboxPolicy::default()
            .plan(
                &SandboxRequest::new("sh", "true")
                    .with_output("a.txt")
                    .with_output("b.txt"),
            )
            .unwrap();
        let file = |path: &str| OutputFile {
            path: path.into(),
            content: b"1".to_vec(),
        };
        let skip = |path: &str| SkippedOutput {
            path: path.into(),
            reason: "missing".into(),
        };
        // Returned and skipped at once, and `b.txt` left out.
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(file("a.txt"));
        out.skipped_outputs.push(skip("a.txt"));
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(ref m)) if m.contains("twice")
        ));
        // `b.txt` left out.
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(file("a.txt"));
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(ref m)) if m.contains("omits")
        ));
        // Each path once.
        let mut out = outcome(RunStatus::Exited { code: 0 });
        out.outputs.push(file("a.txt"));
        out.skipped_outputs.push(skip("b.txt"));
        assert!(out.check_bounds(&plan).is_ok());
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
        assert!(matches!(
            out.check_bounds(&plan),
            Err(SandboxError::Protocol(_))
        ));
    }
}
