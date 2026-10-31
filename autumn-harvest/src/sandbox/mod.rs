//! Sandboxed code activities: run code that an agent wrote.
//!
//! An agent sends a [`SandboxRequest`]: a runtime name, the code, files,
//! stdin and limits. The [`SandboxRuntime`] checks it against the operator
//! [`SandboxPolicy`], runs it on a [`SandboxBackend`], and returns a
//! [`SandboxOutcome`]. A non-zero exit or a timeout is an outcome, not a
//! failure, so the agent can read stderr and fix its code.
//!
//! # Backends
//!
//! - `FirecrackerBackend` (Linux) runs each request in a new, single-use
//!   Firecracker microVM. The microVM has its own guest kernel, no network
//!   device and a read-only root file system. This is the security boundary
//!   for hostile code.
//! - `ProcessBackend` (Linux) runs the code as a host process. The process
//!   gets resource limits, a private work directory and no host environment.
//!   By default it also gets an empty network namespace. It shares the host
//!   kernel, so it is for development and tests only. The default policy
//!   refuses it.
//!
//! The guest agent inside each microVM runs the plan with the same process
//! executor. The microVM is the boundary, and the executor limits are a
//! second layer inside it.
//!
//! # Durability
//!
//! A sandbox run is an ordinary activity. History records the request as the
//! activity input and the outcome as the activity result. Replay reads the
//! recorded outcome and never runs the code again. A retry runs the code
//! again in a new sandbox, so code with side effects outside the sandbox must
//! use `HARVEST_IDEMPOTENCY_KEY`.
//!
//! See `docs/sandbox-activities.md` for the guide and
//! `docs/design/DESIGN-sandbox-activities.md` for the design record.

pub mod activity;
pub mod capture;
#[cfg(target_os = "linux")]
pub mod cgroup;
pub mod error;
#[cfg(target_os = "linux")]
pub mod exec;
#[cfg(target_os = "linux")]
pub mod firecracker;
pub mod guest;
pub mod outcome;
pub mod policy;
#[cfg(target_os = "linux")]
pub mod process;
pub mod protocol;
pub mod request;
pub mod runtime;

pub use activity::{
    ATTEMPT_ENV, IDEMPOTENCY_KEY_ENV, SandboxActivities, SandboxActivity, run_sandbox_activity,
};
pub use error::SandboxError;
pub use outcome::{
    MISSING_OUTPUT_REASON, NOT_A_DIRECTORY_REASON, OutputFile, RunStatus, SandboxOutcome,
    SkippedOutput,
};
pub use policy::{
    IsolationLevel, NetworkPolicy, RuntimeSpec, SandboxLimits, SandboxPolicy, SizeBounds,
};
pub use request::{ExecPlan, PlanFile, RequestedLimits, SandboxFile, SandboxRequest};
pub use runtime::{RunOptions, SandboxBackend, SandboxJob, SandboxRuntime};

/// The longest timeout that the sandbox keeps: one year. A longer one means
/// no bound. The sum of a few such timeouts still fits the engine clock.
pub const MAX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

/// `timeout`, at most [`MAX_TIMEOUT`].
pub(crate) fn bounded(timeout: std::time::Duration) -> std::time::Duration {
    timeout.min(MAX_TIMEOUT)
}

/// The latest deadline that the sandbox sets: 30 years ahead, as Tokio sets
/// for a deadline that never comes. A longer wait means no bound.
const FAR_FUTURE: std::time::Duration = std::time::Duration::from_secs(30 * 365 * 24 * 60 * 60);

/// The instant `wait` after now. A wait past [`FAR_FUTURE`] counts as no
/// bound, so a configured `Duration::MAX` cannot overflow the `Instant`.
pub(crate) fn deadline_after(wait: std::time::Duration) -> std::time::Instant {
    std::time::Instant::now() + wait.min(FAR_FUTURE)
}

/// [`deadline_after`] on the Tokio clock.
pub(crate) fn tokio_deadline_after(wait: std::time::Duration) -> tokio::time::Instant {
    tokio::time::Instant::now() + wait.min(FAR_FUTURE)
}

/// Serde helpers that write bytes as standard base64 text.
pub(crate) mod b64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Write `bytes` as base64.
    ///
    /// # Errors
    ///
    /// Returns the error of the serializer.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    /// Read base64 text as bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for text that is not standard base64.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}
