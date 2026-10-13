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
//! `DESIGN-sandbox-activities.md` for the design record.

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
pub use outcome::{OutputFile, RunStatus, SandboxOutcome, SkippedOutput};
pub use policy::{
    IsolationLevel, NetworkPolicy, RuntimeSpec, SandboxLimits, SandboxPolicy, SizeBounds,
};
pub use request::{ExecPlan, PlanFile, RequestedLimits, SandboxFile, SandboxRequest};
pub use runtime::{RunOptions, SandboxBackend, SandboxJob, SandboxRuntime};

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
