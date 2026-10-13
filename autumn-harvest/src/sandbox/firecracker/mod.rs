//! The Firecracker backend: each run in a new, single-use microVM.
//!
//! - [`config`]: the VM document and the guest kernel arguments.
//! - [`jailer`]: the jailer command line and the jail layout.
//! - [`api`]: a small client for the Firecracker API socket.
//! - [`vsock`]: the host side of the vsock `CONNECT` handshake.
//! - [`launcher`]: start, stop and sweep microVMs.
//! - [`pool`]: a warm pool of booted microVMs.
//! - [`backend`]: the [`crate::sandbox::SandboxBackend`] that ties them.
//! - [`posture`]: version and host checks before the first run.
//!
//! Each microVM boots a read-only root file system with the guest agent as
//! init. It has no network device, no metadata service and no serial
//! console. The host talks to the guest agent over vsock only.

pub mod api;
pub mod backend;
pub mod config;
#[cfg(test)]
pub(crate) mod fake;
pub mod jailer;
pub mod launcher;
pub mod pool;
pub mod posture;
pub mod vsock;

pub use backend::{BackendSettings, FirecrackerBackend};
pub use config::{GuestImage, MachineSpec};
pub use jailer::JailerConfig;
pub use launcher::{BootMethod, Confinement, FirecrackerLauncher, LauncherConfig, SnapshotFiles};
pub use posture::PosturePolicy;
