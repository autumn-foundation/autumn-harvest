//! `cfg(shuttle)` async-primitive shim (issue #1800).
//!
//! A normal build re-exports the tokio and tokio-util types. Production code
//! does not change.
//!
//! Under `RUSTFLAGS="--cfg shuttle"`, the same names resolve to the
//! `shuttle-tokio` and `shuttle-tokio-util` types. Shuttle then controls every
//! await on them. Only the `shuttle` CI job and the command in
//! `docs/testing/shuttle.md` set that flag.
//!
//! Only [`crate::slot_tuner`], [`crate::heartbeat`] and
//! [`crate::adaptive_limit`] import from this shim. The rest of the crate
//! keeps its direct tokio imports. See `docs/testing/shuttle.md`.
//!
//! The shim also maps the std `Mutex` and `AtomicUsize` of
//! [`crate::adaptive_limit`]. Shuttle can then switch tasks at each lock and
//! at each atomic access.
//!
//! `select!` is in the shim because tokio's `select!` picks a branch with its
//! own random number generator. Shuttle cannot replay a schedule that depends
//! on a generator that it does not control.

// `pub use`, not `pub(crate) use`: the module itself is private, and clippy's
// `redundant_pub_crate` prefers `pub` here. Same as `loom_sync.rs`.
#[cfg(shuttle)]
pub use shuttle::sync::atomic::AtomicUsize;
#[cfg(shuttle)]
pub use shuttle::sync::{Mutex, MutexGuard};
#[cfg(shuttle)]
pub use shuttle_tokio::select;
#[cfg(shuttle)]
pub use shuttle_tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
#[cfg(shuttle)]
pub use shuttle_tokio::task::{JoinHandle, spawn};
#[cfg(shuttle)]
pub use shuttle_tokio::time::sleep;
#[cfg(shuttle)]
pub use shuttle_tokio_util::sync::CancellationToken;

#[cfg(not(shuttle))]
pub use std::sync::atomic::AtomicUsize;
#[cfg(not(shuttle))]
pub use std::sync::{Mutex, MutexGuard};
#[cfg(not(shuttle))]
pub use tokio::select;
#[cfg(not(shuttle))]
pub use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
#[cfg(not(shuttle))]
pub use tokio::task::{JoinHandle, spawn};
#[cfg(not(shuttle))]
pub use tokio::time::sleep;
#[cfg(not(shuttle))]
pub use tokio_util::sync::CancellationToken;
