//! `cfg(loom)` synchronization-primitive shim.
//!
//! Under a normal build this is a byte-for-byte re-export of the standard
//! library's [`Arc`](std::sync::Arc) / [`Mutex`](std::sync::Mutex) /
//! [`MutexGuard`](std::sync::MutexGuard): production code is completely
//! unaffected.
//!
//! Under `RUSTFLAGS="--cfg loom"`, these resolve to
//! [`loom`](https://docs.rs/loom)'s instrumented equivalents. Then
//! [`loom::model`] can exhaustively explore the thread interleavings of the
//! modules that opt in. Only the dedicated `tests/loom_models.rs`
//! model-checking target sets that flag, never `cargo test`.
//!
//! ## Scope is deliberately contained
//!
//! Only the two modules with a genuinely concurrent in-process `Mutex`-guarded
//! data structure route their primitives through this alias:
//!
//! * [`crate::circuit_breaker`] — `Mutex<HashMap<String, BreakerState>>` shared
//!   between the worker dispatch path and the management API.
//! * [`crate::sessions`] — `SessionSlotRegistry = Arc<Mutex<HashSet<SessionId>>>`
//!   shared between concurrent session-acquire task claims.
//!
//! The rest of the crate keeps using `std::sync` directly, so the loom
//! surface never sweeps the whole crate (see `docs/testing/loom.md`).
//!
//! ## Poison handling stays identical
//!
//! loom's `Mutex::lock()` returns the same `std::sync::LockResult` type as std.
//! It never poisons; it always yields `Ok`. So the existing poison-tolerant
//! `.unwrap_or_else(std::sync::PoisonError::into_inner)` call sites compile and
//! behave identically under both configurations. The lock call itself needs no
//! `cfg`-gating.

// `pub use`, not `pub(crate) use`. The `loom_sync` module is itself private
// (`mod loom_sync;`), so these re-exports are crate-internal anyway. Clippy's
// `redundant_pub_crate` prefers `pub` in that position.
#[cfg(loom)]
pub use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(loom))]
pub use std::sync::{Arc, Mutex, MutexGuard};
