//! Property-based (proptest) test suite for `autumn-harvest`.
//!
//! Single-entry-point test target (mirrors `tests/integration/mod.rs`). See
//! [`prop_config`] for the bounded-runtime / `PROPTEST_CASES` conventions.
//!
//! Suite layout:
//! - Pure, non-`db`-gated suites (run under `--no-default-features`):
//!   `policy_props`, `queue_fairness_props`, `task_duration_props`,
//!   `completion_trigger_props`, `completion_callback_props`, `event_serde_props`.
//! - `db`-gated suites (only compiled with the `db` feature, because their
//!   target modules live in `#[cfg(feature = "db")]` modules): `build_routing_props`,
//!   `dlq_props`.

#![allow(clippy::unwrap_used, clippy::missing_panics_doc)]

mod prop_config;

mod completion_callback_props;
mod completion_trigger_props;
mod event_serde_props;
mod policy_props;
mod queue_fairness_props;
mod task_duration_props;

#[cfg(feature = "db")]
mod build_routing_props;
#[cfg(feature = "db")]
mod dlq_props;
