#![allow(dead_code)]

// Single integration-test binary for `autumn-harvest-sqlite`. Cargo compiles
// every top-level `tests/*.rs` into its own binary (a full crate+deps link
// each); collapsing the suite into one `[[test]]` target with the per-area
// files as `mod`s cuts CI compile time and disk. Mirrors the core
// `autumn-harvest` crate's `tests/integration/mod.rs` convention.

mod activity_info_features;
mod activity_start_to_close;
mod atomicity;
mod cancellable_timer_unsupported;
mod cross_backend;
mod drive_clock_refresh;
mod durability;
mod freeze_activity_defaults;
mod payload_caps;
mod race_activities;
mod reopen_consistency;
mod retry_policy_and_failure_fidelity;
mod review_fixes;
mod review_fixes_48d54b2;
mod runtime_robustness;
mod signal_timeout;
mod subsecond_timer;
mod workflow_info_features;
