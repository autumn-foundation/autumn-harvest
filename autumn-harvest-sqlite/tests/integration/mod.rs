#![allow(dead_code)]

// Single integration-test binary for `autumn-harvest-sqlite`. Cargo compiles
// every top-level `tests/*.rs` into its own binary (a full crate+deps link
// each); collapsing the suite into one `[[test]]` target with the per-area
// files as `mod`s cuts CI compile time and disk. Mirrors the core
// `autumn-harvest` crate's `tests/integration/mod.rs` convention.

mod activity_context;
mod activity_info_features;
mod activity_start_to_close;
mod append_only_guard;
mod atomicity;
mod cancel_terminate;
mod cancellable_timers;
mod composition;
mod continue_as_new;
mod cross_backend;
mod docs_start_contract;
mod drive_clock_refresh;
mod durability;
mod execution_timeout;
mod fleet_fault_isolation;
mod freeze_activity_defaults;
mod history_caps;
mod local_activities;
mod payload_caps;
mod payload_depth;
mod portable_activity;
mod portable_fixture;
mod race_activities;
mod race_fairness;
mod reopen_consistency;
mod retention;
mod retry_policy_and_failure_fidelity;
mod reuse_policy;
mod review_fixes;
mod review_fixes_48d54b2;
mod runtime_robustness;
mod schedules;
mod signal_ingest;
mod signal_timeout;
mod signal_with_start;
mod single_writer_lock;
mod subsecond_timer;
mod unknown_execution_id;
mod unsupported_terminal;
mod updates;
mod visibility;
mod workflow_info_features;
mod workflow_mutex;
mod workflow_queries;
mod workflow_retry;
