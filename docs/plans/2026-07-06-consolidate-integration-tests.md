# Consolidate Integration Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Consolidate all integration tests in `autumn-harvest` and `autumn-harvest-cli` into a single integration test binary per crate to reduce build artifacts, build times, and linker issues on Windows.

**Architecture:** Create `tests/integration/mod.rs` in both crates, move all integration test files into `tests/integration/`, declare them as modules in `mod.rs`, and define a single `[[test]]` target named `integration` in each crate's `Cargo.toml`.

**Tech Stack:** Rust (Edition 2024), Cargo.

---

### Task 1: Checkout Consolidation Branch

**Files:**
- None

**Step 1: Check out consolidation branch**

Run: `git checkout -b consolidate-integration-tests`
Expected: Switched to a new branch 'consolidate-integration-tests'

**Step 2: Verify git status**

Run: `git status`
Expected: On branch consolidate-integration-tests, working tree clean

---

### Task 2: Consolidate `autumn-harvest` integration tests

**Files:**
- Create: `autumn-harvest/tests/integration/mod.rs`
- Modify: `autumn-harvest/Cargo.toml`
- Move: All 86 `.rs` files in `autumn-harvest/tests/` to `autumn-harvest/tests/integration/`

**Step 1: Move test files in `autumn-harvest/tests/`**

Create directory: `autumn-harvest/tests/integration`
In PowerShell, move all `.rs` files in `autumn-harvest/tests/` to `autumn-harvest/tests/integration/`:
```powershell
New-Item -ItemType Directory -Force -Path autumn-harvest/tests/integration
Get-ChildItem -Path autumn-harvest/tests/*.rs | Move-Item -Destination autumn-harvest/tests/integration/
```

**Step 2: Create `autumn-harvest/tests/integration/mod.rs`**

Create `autumn-harvest/tests/integration/mod.rs` with declarations for all 86 test modules:
```rust
mod activity_failure_tests;
mod activity_outcome_metrics_tests;
mod admission_gate_tests;
mod alert_pack_docs;
mod audit_tests;
mod build_routing_tests;
mod cache_delta_load_tests;
mod cancellation_tests;
mod child_fanout_tests;
mod child_policy_tests;
mod circuit_breaker_wiring_tests;
mod completion_callback_tests;
mod concurrency_key_tests;
mod context_headers_tests;
mod cross_workflow_cancel_tests;
mod cross_workflow_signal_tests;
mod dag_builder;
mod dag_mapping_tests;
mod dag_unified_tests;
mod debounce_tests;
mod delayed_start_tests;
mod det_check_tests;
mod event_batch_tests;
mod executor_span_tests;
mod external_completion_tests;
mod fanout_tests;
mod guardrail_catalog_tests;
mod havoc_reentrancy;
mod havoc_tests;
mod idempotency_tests;
mod integration_e2e;
mod macros_activity;
mod macros_collect;
mod macros_compile_fail;
mod macros_dag;
mod macros_query_handlers;
mod macros_webhook;
mod macros_workflow;
mod metrics_coverage;
mod metrics_integration;
mod metrics_rs_adapter;
mod nd_block_tests;
mod pause_tests;
mod payload_cap_tests;
mod payload_offload_db_tests;
mod payload_offload_replay_tests;
mod poison_pill_tests;
mod priority_tests;
mod query_deadlock;
mod query_tests;
mod queue_fairness_tests;
mod redrive_tests;
mod replay_canary_tests;
mod replay_tests;
mod replay_verifier_tests;
mod replayer_integration_tests;
mod replayer_tests;
mod retry_now_tests;
mod saga_tests;
mod schedule_decisions;
mod schedule_runs_tests;
mod schedule_to_close_tests;
mod scheduled_time_tests;
mod scheduler_auto_pause_tests;
mod scheduler_bounded_runs_tests;
mod scheduler_carryover_tests;
mod scheduler_catchup_tests;
mod scheduler_ha_tests;
mod security;
mod sharding_unit;
mod signal_tests;
mod signal_with_start_tests;
mod sla_breach_tests;
mod slot_tuner_tests;
mod sticky_routing_tests;
mod telemetry_span_tests;
mod transactional_activity_tests;
mod typed_stubs_tests;
mod updt_with_start_tests;
mod webhook_trigger_tests;
mod workflow_handle_tests;
mod workflow_logger_tests;
mod workflow_mutation_tests;
mod workflow_retry_tests;
mod workflow_task_timeout_tests;
mod workflow_test_env_tests;
```

**Step 3: Modify `autumn-harvest/Cargo.toml`**

Remove all separate `[[test]]` targets except for any benches or examples, and declare a single `integration` test:
```toml
[[test]]
name = "integration"
path = "tests/integration/mod.rs"
required-features = ["testing", "db"]
```
Keep benchmarks and examples targets untouched.

**Step 4: Commit changes for `autumn-harvest`**

Run:
```bash
git add autumn-harvest/Cargo.toml autumn-harvest/tests/
git commit -m "refactor(autumn-harvest): consolidate integration tests into single binary"
```

---

### Task 3: Consolidate `autumn-harvest-cli` integration tests

**Files:**
- Create: `autumn-harvest-cli/tests/integration/mod.rs`
- Modify: `autumn-harvest-cli/Cargo.toml`
- Move: All 4 `.rs` files in `autumn-harvest-cli/tests/` to `autumn-harvest-cli/tests/integration/`

**Step 1: Move test files in `autumn-harvest-cli/tests/`**

Create directory: `autumn-harvest-cli/tests/integration`
In PowerShell, move all `.rs` files in `autumn-harvest-cli/tests/` to `autumn-harvest-cli/tests/integration/`:
```powershell
New-Item -ItemType Directory -Force -Path autumn-harvest-cli/tests/integration
Get-ChildItem -Path autumn-harvest-cli/tests/*.rs | Move-Item -Destination autumn-harvest-cli/tests/integration/
```

**Step 2: Create `autumn-harvest-cli/tests/integration/mod.rs`**

Create `autumn-harvest-cli/tests/integration/mod.rs` declaring the 4 test modules:
```rust
mod batch_tests;
mod contract_coverage;
mod http_execution;
mod request_mapping;
```

**Step 3: Modify `autumn-harvest-cli/Cargo.toml`**

Add the `[[test]]` target configuration to `autumn-harvest-cli/Cargo.toml`:
```toml
[[test]]
name = "integration"
path = "tests/integration/mod.rs"
```

**Step 4: Commit changes for `autumn-harvest-cli`**

Run:
```bash
git add autumn-harvest-cli/Cargo.toml autumn-harvest-cli/tests/
git commit -m "refactor(autumn-harvest-cli): consolidate integration tests into single binary"
```

---

### Task 4: Verification

**Step 1: Compile the consolidated tests**

Run: `cargo test --workspace --tests --no-run`
Expected: Compiles with 0 compilation errors. Notice a much faster link phase since only one binary per crate is linked.

**Step 2: Run no-db tests**

Run: `cargo test -p autumn-harvest --no-default-features`
Expected: Tests compile and pass. The consolidated integration test binary is skipped because `testing` and `db` features are disabled.

**Step 3: Run the consolidated integration tests**

Run: `cargo test -p autumn-harvest --all-features` (excluding Docker-backed e2e tests if they require Docker and Docker is not running/available)
Expected: All tests pass.
