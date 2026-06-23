# Deploy-Time Replay Canary Swarm Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all logic, security, database, performance, and documentation issues identified in the Replay Canary Code Review.

**Architecture:** We will introduce a formal `canary_mode` flag to `WorkflowContext` that bypasses strict replay mismatches when history is exhausted, letting running coroutines naturally suspend. DB connections will be dropped before CPU-heavy replay begins, concurrency will be throttled at 20 using a semaphore, and errors in the join loop will be mapped to detailed failures rather than short-circuiting. Axum routers will be secured via proper RBAC authorization checks and admin-gating for all routes under `/admin/*`.

**Tech Stack:** Rust, Axum, Tokio, Diesel, PostgreSQL

---

### Task 1: Shard Database Schema Migration

**Files:**
- Modify: [up.sql](file:///c:/Users/markm/autumn-harvest/autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql)
- Modify: [down.sql](file:///c:/Users/markm/autumn-harvest/autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/down.sql)

**Step 1: Write the failing test**
N/A (database index migration verified via DB migrations running in testcontainers).

**Step 2: Run test to verify it fails**
N/A

**Step 3: Write minimal implementation**
Append to [up.sql](file:///c:/Users/markm/autumn-harvest/autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql):
```sql
CREATE INDEX IF NOT EXISTS idx_harvest_we_canary_order
    ON harvest_workflow_executions (created_at DESC, id DESC)
    WHERE state = 'RUNNING';
```
Append to [down.sql](file:///c:/Users/markm/autumn-harvest/autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/down.sql):
```sql
DROP INDEX IF EXISTS idx_harvest_we_canary_order;
```

**Step 4: Run test to verify it passes**
Run: `cargo test --test replay_canary_tests`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/down.sql
git commit -m "migration: add idx_harvest_we_canary_order index for running executions query"
```

---

### Task 2: Implement Canary Replay Logic & Context Settings (C1, C2)

**Files:**
- Modify: [replay.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/replay.rs:945-950)
- Modify: [context.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/context.rs)
- Modify: [executor.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/executor.rs)

**Step 1: Write the failing test**
Verify strict replay on incomplete history fails in non-canary mode but passes when canary mode is enabled.
(Failing test will be written in `replay_canary_tests.rs`).

**Step 2: Run test to verify it fails**
Run: `cargo test --test replay_canary_tests`
Expected: FAIL

**Step 3: Write minimal implementation**
1. Add `len(&self) -> usize` method to `HistoryMatcher` in [replay.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/replay.rs):
```rust
    /// Total number of events in history.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.events.len()
    }
```

2. Add `canary_mode: bool` field to `WorkflowContext` in [context.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/context.rs), initializing it to `false` in `for_replay_with_state_and_history_policy` and constructor helpers.
3. Define `for_replay_canary_with_state` in `WorkflowContext` in [context.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/context.rs):
```rust
    #[must_use]
    pub fn for_replay_canary_with_state(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
    ) -> Self {
        let mut ctx = Self::for_replay_with_state(exec_id, events, state);
        ctx.strict_replay = true;
        ctx.canary_mode = true;
        ctx
    }
```
4. Bypass strict replay check in `check_strict_replay_no_match` in [context.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/context.rs):
```rust
    fn check_strict_replay_no_match(&self, actual_event: &str) -> HarvestResult<()> {
        if self.strict_replay {
            if self.canary_mode && self.match_history(|m| m.position() >= m.len()) {
                return Ok(());
            }
            return Err(self.nd_error(
                format!("early completion mismatch: expected <end of history>, got {actual_event}"),
                self.match_history(|m| i32::try_from(m.position()).ok()),
                None,
                None,
            ));
        }
        Ok(())
    }
```
5. Implement `run_workflow_canary` in [executor.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/executor.rs) which behaves similarly to `run_workflow_strict` but calls `WorkflowContext::for_replay_canary_with_state` and returns `WorkflowOutcome::Failed` on timeout if `ctx.history_has_unconsumed_events()` is `true`.

**Step 4: Run test to verify it passes**
Run: `cargo test --test replay_canary_tests`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest/src/replay.rs autumn-harvest/src/context.rs autumn-harvest/src/executor.rs
git commit -m "feat: implement canary_mode bypass and run_workflow_canary"
```

---

### Task 3: Refactor Replayer to be Clone-less and Throttled (C2, C3, D1, D2, D4, S4)

**Files:**
- Modify: [testing.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/src/testing.rs)

**Step 1: Write the failing test**
A test validating canary execution under DB load and handling corrupt execution history without aborting the query.

**Step 2: Run test to verify it fails**
Run: `cargo test --test replay_canary_tests`
Expected: FAIL

**Step 3: Write minimal implementation**
1. Annotate `ReplayCanaryOptions` with `#[serde(default)]` so all fields are optional.
2. In `run_canary`, enforce `options.sample_size = options.sample_size.min(1000);` to cap the load.
3. Update `outcome_to_report` in `testing.rs` to take `outcome: WorkflowOutcome` and `canary_mode: bool` without taking `events: &[WorkflowEvent]`. If `canary_mode` is `true` and the outcome is `WorkflowOutcome::Suspended`, return `ReplayStatus::ReplaySucceeded`.
4. Refactor `try_parse_non_determinism` to read `expected`, `actual`, and `event_index` from `NonDeterministicDetails` if available, bypassing history search and cloning.
5. In `replay_from_snapshot` and `replay_from_events`, pass `snapshot.events` and `events` respectively by value/move to `run_workflow_strict` / `run_workflow_canary`.
6. Implement `replay_canary_snapshot(&self, snapshot: HistorySnapshot) -> ReplayReport` in `WorkflowReplayer`.
7. Rewrite `collect_json_files` and `replay_fixture_file` to use `tokio::fs::read_dir` and `tokio::fs::read_to_string` instead of synchronous `std::fs`.
8. In `run_canary`, throttle the concurrency to 20 using `tokio::sync::Semaphore`. Acquire and drop the database connection *inside* the semaphore task before running CPU-bound replay. Catch database, loading, and deserialization errors, mapping them to `ReplayStatus::WorkflowFailed { error, event_index: 0 }` inside the join loop.

**Step 4: Run test to verify it passes**
Run: `cargo test --test replay_canary_tests`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest/src/testing.rs
git commit -m "refactor: optimize replayer allocations, scope connections, throttle concurrency, and isolate failures"
```

---

### Task 4: API security, admin routes, and auditing (S1, S2, S3)

**Files:**
- Modify: [api.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest-plugin/src/api.rs)

**Step 1: Write the failing test**
A test in `preflight_integration.rs` or `replay_canary_integration.rs` showing that an authenticated user who is not an admin fails the check in non-dev profiles, and that accessing admin endpoints without admin authorization fails.

**Step 2: Run test to verify it fails**
Run: `cargo test --test preflight_integration`
Expected: FAIL

**Step 3: Write minimal implementation**
1. Modify `has_harvest_admin_access` in [api.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest-plugin/src/api.rs) to perform RBAC checks in non-dev profiles (e.g. checking `is_harvest_admin`, `is_admin`, or `role` == "admin"/"harvest_admin" session values).
2. In the router mount point of `management_api_routes`, gate all endpoints under `/admin/*` (preflight, shards health, version usage, version retirement check, retention, retention run now, concurrency, rate limits, circuits, circuit details) with `.route_layer(require_admin.clone())`.
3. In `run_replay_canary_handler`, record `status` of the audit log as `STATUS_FAILED` if `report.verdict` is `CanaryVerdict::Fail`.

**Step 4: Run test to verify it passes**
Run: `cargo test --test replay_canary_integration`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest-plugin/src/api.rs
git commit -m "sec: apply RBAC checks, admin-gate all admin endpoints, and audit failed verdicts"
```

---

### Task 5: Contract Coverage & Documentation (T2, T3)

**Files:**
- Modify: [contract_coverage.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest-cli/tests/contract_coverage.rs)
- Modify: [api-contract.json](file:///c:/Users/markm/autumn-harvest/docs/api-contract.json)

**Step 1: Write the failing test**
Run `cargo test -p autumn-harvest-cli --test contract_coverage` to see it check that all body fields are documented.

**Step 2: Run test to verify it fails**
Run: `cargo test -p autumn-harvest-cli --test contract_coverage`
Expected: FAIL (missing canary body fields test or undocumented fields).

**Step 3: Write minimal implementation**
1. Add `canary_body_fields_are_documented` test to [contract_coverage.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest-cli/tests/contract_coverage.rs):
```rust
#[test]
fn canary_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "canary",
        "--sample-size",
        "20",
        "--workflow-name",
        "billing",
        "--queue",
        "critical",
    ]);
}
```
2. Update the contract documentation for `POST /admin/workflows/replay-canary` success response fields in [api-contract.json](file:///c:/Users/markm/autumn-harvest/docs/api-contract.json) to detail nested structures:
  - `"details"` is documented as an array of `CanaryFailureDetail` objects.
  - `"summary_by_type"` is documented as a map from workflow name to `CanaryTypeSummary`.

**Step 4: Run test to verify it passes**
Run: `cargo test -p autumn-harvest-cli --test contract_coverage`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest-cli/tests/contract_coverage.rs docs/api-contract.json
git commit -m "test: add CLI body field documentation tests and expand contract nested type schemas"
```

---

### Task 6: Add negative integration tests for failures (T1)

**Files:**
- Modify: [replay_canary_tests.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest/tests/replay_canary_tests.rs)
- Modify: [replay_canary_integration.rs](file:///c:/Users/markm/autumn-harvest/autumn-harvest-plugin/tests/replay_canary_integration.rs)

**Step 1: Write the failing test**
Add a negative canary test where history has non-deterministic changes.

**Step 2: Run test to verify it fails**
Run: `cargo test --test replay_canary_tests`
Expected: FAIL

**Step 3: Write minimal implementation**
1. Insert divergent event history in `replay_canary_tests.rs` (e.g. inserting a `WorkflowStarted` then a mismatching `ActivityScheduled` event, and asserting the verdict is `CanaryVerdict::Fail` and `details` contains the correct mismatch details).
2. Insert a divergent event history in `replay_canary_integration.rs`, call the `POST /admin/workflows/replay-canary` API, and assert `verdict == "fail"`.

**Step 4: Run test to verify it passes**
Run: `cargo test --test replay_canary_tests` and `cargo test --test replay_canary_integration`
Expected: PASS

**Step 5: Commit**
```bash
git add autumn-harvest/tests/replay_canary_tests.rs autumn-harvest-plugin/tests/replay_canary_integration.rs
git commit -m "test: add negative tests for canary replay non-determinism failures"
```
