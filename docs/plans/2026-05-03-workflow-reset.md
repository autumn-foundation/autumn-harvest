# Workflow Reset Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a single-execution workflow reset primitive that forks history at a valid event boundary and resumes the new execution without mutating the original event rows.

**Architecture:** Implement reset as a core single-shard transaction in `autumn-harvest`, then expose it through the plugin API and CLI. The source execution is sealed terminal with a reset marker, carried events are appended to a new execution, open side effects are cancelled or drained, and a new workflow task is enqueued.

**Tech Stack:** Rust 2024, Diesel async, Postgres, Axum management API, clap CLI.

---

### Task 1: Reset Domain Model And Validator

**Files:**
- Modify: `autumn-harvest/src/event.rs`
- Create: `autumn-harvest/src/reset.rs`
- Modify: `autumn-harvest/src/lib.rs`
- Test: `autumn-harvest/src/reset.rs`

**Steps:**
1. Add failing tests for `WorkflowResetFork` / `WorkflowResetTerminated` event names and reset-boundary validation.
2. Implement the two append-only event variants at the end of `WorkflowEvent`.
3. Add a validator that tracks unresolved activities, timers, child workflows, local activities, external activities, and updates.
4. Verify unit tests fail before implementation and pass after implementation.

### Task 2: Transactional Reset Operation

**Files:**
- Modify: `autumn-harvest/src/reset.rs`
- Modify: `autumn-harvest/src/queue.rs`
- Create: `autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql`
- Create: `autumn-harvest/migrations/20260503000000_harvest_workflow_reset/down.sql`

**Steps:**
1. Add DB tests covering fork copy, terminal-source rejection, child-source rejection, signal buffering, and side-effect teardown.
2. Add migration support for `TERMINATED` workflow executions plus cancelled task/external-task rows.
3. Implement `reset_workflow_execution` and `preview_workflow_reset`.
4. Verify reset inserts a new execution on the same shard, carries raw event JSON, appends reset markers, drains side effects, and enqueues the new workflow task.

### Task 3: API And CLI

**Files:**
- Modify: `autumn-harvest-plugin/src/api.rs`
- Modify: `autumn-harvest-cli/src/lib.rs`
- Test: `autumn-harvest-cli/tests/request_mapping.rs`
- Test: `autumn-harvest-plugin/tests/workflow_reset_integration.rs`

**Steps:**
1. Add failing CLI request-mapping tests for reset and dry-run.
2. Add plugin integration coverage for success, invalid boundary, buffered signal, source terminal teardown, and terminal-source conflict.
3. Wire `POST /workflows/{id}/reset` with optional dry-run query support.
4. Map invalid reset points to `400 Bad Request` JSON and terminal sources to `409 Conflict`.

### Task 4: Replay Helper And Runbook

**Files:**
- Modify: `autumn-harvest/src/testing.rs`
- Modify: `README.md`

**Steps:**
1. Add failing replay-helper tests for `replay_with_reset(history, reset_to_event_id)`.
2. Implement the helper by truncating history through the reset boundary and appending an informational reset marker.
3. Add the operator runbook entry for stack inspection, dry-run, reset, and validation.

### Task 5: Verification

**Commands:**
- `cargo fmt --all`
- `cargo test -p autumn-harvest --features testing reset`
- `cargo test -p autumn-harvest-cli`
- `cargo test -p autumn-harvest-plugin workflow_reset`
- `cargo check --all-targets --all-features`

**Completion Criteria:**
- New behavior has tests that failed before implementation.
- No reset path mutates existing event rows.
- Source execution is terminal and source open side effects are drained.
- CLI and README show the operator path without reading source.
