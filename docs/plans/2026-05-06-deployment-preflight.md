# Deployment Preflight Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a read-only Harvest deployment preflight API and CLI command that catches production-readiness blockers before workflows are started.

**Architecture:** Implement preflight report generation as an additive `autumn-harvest-plugin` module used by `GET /admin/preflight`. The report aggregates runtime catalog metadata, shard read/migration visibility, schedule resolvability, worker coverage, DLQ access, retention visibility, and management auth-boundary state without mutating workflow, DAG, task queue, DLQ, or audit state. Add `harvest preflight` as a CLI GET command that renders a compact table by default and JSON when `--output json` is selected.

**Tech Stack:** Rust 2024, Axum, Diesel async, Postgres, clap, serde JSON, testcontainers-backed integration tests.

---

### Task 1: RED - CLI Contract

**Files:**
- Modify: `autumn-harvest-cli/tests/request_mapping.rs`
- Modify: `autumn-harvest-cli/src/lib.rs`
- Modify: `autumn-harvest-cli/src/main.rs`

**Steps:**
1. Add a failing request-mapping test proving `harvest preflight` maps to `GET /admin/preflight`.
2. Add a failing rendering test proving default output is a compact table, while `--output json` preserves the raw response shape.
3. Add a failing exit-code test proving `overall_status = warn` exits `2`, `fail` exits `1`, and `pass` exits `0`.
4. Run `cargo test -p autumn-harvest-cli preflight` and confirm the failures are for missing preflight support.
5. Implement the minimal clap command, rendering helper, and exit-code helper.
6. Re-run the targeted CLI tests.

### Task 2: RED - API Report Contract

**Files:**
- Create: `autumn-harvest-plugin/src/preflight.rs`
- Modify: `autumn-harvest-plugin/src/api.rs`
- Modify: `autumn-harvest-plugin/src/lib.rs`
- Create: `autumn-harvest-plugin/tests/preflight_integration.rs`

**Steps:**
1. Add failing integration tests for an all-green single-shard report and a non-dev admin API mounted without auth.
2. Add failing integration tests for one shard missing migrations and one unreachable shard.
3. Add failing integration tests for missing worker coverage, stale worker warning, and schedule rows referencing missing runtime registrations.
4. Run `cargo test -p autumn-harvest-plugin --test preflight_integration` and confirm the failures are for missing preflight types/route/report generation.
5. Implement the minimal report model and checker pipeline.
6. Re-run the targeted plugin integration tests.

### Task 3: Refactor and Docs

**Files:**
- Modify: `README.md`
- Modify: `examples/quickstart/README.md`
- Modify: `examples/billing-autumn-web/README.md`
- Modify: `examples/standalone-runner/README.md`

**Steps:**
1. Document `harvest preflight` as a production deploy gate in the root README, including exit codes.
2. Add one `harvest preflight` command to the quickstart and both advanced example run instructions.
3. Run `cargo fmt`.
4. Run targeted CLI and plugin tests.
5. Scan affected areas for TODO/FIXME/stubs and review the diff.
