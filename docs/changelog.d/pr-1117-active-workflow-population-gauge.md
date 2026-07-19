## Phase 3.51 — Active-workflow population gauge (issue #770)

**Implemented.** New continuously-sampled gauge `harvest.workflow.active`
(`METRIC_WORKFLOW_ACTIVE`) reports the count of currently-active workflow
executions, labeled by `workflow` (workflow-type name) and `state` (bounded to
exactly two values: `running` / `paused`). It is the live in-progress
population signal the metric catalogue lacked — a steady rise while
`harvest.workflow.terminal` stays flat is the signature of a leak or backlog
(starts outpacing completions), distinct from a healthy burst that later
drains.

**Source & sampling.** A dedicated background sampler
`worker::spawn_workflow_active_sampler` (a near-copy of
`spawn_history_oversized_sampler`, gated on `metrics.is_enabled()` since it
issues SQL) runs on the worker's `poll_interval` and calls
`worker::sample_active_workflow_counts`, a shard-local
`SELECT workflow_name, state, COUNT(*) … WHERE state IN ('RUNNING','PAUSED')
GROUP BY workflow_name, state`. Counts are summed across every shard of the
worker's `ShardedDbPool` for a fleet-wide gauge. A read failure on any shard
skips the whole tick (never false-clears the gauge during an outage); a
`(workflow, state)` pair present last tick but absent this tick is zero-filled
so a drained pair falls back to `0` rather than going stale. The emit/skip/
zero-fill decision is the pure, unit-tested
`worker::compute_active_gauge_emissions`.

**Bounded cardinality.** The `state` label can only ever be `running` or
`paused` — enforced by construction via the bounded `worker::ActiveWorkflowState`
enum (`from_db_str` maps only `RUNNING`/`PAUSED`, everything else → `None` and
is dropped with a debug log as defense-in-depth). Per ADR-0001 §7, `execution.id`
is never a label. Total series is bounded by `workflow_types × 2`. `PAUSED`
runs (issue #383) count under `state="paused"`; nd-blocked runs (issue #603)
stay `RUNNING` and count under `state="running"` — no special handling.

**Three-touchpoint metric recipe:** (1) `telemetry.rs` —
`pub const METRIC_WORKFLOW_ACTIVE` + `pub const METRIC_LABEL_STATE` + the
no-op-default `MetricsRecorder::record_workflow_active(workflow, state: &str,
count)` trait method; (2) `metrics_rs_adapter.rs` — the `gauge!` bridge with
`workflow`/`state` labels; (3) a real capturing-recorder adapter test asserting
the registered gauge key, labels, and `set` value (`5.0`).

**No new `WorkflowEvent` variant, no migration, no shard-routing/replay change**
— read-only over `harvest_workflow_executions.state`, no write path, no event
schema surface.

**Anti-drift registries:** dashboard panel "Active workflows by type & state"
(`docs/dashboards/starter-pack-v0.1.0.json`, `max by (workflow, state)`),
`DASHBOARD_PROMETHEUS_SERIES` + `SERIES_LABELS`
(`dashboard_pack_docs.rs`), alert rule `harvest_workflow_population_leak`
(`docs/alerts/starter-pack-v0.1.0.json`) with a full runbook section
(`docs/runbooks/harvest-alerts.md`), `REQUIRED_ALERTS` +
`STABLE_PROMETHEUS_METRICS` (`alert_pack_docs.rs`), README mapping row,
ADR-0001 §7 catalogue row, `docs/telemetry.md` call-site + labels tables,
`metrics_coverage.rs` `RecordingMetrics` coverage, and a
`linux autumn-harvest integration` manifest row for the DB test.

**Tests, TDD red→green:** R1 telemetry const/no-op unit tests; R2 bounded-enum
`from_db_str`/`as_str` unit tests + R5 `compute_active_gauge_emissions`
zero-fill/skip-on-read-failure unit tests (both in `worker.rs`, run under the
all-features lib step); R3 capturing-recorder gauge-value bridge test
(`metrics_rs_adapter.rs`); R4 DB integration test
(`tests/integration/active_workflow_gauge_tests.rs`) asserting the grouped read
counts RUNNING/PAUSED per type and excludes terminal executions — executed
against a real local Postgres 16.
