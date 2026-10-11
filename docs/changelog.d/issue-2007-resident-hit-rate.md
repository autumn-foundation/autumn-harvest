## Feature — Resident-state hit rate (issue #2007)

Resident workflow state (#1798) skips replay on a warm decision. It accepts
a narrow set of suspensions only. Two new counters show how often a
decision uses that path, and why it does not:

- `harvest.workflow.resident_hit{workflow, queue}`: the decision resumes the
  parked workflow and replays nothing.
- `harvest.workflow.resident_miss{workflow, queue, reason}`: the decision
  replays cold.

While resident workflows are on, each decision that drives the workflow
records exactly one sample. The worker records no sample while sticky
routing or `resident_workflows` is off. A local-activity re-drive inside one
decision does not count again. The worker records a hit when the parked
future takes its result, before the drive, so a drive that times out still
counts.

**Reasons.** `reason` is `ResidentMiss::as_str`, a closed set of twelve
values: `cold`, `multi_await`, `race`, `mutex`, `hot_swap`, `command`,
`condition`, `signal_handler`, `context`, `key_changed`, `delta` and
`failure`. `docs/telemetry.md` defines
each one.

- `capture` finds a reason when a cycle suspends. The reason travels in the
  cache entry to the next decision. `ResidentWorkflow::capture` and
  `WorkflowContext::resident_blocker` now return the reason instead of
  `None`.
- A resume-time reason comes from `ResumeDeclined` through
  `From<&ResumeDeclined> for ResidentMiss`.
- A race reason wins over the shape reason. A race shows its `race:{seq}`
  marker, a reserved timer id or a `CancelRaceLosers` command. An open
  `ctx.race()` on a later cycle sets the new per-cycle `race_waiting` flag.
  A raw `select!` shows none of these, so it counts as `multi_await`.

The set of suspensions that stay resident is unchanged.

**Internals.** `WorkflowCache` entries and `DriveResult` hold
`Result<ResidentWorkflow, ResidentMiss>` instead of
`Option<ResidentWorkflow>`. `resume_with` splits into `wake` and `drive`.
The public test entry points
`resident::start` and `ResidentWorkflow::resume` keep their signatures.
`MetricsRecorder` gains `record_workflow_resident_hit` and
`record_workflow_resident_miss`. Both default to no-ops, so an existing
recorder needs no change. The metrics-rs adapter bridges both.

**Docs.** `docs/telemetry.md` lists both counters, their labels and a new
"Resident hit rate" section. It also lists `cache_hit` and `cache_miss`,
which were missing. `docs/sticky-routing.md` points at the new counters.
The starter dashboard gains a hit-ratio panel and a misses-by-reason panel.

**Measurements.** `docs/rnd/2026-10-11-resident-hit-rate.md` records one
run of the e2e bench `throughput` scenario and two runs of the real
`agent_loop`. The bench now reports the resident counts in its notes. The
new example crate `examples/agent-loop-hit-rate` runs `agent_loop` on a
Postgres worker with an offline model. CI lints it and runs its unit tests.

No migration. No new `WorkflowEvent` variant. No change to
`harvest_events`.

**CI.** trunk-dev failed to compile the integration tests: #2103 removed
`WorkflowResetRequest::refuse_erased_source`, and #2104 still set it. This
change drops the three stale field lines.

**Tests.**

- `resident.rs`: each capture reason (join, four race shapes, a mutex
  wait, a held mutex, a child workflow, a push handler, a condition), a race
  after its first cycle, work after a settled race, the decline mapping, and
  the closed label set.
- `cache.rs`: an entry keeps the capture reason. A plain insert is `cold`.
- `metrics_rs_adapter.rs`: the bridge emits one hit series and one miss
  series per reason.
- `tests/integration/sticky_default_tests.rs` runs real workers. Warm
  decisions count hits. A two-signal delta counts `delta`. An eviction
  counts `cold`. With sticky routing or resident workflows off, the worker
  records no sample. A join counts `multi_await` and a race counts `race`. Each
  decision records one sample.
