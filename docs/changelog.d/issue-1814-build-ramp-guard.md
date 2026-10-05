## Feature — metric-gated build-ramp abort and a `build_id` metric label (issue #1814)

A build ramp can now abort itself. The opt-in ramp guard compares the target
build of a ramp with its base build at each ramp step. The target build can
fail or ND-block too many more runs than the base build. The guard then clears
the ramp and writes an audit row, and the base build takes all new starts.
This is the AWS "one-box" pattern: an alarm scoped to the new code rolls it
back with no operator.

Turn it on with `HarvestPlugin::ramp_guard` and a
`ramp_guard::RampGuardConfig`. The default config is disabled, so a default
deployment runs no guard SQL. Spec: `docs/operations/build-ramp-guard.md`.

Design decisions:

- The signal comes from `harvest_workflow_executions`, grouped by
  `assigned_build_id`. It is fleet-wide and durable, and it needs no metrics
  backend. The window starts at the policy row's `updated_at`, which is the
  current ramp step. The counts merge over pools per ramp generation (queue,
  both builds and `ramp_id`), so one ramp's failures never count for another.
- Failure is `FAILED` or `TIMED_OUT`. Cancel and terminate are operator
  actions and do not count. ND-block is a `RUNNING` or `PAUSED` row with
  `nd_blocked_at` set. Canary probes do not count.
- The verdict uses the 95 % Wilson lower bound of the target rate. The
  threshold is an increase over the base rate, so a shared outage does not
  abort the ramp. Each build needs `min_samples` runs. A thin base sample near
  100 % gives no verdict.
- The abort is a compare-and-swap on the queue, both builds and the step, on
  every shard pool. It cannot clear a newer ramp or a newer step. A failed
  pool clear stays pending, and the next pass retries it with no new verdict.
  Each operator ramp has a `ramp_id`, the same on every pool. The clear
  adds it to the durable marker list `ramp_aborted` in the same UPDATE. A
  newer abort keeps the older markers. A marker records whether the abort
  was reported. A pass reports an abort whose marker stayed unreported past
  `report_grace`, with reason `unreported`. The recovery claim is a lease
  that covers the claimer's own writes,
  and a marker turns reported only after its audit row commits. A pass
  removes a reported marker once no pool holds its ramp. A trigger clears
  `ramp_id` when an `UPDATE` changes a ramp without a new id, so a replica
  from before the migration cannot reuse an old id. A base-build change
  gives an active ramp a fresh id. A fan-out stores one id per base and
  target. Ramp and policy
  writes with an id are idempotent, so a retry cannot split the identity. After
  a restart, the guard matches markers to ramps by id and base build, not by
  clocks, to finish a partial clear. A clear has a server-side timeout, so it cannot commit late. After a
  cancel, a pass starts no new clear.
- A replica reports an abort only after it cleared a pool itself. The first
  decisive clear in pool order elects the reporter, so racing replicas agree
  on one. The marker tells a guard clear from an operator change.
- The guard fails safe. A failed or slow read aborts nothing. Every read and
  write has a bound, and each read has a server-side `statement_timeout`.

Migration `20261003212318_harvest_ramp_guard_outcome_index` adds the partial
index `idx_harvest_we_ramp_guard_outcome` on `harvest_workflow_executions
(queue_name, assigned_build_id, created_at)`. Migration
`20261003235300_harvest_build_policy_ramp_id` adds the columns
`harvest_build_policies.ramp_id` and `ramp_aborted`, and a trigger that
clears a stale `ramp_id`. Migration
`20261004095020_harvest_ramp_abort_reports` adds the report ledger
`harvest_ramp_abort_reports`, which makes each abort report exactly-once.
None migrates data, adds a `WorkflowEvent` variant or affects replay.

`build_id` label: five families now carry the build of the worker that ran
the task. They are `harvest.workflow.terminal`, `harvest.activity.attempts`,
`harvest.workflow.nondeterministic_block`, `harvest.workflow.duration` and
`harvest.activity.duration`. An outcome that no worker code produced reports
`none`. The cap admits 16 distinct builds per process. A later build, or an id
over 128 bytes, reports `__other__`. A real build id equal to a sentinel, or
one that starts with `build:`, gets a `build:` prefix. So no two builds share a
series, and no build shares a sentinel. New `MetricsRecorder` methods with a
`_for_build` suffix carry the build. Their defaults call the old methods, so a
custom recorder needs no change.

New surface: the `ramp_guard` module (`RampGuardConfig`, `RampGuard`,
`evaluate`, `wilson_lower_bound`, `guard_once`, `run_ramp_guard`,
`abort_ramp`, `mark_abort_reported`, `claim_unreported_abort`,
`ramp_aborted_by_guard`), `build_routing::set_build_policy_with_ramp_id` and
`build_routing::ramp_generation_id`.
Also new: the audit operation `build_routing.ramp.auto_abort`,
the counter `harvest.build.ramp_aborted{queue, reason}` with a dashboard
panel, and `telemetry::{build_id_label, BuildIdLabelCap}`.

Tests: `tests/integration/ramp_guard_tests.rs` runs two real workers. Build B
takes 10 % of new starts and fails every run. The guard loop aborts the ramp
with one audit row, and new starts then go to build A. Seeded tests cover the
ND-block abort, the step window, canary exclusion, `TIMED_OUT`, two pools,
the step compare-and-swap and a promotion in progress. Unit tests cover the
evaluator, the Wilson bound, the config clamps, the label cap, both recorders
and the plugin boot wiring.
