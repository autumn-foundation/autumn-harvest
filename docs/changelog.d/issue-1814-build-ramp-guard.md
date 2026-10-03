## Feature — metric-gated build-ramp abort and a `build_id` metric label (issue #1814)

A build ramp can now abort itself. The opt-in ramp guard compares the ramp's
target build with its base build at each ramp step. When the target build's
failure rate or ND-block rate exceeds the base build's by more than a
threshold, the guard clears the ramp and writes an audit row. The base build
then takes all new starts. This is the AWS "one-box" pattern: an alarm scoped
to the new code rolls it back with no operator.

Turn it on with `HarvestBuilder::ramp_guard` or `HarvestPlugin::ramp_guard`
and a `ramp_guard::RampGuardConfig`. The default config is disabled, so a
default deployment runs no guard SQL. Spec:
`docs/operations/build-ramp-guard.md`.

Design decisions:

- The signal comes from `harvest_workflow_executions`, grouped by
  `assigned_build_id`. It is fleet-wide and durable, and it needs no metrics
  backend. The window starts at the policy row's `updated_at`, which is the
  current ramp step.
- Failure is `FAILED` or `TIMED_OUT`. Cancel and terminate are operator
  actions and do not count. ND-block is a `RUNNING` row with `nd_blocked_at`
  set. Canary probes do not count.
- The threshold is an increase over the base build, so a shared outage does
  not abort the ramp. A verdict needs `min_samples` target-build runs.
- The abort is a compare-and-swap on `(queue, build_id, target_build_id)` on
  every shard pool. It cannot clear a newer ramp to another target. Only the
  pass whose swap changed a row writes the audit row, so many replicas can run
  the guard.
- The guard fails safe. A failed or slow read aborts nothing.

`build_id` label: `harvest.workflow.terminal`, `harvest.activity.attempts`,
`harvest.workflow.nondeterministic_block`, `harvest.workflow.duration` and
`harvest.activity.duration` now carry the build of the worker that ran the
task. A path with no worker reports `none`. `telemetry::build_id_label` caps
the values at 16 distinct builds per process, and longer ids than 128 bytes,
with `__other__` for the rest. New `MetricsRecorder` methods with a
`_for_build` suffix carry the build. Their defaults call the old methods, so
a custom recorder needs no change.

New surface: `ramp_guard` module (`RampGuardConfig`, `evaluate`,
`guard_once`, `run_ramp_guard`, `abort_ramp`), the audit operation
`build_routing.ramp.auto_abort`, the counter
`harvest.build.ramp_aborted{queue, reason}` with a dashboard panel, and
`telemetry::{build_id_label, BuildIdLabelCap}`. No migration and no new
`WorkflowEvent` variant.

Tests: `tests/integration/ramp_guard_tests.rs` runs two real workers. Build B
takes 10 % of new starts and fails every run, and the guard loop aborts the
ramp with one audit row. Further tests cover a healthy ramp, too few samples
and the compare-and-swap. Unit tests cover the evaluator, the config clamps,
the label cap and the label sets of both recorders.
