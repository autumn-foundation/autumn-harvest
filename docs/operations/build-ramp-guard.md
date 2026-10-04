# Build ramp guard (issue #1814)

A build ramp (issue #604) sends a percentage of new starts on a queue to a
target build. The rest go to the base build. An operator aborts a ramp with
one command: `DELETE /admin/build-routing/ramp/{queue_name}`.

The ramp guard aborts a ramp automatically. It compares the target build with
the base build during the current ramp step. When the target build is worse
by more than a threshold, the guard clears the ramp and writes an audit row.
This is the "one-box" deployment pattern: the alarm is scoped to the new
code, and the rollback needs no operator.

The guard is opt-in. A deployment with no guard config runs no guard SQL.

## Turn it on

```rust,ignore
use autumn_harvest::ramp_guard::RampGuardConfig;

let plugin = HarvestPlugin::new().ramp_guard(
    RampGuardConfig::new()
        .with_interval(Duration::from_secs(30))
        .with_min_samples(20)
        .with_max_failure_rate_increase(0.05)
        .with_max_nd_block_rate_increase(0.05),
);
```

The plugin boot and the embedding boot spawn the guard loop in the API
process, beside the load-shed sampler. `HarvestBuilder::ramp_guard` stores the
same config, but a bare builder spawns nothing. Without the plugin, run
`ramp_guard::run_ramp_guard` yourself, or drive `ramp_guard::RampGuard::pass`
from your own loop.

| Setting | Default | Range | Meaning |
|---------|---------|-------|---------|
| `interval` | 30 s | 1 s to 1 h | Time between two guard passes. |
| `min_samples` | 20 | 1 or more | Runs of each build needed for a verdict. |
| `max_failure_rate_increase` | 0.05 | 0 to 1 | Allowed failure-rate increase over the base build. |
| `max_nd_block_rate_increase` | 0.05 | 0 to 1 | Allowed ND-block-rate increase over the base build. |

The setters clamp a value outside its range. A `NaN` rate keeps the value
that the config had before. A threshold of 1 turns its check off, because no
rate can be more than 1 above another rate.

## Signal

The signal comes from `harvest_workflow_executions`, not from a metrics
backend. The table is fleet-wide and durable, so every replica sees the same
data. One query per shard pool groups the runs of the ramp by
`assigned_build_id`. It counts only runs that meet all of these conditions:

- The run is on the ramp's queue.
- The run started at or after the ramp step. The step is the policy row's
  `updated_at`, which `set_build_ramp` sets.
- The run is not a canary probe.

For each build the query returns these counts:

| Count | Definition |
|-------|------------|
| `started` | All counted runs. |
| `completed` | `state = 'COMPLETED'`. |
| `failed` | `state IN ('FAILED', 'TIMED_OUT')`. |
| `nd_blocked` | `state IN ('RUNNING', 'PAUSED')` and `nd_blocked_at IS NOT NULL`. |

Cancelled and terminated runs are operator actions, so they do not count as
failures. An operator can pause a blocked run, so a paused blocked run still
counts as blocked.

The two rates are:

- failure rate = `failed / (completed + failed)`
- ND-block rate = `nd_blocked / started`

A rate with a zero denominator is 0.

The migration `20261003212318_harvest_ramp_guard_outcome_index` adds the
index `idx_harvest_we_ramp_guard_outcome` on
`(queue_name, assigned_build_id, created_at)`. The query reads only the runs
of the current step. Each read runs with a server-side `statement_timeout`, so
a slow scan stops on the server too.

### Limits of the signal

- A run counts against the build it was **assigned** at start. A target
  worker can replay a base run through `harvest_build_compat`. An ND block in
  that run counts against the base build. The
  `harvest.workflow.nondeterministic_block` metric labels the block with the
  worker build instead.
- A child workflow and a continue-as-new run keep the build of their parent.
  One bad parent with many children therefore adds many correlated samples.
- Any change of the policy row starts a new step. That includes a new
  percentage, a repeated `set_build_ramp` and a new base build. The evidence
  of the old step then no longer counts.

## Verdict

`ramp_guard::evaluate` is a pure function. It uses the 95 % Wilson score
lower bound of the target rate, not the point rate. With few runs the bound is
low, so a few unlucky runs cannot abort a healthy ramp.

The function checks the rates in this order:

1. The failure check needs `min_samples` settled runs on each build. Then,
   when the target lower bound is more than `max_failure_rate_increase` above
   the base failure rate, the verdict is **abort** with reason
   `failure_rate`.
2. The ND-block check needs `min_samples` started runs on each build. Then,
   when the target lower bound is more than `max_nd_block_rate_increase`
   above the base ND-block rate, the verdict is **abort** with reason
   `nd_block_rate`.
3. When neither check has enough runs, the verdict is **insufficient data**.
4. Otherwise the verdict is **healthy**.

The threshold is an increase over the base build, not an absolute rate. A
shared outage that fails both builds does not abort the ramp.

The base build needs `min_samples` runs too. At or near 100 % the base build
gets almost no new runs, so its rate is noise. The guard then gives no
verdict. Watch a full cutover with the `build_id` metrics instead. A ramp
whose target is its own base build is a promotion in progress, and the guard
skips it.

## Abort

An abort clears `target_build_id` and `ramp_percent` on every shard pool that
holds the ramp. The base build then takes all new starts. In-flight runs keep
their assigned build, as with a manual clear.

The clear is a compare-and-swap on the queue, the base build, the target
build and the step (`updated_at`). It changes a row only when the row still
holds the step that the verdict used. A verdict about an old target or an old
step therefore cannot clear a newer ramp that an operator set.

After the clear, the guard does these steps once per abort:

- It writes one audit row. The operation is `build_routing.ramp.auto_abort`.
  The target is `build_routing` and the queue name. The actor is `system`,
  and the route is `background.ramp_guard`.
- The audit summary holds the reason, both rates, the target lower bound,
  both builds and the sample counts.
- It increments `harvest.build.ramp_aborted{queue, reason}`.
- It logs a warning.

Many replicas can run the guard. A replica reports an abort only after it
cleared a pool itself, so a failed clear never reports a change that did not
happen. A replica that lost the clear on the first pool that holds the ramp
does not report, because another replica owns that report. Normally one
replica audits each abort.

## Failure behaviour

The guard fails safe: when it cannot read, it does not abort.

- A pass with any failed shard read changes nothing.
- All reads of one pass must end within one bound. The bound is the interval
  or 60 s (`MAX_READ_TIMEOUT`), whichever is less.
- Each clear and each audit write has the same bound. A clear runs with a
  server-side `lock_timeout` and `statement_timeout`, so a slow clear fails
  and rolls back on the server. It cannot commit after the guard gave up.
- The client waits twice the bound for a clear. When the client still gives
  up, the outcome is unknown. A retry that then finds the row changed counts
  the change as the guard's own clear and reports it. An extra audit row is
  better than an abort with none.
- A cancel stops a pass during its read. A pass that has started to clear
  runs to its end, so a clear and its audit row are not split.
- A clear that fails on one pool stays pending. The next pass of the same
  guard retries it with no new verdict. The audit row of such an abort has
  status `failed` and names the pending pools by index.
- When no clear of a pass succeeds, the guard reports nothing yet. It reports
  the abort when a retry clears a pool.
- A failed audit write logs a warning and does not undo the clear.
- A guard keeps its pending clears in memory. Each operator ramp also has a
  `ramp_id`, which the API fan-out writes to every shard pool. Each finished
  clear copies it to the abort marker `harvest_build_policies.ramp_aborted_id`
  in the same `UPDATE`, so the marker cannot be lost. After a restart, a pool
  can still hold a ramp whose `ramp_id` matches a marker on another pool. The
  guard then clears that ramp with no new verdict and no new audit row. The
  match uses ids, not database clocks, so clock skew between pools does not
  matter. An operator ramp set after the abort has a new `ramp_id`, so the
  guard does not clear it. A ramp set before the migration has no id, and the
  guard cannot finish its partial abort after a restart.
- After a cancel, a pass lets the clear in flight finish and starts no new
  clear. Shutdown therefore waits for one bounded clear at most, plus the
  audit write of a clear that the pass made.

## `build_id` metric label

The worker labels these metric families with its own build id (`build_id`):

| Metric | Kind |
|--------|------|
| `harvest.workflow.terminal` | task success and failure |
| `harvest.activity.attempts` | task success and failure |
| `harvest.workflow.nondeterministic_block` | ND block |
| `harvest.workflow.duration` | latency |
| `harvest.activity.duration` | latency |

The label value is the build of the worker that ran the task. A series for an
outcome that no worker code produced has `build_id="none"`. Examples are the
execution-timeout scanner, a cancel through a signal and a race-loser cancel.
Every series in a family therefore has the same label set.
`harvest.workflow.non_determinism` already had the label, and it now goes
through the same cap.

The `metrics-rs` adapter labels all five families. The built-in scrape
endpoint (`HarvestPlugin::with_metrics_scrape`) has only the two duration
families, and it labels both.

### Cardinality cap

`telemetry::build_id_label` caps the label values. A process admits the first
16 distinct build ids that it sees (`MAX_BUILD_ID_LABELS`). It never evicts a
build. A later build id, or one longer than 128 bytes, gets `__other__`. An
empty build id gets `none`. A real build id equal to `none` or `__other__` gets
`build:none` or `build:__other__`, so the two sentinels keep one meaning.

A worker keeps one build id for its whole life, and a process normally hosts
one build. The cap therefore holds the active builds in practice. A process
that starts workers with more than 16 builds reports the rest as `__other__`.

A custom recorder that forwards to another recorder must forward the
`*_for_build` methods too. Otherwise the inner recorder reports `none`.

Compare the builds of a ramp with a query such as:

```promql
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome="failed"}[5m]))
/
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome!="continued_as_new"}[5m]))
```
