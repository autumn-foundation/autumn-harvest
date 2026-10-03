# Build ramp guard (issue #1814)

A build ramp (issue #604) sends a percentage of new starts on a queue to a
target build. The rest go to the base build. An operator aborts a ramp with
one command: `DELETE /admin/build-routing/ramp/{queue_name}`.

The ramp guard aborts a ramp automatically. It compares the target build with
the base build at each ramp step. When the target build is worse by more than
a threshold, the guard clears the ramp and writes an audit row. This is the
"one-box" deployment pattern: the alarm is scoped to the new code, and the
rollback needs no operator.

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

`HarvestBuilder::ramp_guard` takes the same config. The guard runs in the API
process, beside the load-shed sampler.

| Setting | Default | Range | Meaning |
|---------|---------|-------|---------|
| `interval` | 30 s | 1 s to 1 h | Time between two guard passes. |
| `min_samples` | 20 | 1 or more | Target-build runs needed for a verdict. |
| `max_failure_rate_increase` | 0.05 | 0 to 1 | Allowed failure-rate increase over the base build. |
| `max_nd_block_rate_increase` | 0.05 | 0 to 1 | Allowed ND-block-rate increase over the base build. |

The setters clamp a value outside its range. A `NaN` rate keeps the default.

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
| `nd_blocked` | `state = 'RUNNING'` and `nd_blocked_at IS NOT NULL`. |

Cancelled and terminated runs are operator actions, so they do not count as
failures.

The two rates are:

- failure rate = `failed / (completed + failed)`
- ND-block rate = `nd_blocked / started`

A rate with a zero denominator is 0.

## Verdict

`ramp_guard::evaluate` is a pure function. It checks the rates in this order:

1. When the target build has at least `min_samples` settled runs, and its
   failure rate is more than `max_failure_rate_increase` above the base
   build, the verdict is **abort** with reason `failure_rate`.
2. When the target build has at least `min_samples` started runs, and its
   ND-block rate is more than `max_nd_block_rate_increase` above the base
   build, the verdict is **abort** with reason `nd_block_rate`.
3. When the target build has fewer than `min_samples` settled runs and fewer
   than `min_samples` started runs, the verdict is **insufficient data**.
4. Otherwise the verdict is **healthy**.

The threshold is an increase over the base build, not an absolute rate. A
shared outage that fails both builds does not abort the ramp.

## Abort

An abort clears `target_build_id` and `ramp_percent` on every shard pool. The
base build then takes all new starts. In-flight runs keep their assigned
build, as with a manual clear.

The clear is a compare-and-swap. It changes a row only when the row still
ramps the same base build to the same target build. A guard verdict about an
old target therefore cannot clear a newer ramp that an operator set.

After the clear, the guard does these steps once per abort:

- It writes one audit row. The operation is `build_routing.ramp.auto_abort`,
  the target is `build_routing` / the queue name, the actor is `system` and
  the route is `background.ramp_guard`. The summary holds the reason, both
  rates, the target build and the sample counts.
- It increments `harvest.build.ramp_aborted{queue, reason}`.
- It logs a warning.

Only a pass whose compare-and-swap changed a row does these steps. Many
replicas can run the guard, and only the first one to clear a ramp audits
it.

## Failure behaviour

The guard fails safe: when it cannot read, it does not abort.

- A pass with any failed shard read changes nothing.
- The reads of one pass share one bound of one interval.
- A failed clear or audit write logs a warning. The next pass retries a ramp
  that a shard still holds.

## `build_id` metric label

The worker labels these metric families with its own build id (`build_id`):

| Metric | Kind |
|--------|------|
| `harvest.workflow.terminal` | task success and failure |
| `harvest.activity.attempts` | task success and failure |
| `harvest.workflow.nondeterministic_block` | ND block |
| `harvest.workflow.duration` | latency |
| `harvest.activity.duration` | latency |

The label value is the build of the worker that ran the task. A series from a
path with no worker, for example the execution-timeout scanner, has
`build_id="none"`. Every series in a family therefore has the same label set.

The `metrics-rs` adapter labels all five families. The built-in scrape
endpoint (`HarvestPlugin::with_metrics_scrape`) has only the two duration
families, and it labels both.

### Cardinality cap

`telemetry::build_id_label` caps the label values. A process admits the first
16 distinct build ids (`MAX_BUILD_ID_LABELS`). A later build id, or one longer
than 128 bytes, gets `__other__`. An empty build id gets `none`. A worker
process normally reports one build, its own, so the cap holds only active
builds.

Compare the builds of a ramp with a query such as:

```promql
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome="failed"}[5m]))
/
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome!="continued_as_new"}[5m]))
```
