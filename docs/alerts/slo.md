# Harvest SLO Burn-Rate Pack

This optional pack defines three reference SLOs for a Harvest fleet. It
alerts on how fast each SLO spends its error budget (issue #1816). It
follows the multi-window, multi-burn-rate method in the Google SRE Workbook,
chapter "Alerting on SLOs".

| File | Contents |
|---|---|
| [`slo-pack-v0.1.0.rules.yml`](slo-pack-v0.1.0.rules.yml) | Prometheus recording and alerting rules. |
| [`slo-pack-v0.1.0.test.yml`](slo-pack-v0.1.0.test.yml) | `promtool` unit tests. Each alert has a case that fires and a case that stays silent. |
| [`../runbooks/harvest-alerts.md`](../runbooks/harvest-alerts.md) | One runbook section for each SLI, and one for the objective check. |

The [starter pack](README.md) pages on static 5-minute thresholds. This
pack is different: it pages when the long-term objective is at risk. Use
the two packs together. The starter pack names the failing part. This pack
tells you whether users feel it.

## The SLIs and Their Targets

Each SLI is a ratio of bad events to all events, summed over the fleet.

| SLI | Good event | Bad event | Objective | Source series |
|---|---|---|---|---|
| `workflow_task` | A workflow task (one executor cycle) completes, suspends, or continues as new. | The cycle ends the run as `failed`, or the task times out. | 99.9% | `harvest_workflow_duration_count{status}`, `harvest_workflow_task_timeout_total` |
| `schedule_to_start` | A task starts within 5 s of becoming eligible. | A task waits more than 5 s. | 99% | `harvest_queue_schedule_to_start_bucket` |
| `canary` | A synthetic liveness probe completes. | A probe fails or times out. | 99% | `harvest_canary_success_total`, `harvest_canary_failure_total` |

Notes on each SLI:

- **`workflow_task`.**
  - A failed cycle includes a failure that the workflow returns on purpose.
  - A timeout is a cycle that runs longer than
    `WorkerConfig::workflow_task_timeout`, so the worker abandons it.
  - A missing series counts as zero. So a fleet where every task times out
    still pages.
  - Canary probe timeouts are excluded. They belong to the `canary` SLI.
  - Some failures add no sample, so this SLI does not see them: a
    non-determinism block, a contained panic before its retry budget runs
    out, and a deadlock retry. Use `harvest_workflow_non_determinism`, the
    `harvest.workflow.panic` counter, and the stalled-workflow view of
    `GET /api/harvest/admin/status` for those.
- **`schedule_to_start`.**
  - The histogram covers workflow and activity tasks, including canary
    probes. It has no label that separates them.
  - A task adds a sample only when it starts. A task that never starts is
    not visible here. `harvest_queue_uncovered` covers that case.
  - Both sides of the ratio come from buckets: the 5 s bucket and the
    `+Inf` bucket. A worker without buckets still exports `_count`, so
    `_count` would make its tasks look slow.
  - The 5 s bound must be a bucket bound. Prometheus 2 keeps the exporter
    form `le="5"`. Prometheus 3 changes it to `le="5.0"` at scrape time.
    The matcher accepts both.
- **`canary`.**
  - A probe runs once each interval for each probe queue and writable
    shard. At the recommended 30 s interval, a queue on one shard sends
    120 probes an hour.
  - At 99.9%, two failed probes in an hour can page. The objective is 99%
    for that reason.

When an SLI has no traffic, its ratio is `0 / 0`. That value is `NaN`, and
`NaN` is never above a threshold. So an idle fleet does not page.
`harvest_no_active_workers` and `harvest_queue_uncovered` detect a fleet
that stops. The `stale` field of `GET /api/harvest/admin/canary` also
shows it.

## The Alerts

Each SLI has three alerts. The budget period is 30 days. The burn rate is
the error ratio divided by the budget (1 minus the objective). A burn rate
of 1 spends the budget in exactly 30 days.

| Alert suffix | Burn rate | Long window | Short window | Budget spent | Severity |
|---|---|---|---|---|---|
| `_burn_1h` | 14.4 | 1h | 5m | 2% | `page` |
| `_burn_6h` | 6 | 6h | 30m | 5% | `page` |
| `_burn_3d` | 1 | 3d | 6h | 10% | `ticket` |

An alert fires only when both windows are above the threshold. The long
window proves that the burn is large. The short window proves that the
burn continues, so the alert resets soon after a fix.

The full alert names are `harvest_slo_<sli>_burn_1h`, `_burn_6h`, and
`_burn_3d`. Each alert carries the labels `severity`, `slo`, and `window`.
Route `severity="page"` to your pager and `severity="ticket"` to your
ticket queue.

A fast burn also trips the `6h` tier, so one incident can page twice. To
stop that, add an Alertmanager inhibit rule:

```yaml
inhibit_rules:
  - source_matchers: ['window="1h"']
    target_matchers: ['window="6h"']
    equal: [slo]
```

`harvest_slo_objective_invalid` is a ticket. It fires when an objective
record is missing, or when its value is not above 0 and below 1. Without
it, a bad tune makes the burn alerts silent or stuck on, with no error.

## Requirements

- Use the `metrics-rs` adapter. The plugin scrape endpoint does not emit
  the schedule-to-start histogram, the task-timeout counter, or the canary
  counters. See [`docs/telemetry.md`](../telemetry.md).
- Configure bucket bounds for `harvest_queue_schedule_to_start`, with 5 s
  as one bound. Without buckets, the `schedule_to_start` SLI has no data.
- Enable the synthetic liveness canary for the `canary` SLI.

## Install

1. Add `slo-pack-v0.1.0.rules.yml` to `rule_files` in your Prometheus
   configuration.
2. Change each `runbook_url` annotation to an absolute URL. The shipped
   value is a path in this repository, so a pager cannot open it.
3. Route the `severity` label in Alertmanager.
4. Confirm that the recorded ratios have data, for example
   `harvest:workflow_task_error:ratio_rate5m`.

## Tune the SLO Target

Each objective is one recording rule at the top of its group:

```yaml
- record: harvest:slo_objective:ratio
  expr: vector(0.999)
  labels:
    slo: workflow_task
```

To change the target, change the value in `vector(...)`. Write a fraction,
such as `0.995`, not a percentage. All three alerts for that SLI read it.
The fixture cases assume the shipped targets, so update them too. Keep
these points in mind:

- A higher target gives a smaller budget, so a smaller number of errors
  causes a page.
- Do not set a target above the reliability of the systems that Harvest
  uses, such as PostgreSQL and the services that your activities call.
- Low traffic makes ratios noisy. If an SLI pages on single events, lower
  the target.
- The burn rates assume a 30-day period. For another period or window,
  use this formula: burn rate = period × budget share ÷ long window. For
  example, 720h × 2% ÷ 1h = 14.4.
- To change the 5 s bound, edit the `le=~` matcher in the five
  `schedule_to_start` records. Use a bucket bound that your exporter
  emits.
- To remove a workflow type from the `workflow_task` SLI, add a
  `workflow!~"..."` matcher to each `harvest_workflow_duration_count` and
  `harvest_workflow_task_timeout_total` selector. A timeout whose lookup
  fails has `workflow="unknown"`, so a name matcher does not remove it.

### Per-queue SLOs

For a per-queue SLO, add `by (queue)` to each `sum`. Rename the records to
a `queue:` level, for example `queue:workflow_task_error:ratio_rate5m`, and
update the alerts.

The `workflow_task` and `canary` records also need a change to each
`or vector(0)`. `vector(0)` has no labels, so it cannot fill a gap for one
queue. Use the other series times zero instead. For the `workflow_task`
numerator, with `F` for the `failed` selector and `T` for the timeout
selector:

```promql
(sum by (queue) (rate(F[5m])) or sum by (queue) (rate(T[5m])) * 0)
+ (sum by (queue) (rate(T[5m])) or sum by (queue) (rate(F[5m])) * 0)
```

The `schedule_to_start` records need `by (queue)` only.

## Costs and Limits

- The `3d` records read three days of samples on each evaluation. That is
  the most costly part of the pack. Add an `interval`, for example
  `interval: 5m`, to a group if your Prometheus server is under load.
- The SLIs aggregate the whole fleet. A small queue that fails can stay
  under a fleet-wide threshold. The starter pack covers single queues.
- The `metrics-rs` adapter creates a series at its first event. `rate()`
  cannot see that first step, so the first error on a new label set in
  each process does not count.
- A cycle can, in rare cases, add a good sample and then also time out.
  It then counts twice.
- If more than one series holds the same objective, as with a Thanos
  ruler that adds replica labels, the alerts read the largest value.

## Test the Pack

```sh
promtool check rules docs/alerts/slo-pack-v0.1.0.rules.yml
promtool test rules docs/alerts/slo-pack-v0.1.0.test.yml
```

CI runs both commands in the `lint` job with a pinned `promtool`. The
guard suite `autumn-harvest/tests/integration/slo_pack_docs.rs` pins the
rest: the burn-rate pairs, the objectives in this file, the runbook links,
and the metric names.
