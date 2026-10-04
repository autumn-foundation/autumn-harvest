# Harvest SLO Burn-Rate Pack

This optional pack defines three reference SLOs for a Harvest fleet. It
alerts on how fast each SLO spends its error budget (issue #1816). It
follows the multi-window, multi-burn-rate method in the Google SRE Workbook,
chapter "Alerting on SLOs".

| File | Contents |
|---|---|
| [`slo-pack-v0.1.0.rules.yml`](slo-pack-v0.1.0.rules.yml) | Prometheus recording and alerting rules. |
| [`slo-pack-v0.1.0.test.yml`](slo-pack-v0.1.0.test.yml) | `promtool` unit tests. Each alert has a case that fires and a case that stays silent. |
| [`../runbooks/harvest-alerts.md`](../runbooks/harvest-alerts.md) | One runbook section for each SLI. |

The [starter pack](README.md) pages on static 5-minute thresholds. This
pack is different: it pages when the long-term objective is at risk. Use
the two packs together. The starter pack names the failing part. This pack
tells you whether users feel it.

## The SLIs and Their Targets

Each SLI is a ratio of bad events to all events, summed over the fleet.

| SLI | Good event | Bad event | Objective | Source series |
|---|---|---|---|---|
| `workflow_task` | A workflow task (one executor cycle) completes, suspends, or continues as new. | The cycle ends the run as `failed`, or the task times out. | 99.9% | `harvest_workflow_duration_count{status}`, `harvest_workflow_task_timeout_total` |
| `schedule_to_start` | A task starts within 5 s of becoming eligible. | A task waits more than 5 s. | 99% | `harvest_queue_schedule_to_start_bucket`, `_count` |
| `canary` | A synthetic liveness probe completes. | A probe fails or times out. | 99% | `harvest_canary_success_total`, `harvest_canary_failure_total` |

Notes on each SLI:

- **`workflow_task`.** A failed cycle includes a failure that the workflow
  returns on purpose. A timeout is a cycle that the worker abandons after
  `WorkerConfig::workflow_task_timeout`. A missing series counts as zero.
  So a fleet where every task times out still pages.
- **`schedule_to_start`.** The histogram covers workflow and activity
  tasks. It has no label that separates the two. The 5 s bound must be a
  bucket bound. The matcher accepts `le="5"` and `le="5.0"`, because
  Prometheus 3 stores the second form.
- **`canary`.** Probes are few: about one a minute for each queue and shard.
  At 99.9%, one failed probe in an hour can page. The objective is 99% for
  that reason. The canary emits only `harvest.canary.*`, so probes never
  change the `workflow_task` SLI.

When an SLI has no traffic, its ratio is `0 / 0`. That value is `NaN`, and
`NaN` is never above a threshold. So an idle fleet does not page. The
starter pack and the canary detect a fleet that stops.

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

## Requirements

- Use the `metrics-rs` adapter. The plugin scrape endpoint does not emit
  the task-timeout counter, the canary counters, or histogram buckets. See
  [`docs/telemetry.md`](../telemetry.md).
- Configure bucket bounds for `harvest_queue_schedule_to_start`, with 5 s
  as one bound. Without buckets, the `schedule_to_start` SLI has no data.
- Enable the synthetic liveness canary for the `canary` SLI.

## Install

1. Add `slo-pack-v0.1.0.rules.yml` to `rule_files` in your Prometheus
   configuration.
2. Route the `severity` label in Alertmanager.
3. Confirm that the recorded ratios have data, for example
   `harvest:workflow_task_error:ratio_rate5m`.

## Tune the SLO Target

Each objective is one recording rule at the top of its group:

```yaml
- record: harvest:slo_objective:ratio
  expr: vector(0.999)
  labels:
    slo: workflow_task
```

To change the target, change the value in `vector(...)`. All three alerts
for that SLI read it. The fixture cases assume the shipped targets, so
update them too. Keep these points in mind:

- A higher target gives a smaller budget. Then fewer errors page.
- Do not set a target above the reliability of the systems below Harvest.
- Low traffic makes ratios noisy. If an SLI pages on single events, lower
  the target or lengthen the windows.
- To change the 5 s bound, edit the `le=~` matcher in the five
  `schedule_to_start` records. Use a bucket bound that your exporter
  emits.
- To remove a workflow type from the `workflow_task` SLI, add a
  `workflow!~"..."` matcher to each `harvest_workflow_duration_count` and
  `harvest_workflow_task_timeout_total` selector.
- For a per-queue SLO, add `by (queue)` to each `sum`. Then replace each
  `vector(0)` with the other addend times zero, so that labels still match.

## Costs and Limits

- The `3d` records read three days of samples on each evaluation. That is
  the most costly part of the pack. Raise the group `interval` if your
  Prometheus server is under load.
- The SLIs aggregate the whole fleet. A small queue that fails can stay
  under a fleet-wide threshold. The starter pack covers single queues.

## Test the Pack

```sh
promtool check rules docs/alerts/slo-pack-v0.1.0.rules.yml
promtool test rules docs/alerts/slo-pack-v0.1.0.test.yml
```

CI runs both commands in the `lint` job with a pinned `promtool`. The
guard suite `autumn-harvest/tests/integration/slo_pack_docs.rs` pins the
rest: the burn-rate pairs, the objectives in this file, the runbook links,
and the metric names.
