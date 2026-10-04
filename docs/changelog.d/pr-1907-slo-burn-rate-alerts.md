## Observability — SLO definitions and burn-rate alerts (issue #1816)

**New optional pack.** `docs/alerts/slo-pack-v0.1.0.rules.yml` defines three
reference SLOs and pages on error-budget burn. It uses the multi-window,
multi-burn-rate method from the Google SRE Workbook.

| SLI | Objective | Source series |
|---|---|---|
| `workflow_task` | 99.9% | `harvest_workflow_duration_count{status}`, `harvest_workflow_task_timeout_total` |
| `schedule_to_start` | 99% within 5 s | `harvest_queue_schedule_to_start_bucket` |
| `canary` | 99% | `harvest_canary_{success,failure}_total` |

- Each SLI records its error ratio over 5m, 30m, 1h, 6h, and 3d.
- Each SLI has three alerts: 14.4x over 1h/5m (page), 6x over 6h/30m
  (page), and 1x over 3d/6h (ticket).
- Each objective is one `harvest:slo_objective:ratio` record, so a tune is
  one line.
- A missing series counts as zero, so a fleet where every workflow task
  times out still pages.
- The bucket matcher accepts `le="5"` and the Prometheus 3 form `le="5.0"`.

**Docs.** `docs/alerts/slo.md` explains the SLIs, the targets, the alerts,
and how to tune them. `docs/runbooks/harvest-alerts.md` gains one section
for each SLI. Those sections link to the existing triage sections.

**Tests.** `docs/alerts/slo-pack-v0.1.0.test.yml` gives each of the nine
alerts a case that fires and a case that stays silent. It also covers the
time-out-only outage, the `le="5.0"` form, zero traffic, and a short-window
reset. The CI `lint` job runs `promtool check rules` and `promtool test
rules` with a pinned, checksummed `promtool` 3.5.0. The guard suite
`autumn-harvest/tests/integration/slo_pack_docs.rs` pins the burn-rate
pairs, the objectives in the docs, the runbook links, the metric names, and
the CI step.

No engine change: no new `WorkflowEvent` variant, no migration, no new
metric.
