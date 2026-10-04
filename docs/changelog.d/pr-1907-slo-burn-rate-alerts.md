## Observability — SLO definitions and burn-rate alerts (issue #1816)

**New optional pack.** `docs/alerts/slo-pack-v0.1.0.rules.yml` defines three
reference SLOs and pages on error-budget burn. It uses the multi-window,
multi-burn-rate method from the Google SRE Workbook.

| SLI | Objective | Source series |
|---|---|---|
| `workflow_task` | 99.9% | `harvest_workflow_duration_count{status}`, `harvest_workflow_task_timeout_total` |
| `schedule_to_start` | 99% within 5 s | `harvest_queue_schedule_to_start_bucket` (5 s and `+Inf`) |
| `canary` | 99% | `harvest_canary_{success,failure}_total` |

- Each SLI records its error ratio over 5m, 30m, 1h, 6h, and 3d.
- Each SLI has three alerts: 14.4x over 1h/5m (page), 6x over 6h/30m
  (page), and 1x over 3d/6h (ticket).
- Each objective is one `harvest:slo_objective:ratio` record, so a target
  change is a one-line edit.
- `harvest_slo_objective_invalid` (ticket) fires when an objective is
  missing or not between 0 and 1. A bad tune would otherwise make the burn
  alerts silent or stuck on.
- A missing series counts as zero, so a fleet where every workflow task
  times out still pages. Canary probe timeouts count only in the `canary`
  SLI.
- Both sides of the schedule-to-start ratio read buckets. A worker that
  exports no buckets cannot make fast tasks look slow.
- The bucket matcher accepts `le="5"` and the Prometheus 3 form `le="5.0"`.

**Docs.** `docs/alerts/slo.md` explains the SLIs, the targets, the alerts,
how to tune them, and what each SLI cannot see.
`docs/runbooks/harvest-alerts.md` gains one section for each SLI and one for
the objective check. Those sections link to the existing triage sections.

**Tests.** `docs/alerts/slo-pack-v0.1.0.test.yml` gives each of the ten
alerts a case that fires and a case that stays silent. It also covers the
time-out-only outage, timeouts mixed with completed cycles, excluded canary
timeouts, the `le="5.0"` form, a worker without buckets, zero traffic, a
short spike that the long window holds back, and a short-window reset for
every tier. Cases near a threshold prove that each total includes its bad
events. A mutation run of the rules (17 mutants) kills every mutant. The CI `lint` job runs `promtool check rules` and `promtool test
rules` with a pinned, checksummed `promtool` 3.5.0. The guard suite
`autumn-harvest/tests/integration/slo_pack_docs.rs` pins the burn-rate
pairs, the objectives in the docs, the runbook links, the metric names, and
the CI step.

No engine change: no new `WorkflowEvent` variant, no migration, no new
metric.
