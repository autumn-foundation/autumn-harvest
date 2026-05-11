# Harvest Starter Alert Pack

This directory contains the versioned starter alert pack for production Harvest
deployments:

- `starter-pack-v0.1.0.json` is the structured rule catalogue.
- `../runbooks/harvest-alerts.md` is the linked response runbook.
- `../runbooks/synthetic-incident-drills.md` contains the incident drills that
  prove the first five common failure modes route to the expected alert.

The thresholds are starter defaults, not universal SLOs. Tune them to workload
volume, downstream SLAs, deployment topology, queue count, shard count, and the
cadence of your scheduled workflows. A queue that normally drains 10 tasks per
minute should not page on the same backlog depth as a queue that drains 10,000.

## Dependency Tags

Rules in `starter-pack-v0.1.0.json` are dependency-tagged so operators can tell
which alerts are enforceable today and which need another exported signal:

| Dependency | Used for |
|---|---|
| ADR-0001 / #138 | Stable metric names and bounded label rules. |
| #100 | Worker list/detail/health API. |
| #145 | Workflow stack API for stuck execution triage. |
| #160 | Deployment preflight report. |
| #161 | Shard health/readiness report. |
| #171 | Build-id routing. The `no_compatible_worker` alert is marked pending until a native bounded management signal is exported. |

Pending rules are intentionally not presented as enforceable. That keeps the
pack honest; nothing good happens when an alert pretends an API exists.

## Prometheus / Grafana Path

For metric-backed rules, use the ADR-0001 metric catalogue from
`docs/adr/0001-otel-trace-contract.md` and the `metrics-rs` wiring in
`docs/telemetry.md`. The sample PromQL in the JSON pack uses only the stable
Harvest metric names that are expected after Prometheus normalization:

| ADR metric | Prometheus series |
|---|---|
| `harvest.workflow.started` | `harvest_workflow_started_total` |
| `harvest.workflow.duration` | `harvest_workflow_duration_count`, `harvest_workflow_duration_sum`, `harvest_workflow_duration_bucket` |
| `harvest.activity.duration` | `harvest_activity_duration_count`, `harvest_activity_duration_sum`, `harvest_activity_duration_bucket` |
| `harvest.timer.started` | `harvest_timer_started_total` |
| `harvest.queue.depth` | `harvest_queue_depth` |
| `harvest.dlq.entries` | `harvest_dlq_entries` |
| `harvest.schedule.runs` | `harvest_schedule_runs_total` |
| `harvest.schedule.skipped` | `harvest_schedule_skipped_total` |
| `harvest.retention.deleted` | `harvest_retention_deleted_total` |

Use only bounded labels from ADR-0001/#138: `workflow`, `activity`, `queue`,
`status`, `shard`, `kind`, `name`, and `reason`. Never use `execution.id`,
`harvest.execution.id`, raw workflow IDs, task IDs, payload values, tenant IDs,
or user IDs as metric labels. Those belong in traces, logs, or API payloads.

Readiness-style alerts do not have native ADR metrics. Either run the CLI/API
checks directly or export the management API result through your own probe with
bounded labels such as `check`, `status`, `queue`, and `shard`.

## Non-Prometheus CLI/API Path

Embedders that only mount the management API can still use the pack. The first
action in every rule points at a CLI or API check.

| Check | CLI | API |
|---|---|---|
| Deployment preflight | `harvest preflight --output json` | `GET /api/harvest/admin/preflight` |
| Worker fleet health | `harvest worker health --output json` | `GET /api/harvest/workers/health` |
| Worker queue coverage | `harvest worker list --queue <queue> --output json` | `GET /api/harvest/workers?queue=<queue>` |
| Shard readiness | `harvest shard health --fail-on-unready --output json` | `GET /api/harvest/admin/shards/health` |
| Dead letters | `harvest dlq list --limit 25` | `GET /api/harvest/dead-letters` |
| Schedules | `harvest schedule list --output json` | `GET /api/harvest/admin/schedules` |
| Concurrency saturation | `harvest concurrency status --output json` | `GET /api/harvest/admin/concurrency` |
| Workflow wait state | `harvest workflow stack <execution_id>` | `GET /api/harvest/workflows/{execution_id}/stack` |
| Retention | `harvest retention status`; `harvest retention run-now` | `GET /api/harvest/admin/retention`; `POST /api/harvest/admin/retention/run-now` |

Recommended check cadence:

| Surface | Starter cadence |
|---|---|
| Preflight | Before deploy and every 1 minute in production. |
| Worker health | Every 10 seconds, or 2x worker heartbeat interval. |
| Shard health | Every 30 seconds during rollouts, every 1 minute otherwise. |
| DLQ and schedules | Every 1 minute. |
| Concurrency and workflow stack | On alert, or every 1 minute for known hot paths. |

## Installing the Pack

1. Wire metrics if you use Prometheus/Grafana. See `docs/telemetry.md`.
2. Import the native metric expressions from `starter-pack-v0.1.0.json`.
3. Add API checks for readiness rules that are not native metrics.
4. Link each alert to `docs/runbooks/harvest-alerts.md`.
5. Run the drills in `docs/runbooks/synthetic-incident-drills.md` before
   calling the deployment production-ready.

No rule in this pack requires a new `WorkflowEvent` variant, event-history
rewrite, migration, replay-rule change, or macro path change.
