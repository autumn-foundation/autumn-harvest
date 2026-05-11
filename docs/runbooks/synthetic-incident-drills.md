# Synthetic Incident Drills for Harvest Alerts

Run these drills in staging before enabling the starter alert pack in
production. Each drill should trigger the expected alert within two scrape
intervals or health-check windows, and the linked runbook step should get the
operator to a safe first action in under 5 minutes.

Do not run these drills against production unless the incident commander
explicitly approves the blast radius.

## queue-backlog

Inject work onto a test queue with worker coverage intentionally scaled below
the incoming rate.

Expected alert: `harvest_queue_backlog_growth`.

Runbook step: `harvest_queue_backlog_growth` -> `### Triage steps`, especially
`harvest worker list --queue <queue> --output json` and
`harvest concurrency status --output json`.

Resolution target: add enough test workers to flatten `harvest_queue_depth`, or
pause the synthetic producer.

## dlq-spike

Deploy a test activity that returns a deterministic error after retry
exhaustion, then start a small batch of staging workflows that call it.

Expected alert: `harvest_dlq_growth`.

Runbook step: `harvest_dlq_growth` -> `### Triage steps`, especially
`harvest dlq list --limit 25`.

Resolution target: roll back the test activity, confirm no new DLQ entries are
created, then run `harvest dlq bulk-replay --dry-run` before replaying the
synthetic entries.

## stale-worker-fleet

Start a staging worker group, stop the processes without a graceful drain, and
wait for more than two heartbeat windows.

Expected alert: `harvest_no_active_workers` or `harvest_worker_saturation`,
depending on whether replacement workers remain active.

Runbook step: `harvest_no_active_workers` -> `### Triage steps`, especially
`harvest worker health --output json`.

Resolution target: restart the worker group and verify the health report shows
fresh active coverage for the required queue and shard.

## missed-schedule

Create a staging workflow schedule with a short interval, then pause it or set
`max_active_runs` low enough that a previous synthetic run blocks the next
firing.

Expected alert: `harvest_schedule_missed_runs`.

Runbook step: `harvest_schedule_missed_runs` -> `### Triage steps`, especially
`harvest schedule list --output json` and `harvest preflight --output json`.

Resolution target: resume the schedule or clear the synthetic active run, then
verify `harvest_schedule_runs_total` increases on the next interval.

## shard-unready

In a multi-shard staging environment, remove worker coverage for a writable
test shard or point one candidate shard at an unmigrated database.

Expected alert: `harvest_shard_unready`.

Runbook step: `harvest_shard_unready` -> `### Triage steps`, especially
`harvest shard health --fail-on-unready --output json`.

Resolution target: restore migrations or worker coverage, rerun shard health,
and only promote the shard when `readiness` is `ready`.
