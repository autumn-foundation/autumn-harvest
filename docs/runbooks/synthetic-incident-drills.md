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

## runaway-execution-containment

Start a long-running synthetic workflow in staging (a loop of short activities
with a durable timer between iterations works well — it exercises both the
activity and timer wake paths). Start a second, identical control run and let
it complete un-paused; record its output. Pause the first run mid-flight with
`harvest workflow pause <execution_id> --reason "drill"` (or
`POST /workflows/{id}/pause`), wait at least two poll intervals, and confirm
zero further dispatch progress: `state` is `PAUSED` on
`GET /workflows/{id}` with `paused_at`/`pause_reason`/`pause_actor` set, the
parked workflow task row is idle (`PENDING`, or `RUNNING` with
`worker_id IS NULL` for a parked decision task — parked rows carry no
worker), and the event history gains no new
`ActivityScheduled`/`TimerStarted` events (activity-layer work that was
already scheduled at pause time still runs: an in-flight attempt records its
completion, an enqueued-but-unclaimed activity task is still claimed and
executed, and a failing activity's retries continue and record their
outcomes — all expected; only new workflow-driven dispatch must stop).
Then resume with `harvest workflow resume <execution_id>` and let the run
finish.

Expected signal: `harvest.workflow.paused` increments once on pause;
`harvest.workflow.pause_duration` records the span on resume; the run appears
in `GET /workflows?state=PAUSED` only between the two calls.

Runbook step: `contain-runaway-execution.md` -> `## Verify containment`.

Resolution target: the paused-then-resumed run reaches `COMPLETED` with output
**identical** to the un-paused control run (deterministic-resume correctness —
the issue #609 success metric), and repeating the resume call afterwards
returns `200` with `newly_resumed: false` rather than an error.

## shard-unready

In a multi-shard staging environment, remove worker coverage for a writable
test shard or point one candidate shard at an unmigrated database.

Expected alert: `harvest_shard_unready`.

Runbook step: `harvest_shard_unready` -> `### Triage steps`, especially
`harvest shard health --fail-on-unready --output json`.

Resolution target: restore migrations or worker coverage, rerun shard health,
and only promote the shard when `readiness` is `ready`.

## restore-resumability

Unlike the drills above, this one is not alert-driven — it rehearses a
**recovery decision** rather than a detection path. Restore a recent backup into
a scratch database and verify it is safe to resume before you ever need to.

Restore the snapshot into a throwaway database, then:

```
harvest backup verify --shard '<scratch DSN>' --i-know-this-is-scratch
```

Expected result: exit `0` with verdict `clean` or `resumable_with_reclaim`. The
`reclaimable` findings (dead-worker `RUNNING` rows, expired leases, `INFLIGHT`
completion deliveries) are **expected** in any healthy restore — each names the
scanner that heals it.

Resolution target: the whole drill — restore, verify, decide — completes in
under 30 minutes, and the operator can state from the exit code alone whether
workers may start. Exit `1` (`incoherent`) means restore from a different point;
exit `2` (`unavailable`) means the drill did not actually check anything and must
be re-run.

In a multi-shard environment, restore **every** shard, pass every shard to a
**single** `harvest backup verify` invocation, and start workers only after that
one run exits `0`.

Runbook: [`backup-restore.md`](backup-restore.md).
