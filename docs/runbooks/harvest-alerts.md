# Runbook: Harvest Starter Alerts

Use this runbook with `docs/alerts/starter-pack-v0.1.0.json`. Every action is
read-only unless explicitly called out as a safe action. Start with the linked
first action, confirm the blast radius, then choose the smallest reversible
step. The pack protects workflow execution; it does not replace app-specific
SLOs.

## harvest_preflight_failed

### Triage steps

1. Run `harvest preflight --output json`.
2. Inspect failing `checks[]` and affected `shards[]`.
3. If the failure is worker coverage, also run `harvest worker health --output json`.
4. If the failure is shard readiness, run `harvest shard health --fail-on-unready --output json`.

### Likely causes

Missing migrations, unreachable shard, no worker for a required queue, stale
workers, disabled scheduler path for registered schedules, DLQ read failure, or
an admin API mounted without the expected auth boundary.

### False positives

Warning-only reports can be expected during deploys when workers are draining.
A no-schedule deployment should not warn about scheduler coverage; if it does,
treat that as a preflight bug and keep the deploy gate closed.

### Safe actions

Block promotion, restore the last known-good config, restart only the broken
worker group, or re-run migrations on the affected shard. Do not start, cancel,
reset, or replay workflows as part of preflight triage.

### Escalation criteria

Escalate to the platform owner if any shard is unreachable or migration state
cannot be proven. Escalate to the workflow owner if catalog or schedule
registration is inconsistent with the deployed binary.

## harvest_no_active_workers

### Triage steps

1. Run `harvest worker health --output json`.
2. Run `harvest worker list --queue <queue> --output json` for the missing queue.
3. Check shard coverage with `harvest shard health --fail-on-unready --output json`.
4. Compare worker build/deployment identity against the current release plan.

### Likely causes

Worker deployment crashed, wrong queue name, shard assignment mismatch, workers
drained before replacements were active, or the process cannot heartbeat.

### False positives

Single-purpose queues may be idle by design, but required queues named by
registered workflows or schedules must always have fresh coverage.

### Safe actions

Restart or scale the worker deployment for the named queue, roll back a bad
worker config, or pause the deploy before more starts enter the uncovered
queue. Avoid bulk replay until coverage is fresh.

### Escalation criteria

Escalate if coverage is absent for more than two heartbeat windows after a
worker restart, or if multiple shards report the same missing queue.

## harvest_worker_saturation

### Triage steps

1. Run `harvest worker health --output json`.
2. Run `harvest worker list --status Active --output json`.
3. Run `harvest concurrency status --output json` to check caps and deferred work.
4. Compare draining workers against the deploy or maintenance window.

### Likely causes

Too many workers are stale or draining, downstream latency is holding
in-flight slots, a concurrency cap is saturated, or a rolling deploy drained
old workers too early.

### False positives

Draining saturation is expected during planned scale-in if backlog is flat and
replacement workers are already active. Stale workers can linger briefly for up
to the configured freshness window.

### Safe actions

Pause further drains, add replacement workers, raise a well-understood
concurrency cap, or roll back a worker release that introduced slow handlers.
Do not terminate draining workers while `in_flight_count` is nonzero unless the
incident is worse than retrying the work.

### Escalation criteria

Escalate when all workers for a required queue are stale or draining, or when
deferred concurrency work grows while downstream owners report elevated errors.

## harvest_queue_backlog_growth

### Triage steps

1. Check the alert labels for the queue.
2. Run `harvest worker list --queue <queue> --output json`.
3. Run `harvest concurrency status --output json`.
4. For a sample stuck execution, run `harvest workflow stack <execution_id>`.

### Likely causes

Worker count too low, workers cannot claim the queue, downstream dependency
latency, concurrency caps, shard readiness issues, or incompatible build
routing for part of the fleet.

### False positives

Batch windows can create expected short spikes. Alert only when depth stays
high and growth remains positive for the rule window.

### Safe actions

Scale the worker pool, roll back the deployment that changed queue assignment,
temporarily raise a proven concurrency cap, or pause a schedule that is flooding
the queue. Prefer pausing producers over replaying DLQ entries into an already
backlogged queue.

### Escalation criteria

Escalate if backlog growth persists for two alert windows after workers are
fresh, or if the queue backs a customer-facing workflow with a breached SLA.

## harvest_activity_failure_surge

### Triage steps

1. Check the `activity`, `queue`, and `status` labels.
2. Run `harvest dlq list --limit 25`.
3. For active impacted executions, run `harvest workflow stack <execution_id>`.
4. Check application logs for the named activity handler and downstream calls.

### Likely causes

Downstream outage, bad credentials, schema or payload drift, code regression,
timeout too low, or retry policy that exhausts before the dependency recovers.

### False positives

New workflows can expose intentionally failing validation activities. Treat
failures as actionable when they affect production activities, grow quickly, or
match DLQ entries.

### Safe actions

Roll back the handler release, restore credentials, pause schedules that are
triggering the activity, or tune the timeout/retry policy in a follow-up
release. Replay dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the downstream owner for sustained external errors. Escalate to
the workflow owner if failures are deterministic payload or code regressions.

## harvest_dlq_growth

### Triage steps

1. Run `harvest dlq list --limit 25`.
2. Group entries by activity, workflow, shard, and error summary.
3. Check whether the same activity failure alert is firing.
4. Pick one affected execution and run `harvest workflow stack <execution_id>`.

### Likely causes

Retry exhaustion from downstream outage, unrecoverable payload validation
error, worker panic, timeout misconfiguration, or a replay determinism error.

### False positives

Old DLQ entries are not growth. Page on new entries or increasing gauge depth,
not on a stable backlog that is already being drained under an incident ticket.

### Safe actions

Stop the producer or pause schedules if new DLQ entries are flooding in. Fix
the handler or dependency, then use `harvest dlq bulk-replay --dry-run` before
real replay. Discard only entries that are proven unrecoverable and approved by
the workflow owner.

### Escalation criteria

Escalate immediately for replay determinism failures or customer-visible
payment, fulfillment, identity, or notification workflows entering DLQ.

## harvest_schedule_missed_runs

### Triage steps

1. Run `harvest schedule list --output json`.
2. Inspect `is_paused`, `next_run_at`, `last_run_at`, and schedule kind.
3. Run `harvest preflight --output json` to confirm scheduler coverage.
4. Check queue backlog for the schedule's dispatch queue.

### Likely causes

Schedule paused, scheduler disabled, max active runs reached, catchup disabled,
worker outage, queue backlog, or shard readiness issue.

### False positives

Manual schedules and intentionally paused schedules should not page. Cron
schedules with long intervals need a rule window larger than two expected
firings.

### Safe actions

Resume an accidentally paused schedule, restore scheduler coverage, scale the
dispatch queue, or trigger a manual catchup only after idempotency is confirmed.
Avoid blind backfills while downstream systems are unhealthy.

### Escalation criteria

Escalate when a regulatory, billing, or customer-notification schedule misses
one required firing, or when catchup would exceed downstream capacity.

## harvest_retention_lag

### Triage steps

1. Run `harvest retention status`.
2. Confirm retention is enabled and note `max_age`, `batch_size`, and dry-run state.
3. Run `harvest retention run-now` only if the status shows eligible rows and no shard blockers.
4. Check database storage pressure before changing retention settings.

### Likely causes

Retention disabled, dry-run mode left on, shard unavailable, batch size too
small, long-running terminal scan, or no eligible terminal executions.

### False positives

Zero deleted rows is normal when no terminal histories are older than the
configured max age. Dry-run deployments should ticket, not page, unless storage
pressure is active.

### Safe actions

Enable retention intentionally, lower max age only with product approval, raise
batch size gradually, or run a one-off retention tick during low traffic. Do not
delete active workflow histories.

### Escalation criteria

Escalate to the database owner if storage pressure is rising or retention
queries are impacting production latency.

## harvest_shard_unready

### Triage steps

1. Run `harvest shard health --fail-on-unready --output json`.
2. Identify affected shard ids, readiness verdict, and reason codes.
3. Check whether the shard is writable, readable-only, or a promotion candidate.
4. Run `harvest preflight --output json` to see whether the same blocker appears globally.

### Likely causes

Database unreachable, schema mismatch, missing worker coverage, stale scheduler
coverage, stale health sample, queue pressure, or DLQ pressure on the shard.

### False positives

Readable-only candidate shards can be unready while still safe for existing
traffic. Writable shard unready is never a cosmetic warning.

### Safe actions

Remove an unready candidate from promotion, keep `writable_shards` unchanged,
repair migrations, restore worker coverage, or roll back shard config. Do not
move existing executions across shards; Harvest does not rebalance them.

### Escalation criteria

Escalate if a writable shard is unavailable, if multiple shards are partial, or
if readiness cannot be proven during a rollout.

## harvest_no_compatible_worker

### Triage steps

1. Confirm the alert status is still `pending-management-signal`; use it only if your deployment exports this signal.
2. Run `harvest workflow stack <execution_id>` for an affected execution.
3. Run `harvest worker health --output json` and inspect worker build/deployment identity.
4. Review `docs/runbooks/safe-deploy.md` for build compatibility and safe-to-retire checks.

### Likely causes

New build policy moved starts to a build with no active workers, old workers
were drained before in-flight executions finished, compat was not declared, or
legacy workers with empty build IDs are masking the real routing state.

### False positives

This rule is pending until Harvest exports a native bounded
`no_compatible_worker` management signal. Do not page on inferred symptoms
unless queue backlog or stack evidence confirms stuck work.

### Safe actions

Start workers for the required build, declare compatibility only after replay
fixtures prove it safe, shift new starts back to the previous build, or stop
draining old-build workers. Never route incompatible workers just to clear a
queue; replay safety is the reason this alert exists.

### Escalation criteria

Escalate to the release owner when production work is stuck behind build
compatibility or when rollback requires reverse compatibility declarations.

## harvest_schedule_ha_domination

### Triage steps

1. Check the `lost_race / (lost_race + claimed)` ratio in Grafana for the last 5–10 minutes.
2. Query Postgres for stuck claim tokens:
   ```sql
   SELECT id, workflow_name, fire_claim_token, fire_claimed_until, next_run_at
   FROM harvest_schedules
   WHERE fire_claimed_until IS NOT NULL
   ORDER BY fire_claimed_until DESC
   LIMIT 10;
   ```
3. Verify all replicas share the same `DATABASE_URL` and shard routing configuration.
4. Run `harvest worker health --output json` to confirm fleet coverage.

### Likely causes

- One or more replicas point to a **different Postgres instance** than the majority of the fleet (the "different DB" replica claims a disjoint set; others always see it as locked).
- Incorrect `shard_assignments` exclude most replicas from the affected shard.
- A stuck or crashed replica's claim token has not expired (token older than 30 s + tick interval is a bug; see escalation).

### False positives

In a **single-replica deployment** or **initial startup** before the first tick, `lost_race = 0` and `claimed = 1 per tick`. This alert should never fire for single-replica deployments (ratio is always 0).

In a **two-replica deployment**, healthy steady-state is approximately `lost_race ≈ claimed` (each replica wins about half the slots at the tick boundary). The 0.98 threshold ensures this alert does not fire for expected contention.

### Safe actions

Fix the database configuration so all replicas share the same shard pools. Do not manually clear `fire_claim_token` rows unless you have confirmed the claiming replica has stopped; the 30-second TTL handles crash recovery automatically.

### Escalation criteria

Escalate to the platform owner if:
- A `fire_claim_token` row has `fire_claimed_until` more than 2 minutes in the past and `next_run_at` has not advanced (indicates the claiming process is alive but wedged without completing the fire or clearing the claim — this should not happen with the current implementation and would indicate a bug).
- The alert fires on a single-replica deployment (indicates a misconfiguration or metric collection error).
