# Runbook: Harvest Starter Alerts

Use this runbook with `docs/alerts/starter-pack-v0.1.0.json`. Every action is
read-only unless explicitly called out as a safe action. Start with the linked
first action, confirm the blast radius, then choose the smallest reversible
step. The pack protects workflow execution; it does not replace app-specific
SLOs.

First responders: import the starter Grafana dashboard pack
(`docs/dashboards/starter-pack-v0.1.0.json`) for the visual side of every
metric-backed rule below — each alert maps to a named panel (the mapping
table lives in `docs/dashboards/README.md`), and each mapped panel's
description links back to its section in this runbook.

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

## harvest_worker_slot_saturation

**Worker dispatch-slot bottleneck** (issue #531). Fires when a worker's slot
utilisation (`slots_in_use / (slots_in_use + slots_available)`) for either
`slot_type` (`workflow` or `activity`) stays above 90 % for 5 minutes. This
means the worker itself is the bottleneck — not the queue or the database.

### Triage steps

1. **Read the alert labels.** The firing alert carries `slot_type=workflow` or
   `slot_type=activity` and the Prometheus `instance`/`job` labels that identify
   the saturated worker process. The `harvest.worker.slots_in_use` /
   `harvest.worker.slots_available` gauges are per-process in-memory reads, so
   the scrape target is the authoritative source for which worker and slot type
   is saturated. `harvest worker health --output json` returns aggregate fleet
   counts (healthy/stale/draining by queue/shard) and does **not** include
   per-worker slot-type breakdowns — use it for fleet context only.
2. Run `harvest worker list --output json` to confirm expected worker count per
   queue and check for stale heartbeats or workers already draining.
3. Run `harvest concurrency status --output json` (`GET /api/harvest/admin/concurrency`)
   to check whether per-key concurrency limits are deferring work on top of the
   slot pressure.
4. Compare `harvest.queue.depth` for the same queue:
   - **Slots saturated AND backlog growing** → worker is the bottleneck; raise
     `max_concurrent_workflows` / `max_concurrent_activities` or add workers.
   - **Backlog growing but slots are free** → bottleneck is downstream
     (DB, throttled dependency); adding workers will not help.

### Likely causes

Activity handlers that run slowly or block the executor leave activity slots
occupied for longer than expected. A concurrency cap set below the worker's
physical capacity. Too few workers for the offered throughput. A slow downstream
dependency extending handler latency.

### False positives

Sustained high utilisation is expected and healthy for throughput-oriented
fleets. Tune the threshold per queue and latency target before treating this as
a page. A brief spike during traffic bursts that self-resolves within the
5-minute window will not fire.

### Safe actions

Raise `WorkerConfig::max_concurrent_workflows` or `max_concurrent_activities` if
the host has headroom. Add worker replicas for the affected queue. Investigate
slow activity handlers with `harvest.activity.duration` histograms. Temporarily
lower the ingestion rate if downstream dependencies are the bottleneck.

### Escalation criteria

Escalate when slot saturation persists after adding workers, or when the
`harvest.queue.oldest_pending_age` gauge climbs alongside this alert, indicating
work is stalling rather than simply queuing briefly.

## harvest_queue_schedule_to_start_high

**Primary queue-saturation page** (issue #501). Fires when p99
`harvest.queue.schedule_to_start` for a queue exceeds your configured SLA
threshold (default 30 s). This is the canonical "do I need more workers?"
signal — it measures actual wait time rather than a depth heuristic.

### Triage steps

1. Check the `queue` label in the alert to identify the saturated queue.
2. Run `harvest worker health --output json` — look for stale heartbeats or
   workers draining.
3. Run `harvest worker list --queue <queue> --output json` — verify workers are
   claiming tasks. Zero claimers means workers are absent or filtered by build-id.
4. Run `harvest concurrency status --output json` — a concurrency cap at 100%
   will stall claims even when workers are healthy.
5. Check `harvest_queue_oldest_pending_age{queue=…}` — a large value here
   confirms a task has been stuck longer than the histogram quantile.
6. For a sample stuck execution, run `harvest workflow stack <execution_id>`.

### Likely causes

Worker count too low for current throughput, workers cannot claim the queue
(build-id mismatch, concurrency cap saturated, activity circuit breaker open),
downstream dependency latency inflating per-task duration and reducing worker
throughput, shard readiness issues, or a schedule burst flooding the queue.

### False positives

Planned maintenance windows, expected burst periods, or a very small number
of tasks with unusually large payloads that temporarily saturate workers. Alert
only when the p99 sustains above threshold for the full rule window.

### Safe actions

Scale the worker pool, roll back the deployment that changed queue assignment,
temporarily raise a proven concurrency cap, or pause a schedule that is flooding
the queue. Prefer pausing producers over replaying DLQ entries into an already
saturated queue.

### Escalation criteria

Escalate if wait times persist for two alert windows after fresh workers are
added, or if the queue backs a customer-facing workflow with a breached SLA.

## harvest_queue_backlog_growth

**Secondary signal** — superseded as the primary page by
`harvest_queue_schedule_to_start_high` (#501). Use this as a corroborating
signal or as a fallback when histogram metrics are not yet wired.

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

### DLQ flood — the first 60 seconds

When the DLQ entry count crosses the alert threshold during an incident, the
first question is *the shape of the fire*, not any single entry. Lead with the
aggregation endpoint instead of paging the flat list or opening `psql`:

```bash
# What is this fire made of? One root cause or ten?
harvest dlq aggregate \
  --group-by workflow_name,failure_signature \
  --since 24h --samples-per-group 3
```

Read the table top-down:

- **One dominant group** (e.g. `onboarding / "stripe: rate limited" → 1,842`):
  a single root cause. Fix the dependency, then bulk-replay with confidence
  using the sample IDs to spot-check first
  (`harvest dlq bulk-replay --dry-run …`).
- **A flat spread across many groups**: unrelated bugs. Do *not* bulk-replay;
  triage case by case from the largest groups down.

`failure_signature` normalizes UUIDs, hex, and numbers in the first line of
each error to fixed placeholders, so the same root cause aggregates identically
across queries and shards. Counts sum across shards; `_other` rolls up the long
tail so totals reconcile to `filtered_total`. Narrow with the same filters as
the list endpoint (`--workflow-name`, `--activity-name`, `--queue-name`,
`--since`, `--until`, `--min-attempts`) and pivot to a single entry via the
`sample_dead_letter_ids`. The endpoint backing this is
`GET /api/harvest/dead-letters/aggregate` (admin auth, parity with the list
endpoint). The same view is one click away in the Vantage UI: open the **Dead
Letters** page and flip the **Summary** toggle to see the top groups, switch the
`group_by` dimension, and drill into the filtered list.

### Triage steps

1. Run the `harvest dlq aggregate` summary above to classify the flood.
2. For the dominant group, pivot to its `sample_dead_letter_ids`, or list a
   slice with `harvest dlq list --limit 25` for full row detail.
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

### Redrive — getting work back out after a fix (issue #510)

`bulk-replay` re-enqueues a dead-letter row only if its owning execution is
still `RUNNING`. Almost every real DLQ entry was sealed `FAILED` at quarantine
time (history cap, poison-pill, workflow-task timeout), so the recovery path is
**redrive**, which reactivates the `FAILED` execution before re-enqueuing:

```bash
# 1. Deploy the fix that addresses the root cause first.
#
# 2. Preview the blast radius — counts only, no mutation. matched > max means
#    there is more to do than this call will redrive.
harvest dlq redrive \
  --error-contains "stripe: rate limited" \
  --dead-lettered-after 2026-05-30T00:00:00Z \
  --max 500 --dry-run

# 3. Redrive for real once the sample looks right. Idempotent: re-running the
#    same command is a no-op for anything already redriven (reported skipped).
harvest dlq redrive \
  --error-contains "stripe: rate limited" \
  --dead-lettered-after 2026-05-30T00:00:00Z \
  --max 500 --reason "stripe rate-limit cleared, incident-1234"
```

The response distinguishes `matched` (total filtered), `redriven`,
`skipped`, and `failed`. Read it:

- **`redriven < matched`** — the `max` cap stopped early. Re-run to drain the
  rest (the filter is stable; already-redriven rows skip).
- **`skipped`** — rows that were already redriven, or whose execution has since
  progressed past that step. Expected on a re-run; never a duplicate enqueue.
- **`failed`** — per-row rejections (see `failures[].reason`). The most common
  is an owning execution in a non-`FAILED` terminal state
  (`COMPLETED`/`CANCELLED`/`TIMED_OUT`/`TERMINATED`): redrive **refuses to
  resurrect** these and leaves the row in place rather than silently
  re-running a finished workflow.

### Verify a redrive

- The owning execution flips `FAILED → RUNNING`
  (`harvest workflow show <execution_id>`); a single `WorkflowRedriven` event is
  appended after the superseded `WorkflowFailed`.
- The `harvest.dlq.redriven{queue, outcome}` counter increments once per row
  (one of `redriven` / `skipped` / `failed`).

### Guarantees

- **Append-only.** Redrive never rewrites, removes, or reorders an existing
  event. It appends one `WorkflowRedriven` reactivation marker; the fresh
  attempt then records new events (e.g. a new `ActivityScheduled`) as the
  re-enqueued task resumes from existing history. The replay engine treats the
  `WorkflowRedriven` event and the `WorkflowFailed` it supersedes as
  transparent, so the run resumes deterministically.
- **Idempotent.** The DLQ row is deleted on redrive, so redriving the same
  entry twice (or a row whose execution already progressed) is a no-op reported
  as `skipped` — never a duplicate side-effect.
- **Shard-aware.** A filtered redrive fans out across all shards; each row is
  re-enqueued on the shard that owns its `workflow_exec_id`.

The endpoint backing this is `POST /api/harvest/dlq/redrive` (admin auth,
audit op `dlq.redrive`).

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

## harvest_workflow_failure_rate

The fraction of terminal workflow executions ending `failed` has exceeded the
threshold (starter default: 10% over 5 minutes) for a workflow type. This is
the primary success-rate SLO signal, computed from the
`harvest.workflow.terminal{outcome}` counter (issue #519), which fires exactly
once per terminal outcome. The `outcome` label separates `failed` from
operator-driven `cancelled`/`terminated` and from `timed_out`, so the ratio
pages only on genuine failures.

### Triage steps

1. Identify the `workflow` label from the alert.
2. List recent failures: `harvest workflow list --state FAILED --workflow <name>`
   (or `GET /api/harvest/workflows?state=FAILED&workflow_name=<name>`) and read
   the `error` field on a sample of rows.
3. Check the DLQ for poison-pill or retry-exhaustion entries from the same
   workflow: `harvest dlq list --limit 25`, or aggregate root causes with
   `harvest dlq aggregate --group-by workflow_name,failure_signature`.
4. Compare the failure onset against recent deploys and against the
   `harvest.activity.failed{error_type}` breakdown — a workflow failure surge
   is usually downstream of an activity failure surge.
5. If failures are non-determinism related, follow
   `#harvest_workflow_non_determinism` instead (check
   `GET /api/harvest/workflows?nd_blocked=true`).

### Likely causes

A deployment regression in the workflow or one of its activities, a downstream
dependency outage exhausting activity retries, poison-pill quarantine failing
the owning workflows, a too-tight `execution_timeout` misclassifying slow runs
(those surface as `timed_out`, not `failed`, unless a handler maps them), or a
payload/schema drift that makes the workflow fail deterministically on new
input.

### False positives

Low-traffic workflow types produce unstable ratios over short windows — a
single failure in a 5-minute window with two terminal runs is 50%. Alert only
on workflow types with steady baseline traffic, or lengthen the window.
Deliberate operator terminations and cancellations are separate `outcome`
values and never count toward this ratio's numerator; `continued_as_new` is
excluded from the denominator by the **dashboard** pack's ratio panel — the
alert pack's default expression carries no such exclusion, so add it to your
alert expression if continue-as-new volume is significant.

### Safe actions

Roll back the offending workflow/activity release, force-open a circuit
breaker if a known-bad downstream is burning retries
(`POST /api/harvest/admin/circuits/{activity_name}/force-open`), pause the
schedules feeding the failing workflow type, or pause individual runaway
executions. Redrive dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the workflow owner when the failure ratio stays above the
threshold for more than two windows, and to the release owner when the onset
correlates with a deploy. Page immediately if the failing workflow type backs
a customer-facing SLO or if DLQ entries for it are accumulating rapidly.

## harvest_workflow_non_determinism

Since issue #603 an engine-detected replay divergence **blocks the affected
execution non-terminally** instead of failing it: the run stays `RUNNING`, no
`WorkflowFailed` event is written, and the workflow task is re-dispatched with
a capped-exponential backoff (5s doubling to a 300s ceiling, indefinitely).
Rolling back or fixing the offending build resumes the whole cohort with **no
per-execution operator action**. Full playbook:
`docs/runbooks/nondeterminism-block.md`.

### Triage steps

1. Locate blocked executions by querying the management API:
   ```bash
   GET /api/harvest/workflows?nd_blocked=true
   ```
   (Executions that terminally failed under the pre-#603 behavior remain
   discoverable via `GET /api/harvest/workflows?state=FAILED&failure_cause=non_determinism`.)
2. Read each row's divergence diagnostic: `nd_block_reason`, `nd_block_count`,
   `nd_blocked_at` on the execution, plus the structured fields in
   `search_attrs`:
   - `expected` (the event/command the current code generated)
   - `actual` (the event recorded in the history)
   - `event_index` (index where the divergence occurred)
   - `build_id` (the build ID of the worker that observed the divergence)
3. Check recent deployment history to see if a new release was shipped without proper version gating or routing protection.
4. Run replay tests on the workflow using the exported history to reproduce the non-determinism error.

### Likely causes

- Code deployment that modifies workflow logic (adding, removing, or reordering activities, signals, timers, or child workflows) without updating the version gate.
- Side effects that are not wrapped in `WorkflowContext::side_effect()`, such as direct system calls, time queries (`Instant::now()`), or random number generation.
- Iteration order on non-deterministic collections (like `HashMap` or `HashSet`) in the workflow function.

### False positives

None. A non-determinism mismatch means the workflow code generated a different sequence of commands/actions than what was recorded in history, making replay safety impossible. Author `Err(...)` returns are never classified as divergence — they still fail terminally.

### Safe actions

1. Roll back the offending deployment immediately to the last known-good version. Blocked executions resume automatically on their next backoff re-dispatch (within ≤300s) — confirm `nd_blocked_at` clears and the cohort progresses.
2. If the deployment must stay, declare build compatibility appropriately or use version gates.
3. Escalation only — for a history that stays blocked even under the rolled-back build (e.g. the divergent build appended inline events before diverging), reset to the pre-divergence event index:
   ```bash
   POST /api/harvest/workflows/{execution_id}/reset
   ```
   Specifying the event index prior to the divergence.

### Escalation criteria

Escalate immediately to the release owner and the team who shipped the latest version. Replay divergence blocks execution progress for all active workflows of that type; while no work is destroyed (the block is recoverable), the cohort makes no forward progress until the build is rolled back or fixed.

## harvest_activity_success_ratio

### Triage steps

1. Identify the `activity` and `queue` labels from the alert.
2. Run `harvest dlq list --limit 25` to check for dead-letter accumulation from that activity.
3. Run `harvest workflow stack <execution_id>` on affected executions to see pending activity state.
4. Check `harvest.activity.failed{activity=<name>}` for `error.type` breakdown to classify the failure mode.
5. Inspect the circuit breaker state: `GET /api/harvest/admin/circuits/<activity_name>`.

### Likely causes

Downstream outage or degradation, bad credentials, schema or payload drift, deployment regression
in the activity handler, timeout configured too low, or retry exhaustion tipping into the DLQ.

### False positives

Newly deployed activities with sparse traffic produce unstable ratios over short windows. Alert
only when the activity has steady baseline traffic and the ratio drops for more than one 5-minute
window. A ratio of exactly 0 with no `outcome=completed` series usually indicates the activity
has never succeeded in the window — check if it is newly deployed or was just restarted.

### Safe actions

Roll back the handler release, restore downstream credentials, force-open the circuit breaker
(`POST /api/harvest/admin/circuits/{activity_name}/force-open`) if the downstream is known-bad,
or pause the schedules that are triggering the activity until the downstream recovers.
Replay dead letters only after the root cause is fixed.

### Escalation criteria

Escalate to the downstream team for sustained external errors. Escalate to the workflow owner
for payload or schema regressions. Page immediately if the success ratio stays below 50% for
more than 10 minutes or if DLQ entries are accumulating rapidly.

## harvest_activity_retry_storm

### Triage steps

1. Identify the `activity` and `queue` labels from the alert.
2. Check the retry rate trend: is it growing, stable, or receding?
3. Run `harvest dlq list --limit 25` to determine whether retries are eventually succeeding or tipping into the DLQ.
4. Check `harvest.activity.failed{activity=<name>,error.type=<type>}` to identify the failure class driving retries.
5. Inspect circuit breaker state: `GET /api/harvest/admin/circuits/<activity_name>`.
   If the breaker is closed and failures are consistent, consider force-opening it to stop flooding: `POST /api/harvest/admin/circuits/{activity_name}/force-open`.

### Likely causes

Intermittent downstream degradation that recovers before retry exhaustion, too-aggressive retry
policy (high max_attempts or very short backoff), cascading failure across many parallel workflow
executions, or a transient infrastructure event (restart, rebalance, network blip).

### False positives

A burst of retries after a brief dependency restart is expected and self-resolving. Alert only
when the retry rate is sustained (> 5 min) or growing. If the circuit breaker opens automatically,
the storm signal will drop even if the underlying dependency is still recovering.

### Safe actions

1. If the downstream is known-bad, force-open the circuit breaker to stop the retry flood immediately.
2. Reduce concurrency on the affected queue temporarily via `WorkerConfig::max_concurrent_activities`.
3. Pause schedules that are generating the affected work until the downstream recovers.
4. Tune the retry policy (`max_attempts`, `initial_interval`) in a follow-up release if the storm is structural.

### Escalation criteria

Escalate to the downstream owner for external dependency outages. Escalate to the workflow owner
if the retry policy is misconfigured. Page (`harvest_activity_retry_storm_critical`) when the retry
rate exceeds 20/s — at that scale the task queue backlog will grow and other queues will be starved.

## harvest_saga_compensation_spike

**What a compensation spike means:** sagas are rolling back en masse. Each
`harvest.saga.compensated` increment is one real `compensate_all` /
step-failure unwind actually running forward (exactly once per sequence —
replays never re-count), so an elevated rate means many workflows hit a
failing forward step and are undoing already-committed work. This is the
canonical *leading* indicator of a downstream-dependency outage: it fires as
soon as unwinds start, before retry exhaustion, DLQ growth, or terminal
workflow failures become visible.

### Triage steps

1. Read the `workflow` label on the spiking series to identify which saga
   workflow type is rolling back, and `queue` to locate the worker pool.
2. Find the failing forward step: check
   `harvest.activity.failed{workflow.type=<name>}` for the `error.type`
   breakdown, and `harvest.activity.retries` for a concurrent retry storm on
   the same activity.
3. Inspect recent failures directly: `harvest workflow list --state FAILED
   --workflow-name <name> | head -20` (or `GET /api/harvest/workflows?state=FAILED`)
   and read the `error` field for the original step error.
4. Check whether `harvest_saga_compensation_failed` is ALSO firing — a spike
   plus failed compensations means dangling state is accumulating; treat as
   the page, not the ticket.
5. Check the downstream dependency the failing step calls (status page,
   circuit breaker state via `GET /api/harvest/admin/circuits`).

### Likely causes

- A downstream dependency (payment gateway, inventory service, partner API)
  started erroring, so forward steps fail and every in-flight saga unwinds.
- A bad deploy of an activity that a mid-saga step depends on.
- A legitimate business burst of rollbacks (mass cancellation event,
  end-of-sale inventory exhaustion) — elevated but expected.

### False positives

A workflow type that compensates as part of its normal business flow (e.g. a
quote-then-release pattern) has a non-zero baseline compensation rate — the
starter expression is baseline-relative for exactly this reason, but tune the
multiplier per workflow. A short burst after a dependency restart that
self-resolves within one evaluation window is expected.

### Safe actions

1. If a downstream outage is confirmed, force-open the failing activity's
   circuit breaker (`POST /api/harvest/admin/circuits/{activity}/force-open`)
   so new sagas fail fast at the first step instead of committing work they
   will immediately unwind.
2. Pause the schedules or gate the admissions that feed the affected workflow
   type until the downstream recovers.
3. Let in-flight unwinds run — compensations are idempotent by contract and
   the counter confirms they are progressing.

### Escalation criteria

Escalate to the downstream owner when the failing step's dependency is
external. Page (rather than ticket) when the spike is sustained > 15 minutes,
when `harvest_saga_compensation_failed` fires alongside it, or when the
affected workflow type moves money or inventory.

## harvest_saga_compensation_failed

**What to do when a compensation fails:** a rollback itself failed
(`HarvestError::SagaCompensationFailed`), so the system is holding
partially-committed, dangling state — a charge that was not refunded, a
reservation that was not released. Nothing in the engine re-attempts a
failed compensation on its own timeline: for the documented durable pattern
(a compensation that invokes an activity), the activity's recorded
`ActivityFailed` terminal is **replayed, never re-executed** — deterministic
replay returns the recorded failure — and a pure in-memory compensation
closure only re-runs if the still-RUNNING execution replays again, which
does not survive an author-caught, completed run. Every increment therefore
represents work for a human until reconciled. The counter fires exactly once per failed unwind,
including unwinds whose error the workflow author caught — it is deliberately
separable from `harvest.workflow.terminal{outcome=failed}` so you can page on
it alone.

### Triage steps

1. Identify the workflow type and queue from the metric labels.
2. List candidate executions: `harvest workflow list --state FAILED
   --workflow-name <name>` — the execution `error` field carries the
   `SagaCompensationFailed` message with the original step error AND the
   per-compensation error strings. For author-caught cases the run may be
   COMPLETED; search the execution history for the failing compensation
   activity's `ActivityFailed` event instead
   (`GET /api/harvest/workflows/{execution_id}/history`).
3. Read the `compensation_errors` list in the error message to see exactly
   which compensations failed and why.
4. Determine the dangling resource for each failed compensation (the forward
   step's recorded output in history names it: charge id, reservation id).

### Likely causes

- The same downstream outage that triggered the unwind also broke the
  compensation path (refund API down while charge API degraded).
- A compensation activity with a bug or a missing idempotency guard.
- A compensation depending on state the forward step no longer guarantees
  (release-most-recent anti-pattern — see docs/saga.md).

### False positives

None by design — each increment is a real failed unwind. (False *negatives*
are limited to the documented conservative edge: an unwind entered at an
unresolvable history position — e.g. a signal-with-start run whose unwind
starts before the staged signal is awaited — is uncounted as a whole; see
the accepted edges in `docs/saga.md`.) If an increment is expected (e.g. a
deliberately non-compensatable step), the workflow should not register a
compensation for that step at all.

### Safe actions

1. Manually reconcile each dangling resource (issue the refund, release the
   reservation) using the resource ids recorded in the execution history.
2. Re-driving the workflow task helps **only for pure in-memory
   compensation closures** in a still-RUNNING execution: those re-run
   wholesale on every replay (idempotent by contract), so a transient
   failure can clear. It is safe but **ineffective** for the documented
   durable pattern (activity-backed compensations): the compensation
   activity's retries are already exhausted and its recorded `ActivityFailed`
   is replayed, not re-executed — `retry-now` (#516) only helps a
   *backing-off PENDING* task, not a terminally-failed one. For durable
   compensations, reconcile manually (step 1) or reset the execution from
   history before the failed compensation (#148, `docs/runbooks/` reset
   tooling) after fixing the downstream.
3. Fix the compensation activity (or its downstream) before re-enabling the
   traffic source; a broken compensation path turns every future rollback
   into manual work.

### Escalation criteria

Page immediately — this is dangling state by definition. Escalate to the
workflow owner for the reconciliation runbook of the specific resource, and
to the downstream owner when the compensation path's dependency is the thing
that failed. Escalate to the on-call engineering lead if increments continue
accruing after the traffic source is paused (indicates unwinds still failing
mid-flight).
