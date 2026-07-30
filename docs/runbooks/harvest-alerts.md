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

## First 60 seconds — one-call incident triage

When an alert fires or a page lands, hit **one** endpoint first:

```bash
curl -s https://your-app/api/harvest/admin/status | jq
```

`GET /api/harvest/admin/status` (issue #679) is the single incident-triage
entry point. It rolls up five subsystems — workers, shards, dead-letters,
queues, and stalled workflows — into **one** JSON document with a single
cluster verdict (`healthy` / `degraded` / `critical`) and, per subsystem, a
verdict, machine-readable `reason_codes`, and a `drill_down` pointer to the
endpoint that investigates it. Read this first instead of correlating
`/workers/health`, `/admin/shards/health`, `/dead-letters/aggregate`, and
`/workflows?no_progress_minutes=N` by hand: it tells you the overall verdict
and the single worst subsystem in one request, then points you straight at
where to look next.

### Example response

```json
{
  "status": "degraded",
  "as_of": "2026-07-09T14:32:05Z",
  "subsystems": [
    {
      "name": "workers",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "active": 4,
      "draining": 0,
      "unhealthy": 0,
      "total": 4
    },
    {
      "name": "shards",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "ready": 2,
      "degraded": 0,
      "unavailable": 0
    },
    {
      "name": "dead_letters",
      "status": "degraded",
      "reason_codes": ["dlq_backlog"],
      "drill_down": "/dead-letters/aggregate",
      "total": 7,
      "newest_entry_age_secs": 5400
    },
    {
      "name": "queues",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "max_backlog": 12,
      "max_backlog_queue": "default"
    },
    {
      "name": "stalled_workflows",
      "status": "healthy",
      "reason_codes": [],
      "drill_down": null,
      "count": 0
    }
  ],
  "unavailable_shards": []
}
```

Each subsystem block carries its verdict, its `reason_codes`, its headline
numbers (flattened into the block), and a `drill_down` path — which is
populated **only when the subsystem is not `healthy`** and is `null`
otherwise. The `drill_down` paths are relative to the management-API mount
(e.g. `/api/harvest`).

### Subsystem → drill-down

| Subsystem | Headline metrics | `drill_down` when non-`healthy` |
|---|---|---|
| `workers` | `active` / `draining` / `unhealthy` / `total` | `/workers/health` |
| `shards` | `ready` / `degraded` / `unavailable` | `/admin/shards/health` |
| `dead_letters` | `total` / `newest_entry_age_secs` | `/dead-letters/aggregate` |
| `queues` | `max_backlog` / `max_backlog_queue` | `/admin/shards/health` |
| `stalled_workflows` | `count` | `/workflows?no_progress_minutes=N` (N = `stalled_no_progress_minutes`, default 60) |

### How the verdict is computed

The top-level `status` is the **worst of** the five subsystem verdicts —
`critical` dominates `degraded` dominates `healthy`. It never fails wholesale:
an **unreachable shard degrades the affected subsystems** (each non-shard
subsystem that could not read complete data drops to at least `degraded` and
carries the `shard_unreachable` reason code; the `shards` subsystem reflects it
through its own readiness) and the shard is **named in `unavailable_shards`**
rather than returning a `500`. So a partial answer is always honest, never a
silent success and never an error page.

Once you have the verdict, follow the worst subsystem's `drill_down` into the
matching per-domain endpoint — those map directly onto the alert sections
below (e.g. `dead_letters` → `harvest_dlq_growth`, `queues` /
`stalled_workflows` → `harvest_queue_schedule_to_start_high`, `workers` →
`harvest_no_active_workers` / `harvest_worker_saturation`, `shards` →
`harvest_shard_unready`).

### Partial cross-shard list reads (issue #756)

The cross-shard **list/aggregate read** endpoints — `GET /workflows` (including
the `?no_progress_minutes` stalled loader), `GET /workers`,
`GET /workers/health`, `GET /admin/schedules`, `GET /dead-letters`, and
`GET /dead-letters/aggregate` — **degrade instead of failing** when one shard's
pool is unreachable. Rather than turning a single down shard into a whole-request
`500`, they return `200 OK` with the union of the *reachable* shards' results
plus a machine-readable partiality indicator:

```json
{
  "workflows": [ /* … reachable-shard rows … */ ],
  "status": "partial",
  "unavailable_shards": [ { "shard_id": 3, "reason": "database connection for shard 3 could not be acquired" } ]
}
```

A `status` of `partial` (some shards read) or `unavailable` (none read), and a
non-empty `unavailable_shards`, is a **healthy degradation, not an error** — the
data you see is real, just incomplete, and the down shard is *named* rather than
silently dropped. On the happy path (every shard reachable) these endpoints
return their unchanged legacy shape (a bare JSON array, or the paginated
`{ workflows, next_cursor }` object), so there is no change for existing clients.

When you see `status: partial`/`unavailable`, drill into
**`GET /admin/shards/health`** to identify *why* the named shard is down
(unreachable pool, mid shard-add rollout, schema-unreadable) and act on the
`shards` subsystem verdict. The `harvest` CLI's `workflow list` / `worker list` /
`schedule list` / `dlq list` subcommands surface the same partiality as a
`WARNING: cross-shard read is partial; N shard(s) unavailable: …` notice line
above the data.

Note: cross-shard **writes** (batch reset, bulk DLQ replay/discard/redrive,
schedule pause/delete, completion-trigger create) deliberately keep strict
failure semantics — a write that could not reach every shard fails loudly rather
than silently applying to a subset. The single-execution business-id resolver
(`GET /workflows/by-id/…`, issue #805) likewise keeps returning `503` while a
shard is down, to avoid a false `404`.

To **halt new workflow starts** fleet-wide (or by name/queue/shard/owner) while
you investigate — the incident-containment lever above the per-run
pause/cancel/terminate — raise an admission gate. Which start producers honour a
gate, which are exempt-by-design, and how to watch the
`harvest.admission.bypassed` counter are documented in
`docs/operations/admission-gate-producers.md`.

### Configurable thresholds

The verdict boundaries are **starter defaults, not universal SLOs** — every
deployment tolerates a different DLQ depth, queue backlog, and stalled-run
count. Override them per environment with
`HarvestPlugin::with_status_thresholds(StatusThresholds { .. })`.

| Field | Starter default | Effect |
|---|---|---|
| `dlq_degraded_total` | `1` | DLQ total at/above ⇒ `degraded` (any dead letter is worth a look) |
| `dlq_critical_total` | `100` | DLQ total at/above ⇒ `critical` |
| `dlq_critical_recent_secs` | `300` | A dead letter newer than this ⇒ `critical` (active failure) |
| `queue_degraded_backlog` | `1000` | Busiest-queue claimable backlog at/above ⇒ `degraded` |
| `queue_critical_backlog` | `10000` | Busiest-queue claimable backlog at/above ⇒ `critical` |
| `stalled_no_progress_minutes` | `60` | No-progress window for the stalled count and drill-down link |
| `stalled_degraded_count` | `1` | Stalled-execution count at/above ⇒ `degraded` |
| `stalled_critical_count` | `50` | Stalled-execution count at/above ⇒ `critical` |
| `worker_degraded_unhealthy_fraction` | `0.25` | Unhealthy (stale) worker fraction at/above ⇒ `degraded` |
| `worker_critical_unhealthy_fraction` | `0.5` | Unhealthy fraction at/above ⇒ `critical` (zero active with any registered is always `critical`) |

### Where this fits among the health surfaces

- **`/admin/status` is the PULL triage surface** — one call, on demand, when you
  are already responding to an incident.
- The **static alert pack** (`docs/alerts/starter-pack-v0.1.0.json`, the rest of
  this runbook) owns **PUSH alerting** — it tells you *when* to look.
- **`/api/harvest/health`** is **liveness** — is the process up — not a rollup.
- **`/api/harvest/admin/preflight`** is **startup validation** — is this
  deployment safe to promote — run at deploy time, not during an incident.
- **`/api/harvest/admin/config`** is the **effective-config introspection**
  surface (issue #695): what config is this fleet actually running with, right
  now? Pull the resolved effective runtime configuration (secret-free,
  admin-gated) before guessing at a knob:
  `curl -s .../api/harvest/admin/config | jq`. Durations are milliseconds;
  unset ceilings are explicit `null`; secret-bearing fields (notification URLs,
  sharded pool) show only presence booleans/counts.

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
  (`harvest workflow get <execution_id>`); a single `WorkflowRedriven` event is
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

The primary signal is the server-side overdue gauge `harvest_schedule_overdue`
(issue #696): `1` for any active schedule that is past its own cadence grace
(`now − next_run_at > cadence step + jitter + scheduler tick`), computed from
the schedule's own `next_run_at` + cadence — no per-schedule interval needs
hand-encoding. Alert on `max by (kind, name) (harvest_schedule_overdue) > 0`; it
names the wedged schedule directly. The gauge is sampled by the worker (not the
scheduler tick), so a wedged tick or a dead scheduler is still detected as long
as any worker is alive; a *total* process outage (all workers and the scheduler
down) is caught only by the tertiary absence-of-runs expression, which must be
paired with an `up`/scrape-health signal.

### Triage steps

1. Run `harvest schedule list --output json` and look for `overdue: true`. The
   per-schedule `overdue` and `overdue_by_secs` fields (also on
   `GET /api/harvest/admin/schedules` and `/{id}`) name exactly which schedule is
   wedged and how many seconds past its slot it is.
2. Inspect `is_paused`, `auto_paused_at`, `next_run_at`, `last_run_at`,
   `exhausted_at`, and schedule kind for the overdue schedule.
3. Run `harvest preflight --output json` to confirm scheduler coverage.
4. Check queue backlog for the schedule's dispatch queue.

### Likely causes

Scheduler loop stalled or disabled, `next_run_at` wedged in the past, an HA fire
claim (#350) that never released, all scheduler replicas down, worker outage,
queue backlog, or shard readiness issue. (A schedule at `max_active_runs`, or one
paused/auto-paused/manual/exhausted, is deliberately not firing and reads
`overdue: false` — see below.)

### False positives

Intentionally-not-firing schedules are excluded from the overdue gauge by
construction (issue #696 AC3), so they never page: `is_paused`, `auto_paused_at`
set (#360), `Schedule::Manual`, and `end_at`/`max_runs`-exhausted schedules
(#478/#543) all read `overdue: false`. A schedule deliberately deferring because
it is at `max_active_runs` — including fires deferred into the #607 throttle
queue, which the at-capacity check counts exactly as the scheduler tick does — is
also suppressed; that is a capacity condition, not a stalled cron. The grace
window already absorbs jitter and one scheduler tick, so a healthy schedule
caught mid-tick is never flagged.

Transient / self-healing cases (add `for: 2m` to the primary rule so none of
these page — see below):
- **Just-resumed long-paused schedule** (F3): resume/unpause does not recompute
  `next_run_at`, so for ~1 scheduler tick after resuming a schedule that was
  paused for a long time, its stale far-past `next_run_at` can read `overdue`.
  It self-heals on the next tick (catchup advances `next_run_at`), well within
  the ~30s sampler cadence and a `for: 2m` hold.
- **Short-cadence clock skew** (F5): `lag = now − next_run_at` is measured on a
  different process (worker/API) than the one that wrote `next_run_at` (the
  scheduler), so fleet clock skew inflates the lag. Absorbed by grace's tick term
  and `for: 2m` for normal cadences; for very short cadences (few-second
  intervals) keep clocks NTP-synced and prefer a cadence ≥ your worst-case skew.

Stuck-firing case: a schedule that was overdue (gauge = 1) and then **deleted**
leaves its `harvest_schedule_overdue{kind,name}` series stuck at 1 until the
emitting worker process restarts (an inherent gauge property — no in-process
clear path for a vanished key). Cross-check the read: `GET /admin/schedules` no
longer lists the schedule, so the firing is for a ghost. A `for: 2m` hold plus a
metric-staleness/`up` check resolves it; treat it as cleared.

A *total* process outage (nothing emits the overdue gauge at all) is covered by
the tertiary absence-of-`harvest.schedule.runs` expression plus your
scrape-health/`up` signal, not by the overdue gauge.

### Safe actions

Configure the primary overdue rule with `for: 2m` (above the ~30s sampler cadence
and one scheduler tick) so the transient/self-healing cases above never page
while a genuine wedge (persists many minutes) still fires. Then: resume an
accidentally paused schedule, restore scheduler coverage, restart a wedged
scheduler replica (a stale HA claim self-releases after ≤30s), scale the dispatch
queue, or trigger a manual catchup only after idempotency is confirmed. Avoid
blind backfills while downstream systems are unhealthy.

### Escalation criteria

Escalate when a regulatory, billing, or customer-notification schedule reports
`overdue: true` for one required firing, or when catchup would exceed downstream
capacity.

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
3. Diagnose one specific execution on demand against the **currently-deployed**
   code with `POST /api/harvest/workflows/{id}/replay-diagnosis` (issue #614) —
   it returns the same `{kind, event_index, expected, actual}` vocabulary and,
   after a candidate rollback/fix is deployed, a `clean` verdict confirms the
   run will resume. See the **"Diagnose the divergence"** section of
   [`docs/runbooks/nondeterminism-block.md`](nondeterminism-block.md#diagnose-the-divergence-issue-614)
   for the curl and the confirm-a-fix workflow.
4. Check recent deployment history to see if a new release was shipped without proper version gating or routing protection.
5. Run replay tests on the workflow using the exported history to reproduce the non-determinism error.

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
   per-compensation error strings. If nothing is FAILED, the author caught
   the error and the run COMPLETED normally: also list recent completions
   (`harvest workflow list --state COMPLETED --workflow-name <name>
   --limit 20`) and search each run's history
   (`GET /api/harvest/workflows/{execution_id}/history`) for the
   `saga_compensation_failed:` durable marker (`MarkerRecorded`) or the
   `SagaCompensationFailed` error string, plus the failing compensation
   activity's `ActivityFailed` event — either path always yields an
   execution id to inspect.
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

## harvest_update_rejected_rate

**What to do when workflow updates are being rejected by their validators:**
the `harvest.update.rejected` counter (issue #684) fires once per workflow
update rejected by its **registered validator** before admission (a durable
pre-admission `422`). The metric's `workflow` + `name` labels name the
workflow type and the update. Scope is deliberately validator-only:
non-`RUNNING`/paused admission-state conflicts are surfaced to the caller as
errors, not counted here. A rejection is cheap and safe (the update never
becomes durable history), so this alert is about *callers failing to drive
the workflow*, not about engine health.

### Triage steps

1. Read the `workflow` and `name` labels to identify the workflow type and
   the update handler being rejected.
2. Inspect the registered validator and the caller's payload shape:
   `GET /api/harvest/workflows/types/{workflow_name}/handlers` lists the
   workflow's declared update handlers.
3. Correlate with recent deploys: a client that started sending a new/changed
   payload, or a workflow whose validator got stricter, is the usual cause.
4. Capture a sample rejected payload from the caller's logs and run it
   against the validator locally to confirm the rejection reason.

### Likely causes

- A client/version drift after a deploy: the caller's update payload no longer
  matches the workflow's validator (renamed/removed field, tightened bound).
- A buggy or misconfigured caller sending malformed update requests.
- A retry storm of an already-invalid payload (the caller retries a request
  the validator will always reject).

### False positives

Expected during a controlled client/workflow migration window where old and
new payload shapes briefly coexist. A small steady trickle can also be a
mis-behaving but non-critical caller. Alert on a sustained rise above the
workflow's own baseline, not an absolute floor — a healthy client should see
near-zero validator rejections.

### Safe actions

1. Pause or roll back the offending caller (or its recent deploy) if it is
   sending an invalid payload shape.
2. Fix the caller's payload to match the validator, or (if the validator is
   wrong) fix and redeploy the workflow's update handler.
3. No engine-side remediation is needed: rejected updates append nothing to
   history and never wedge a workflow.

### Escalation criteria

Ticket the workflow owner with a sample rejected payload and the validator's
reason. Escalate to the caller's owning team when the source is a client
deploy. Page only if the rejected update path is on a critical control flow
(e.g. an operator-driven pause/adjust update) and legitimate operators are
being blocked.

## harvest_signal_unhandled_rate

**What to do when workflows are leaving signals unconsumed:** the
`harvest.signal.unhandled` counter (issue #684) fires once per delivered
signal a workflow left unconsumed (no `wait_for_signal`/`receive_signal`, no
push handler) by the time it reached a **Completed or Failed** terminal
outcome. The `workflow` + `queue` labels name the workflow type and its task
queue. The signal **name is not a label** (issue #684, Codex P2 — signal names
come from the free-form send route and have no declared registry to bound them,
so they cannot be a bounded metric label); identify *which* signal was dropped
from the run's history / stack (below), not from the metric. Signals excused by
a lost signal-or-deadline race (issue #476) are never counted.

> **Known scope limitation — read this before trusting a *low* number.** This
> counter is emitted **only** from graceful `Completed`/`Failed` terminals
> reached through the workflow drive. Forced-failure / scanner terminal paths —
> **`TIMED_OUT`, `CANCELLED`, `TERMINATED`, parent-close cascade, and
> history-cap failure** — are driven to termination without a workflow drive, so
> there is no matcher to reconstruct which signals were consumed; those runs'
> undrained signals are **NOT counted here**. In particular this **excludes the
> motivating "stuck workflow that ignored a signal and then timed out" case**:
> to catch a run that is wedged past its deadline, watch the
> `harvest.workflow.timeout` counter and inspect the run with
> `GET /api/harvest/workflows/{execution_id}/stack` (which shows what it is
> blocked on and any pending, never-consumed signals). A low
> `harvest.signal.unhandled` rate does **not** imply "no signals are being
> dropped" — only "no signals are being dropped on runs that finished
> gracefully."

### Triage steps

1. Read the `workflow` (and `queue`) label to identify the workflow type. The
   signal name is not a label — identify the specific dropped signal from a
   run's history / stack in step 3.
2. Confirm whether that workflow is *expected* to consume that signal on every
   run, or whether the signal is a best-effort/late notification the workflow
   deliberately ignores.
3. List recent runs of the workflow type and inspect a completed run's history
   for the delivered-but-unconsumed `SignalReceived`:
   `GET /api/harvest/workflows?workflow_name=<name>&state=COMPLETED`, then
   `GET /api/harvest/workflows/{execution_id}/history`.
4. Correlate with recent workflow deploys: a removed or renamed
   `wait_for_signal`/`receive_signal`/`register_signal_handler` call is a
   common cause.

### Likely causes

- A race between a signal-sending caller and the workflow's own control flow
  (the signal arrives after the workflow already decided to finish).
- A workflow deploy that removed or renamed the handler for a signal callers
  are still sending.
- A caller sending a signal the workflow never handled (wrong signal name).

### False positives

A small steady rate is often legitimate: late or duplicate signals a workflow
intentionally ignores (e.g. a second approval after the run already decided).
Alert on a change from the workflow's own baseline, not an absolute floor.

### Safe actions

1. If the workflow should handle the signal, restore or rename the
   `wait_for_signal`/`receive_signal`/`register_signal_handler` call and
   redeploy (in-flight runs replay deterministically; new runs pick it up).
2. If callers are sending the wrong signal, fix the caller.
3. No engine-side remediation is needed: an unhandled signal does not fail the
   run — the metric is purely an observability signal.

### Escalation criteria

Ticket the workflow owner with the signal name and a sample run id. Escalate
to the caller's owning team when the source is a caller sending an unexpected
signal. Page only if the dropped signal is on a critical control path (e.g. a
cancel/approval the run must observe) and business-critical runs are visibly
diverging.

## harvest_workflow_population_leak

**What to do when a workflow type's active population keeps growing:** the
`harvest.workflow.active` gauge (issue #770) reports the live count of
currently-active executions per workflow type, split by lifecycle state
(`running` / `paused`). This alert fires when a type's active population rises
steadily over an hour while its terminal completion rate (`harvest.workflow.terminal`)
stays effectively flat — i.e. starts are outpacing completions and executions
are accumulating rather than draining. That is the signature of a leak or a
backlog, not a healthy burst that later drains. `PAUSED` runs count under
`state="paused"`; nd-blocked runs (issue #603) stay `RUNNING` and count under
`state="running"`. The gauge is summed across every shard.

### Triage steps

1. Read the `workflow` and `state` labels to identify which type is growing and
   whether the growth is in `running` or `paused` executions.
2. List the active runs to see how long they have been in flight:
   `harvest workflow list --state RUNNING --limit 25` (or
   `GET /api/harvest/workflows?state=RUNNING`). A large paused population instead
   points at forgotten operator pauses (issue #383) — list `state=PAUSED`.
3. Compare the start rate against the terminal rate for the type
   (`harvest.workflow.started` vs `harvest.workflow.terminal`): a sustained gap
   confirms the imbalance.
4. Inspect a long-running instance with
   `GET /api/harvest/workflows/{execution_id}/stack` to see what it is blocked on
   (a slow/failing activity, an unfired timer, an unreceived signal, a stuck
   child workflow).
5. Check worker slot saturation (issue #531, `harvest.worker.slots_in_use` vs
   `slots_available`) and queue depth — a starved fleet lets active runs pile up
   even when each is healthy.

### Likely causes

- Workers are saturated or under-provisioned, so runs start faster than they can
  be driven to completion (a backlog, not a bug).
- A downstream dependency an activity calls is slow or failing, so runs park mid-
  flight and never finish.
- A workflow bug that leaves runs waiting forever (a signal that never arrives, a
  timer that is never set, an unbounded loop that never continues-as-new).
- A large batch of operator pauses that were never resumed (growth concentrated
  in `state="paused"`).

### False positives

A legitimate traffic surge (a scheduled batch, a marketing event) briefly raises
the active population and then drains as the runs complete — the terminal rate
rises alongside it, so the `and … terminal rate < 0.01` clause keeps the alert
quiet. Alert on population growth *paired with a flat terminal rate*, and tune
the `> 50` growth threshold to the workflow type's expected concurrency; a
high-throughput type may legitimately sit at thousands of active runs.

### Safe actions

1. If workers are saturated, scale the worker fleet (add replicas / raise
   `max_concurrent_workflows`) so the backlog drains.
2. If a downstream dependency is the bottleneck, remediate it; parked runs
   resume automatically once activities succeed.
3. If runs are genuinely stuck on a workflow bug, inspect one via `stack`, fix
   and redeploy the handler (in-flight runs replay deterministically), or
   cancel/terminate individual wedged runs.
4. If the growth is forgotten pauses, resume them
   (`POST /api/harvest/workflows/{execution_id}/resume`).

### Escalation criteria

Ticket the workflow owner with the type name and the current active count.
Escalate to page if the population growth is unbounded and accelerating, the
worker fleet is already at capacity, and business-critical runs are visibly
stalled (their `stack` shows no forward progress across successive checks).

## harvest_queue_paused_too_long

**What to do when a task queue has been held by an operator for too long:** the
`harvest.queue.paused` gauge (issue #619) reports `1` for every queue whose
dispatch is currently held and `0` otherwise. A pause is *safe* — nothing fails,
retries, or dead-letters, and already-`RUNNING` tasks finish naturally — but it
is also *silent*: work accumulates as `PENDING`, no other alert fires, and a
forgotten pause is indistinguishable from a healthy idle queue until the backlog
breaches an SLA. This alert exists to make a forgotten freeze loud.

> **"Nothing fails" has one exception: `schedule_to_close`.** A pause suspends
> only the *relative* `schedule_to_start` timer. An activity carrying an
> **absolute** `schedule_to_close` deadline (issue #378) is still timed out
> while the queue is held — a pause is not an SLA extension. The longer the
> hold, the more of the held backlog can cross its own absolute deadline, so
> treat this alert's `for` window as a real budget, not a formality. Crediting
> held time back to `schedule_to_close` is explicitly out of scope for issue
> #619.

Because the gauge is binary, the **`for` duration is the threshold**, not the
value. Alert on `max by (queue) (harvest_queue_paused) > 0` with `for: 1h`
(ticket) or `for: 4h` (page).

### Triage steps

1. **See what is held and why.** The read endpoint returns the reason, the
   operator who placed the hold, when it was placed, and how much work is
   waiting behind it:

   ```bash
   harvest queue list-paused
   # or: curl -s .../api/harvest/admin/queues/paused | jq
   ```

   The Vantage **Workers** page shows the same thing as a banner — the fleet
   page an operator lands on when investigating idle workers.

   Check `effective_scope` on each entry (the banner's **Scope** column shows
   the same thing). It reports what the hold *actually* covers, derived from the
   shards that hold the queue versus the expected shard set — not the
   `scope_shard_id` intent recorded when the row was written:

   - `fleet` — every expected shard holds it.
   - `partial_fleet` — **some shards are still dispatching.** A fleet-wide pause
     that only reached part of the fleet writes `scope_shard_id: null` on the
     shards it reached and *no row* on the ones it missed, so intent alone cannot
     tell this apart from a complete hold. Re-issue the pause (it is idempotent);
     the original reason and operator are preserved. An unreachable shard also
     reports `partial_fleet` — it may still be dispatching, so that is the safe
     reading.
   - `shard` — a deliberately shard-scoped hold.

2. **Confirm the downstream is actually back.** The reason field records why the
   hold was placed (`"SMTP provider outage"`). Verify that dependency is healthy
   before thawing, or the backlog will simply fail on release.

3. **Check how much is held.** `held_task_count` on the read endpoint is the
   number of `PENDING` tasks on the queue. A large number means the thaw will
   produce a burst — see *False positives* below for why that burst is safe.

4. **Resume.**

   ```bash
   harvest queue resume email-workers
   ```

   Held tasks become immediately claimable. The response echoes the
   `released_reason` and `released_paused_by` of the hold you just released —
   the pause row is deleted on resume, so this is the last moment the *why* is
   recoverable *from that row*.

   For a **post-incident** review after the row is gone, read the audit log
   instead — who/why/when survives the resume permanently there:

   ```bash
   harvest audit list --operation queue.pause --target-id email-workers
   harvest audit list --operation queue.resume --target-id email-workers
   ```

   The reason is carried in the record's **`error_summary`** field, which is the
   only free-text column on `harvest_audit_log` (there is no metadata column) —
   so it is populated on `succeeded` rows too, not just failures. It reads:

   - `reason: <why>` on a `queue.pause` row;
   - `reason: released hold by <actor>: <why>` on the matching `queue.resume`
     row (with `(shards disagreed; longest hold shown)` appended when the shards
     carried different holds);
   - `... | failures: <detail>` appended on either when the operation only
     partially applied, naming the shards it did not reach.

5. **Confirm the thaw.** `harvest queue list-paused` should now be empty and the
   `harvest_queue_depth` / `harvest_queue_oldest_pending_age` panels should
   start falling.

### Likely causes

- **A genuine, still-ongoing downstream outage.** The hold is doing its job; the
  alert is telling you the outage has outlasted its expected window.
- **A forgotten hold.** The incident was resolved but nobody resumed the queue.
  This is the failure mode the alert exists for.
- **A pause placed on the wrong queue name.** Check `list-paused` against the
  queue you meant to freeze; a typo produces a hold on a queue nobody is
  watching. (A name with *surrounding whitespace* is rejected with a `400`
  rather than accepted: queue names are matched exactly, so a stray space would
  hold a different queue than the one intended.)
- **A shard-scoped pause left behind** after a fleet-wide one was released
  (`shard_id` is surfaced on the read endpoint).
- **A fleet-wide pause that only partially applied** — `effective_scope:
  "partial_fleet"`. The shards it missed never stopped dispatching; re-issue it.

### False positives

- **A short, deliberate freeze during a planned dependency migration.** Expected —
  this is why the rule uses a `for` duration rather than firing immediately.
  Tune `for` to the longest freeze your team plans for.
- **A pause on a genuinely idle queue.** The hold is harmless because there is
  nothing to hold. Use the sharper alert variant in the pack
  (`harvest_queue_paused > 0 and harvest_queue_depth > 1000`) to only fire when
  the hold is actually accumulating a backlog.
- **A rising `harvest_queue_depth` while this gauge is `1` is NOT a capacity
  incident.** It is the deliberate hold working as designed. This pairing is the
  single most common false page during an outage freeze — check this gauge
  before escalating a backlog alert.
- **`harvest_queue_schedule_to_start` does not spike on a thaw.** Resume credits
  the held time back to each task's `scheduled_at`, so a queue held for six
  hours does not report six hours of schedule-to-start latency on release. The
  credit is measured against the live clock at release, so the time the resume
  itself takes — waiting on its queue lock and shifting a large backlog — is
  credited too, and does not count against any task's `schedule_to_start`.

### Safe actions

- `harvest queue list-paused` — read-only.
- `harvest queue resume <queue>` — releases the hold. Idempotent: resuming a
  queue that is not paused is a success no-op, so a retry after a lost response
  is safe.
- `harvest queue pause <queue> --reason "<why>"` — re-freezing is idempotent and
  **preserves the original reason and operator**, so a re-pause never overwrites
  the provenance of an existing hold.
- Both mutating commands **exit non-zero when a fleet-wide hold only partially
  applied** (HTTP `207`): the shards the request missed keep dispatching into the
  outage, so a scripted `harvest queue pause` step fails loudly rather than
  reporting success. Re-issue it — both operations are idempotent — and confirm
  with `harvest queue list-paused` that `effective_scope` reads `fleet` rather
  than `partial_fleet`.
- Inspecting a held task with `harvest workflow stack <id>` or the eligibility
  explainer (`GET /admin/queues/{queue}/eligibility`), which reports
  `queue_paused` as the first impediment.

### Escalation criteria

Escalate to the team that owns the downstream dependency named in the pause
reason. Page if the held backlog is breaching a business SLA and the dependency
has no ETA — at that point the decision is a business one (keep holding and
accumulate, or resume and let the work fail into the DLQ where it can be
redriven later).
