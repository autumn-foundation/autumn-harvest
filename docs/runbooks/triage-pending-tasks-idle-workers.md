# Triage: pending tasks, idle workers

Use this runbook when an alert fires indicating that tasks are sitting in `PENDING` state on a queue while connected workers appear idle.

> **First check for a heartbeating activity.** Before assuming an activity is
> stalled, confirm it is actually stuck rather than just slow. Call
> `GET /workflows/{exec_id}/stack` and read the activity's
> `heartbeat_details` field on `pending_activities[]` (issue #503): it holds the
> latest progress checkpoint the activity reported via `ctx.heartbeat(...)`
> (e.g. `{"processed": 4500, "total": 10000}`). If the checkpoint is advancing
> across successive calls, the activity is making forward progress and no
> intervention is needed. A stale checkpoint (with a `last_heartbeat_at` far in
> the past) points to a genuine stall — continue with the eligibility triage
> below. `heartbeat_details` is `null` for activities that have not yet flushed
> a heartbeat and for local activities (which do not heartbeat).

To triage eligibility blocks, call the eligibility explainer API:

```text
GET /admin/queues/{queue_name}/eligibility
```

Or for a specific task ID:

```text
GET /admin/tasks/{task_id}/eligibility
```

The response contains a `summary.diagnosis` field which provides a headline explanation, along with lists of `eligible_workers` (workers passing all gates) and `ineligible_workers` (including reason codes for why they failed).

---

## Diagnoses and Corrective Actions

### 1. `no_online_workers`

**What it means**: There are no active workers polling this queue/shard that have sent a heartbeat recently.

**How to verify**:
Inspect the response body. `eligible_workers` and `ineligible_workers` will both be empty (or only show stale workers, which are excluded from the eligibility lists).

**Corrective Actions**:
- Check the worker processes. Are they running? Check their container/host logs.
- Verify worker network connectivity to the Postgres database shard.
- Scale up the replica count for worker deployments subscribed to the target queue.

---

### 2. `all_draining`

**What it means**: Workers are connected, but they have all been placed into `Draining` state. They are completing their active in-flight tasks but are refusing to claim any new ones from the queue.

**How to verify**:
Look at the `ineligible_workers` list. You will see workers with the reason code `worker_draining`.

**Corrective Actions**:
- If a rolling deploy is in progress, this is normal; wait for new workers to start and register.
- If this is accidental, or a deploy was canceled, undo the draining state on the worker fleet by restarting them or triggering a fresh deployment.

---

### 3. `all_capacity_full`

**What it means**: Eligible workers are connected and polling the queue, but they are all running at their maximum concurrency limit (`in_flight_count >= max_concurrency`).

**How to verify**:
Verify that the `eligible_workers` list is non-empty, but all workers have `in_flight_count` equal to or exceeding their `max_concurrency` (visible via the standard `/workers` endpoint or worker logs).

**Corrective Actions**:
- Scale out the worker fleet (increase replica count) to add capacity.
- If worker processes have spare CPU/RAM, consider increasing `max_concurrency` in the worker configuration.
- Check if tasks are running slower than usual (downstream outage, slow database queries), causing worker slots to back up.

---

### 4. `no_eligible_workers`

**What it means**: Workers are online and active, but none of them pass the gates required to claim the pending tasks.

**How to verify**:
Look at `ineligible_workers` and their `reason_codes`.

Common reason codes and their fixes:

#### `wrong_queue_subscription`
- **Meaning**: None of the online workers are subscribed to the queue name.
- **Fix**: Check the worker configuration and ensure they list this queue name in their queue subscriptions (e.g. `WorkerConfig::with_queues`).

#### `wrong_shard_assignment`
- **Meaning**: The workers are registered on this database, but their configured `shard_assignments` do not include the shard ID of the queue/task.
- **Fix**: Check worker shard assignment configuration. Ensure worker fleet covers all active shards.

#### `build_incompatible`
- **Meaning**: The pending tasks require a specific build ID (due to an active build policy), but connected workers are running an incompatible build.
- **Fix**: 
  - Deploy workers running the required build.
  - Or declare the current worker build compatible using:
    ```bash
    cargo run -p autumn-harvest-cli -- build-routing compat declare --build-id <worker-build> --compatible-with <task-required-build>
    ```
  - Or kick/update the build policy on the queue.

#### `sticky_owned_by_other_worker`
- **Meaning**: The task is sticky-pinned to a specific worker that is currently offline or busy, and the sticky preference window has not yet expired.
- **Fix**: 
  - Wait for the sticky window to expire (`sticky_until` timestamp in the task/eligibility response). Once expired, any worker can claim it.
  - Or restart the targeted worker process to let it resume the task.

#### `concurrency_saturated`
- **Meaning**: The tasks have a concurrency group key, and the number of currently running tasks with that key has reached the configured limit.
- **Fix**:
  - Wait for in-flight tasks of that concurrency key to finish.
  - Or increase the concurrency cap for that key/workflow.

#### `rate_limit_saturated`
- **Meaning**: The tasks have a rate limit key, and the rate limit bucket for this key has fewer than 1.0 token.
- **Fix**:
  - Wait for the rate-limit bucket to refill.
  - Or increase the rate limit refill rate or burst size.

---

### 5. `healthy`

**What it means**: Eligible workers with available capacity are connected and polling.

**How to verify**:
`eligible_workers` is non-empty and has workers where `in_flight_count < max_concurrency`.

**Corrective Actions**:
- If tasks are still not being claimed, verify if Postgres lock contention or transaction isolation issues are blocking `claim_task` (e.g. locks held by another transaction).
- Verify the Postgres LISTEN/NOTIFY channel is healthy and worker threads are waking up.

---

## Where did the time go? (per-execution timeline)

When a *single* execution is slow — rather than a whole queue backing up — reconstruct exactly where its wall-clock time went from recorded history, without re-running anything:

```text
GET /api/harvest/workflows/{exec_id}/timeline
```

or from the CLI:

```bash
harvest workflow timeline <exec_id>
```

The response projects the execution's `harvest_events` into ordered `steps` (activities, local activities, timers, child workflows, signal waits, side effects) plus a `rollup`. It is purely read-only: no new events are appended and no state is recomputed. An unknown execution — including a classic DAG run, which is not on the standard execution path — returns `404`; a malformed id returns `400`. The timeline surfaces only durations, names, ids, and outcomes (never `input`/`output`/`payload`/`error` values), so it is safe to expose to on-call without payload-decode concerns.

**Read the `rollup` first**, then use `slowest_step` and the per-step split to attribute the time to one of three buckets:

- **Queue-wait** (`wait_ms` on an `activity` step, i.e. `scheduled_at → started_at`): the task sat in `PENDING` waiting for a free worker slot. → **add workers / capacity** (see the eligibility triage above). A large `rollup.wait_ms` dominated by activity waits points here.
- **Activity / child-workflow execution** (`exec_ms` on an `activity` step, or an `activity`/`local_activity`/`child_workflow` step's `total_ms`): real work took a long time. → the **downstream is slow** (external service, DB, the child's own logic). Drill into the child with its own `.../timeline`, or check the downstream dependency.
- **Timer / signal wait** (`timer` and `signal_wait` steps, counted in `rollup.wait_ms`): the run was *intentionally* parked. A long `timer` step is a durable sleep the workflow asked for; a long `signal_wait` step is a human/callback the workflow was waiting on. → usually **not a bug** — confirm the wait is expected before escalating.

**Wait/exec split availability.** The `wait_ms`/`exec_ms` split is only populated for regular activities, and only when an `ActivityStarted` event was recorded (i.e. the activity was actually claimed by a worker). When both are `null`, only `total_ms` is meaningful — this is always the case for local activities, timers, child workflows (the parent records no child-start timestamp), **external activities** (dispatched via `ActivityAwaitingExternal` and completed/failed through the management API — they render as `activity` steps with no claim/start event, so they are often the longest-running work with `total_ms` only), signal waits, and side effects. A **regular** activity (one reporting an `attempt`) showing `total_ms` but a `null` split never reached a worker — it was still queued (`pending`) or timed out while queued (schedule-to-start) — so its whole `total_ms` is queue-wait and the `rollup` attributes it to `wait_ms`. **External** activities are also `activity` steps with a `null` split, but they report no `attempt` and are downstream work, so the `rollup` attributes their `total_ms` to `busy_ms` (like `child_workflow`).

**Retried-activity `wait_ms` is an upper bound.** For a retried activity the split is measured against the *last* attempt's start, but `scheduled_at` is the *original* schedule event — so `wait_ms` spans all prior attempts' execution and backoff, not only queue wait. Treat it as an upper bound on queue wait (sibling to the `attempt` field being a lower bound on the final attempt number).

**`busy_ms + wait_ms` need not equal `total_wall_clock_ms`.** The gap is unattributed orchestration/suspension time (the workflow coroutine deciding what to do next between steps). A large unattributed gap with small step totals usually means the run spent its time suspended between decisions rather than in any single step — check for slow replay or an overloaded worker rather than a slow downstream.

**Known limitation — unbounded history load.** The timeline loads the execution's *entire* event history with no `LIMIT` (a hard cap is deliberately avoided — silent truncation would make the rollup wrong). History size is bounded by workflow continue-as-new discipline (the ≤500-event target); a run that accumulates a very large history makes this read proportionally expensive.

A step whose `outcome` is `pending` (with a `null` `ended_at`) is still open; its `total_ms` is measured to "now", so a growing pending step on repeated calls is the currently-stuck point.
