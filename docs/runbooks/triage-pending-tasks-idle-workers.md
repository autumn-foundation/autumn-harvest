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

> **Why is this activity retrying?** If a `pending_activities[]` (or
> `pending_local_activities[]`) entry has a `next_retry_at` in the future, it is
> backing off after a failure rather than blocked on eligibility. The same
> `GET /workflows/{exec_id}/stack` response now carries a `last_failure` object
> on that entry (issue #773). Read it to see *why* the activity keeps failing
> (e.g. a downstream `503`, a validation error) without a second round-trip or a
> manual `harvest_events` query. `last_failure` is omitted entirely for an
> activity that has never failed, so its presence alone flags a retrying task.
>
> **What's in `last_failure` depends on the activity kind** — because retryable
> failures are not event-sourced:
>
> - For a **backing-off regular (task-queue) activity** — the common retry-loop
>   case — the message comes from the task row's `error` column (what the worker
>   writes when it reschedules a retryable failure; no `ActivityFailed` event is
>   appended). You get `error` (the message) and `attempt`, with
>   `error_type: "Error"`, `non_retryable: false`, and `failed_at`/`details`
>   omitted (the task row has no failure timestamp or structured detail).
> - For a **local activity** (and any event-sourced failure) the full typed set
>   is present, including `failed_at` (the event timestamp).
>
> This reduced regular-activity shape is deliberate: event-sourcing every
> retryable failure would require a schema migration, which #773 intentionally
> avoids. Continue with the eligibility triage below only once you have ruled out
> a downstream-caused retry loop.

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
