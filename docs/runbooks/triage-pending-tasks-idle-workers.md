# Triage: pending tasks, idle workers

Use this runbook when an alert fires indicating that tasks are sitting in `PENDING` state on a queue while connected workers appear idle.

> **Start here for a single wedged run: `GET /workflows/{exec_id}/diagnose`.**
> Issue #809 collapses the correlation this runbook otherwise walks you through
> — the pending stack, per-queue worker liveness, the circuit registry,
> rate-limit / concurrency saturation, queue pauses, and the task row's own
> retry timing — into **one** admin-gated, read-only call that names the actual
> root cause:
>
> ```bash
> harvest workflow diagnose <exec-id>            # human-readable verdict
> harvest workflow diagnose <exec-id> --json     # raw body
> curl -s "$HARVEST/api/harvest/workflows/$EXEC/diagnose" | jq .
> ```
>
> The response leads with a one-word `health` (`healthy` | `stalled` |
> `blocked_external` | `terminal`) and a discriminated `blocked_on` object:
>
> | `blocked_on.type` | `health` | What to do |
> |---|---|---|
> | `activity_no_worker` | `stalled` | **No live worker can run this row.** For a *pending* row that means nothing `Active` can claim it — deploy/scale a worker for `queue`, or fix the queue name (a `Draining` worker claims nothing new, so a drain-only fleet reports this too). For an *already-held* row it means the recorded claimant itself is gone — a poison-pill orphan (#367) a peer cannot pick up. Cross-check with `GET /admin/queue-coverage` (#774). |
> | `activity_circuit_open` | `stalled` | The breaker for `activity_name` is open until `cooldown_until`. Read `phase` first: `half_open` means a probe is admitted **right now** and the breaker closes on success — no operator action. `open` with a `cooldown_until` self-heals at that instant; `open` with the field **absent** was operator-forced, admits no timed probe, and needs an explicit `force-close`. See `docs/runbooks/activity-circuit-breaker.md`. |
> | `workflow_no_worker` | `stalled` | **No live worker polls the queue the run's own decision cycle sits on.** Nothing individual is wedged — the workflow task itself can never be claimed. Also covers a claim left behind by a crashed worker (a poison-pill orphan, #367) and a task deferred to a future `scheduled_at` that nothing will claim when it arrives. Same remedy as `activity_no_worker`, applied to the workflow queue. |
> | `no_pending_work` | `stalled` | A `RUNNING` run with nothing pending and a *parked* workflow task — executor loss / lost task. Consider a redrive. |
> | `activity_retrying` | `healthy` | Backing off; `last_error` says why, `next_attempt_at` says when. |
> | `activity_deferred` | `healthy` | Pushed forward by the dispatcher with **no failure recorded** — a dispatch-time rate-limit deferral (#699/#369), session capacity (#606), or a capability-miss redelivery (#804). Distinct from `activity_retrying`: nothing failed, so do not go hunting an error. |
> | `activity_rate_limited` / `activity_concurrency_deferred` | `healthy` | Deliberately paced by `key`. Raise the limit or wait. |
> | `activity_queue_paused` | `blocked_external` | An operator paused `queue` (#619). Resume it. |
> | `sleeping_timer` | `healthy` | A durable sleep until `fires_at` — **not** a stall, however old the last event is. Reported only when the run's own workflow task is actually claimable: a timer fires when a worker claims that task, so a paused or pollerless workflow queue reports `workflow_queue_paused` / `workflow_no_worker` instead. |
> | `timer_overdue` | `stalled` | A timer was due at `fires_at` (`overdue_by_seconds` ago), nothing fired it, **and that timer owned the run's wake** (the workflow task is `PENDING` long past its own `scheduled_at`). Timers fire only when a worker claims the owning workflow task, so this is a lost-task wedge — same remedy as `no_pending_work`. A timer armed alongside another wait (an external handoff, a child, a signal) does *not* own the wake and is reported under that wait instead, so an overdue row there is expected, not a stall. |
> | `pending_child` | `healthy` | Waiting on `child_exec_id`; diagnose *that* id next. |
> | `awaiting_signal` / `awaiting_external_handoff` | `blocked_external` | Waiting on an external party. Nothing engine-side is wrong. |
> | `workflow_queue_paused` | `blocked_external` | An operator paused the queue the run's own decision cycle sits on (#619). Resume it. Outranks every timer verdict, because a held queue stops the claim that would fire the timer. |
> | `workflow_no_worker` | `stalled` | Nothing polls the queue the run's own decision cycle sits on. Start a worker for `queue`. Like the pause above, this outranks a timer verdict — an unclaimable task can never fire its timer. |
> | `awaiting_replay_wait` | `blocked_external` | Parked on a durable wait that leaves no side-table row — `wait_kind` names which: `condition` (a `ctx.await_condition` park), `mutex` (#691 — `name` is the contended key; find the holder), `update`, or `external_workflow`. Only the replay-derived wait set can see these, so `wait_set` will read `replayed`. |
> | `paused` | `blocked_external` | Operator-paused (#383); resume when ready. |
> | `non_determinism_blocked` | `stalled` | **Replay diverged and the engine blocked the run (#603) rather than failing it.** `reason` is the recorded divergence, `since` when it was first blocked and `block_count` how many times it has re-fired. The run re-pends its workflow task on a capped-exponential backoff **indefinitely**, so by row shape alone it is identical to a healthy deferral. Roll the divergent build back (or forward-fix it) — see `docs/runbooks/nondeterminism-block.md`. Outranks every side-table cause, because the block re-fires on every decision cycle whatever those rows are doing; the activities' own impediments still appear in `contributing_reason_codes`. |
> | `healthy_in_progress` | `healthy` | Genuinely running — an activity in flight, or the run's own decision cycle claimed by a live worker or awaiting dispatch on a covered queue. |
>
> `contributing_reason_codes` lists **every** claim-time impediment on the
> diagnosed activity, not just the highest-precedence one — so a task that is
> both rate-limited *and* on a paused queue shows both. In a fan-out the
> **worst** slot wins, so one wedged slot among nineteen healthy ones is never
> masked.
>
> The endpoint is read-only: it appends no events, mutates no task-queue row,
> and never advances a circuit breaker's phase, so it is safe to poll. Reach for
> the per-endpoint triage below when you need the raw underlying detail, or when
> you are diagnosing a **queue** rather than a single execution.

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

#### `rate_limit_exhausted`
- **Meaning**: The tasks have a rate limit key, and the rate limit bucket for this key has fewer than 1.0 token.
- **Fix**:
  - Wait for the rate-limit bucket to refill.
  - Or increase the rate limit refill rate or burst size.
- **Note**: For an activity that *also* declares a circuit breaker, rate limiting is enforced at dispatch rather than at claim (issue #369), so a saturated bucket never surfaces as `rate_limit_exhausted` for it — you will see `circuit_open`/`circuit_half_open` instead.

#### `circuit_open`
- **Meaning**: The task's next activity is guarded by a circuit breaker that is currently **open** — repeated retryable failures (a downstream outage) tripped it, so dispatches fast-fail until the cooldown elapses. No worker will claim the task while the breaker is open.
- **Fix**:
  - Wait for the breaker's cooldown to elapse; it then admits a single probe dispatch and, if that succeeds, closes automatically.
  - Inspect the breaker: `GET /admin/circuits/{activity_name}` (or `GET /admin/circuits` for the full list) to see `last_trip`, `rolling_failure_count`, and `time_until_probe_secs`.
  - Once the downstream has recovered, force it closed to resume immediately: `POST /admin/circuits/{activity_name}/force-close`.
  - **Caveat**: the explainer reads the breaker with a non-mutating snapshot, and the Open→HalfOpen transition is driven only by an admitted probe dispatch — so a breaker whose cooldown has already elapsed still reads `circuit_open` here until the next dispatch admits a probe.
- **Scope**: `circuit_open`/`circuit_half_open` are surfaced only when the breaker is actually **open** or **half-open**. A CB activity whose breaker is **closed** but whose rate-limit token bucket is exhausted will *not* surface any claim-time impediment here: for a CB activity rate limiting is enforced at dispatch, not at claim (issue #369), so its stall is diagnosed via `GET /admin/circuits/{activity_name}` and the activity's rate-limit bucket — not this eligibility explainer.

#### `circuit_half_open`
- **Meaning**: The task's next activity breaker is **half-open** — the cooldown elapsed and a single probe dispatch is in flight to test whether the downstream has recovered. Tasks other than the probe remain blocked while the breaker decides. This is a normal, transient recovery state.
- **Fix**:
  - Usually no action: the probe resolves within one attempt and the breaker either closes (recovered) or re-opens (still failing).
  - If it persists, the downstream is flapping — inspect `GET /admin/circuits/{activity_name}` and treat it as a `circuit_open` outage.

---

### 5. `queue_paused` — an operator is deliberately holding this queue

**What it means**: An operator has paused dispatch on the whole task queue
(issue #619), typically to ride out a downstream outage without failing,
retrying, or dead-lettering the affected work. **This is not an incident.** Held
tasks stay `PENDING`, already-`RUNNING` tasks finish naturally, and the
`schedule_to_start` timer is suspended for the duration of the hold.

> **A pause does not stop the `schedule_to_close` clock.** Only the *relative*
> `schedule_to_start` timer is suspended. An activity declaring an **absolute**
> `schedule_to_close` deadline (issue #378) will still be timed out during a
> long hold — a pause is not an SLA extension. Before holding a queue for longer
> than the shortest `schedule_to_close` on it, check what the affected
> activities declare; that work will fail on its own deadline even though the
> queue is frozen. Crediting held time back to `schedule_to_close` is explicitly
> out of scope for issue #619.

It is listed **first** in the eligibility explainer's reason set precisely
because it is the operator's own deliberate action — check it before chasing a
capacity or routing cause.

**How to verify**:

```bash
harvest queue list-paused
# or: curl -s .../api/harvest/admin/queues/paused | jq
```

The response gives, per held queue: `reason` (why), `paused_by` (who),
`paused_at` (since when), and `held_task_count` (how much work is waiting). The
same information appears as a banner on the Vantage **Workers** page, and the
`harvest.queue.paused` gauge reads `1` for held queues.

**This is the single most common cause of a false capacity page.** A rising
`harvest_queue_depth` while `harvest_queue_paused` is `1` is the hold working as
designed — not a shortage of workers.

**Corrective Actions**:
- Confirm the downstream named in `reason` has actually recovered.
- Release the hold:

  ```bash
  harvest queue resume email-workers
  ```

  Held tasks become claimable immediately. The time they spent held is credited
  back to their `scheduled_at`, so a thaw does **not** retroactively
  schedule-to-start-time-out the backlog, and the schedule-to-start histogram
  does not spike on release.
- If the outage is still ongoing, leave the hold in place. The
  `harvest_queue_paused_too_long` alert exists to make sure a forgotten freeze
  eventually gets attention — see
  [that runbook section](harvest-alerts.md#harvest_queue_paused_too_long).

---

### 6. `healthy`

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

## What created this run? (start_source provenance)

Every workflow execution durably records a bounded `start_source` classifier (issue #740) naming the mechanism that started it, plus optional `start_source_ref` (a correlating id — schedule id, triggering execution id, idempotency key) and `started_by` (an operator/actor). Use these to answer "where did this run come from?" during a spawn storm without grepping logs.

**Bounded source set:** `api`, `schedule`, `backfill`, `signal_with_start`, `update_with_start`, `completion_trigger`, `webhook`, `child`, `batch`, `continue_as_new`, `reset`, `outbox`. A row started before the upgrade (or with no classifier) reports `unknown`.

### An unexpected run appeared — find what started it

Read the provenance fields off the single execution:

```text
GET /api/harvest/workflows/{exec_id}
```

or from the CLI:

```bash
harvest workflow get <exec_id>
```

The response's execution object carries `start_source` (always present; `unknown` for pre-upgrade rows), and — when set — `start_source_ref` and `started_by`. Example: a run showing `"start_source": "completion_trigger"` with `"start_source_ref": "<triggering exec id>"` was spawned by that upstream workflow finishing, not by a client call — follow the ref to the culprit.

### The fleet is flooded — isolate the runaway mechanism

Group/filter the list by `start_source` to find which start path is the source of the flood:

```text
GET /api/harvest/workflows?start_source=webhook
GET /api/harvest/workflows?start_source=completion_trigger&state=RUNNING
```

or from the CLI:

```bash
harvest workflow list --start-source webhook
harvest workflow list --start-source completion_trigger --state RUNNING
```

`start_source` accepts exactly one value from the bounded set above (or `unknown`); any other value returns `400` (never a silent empty match). Once you know the mechanism, go turn it off at the source — pause the offending schedule, disable the completion trigger, or throttle the webhook — rather than cancelling runs one at a time.
