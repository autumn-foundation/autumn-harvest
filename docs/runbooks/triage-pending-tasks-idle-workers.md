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

### 6. `activity_paused` — an operator is deliberately holding one activity type

**What it means**: An operator has paused dispatch for a single **activity
type**, fleet-wide (issue #807) — the surgical sibling of the queue pause above.
It is the right tool when one downstream dependency is broken but the healthy
work sharing its queue should keep flowing. **This is not an incident.** Held
tasks stay `PENDING`, already-`RUNNING` tasks finish naturally, and the
`schedule_to_start` timer is suspended for the duration of the hold.

> **A pause does not stop the `schedule_to_close` clock.** Exactly as for the
> queue pause above, only the *relative* `schedule_to_start` timer is suspended;
> an activity declaring an **absolute** `schedule_to_close` deadline (issue #378)
> will still be timed out during a long hold. Check what the held activity
> declares before freezing it for longer than that deadline.

Like `queue_paused`, it is listed **first** in the eligibility explainer's reason
set because it is the operator's own deliberate action — check it before chasing
a capacity or routing cause. A task can be held by **both** pauses at once; both
codes are reported, so resuming the one you see first does not silently leave the
task stuck behind the other.

**Reported only for `task_type='activity'` rows.** A *workflow* task can
legitimately carry a non-NULL `activity_name` (the engine stamps the
`'mixed_signal_suspension'` sentinel there), and the claim gate is scoped by
`task_type` so pausing an activity of that name cannot hold those workflow
tasks. The explainer matches that scoping, so `activity_paused` never appears
against a task the engine would happily dispatch.

**Distinguish it from `circuit_open`.** Both name one activity type, but they
answer different questions: `circuit_open` is the engine's *automatic, reactive*
breaker fast-failing after failures accumulated (issue #369), while
`activity_paused` is a human's *manual, proactive* hold that fails nothing. If
you see `circuit_open`, something broke on its own; if you see
`activity_paused`, someone decided.

**How to verify**:

```bash
harvest activity list             # every registered type + its pause state
harvest activity get charge_card  # just this one
```

`list` is driven by the registered **catalogue**, not the pause table, so a
healthy activity is listed with `paused: false` — this is the surface that
answers "is `charge_card` held?", and one that listed only the already-broken
things could not. A paused-but-**unregistered** activity is listed too, flagged
`registered: false`, so a mistyped hold can never become invisible on the one
page used to find it. Each entry carries `reason` (why), `paused_by` (who),
`paused_at` (since when), and `held_task_count` (how much work is waiting).

**Check the `SCOPE` column before trusting `PAUSED: yes`.** It reports what the
hold *actually* covers, derived from the shards that hold the activity versus
the expected shard set — not the `scope_shard_id` intent recorded when the row
was written:

- `fleet` — every expected shard holds it.
- `partial_fleet` — **some shards are still dispatching this activity.** A
  fleet-wide pause that only reached part of the fleet writes
  `scope_shard_id: null` on the shards it reached and *no row* on the ones it
  missed, so intent alone cannot tell this apart from a complete hold. The
  original request returned `207`, but a later read that reaches every shard
  looks `complete` and its rows agree — this column is what still says
  otherwise. Re-issue the pause; it is idempotent and preserves the original
  provenance.
- `shard` — every row is deliberately shard-scoped and they do not span the
  fleet. Expected for a scoped hold; suspicious if you asked for fleet-wide.

An unreachable shard counts as expected-but-not-holding — the safe direction,
since it may still be dispatching — so a `partial` read also reports
`partial_fleet`.

**Corrective Actions**:
- Confirm the downstream named in `reason` has actually recovered.
- If `SCOPE` reads `partial_fleet`, re-issue the pause **before** triaging
  anything else: part of the fleet never stopped.
- Release the hold:
  ```bash
  harvest activity resume charge_card
  ```
  Held tasks become claimable immediately, and the time they spent held is
  credited back to their `scheduled_at`, so a thaw does **not** retroactively
  schedule-to-start-time-out the backlog. The command exits non-zero if the
  release only partially applied (a shard was unreachable) — a `207` must never
  look like success, because the shards it missed are still holding.
- If the outage is still ongoing, leave the hold in place.
- If the *whole* queue needs holding rather than one activity, prefer the queue
  pause in §5; if many activity types on one queue are affected, holding the
  queue once is easier to remember to release than N activity holds.

**The full loop** — pause → triage → resume:

```bash
# 1. CONTAIN. Reason and actor are optional: containment must not wait on
#    paperwork. Effective on every shard and worker within one poll interval.
harvest activity pause charge_card --reason "payments provider 5xx" --actor alice

# 2. TRIAGE. The backlog is visible and bounded, and it is not decaying:
#    nothing is being failed, retried, or dead-lettered while held.
harvest activity get charge_card       # held_task_count, reason, paused_by
harvest workflow list --state RUNNING  # in-flight attempts finish naturally

# 3. RELEASE, once the downstream is actually healthy.
harvest activity resume charge_card
```

**Choosing between the three holds.** All three stop work; they differ in blast
radius and in who decides:

| | `harvest activity pause` (#807) | `harvest queue pause` (#619) | Circuit breaker (#369) |
|---|---|---|---|
| Scope | One **activity type**, fleet-wide | One **queue**, and everything on it | One activity type |
| Who decides | A human, proactively | A human, proactively | The engine, reactively |
| Effect on work | Held `PENDING`; nothing fails | Held `PENDING`; nothing fails | Fast-**fails** with `CircuitOpen` |
| Reach for it when | One downstream is broken and healthy types share its queue | The whole queue's dependency is down, or many types on it are affected | You want automatic protection, configured ahead of time |

An operator pause **wins over** a tripped breaker: the task is held before
dispatch, so the breaker never gets to fast-fail it. Resuming the pause does
**not** auto-close a tripped breaker — the breaker's own half-open probe still
governs recovery, so a resume cannot accidentally re-arm a broken dependency.

---

### 7. `healthy`

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
