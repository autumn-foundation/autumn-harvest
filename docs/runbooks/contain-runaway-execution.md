# Runbook: Contain a Runaway Execution (Pause / Resume)

A single workflow execution is misbehaving — hammering a downstream API in a
retry loop, spawning work you don't understand yet, or advancing through steps
you need to inspect before they commit — and you want to **freeze it in place
without losing it**. Cancelling or terminating destroys the run; pausing
(issues #383/#609) is the reversible containment lever: the execution stops
making forward progress, its history stays intact, and resuming later picks up
from exactly where it stopped with deterministic replay.

```bash
# Freeze it:
curl -sS -X POST "$HARVEST_URL/api/harvest/workflows/$EXEC_ID/pause" \
  -H 'content-type: application/json' \
  -d '{"reason": "INC-4821: investigating duplicate charge reports"}'

# ... investigate ...

# Un-freeze it:
curl -sS -X POST "$HARVEST_URL/api/harvest/workflows/$EXEC_ID/resume"
```

Both routes are admin-guarded and audited (`workflow.pause` /
`workflow.resume` audit operations, with your actor identity and request id on
the audit row; the pause **reason** is recorded on the execution row
(`pause_reason`) and the appended `WorkflowExecutionPaused` history event —
not in the audit log).

> **Pause is an orchestration freeze, not an activity kill switch.** Pause
> stops new workflow *decision cycles*; it does not reach into the activity
> layer. An activity task that was already scheduled before the pause is
> still claimed and executed, an in-flight attempt runs to completion, and a
> **failing activity keeps retrying** through its `RetryPolicy` attempt
> budget while the execution is paused (for an activity with no
> `schedule_to_close` and a generous attempt budget, pause does *not* stop a
> retry loop hammering a downstream — an activity with a `schedule_to_close`
> freezes once that deadline elapses mid-pause). If the incident is exactly
> that retry loop, pair the pause with a circuit-breaker force-open
> (`POST /admin/circuits/{activity}/force-open`, issue #369) for immediate
> downstream relief. See
> [What pause does and does not stop](#what-pause-does-and-does-not-stop).

---

## Pause vs cancel vs terminate — pick the right lever

| | **Pause** (`POST /workflows/{id}/pause`) | **Cancel** (`POST /workflows/{id}/cancel`) | **Terminate** (`POST /workflows/{id}/terminate`) |
|---|---|---|---|
| Reversible | **Yes** — `resume` continues the run from where it stopped | No — terminal | No — terminal |
| Workflow-body cooperation needed | **None** — enforced at the executor/claim layer, no author code required | Yes — the body must observe it (`is_cancelled()` / `check_cancellation()`); Saga compensation can run | **None** — unilateral seal at the durable/orchestration layer |
| Already-dispatched (in-flight) activities | Run to completion; results are recorded and queue behind the pause | Cooperative heartbeat cancellation, then grace-period hard-abort | Outstanding task-queue rows are failed |
| Terminal state produced | None — execution stays a non-terminal active run (`PAUSED`) | `CANCELLED` | `TERMINATED` |
| Completion triggers fired | None (nothing terminal happened) | `TerminalState::Cancelled` | `TerminalState::Terminated` (not `Cancelled` — force-kills are a distinct downstream event, issue #504) |
| What a result-awaiting caller sees | Keeps waiting (the run will still produce its real result) | Cancelled outcome | `HarvestError::Terminated` |
| Typical incident use | "Stop the bleeding while I look" — contain, inspect, then resume or escalate | Graceful stop of a run you no longer want, with compensation | Force-kill a wedged or hostile run |

Rules of thumb:

- **Not sure yet whether the run is actually bad? Pause.** It's the only
  option you can walk back.
- **Cancellation beats pause.** You can cancel (or terminate) a `PAUSED`
  execution directly — no resume required first. Escalating from
  "contained" to "killed" is one call.
- Pause is per-execution. For fleet-scale levers see
  [Which lever for which problem](#which-lever-for-which-problem) below.

## How to pause

### API

```bash
curl -sS -X POST "$HARVEST_URL/api/harvest/workflows/$EXEC_ID/pause" \
  -H 'content-type: application/json' \
  -d '{"reason": "INC-4821: runaway retry loop against payments API"}'
```

The body is optional — a bare `POST` with no body pauses with no reason. The
`reason` is capped at 500 characters (`400` if over) and lands on the
execution row (`pause_reason`) and in the appended `WorkflowExecutionPaused`
history event (the audit row records the actor, route, and request id, not
the reason) — always include the incident ticket.

Response (`200 OK`):

```json
{
  "ok": true,
  "execution_id": "0000a1b2-...",
  "state": "PAUSED",
  "reason": "INC-4821: runaway retry loop against payments API",
  "actor": "oncall@example.com",
  "newly_paused": true,
  "paused_at": "2026-07-07T14:03:22Z"
}
```

- `newly_paused: true` — this call performed the pause.
- `newly_paused: false` — the execution was already paused (idempotent
  retry; the original `reason`/`actor` are preserved, yours is not applied).
- `paused_at` — when the pause took effect (the anchor for the auto-resume
  ceiling and the deadline shift below).

Status codes: `200` paused (or already paused), `404` unknown execution id,
`409` the execution is already terminal (there is nothing left to contain —
see the matrix), `400` over-long reason.

### CLI

```bash
harvest workflow pause <execution_id> --reason "INC-4821: runaway retry loop"
harvest workflow resume <execution_id>
```

Both map 1:1 onto the endpoints above (same idempotency, same status
mapping).

### Resume

```bash
curl -sS -X POST "$HARVEST_URL/api/harvest/workflows/$EXEC_ID/resume"
```

```json
{
  "ok": true,
  "execution_id": "0000a1b2-...",
  "state": "RUNNING",
  "actor": "oncall@example.com",
  "pause_duration_secs": 1843.2,
  "newly_resumed": true
}
```

Resume is **idempotent**: repeating it against an execution that is not
paused — because your first call already landed, an auto-resume beat you to
it, or the run has since gone terminal — returns `200` with
`newly_resumed: false` and a zero pause duration rather than an error.
Only an unknown execution id is a `404`. Safe to retry blindly from
automation.

Resume wakes the parked workflow task immediately. Everything that queued up
during the pause — timers whose fire time elapsed, signals, completed
activity results — is processed **in its original order** on the next
decision cycle.

## What pause does and does not stop

Pause is enforced at the **claim layer**: `queue::claim_task` skips workflow
tasks whose owning execution is `PAUSED`, and a task claimed in the same
instant the pause lands is caught by an authoritative persist-time re-check
(the worker re-verifies `PAUSED` under the same row lock the pause takes, and
discards the pending decision if the pause committed first). No workflow-author
cooperation is needed and no code path in the workflow body runs while paused.

| Surface | Behavior while paused |
|---|---|
| Workflow decision tasks | **Never claimed.** A parked task woken by a timer fire, signal arrival, or activity completion stays `PENDING` until resume. |
| Already-dispatched activities | **Run to completion.** Pause does not reach into in-flight work; results are recorded normally and queue behind the pause. |
| Activity tasks enqueued before the pause but not yet claimed | **Still claimed and executed.** The claim-layer pause gate is scoped to workflow tasks (`task_type = 'workflow'`) — activity rows of a paused execution stay claimable. |
| Activity retries of an already-scheduled activity | **Continue while paused**, bounded only by the activity's `RetryPolicy` attempt budget; once the activity's `schedule_to_close_at` elapses mid-pause the row becomes frozen (unclaimable) until resume shifts the deadline. An activity with no `schedule_to_close` keeps retrying for its whole attempt budget — pair pause with a circuit-breaker force-open (#369) if the retries are the problem. |
| New activity dispatch | None — new activities are only scheduled by workflow decision cycles, which don't run. |
| Timers | The timer row still fires, but the workflow does not advance on it until resume (then fires immediately, in order). |
| Signals | Accepted and durable; delivered in order on resume. |
| Queries | **Still served** — you can inspect workflow-internal state mid-pause. |
| Updates | Rejected with `409` (`HarvestError::WorkflowPaused`) at admission — including `update-with-start` attaching to this run (#479). |
| Scheduler / batch operations | `PAUSED` counts as an **active** run everywhere (overlap policies, `max_active_runs`, batch cancel/signal target sets) — pausing does not free a schedule slot. |

## Deadlines are suspended, not burned

An investigation pause can never itself trip a timeout or SLA. On resume,
every forward-looking deadline is shifted by the (clamped, non-negative) time
spent paused:

- **Execution timeout `deadline_at` (#243)** — the hard-timeout scanner only
  scans `RUNNING` rows, so a paused run cannot time out mid-pause; resume
  pushes `deadline_at` forward by the pause span.
- **Soft SLA `sla_deadline_at` (#487)** — shifted forward the same way,
  **but only if the deadline was still ahead when the pause began**. A
  deadline that had already elapsed before you paused stays in the past, so
  the breach (which genuinely occurred while `RUNNING`) is still observed and
  counted after resume rather than silently pushed into the future.
- **Activity `schedule_to_close_at` (#378, closed by issue #609)** — the
  cross-retry wall-clock deadline is pause-aware end to end:
  - The `ScheduleToClose` timeout scanner skips tasks whose owning execution
    is `PAUSED` — both ordinary task-queue rows and external
    (`harvest_external_tasks`) tasks.
  - The worker's pre-requeue deadline check requeues (instead of
    deadline-failing) a retry whose owning execution is paused, and before
    ever failing a task terminally it re-validates the row-*current* deadline
    under the execution row lock — so a resume that shifted the deadline
    while an attempt was in flight can never deadline-fail it against the
    stale pre-shift value.
  - On resume, every still-open deadline-bearing row — `PENDING`/`RUNNING`
    task-queue rows and `PENDING` external tasks — has `schedule_to_close_at`
    shifted forward by the pause span.
  - A `PENDING` row whose deadline elapses *during* the pause becomes
    **frozen**: unclaimable (the claim query requires
    `schedule_to_close_at > NOW()`) and spared by the scanners until resume
    shifts the deadline. Resume also shifts a frozen row's `scheduled_at`
    forward by the pause span, restoring its `schedule_to_start` budget and
    retry-backoff position so the first post-resume scan doesn't instantly
    kill it.

  Unlike the SLA deadline above, this shift is **unconditional** — no
  elapsed-before-pause carve-out. The shift itself never revives a deadline
  that had already elapsed before the pause began (still elapsed after resume
  by arithmetic; the scanner times it out on its next tick), and a deadline
  still ahead at pause time retains exactly its remaining budget. Be aware of
  what that means combined with the table above: because activity tasks stay
  claimable during a pause, an activity whose deadline was still ahead when
  the pause landed can run and retry *during* the pause **and** keep its full
  remaining budget after the shift — so its total runnable wall-clock can
  exceed the configured `schedule_to_close` by up to the budget that remained
  at pause time.

Deliberately **unchanged**: per-attempt `start_to_close` and
`heartbeat_timeout` enforcement stay pause-blind — in-flight work still times
out on its own merits; pause governs orchestration progress, not a hung
activity attempt. `schedule_to_start` enforcement also stays live for a
paused execution's ordinary (unfrozen) pending activities — they remain
claimable, so their schedule-to-start clock is still a genuine
worker-capacity signal — with exactly one carve-out: the frozen rows above
(paused owner + elapsed `schedule_to_close_at`) are spared, since they are
unclaimable by construction until resume shifts them.

## The pause is not indefinite: the auto-resume ceiling

A pause left behind after an incident would otherwise strand the run forever.
The auto-resume scanner force-resumes any execution paused longer than
`WorkerConfig::max_workflow_pause_duration` — **default 24 hours** — with
`actor = "auto-resume(timeout)"` (recorded on the appended
`WorkflowExecutionResumed` history event and a warn-level log line, so a
surprise resume is attributable — note the scanner writes no `workflow.resume`
audit row; only the HTTP/CLI resume path is audited).

```rust
// Lengthen (or shorten) the ceiling fleet-wide:
WorkerConfig::default().with_max_workflow_pause_duration(Duration::from_secs(72 * 3600))
```

If your investigation will outlast the ceiling, either raise it or convert
the containment into a decision: resume, cancel, or terminate.

## Which lever for which problem

Per-execution pause is the **narrowest** containment lever. Reach for it when
*one specific run* is the problem. For broader blast radii:

| Problem | Lever |
|---|---|
| One specific execution is misbehaving | **This runbook** — `POST /workflows/{id}/pause` |
| A schedule keeps firing new runs you don't want | Schedule pause (#229): `POST /admin/schedules/{id}/pause` — stops future firings; already-running executions are untouched |
| New *starts* of a workflow type / queue must stop (deploy freeze, incident) | Admission gates (#377) — reject or defer new starts at the API boundary; in-flight runs are untouched |
| One activity's downstream dependency is down, fleet-wide — or a paused run's own activity retries are still hammering it | Circuit breaker (#369): `POST /admin/circuits/{activity}/force-open` — fast-fails that activity's dispatch everywhere, immediately. Pause does **not** stop activity claiming or retries (see above), so for immediate downstream relief pair the two. |
| The run is wedged and must die | Terminate (#504): `POST /workflows/{id}/terminate` |

These compose: during a serious incident it is normal to pause the schedule
(stop new runs), gate admissions (stop manual starts), and pause the one live
execution under investigation.

## Verify containment

1. **State and metadata** — `GET /api/harvest/workflows/{id}`: the embedded
   execution object shows `state: "PAUSED"` plus `paused_at`, `pause_reason`,
   and `pause_actor`. The history shows the appended
   `WorkflowExecutionPaused` event (activity results recorded during the
   pause may append after it).
2. **Fleet view** — `GET /api/harvest/workflows?state=PAUSED` lists every
   currently paused run (the filter composes with `workflow_name=`,
   pagination, etc.).
3. **No forward progress** — `harvest workflow stack <execution_id>` (or
   `GET /workflows/{id}/stack`) is frozen; the event count only grows by
   terminal activity outcomes of already-scheduled activities (completions,
   terminal failures, per-attempt timeouts — including activities claimed or
   retried *during* the pause), never by new `ActivityScheduled` /
   `TimerStarted` events. The parked workflow task row sits idle at `PENDING`
   (a task woken by a timer/signal/activity completion and deferred by the
   pause), or at `RUNNING` with `worker_id IS NULL` for a signal-parked or
   pause-discarded decision task — a `RUNNING` row with no `worker_id` is
   parked, not executing; don't misread it as a live decision cycle.
4. **Metrics** — `harvest.workflow.paused{workflow, queue}` incremented once
   on the pause; `harvest.workflow.pause_duration{workflow, queue}` records
   the span on resume. A sustained non-zero `PAUSED` population is worth a
   dashboard panel so forgotten pauses surface before the 24h auto-resume
   does it for you.
5. **Audit** — one `workflow.pause` (and later `workflow.resume`) row with
   actor and request id. The pause *reason* is not on the audit row — read it
   from the execution row (`pause_reason`) or the `WorkflowExecutionPaused`
   history event.

After resume, confirm the run advances again (state back to `RUNNING`, new
events appending) and — for a run with deadlines — that `deadline_at` (and
`sla_deadline_at`, if it was still ahead when the pause began) moved forward
by roughly `pause_duration_secs`.

## Related

- `docs/operations/admission-gate-producers.md` — pause/cancel/terminate act on
  a *single* execution. To halt **new** starts fleet-wide (or by
  name/queue/shard/owner) while in-flight work drains, raise an admission gate
  instead; that page documents which start producers honour a gate and which are
  exempt-by-design (with an observable bypass counter).
- `docs/runbooks/synthetic-incident-drills.md` — the
  `runaway-execution-containment` drill rehearses this runbook end-to-end in
  staging, including the deterministic-resume correctness check.
- `docs/runbooks/nondeterminism-block.md` — a run blocked on replay
  divergence is *already* frozen by the engine; pause/cancel/terminate remain
  the operator escape hatches there too.
- Issue #383 (Phase 3.23) shipped the primitive; issue #609 closed the CLI,
  resume-idempotency, and `schedule_to_close`-shift gaps this runbook
  documents.
