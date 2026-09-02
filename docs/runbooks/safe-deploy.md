# Runbook: Safe Worker Drain Before a Deploy

Use the Harvest drain controls to quiesce one or more workers gracefully before
deploying a new version or taking a host out of rotation. This avoids dropped
in-flight tasks and mid-execution interruptions without needing SSH or raw
process signals.

**When to use:** rolling deploys, host maintenance, canary rollbacks, scheduled
scale-in events.

**When _not_ to use:** emergency kills — use SIGKILL directly and let Harvest's
retry/timeout logic recover the in-flight tasks.

---

## Step 1 — Identify the target worker(s)

List all active workers to find the candidates:

```bash
harvest worker list --status Active
```

Filter by queue or shard when operating a large fleet:

```bash
harvest worker list --queue email-workers --shard-id 0
```

Key fields in the response:
- `worker_id` — the string passed to `--drain` below.
- `in_flight_count` — tasks currently executing on this worker.
- `status` — `Active`, `Draining`, or `Stopped`.
- `health` — `healthy` (heartbeat recent) or `stale` (heartbeat expired).

---

## Step 2 — Dry-run with drain-preview

Before draining, confirm which workers would be affected:

```bash
harvest worker drain-preview --queue email-workers
```

`drain-preview` is read-only and never changes any state. The response lists
every matching active worker with its current `in_flight_count`.

---

## Step 3 — Request the drain

Drain a specific worker:

```bash
harvest worker drain <worker-id>
```

The server sets the worker's status to `Draining`. The worker will finish its
current tasks and then transition to `Stopped` within one heartbeat interval
(default: 5 s) after quiescing.

To specify an explicit deadline (RFC 3339):

```bash
harvest worker drain <worker-id> --deadline 2026-05-09T14:30:00Z
```

When `--deadline` is omitted the server uses the configured
`WorkerConfig::shutdown_timeout` (default 30 s from the current time).

### Drain outcome codes

| `outcome`         | Meaning                                                     |
|-------------------|-------------------------------------------------------------|
| `accepted`        | Drain requested; worker will quiesce and stop.              |
| `already_draining`| Worker is already draining; deadline was refreshed.         |
| `already_stopped` | Worker has already stopped; no action taken.                |
| `stale_worker`    | Worker heartbeat is stale; drain was written but the process may already be gone. |
| `not_found`       | Worker ID not found on any shard.                           |

---

## Step 4 — Wait for the worker to stop

### Option A — CLI wait mode (recommended)

The `--wait` flag blocks until the worker reaches `Stopped` or the timeout
expires, polling every 2 s:

```bash
harvest worker drain <worker-id> --wait --wait-timeout-secs 120
```

Exits 0 when the worker stops, exits 1 on timeout.

### Option B — Manual polling

```bash
watch -n 2 'harvest worker get <worker-id> | jq .status'
```

Wait until `"status": "Stopped"` appears.

### Option C — Management API

```http
GET /workers/<worker-id>
```

Poll until `status == "Stopped"`.

---

## Step 5 — Terminate the process

Once the worker is `Stopped` you can safely send SIGTERM (or SIGKILL) to the
process, redeploy the binary, or decommission the host. No in-flight tasks will
be lost.

---

## Degraded mode: unavailable shards

When a shard is temporarily unreachable the drain response includes
`unavailable_shards: [<id>, ...]` and an `outcome` of `not_found`. The worker
_may_ live on an unavailable shard. Retry the drain once the shard recovers, or
use `GET /admin/shards/health` to investigate.

---

## Drain audit trail

Every `POST /workers/{id}/drain` call is recorded in the audit log:

```bash
harvest audit list --operation worker.drain --target-id <worker-id>
```

Audit fields include `actor`, `occurred_at`, `status` (`succeeded` / `failed`),
and `request_id`.

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Terminating the process before `Stopped` | Poll with `--wait` or `worker get` until status is `Stopped` |
| Forgetting `--deadline` on a slow worker | The default deadline is `shutdown_timeout` (30 s); set a longer deadline for workers with large in-flight batches |
| Draining the wrong shard | Use `--shard-id` with `drain-preview` to scope the preview first |
| Ignoring `unavailable_shards` in the response | The worker may be on an unreachable shard; retry after shard recovers |

---

## Queue-coverage check — confirm every queue has a live poller (issue #774)

Draining removes a worker's capacity for every queue it served. If it was
the *last* worker polling one of those queues, pending work on that queue is
now stranded with nothing to claim it — silently, since a successful drain
call does not fail just because it happened to orphan a queue. The same
failure shape shows up **before** a drain too: a workflow that schedules
activities onto a brand-new queue whose worker deployment hasn't shipped
yet (or a typo'd queue name) produces `PENDING` rows no live worker will
ever claim — and that queue never appears in `fleet_health.by_queue` at
all, since it counts workers *per queue* and has nothing to count for a
queue with zero subscribers.

Cover both directions — before any work is scheduled onto a new queue, and
after every drain or deploy:

- **Post-drain / post-deploy smoke check** — after every drain **has
  actually finished** (either `harvest worker drain --wait`, or poll
  `GET /workers/{id}` until `status: "Stopped"`), and again once a rolling
  deploy's replacement workers are up, to confirm nothing was left orphaned.
  Running the check the instant `harvest worker drain` returns (without
  `--wait`) is not enough on its own: a `Draining` worker still counts as
  covering every queue it was assigned, so the report can read "covered"
  right up until the worker finishes its in-flight work and transitions to
  `Stopped` — check after that transition, not before it.
- **Pre-cutover check when adding a queue** — before routing traffic (or
  flipping a workflow's `queue` attribute) onto a queue name that didn't
  exist before, confirm at least one **live** worker is already subscribed
  with `harvest worker list --queue <name> --status Active --health healthy`
  (or `GET /workers?queue=<name>&status=Active&health=healthy`), not
  `harvest queue coverage`. Filter on both `--status` and `--health` — an
  unqualified `harvest worker list --queue <name>` still returns a worker row
  that registered, crashed, and stopped heartbeating (still `Active` but
  `stale`), or one that has fully transitioned to `Stopped`, so it can't
  distinguish "a live poller is subscribed" from "a subscription used to
  exist." The coverage report is built entirely from *pending*
  `harvest_task_queue` rows, so a brand-new queue with no scheduled work yet
  has nothing to compare workers against and always reports
  `uncovered: false` — a vacuous "fine" regardless of whether any worker is
  actually subscribed. Once traffic is flowing,
  `harvest queue coverage --queue <name>` is the right tool to confirm
  nothing is left stranding.

  **On a multi-shard deployment, scope the pre-cutover check to every shard
  that could own the new work, not just the fleet as a whole.** A queue name
  carries no fixed shard affinity — the new work's `PENDING` rows can land on
  any shard in `writable_shards` (spread by the rendezvous hash over each new
  execution's `(workflow_name, workflow_id)`, or landing on one specific
  shard if the workflow uses explicit residency placement, see
  `docs/sharding.md`) — so an unscoped `harvest worker list --queue <name>
  --status Active --health healthy` can find a healthy worker that is only
  assigned to shard 0 and read as "covered" even though the new work will
  route to shard 1, which has zero pollers. Repeat the check with
  `--shard-id <N>` (or `GET /workers?queue=<name>&status=Active&health=
  healthy&shard_id=<N>`) for every shard in your deployment's
  `writable_shards`, and require a hit on each one before cutover — a
  single-shard deployment has nothing extra to do here, since its only
  writable shard is `0`. If any response carries a non-empty
  `unavailable_shards` (the `partial`/`unavailable` cross-shard degradation,
  issue #756), treat the check as **inconclusive**, not a pass — retry once
  every shard is reachable.

```bash
harvest queue coverage --json
# or: GET /api/harvest/admin/queue-coverage
```

`uncovered: true` (equivalently, a nonzero `total_uncovered_queues`) means at
least one queue has `PENDING` tasks and zero live pollers assigned to it on
the shard that owns the pending work — `Active` **or** `Draining` workers
both count as covering (a draining worker still finishes work already in
flight), only `Stopped` and stale (heartbeat-expired) workers do not, and
coverage means *a poller exists*, not that capacity is free — a queue whose
workers are all saturated at `max_concurrency` is still covered (that's
#531/#742's job). Each uncovered queue's report entry carries bounded sample
`task_ids`/`execution_ids` (capped at 5) so you can open a specific stranded
task directly:

```bash
harvest workflow stack <execution_id>
```

`?queue_name=<name>` (`--queue <name>` on the CLI) narrows the check to one
queue. `harvest queue coverage` exits non-zero (exit code 2 — the same
"deploy hazard" convention `harvest workflow-types reachability` uses)
whenever `uncovered: true` **or** the cross-shard `status` is `partial`/
`unavailable` — an incomplete answer is treated as unsafe, never silently
downgraded to "looks fine" just because the shards it *did* reach happened
to be clean.

**A queue currently paused via the queue-pause primitive is deliberately
excluded** from `items`/`uncovered` (that intent is already surfaced by the
separate `harvest_queue_paused_too_long` alert), but a paused queue that
also has real pending work and zero live pollers is still named in
`excluded_paused_queues` — the moment it's unpaused without a worker being
added, it becomes uncovered, and `harvest_queue_paused_too_long` alone can
miss that combination for its full grace window. Treat a non-empty
`excluded_paused_queues` as a pre-unpause TODO, not a false negative.

**This is a distinct question from build-id reachability (#171) and the
pre-cutover handler-coverage gate immediately below (#520/#700).** Build-id
routing asks *"can a worker running this build safely resume this
execution's history?"*; the handler-coverage gate asks *"does a registered
handler still exist for this workflow type?"*; queue coverage asks a
narrower, worker-fleet-topology question that is orthogonal to both: *"is
any live worker even polling the queue this pending task sits on, regardless
of which build or handler it's running?"* A queue can be reported uncovered
even when every worker in the fleet is fully build-compatible and
handler-complete — it simply isn't subscribed to that queue name (a typo'd
`--queues` flag, a config that dropped a queue during a rolling deploy, or a
drain that removed the last subscriber). Fix by adding or re-subscribing a
worker to the named queue, not by adjusting build policy or removing
handlers. An unreachable shard is never silently dropped from the report —
it is named in `shards[]` with `status: "unavailable"` and degrades the
top-level `status` to `partial`/`unavailable`, so a partial report is never
mistaken for "fully covered".

---

## Worker-fleet handler contract — capability misses are released, not failed (issue #804)

**The contract: all workers polling a queue should register the same handler
set.** Harvest's task queue has no claim-time capability filter, and — by
construction — cannot have one: a worker can enumerate the handlers it *has*
registered, but not the ones it has *not*. `SKIP LOCKED` therefore hands any
eligible worker any eligible task, including a task for a handler that worker
has never heard of.

A rolling deploy breaks that contract *by design*, for the length of the
rollout: while old and new pods coexist, a workflow or activity introduced in
the new build is enqueued by a new pod and can be claimed by an old one.

**What happens now.** A worker that claims a task whose handler it does not
register **releases the claim** back to `PENDING` for a capable peer, with
capped-exponential backoff (1s, 2s, 4s, 8s, 16s, then a 30s cap — the default
budget of 5 reaches 16s). The release itself touches only `harvest_task_queue`
and the execution stays `RUNNING` — the old pod is healthy, it simply is not
the right pod for this task.

> A workflow task woken by a **due timer or a pending signal** has already
> durably ingested that `TimerFired`/`SignalReceived` before the handler lookup
> runs, so a release is not always a literal no-op on `harvest_events`. Those
> are genuine wake facts any worker would have appended, and a capable peer
> replays them normally: no new event variant is introduced, the event JSON
> contract is unchanged, and replay determinism is unaffected (AC7).

**The release is bounded — per distinct worker.** Each release records the
releasing worker in the task's distinct-miss set and backs the task off. Both
the set and the total counter are reset by every path that proves the claiming
worker *was* capable (an activity requeue, a clean continuation, and the
workflow park), so they measure *consecutive* misses. Once a claim would grow
that set beyond `WorkerConfig::capability_miss_max_redeliveries` (default **5**)
the task **escalates** through the ordinary terminal-failure path — a `WorkflowFailed`
event and a `FAILED` execution row, **not** a dead-letter entry — with a
greppable reason:

```
no_capable_worker: no workflow handler registered for 'ship_order' (escalated after 5 capability-miss redeliveries across 5 distinct worker(s); capability_miss_max_redeliveries = 5; every worker with a live heartbeat here has now missed it, so no live worker on this queue has the handler)
```

The release count and the distinct-worker count are the **real** persisted ones,
reported separately from the configured knob. They coincide only when each
worker missed exactly once; if the registry could not confirm the fleet, repeat
misses are free (they back off but consume no budget), so the release count can
run well past `capability_miss_max_redeliveries` before a fresh distinct worker
finally exhausts it.

Counting *distinct* workers rather than total releases is what stops a single
incapable pod from exhausting the shared budget by repeatedly winning the claim
race on its own released row — otherwise a capable peer could sit live and idle
while the run was failed underneath it. A repeat miss by a worker already in the
set backs off but consumes no budget.

A distinct-worker count alone cannot bound a fleet **smaller** than the budget:
one incapable pod pins the set at 1 forever, and `1 > 5` never holds. So once
the registry confirms every live eligible worker on the queue has already missed
the task — the set can no longer grow from the fleet as it stands — the same
`capability_miss_max_redeliveries` value bounds *total* releases too. That is
what keeps the knob a real maximum for the common small deployment: a
single-worker fleet escalates after 5 releases (~31 s), not 50.

That total bound requires **confirmed** coverage, not merely the absence of an
objection. If the worker registry cannot be read, `budget` total releases may all
have been won by the same pod, which is precisely the case the distinct count
exists to reject. A third, deliberately generous **absolute ceiling** of `10 ×`
the budget therefore remains as the backstop for the two states where coverage
is unprovable — a live worker that has never missed the task, or an unreadable
registry — so the release is always bounded even when nothing can be concluded
about the fleet.

**The budget is gated on the live fleet, not just on its own count.** A fixed
number of distinct workers still has no relationship to how many workers are
actually up: a rollout with `budget + 1` old pods plus one new capable pod could
hand `budget + 1` distinct incapable ids to the budget while the capable pod was
live and polling. So before the budget may terminate a task, the workers
recorded as having missed it must **cover the live fleet for its queue** —
`harvest_workers` rows with a fresh heartbeat advertising that queue. While any
live worker there has never missed the task, the budget is withheld and the
task keeps being offered around.

That freshness window is **not** the one the poison-pill reclaimer (#367) and
the broken-session scanner (#606) use. Those judge rows they own, with a window
derived from their own configured cadence; this query judges *peers*, whose
cadence nothing in `harvest_workers` records — so it uses
`2 × worker_heartbeat_interval` **floored at 120 s**. At the default 5 s
cadence that is 120 s here against 10 s there, and dropping
`worker_heartbeat_interval` below 60 s does not shorten it.

Predict the timing accordingly: for up to 120 s after a pod dies, its row still
reads as "a capable peer may exist", and in that interval **both** evidence-derived
bounds are withheld — this fleet-covering one *and* the distinct-worker one. The
absolute release ceiling (`10 ×` the budget) is what still fires, so the task is
still bounded, but it waits on that ceiling rather than on your configured
`max_redeliveries`. The delay costs extra redeliveries, never the run.

This is the mechanism behind the guarantee: **as long as at least one capable
worker is live on the queue, the budget cannot fail the run.** Two consequences
worth knowing before you tune anything:

- The bound is `max(budget, live fleet size)` redeliveries, not exactly
  `budget` — you cannot prove "no worker here has the handler" in fewer
  redeliveries than there are workers to ask.
- A worker that is *not* registered in `harvest_workers` (heartbeats disabled,
  or advertising a different queue name) makes the registry unusable, and the
  budget falls back to bounding on its own. The escalation reason says so
  explicitly rather than claiming a fleet conclusion it never established —
  see row 1b in
  [`harvest-alerts.md`](harvest-alerts.md#harvest_no_capable_worker).

A small fleet that exhausts the budget on *total* releases reports the same
budget-exhausted reason as a distinct-worker sweep, because the registry
confirmed the same fact — every live worker here has now missed it:

```
no_capable_worker: no workflow handler registered for 'ship_order' (escalated after 5 capability-miss redeliveries across 1 distinct worker(s); capability_miss_max_redeliveries = 5; every worker with a live heartbeat here has now missed it, so no live worker on this queue has the handler)
```

Note the `1` — a single-pod fleet exhausts the budget on *total* releases, so the
distinct count stays at one throughout. That is the tell for this sub-case, and
it is why the two counts are reported separately.

The **absolute ceiling** escalates with a different reason string, because it
supports a weaker conclusion — it is reached only when the fleet could not be
concluded about at all, so the queue was never provably swept. It still fires
the page-severity alert: the executions are failing either way, and under-paging
a genuinely missing handler is the worse error. Check the named
`distinct_incapable_workers` count against `GET /api/harvest/workers` before
concluding the whole queue lacks the handler.

The commonest way to reach it is a live worker on the queue that never missed
the task, which gets its own wording pointing at the peer rather than at the
deploy:

```
no_capable_worker: no workflow handler registered for 'ship_order' (escalated after 50 capability-miss redeliveries spread across only 6 distinct worker(s), hitting the absolute release ceiling of 50 releases (10x capability_miss_max_redeliveries (5)); a live worker on this queue never missed this task, so it may well have the handler and simply lost every claim race — check whether it is saturated, draining, advertising a stale queue list, or ineligible for this activity's registered capability requirements before concluding the handler is missing)
```

The ceiling is printed as the **computed product** (`50`), with the multiplier
and the knob shown alongside it so the arithmetic is checkable. Do not size the
knob off the multiplier alone: raising `capability_miss_max_redeliveries` moves
this ceiling by `10 ×` the change.

The counts in every reason string are the **persisted** ones: redeliveries that
actually completed, and workers that actually released. The claim that escalated
never ran a release, so it is not counted — the string describes the durable
record you can go and read.

That bound is what keeps a genuinely-missing handler — a workflow type deleted
or renamed in the new build with runs still in flight — from bouncing around
the fleet forever. It is a *deploy-skew absorber*, not a substitute for
shipping the handler.

**Prior behaviour (before #804):** the claiming worker failed the execution
terminally on the spot. Mid-deploy that was a self-inflicted outage — healthy
old pods failing perfectly good new-build work.

### What to watch during a rollout

| Signal | Meaning | Action |
|---|---|---|
| `harvest.task.capability_miss{outcome="released"}` rising, then falling to zero | Normal deploy skew, self-healing | None. Confirm it returns to zero once the rollout completes. |
| `harvest.task.capability_miss{outcome="released"}` sustained past the rollout | Some pods are stuck on the old build, or the new handler never shipped to part of the fleet | Finish/roll back the deploy; check the fleet's build IDs. |
| `harvest.task.capability_miss{outcome="escalated"}` non-zero | The task was offered around the queue and nobody took it. Executions are now failing. Two sub-cases, told apart by the reason string: the configured budget was exhausted with the registry confirming every live worker here has missed it (**no live worker on that queue registers the handler**), or the absolute release ceiling tripped because coverage could not be confirmed at all — a peer that never missed it, or an unreadable registry. Check those workers first in the second case. | Page. Ship the handler, or accept the failures deliberately. |
| `harvest.task.capability_miss{outcome="escalated_never_offered"}` non-zero | Executions are failing, but the task was failed on its **first** claim after **zero** releases — either `capability_miss_max_redeliveries = 0`, or a worker-session pin (#606) whose host lacks the handler. **This is not evidence the fleet lacks the handler**; a capable worker may be live and idle the whole time. | Ticket. Read the `no_capable_worker:` reason on a failed execution — its parenthetical names which of the two causes applies. |

The two escalation outcomes are deliberately separate label values because they
carry **opposite** conclusions. `escalated` is fleet evidence and fires the
page-severity `harvest_no_capable_worker`; `escalated_never_offered` is evidence
about one config knob or one task's pin and fires the ticket-severity
`harvest_capability_miss_never_offered`. See
[`harvest-alerts.md`](harvest-alerts.md#harvest_no_capable_worker) and
[`harvest_capability_miss_never_offered`](harvest-alerts.md#harvest_capability_miss_never_offered)
for the full triage.

### Sizing the budget

`capability_miss_max_redeliveries` (default 5) is a *redelivery* budget, not a
time budget, but the backoff makes it dwell. A budget of `N` grants exactly `N`
releases and escalates on the `N + 1`th claim, so the default allows five
releases whose backoffs sum to **31 s** (1 + 2 + 4 + 8 + 16) of queue dwell
before escalation — that is the *minimum* window a capable peer has to appear,
measured **on a single worker**, and it is what a single-worker fleet actually
gets: the budget bounds total releases once the registry confirms the whole live
fleet has missed the task. In a wider fleet, releases are consumed by whichever
worker claims next, so a wide fleet of incapable workers burns the budget in
fewer seconds of wall clock.

**Size it against your rollout, not against a single pod restart.** Escalation is
the loud signal that nobody can run the work, and it now arrives at the
configured budget on *every* fleet size rather than being stretched to `10 ×` on
small ones. If your replacement pods routinely take longer than the dwell above
to come up, raise the knob — 31 s is not much of a rollout window.

Escalation is also **probabilistic, not exhaustive**: a release carries no
affinity, so in an `M`-incapable / `1`-capable fleet a task can in principle
exhaust its budget without the capable worker ever winning a claim. Raise the
budget if your rollouts are slow, your fleet is wide, or you replace the first
pod of a large fleet (the widest skew ratio, and exactly when a newly-added
workflow type is first enqueued):

```rust
WorkerConfig::default().with_capability_miss_max_redeliveries(20)
```

Setting it to `0` escalates on the first miss — i.e. opts back into the
pre-#804 fail-fast behaviour. A run failed that way says so explicitly
(`capability-miss redelivery is disabled …; a capable worker may exist on this
queue`) rather than claiming the fleet lacks the handler, because with a budget
of `0` exactly one worker demonstrated it lacks the handler and no peer was
ever asked. That matters if you reach for `0` as a rollback switch *during* an
incident: the resulting terminal error names the knob, not a missing deploy.

**The budget is per *frontier*, not per task.** A workflow can be stuck on a
different handler on each dispatch — it advances through local activities
inline, it parks and is woken by a signal, a durable timer fires. Each of those
is a new question for the fleet ("who has handler *Y*?"), so it gets its own
budget rather than inheriting the spend of a question that has already been
answered. Harvest records which handler the counters are about
(`harvest_task_queue.capability_miss_handler`) and applies them only to that
one; a miss on a handler the row was not recorded against starts at 1. Without
this, a run that had spent its budget looking for *X*, then moved on and got
stuck on *Y*, would be failed on the **first** worker to miss *Y* — while a peer
that could have run *Y* was live and never asked. This is invisible in the
reason string, which always names the handler the run was actually stuck on;
the observable effect is simply that a long-lived workflow does not accumulate
budget pressure across unrelated deploys.

This does not weaken the bound AC3 asks for. A frontier is a pure function of
the recorded history, so consecutive dispatches with no new events land on the
same handler and cannot oscillate; the frontier only moves when history grows,
and every such event is appended once and consumed once. The number of
frontiers one task can present is therefore bounded by the history hard cap,
and no single frontier can release forever.

### Interactions and carve-outs

- **Not a poison pill (#367).** A capability miss is a clean "wrong pod", not a
  crash. It never increments `crash_strikes`, never consumes an activity's retry
  budget (`attempt` is restored whenever the handler was never reached, which is
  every activity-task miss), and never produces a `PoisonPill`
  dead-letter row — escalation writes no dead-letter row at all. The `harvest.task.quarantined` metric and the
  `harvest_no_capable_worker` alert
  are therefore mutually exclusive diagnoses. It also does not *erase* a
  poison task's history: a release only clears `crash_strikes` when the miss
  was found **after** the workflow body ran to a conclusion (persisting its
  commands hit an unregistered activity or child type). A miss found before or
  during the body ran nothing, so it leaves the counter alone — otherwise, in a
  mixed fleet, an incapable claim landing between two capable crashes would
  reset the streak every time and `poison_pill_threshold` would never be
  reached.
- **Not a hung body (#494).** The workflow-task timeout strike counter follows
  the same rule for the same reason: a released task that never ran a handler
  is not evidence the body stopped hanging.
- **Not a panicking body (#782).** The consecutive-workflow-handler-panic strike
  follows the same rule again. A pre-/mid-handler miss is *non-terminal* — the
  task is released, not failed — so it must not reset the streak; otherwise a
  worker that alternates between panicking on the body and missing an
  unregistered local activity would keep `workflow_panic_max_attempts` out of
  reach indefinitely. All three counters derive their answer from one question:
  *did this dispatch watch the handler reach a conclusion?*
- **Session-pinned tasks (#606) escalate immediately.** A session task is
  hard-pinned to its acquiring host, so "release for a capable peer" is false
  by construction: no other worker can ever claim it. Such a task escalates on
  the first miss regardless of the budget.
- **Orthogonal to build-id routing (#171), below.** Build routing asks *"is
  this worker's build allowed to resume this history?"*; a capability miss
  asks *"does this worker have the handler at all?"* A fleet using build
  policies still benefits: routing narrows who *may* claim, but a worker that
  passes the build gate can still lack a brand-new handler. The budget's
  fleet check reads `harvest_build_compat` so a cross-build peer counts as a
  possible claimant; if that read fails it **keeps** cross-build peers rather
  than concluding they are ineligible, so a blip in the declaration table can
  only delay escalation, never cause one.
- **Keep `worker_heartbeat_interval` at or under 60 s.** The fleet check that
  decides whether "no live worker here has the handler" is *true* reads
  `harvest_workers.last_heartbeat_at`, and nothing in that table records the
  cadence each worker chose — so one fleet-wide freshness window is applied to
  every row. Harvest floors that window at **120 s** (`2 ×` the supported 60 s
  cadence) rather than deriving it from the *reading* worker's own interval,
  precisely so a pod configured to heartbeat every second cannot decide a peer
  on the default 5 s cadence is dead and escalate a task that peer could have
  run. A worker configured past 60 s logs a warning at startup naming this; it
  still boots, and only the configured-total bound is affected (the
  distinct-worker bound and the absolute ceiling are unaffected), but it can be
  escalated against early by a faster peer. The floor errs the other way — a
  worker that is genuinely gone lingers in the fleet view for up to two minutes,
  which *delays* an escalation rather than fabricating one.
- **Orthogonal to the handler-coverage gate (#520/#700), below.** That gate is
  a *pre-cutover* check for handlers you are about to **remove**; this is a
  *runtime* absorber for handlers not yet **added**.
- **No replay impact.** Release and redelivery are task-queue state, not
  event-log state: no new `WorkflowEvent` variant, nothing appended on a
  release, and the adjacently-tagged event JSON contract is unchanged.

---

# Runbook: Build-Id Routing for Safe Rolling Deploys

Use Harvest's build-id routing to gate which workers may resume specific
workflow executions. This prevents a worker running an incompatible binary from
replaying history in a way that causes non-determinism.

**When to use:** deployments that add or change workflow logic (new
`ctx.schedule_activity` calls, reordered steps, removed branches) where replay
safety cannot be proven by inspection alone.

**When _not_ to use:** purely additive changes that do not alter execution
order (new unrelated workflows, configuration changes, dependency bumps with no
behaviour change). Those deploys can use the plain drain runbook above.

---

## Concepts

| Term | Meaning |
|------|---------|
| `build_id` | Immutable string (Git SHA, semver tag, CI job ID) advertised by a worker at startup via `WorkerConfig::with_build_id("sha-abc123")`. Empty string = legacy worker that can claim any task. |
| Build policy | Per-queue row in `harvest_build_policies`. New workflow starts on the queue receive `assigned_build_id = policy.build_id`. Updated by operators when a new build ships. |
| Compat declaration | Row in `harvest_build_compat`. Means "workers running build B may process executions assigned to build A". Added after replay tests confirm safety. |
| `required_build_id` | Denormalized onto `harvest_task_queue`. Workers skip tasks whose `required_build_id` they are not eligible for. |
| Build reachability | Per-build snapshot: `open_executions`, `pending_tasks`, `active_workers`, `stale_workers`, `safe_to_retire`. Used to decide when old-build workers can be retired. |

---

## Scenario A: Backward-compatible deploy (new code can replay old history)

Use this when your replay test suite (e.g. `WorkflowReplayer`) confirms the new
build handles all in-flight histories safely.

**Step 1 — Deploy new workers with the new build id.**

In your `WorkerConfig`:

```rust
WorkerConfig::default()
    .with_build_id("sha-new123")
    .with_deployment_name("v2.3.0")   // optional human label
```

Start the new workers alongside the existing fleet. They register in
`harvest_workers` with `build_id = "sha-new123"`.

**Step 2 — Declare compat so new workers can resume old executions.**

```rust
// In your deploy tooling or a one-off migration script:
use autumn_harvest::build_routing::declare_compat;

declare_compat(&mut conn, "sha-new123", "sha-old456").await?;
// "workers running sha-new123 may process executions assigned to sha-old456"
```

> **Sharded deployments:** `harvest_build_compat` lives on every shard.
> Fan the write out to all shards:
> ```rust
> for (_, pool) in sharded_pool.iter_shards() {
>     let mut conn = pool.get().await?;
>     declare_compat(&mut conn, "sha-new123", "sha-old456").await?;
> }
> ```

**Step 3 — Advance the build policy so new starts land on the new build.**

```rust
use autumn_harvest::build_routing::set_build_policy;

set_build_policy(&mut conn, "default", "sha-new123", Some("v2.3.0")).await?;
```

> **Sharded deployments:** `harvest_build_policies` lives on every shard.
> Fan the write out to all shards:
> ```rust
> for (_, pool) in sharded_pool.iter_shards() {
>     let mut conn = pool.get().await?;
>     set_build_policy(&mut conn, "default", "sha-new123", Some("v2.3.0")).await?;
> }
> ```

New executions now get `assigned_build_id = "sha-new123"` and old-build
workers are ineligible to claim them.

> **First-adoption prerequisite:** this exclusion only applies to workers
> that advertise a non-empty `build_id`. Workers using the default
> `WorkerConfig` have `build_id = ""` (the legacy sentinel) and the claim
> filter allows them to pick up **any** task regardless of
> `required_build_id`. Before advancing the build policy, ensure the entire
> old fleet is already running with `with_build_id("sha-old456")` — or drain
> all legacy workers first. A mixed fleet with even one legacy worker
> invalidates the routing guarantee.

**Step 4 — Drain and retire old-build workers.**

Check reachability first:

```rust
use autumn_harvest::build_routing::build_reachability;
use std::time::Duration;

let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;
println!("safe_to_retire: {}", r.safe_to_retire);
```

Wait until `safe_to_retire == true` (no open executions, no pending tasks), then
drain and stop the old workers using the drain runbook above.

---

## Scenario A.5: Percent-ramp a new build (issue #604)

Scenario A's Step 3 is a hard cutover: the instant `set_build_policy` runs,
**100%** of new starts move to the new build. For anything customer-facing
you usually want to validate a risky deploy against a *slice* of live
traffic first, watch its failure/DLQ/duration metrics, and ramp up — or
abort instantly — before it touches everyone.

The ramp is a second, optional pair of columns (`target_build_id`,
`ramp_percent`) on the same per-queue build policy row. It requires a base
policy to already exist (Scenario A, Steps 1–2 — new workers registered,
compat declared) but does **not** require advancing `build_id` itself: the
base policy keeps routing the rest of traffic exactly as before.

**Step 1 — Set an initial 5% ramp.**

```bash
harvest build ramp set \
  --queue default \
  --target-build-id sha-new123 \
  --percent 5
```

Or over HTTP:

```bash
curl -X POST /api/harvest/admin/build-routing/ramp \
  -H 'Content-Type: application/json' \
  -d '{"queue_name": "default", "target_build_id": "sha-new123", "ramp_percent": 5}'
```

Or from Rust:

```rust
use autumn_harvest::build_routing::set_build_ramp;

set_build_ramp(&mut conn, "default", "sha-new123", 5).await?;
```

Roughly 5% of new starts on `default` now get `assigned_build_id =
"sha-new123"`; the rest keep the base `build_id`. The decision is a pure,
deterministic function of the workflow's `ExecutionId` (the same rendezvous
hash idiom used by shard routing and schedule jitter), so a start retry or
outbox redelivery for the same execution never flips its build, and
in-flight executions are never re-routed — only the one-time start decision
is affected.

> **Sharded deployments:** the management API route fans the write out to
> every shard automatically. Calling `set_build_ramp` directly against a
> single connection only affects that shard — loop over
> `sharded_pool.iter_shards()` as in Scenario A above if you're calling the
> core function rather than the HTTP/CLI surface.

**Step 2 — Observe.**

```bash
harvest build ramp show
# or: GET /api/harvest/admin/build-routing
```

The response's `policies` array carries `build_id`, `target_build_id`, and
`ramp_percent` per queue, alongside the existing cross-shard `reachability`
snapshot. Watch the canary build's failure rate, DLQ entries
(`harvest dlq aggregate --group-by workflow_name,failure_signature`), and
`harvest.workflow.terminal{outcome=...}` / `harvest.activity.failed` metrics
segmented by `assigned_build_id` before deciding to ramp up.

**Step 3 — Ramp up, or abort.**

Ramp up in increments (5% → 25% → 100%) by re-issuing the same command with a
higher `--percent`:

```bash
harvest build ramp set --queue default --target-build-id sha-new123 --percent 25
```

If the canary misbehaves, abort in one command — this is the whole point of
the feature, and it's designed to be the fastest possible operator action
(< 30s MTTR):

```bash
harvest build ramp set --queue default --target-build-id sha-new123 --percent 0
# or, equivalently:
harvest build ramp clear --queue default
```

Either form immediately stops new starts from reaching the target build; a
follow-up start lands on the base build on its very next attempt. Ramping to
`0` keeps the ramp record around (useful if you want to retry later without
re-declaring `target_build_id`); `clear` removes it entirely.

**Step 4 — Promote to full cutover.**

Once you're satisfied at 100% ramp, promote the target to the queue's base
policy so the ramp bookkeeping can be retired:

```bash
harvest workflow ...   # (verify no regressions first, per your own bar)
harvest build ramp show   # confirm ramp_percent is 100
```

```rust
// Promote: the target becomes the new base. Setting the base policy to the
// same build id the ramp was already routing 100% of traffic to is a no-op
// from the traffic's perspective — every start already resolves to
// "sha-new123" either way.
set_build_policy(&mut conn, "default", "sha-new123", Some("v2.3.0")).await?;
// Then retire the now-redundant ramp record:
clear_build_ramp(&mut conn, "default").await?;
```

Then continue with Scenario A's Step 4 (drain and retire old-build workers)
using the same `build_reachability` check.

**Compatibility gating still applies.** Ramping does not bypass
`harvest_build_compat`: a worker on the base build declared compatible with
the target build (or vice versa) still claims correctly, because the ramp
only changes which build a *new start* is assigned to — the existing claim
filter and compatibility declarations are unaffected.

---

## Scenario B: Breaking deploy (new code cannot replay old history)

Use this when the new binary changes execution order in a way that would corrupt
in-flight workflows if replayed. Requires gating the breaking branch — with
`ctx.patched()` for the common two-state before/after change, or `ctx.version()`
when more than two versions must coexist.

**Step 1 — Gate the breaking code branch.**

`ctx.patched()` is the default two-state fence (issue #687):

```rust
#[workflow]
async fn my_workflow(ctx: &WorkflowContext, ...) -> Result<(), String> {
    if ctx.patched("add-step-2") {
        // new code path — runs for any execution that reaches this code for the first time
        ctx.schedule_activity(new_step, ...).await?;
    }
    // existing steps ...
}
```

`ctx.version()` remains the escape hatch for gates that need **more than two**
concurrent versions (note it is synchronous and infallible — no `.await`, no `?`):

```rust
#[workflow]
async fn my_workflow(ctx: &WorkflowContext, ...) -> Result<(), String> {
    if ctx.version("add-step-2", 1, 2) >= 2 {
        // new code path — runs for any execution that reaches this code for the first time
        ctx.schedule_activity(new_step, ...).await?;
    }
    // existing steps ...
}
```

Both record a marker event on first live call and replay the recorded marker
on re-entry, so the branch is stable across replays.

**Step 2 — Deploy new workers and set the policy (same as Scenario A steps 1 and 3).**

Do **not** declare compat. Old-build workers will stop receiving new tasks; new
tasks go only to new-build workers. In-flight old-build executions drain on
old-build workers.

**Step 3 — Wait for old-build executions to drain.**

Poll until `safe_to_retire` flips:

```rust
loop {
    let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;
    if r.safe_to_retire { break; }
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

Once `true`, retire the old workers.

---

## Scenario C: Emergency rollback

When a bad deploy must be reversed immediately:

**Step 1 — Advance the build policy back to the previous build.**

```rust
set_build_policy(&mut conn, "default", "sha-old456", None).await?;
```

New starts immediately revert to the previous build. In-flight new-build
executions continue on new-build workers.

**Step 2 — If the new build is unsafe to continue, drain new-build workers.**

Use the drain runbook to quiesce all workers advertising `build_id = "sha-new123"`.
New-build executions that were in-flight will be retried and, if the build policy
is back to `sha-old456`, picked up by old-build workers — but only if a compat
declaration exists (or the new executions have no `required_build_id`).

**Step 3 — Declare compat so old-build workers can resume in-flight new-build executions.**

New-build executions that were in-flight when the workers were drained have
`assigned_build_id = "sha-new123"`. For old-build workers to claim those tasks,
declare the reverse compat direction:

```rust
use autumn_harvest::build_routing::declare_compat;

declare_compat(&mut conn, "sha-old456", "sha-new123").await?;
// "workers running sha-old456 may process executions assigned to sha-new123"
```

Optionally revoke the original forward compat to prevent any surviving
new-build workers from claiming old tasks while they finish draining:

```rust
use autumn_harvest::build_routing::revoke_compat;

revoke_compat(&mut conn, "sha-new123", "sha-old456").await?;
// "workers running sha-new123 may NO LONGER process executions assigned to sha-old456"
```

> **Sharded deployments:** fan both calls out over `ShardedDbPool::iter_shards()`
> as shown in Scenario A Steps 2–3 above.

---

## Build reachability reference

```rust
use autumn_harvest::build_routing::{build_reachability, all_build_reachability_sharded};
use std::time::Duration;

// Single build, single shard
let r = build_reachability(&mut conn, "sha-old456", Duration::from_secs(60)).await?;

// All builds across all shards
let all = all_build_reachability_sharded(&pool, Duration::from_secs(60)).await?;
```

Response fields:

| Field | Meaning |
|-------|---------|
| `open_executions` | Non-terminal executions with this `assigned_build_id` |
| `pending_tasks` | Tasks in PENDING state with this `required_build_id` |
| `active_workers` | Workers with this `build_id` and a recent heartbeat |
| `stale_workers` | Workers with this `build_id` whose heartbeat has expired |
| `safe_to_retire` | `true` when `open_executions == 0` and `pending_tasks == 0` |

**Old-build workers are safe to stop once `safe_to_retire` is `true`.**

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Retiring old workers before `safe_to_retire` | Check reachability; in-flight executions will be orphaned |
| Forgetting to declare compat in Scenario A | New workers skip old-build tasks; old-build executions stall |
| Breaking deploy without a `ctx.patched()` / `ctx.version()` gate | New workers corrupt in-flight histories on replay; use a patched gate (or a version gate for >2 versions) |
| Rollback without updating the build policy | New starts continue landing on the bad build; set policy back first |
| Empty `build_id` on new workers | Legacy sentinel — the worker claims any task, bypassing all routing |

---

# Runbook: Pre-cutover handler-coverage gate

Before cutting a **default (non-build-routed)** deployment over to code that has **deleted or renamed a `#[workflow]` handler**, confirm no in-flight run still needs that handler. A non-terminal execution's `workflow_name` names the handler its next replay requires — removing it strands those runs in permanent `HandlerNotFound` replay failure, surfacing only later as a timeout/DLQ entry.

**Where this sits in the deploy ladder:** worker drain (#386) → **this pre-cutover handler-coverage gate** → [queue-coverage check](#queue-coverage-check--confirm-every-queue-has-a-live-poller-issue-774) (#774) → [Pre-Deploy Replay Canary](#runbook-pre-deploy-replay-canary) (#512) → non-determinism block (#480) → history reset/redrive (#614) → reset-from-history (#538 / #510). Run this gate **before** the replay canary: the canary replays runs of handlers that still exist; this gate catches runs whose handler is about to disappear entirely. Queue coverage is a sibling gate, not a stricter/looser version of this one — it asks a worker-fleet-topology question (*is anyone polling this queue at all?*) that is orthogonal to handler coverage's code-compatibility question, so run both.

This gate answers *"does a registered handler still exist for this workflow type?"* — a code-compatibility question. It is deliberately distinct from the [queue-coverage check](#queue-coverage-check--confirm-every-queue-has-a-live-poller-issue-774) above, which answers a worker-fleet-topology question instead — *"is any live worker even polling the queue this pending task sits on?"* A run can fail either check independently: a fully covered queue can still be `orphaned` if its handler was removed, and a fully handler-complete deployment can still leave a queue `uncovered` if no worker subscribes to it.

## CI/CD gate — one field asserts "zero orphans"

```bash
# Fails the pipeline (exit 2) on any orphaned type OR a partial/unavailable
# report OR a transport/auth error (fail-closed: uncertain = unsafe).
harvest workflow-types reachability
```

Or drive the API directly and gate on a single field:

```bash
# orphaned == false AND status == "complete" ⇒ safe to cut over.
curl -s .../api/harvest/admin/workflow-types/reachability | jq '{orphaned, total_orphaned_executions, status}'
```

`orphaned` (bool) and `total_orphaned_executions` (count of stranded **executions**, not types) are the single-field CI gate. Each `items[]` entry additionally carries `sample_execution_ids` — a bounded set (cap 5) of representative non-terminal execution ids so you can drill straight into a stuck run:

```bash
curl -s '.../api/harvest/admin/workflow-types/reachability?workflow_type=legacy_export' \
  | jq '.items[0] | {verdict, non_terminal_count, sample_execution_ids}'
```

If a type is still `in_use` (registered) that is normal; only an `orphaned` verdict (live runs, **no** registered handler) blocks the cutover — drain or wait for those runs, or keep the handler until they finish.

## Boot-time gate (on by default)

Harvest runs this same check at startup, so a mis-sequenced deploy is caught before it serves traffic. **The gate runs before the worker poll loop and schedulers are spawned** — so under `fail` a boot is refused *before any task can be claimed*, and a worker can never claim and terminally fail an orphaned-type run in the boot window. **It runs by default** — the default `warn` action still executes the reachability check on every boot; only `off` skips it entirely:

```toml
[harvest.startup]
# off  — skip the check entirely (the only zero-cost setting).
# warn — (default) run the check, log the orphaned types, and continue.
#        Non-breaking: a mixed fleet mid-rollout must not crash-loop just
#        because an old handler was removed on one node.
# fail — run the check and refuse startup when orphaned types are present.
orphaned_workflows = "warn"
```

(Env override: `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS=fail`.)

**Both boot paths are gated (issue #1128).** The `HarvestPlugin` web-app path and the standalone `HarvestRunner::start` embedder path run the *same* check, from the same code, driven by the same `[harvest.startup] orphaned_workflows` setting. The standalone runner runs it as the first act of `start` — after pure configuration validation, but before it resolves the runtime, installs any process global or syncs completion triggers, and long before it spawns a worker — so a refused standalone boot leaves the process and the database exactly as it found them. On a **multi-shard** standalone deployment (`HarvestRunnerResources::with_sharded_pool`, #522) the gate fans out across *every* shard in the resolved pool, so an orphan on a non-zero shard refuses boot just as one on shard 0 does; a shard the router names but this process has no pool for is reported `unavailable`, which degrades the report to `partial` and therefore warns rather than aborts — the runner then refuses that router/pool pair outright a moment later with a `ShardRouter references shards …` error, so boot is still refused, just with the configuration error rather than the orphan one. Each shard's query is bounded (10s); a shard that is reachable-but-silent is reported `unavailable` rather than parking the boot indefinitely.

**On the standalone path, the setting is whatever config you pass.** The plugin loads `[harvest.startup] orphaned_workflows` (and its `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS` override) from configuration automatically. `HarvestRunner::start` uses the `HarvestRuntimeConfig` the embedder hands it — so an embedder that builds that struct in code (as `examples/standalone-runner` does, with `..HarvestRuntimeConfig::default()`) rather than via `HarvestRuntimeConfig::load()` must thread the setting through itself. Put `fail` in a TOML file that nothing loads and the action silently stays `warn`.

**A deliberately handler-free process** (a control plane that registers no `#[workflow]`s but shares the Harvest database) will see every in-flight type as orphaned. That is the setting doing what it says — under the default `warn` it logs and boots; set `orphaned_workflows = "off"` on such a process rather than leaving it to refuse boot under `fail`.

**The same applies to a partially-registering fleet.** The gate runs regardless of `worker_enabled`, and "registered" means *registered in this process*. So a fleet split into processes that each register a subset of the workflow types — a common standalone shape, e.g. one process per queue — will have each process see the other processes' in-flight types as orphaned. Under `fail` every one of them refuses to boot. `fail` is a statement about a deployment whose registered handlers are expected to cover the whole fleet's in-flight work; on a split-registration fleet use `warn` (or `off`) and run the CLI gate against the *union* of registered types in CI instead.

**Boot cost:** the check is a single bounded cross-shard `GROUP BY … COUNT(*) … ARRAY_AGG` aggregate (never a per-execution row load), and the per-group sample slice is bounded (drop-in `LATERAL (SELECT … ORDER BY started_at LIMIT n)` fallback keeps memory bounded even on a huge group). It runs by default under `warn`; a deployment concerned about boot latency on a very large non-terminal backlog can set `orphaned_workflows = "off"` to make boot zero-cost.

**Crash-loop safety:** `fail` aborts boot **only** when orphaned types are present **and** the cross-shard report is *complete*. An `incomplete` report — `partial`/`unavailable`, e.g. a transient shard/DB outage — means orphan detection did **not** fully run, so it **always warns** (even when no orphans were detected on the reachable shards) rather than silently continuing, but it never aborts: a boot loop has no human in the loop to read an exit code. A *complete* report with no orphans is the only case that boots silently. This is deliberately **asymmetric** with the CLI gate above, which fails *closed* on a partial report (a human is reading its exit code, so "uncertain = unsafe" is the safe default there).

**Enable `fail` only after confirming zero orphans** with the CLI gate above. A persistent orphan (a removed handler with still-live runs) will refuse boot **fleet-wide** — every node running `fail` aborts on every restart until those orphaned runs drain. That is the intended gate behavior, but the blast radius means you should **drain first**: keep the old handler registered (or let the runs finish) until the CLI gate reports `orphaned == false`, then flip `fail` on.

This is the **type-level** reachability question. It is distinct from **build-id** reachability ("can I retire this worker *build*", `GET /admin/build-routing`) and from the **`ctx.version()`** gate-retirement check ("can I remove this version branch *inside* a handler"). See [safe-handler-removal.md](safe-handler-removal.md) for the three-way distinction.

---

# Runbook: Pre-cutover in-flight replay-drift gate (issue #798)

Run this **before** cutting over — it is the CI-side counterpart to the
post-deploy replay canary below.

The canary runs *server-side against the deployment*, so by the time it can
speak the build is already reachable. The drift gate runs *in CI, against the
candidate build*, so a determinism regression is caught before the artifact is
promoted at all:

```bash
# In CI, against staging (read-only credential is sufficient).
# --payload-policy full is REQUIRED: the CLI defaults to `redacted`, and the
# gate REFUSES a redacted bundle (redaction rewrites the very activity inputs
# replay compares against), so omitting it exits 2 on every fixture. Treat the
# bundle as production data.
harvest history export-sample \
  --payload-policy full \
  --per-workflow 50 \
  --output-dir ./fixtures/in-flight

# Then, in your own ~15-line gate binary linked against the candidate build:
cargo run --release --bin replay-drift-gate -- ./fixtures/in-flight
```

Exit `0` = promote. Exit `1` = an in-flight execution would diverge — gate the
change with `ctx.patched(...)` and re-run. Exit `2` = the gate could not fully
run (a redacted bundle, a fixture that failed to replay, or an export that
delivered fewer fixtures than it selected) — fix the export, never override.
Exit `3` = the bundle was empty (a gate that verified nothing is never a pass;
opt out with `allow_empty_bundle(true)` only for a genuinely idle fleet).

The export is `SELECT`-only and safe against production. The per-type
stratification (`--per-workflow`) means a noisy workflow type cannot crowd every
other type out of the sample, and the emitted `harvest-sample-manifest.json`
states `sampled` versus `in_flight_total` per type so a clean gate is never
mistaken for a fleet-wide guarantee.

Full recipe, exit-code table, coverage semantics, and the payload-policy
trade-off: [`docs/replay-drift-gate.md`](../replay-drift-gate.md).

**Where this sits in the deploy ladder:** worker drain (#386) → [pre-cutover
handler-coverage gate](#runbook-pre-cutover-handler-coverage-gate) (#520) →
[queue-coverage check](#queue-coverage-check--confirm-every-queue-has-a-live-poller-issue-774)
(#774) → schema contract gate (#794) → **this in-flight drift gate (#798)** →
[Pre-Deploy Replay Canary](#runbook-pre-deploy-replay-canary) (#512) →
non-determinism block (#480/#603).

---

# Runbook: Pre-Deploy Replay Canary

Use the deploy-time replay canary to verify that a candidate build is compatible with currently running executions before advancing the build policy or declaring compatibility.

> **Complement, not substitute.** The [in-flight replay-drift gate](#runbook-pre-cutover-in-flight-replay-drift-gate-issue-798)
> (#798) runs the same class of check in CI *before* promotion; this canary runs
> server-side *at* deploy time. Run both — the gate blocks a bad artifact, the
> canary catches anything the sample missed.

**When to use:** before rolling out any new worker deployment where you expect the new code to handle existing in-flight workflow executions (i.e. Scenario A).

**When _not_ to use:** for breaking deploys (Scenario B) where you explicitly do not declare compatibility and instead let old workflows run to completion on old workers.

---

## Concepts

The replay canary performs an in-memory execution audit:
1. **Zero Mutations:** The canary never executes activities, schedules timers, sends signals, writes events, or mutates any database records.
2. **Multi-Shard Sampling:** It queries currently `RUNNING` workflow executions across all active shards (using `iter_shards()`).
3. **In-Memory Replay:** It uses the candidate build's `WorkflowReplayer` and registered workflow definitions to replay those executions' history in-memory from start to current state.
4. **Compatibility Report:** If any execution fails to replay (due to non-determinism, changed steps, or code panics), the canary returns a `fail` verdict and details the failing execution ID, event index, and expected vs actual mismatch.

---

## Step 1 — Run the Canary Check

Run the canary command from your candidate deployment (or continuous delivery pipeline) targeting the active database:

```bash
harvest canary --sample-size 500
```

You can narrow the canary check to a specific workflow name or queue:

```bash
harvest canary --sample-size 100 --workflow-name billing_checkout --queue default
```

### Canary Command Output

A successful canary run outputs a summary table:

```text
Canary Verdict: PASS
Sampled: 142 (succeeded: 142, failed: 0, truncated: false)

Summary by Workflow Type:
WORKFLOW TYPE     SAMPLED  SUCCEEDED  FAILED
billing_checkout  80       80         0
user_onboarding   62       62         0
```

If any execution fails to replay, the canary outputs the failure details and exits with exit code `1`:

```text
Canary Verdict: FAIL
Sampled: 142 (succeeded: 140, failed: 2, truncated: false)

Summary by Workflow Type:
WORKFLOW TYPE     SAMPLED  SUCCEEDED  FAILED
billing_checkout  80       78         2
user_onboarding   62       62         0

Replay Failures:
EXECUTION ID                          WORKFLOW TYPE     KIND             EVENT IDX  ERROR
9bc9759c-6aef-4a1e-8495-2d4e7f91cc09  billing_checkout  missing_command  14         expected ScheduleActivity(charge_card), got ScheduleActivity(charge_card_v2)

Diagnostic details for execution 9bc9759c-6aef-4a1e-8495-2d4e7f91cc09:
  Expected: ScheduleActivity(charge_card)
  Actual:   ScheduleActivity(charge_card_v2)
```

To fetch the raw JSON report (e.g. for custom scripting or diagnostic tools), pass the `--json` flag:

```bash
harvest canary --sample-size 200 --json
```

---

## Step 2 — Combine with Build-Id Routing

1. **Verify candidate build compatibility:** Run `harvest canary` before promoting the new build.
2. **If Canary Passes:** The candidate build is compatible with all sampled running executions. You can safely deploy the new workers, declare compatibility (e.g. `declare_compat("sha-new", "sha-old")`), and advance the routing policy.
3. **If Canary Fails:** Replay safety is compromised. You must either:
   - Fix the non-determinism in the candidate build.
   - Use `ctx.patched()` (or `ctx.version()` for >2 versions) gates to isolate the new behavior.
   - Run the deploy as a breaking deploy (Scenario B).

---

## Caveats and Statistical Nature

> [!WARNING]
> The replay canary is **statistical**, not mathematical proof of 100% compatibility.
> Because it samples up to `sample_size` executions, it may miss rare or edge-case workflow histories that contain compatibility issues.
> For high-risk deployments:
> - Keep `sample_size` sufficiently high (e.g., 500 to 1000).
> - Keep a comprehensive test suite of workflow history replay fixtures (using `harvest history export`).
> - Combine the canary with gradual rollout of workers and active monitoring of non-determinism alarms.

