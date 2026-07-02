# Adaptive Worker Slot Tuner

This document describes the opt-in adaptive dispatch-slot tuner (issue #548):
what it does, the signals its default controller reacts to, how it composes
with the other concurrency knobs harvest already has, and its operational
scope.

## What it does

A harvest worker bounds concurrent dispatch with two in-memory
`tokio::sync::Semaphore`s — one for workflow tasks
(`WorkerConfig::max_concurrent_workflows`), one for activity tasks
(`max_concurrent_activities`). Those are static numbers an operator picks
once. Too high and a load burst can exhaust the worker's Postgres connection
pool, pressure memory, or overload downstream dependencies. Too low and
capacity sits idle while queue depth and schedule-to-start latency climb. The
right value drifts with payload size, activity mix, database load, and time
of day.

The slot tuner lets the worker **resize its own dispatch capacity live**,
within an operator-configured `[min_slots, max_slots]` band, driven by
signals harvest already observes in-process. It composes with the
slot-utilization gauges from issue #531 on the same dashboard: those gauges
are the *observe* half, the tuner is the *act* half.

## Enabling it

```rust
use autumn_harvest::prelude::*;
use autumn_harvest::slot_tuner::SlotTunerConfig;

let worker_config = WorkerConfig::default()
    .with_slot_tuner(SlotTunerConfig::new(/* min_slots */ 5, /* max_slots */ 50));
```

`SlotTunerConfig::new(min, max)` installs the harvest-provided default
controller. **When `with_slot_tuner` is never called, the worker's
fixed-concurrency semaphore behaviour is byte-for-byte identical to before
this feature existed** — this is opt-in with zero default-path change.

Both the workflow and the activity dispatch semaphore get their own
independent controller instance, but they share the one configured
`[min_slots, max_slots]` band. Per-slot-type bands are a documented
follow-up, not part of this slice.

## The `[min_slots, max_slots]` band

- **`min_slots`** is a liveness floor: the controller never resizes a
  semaphore's dispatch target below this value, no matter what the signals
  say. Set it high enough that the worker can still make forward progress
  under a quiet period without waiting on a resize.
- **`max_slots`** is a hard safety cap: the controller never resizes above
  this value. This is also the *total* number of permits the underlying
  semaphore is created with — grow releases permits already reserved for the
  semaphore, it never allocates new ones beyond `max_slots`.
- The **initial target** at worker startup is the configured
  `max_concurrent_workflows` / `max_concurrent_activities` value, clamped
  into `[min_slots, max_slots]`. If your existing static value already falls
  inside a sensible band, you don't need to change it when adding a tuner —
  just wrap it with a band around it.
- A misconfigured band (`min_slots > max_slots`, `max_slots == 0`, or a
  configured static value outside the band) never fails worker startup — it
  is logged via `tracing::warn!` and the tuner degrades to an inert-but-safe
  state (clamped to whatever is achievable), matching the precedent set by
  `WorkerConfig::queue_weights` validation.

## The default controller's inputs

`SlotTunerConfig::new` installs `DefaultSlotTuner`, which consumes **only
signals harvest already owns in-process** — no new external dependency, no
`execution.id` sampling:

1. **Slot utilization** (issue #531) — is every currently-targeted dispatch
   slot occupied?
2. **Worker DB-pool pressure** (`pool.rs` / the worker's `deadpool` pool
   `status()`) — is the pool at capacity, or does it have callers waiting for
   a connection?
3. **Claim-to-dispatch permit-wait latency** — the longest time, since the
   tuner's last tick, that a claimed task spent waiting for a local dispatch
   permit before starting.

Decision order (first match wins), evaluated once per control-loop tick:

1. **Pool pressure** → shrink. Protecting the worker's own database
   connection pool from exhaustion takes priority over slot utilization — a
   burst that is about to exhaust the pool must back off even if dispatch
   slots still look busy.
2. **Every current slot occupied AND a sustained permit wait observed** →
   grow. Full occupancy alone is not a signal to grow — a burst that drains
   within a single tick shouldn't ratchet capacity up.
3. Otherwise → hold.

Growth and shrink both move a fixed step (default 2 slots) per tick, clamped
to the configured band.

### Writing a custom controller

`SlotTuner` is a small trait:

```rust
pub trait SlotTuner: Send + Sync {
    fn decide(&self, observations: &SlotObservations) -> SlotTunerAction;
    fn name(&self) -> &'static str { "custom" }
}
```

`SlotObservations` exposes the current target, the configured band, current
in-flight occupancy, optional pool pressure, and the optional longest
observed permit wait. Install a custom controller with
`SlotTunerConfig::with_tuner(min, max, Arc::new(MyTuner))`.

## Resize mechanics (no replay or persistence surface)

Resizing is purely an in-process control over the existing semaphore:
**no new `WorkflowEvent` variant, no migration, no change to
`harvest_events`, no replay/determinism surface.** A workflow's history is
identical whether it ran under a static or a tuned worker.

When a tuner is configured, a dispatch semaphore is created with
`max_slots` total permits. The tuner immediately withholds
`max_slots - initial_target` of them as permits it holds itself:

- **Grow** simply drops withheld permits, making them available for
  dispatch.
- **Shrink** opportunistically re-acquires *free* permits back into the
  withheld set on every control-loop tick. It never blocks and it never
  revokes a permit already held by an in-flight task.

**Fair-queue fallback for a busy worker.** `dispatch_task` spawns every
claimed task as a queueing `semaphore.acquire()`, and claiming is not
throttled against the live target — under a real backlog (claims outrunning
dispatch capacity), the semaphore's wait queue can be continuously
non-empty. `tokio::sync::Semaphore` always assigns a released permit
directly to the oldest queued waiter before it is ever visible to a
concurrent `try_acquire()`, so a shrink that only ever tried `try_acquire`
could lose that race indefinitely for as long as the backlog persisted —
exactly the scenario a pool-protecting shrink exists to help with. When the
opportunistic `try_acquire` can't land a shrink in one tick, the runtime
instead joins the semaphore's own fair FIFO queue with a background
`acquire_owned()` (at most one such request in flight at a time). Once
queued there, nothing can "cut in front" of it — tokio's queue guarantees
eventual forward progress, so the shrink is retried on later ticks and
lands as soon as its turn comes up, rather than needing to win a race
against `try_acquire` on every single attempt. If a later tick decides to
grow instead (reversing the earlier intent), any outstanding queued
request is cancelled rather than left to compete with the grow's own
permit release.

This means **graceful shutdown and draining are unaffected for already-running
work**: a shrink decision never cancels or reclaims an already-dispatched
task. On worker shutdown the tuner's control loop releases every withheld
permit before exiting, so the existing drain path (which waits for all of a
semaphore's permits to become available again) completes correctly
regardless of whether the tuner had withheld capacity at the time.

**Known limitation — shutdown-time concurrency spike.** The withheld-permit
release above happens all at once, as soon as the worker's shutdown signal
fires, rather than being staged behind the drain. If tasks were already
claimed from the queue but are still blocked waiting for a dispatch permit
(because the tuner had shrunk below what was claimed), releasing the
withheld permits lets those queued dispatches start running immediately —
a brief burst of concurrency above the tuner's shrunk target, right as the
worker is trying to wind down and exactly when the tuner may have shrunk
*because* a downstream resource (typically the DB pool) was under
pressure. In practice the window is narrow (bounded by how many tasks were
claimed beyond the current live target at the moment of shutdown) and every
started task still completes normally — this does not corrupt state, only
temporarily exceeds the tuned band during shutdown. A future fix would have
`drain_in_flight` wait for the tuner's live target directly instead of the
full `max_slots`, removing the need to force-release withheld permits at
all; tracked as follow-up work under issue #548.

## Telemetry

Two new metrics, following ADR-0001 cardinality rules — labelled by
`slot_type` (and `decision` for the counter) only, never `execution.id`:

- `harvest.worker.slot_target` (gauge) — the tuner's current band-clamped
  target for one slot type. Composes with the `harvest.worker.slots_in_use`
  / `slots_available` gauges from issue #531 on the same dashboard.
- `harvest.worker.tuner_decisions` (counter) — one increment per
  control-loop tick, labelled with the decision that actually took effect
  (`grow` / `shrink` / `hold`) after band clamping.

See `docs/telemetry.md` for the full metric and label catalogue and example
PromQL queries.

## Composition with other concurrency knobs

The slot tuner is one of three independent concurrency controls in harvest,
and they compose as the *minimum* of whichever is most restrictive at any
moment — none of them is aware of, or adjusts, the others:

- **Per-key concurrency (issue #247, `#[workflow(concurrency(key = ...,
  limit = ...))]`)** bounds how many executions sharing the same resolved
  key (e.g. a tenant ID) may be `RUNNING` at once, enforced at claim time via
  an advisory lock. The slot tuner bounds the worker's *total* dispatch
  capacity across all keys; per-key concurrency bounds *admission* within
  that capacity. A worker with 50 tuned dispatch slots and a per-tenant
  limit of 10 still only ever runs 10 tasks for a single tenant at once,
  regardless of how many slots the tuner has granted overall.
- **Per-activity rate limits (issue #88, `#[activity(rate_limit_* = ...)]`)**
  bound the dispatch *throughput* of one activity type via a token bucket,
  enforced at claim/dispatch time. The slot tuner never grants extra tokens
  or bypasses a rate limit — a rate-limited activity can still be
  throttled even when the tuner has grown the semaphore to `max_slots`.

## Scope and cadence

- **Worker-local.** Each worker tunes its own dispatch semaphores
  independently; there is no fleet-global coordination or shared controller.
  Two workers in the same fleet can converge to different targets under the
  same load if their DB-pool sizing or observed latency differ.
- **Decoupled from the hot dispatch path.** The control loop runs on the
  worker's existing monitoring cadence (`poll_interval`, the same cadence
  the timeout and poison-pill checkers use — 500 ms by default), alongside
  those checkers. Dispatch itself only ever touches a lock-free atomic to
  record the longest recent permit wait; it never waits on the tuner.
- **Best-effort, not a hard rate guarantee.** The controller can, at most,
  leave the target anywhere within `[min_slots, max_slots]`; ramp speed is
  bounded by the step size and the tick cadence. Do not depend on the tuner
  converging within a specific number of milliseconds for correctness —
  size `min_slots` for your steady-state floor and let the tuner handle
  bursts.
- **Runs even when metrics are disabled.** Unlike the pure observability
  samplers, the tuner is a controller with a real effect on dispatch
  capacity — only the per-tick telemetry *emission* is gated on a metrics
  recorder being configured, never the control decision itself.
- **Does not resize the database pool.** `pool.rs` sizing stays entirely
  operator-configured; the tuner *reads* pool saturation as an input signal,
  it never grows or shrinks the pool itself.
