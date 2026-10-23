## Engine — Keyed-entity wakes: run an op at a deadline (issue #2266)

Part of epic #2262. [ADR 0006](../adr/0006-keyed-entity.md) records the
amendment. An entity can now run a heartbeat or a follow-up with no
scheduler outside it.

**API.** `Entity::run_with_wakes(handler, wake)` and `entity::Wake { at, op }`,
built with `Wake::new(at, op)`. The hook `wake(&state)` runs before each
step. It returns a deadline and an op, or `None`.

**Semantics.**

- `None` gives the step of `Entity::run`: a waiting op, else a wait for the
  op signal.
- A wake makes the loop read the clock once with `ctx.system_now()`. A due
  wake runs before any waiting op, so a backlog cannot starve it.
- A wake that is not due lets a waiting op run first. With no waiting op,
  the loop races the op signal against the deadline with
  `wait_for_signal_timeout`. A signal that wins runs as usual. A timer that
  fires runs the wake op.
- A wake op is typed, so it does not decode and adds no op bytes. The
  handler, the `Ok` and `Err` rules, the op count, the snapshot, the
  published view and the checkpoint check apply to it as to a decoded op.
  A delete keeps its rules.

**Invariants.** No new `WorkflowEvent` variant and no migration.
`Entity::run` is a thin call into the same loop with no hook. It reads no
clock and starts no timer, so existing entity histories replay with no
change. A step with a wake records one `SideEffectRecorded` event of kind
`Now`. A wait also records a timer. The hook must be a pure function of the
state. A failed wake op keeps the old state, so the same wake is due again
at once. The timer counts whole seconds, so a wake can run up to one second
late.

**Tests.** `entity::tests`: a wake fires with no op waiting; a queued op
runs before a wake that is not due; a due wake runs before carried and
signalled ops; a hook that returns `None` starts no further timer; `run`
records the pinned event shape with no clock read and no timer, and a hook
that never wakes records the same events; a checkpoint carries the state,
and the next run computes its wake from it. Each wake run replays clean.
