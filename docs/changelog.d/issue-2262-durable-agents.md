## Phase X.Y — durable agents and agent meshes (epic #2262)

The agent loop (#1973), the keyed entity (#1975) and the agent mesh join into
one durable `Agent`. `DESIGN-2262.md` is the spec. Each sub-issue below has
its own red tests.

### A first-class durable `Agent` (issue #2263)

- `autumn_harvest_agent::agent`: `Agent`, `AgentId` (`<kind>/<name>`, the
  workflow id), `AgentOp` (`user_message`, `control`), `Control` (`pause`,
  `resume`, `reset_memory`, `clear_transcript`), `AgentProfile` (one per kind,
  on the harness), `AgentState` and `AgentStatus`.
- The workflow `agent` runs `Entity::run`. Each op runs one segment of the
  agent loop with the whole harness: tools, policy, approvals, budgets,
  memory, the loop guard and delivery.
- The activity `agent_profile` reads the profile once per run. Replay reads
  the record. The activity `agent_memory_reset` clears the scope
  `agent:<id>`. `MemoryStore::clear` is a new method. Its default refuses.
- The inbox kernel `agent::inbox::admit` holds work while the agent is
  paused, and runs it in order on resume.
- A failed op keeps the transcript as it was, and keeps the labels and the
  tokens that it used.
- Run ids are `<incarnation>:s<n>`. The state carries the tool-round counter,
  so approval names never repeat.
- SQLite: `sqlite::send`, `sqlite::delete`, `sqlite::drive`,
  `sqlite::state` and `sqlite::current_run`.
- `examples/agent_session_entity.rs` moves to the agent crate and runs on the
  new type. The §9 limit "the session entity is separate" is gone.

No migration. No `WorkflowEvent` variant. An `agent_loop` history replays as
it ran: the refactor changes no command of `agent_loop`.

Tests: `autumn-harvest-agent/tests/durable_agent.rs` (12 tests: order, a
restart at an approval, a process crash inside a model call, labels across
a checkpoint, pause and resume, a failed op, approval names, an unknown kind,
delete and a new life, reset memory, replay). Each guarantee was checked by a
mutation that turns its test red.

### Mid-turn messages: queue, steer and interrupt (issue #2265)

- `UserMessage::urgency`: `queue` (the default), `steer` and `interrupt`.
  `Agent::urgency_signal` names the signal that goes before the op:
  `agent.steer` with a `Steer`, or `agent.interrupt` with an `Interrupt`.
- A durable agent races each model turn against `agent.interrupt`. The race
  records its winner, so replay takes the same branch.
- `Tool::interruptible` (default `false`) and `FnTool::interruptible`. Only
  an opted-in tool call races an interrupt. Any other call runs to its end,
  and no later call of the round runs.
- An approval wait ends as a rejection on an interrupt.
- `AgentStop::Interrupted`. `ModelTurnRequest::interruptible` and
  `ModelTurn::interruptible` are left out at their defaults, so an
  `agent_loop` request records the same bytes as before.
- A steer and its op dedupe through `AgentState::steer`, so each steer runs
  once.

Tests: `autumn-harvest-agent/tests/agent_interrupts.rs` (9 tests). Two are
simulator sweeps. In every seed a write tool is never cancelled halfway, and
some seeds cancel an opted-in tool. Each guarantee was checked by a mutation
that turns its test red.

### Keyed-entity wakes: run an op at a deadline (issue #2266, engine part)

Part of epic #2262. ADR 0006 records the
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

### The agent cost ledger on SQLite (issue #2275)

Section 13 of the design note `DESIGN-2262.md` of epic #2262. The SQLite
backend now writes the agent cost ledger of issue #1996. Before
this change, the SQLite agent path called the model with no activity
context, so it wrote no ledger rows.

**Table.** The SQLite schema gains `harvest_llm_ledger`, with the columns
and CHECK bounds of the Postgres table. The key is `(exec_id, event_seq,
call_index)`. `event_seq` is the `seq` of the completion event, as
`event_id` is on Postgres. `recorded_at` is the drive clock at the
completion, in epoch milliseconds, with an index for window reads. The
table is new, so `CREATE TABLE IF NOT EXISTS` adds it to an older file on
the next open. `harvest_events` does not change, and no `WorkflowEvent`
variant is added, so replay does not change.

**Writes.** The runtime writes one row per call in the transaction that
appends `ActivityCompleted` or `LocalActivityCompleted`. A crash before that
commit writes no row, and the re-run writes the rows of its own attempt
once. A failed, timed-out or oversized attempt writes no row, as on
Postgres. Two sources feed it:

- `ActivityContext::record_llm_call` in an `#[activity]` body, on the
  worker path and the local path.
- The new `SqliteRuntime::register_activity_with_ledger(&info, body)`. The
  closure returns `Ok((output, Vec<LlmCall>))`.

**Reads.** `SqliteRuntime::llm_ledger(exec)` and
`SqliteRuntime::llm_ledger_between(from, until)` return `LlmLedgerRow`
values. `SqliteReader` has both reads, and a file from before this change
reads as having no rows. Retention removes the rows with their run.
`autumn_harvest_sqlite` re-exports `LlmCall`.

**Agent adapter.** `sqlite::register` registers `agent_model_turn` with the
ledger variant. One builder, `AgentHarness::ledger_call`, makes the call for
both backends, so one script gives the same rows. A response cache hit
records no row. The §9 limit "The cost ledger needs Postgres" is removed
from `docs/agent-adapter.md`.

**Tests.**

- `autumn-harvest-agent/tests/sqlite_ledger.rs`: one row per model call of
  a three-turn script; the same rows as the Postgres activity for the same
  script; no row for a cache hit or a tool call; a crash inside a model
  call and inside a tool call, each in a child process that aborts, then
  resumes with exactly one row per turn.
- `autumn-harvest-sqlite/tests/integration/llm_ledger.rs`: a ledger
  closure, an `#[activity]` body and a local activity; a failed attempt
  and its retry; a timeout; a restart; an older file; the reader; a window
  read; retention.
- Unit tests in the SQLite `llm_ledger.rs` and `worker.rs`: the CHECK
  bounds match the core limits, the model id charset, the per-attempt cap,
  and a rolled-back or failed finalize writes no row.
