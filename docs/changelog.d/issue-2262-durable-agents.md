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

### Per-agent heartbeats and follow-ups (issue #2266)

- `AgentHeartbeat`, `AgentProfile::heartbeat`, `HeartbeatSetting` and
  `Control::SetHeartbeat`. Each agent key has its own heartbeat, driven by
  its own durable timer through `Entity::run_with_wakes`.
- `AgentOp::HeartbeatTick` and `AgentOp::FollowupDue` are inbox ops. They do
  not decode from a signal.
- `AgentProfile::followups`: a booked follow-up is a deadline in the state.
  `AgentState::chain` keeps the chain cap across a checkpoint.
- A tick is unattended and read-only unless it allows actions. A silent tick
  leaves the transcript as it was. `agent::wake::next_heartbeat` books the
  next tick strictly after the time a tick ran.
- Each op records one `system_now` side effect and upserts the search
  attributes, `agent_next_wake_at` and `agent_wake_reason` among them.
- `docs/agent-adapter.md` §10 drops the "one heartbeat per deployment"
  caveat.

Tests: `autumn-harvest-agent/tests/agent_wakes.rs`. 1,000 agents with
distinct intervals tick on their own schedule. A tick and a message never
overlap, in either order. An unattended tick cannot run a write tool without
`allow_actions`. A restart during a wait ticks and reports once. A paused
agent skips ticks and catches up once. The follow-up chain cap holds across
checkpoints. The next wake is listed. Unit and property tests pin the wake
kernel.

### Durable peers with exactly-once messages that carry mandates (issue #2269)

- `Peer::agent(name, description, &id)` and `Peer::reply_timeout`. A
  `delegate` call to a durable peer sends `AgentOp::PeerMessage` with a
  `PeerEnvelope` and waits on a durable promise for a `PeerReply`.
- The send is `signal_external_workflow_by_id_with_idempotency` with the key
  `<from>:<seq>`. A durable sender sends `from` as
  `<agent id>@<incarnation>`. `AgentState::outbound_seq` keeps one sequence
  for each receiver.
- The receiver keeps an applied set of each sender
  (`agent::dedupe::Inbound`): a floor and a set above it. It never drops a
  number for being under a high-water mark.
- The receiver runs under `Mandate::accept` of its local mandate and the
  inbound one, and starts with the labels of the sender. The reply carries
  its labels back.
- Each envelope carries the waiting chain. `Refusal::Deadlock` refuses a send
  to an agent that waits. `Refusal::PeerUnavailable` refuses a send to an
  agent with no run.
- `ReportSource::Peer`.

Tests: `autumn-harvest-agent/tests/durable_peers.rs` (8 tests): delivery once
across a restart, a duplicate dropped and a late message kept, a mandate that
cannot widen in either direction, labels that taint the receiver, an A→B→A
request that ends as a deadlock with no timer, an agent with no run, and the
reply deadline. Unit and property tests pin the applied set. Each guarantee
was checked by a mutation that turns its test red.

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

### A model per agent and per peer (issue #2271)

- `AgentHarness::model_named(name, model)` adds a model to a registry.
  `DEFAULT_MODEL` names the model of `AgentHarness::new`.
- `AgentTask::model`, `Peer::model`, `AgentProfile::model`,
  `AgentHeartbeat::model` and `HeartbeatTask::model` name a model. A child
  with no model keeps the model of its parent.
- `ModelTurnRequest::model` records the name in the turn input, so replay
  asks the same model. An unknown name fails the turn with no retry.
- The response cache key and the cost ledger use the id of the model that
  answers the turn.
- `Mandate::models` is an allowlist that only narrows. A delegation to a
  peer on another model is refused as `Refusal::ModelForbidden`. A root run
  on a forbidden model fails before any model call. A durable receiver
  checks its own model against the mandate that it accepts.
- `eval::compare` runs two candidates on one recorded run.
  `Candidate::model` and `Candidate::agent_model` set the model of each.
- The §9 limit "All agents share the harness model" is removed from
  `docs/agent-adapter.md`, and the matching §12 limit from
  `docs/agent-mesh.md`.

**Tests.** `autumn-harvest-agent/tests/model_per_agent.rs`: a parent and a
child on two models record both ids in the ledger; a child keeps the model
of its parent; the cache never serves one model's answer to another; a
mandate that forbids a model refuses the delegation; a root on a forbidden
model fails before any call; an unknown model fails with no retry; a
profile and its heartbeat name their models. `durable_peers.rs`: a receiver
on a forbidden model refuses the message. `eval.rs`: two models compared on
one source, and one agent of a tree on another model. Mandate property
tests cover the allowlist.

### Concurrent children (issue #2270)

- `AgentTask::parallel_children` and `AgentProfile::parallel_children` let
  one model turn run up to that many delegations at the same time, bounded
  by `MAX_PARALLEL_CHILDREN`.
- Each child of a batch runs as a child `agent_loop` run. The parent joins
  the runs and merges their usage, steps, labels and records in call order.
- The siblings split the token remainder and the delegation remainder with
  `bounds::sibling_cap`. Each child starts with the labels of the tree.
- `AgentReport::run_id` names the run, so a parent records the run id of a
  child run.
- `eval::evaluate` answers each child run of a source with its recorded
  report.
- ADR 0013 records why a child is a child run: two joined branches that
  each take many steps can swap results on replay. The §12 limit "children
  never run at the same time" in `docs/agent-mesh.md` is narrowed.

**Tests.** `autumn-harvest-agent/tests/fan_out.rs`: three children as child
runs with exact replay; no bound keeps the inline path; the bound splits a
batch; the token split; the delegation split; the labels of every sibling
reach the parent; a child starts with the labels of its parent; a durable
agent fans out; a simulator sweep over completion orders. `eval.rs`: a
fan-out source evaluates from its recorded children. The wall-time test
runs on Postgres workers in the `examples/agent-mesh` suite.
