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

**Tests.** `autumn-harvest-agent/tests/durable_agent.rs`:

- Two messages run one at a time, in order.
- A restart at an approval asks no recorded model call again.
- A process crash inside a model call runs only that call again.
- The flow labels survive a checkpoint.
- A paused agent holds its work and runs it in order on resume.
- A failed op keeps the transcript and the labels.
- Approval names never repeat across ops.
- An unknown kind fails each op and keeps the agent.
- A delete ends the life, and a new send starts another.
- `reset_memory` clears the scope of the agent.
- A recorded agent history replays.

A mutation of each guarantee turns its test red.

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

**Tests.** `autumn-harvest-agent/tests/agent_interrupts.rs`:

- An interrupt during a model call cancels the call and records it.
- An interrupted history replays.
- An interrupt ends an approval wait as a rejection.
- A stale interrupt does not stop the next op.
- A steer joins the running op, and its own op does nothing.
- A steer with no running op runs once, as a message.
- A message acknowledged before a crash runs once.
- Two simulator sweeps. No seed cancels a write tool halfway. Some seeds
  cancel an opted-in tool.

A mutation of each guarantee turns its test red.

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
- `docs/agent-adapter.md` §10 narrows the "one heartbeat per deployment"
  caveat. It still holds for `heartbeat::schedule`. A durable agent runs its
  own heartbeat.

**Tests.** `autumn-harvest-agent/tests/agent_wakes.rs`:

- 1,000 agents with distinct intervals tick on their own schedule.
- A tick and a message never overlap, in either order.
- An unattended tick cannot run a write tool without `allow_actions`.
- A restart during a wait ticks and reports once.
- A paused agent skips ticks and catches up once.
- The follow-up chain cap holds across checkpoints.
- The next wake is listed.

Unit and property tests pin the wake kernel.

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

**Tests.** `autumn-harvest-agent/tests/durable_peers.rs`:

- A peer message arrives once across a restart, and gets its answer.
- Each receiver counts its own seqs.
- The receiver does not take a new life of a sender for a duplicate.
- The receiver drops a duplicate and keeps a late message.
- A mandate cannot widen in either direction.
- The labels of the sender taint the receiver.
- An A→B→A request ends as a deadlock, with no timer.
- A send to an agent with no run gets `peer_unavailable`.
- The sender does not wait for a reply after its deadline.

Unit and property tests pin the applied set. A mutation of each guarantee
turns its test red.

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

**Tests.** `entity::tests`:

- A wake fires with no op waiting.
- A queued op runs before a wake that is not due.
- A due wake runs before carried and signalled ops.
- A hook that returns `None` starts no further timer.
- `run` records the pinned event shape, with no clock read and no timer.
- A hook that never wakes records the same events as `run`.
- A checkpoint carries the state, and the next run computes its wake from
  it.
- Each wake run replays clean.

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

- `autumn-harvest-agent/tests/sqlite_ledger.rs`:
  - One row per model call of a three-turn script.
  - The same rows as the Postgres activity for the same script.
  - No row for a cache hit or a tool call.
  - A crash inside a model call, and one inside a tool call. Each runs in a
    child process that aborts. The resume writes exactly one row per turn.
- `autumn-harvest-sqlite/tests/integration/llm_ledger.rs`:
  - A ledger closure, an `#[activity]` body and a local activity.
  - A failed attempt and its retry, a timeout and a restart.
  - An older file, the reader, a window read and retention.
- Unit tests in the SQLite `llm_ledger.rs` and `worker.rs`:
  - The CHECK bounds match the core limits.
  - The model id charset and the per-attempt cap.
  - A rolled-back or failed finalize writes no row.

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

**Tests.**

- `autumn-harvest-agent/tests/model_per_agent.rs`:
  - A parent and a child on two models record both ids in the ledger.
  - A child keeps the model of its parent.
  - The cache never serves the answer of one model to another.
  - A mandate that forbids a model refuses the delegation.
  - A root on a forbidden model fails before any call.
  - An unknown model fails with no retry.
  - A profile and its heartbeat name their models.
- `durable_peers.rs`: a receiver on a forbidden model refuses the message.
- `eval.rs`: two models compared on one source, and one agent of a tree on
  another model.
- Mandate property tests cover the allowlist.

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

**Tests.**

- `autumn-harvest-agent/tests/fan_out.rs`:
  - Three children run as child runs, with exact replay.
  - With no bound, the inline path stays.
  - The bound splits a batch.
  - The token split and the delegation split.
  - The labels of every sibling reach the parent.
  - A child starts with the labels of its parent.
  - A durable agent fans out.
  - A simulator sweep over completion orders.
- `eval.rs`: a fan-out source evaluates from its recorded children.
- The wall-time test runs on Postgres workers in the `examples/agent-mesh`
  suite.

### Verus proofs of the agent kernels (epic #2262)

The blue-team step of `DESIGN-2262.md` asks for Verus proofs of the pure
kernels. Four new files in `formal/verus/src/` mirror them:

- `agent_inbox.rs` proves `agent::inbox::admit`. A spec models the inbox
  under any sequence of ops. The ops that ran, then the held ops, are
  exactly the work ops received, in arrival order. An acknowledged op is
  never lost, and none runs twice (invariant I2).
- `agent_dedupe.rs` proves `is_new` and `next_floor`. After any order of
  numbers, the floor and the set above it equal the plain applied set. So
  the receiver never drops a new message as a duplicate. The bound on the
  set makes no false duplicate either.
- `agent_wake.rs` proves `next_heartbeat`. A tick books the next tick
  strictly after the time that it ran. Messages never move a booked tick.
- `agent_siblings.rs` proves `sibling_cap`. The shares of all siblings sum
  to the remaining tokens, and each share is the base share or one more.

`next_heartbeat` now saturates by hand, because Verus has no spec of
`i64::saturating_add`. The result is the same for every input. The guard
`verus_proofs_coverage.rs` now reads the doc comments of
`autumn-harvest-agent/src` too.

Each proof was checked by a mutation of its spec or its copy that Verus
then rejects.

### Each model turn records only its new messages (issue #2264)

- `ModelTurnRequest::prefix` names the window that a turn adds to.
  `ModelTurnRequest::remember` keeps the window of a turn for the next one.
- `transcript::chain` is a chained SHA-256 digest over the messages. The
  workflow hashes each message once.
- `TranscriptStore`, `InMemoryTranscriptStore` and
  `AgentHarness::transcript_store` hold the windows. On a miss, a turn fails
  with `TranscriptPrefixMissing` before any model call, and the workflow
  sends it again in the full form.
- `AgentTask::transcript_delta` turns it on, and `AgentTask::new` sets it.
  A recorded task has no flag, and `agent_loop` asks the patch gate
  `agent.transcript-delta`, so an old run replays as it ran.
- A durable agent keeps the last sent window in `AgentState::sent`, so a
  later op sends a delta too.
- The §9 limit "The transcript rides in history" in `docs/agent-adapter.md`
  is revised.

**Tests.** `autumn-harvest-agent/tests/transcript_delta.rs`:

- A 50-turn run stores at least 80% less history (86% measured).
- A full-form history replays.
- A delta history replays strictly.
- A cold store falls back to the full form.
- The cache key is the same in both forms.
- Eval gives the same result.
- A durable agent sends deltas across ops.
- A trimmed transcript goes in the full form.
- The request cap measures the whole window.
- The in-memory store has a byte bound.

### Mesh-wide controls (issue #2272)

- `AgentProfile::mesh` and `AgentTask::mesh` name the mesh of an agent. Each
  model turn and peer message carries it, and each run publishes the search
  attribute `agent_mesh`.
- `MeshBudget` caps the tokens or the cost of a mesh for each day, month or
  fixed window. `AgentHarness::mesh_budget` and `AgentHarness::mesh_spend`
  set it up. The model-turn activity refuses a turn of a spent mesh with
  `MeshBudgetExceeded` before the model call.
- A refused durable agent parks (`AgentStatus::Parked`). Its work waits, the
  refused message first, its ticks are skipped, and a wake at the period
  end opens it.
- `sqlite::LedgerSpend` sums the cost ledger of the runs of a mesh.
- `sqlite::pause_mesh` interrupts and pauses every running agent of a mesh,
  and returns a `MeshPause` audit record.
- A memory entry keeps the flow labels of the run that wrote it
  (`MemoryBlock::labels`). A run that reads the snapshot takes them. The
  snapshot activity records `MemorySnapshot`, which is plain text when it
  has no labels, so an old history still reads.
- `MemoryStore::apply_labelled` refuses a write from a run with labels by
  default, so a store that cannot keep labels never holds an untainted
  copy. `InMemoryMemoryStore` keeps labels.
- `Control::ClearTranscript` keeps only the labels of the memory.
- The §12 limit "A memory note carries no label" in `docs/agent-mesh.md` is
  removed.

**Tests.**

- `autumn-harvest-agent/tests/mesh_controls.rs`:
  - A mesh over its daily budget parks the next turn of every agent.
  - A parked agent opens when the period ends.
  - Another mesh keeps its own budget.
  - A pause stops every agent of a mesh within one tick.
  - A pause stops an agent that waits for an approval.
  - A pause leaves other meshes alone.
  - A note written under untrusted input taints the run that reads it. This
    holds for a store and for state memory.
  - A store that cannot keep labels refuses a labelled write.
- Unit tests in `mesh_control.rs` cover the period windows and the caps.

### Agent memory as entity state (issue #2273)

- `AgentMemory::State` keeps the memory blocks in `AgentState::memory`. The
  workflow runs the `memory` tool itself, so an edit commits with the op
  snapshot. No store and no edit key are involved.
- The system prompt of an op holds the memory as it was when the op
  started, so a provider prompt cache keeps its prefix.
- The memory lives in the entity snapshot events and the
  `harvest_entity_state` projection, which erasure and codec rotation cover
  (issue #2259).

**Tests.** `autumn-harvest-agent/tests/memory_state.rs`:

- An edit commits with the op and shows in the next op.
- Memory reads are stable within a segment.
- The memory is in the entity snapshot.
- `ResetMemory` clears it.
- A crash in a child process after the memory call leaves memory and
  history consistent. The edit applies once.

### Live tokens from inside the model-call activity (issue #2268)

- `ActivityContext::publish_progress` sends a live, best-effort chunk while
  an activity runs. Nothing is stored: no event and no
  `harvest_stream_chunks` row. Each chunk carries the activity, the attempt
  and its index in the attempt, so a reader drops the text of a failed
  attempt. `ActivityContext::with_progress_sink` and `CaptureProgressSink`
  serve tests and embedded runtimes.
- On Postgres a background task sends each chunk with `pg_notify` on the
  progress channel of the run. A full queue of 256 drops a chunk, so a slow
  database never slows the activity.
- `GET /workflows/{id}/stream` sends an activity chunk as an `activity`
  frame with no `id:`. `ProgressNotifyPayload::activity` marks it, so the
  `seq` contract of the workflow chunks stays as it was.
- `DurableCall::live` adds the `activity` frames to a durable route stream.
  One route then gives both layers: live tokens, and the durable text that a
  resume gets once. `DurableStreamListener::connect_live` listens on both
  channels of a run with one connection.
- Agent: `AgentModel::chat_stream`, `AgentTask::stream`,
  `AgentProfile::stream`, `AgentHarness::live_sink`, `LiveEvent` and
  `TurnText`. Small deltas join until 256 bytes wait, or until 50 ms pass
  and the provider sends its next event. The workflow
  publishes the text of each finished turn with `publish_durable_progress`.

**Tests.**

- `autumn-harvest/tests/integration/activity_progress_db_tests.rs`:
  - A live chunk arrives before the activity completes, and nothing stores
    it.
  - A retried attempt tags its chunks with the new attempt.
- `durable_stream_tests.rs`: a live listener keeps the activity chunks in
  order, drops workflow chunks, and puts a stored wake first.
- `autumn-harvest-plugin/tests/durable_stream_integration.rs`: one stream
  gives the live chunks with no id and the stored text once.
- The `durable_route::stream` unit tests prove that a live chunk costs no
  table read and never moves the cursor.
- `autumn-harvest-agent/tests/live_tokens.rs`:
  - Tokens arrive before the turn completes.
  - A retry starts the live text again.
  - Live text never reaches history.
  - A reconnect gets the durable text of each turn once.

### Agent lifecycle: idle, list, pause and erase (issue #2267)

- An idle agent is a row in the database. A fresh runtime serves its next
  message and its next wake.
- `agent_mesh` joins the agent search attributes, and `parked` joins the
  statuses.
- Migration `20261023202848_harvest_agent_wake_index` adds the partial
  expression index `idx_harvest_we_agent_wake` on
  `(search_attrs ->> 'agent_next_wake_at')::numeric`. A list of the agents
  due to wake reads only those agents.
- The `filter` grammar of `GET /workflows` takes `workflow_id` (`=`, `!=`,
  `IN`), so a caller lists every run of one business id.
- `EraseOutcome::ledger_rows_deleted`: an erasure deletes the agent cost
  ledger rows of the run (issue #1996). The spend of an erased run leaves
  the budget sums.
- CLI: `harvest agents list | describe | pause | resume | erase`. `erase`
  resets the memory and the workspace of a running agent, then deletes it.
  It erases each run of each life and prints one report.

**Tests.**

- `autumn-harvest-agent/tests/agent_lifecycle.rs`:
  - An idle agent needs no runtime and wakes on a message.
  - It wakes on its timer in a fresh runtime.
- `workflow_filter_integration.rs`: the agent wake list reads
  `idx_harvest_we_agent_wake` with a range condition (`EXPLAIN`).
- `llm_ledger_tests.rs`: an erase deletes the ledger rows of the run.
- `autumn-harvest-cli`:
  - The request mapping, the filter literal and the erase report.
  - `agents erase` against a scripted server.

### Persistent workspaces (issue #2274)

- `autumn_harvest_agent::workspace`: `Workspace`, `WorkspaceRef`,
  `WorkspaceMount`, `WorkspaceStore`, `WorkspaceBlobs`,
  `InMemoryWorkspaceBlobs` and `DirWorkspaceBlobs`.
- A snapshot is content-addressed by the SHA-256 digest of its plain bytes,
  encoded by the payload codecs, and kept outside history. History records
  only its reference.
- `AgentProfile::workspace` and `AgentHarness::workspace` turn it on.
  `Tool::uses_workspace` and `FnTool::uses_workspace` opt a tool in.
  `ToolContext::workspace` holds the files of one call. `SandboxTool`
  mounts the files and keeps its outputs.
- `ToolOutcome::workspace` carries the new head. Only a recorded result
  moves the head, so the snapshot of a stale attempt never becomes it.
- The end of each op that moved the head keeps one snapshot. The activity
  `agent_workspace_retain` deletes the others of its generation or older.
  `Control::ResetWorkspace` and the activity `agent_workspace_reset` delete
  them all. `harvest agents erase` sends the reset.
- Caps: 64 MiB of file content and 10,000 files for each snapshot by
  default.

**Tests.**

- `autumn-harvest-agent/tests/agent_workspace.rs`:
  - A file from turn 1 is visible in turn 5 after a restart.
  - A stale attempt never becomes the head, and the op end deletes its
    snapshot.
  - A reset removes every snapshot, an orphan included.
  - The store holds only ciphertext.
- Unit tests in `workspace.rs` and `sandbox.rs` cover the digest check, the
  caps, the path rules, the directory store and the sandbox mount.

### The agent-mesh capstone (issue #2276)

- `examples/agent-mesh` runs an always-on support desk with three durable
  agents in one SQLite file. The triage agent wakes on a heartbeat. It asks
  the researcher and the critic as durable peers, under mandates that only
  narrow. A write waits for a person. The mesh shares one daily budget.
- `--model claude` runs the agents on `autoresearch::claude::ClaudeModel`,
  which streams over Server-Sent Events. The offline model needs no key.
- `docs/comparison.md` §12 compares durable agents, with each competitor
  claim cited.

**Tests.**

- `tests/e2e.rs`:
  - A check reports the inbox and files nothing.
  - An answer asks each peer once, waits for approval across a restart and
    files once.
  - A rejected reply is never filed.
  - A spent budget parks the mesh until the next day.
- `tests/simulation.rs`: a seeded sweep with restarts, reordered steps,
  duplicate ops, clock jumps and a reckless model. Four invariants hold on
  every seed, and each nemesis fires on some seed.
- `tests/postgres.rs`:
  - Three children of one turn overlap on the wall clock.
  - One slot runs them one after another.
  - The live text of a turn reaches a progress listener while the turn
    runs.
  - A durable peer on another shard gets one message and answers once.
  - An erased agent keeps no memory, stream chunk or transcript text in
    any table.
- CI runs the offline suites, the README demo and clippy. The Postgres
  suite runs from its manifest row.

### Review fixes (epic #2262)

A review from five angles found these. Each fix has a test.

- **Mandates.** A read-only inbound mandate makes a durable receiver
  read-only. A peer segment books no follow-up. A `memory` call in a segment
  that may not write memory gets an error, whatever the policy says.
- **Peer replies.** Only the segment of a message answers it. A failed
  segment sends an error and no text. A full held queue answers a waiting
  sender at once.
- **Heartbeats.** `Control::SetHeartbeat` only narrows the profile:
  `AgentHeartbeat::narrowed` and `MIN_CUSTOM_HEARTBEAT_SECS`. A tick books
  the next tick from its own end, so a long tick never starves messages.
- **Durability.** `Control::Resume` stops when an op parks the agent again,
  so a spent budget never loops. A steer that a failed or silent segment
  took runs later as its own op. A failed profile lookup keeps the run and
  its state. A fan-out child run gets the waiting chain of its parent, so a
  send back up the chain is a deadlock.
- **Clock.** A SQLite ledger body gets the drive clock of its claim, and the
  mesh budget window uses it. `LedgerSpend` reads rows with no upper bound.
  On SQLite, a live `system_now` returns the clock of its decision cycle,
  through `WorkflowExecuteSpanMeta::drive_clock`. An `_as_of` drive then
  keeps one clock, so a parked agent never wakes at once and parks again.
- **Workspace.** A read-only run never moves the head. The state keeps the
  flow labels of the run that last moved it, and `ClearTranscript` keeps
  them. A store failure runs a workspace call again from the same head, up
  to three times. `DirWorkspaceBlobs` writes files that only the owner
  reads. A store with no keyed codec logs a warning.
- **Transcript store.** A read checks the digest of the window, so a
  changed window is a miss.
- **Erase.** `harvest agents erase` reads the `execution` object of the
  by-id answer. It pauses the agent first. It restarts an agent that ended,
  so its memory and workspace reset too. `stores_reset` reflects the final
  state, and `stores_error` names a failed reset.
- **Live tokens.** The Postgres progress sink sends every chunk that waits
  in one `pg_notify` statement, so a burst of tokens costs one transaction.
  The sink gives its pool connection back when its queue is empty. When it
  cannot get a connection, every chunk that waits drops.
  `GET /workflows/{id}/stream` resolves the shard of a run at most once per
  keepalive. The Claude adapter decodes complete lines only, so a character
  or a `\r\n` pair split across network chunks stays whole. Upgrade every
  API server before a worker sends activity chunks: an older server sends
  an activity chunk as a `progress` frame.
- **Capstone.** The offline desk model reads only the current op. A note
  search puts the best match first. A reply waits one day of desk clock for
  approval: `MeshConfig::approval_timeout`. The simulation checks the spend
  of each day apart, checks that triage applies each op key once, and
  asserts that each nemesis fires. A CI guard checks that each agent-mesh
  test target has a runner. A Postgres worker of the agents also polls the
  `default` queue, where the agent activities go.
- **PR review.** A follow-up delay counts from the end of its segment, so a
  long segment never books a follow-up that is already due. Each
  `DirWorkspaceBlobs` put writes its own scratch file. Two attempts that
  save one digest at once then never mix their bytes.
- **Workspace generations.** A late attempt of a retention could delete a
  newer head, because a key was the content digest only. The key now holds
  the generation of its segment, and `WorkspaceRef::generation` records it.
  Retention deletes only keys of the head generation or older, and a reset
  only keys below its own generation. `next_generation` keeps it above the
  last one and at least the recorded time. The model
  `formal/tla/AgentWorkspaceRetention.tla` and the tests
  `a_stale_retain_never_deletes_a_newer_head`,
  `a_newer_head_with_old_content_survives_a_stale_retain`,
  `a_stale_reset_never_deletes_a_later_head` and
  `a_late_retention_of_an_old_op_keeps_the_newest_head` cover it.

### Advisory RUSTSEC-2026-0387 (crypto-bigint 0.5.5)

The advisory appeared during this PR and fails `cargo deny` on `trunk-dev`
too. crypto-bigint 0.5.5 comes in through elliptic-curve 0.13, p256 and
p384, which autumn-web 0.8.0 and jsonwebtoken 11.1.0 pin. Each crate of the
chain is at its last release, so no bump is possible. `deny.toml` ignores
the advisory with a verdict of "undetermined from this repo". Code in this
workspace runs no P-256 or P-384 secret-key operation. The Web Push module
of autumn-web does, when a host app turns it on. The entry names the revisit
triggers.
