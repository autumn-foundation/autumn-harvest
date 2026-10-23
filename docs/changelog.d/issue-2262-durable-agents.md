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
