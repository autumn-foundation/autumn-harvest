## Agent episode log and consolidation pass (issue #2454)

`AgentProfile::episodes` keeps an episode log for each durable agent of a
kind. It is off by default. The workflow `agent_consolidate` turns the
episodes into entity–relation facts in the memory of the agent. The design
is in `docs/design/DESIGN-2454.md`.

- At the end of an op that keeps its transcript and may write memory, the
  activity `agent_episode_append` appends one episode to the scope
  `agent:<id>:episodes`. The key `episode:<run id>` makes a retry or a
  crash append once.
- `max_episodes` bounds the log. The oldest goes first, and the next pass
  reports each unconsolidated episode that an eviction dropped.
- A pass checks the LLM budget of its run before its one model call
  (#1997), and records one cost ledger call (#1996). Each fact write
  carries `<pass>:<hash>`, so a crash mid-pass writes no fact twice.
- A fact lists its source episode ids and carries the labels of every
  episode of its pass. A store with no label support refuses it.
- The trigger is a schedule (`consolidate::schedule`) or every N episodes,
  as a child pass. The trigger counts appends since the last pass, so a
  failed pass waits for N more appends, also on a full log. A failed pass
  fails the op. On SQLite, `sqlite::consolidate` starts one pass.
- A clear keeps an empty log with a new generation. A pass that runs
  across a reset writes no fact after it and ends as `reset`. A log keeps
  only hashes of derived facts.
- `Control::ResetMemory` clears the episodes, behind the patch gate
  `agent_episode_reset`, and counts them in `AgentState::episode_reset`.
  `harvest agents erase` reports `episodes_erased` and `facts_erased`, and
  erases each scheduled pass by its search attribute
  `agent_consolidation_of`.
- New activities: `agent_consolidate_read`, `agent_consolidate_turn`,
  `agent_consolidate_write`, `agent_episode_append` and
  `agent_episode_reset`. A worker with a hand-picked activity list must add
  them. No new `WorkflowEvent` variant and no migration.
- `formal/verus/src/agent_episodes.rs` proves the eviction and trigger
  kernels in `bounds.rs`.

Tests: `autumn-harvest-agent/tests/episodes.rs` covers each acceptance
criterion, with two crash tests that abort a child process. The CLI tests
in `agents_cli.rs` and `agents.rs` cover the erase report and the scheduled
passes.
