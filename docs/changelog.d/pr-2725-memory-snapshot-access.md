## Memory snapshot head follows the `memory` tool (PR #2725)

The memory snapshot told every run to "Edit them with the `memory` tool".
A run without that tool read the same sentence: a heartbeat tick or a
follow-up segment without `allow_memory_writes`, a read-only run, and each
draw of a strategy op. The model was told to call a tool that was not in
its tool list.

- `memory::render_snapshot` takes a `SnapshotAccess`. `Edit` keeps the old
  head. `Read` says that the run has no `memory` tool and cannot edit the
  notes, and that it records a correction with another tool or in its
  answer.
- Both heads now say "when this request started" and "from the next
  request or follow-up". The snapshot is read again at the start of each
  segment: a run, a follow-up, or one op of a durable agent. The old head
  said "run", which was wrong for a durable agent. The module doc of
  `memory.rs` says the same.
- The snapshot activity `agent_memory_snapshot` still records the edit
  head, so its output does not change. A run without the tool swaps the
  head with `memory::with_access`, which also swaps the old edit head of a
  snapshot recorded before this change.
- State memory (`AgentMemory::State`) renders the head of
  `AgentTask::memory_writable`. A draw always renders the read-only head.
- **Breaking:** `render_snapshot` takes a second argument. Pass
  `SnapshotAccess::Edit` for the old text.
- **Replay:** the patch `agent_read_only_memory_snapshot` gates both new
  heads. A store-memory run asks it only when it has no `memory` tool,
  because its activity records the head. A state-memory segment asks it
  at each render, so a history from before the patch rebuilds the old
  edit head byte for byte, for a segment and for the inputs of its draws. No new
  `WorkflowEvent` variant, no migration.
- Tests: unit tests in `memory.rs` for both heads and the swap.
  `always_on` checks the head of a tick with and without
  `allow_memory_writes`, and of an attended and a woken follow-up segment.
  `memory_state` checks the head of a state-memory tick.
