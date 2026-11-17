## Memory snapshot head follows the `memory` tool

The memory snapshot told every run to "Edit them with the `memory` tool".
A run without that tool read the same sentence: a heartbeat tick or a
follow-up segment without `allow_memory_writes`, a read-only run, and each
draw of a strategy op. The model was told to call a tool that was not in
its tool list.

- `memory::render_snapshot` takes a `SnapshotAccess`. `Edit` keeps the old
  head. `Read` says that the run has no `memory` tool and cannot edit the
  notes, and that it records a correction with another tool or in its
  answer.
- The snapshot activity `agent_memory_snapshot` still records the edit
  head, so its output does not change. A run without the tool swaps the
  head with `memory::with_access`.
- State memory (`AgentMemory::State`) renders the head of
  `AgentTask::memory_writable`. A draw always renders the read-only head.
- **Breaking:** `render_snapshot` takes a second argument. Pass
  `SnapshotAccess::Edit` for the old text.
- **Replay:** the patch `agent_read_only_memory_snapshot` gates the
  read-only head. A run asks it only when it has no `memory` tool. A
  history from before the patch replays with the edit head. No new
  `WorkflowEvent` variant, no migration.
- Tests: unit tests in `memory.rs` for both heads and the swap.
  `always_on` checks the head of a tick with and without
  `allow_memory_writes`, and of an attended and a woken follow-up segment.
  `memory_state` checks the head of a state-memory tick.
