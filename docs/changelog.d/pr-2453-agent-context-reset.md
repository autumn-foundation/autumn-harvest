## Phase 2452.2453 — Progress notes and context reset in one agent run (issue #2453)

`ContextReset` is a new operator setting on `AgentTask` and
`AgentProfile`. It is off by default. With it on, the model gets the
`save_progress` tool. The workflow handles the call itself, as it handles
`schedule_followup`. Before each request, the loop measures the request. A
request that reaches `reset_at_percent` of the cap makes a reset due. The
loop then asks for a save turn. It sends a fixed notice, and only
`save_progress` runs in that round. The reset runs before the next request.
The new window is the system prompt, the task, the follow-up bookings and
the notes. The steers of a durable agent op follow.

Design decisions (`docs/design/DESIGN-2453.md`):

- The notes stay in an assistant `save_progress` call with a fixed result.
  No user message holds model text, as in `followup.rs`.
- The save turn lets the model read the last round before the reset drops
  it. No call with an effect runs in that turn.
- A reset whose window still reaches the threshold does not run.
- A reset clears the last sent window, so the next turn goes in full.
- A reset changes only the transcript. A reset clears no taint.
- A durable agent keeps the notes in `AgentState::notes`, with their flow
  labels. Each op shows them after its input. The transcript holds no copy,
  so `trim_window` never drops them. Only a segment that may write memory
  keeps them, and only such a segment gets the reset and the tool.
  `Control::ClearTranscript` clears them.
- A delegated child takes the setting and starts with no notes.
- `AgentReport::resets` and `DelegationRecord::resets` count the resets.
  Neither is written when zero.

History: no new `WorkflowEvent` variant, no migration and no patch gate. A
task with no `context_reset` field takes no new path, so a recorded run
replays as it ran. The reset reads only recorded data, so replay takes the
same reset.

Proof: `formal/verus/src/agent_reset.rs` proves `reaches_percent` and
`reset_due` in `bounds.rs`. A request over the cap always reaches the
threshold, and no run passes `max_resets`. `formal/verus/contracts.txt`
lists the new contracts.

Tests: `autumn-harvest-agent/tests/context_reset.rs` covers each acceptance
criterion, a crash right after a reset included. Unit tests and proptests
are in `bounds.rs` and `context_reset.rs`.

Known limit: a tool round that passes the cap in one step still ends as
`transcript_full`. The reset runs only before a request.
