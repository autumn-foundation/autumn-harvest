## A notice when a tool call parks on approval (issue #2603)

A gated tool call now sends one approval notice before it waits. A chat app
pushes "Approve this?" at once, and needs no poll of `agent_status`. The
design is in `docs/design/DESIGN-2603.md`. The guide is
`docs/agent-adapter.md` sections 5 and 15.

- New default method `Delivery::notify_approval`. The default writes the
  names to the `tracing` log. It never logs the arguments or the reason. An
  existing `Delivery` keeps compiling.
- New activity `agent_approval_notice`, in `workflow::activities()`, in the
  SQLite registration and in the evaluation mocks. Its result is recorded,
  so a restart during the wait sends no second notice.
- New types `delivery::ParkedCall` (the activity input) and
  `delivery::ApprovalNotice` (what the app gets). The notice holds the run
  id, the execution id, the agent id, the session id, the signal, the tool,
  the policy reason (cut to 256 bytes), the arguments cut to the tool
  result cap, the timeout and the deadline. `ApprovalNotice::key` is
  `<run id>:<execution id>:<signal>`. A retry keeps it. A reset or a fork
  gets a new key.
- A failed notice is recorded and ignored. It never fails the run and never
  releases the call.
- A probe sends no notice. An evaluation stubs it.
- The patch `agent_approval_notice` gates the notice. A run that parked
  before the upgrade replays with no notice. The fixture
  `tests/fixtures/pre_notice_history.json` proves it.
- **Engine fix.** A `ctx.race()` that a signal settles on its first poll
  arms no other branch. Its replay diverged at the winner marker, so a
  durable agent whose decision came before its wait opened failed its next
  replay. A timer, activity or child branch that meets the winner marker of
  its own race now reads as not armed. Suite `race_buffered_signal` in
  `autumn-harvest-sqlite`.
- No migration. No new `WorkflowEvent` variant.
- Tests: `tests/approval_notices.rs` (SQLite),
  `a_gated_call_on_a_durable_agent_sends_one_notice_that_decide_accepts`
  and `a_decision_sent_from_the_notice_releases_the_call_and_replays` in
  `tests/postgres_agents.rs`, `no_approval_notice_is_sent_during_evaluation`
  in `tests/eval.rs` and `a_probe_run_sends_no_approval_notice` in
  `tests/live_probe.rs`.
