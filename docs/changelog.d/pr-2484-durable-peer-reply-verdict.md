## A verdict and a key for an unanswered durable peer (issue #2484)

A `delegate` call to a durable peer that gets no reply now has a named
verdict, and each sent message has its key on the record. The design is in
`docs/design/DESIGN-2484.md`. The guide is `docs/agent-mesh.md` sections 5
and 13.

- New `Refusal` variants `ReplyTimeout`, `ReplyRejected` and
  `ReplyInterrupted`, with the codes `reply_timeout`, `reply_rejected` and
  `reply_interrupted`. New `Refusal::unanswered` is `true` for these three.
  The message went out, so the peer can still apply it.
- The record of an unanswered delegation keeps the `run_id` of the peer and
  its draws. A parent still counts its unit. It also keeps
  `serial: durable_peer`.
- New `DelegationRecord::idempotency_key`: the key `<from>:<seq>` of each
  sent message. A refusal before the send holds none. The model does not
  see it. The anonymised evaluation clears it with the run ids, and the
  evaluation rebases it with the run ids.
- New `Inbound::applied_key`. Read the receiver state, then check the key
  before a new ask. A `true` is sound.
- The model reads `{"error": ..., "verdict": ...}`, as for each other
  refusal. The error says that a new ask can run the task twice. A
  rejection reason is cut to 256 characters.
- The patch `agent_peer_reply_verdict` gates the new text. A sent message
  asks it before its wait, so a run that waits across the upgrade keeps the
  old text. Strict replay does not accept a marker right after a fired wait
  deadline.
- A report with a new verdict does not decode on an older worker. Roll out
  to every worker first.
- The guide now lists `strategy_needs_durable_peer` in its verdict table.
  A unit test reads the table for each verdict, through an exhaustive match.
- No migration. No new `WorkflowEvent` variant.
- Tests: four new tests in `tests/durable_peers.rs`, including the replay
  gate test under the `eval` feature. Unit tests in `mesh.rs`,
  `workflow.rs`, `eval.rs`, `eval/anonymise.rs` and `agent/dedupe.rs`.
