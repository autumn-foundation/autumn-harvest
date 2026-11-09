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
  its draws. A parent and a beam still count its unit. It also keeps
  `serial: durable_peer`.
- A resolved reply promise with a value that is not a reply is now
  `reply_rejected`. Before, it failed the call with no record.
- New `DelegationRecord::idempotency_key`: the key `<from>:<seq>` of each
  sent message. A refusal before the send holds none. The model does not
  see it. The evaluation rebases it and the anonymised evaluation clears
  it, each with the run ids.
- New `Inbound::applied_key`. Read the state of the peer that `run_id`
  names, then check the key before a new ask. A `true` is sound.
- The model reads `{"error": ..., "verdict": ...}`, as for each other
  refusal. The error says that a new ask can run the task twice. A
  rejection reason is cut to 256 characters.
- The patch `agent_peer_reply_verdict` gates the new text, the record
  verdict and the decode rule. A sent message asks it before its wait, so a
  run that waits across the upgrade keeps the old shape.
- A report with a new verdict does not decode on an older worker. Upgrade
  every worker before a run can record one.
- The guide now lists `strategy_needs_durable_peer` in its verdict table.
  A unit test reads the table for each verdict, through an exhaustive match.
- No migration. No new `WorkflowEvent` variant.
- Tests: seven new tests in `tests/durable_peers.rs`. They cover the
  timeout, the rejection, a value that is not a reply, an interrupt of a
  durable sender with its replay, a receiver refusal, and the replay gate
  under the `eval` feature. Unit tests in `mesh.rs`, `workflow.rs`,
  `sampling/sample.rs`, `eval.rs`, `eval/anonymise.rs` and
  `agent/dedupe.rs`.
