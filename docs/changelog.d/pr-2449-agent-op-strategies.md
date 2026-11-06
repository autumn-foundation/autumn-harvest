## Strategies in a durable agent and a mesh (issue #2449)

A durable agent op can now run a strategy, and a mandate bounds the draws
of a whole subtree. The design is in `docs/design/DESIGN-2449.md`.

- `sampling::OpSample` holds a `SampleStrategy`, a `ScorerRef` and an
  optional concurrency. `UserMessage::sample` and `PeerEnvelope::sample`
  carry it. `AgentOp::with_sample` sets it. A missing field decodes as
  `None`, so each recorded op decodes as before.
- The `agent` workflow runs such an op through the core of
  `agent_sample`, inline. Each draw gets the transcript as history, the
  labels of the agent and the mandate of the op. The policy of the kind
  still applies. The selected draw gives the answer and the transcript.
  The tokens and the labels of each draw go into the state.
- `Mandate::max_draws: Option<u32>` caps the draws of one strategy run.
  `Peer::max_draws` narrows a child. `delegate`, `outbound` and `accept`
  take the smaller cap with `mesh::meet_cap`. `from_meta` reads the new
  key. A cap of `None` sends no key, so an old receiver still reads it.
- Each draw counts as one delegation. `Mandate::admit_draws` refuses with
  the new `Refusal::DrawsExceeded`, then `Refusal::FanOutExhausted`. The
  draws split the rest of the fan-out with `sibling_cap`.
- `sampling::Refusal::Mandate` carries a mandate refusal of a sample run.
  A peer gets it as `PeerReply::refusal`.
- `agent_sample` checks the profile mandate too, behind the patch
  `agent_sample_mandate`. A run that started before it skips the check on
  replay.
- `SampleReport::flow_labels` holds the labels of each draw, selected or
  not. A score input drops the history of its task.
- Each draw carries the mesh of the profile, so a mesh budget stops the
  next draw. A strategy op does not park the agent.
- `formal/verus/src/agent_mandate_draws.rs` proves `meet_cap` and
  `draw_verdict`: a meet never widens a cap, and admitted draws fit the cap
  and the room of the tree.

Tests: `tests/agent_strategies.rs` (best-of-3 op, policy refusal, mandate
and fan-out refusals, peer refusal and reply, mesh budget with the mesh
ledger), property tests `delegate_never_widens_max_draws`,
`accept_never_widens_max_draws` and
`admitted_draws_fit_the_cap_and_the_fan_out`, and wire tests in `mesh.rs`
and `agent/mod.rs`.
