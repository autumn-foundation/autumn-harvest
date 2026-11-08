## Delegated strategies, the strategies example, guide section and ADR (issues #2610 and #2450, PR #2617)

These are the last two parts of epic #2438. The designs are in
`docs/design/DESIGN-2610.md` and `docs/design/DESIGN-2450.md`.

- **Delegated strategies (#2610).** The `delegate` tool takes an optional
  `sample`, an `OpSample`. A durable peer runs the message as a strategy
  op and replies with the selected answer.
  - The cap of a call is the smaller of the caller `max_draws` and the
    `Peer::max_draws` of the peer role. With neither, the call admits one
    draw. The tool shows `sample` only when a durable peer has a cap of 2
    or more.
  - After the usual delegation checks, the caller checks the draws before
    the send: `draws_exceeded`, then `fan_out_exhausted`. The delegation
    takes one unit of `max_delegations`, and each draw one more once the
    message is out. A beam checks the cap only. `mesh::delegated_verdict` holds the rule, and Verus proves it in
    `formal/verus/src/agent_mandate_draws.rs`.
  - An inline peer refuses a strategy with the new refusal
    `strategy_needs_durable_peer`. A call with `sample` never joins a
    batch.
  - `DelegationRecord::draws` keeps the draw units. A fan-out parent and
    a beam step count them.
  - A receiver decodes `PeerEnvelope::mandate` strictly with
    `Mandate::from_wire`. An unknown key fails the op.
  - A patch marker gates each call that holds a `sample` key, so a
    history from before it replays as it ran.
- **Example (#2450).** `examples/agent-strategies` runs one task as a
  single draw, best-of-3, a vote and a two-tier cascade, on SQLite with a
  scripted model and no key. It prints the selected answer, each draw and
  the cost. CI runs it and checks each strategy name.
- **Guide.** `docs/agent-adapter.md` gets section 28, "Inference
  strategies", with one table: each strategy, its draws, when it pays off
  and its policy setting. Section 15 and `docs/agent-mesh.md` section 13
  describe the `sample` argument.
- **ADR.** `docs/adr/0017-inference-strategies.md` records four
  decisions: strategies are workflows over child runs, scores come from
  verifiers, a draw that the run does not select only reads, and what is
  not adopted. Both indexes list it.

The change adds no engine change, no `WorkflowEvent` variant and no
migration. A new `Refusal` variant does not decode on an older worker.
Upgrade every worker of a mesh before a model can call `delegate` with
`sample`.

Test evidence: `autumn-harvest-agent/tests/delegated_strategies.rs` covers
each acceptance line of #2610 on SQLite. Unit and property tests in
`mesh.rs`, `workflow.rs` and `agent/mod.rs` cover the kernel, the tool
schema, the nested count and the strict decode.
`examples/agent-strategies/tests/strategies.rs` checks each selected
answer and the best-of-3 cost. `autumn-harvest-agent/tests/strategies_docs.rs`
guards the guide section, the ADR, both indexes, this entry and the CI
steps.
