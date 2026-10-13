## Feature — Agent mesh primitives: delegation, mandates, budgets and flow rules (PR #2148)

**What shipped.** `autumn-harvest-agent` gains agent mesh primitives. A run
can delegate a task to a peer agent. The peer runs as a sub-agent inside the
same workflow, on Postgres and on SQLite. `docs/agent-mesh.md` shows them end
to end. `docs/rnd/2026-10-13-agent-mesh-primitives.md` holds the research,
and `DESIGN-agent-mesh.md` the plan.

**Design.**

- **Roster and discovery.** `AgentHarness::peer` adds a `Peer`: a name, a
  description, a prompt, tools and bounds. The activity `agent_discover_peers`
  reads the peers that a mandate names, once per agent run. Replay reads the
  record, so a roster change cannot change a recorded run.
- **Delegation.** The workflow tool `delegate` runs a peer inline. The child
  gets its own prompt, a fresh transcript and the run id
  `{root run id}:d{n}`. Only its final answer returns. A run with a mandate
  reserves the name `delegate`, even when the lookup finds no peer.
- **Mandate.** `AgentTask::mandate` bounds the tree: depth, delegations,
  peers, tools and read-only. A child mandate is the meet of the parent
  mandate and the peer role. Property tests prove that authority only
  narrows. A tool outside the mandate is hidden, denied and refused again at
  call time. An allowlist must name `memory` to keep the memory tool.
- **Refusals.** A refused delegation names its verdict: `invalid_arguments`,
  `not_permitted`, `unknown_peer`, `cycle`, `depth_exceeded`,
  `fan_out_exhausted` or `budget_exhausted`. `Mandate::accept` can also
  return `invalid_mandate`. `Refusal` implements `std::error::Error`.
- **Budgets.** A child cap is the smaller of the peer cap and the parent
  remainder. A cap of zero is refused. The parent adds the child usage, and
  stops before another model call when the tree passes its cap.
  `AgentReport::delegations` lists each delegation with its subtree usage,
  rounds and calls. The report counters cover the whole tree.
- **Flow rules.** `Tool::labels` and `FlowRules` gate the call that completes
  a forbidden label set in one run tree. Each later call that touches the set
  needs its own approval. `FlowRules::rule_of_two()` follows Meta's Agents
  Rule of Two. The workflow holds the exercised labels, so model
  output cannot remove one. A call after a delegation in the same turn does
  not run on a stale decision. With flow rules, an unknown tool is denied.
  The tool-call activity refuses a tool whose labels grew after the
  decision.
- **Label carry.** `AgentReport::flow_labels` returns the labels of a run.
  `AgentTask::flow_labels` and `HeartbeatTask::flow_labels` seed the next
  run of the same conversation.
- **Wire form.** `MANDATE_META_KEY`, `Mandate::outbound` and
  `Mandate::accept` carry a mandate to another process. The receiver can only
  narrow it. `Mandate::from_meta` refuses an unknown key.
- **Approval names.** One tool-round counter spans the tree, so a parent and
  a child never share a signal name. A run with no delegation keeps its old
  names.
- **Evaluation.** `eval::evaluate` mocks the peer lookup with the recorded
  answer, or with the recorded error of a lookup that failed the source. A recorded tool outcome answers only an equal call of the same
  agent. A candidate call takes a recorded call id only from a turn of the
  same agent.
- **Response cache.** A child of a run whose run id is the workflow id skips
  the cache, as its parent does.
- **Fix.** `remote_task::activities` set `input_schema` and `output_schema`
  twice, so the workspace did not compile. This PR removes the duplicate
  pair.
- **Fix.** The `verus` CI job pinned a rolling Verus release whose assets
  Verus deleted, so the download failed with 404. The script now pins the
  weekly release `0.2026.10.04.426d8b0`.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route. Each
new field of a recorded payload is left out at its default, so a run with no
mandate records the same bytes as before. `AgentTask`, `ModelTurnRequest`,
`ModelTurn`, `ToolCallRequest`, `AgentReport` and `HeartbeatTask` gain
fields. A struct literal of one of them needs the new fields.
`DelegationRecord` is `#[non_exhaustive]`.

**Tests.** `mesh` and `flow` unit and property tests cover the mandate
rules, the wire form and the flow check.
`autumn-harvest-agent/tests/mesh.rs` runs each acceptance criterion on
SQLite. It covers isolation, refusals, budgets, the tool allowlist, flow
rules in both directions and across runs, and the same-turn delegation case.
It also covers tree-unique approval names, a restart inside a child and the
model-turn span. Five `eval` tests cover delegation runs, per-agent
outcome matching, per-agent call ids and a failed lookup.
