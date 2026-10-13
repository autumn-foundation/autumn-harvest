## Feature — Agent mesh primitives: delegation, mandates, budgets and flow rules

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
  `{root run id}:d{n}`. Only its final answer returns.
- **Mandate.** `AgentTask::mandate` bounds the tree: depth, delegations,
  peers, tools and read-only. A child mandate is the meet of the parent
  mandate and the peer role. Property tests prove that authority only
  narrows. A tool outside the mandate is hidden, denied and refused again at
  call time.
- **Refusals.** A refused delegation names its verdict: `not_permitted`,
  `unknown_peer`, `cycle`, `depth_exceeded`, `fan_out_exhausted` or
  `budget_exhausted`.
- **Budgets.** A child cap is the smaller of the peer cap and the parent
  remainder. The parent adds the child usage, and stops before another model
  call when the tree passes its cap. `AgentReport::delegations` lists each
  delegation with its subtree usage.
- **Flow rules.** `Tool::labels` and `FlowRules` gate the call that completes
  a forbidden label set in one run tree. `FlowRules::rule_of_two()` follows
  Meta's Agents Rule of Two. The workflow holds the exercised labels, so model
  output cannot remove one.
- **Wire form.** `MANDATE_META_KEY` and `Mandate::accept` carry a mandate to
  another process. The receiver can only narrow it.
- **Approval names.** One tool-round counter spans the tree, so a parent and
  a child never share a signal name. A run with no delegation keeps its old
  names.
- **Evaluation.** `eval::evaluate` mocks the peer lookup with the recorded
  answer. A recorded tool outcome answers only an equal call of the same
  agent.
- **Fix.** `remote_task::activities` set `input_schema` and `output_schema`
  twice, so the workspace did not compile. The duplicate pair is removed.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route. Each
new field of a recorded payload is left out at its default, so a run with no
mandate records the same bytes as before. `ModelTurnRequest`, `ModelTurn`,
`ToolCallRequest` and `AgentReport` gain fields, so a struct literal of one of
them needs the new fields.

**Tests.** `mesh` and `flow` unit and property tests.
`autumn-harvest-agent/tests/mesh.rs` runs each acceptance criterion on
SQLite: isolation, refusals, budgets, the tool allowlist, flow rules in both
directions, tree-unique approval names and a restart inside a child. Two
`eval` tests cover a delegation run and per-agent outcome matching.
