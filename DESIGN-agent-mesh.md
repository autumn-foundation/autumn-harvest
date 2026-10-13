# Design — agent mesh primitives for the durable agent loop

The research is in `docs/rnd/2026-10-13-agent-mesh-primitives.md`. It finds
that Harvest runs one durable agent well, but it has no second agent. It has
no delegation, no hop or cycle control, no budget split, and no rule over a
sequence of tool calls. This change adds those primitives to
`autumn-harvest-agent`.

**No migration. No new `WorkflowEvent` variant. No core engine change except
the AC16 compile fix.** The change adds one activity, `agent_discover_peers`,
and new optional fields on recorded payloads. Each new field is left out at
its default, so a run with no mandate records the same bytes as before.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The agent loop runs tool calls one at a time (`run_round`). A tool runs in
  `agent_tool_call`, with one attempt and a 10-minute limit. A tool cannot
  durably wait on another agent.
- A workflow-handled tool already exists: `schedule_followup`. The model
  turn offers it through `extra_tools`. The harness wraps it in
  `WorkflowTool`, with the `Internal` effect.
- The SQLite backend has no child workflows and no external activities. Only
  inline work runs on both backends.
- Approval signal names are `tool_approval:{step}:{position}:{call_id}`. The
  step is `step_base + step`, the round count of the run so far.
- `ToolContext` uses `(run_id, step, call_id)` as the idempotency key.
- A new field on a recorded request must be left out at its default. The test
  `requests_from_before_the_always_on_fields_decode_and_encode_alike` pins
  this for strict replay.
- `eval::evaluate` mocks each agent activity by name. An unmocked activity
  fails the evaluation.
- `SqliteRuntime::load_history` returns the events of a run, so a test can
  count recorded activities.
- The base branch does not compile: `remote_task::activities` sets
  `input_schema` and `output_schema` twice. That is a merge artifact.

### 0.2 Brainstorm — how can Harvest adopt mesh primitives?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A sub-agent as a child workflow per delegation. | Rejected for now. SQLite has no children. A child needs its own budget hand-off and result path. |
| B2 | A sub-agent as an inline loop in the parent workflow, behind a workflow-handled `delegate` tool. | **Adopted.** It runs on both backends. Each child step is an activity of the parent history. |
| B3 | A sub-agent as a tool inside `agent_tool_call`. | Rejected. The activity has one attempt and 10 minutes. After a crash, the whole child runs again. |
| B4 | Put the peer roster in `AgentTask`. | Rejected. Each run would copy every prompt into its input. The roster is operator state. |
| B5 | Keep the roster on the harness. Read it through one recorded activity per agent run. | **Adopted.** Replay reads the record, so a roster change cannot change a recorded run. |
| B6 | A `Mandate` that a child gets as the meet of its parent mandate and its peer role. | **Adopted.** It follows the IETF delegation-chain drafts and Bounded Agents. |
| B7 | A token budget per child, cut from the parent remainder. | **Adopted.** The tree runs one step at a time, so no sibling reads the same remainder. |
| B8 | Refuse a delegation with one generic error. | Rejected. The Agent Mesh paper asks for distinct verdicts. |
| B9 | Refuse with a named verdict: `unknown_peer`, `not_permitted`, `cycle`, `depth_exceeded`, `fan_out_exhausted`, `budget_exhausted`. | **Adopted.** The model reads the verdict, and the report lists it. |
| B10 | A flow rule in the workflow, from tool names. | Rejected. The workflow cannot see tool labels. |
| B11 | Flow rules in the model-turn activity. The workflow sends the exercised labels. The turn records the labels of each call. | **Adopted.** The decision stays in the recorded turn. The workflow holds the label set. |
| B12 | One label set per run tree, shared up and down. | **Adopted.** A child answer enters the parent context, and a parent task text enters the child. Both carry taint. |
| B13 | Sign the mandate. | Rejected. Key management belongs to the platform. An inbound mandate can only narrow, so a forged one cannot widen a run. |
| B14 | A wire key for the mandate, and a meet with an inbound mandate. | **Adopted.** It lets two processes keep one chain and one depth bound. |
| B15 | Per-peer models. | Deferred. One harness has one model client today. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A run with no mandate records different bytes, so strict replay of an old run fails. | Every new field is left out at its default. A test encodes a request with no mandate and checks the keys. |
| R2 | Child approval names collide with parent names in one execution. | One counter numbers every round of the tree. For a run with no delegation it equals `step_base + step`. A test pins both. |
| R3 | Child tool calls reuse the parent idempotency key. | A child run id is `{root run id}:d{n}`. The ordinal `n` counts delegations in the tree. |
| R4 | A to B to A loops forever. | The chain refuses a peer already in it (`cycle`). |
| R5 | A deep or wide tree burns the budget. | `max_depth` (default 3) and `max_delegations` (default 16) bound the tree. Both only shrink down the tree. |
| R6 | A child spends past the parent cap. | The child cap is the smaller of the peer cap and the parent remainder. The parent checks the cap before its next model call. |
| R7 | A child widens its own authority. | A pure `Mandate::delegate` computes the meet. Property tests prove each set only narrows. |
| R8 | The model names a tool outside the mandate. | The turn hides the tool, and the decision denies the call. The tool-call activity checks again. |
| R9 | A roster change mid-run changes replay. | The lookup is an activity. Replay reads its recorded result. |
| R10 | A child reads untrusted content, then the parent sends it out. | One label set spans the tree. The parent send completes the forbidden set and needs approval. |
| R11 | A refused gated call still adds its labels. | A label counts only when its call runs. Inside one turn, the decision step counts every call that is not denied. That fails closed. |
| R12 | An inbound mandate from another process widens the local run. | `Mandate::accept` returns the meet. A test proves the result is never wider than either side. |
| R13 | A peer name injects text into the tool description or the run id. | A name has 1 to 64 characters from `a-z`, `0-9`, `_` and `-`. Discovery drops a peer with any other name. |
| R14 | The child answer floods the parent transcript. | The result keeps 8 000 characters. The request cap still applies. |
| R15 | An evaluation of a delegation run fails on the new activity. | `eval` mocks `agent_discover_peers` with the recorded result. |
| R16 | Flow labels change the turn shape for every user. | The turn records labels only when the harness has flow rules. |
| R17 | A cancel during a child leaves the parent waiting. | The child runs in the parent workflow. A cancel ends both at once. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The loop is sequential. Workflow tools exist. SQLite has no children. The A2A drafts carry depth, max depth and a parent hash. Budget incidents cluster in retry loops and fan-out races. |
| Red | Delegation as a tool feels natural to agent authors. Inline children feel lighter than a child workflow per call. |
| Black | One history grows with each child turn. An inline child cannot run on another worker. Flow labels are only as good as the tool author's labels. Per-peer models are missing. |
| Yellow | No migration and no core change. Both backends work. The tree is auditable in one history. The fan-out race cannot happen. |
| Green | Later: remote peers through `call_remote_task` with the mandate in `_meta`. Concurrent children with reserve-then-settle. A handoff tool. An A2A server and Agent Card. Per-peer models. |
| Blue | TDD: pure mandate and flow tests, then SQLite loop tests, then crash and replay tests. Then docs, the changelog fragment and a review. |

### 0.5 Decisions

1. Module `autumn_harvest_agent::mesh` holds `Peer`, `Mandate`, `Refusal`,
   `DelegationRecord`, `DELEGATE_TOOL` and `MANDATE_META_KEY`.
   Module `autumn_harvest_agent::flow` holds `FlowRules`, `FlowRule`, the
   `labels` constants and `effect_label`.
2. `AgentHarness::peer(Peer)` adds a roster entry.
   `AgentHarness::flow_rules(FlowRules)` installs flow rules.
3. `AgentTask::mandate(Mandate)` turns delegation on. A task with no mandate
   runs as before.
4. At the start of each agent run with peers, the activity
   `agent_discover_peers` returns the known, valid peers that the mandate
   names. The workflow builds the `delegate` tool from them.
5. A `delegate` call runs the peer inline:

   | Child setting | Value |
   |---|---|
   | System prompt | The peer `system` |
   | Input | The `task` argument |
   | Transcript | Fresh |
   | Run id | `{root run id}:d{n}` |
   | Steps | The peer `max_steps` |
   | Token cap | The smaller of the peer cap and the parent remainder |
   | Read-only | Parent, peer or mandate |
   | Memory, follow-ups, delivery | Off |

6. The delegate result is JSON: `agent`, `stop` and `answer`. It is an error
   result when the child stop is not `completed`. A refusal is an error
   result with `error` and `verdict`.
7. The parent adds the child usage. `AgentReport::delegations` lists each
   delegation of the tree, refused ones too.
8. Flow rules check each call against the labels that the tree has
   exercised. `FlowRules::rule_of_two()` asks for approval before a call
   completes untrusted input, private data, and a write or external effect.

### 0.6 Out of scope

- A remote peer over MCP or A2A. The wire form of the mandate is ready for it.
- Concurrent children.
- A handoff that changes the active agent for the rest of the run.
- An A2A server and an Agent Card route.
- A model per peer.

---

## 1. Acceptance criteria

| AC | Criterion |
|----|-----------|
| AC1 | A harness holds a peer roster. A run with a mandate reads only the peers that its mandate names, through one recorded activity per agent run. A restart reads the record and does not look up again. |
| AC2 | The `delegate` tool appears only for a run with a mandate and at least one known peer. A run with a mandate reserves the name, even with no known peer. A run with no mandate records the same request bytes as before. |
| AC3 | A `delegate` call runs the peer inline with its own prompt, a fresh transcript and its own step bound. Only its final answer returns to the parent. |
| AC4 | A crash inside a child resumes without a second call for any finished child model turn. |
| AC5 | A child mandate is the meet of the parent mandate and the peer role. Property tests prove that each bound only narrows. |
| AC6 | A child cannot run a tool outside its mandate. The turn hides it, the decision denies it, and the tool-call activity refuses it. |
| AC7 | A refused delegation returns a distinct verdict: `unknown_peer`, `not_permitted`, `cycle`, `depth_exceeded`, `fan_out_exhausted` or `budget_exhausted`. |
| AC8 | The child token cap is the smaller of the peer cap and the parent remainder. The parent adds the child usage and stops before another model call when the tree passes its cap. |
| AC9 | `AgentReport::delegations` lists each delegation with its chain, run id, stop or verdict, and subtree usage. |
| AC10 | With `FlowRules::rule_of_two()`, a call that completes the forbidden label set needs approval. Each later call that touches the set needs its own approval. Labels that a child exercises count for its parent, and the reverse. |
| AC11 | A mandate encodes under `io.autumn-harvest/mandate`. `Mandate::accept` narrows a local mandate by an inbound one. It refuses a cycle and a depth overflow. |
| AC12 | A gated call inside a child waits on a signal name that is unique in the run tree. A run with no delegation keeps its old names. |
| AC13 | Old histories decode. All existing tests pass. `eval` re-drives a delegation run. |
| AC14 | Each model turn records the acting agent name. The model-turn span carries `gen_ai.agent.name`. |
| AC15 | The user guide, the research note, this plan and a changelog fragment exist. Comments and docs use ASD-STE100 style. The comment-hygiene audit passes. |
| AC16 | The workspace compiles. This PR removes the duplicate `ActivityInfo` fields in `remote_task.rs`. |
| AC17 | Flow labels carry across runs. `AgentReport::flow_labels` returns them, and `AgentTask::flow_labels`, `HeartbeatTask::flow_labels` and the wire mandate seed them. |
| AC18 | A call after a delegation in the same turn never runs on a decision that predates the child labels. |
| AC19 | The wire form is strict and bounded. `from_meta` refuses an unknown key. `validate` bounds names and set sizes. `accept` needs a root. `outbound` sends the remainder of the fan-out. |

## 2. Tests (red first)

| Test | AC | Kind |
|------|----|------|
| `mesh` unit and property tests: meet, chain, depth, fan-out, verdict codes, names, wire round trip, `accept` | 5, 7, 11 | No DB |
| `flow` unit tests: subset rule, preset, effect labels | 10 | No DB |
| `types` tests: no-mandate request shape, old report decodes | 2, 13 | No DB |
| `tests/mesh.rs` on SQLite: discovery, delegation, isolation, refusals, budget, report, allowlist, flow rules, child approval, crash resume | 1–4, 6–10, 12, 14 | SQLite file |
| `tests/eval.rs`: a delegation run evaluates | 13 | In-memory engine |
| `engine_activities`: the registration includes the new activity | 1 | No DB |

## 3. Review findings and fixes

Four review agents read the change: correctness and replay, security,
API and docs, and test quality. Each row names a finding and its fix.

| # | Finding | Fix | AC |
|---|---------|-----|----|
| F1 | Labels reset on each run. A later run that continues the transcript can send what an earlier run read. | `AgentTask`, `AgentReport`, `HeartbeatTask` and `Mandate` carry `flow_labels`. | AC17 |
| F2 | A turn decides a call after `delegate` before the child adds its labels. | `run_round` does not run a later call when the delegation added labels. The model reads "call it again". | AC18 |
| F3 | An unknown tool gets no labels, so a worker with another tool list can run it unlabelled. | With flow rules, the decision denies an unknown tool. | AC10 |
| F4 | A wire mandate keeps the full fan-out and drops labels. | `Mandate::outbound` sends the remainder and the labels. `accept` unions the labels. | AC19 |
| F5 | Inbound tool names and sets are unbounded. An unknown key is dropped. | `validate` bounds them. `from_meta` refuses an unknown key. | AC19 |
| F6 | A tool allowlist cannot remove the `memory` tool. | An allowlist must name `memory`. | AC6 |
| F7 | A malformed `delegate` call leaves no record. A refused record keeps the raw model text. | `invalid_arguments` verdict. A name that is not valid shows as `_invalid`. | AC7 |
| F8 | A peer cap of zero still allows one model call. | `budget_exhausted`. | AC8 |
| F9 | `accept` drops the chain prefix of a local mandate that is not a root. | `accept` needs a root. | AC19 |
| F10 | A child of a workflow-id run uses the response cache. | `shares_run_id` covers `{root}:d{n}`. | AC13 |
| F11 | Docs: a non-compiling example, stale adapter docs, loose claims, long sentences. | Corrected. `Refusal` implements `Error`. | AC15 |
| F12 | Codex: the tool-call worker does not check the labels that the decision saw. A deploy can change a tool between the two. | `ToolCallRequest::labels` carries the recorded labels. The activity refuses a tool whose labels grew. | AC10 |
| F13 | CI: the `verus` job fails on `trunk-dev` too. The pinned rolling Verus release has no assets now. | Pin the weekly release `0.2026.10.04.426d8b0`. All 33 proofs verify with it. | AC16 |
