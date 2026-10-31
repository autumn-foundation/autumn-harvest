# Agent mesh primitives — research and gap analysis

**Status: research note.** This page surveys the primitives that the term
"agent mesh" names in 2026. It maps each primitive to what Harvest ships. It
picks the primitives that Harvest adopts now. The plan for that work is in
[`docs/design/DESIGN-agent-mesh.md`](../design/DESIGN-agent-mesh.md). The user guide is
[`docs/agent-mesh.md`](../agent-mesh.md).

## 1. The question

Which agent mesh primitives can Harvest adopt? Which of them does a durable
workflow engine make stronger than a gateway or a framework does?

**Short answer.** Section 5 lists eight primitives to adopt now. The core
ones are a peer roster, a delegation mandate, and budget roll-up. Cycle and
depth guards, flow rules and mandate propagation complete them. The engine
holds the delegation tree in one history. Replay never pays for a
finished step twice. The tree runs one step at a time, so siblings cannot
race on a shared budget.

## 2. What "agent mesh" means

The term has no single owner. The sources use it in four ways:

| Use | Sources | Core idea |
|-----|---------|-----------|
| A service mesh for agents | Solo.io agentgateway, Gravitee Agent Mesh | A data plane between agents, tools and models. It adds authentication, policy, rate limits and traces. |
| An event mesh for agents | Solace Agent Mesh, Confluent, Dapr Agents | Agents talk over a broker with A2A or pub/sub. An orchestrator splits a task. |
| An architecture | McKinsey "agentic AI mesh" | Composability, distributed intelligence, layered decoupling, vendor neutrality, governed autonomy. |
| Reliability primitives | arXiv 2608.26225 "Agent Mesh" | Seven primitives per delegation. Service-mesh retries assume idempotent calls. Agent delegations are not idempotent. |

The common ground is this: many agents delegate work to each other. The
infrastructure, not the model, must bound and govern that delegation.

## 3. The primitive catalog

### 3.1 Identity and discovery

- **Agent Card (A2A v1.0).** A JSON document at
  `/.well-known/agent-card.json`. Its required fields are `name`,
  `description`, `supportedInterfaces`, `version`, `capabilities`,
  `defaultInputModes`, `defaultOutputModes` and `skills`. A card can carry a
  JWS signature over its JCS form (RFC 8785).
- **Directories.** AGNTCY runs a federated, signed directory on OCI
  registries, with the OASF schema. The ANS drafts propose a DNS-like name
  service with a transparency log. Gravitee and agentgateway keep an agent
  catalog.
- **Roster in a framework.** Google ADK routes on the `description` of each
  sub-agent. The Claude Agent SDK defines subagents with a description, a
  prompt and a tool list.

### 3.2 Delegation and handoff

- **Agent as a tool.** The caller keeps control. The callee runs in a fresh
  context and returns only its final answer (ADK `AgentTool`, Claude Agent
  SDK subagents, OpenAI "agents as tools").
- **Handoff.** The callee takes over the rest of the conversation (OpenAI
  `transfer_to_<agent>`, LangGraph `Command(goto=...)`, Microsoft
  `HandoffBuilder`).
- **Orchestration patterns.** Sequential, concurrent, handoff, group chat and
  Magentic (Microsoft Agent Framework 1.0). Inngest AgentKit uses a router
  over shared state.
- **Handoff contracts.** A typed work packet: task id, goal, inputs,
  constraints, allowed tools, output schema, deadline, postcondition and
  memory rights (urandom.io, 2026). Agent Contracts (arXiv 2601.08815) adds
  resource limits, time bounds and terminal states.

### 3.3 Authority attenuation

- **Delegation chains.** The IETF drafts
  `draft-niyikiza-oauth-attenuating-agent-tokens` and
  `draft-asor-wimse-agent-delegation-chain` carry `del_depth`,
  `del_max_depth` and `par_hash`. Depth goes up by one per hop. The maximum
  depth only goes down. Expiry only goes down.
- **Meet, not union.** A child scope is the meet of the parent scope and the
  child role. Tools, resources and constraints can only narrow. An unknown
  constraint denies the call (fail closed).
- **Bounded Agents (arXiv 2608.15888).** A delegation budget has six
  ceilings: depth, blast radius, irreversible effects, sensitivity, cross-domain
  composition and cost. A child inherits the consumed budget of its parent as a
  floor. Theorem 4.6 proves that the reachable blast radius never grows along a
  chain.

### 3.4 Budget propagation

- **A2A budget extension proposal (a2aproject/A2A#2121).** The caller sends a
  remaining `cost`, `tokens` and `deadline`. The callee reports `incurred`
  cost, `aggregatedChildCost` and a delegation count.
- **Token Budgets (arXiv 2606.04056).** A catalog of 63 budget-overrun
  incidents. Retry loops lead (27 rows). Delegation fan-out races are 11
  rows: concurrent children each see the same remainder. The fix is a split
  budget that a child cannot copy.
- **Agent Contracts.** Child allocations must sum to no more than the parent
  allocation. Unspent budget returns to the pool.

### 3.5 Loop and cycle safety

- **Hop limits.** A time-to-live or a maximum hop count is the simplest guard
  (NeuralTrust). A caller chain lets an agent refuse a delegation that
  returns to itself.
- **Progress-based breaking.** Agents fail by ceasing to converge, not by
  erroring (arXiv 2608.26225, finding F1). Trip on no-progress fingerprints,
  not on error rate.
- **A2A has no hop control.** A2ABreak (arXiv 2609.10871) shows that identity
  is lost across hops. A callee sees only its direct caller.

### 3.6 Governance

- **Policy outside the model.** AgentCore Policy evaluates Cedar before each
  tool call. agentgateway applies RBAC. Progent gates tool calls with a JSON
  Schema policy language.
- **Composition closure.** Some actions are safe alone but dangerous together.
  Willison's "lethal trifecta" is private data, untrusted content and external
  communication. Meta's "Agents Rule of Two" says a session holds at most two
  of untrusted input, sensitive data, and state change or external send.
  Bounded Agents enforces prohibited pairs against a prior-action state that
  the infrastructure holds.
- **Distinct verdicts.** A duplicate, a stall, an exhausted budget and a gate
  refusal must stay distinct outcomes (arXiv 2608.26225).

### 3.7 Observability

- OpenTelemetry GenAI conventions name `invoke_agent` and `execute_tool`
  spans, with `gen_ai.agent.name` and `gen_ai.agent.id`. The conventions are
  still in Development status.

### 3.8 Shared state

- Blackboard systems (arXiv 2507.01701, 2510.01285) share one board. The Mesh
  Memory Protocol (arXiv 2604.19540) tracks lineage per claim to catch echoes.

## 4. What Harvest ships today

| Area | Shipped | Gap |
|------|---------|-----|
| One durable agent | `agent_loop`: model and tool calls as activities, approvals as signals | No second agent |
| Tool governance | `ToolPolicy`, `ToolRules`, `Strictest`, read-only runs | No rule over a sequence of calls |
| Loops | `LoopGuard` within one run | No guard across agents |
| Budgets | `max_steps`, `max_total_tokens` per run | No split or roll-up across agents |
| Cost | The agent cost ledger, per execution | No attribution per agent |
| Remote agents | `ctx.call_remote_task` (MCP and A2A client), MCP Tasks server | No A2A server, no Agent Card |
| Context to children | Headers, tenant and trace context | No depth, chain or budget |

Sources: `autumn-harvest-agent/src/`, `autumn-harvest/src/remote_task.rs`,
`autumn-harvest-plugin/src/mcp_tasks.rs`, `docs/remote-tasks.md`.

## 5. Verdicts

| Primitive | Verdict | Why |
|-----------|---------|-----|
| Peer roster with a durable lookup | **Adopt now** | The roster lives on the worker. The run records what it saw, so replay is stable. |
| Delegation as a workflow tool | **Adopt now** | Each child model call and tool call is an activity. After a crash, no finished call runs again. |
| Mandate with monotone narrowing | **Adopt now** | A pure function. Property tests can prove it. |
| Budget roll-up and split | **Adopt now** | The tree runs one step at a time, so the fan-out race cannot happen. |
| Cycle, depth and fan-out guards with distinct verdicts | **Adopt now** | Small, pure and directly testable. |
| Flow rules (Rule of Two) | **Adopt now** | History holds the prior-action state. The model cannot reset or forge it. |
| Mandate propagation between processes | **Adopt now (wire form)** | A meet with an inbound mandate can only narrow. |
| Agent name on model-turn traces | **Adopt now** | One span field. |
| A2A server and Agent Card | Later | Needs an HTTP route, auth layers and Postgres tests in the plugin. |
| Delegation to a remote peer | Later | Postgres only today. It needs the mandate in the remote call. |
| Handoff (transfer of control) | Later | Changes the active role mid-run. Delegation covers most uses first. |
| Concurrent children | Later | Needs a reserve-then-settle budget across parallel activities. |
| Effect ledger across retries | Later | Activity idempotency keys cover part of it today. |
| Signed cards, directories | Not for Harvest | Identity infrastructure belongs to the platform. |
| Blackboard, memory lineage | Not now | A shared `MemoryScope` and the keyed entity cover simple cases. |

## 6. Why a durable engine is a good host

1. **One history holds the tree.** Every child turn, tool call and refusal is
   an event in the parent run. An auditor reads one history.
2. **Replay sends no finished call again.** A crash in a child resumes at
   the child step that was in flight. Earlier model calls come back from
   history.
3. **No fan-out race.** The tree runs one step at a time inside one workflow.
   A child budget is a slice of the parent remainder. Two children cannot
   read the same remainder.
4. **Unforgeable prior-action state.** Flow rules read labels that the
   workflow records. Model output cannot remove them.

## 7. Sources

- arXiv 2608.26225, *Agent Mesh: Reliability Primitives for Non-Idempotent
  Agent Delegation*. <https://arxiv.org/abs/2608.26225>
- arXiv 2608.15888, *Bounded Agents: Delegation Security for Multi-Agent AI
  Systems*. <https://arxiv.org/abs/2608.15888>
- arXiv 2606.04056, *Token Budgets: An Empirical Catalog of 63 LLM-Agent
  Budget-Overrun Incidents*. <https://arxiv.org/abs/2606.04056>
- arXiv 2601.08815, *Agent Contracts*. <https://arxiv.org/abs/2601.08815>
- arXiv 2609.10871, *A2ABreak*. <https://arxiv.org/abs/2609.10871>
- arXiv 2604.19540, *Mesh Memory Protocol*. <https://arxiv.org/abs/2604.19540>
- arXiv 2503.13657, *MAST: Why Do Multi-Agent LLM Systems Fail?*
  <https://arxiv.org/abs/2503.13657>
- arXiv 2503.18813, *CaMeL*. <https://arxiv.org/abs/2503.18813>
- arXiv 2504.11703, *Progent*. <https://arxiv.org/abs/2504.11703>
- A2A specification v1.0. <https://a2a-protocol.org/latest/specification/>
- A2A budget extension proposal. <https://github.com/a2aproject/A2A/issues/2121>
- IETF draft, attenuating agent tokens.
  <https://www.ietf.org/archive/id/draft-niyikiza-oauth-attenuating-agent-tokens-01.html>
- IETF draft, agent delegation chain.
  <https://www.ietf.org/archive/id/draft-asor-wimse-agent-delegation-chain-01.html>
- WorkOS, the multi-hop delegation problem.
  <https://workos.com/blog/oauth-multi-hop-delegation-ai-agents>
- Meta, practical AI agent security (Agents Rule of Two).
  <https://ai.meta.com/blog/practical-ai-agent-security/>
- AGNTCY documentation. <https://docs.agntcy.org>
- agentgateway FAQ. <https://agentgateway.dev/docs/standalone/main/faqs/>
- Gravitee Agent Mesh. <https://documentation.gravitee.io/apim/agent-mesh>
- Solace Agent Mesh. <https://docs.solace.com/Agent-Mesh/agent-mesh.htm>
- Temporal Nexus. <https://docs.temporal.io/nexus>
- Microsoft Agent Framework orchestrations.
  <https://learn.microsoft.com/agent-framework/workflows/orchestrations>
- Claude Agent SDK subagents.
  <https://code.claude.com/docs/en/agent-sdk/subagents>
- Google ADK multi-agent systems. <https://adk.dev/agents/multi-agents>
- Inngest AgentKit networks. <https://agentkit.inngest.com/concepts/networks>
- NeuralTrust, loops in multi-agent systems. <https://neuraltrust.ai/blog/a2a-loop>
- Handoff contracts.
  <https://urandom.io/blog/2026-04-09-multi-agent-systems-need-handoff-contracts>
- OpenTelemetry GenAI conventions.
  <https://github.com/open-telemetry/semantic-conventions-genai>
