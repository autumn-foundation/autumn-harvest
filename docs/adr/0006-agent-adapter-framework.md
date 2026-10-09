# ADR 0006: The agent adapter owns its primitives

## Status

Accepted (issue #1973).

## Context

- Managed agent products share one pattern. The agent loop is a workflow.
  Model calls and tool calls are steps. Human approval is a durable wait.
- The engine has workflows, activities, and signals with deadlines. It had
  no adapter that wires an agent loop to them.
- ADR 0002 keeps execution Rust-native. A Python framework is out of scope.
- `autumn-plugin-agent` has a good set of agent primitives: a
  provider-neutral model trait, tools with an effect level, a tool policy
  with allow, ask and deny, and a serialisable approval. It is an Autumn
  plugin.
- `examples/claude-agent-daemon` shows the pattern, but it is an example
  and speaks one provider.

## Decision

The adapter is a new crate, `autumn-harvest-agent`. It depends on the core
engine only. It takes no agent framework as a dependency.

It owns a small agent contract, modelled on the `autumn-plugin-agent`
primitives:

| `autumn-harvest-agent` | Guide in `autumn-plugin-agent` | Engine primitive |
|---|---|---|
| `AgentModel` (one `chat` method) | `LlmClient` | Activity `agent_model_turn` |
| `Tool`, `ToolEffect`, `FnTool` | `Tool`, `ToolEffect`, `FnTool` | Activity `agent_tool_call` |
| `ToolPolicy`, `ToolRules`, `ToolDecision` | the same names | Recorded with the model turn |
| `Approval` | `Approval` | Signal with a deadline |
| `ErrorKind::is_retryable` | `ErrorKind` | Retry policy |
| `agent_loop` | `Agent` loop and budgets | Workflow |
| `agent_heartbeat`, `Precheck` | `Heartbeat` | Workflow, started by a schedule |
| `Followups`, `schedule_followup` tool | `FollowupTool` | Durable timer |
| `Delivery` | `Delivery` | Activity `agent_deliver` |
| `MemoryStore`, `memory` tool | `MemoryStore`, `MemoryTool` | Activity `agent_memory_snapshot` |
| `LoopGuard` | `LoopGuard` | Workflow code over recorded results |

An app implements `AgentModel` for its provider, or bridges a framework it
already uses. The adapter ships no HTTP client.

## Why the adapter owns the contract

- **One Autumn plugin is enough.** The engine already has
  `autumn-harvest-plugin`. An engine crate that also needs a second plugin
  ties two plugin release trains together.
- **History needs a stable contract.** The message types are recorded in
  workflow history, and replay reads them back. The engine must own their
  serde shape.
- **The engine needs no web stack.** The contract has no `autumn-web` and no
  HTTP client.
- **A bridge is small.** One `AgentModel` impl adapts any client, including
  the `autumn-plugin-agent` `LlmClient`.

## Rejected options

- **Depend on `autumn-plugin-agent`.** Harvest would rely on two Autumn
  plugins, and that crate would need a breaking feature split to drop
  `autumn-web`. The engine would also not own its history types.
- **`rig-core`.** It has no approval primitive and no tool-effect model, and
  its loop does not split into steps that an activity can own.
- **The adapter in `autumn-harvest-plugin`.** That crate needs `autumn-web`.

## Consequences

- `scripts/check-agent-adapter-deps.sh` fails CI if the adapter depends on
  any Autumn crate outside the engine.
- The policy runs inside the model-turn activity. Its decision is recorded
  with the reply, so replay never asks the policy again.
- A tool result is cut to fit the activity-result cap (2 MiB by default,
  configurable). A transcript that cannot fit the next request ends the run
  as `transcript_full`.
- The daemon example keeps its Anthropic-native turn, because it replays
  thinking blocks verbatim. It takes the approval names, the durable
  approval wait and the payload checks from the adapter.
- The always-on primitives follow the same rule. Heartbeats, follow-ups,
  delivery, memory and the loop guard are in this crate, modelled on
  `autumn-plugin-agent`. A heartbeat is a one-tick workflow that an engine
  schedule starts. A follow-up is a durable timer in the same workflow.
  Delivery and the memory snapshot are activities. The loop guard runs in
  the workflow over recorded results.
- An unattended run, a heartbeat tick or a follow-up segment, is read-only
  by default. It can neither act outside nor write memory unless the app
  opts in.
