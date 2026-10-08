# ADR 0006: The agent adapter targets autumn-plugin-agent

## Status

Accepted (issue #1973).

## Context

- Managed agent products share one pattern. The agent loop is a workflow.
  Model calls and tool calls are steps. Human approval is a durable wait.
- The engine has workflows, activities, and signals with deadlines. It had
  no adapter that wires an agent framework to them.
- ADR 0002 keeps execution Rust-native. A Python framework is out of scope.
- `examples/claude-agent-daemon` shows the pattern, but it is an example
  and speaks one provider.
- `cargo-deny` allows crates.io sources only. A dependency must be a
  published crate.

## Decision

The adapter targets `autumn-plugin-agent` 0.3, with no default features. It
lives in a new crate, `autumn-harvest-agent`.

| Engine primitive | plugin-agent primitive |
|---|---|
| Workflow `agent_loop` | The agent loop, its step and token budgets |
| Activity `agent_model_turn` | `LlmClient::chat`, then `ToolPolicy::decide` per call |
| Activity `agent_tool_call` | `Tool::execute` with a `ToolContext` |
| Signal with a deadline | `ToolDecision::RequireApproval`, answered by `Approval` |
| Retry policy | `ErrorKind`: `RateLimited`, `Transport` and `Unavailable` retry |

## Why this framework

- **It has the parts the engine needs.** A provider-neutral `LlmClient`
  (OpenAI-compatible and Anthropic), a `Tool` trait with a `ToolEffect`, a
  `ToolPolicy` with allow, ask and deny, and a serialisable `Approval`.
  Each one maps to one engine primitive.
- **Its persisted types are stable.** `ChatMessage`, `ContentPart`,
  `ToolCall`, `TokenUsage` and `ToolDecision` already have a serde contract,
  because its session stores persist them.
- **It needs no web stack.** Version 0.3 puts `autumn-web` behind a default
  `autumn` feature. With no default features, nothing pulls it in.
- **It is in the same family.** One team owns both crates, so a change that
  the adapter needs can land upstream.

## Rejected options

- **`rig-core`.** No approval primitive and no tool-effect model. Its loop
  does not split into steps that an activity can own.
- **`autumn-plugin-agent` 0.2.** It needs `autumn-web`.
- **A copy of the primitives in the engine.** Two copies drift.
- **The adapter in `autumn-harvest-plugin`.** That crate needs `autumn-web`.

## Consequences

- The policy runs inside the model-turn activity. Its decision is recorded
  with the reply, so replay never asks the policy again.
- The adapter owns its history types. An upstream type without a serde
  contract never reaches history.
- A tool result is cut to fit the activity-result cap (2 MiB by default,
  configurable). A transcript that cannot fit the next request ends the run
  as `transcript_full`.
- The daemon example keeps its Anthropic-native turn, because it replays
  thinking blocks verbatim. It takes the approval names, the durable
  approval wait and the payload checks from the adapter.
- Plugin-agent 0.3 adds `ErrorKind::Unavailable` for 408, 5xx and 529, so a
  provider outage retries instead of failing a paid run.
- The engine release that ships this crate needs plugin-agent 0.3.0 on
  crates.io first.
