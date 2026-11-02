# autumn-harvest-agent

A durable agent loop for [autumn-harvest](../README.md) (issue #1973).

It owns a small agent contract (`AgentModel`, `Tool`, `ToolPolicy`,
`Approval`). It follows the shape of the `autumn-plugin-agent` primitives,
but it does not depend on that crate. On the engine:

- The agent loop is the workflow `agent_loop`.
- Each model call is the activity `agent_model_turn`. It also records the
  tool-policy decision for each call.
- Each tool call is the activity `agent_tool_call`.
- A call that the policy gates waits on a durable signal with a deadline.

It also has the always-on primitives: heartbeats, follow-ups on a durable
timer, delivery, a frozen memory snapshot, and a loop guard. An opt-in
response cache serves an identical model call across runs, per tenant. An
opt-in context reset (issue #2453) lets a long run save progress notes and
restart from them near the request cap.

A durable `Agent` (issue #2263) is a named, long-lived actor. It is the agent
loop inside a keyed entity: the workflow `agent`, with the `AgentId`
`<kind>/<name>` as its workflow id. It handles one message at a time from a
durable inbox, and it survives a crash. A message can queue, steer the running
segment, or interrupt it (issue #2265). See section 15 of
[`docs/agent-adapter.md`](../docs/agent-adapter.md).

A run can delegate a task to a peer agent. An inline peer runs as a sub-agent
in the same workflow. A durable peer is an `Agent` with its own history, inbox
and budget (issue #2269). A mandate bounds the delegation tree, and flow rules
gate a dangerous sequence of tool calls. See
[`docs/agent-mesh.md`](../docs/agent-mesh.md).

A crash costs at most the one step that was in flight. The crate depends on
the core engine only. It has no Autumn plugin and no `autumn-web` dependency.
An app implements `AgentModel` for its provider.

```sh
cargo run -p autumn-harvest-agent --features sqlite --example sqlite_agent
cargo run -p autumn-harvest-agent --features sqlite --example agent_session_entity
```

The first example runs the agent loop once. The second runs a chat session
as a durable `Agent`.

| Feature | Contents |
|---|---|
| `sqlite` | `sqlite::register`, `start`, `start_heartbeat` and `decide` for the embedded backend |
| `eval` | `eval::evaluate`: re-drive a recorded run with a candidate model or prompt, and diff the decisions (issue #2001) |
| `sandbox` | A tool that runs agent code in a sandbox. See [`docs/sandbox-activities.md`](../docs/sandbox-activities.md) |

Read [`docs/agent-adapter.md`](../docs/agent-adapter.md) for the whole
pattern, and [ADR 0006](../docs/adr/0006-agent-adapter-framework.md) for the
choice of framework.
