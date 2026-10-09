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
timer, delivery, a frozen memory snapshot, and a loop guard.

A crash costs at most the one step that was in flight. The crate depends on
the core engine only. It has no Autumn plugin and no `autumn-web` dependency.
An app implements `AgentModel` for its provider.

```sh
cargo run -p autumn-harvest-agent --features sqlite --example sqlite_agent
```

| Feature | Contents |
|---|---|
| `sqlite` | `sqlite::register`, `start`, `start_heartbeat` and `decide` for the embedded backend |

Read [`docs/agent-adapter.md`](../docs/agent-adapter.md) for the whole
pattern, and [ADR 0006](../docs/adr/0006-agent-adapter-framework.md) for the
choice of framework.
