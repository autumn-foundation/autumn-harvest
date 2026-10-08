# autumn-harvest-agent

A durable agent loop for [autumn-harvest](../README.md) (issue #1973).

It adapts [`autumn-plugin-agent`](https://crates.io/crates/autumn-plugin-agent)
to the engine:

- The agent loop is the workflow `agent_loop`.
- Each model call is the activity `agent_model_turn`. It also records the
  tool-policy decision for each call.
- Each tool call is the activity `agent_tool_call`.
- A call that the policy gates waits on a durable signal with a deadline.

A crash costs at most the one step that was in flight. The crate has no
`autumn-web` dependency.

```sh
cargo run -p autumn-harvest-agent --features sqlite --example sqlite_agent
```

| Feature | Contents |
|---|---|
| `sqlite` | `sqlite::register`, `sqlite::start`, `sqlite::decide` for the embedded backend |

Read [`docs/agent-adapter.md`](../docs/agent-adapter.md) for the whole
pattern, and [ADR 0006](../docs/adr/0006-agent-adapter-framework.md) for the
choice of framework.
