# claude-agent-daemon

A **local daemon** that runs Claude agent sessions as **durable workflows**, on
the embedded [`autumn-harvest-sqlite`](../../autumn-harvest-sqlite) backend.
No database server, no Docker, no message broker — one binary and one file.

The point of the example: an agent loop is exactly the workload that *cannot*
be fire-and-forget. Turns are expensive, tools touch the real world, and a
session can sit for minutes waiting on a human. That is a durable workflow, and
this example shows what the engine gives you once the loop lives inside one:

| Agent-harness problem | What the engine does here |
| --- | --- |
| A restart loses the session | Every turn is in history; a restart **replays** it and pays for nothing twice |
| A tool must not run unattended | The loop parks on a durable **signal** with a **deadline**, not on an in-memory future |
| A rate limit kills the run | The model call is an **activity** with a retry policy and backoff |
| "What did it actually do?" | The **event log** is the audit trail — `agentd history <id>` |
| A crash mid-tool re-runs it | Activity execution is **at-least-once**, so the tool bodies are idempotent |

## Try it in one minute — no API key needed

With no `ANTHROPIC_API_KEY` set, the daemon registers a small **offline stub
model** instead of calling the API. The stub drives the same loop: it lists the
workspace, proposes one file write (which needs your approval), and answers.

```bash
mkdir -p /tmp/agent-demo && cd /tmp/agent-demo
echo '# Demo project' > README.md && cd ..

cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &

cargo run -p claude-agent-daemon -- submit "summarise the README"
#  ffffc7df-4ebd-4fea-89e0-74f4861aacba

cargo run -p claude-agent-daemon -- status <id>
#  ffffc7df-…  RUNNING
#    goal:    summarise the README
#    blocked: waiting for a tool approval

cargo run -p claude-agent-daemon -- approve <id>
cargo run -p claude-agent-daemon -- status <id>
#  ffffc7df-…  COMPLETED
#    answer:  [end_turn after 3 turns, 2 tool calls] …
```

## Run it against Claude

```bash
export ANTHROPIC_API_KEY=sk-ant-…
cargo run -p claude-agent-daemon -- serve --workspace /path/to/your/project
cargo run -p claude-agent-daemon -- submit "find the TODOs and write them to TODO.md"
```

The request uses `claude-opus-5` with adaptive thinking, the three workspace
tools below, and server-side refusal fallbacks (beta
`server-side-fallback-2026-07-01`), so a declined request is routed by category
instead of ending the session. To turn the fallbacks off, delete the
`anthropic-beta` header and the `"fallbacks"` field in
[`src/claude.rs`](src/claude.rs) together. Pick another model with `--model`.

## The restart proof

This is the property the whole example exists to show. Park a session on its
approval gate, kill the daemon outright, start it again, and let it finish:

```bash
cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &
cargo run -p claude-agent-daemon -- submit "summarise the README"
cargo run -p claude-agent-daemon -- status <id>     # blocked: waiting for a tool approval

pkill -x agentd                                     # the process dies with work in flight

cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &
cargo run -p claude-agent-daemon -- approve <id>
cargo run -p claude-agent-daemon -- status <id>     # COMPLETED
cargo run -p claude-agent-daemon -- history <id>
#    1  WorkflowStarted
#    2  ActivityScheduled      ← turn 1: the model call
#    3  ActivityCompleted
#    …
#    8  TimerStarted           ← the approval deadline
#    9  SignalReceived         ← your `approve`
#   10  ActivityScheduled      ← the gated write
#   …
#   14  WorkflowCompleted
```

The second daemon re-registers the same handlers and replays the recorded
history. The turns from the first process are *not* sent to the API again. The
test `a_restart_resumes_the_session_without_repeating_model_calls` asserts
exactly that, by counting model calls in each process.

## Commands

| Command | What it does |
| --- | --- |
| `agentd serve` | Run the daemon. This process is the single writer. |
| `agentd submit "<goal>"` | Start one session; prints its execution id. |
| `agentd status <id>` | One session: state, why it is parked, its answer. |
| `agentd list` | Every session in the database. |
| `agentd history <id>` | The recorded event log — the audit trail. |
| `agentd approve <id>` / `deny <id>` | Release or refuse a gated tool call. |

Flags: `--db` (default `agentd.db`), `--socket` (default `agentd.sock`),
`--workspace`, `--model`, `--max-tokens`, `--tick-ms`. Each also reads an
`AGENTD_*` environment variable.

## How it is put together

```text
  agentd submit / status / approve / list / history
                    │
                    │  Unix socket: one JSON line in, one out
                    ▼
┌─ agentd serve ─────────────────────────────┐
│ the ONLY writer: it owns the database file │
│                                            │
│ select { control command | drive tick }    │
│             │                              │
│             ▼                              │
│ SqliteRuntime  ───────────────►  agentd.db │
│   · agent_session   workflow       events  │
│   · claude_turn     activity       tasks   │
│   · run_tool        activity       timers  │
└────────────────────────────────────────────┘
```

- **[`src/session.rs`](src/session.rs)** — the agent loop, as a `#[workflow]`.
  Ordinary Rust: a `for` loop over turns, a model call, a tool call per
  `tool_use` block. Durability comes from the awaits. The transcript is rebuilt
  from activity results on every replay, so it is a projection of history rather
  than state the daemon has to hold.
- **[`src/claude.rs`](src/claude.rs)** — one Messages API request, as one
  durable activity body, plus the offline stub. The assistant content blocks are
  stored and replayed **verbatim**, which is what keeps thinking blocks valid
  across turns on the same model.
- **[`src/tools.rs`](src/tools.rs)** — `list_files`, `read_file`, and
  `write_file`, each confined to the workspace directory. `write_file` is the
  one tool the workflow gates on approval.
- **[`src/daemon.rs`](src/daemon.rs)** — the socket, the drive tick, and the
  single-writer main loop.
- **[`src/inspect.rs`](src/inspect.rs)** — a second, **read-only** connection
  for the listing the runtime does not expose.

## Tests

```bash
cargo test -p claude-agent-daemon
```

Five tests, all offline: the happy path, a denied tool call, the restart proof,
the workspace sandbox, and one end-to-end run through the daemon socket.

## What this example does not do

Honest limits, so nothing here reads as a promise:

- **One writer.** The daemon owns the database file. A second `serve` against
  the same file is refused while the first holds the socket, and two writers on
  one file are outside the backend's contract. A fleet wants the Postgres core.
- **The runtime is serialised.** A control command waits while a model call is
  in flight, because both need the runtime mutably. That is the single-writer
  model, made visible.
- **The transcript rides in history.** Each turn stores the whole conversation
  as its activity input, which is simple and replay-exact but grows with the
  session. A long-running agent should store the transcript outside the engine
  and pass a handle instead. The 2 MiB payload cap is the hard bound.
- **Polling, not push.** `SQLite` has no `LISTEN`/`NOTIFY`, so progress comes
  from the `--tick-ms` poll. A production daemon would sleep until the next
  timer deadline.
- **No streaming.** One turn is one non-streaming request, because an activity
  result is a value, not a stream. Token-by-token output needs a side channel.
- **Unix only.** The control surface is a Unix domain socket, so the daemon
  runs on Linux and macOS. A Windows port needs a named pipe or a TCP port.
- **The offline stub is not Claude.** It exists so the durability story is
  demonstrable and testable with no key and no network.

## Where to read next

- [`docs/sqlite-backend.md`](../../docs/sqlite-backend.md) — the task-oriented
  guide to this backend: the drive model, signals, and crash recovery.
- [`autumn-harvest-sqlite`](../../autumn-harvest-sqlite) — the backend crate,
  including the two smaller examples (`quickstart`, `durability`).
- [`examples/standalone-runner`](../standalone-runner) — the same engine on the
  Postgres core, with a worker fleet and the management API.
