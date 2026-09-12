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
| A restart loses the session | Every committed turn is in history; a restart **replays** it instead of buying it again |
| A tool must not run unattended | The loop parks on a durable **signal** with a **deadline**, and the signal names the one call it releases |
| A rate limit kills the run | The model call is an **activity** with a retry policy and backoff |
| "What did it actually do?" | The **event log** is the audit trail — `agentd history <id>` |
| A crash mid-tool re-runs it | Activity execution is **at-least-once**, so the tool bodies are idempotent |

## Try it in one minute — no API key needed

With no `ANTHROPIC_API_KEY` set, the daemon registers a small **offline stub
model** instead of calling the API. A variable holding only whitespace counts
as unset, and a key is trimmed before use. The stub drives the same loop: it lists the
workspace, proposes one file write (which needs your approval), and answers.

Run these from the repository root — Cargo needs the workspace manifest:

```bash
mkdir -p /tmp/agent-demo && echo '# Demo project' > /tmp/agent-demo/README.md

cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &

cargo run -p claude-agent-daemon -- submit "summarise the README"
#  ffffc7df-4ebd-4fea-89e0-74f4861aacba

cargo run -p claude-agent-daemon -- status <id>
#  ffffc7df-…  RUNNING
#    goal:    summarise the README
#    blocked: waiting for a tool approval
#    pending: write_file (toolu_offline_write)
#             {"content":"# Offline stub\n…","path":"agent-notes.md"}
#    decide:  agentd approve ffffc7df-… tool_approval:2:0:toolu_offline_write   (or `deny`)

cargo run -p claude-agent-daemon -- approve <id> <token>
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

The request uses `claude-opus-5` with adaptive thinking and the three workspace
tools below. Pick another model with `--model`.

Server-side refusal fallbacks are deliberately **not** used. A fallback answers
one turn on a different model, and this is a multi-turn loop: the next request
would return to the configured model, which changes the conversation's model
without saying so, and would send thinking blocks to a model that did not
produce them. A refusal ends the session under its own stop reason instead,
where the operator can see it.

## The restart proof

This is the property the whole example exists to show. Park a session on its
approval gate, kill the daemon outright, start it again, and let it finish:

```bash
cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &
cargo run -p claude-agent-daemon -- submit "summarise the README"
cargo run -p claude-agent-daemon -- status <id>     # blocked: waiting for a tool approval

pkill -x agentd                                     # the process dies with work in flight

cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo &
cargo run -p claude-agent-daemon -- approve <id> <token>
cargo run -p claude-agent-daemon -- status <id>     # COMPLETED
cargo run -p claude-agent-daemon -- history <id>
#    1  WorkflowStarted  {"input":{"goal":"summarise the README",…}}
#    2  ActivityScheduled  {"activity_name":"claude_turn",…}   ← turn 1
#    3  ActivityCompleted  {"output":{"stop_reason":"tool_use",…}}
#    …
#    8  TimerStarted  {…}                    ← the approval deadline
#    9  SignalReceived  {"name":"tool_approval:2:0:toolu_…",…}
#   10  ActivityScheduled  {"activity_name":"run_tool",…}      ← the gated write
#   …
#   14  WorkflowCompleted  {"output":{"stop":"end_turn",…}}
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
| `agentd status <id> [--full]` | One session: state, why it is parked, the exact pending call, its answer. `--full` prints the call's arguments untrimmed. |
| `agentd list` | Every session in the database. A long listing prints its newest sessions and names the `--before <row>` that reads the ones before them, so an old session waiting for a decision stays reachable however many newer ones arrive. |
| `agentd history <id>` | The recorded event log with each event's data — the audit trail. An id that names no session is refused, so a typo cannot read as a session that did nothing. A long log prints its newest events and names the `--before <seq>` that reads the ones before them. |
| `agentd approve <id> <token>` / `deny <id> <token>` | Release or refuse the gated tool call that token names. A decision after the call's deadline is refused, because the session denies that call whatever the answer says. |

The `decide:` line carries `--socket` whenever you chose one, so the command
you copy reaches the daemon that printed it.

### One loop drives and serves, and that bounds how fast it answers

The daemon drives sessions and answers commands on ONE task. A drive runs
`run_until_blocked`, which carries a session forward until it needs a decision,
needs a timer, or ends. A session whose turns call only read tools reaches none
of those, so one drive can run every turn it has.

Each turn can take up to the request timeout of 840 seconds, and `--max-turns`
defaults to 8. A session that uses its whole budget can therefore hold the loop
for around two hours, and no command is answered in that time. Raising
`--max-turns` raises that bound with it.

The cause is the loop and not the engine. A `status`, `list` or `history` reads
through a second, read-only connection and needs nothing the drive holds, so
those three could be answered during a drive by a daemon that served them on
another task. `submit` and `approve` write, and they would still wait: this
backend has one writer by design.

A reader who wants an always-answering control socket should serve commands on
a task of their own and leave the drive to this one. That is a change to the
shape of the daemon rather than a setting, so this example keeps the single
loop and states the cost here.

Flags: `--db` (default `agentd.db`), `--socket` (default `agentd.sock`),
`--workspace`, `--model`, `--max-tokens`, `--tick-ms`. Each also reads an
`AGENTD_*` environment variable. The API key is **not** among them: it comes
from `ANTHROPIC_API_KEY` only. A process's arguments are readable by every user
of the host, and this daemon runs for as long as its sessions do.

A finished session reports the model's own stop reason, so an incomplete run
never reads as a clean one: `end_turn` is a finished answer, `max_tokens` means
the turn hit the output cap and the answer is cut short (raise `--max-tokens`),
and `refusal` means a classifier declined the request. A tool runs only under
the `tool_use` stop reason that asks for one. A turn that stops for any other
reason ends the session under that name, with its tool calls dropped unrun. The
one pair that cannot be reconciled is refused instead: a turn that says it
ended and still asks for a tool is malformed, and no guess about which half is
wrong would be safe.

**The database lives outside the workspace.** The agent can write any path
inside the workspace, and a write replaces its target. A database the agent can
reach is therefore one approved tool call away from replacement, while `SQLite`
still holds the old inode — the recorded history of every session, gone. The
daemon refuses to start in that layout, and names the flags to change. The
defaults already meet the rule: `agentd.db` in the current directory, and
`--workspace agent-workspace` beside it rather than around it.

**The database is owner-only too.** It holds every prompt, tool input, and tool
result, including the content of each file the agent read — so a new database
is created `0600`, and the `-wal` and `-shm` sidecars are created under a
narrowed umask. An existing database keeps whatever mode the operator gave it.

**The control socket is owner-only, and the daemon checks.** Whoever can
connect can spend money and approve writes with the daemon's privileges, so the
socket is created `0600` under a narrowed umask — private at creation, with no
window to connect through. That mode is not the whole control: Linux enforces a
socket's mode on `connect`, and macOS does not, so a socket in a directory
other users can search would accept them there. The daemon therefore asks the
kernel who is calling and serves only its own user. A caller it cannot identify
is refused. A path that already holds something other than a socket is never
removed: a typo in `--socket` reports an error instead of deleting a file.

**A mismatched daemon refuses to start.** The daemon checks every resumable
session before it drives anything. Mistype `--workspace` and you get an error
at startup with the session untouched — not a run failed past recovery, which
is what a mismatched tool call would cause, because only a running session is
ever driven again. A workspace path that is not valid UTF-8 is refused for the
same reason: a name that cannot be recorded exactly could never match again.

**A session is bound to its workspace and its model.** Both are recorded in the
session's history at submit time, and a call is refused when the daemon serves
a different one. Otherwise a restart pointed at another directory could apply
an already-approved write to the wrong project, and a restart under another
model — or with an API key where there was none — would continue one
conversation on a different model, or move an offline session onto billed
calls. `offline-stub` is the identity the daemon records for its own stub, so
it is refused as a `--model` name when a key is set: it would otherwise match
an offline session and send that local transcript to the API. `--max-tokens` is deliberately not fenced: it is a per-request budget
rather than an identity, so changing it between restarts is ordinary tuning.

**Approval is per wait, not per session — and not per tool-use id either.**
`status` prints the exact call (the tool, its id, its arguments) and an
**approval token** that names one wait of one run. The decision carries that
token back, and the daemon matches it exactly. An id would not be enough: the
model can reuse one across turns, so a decision read from an older status could
release a later call. A deadline can also expire while you read, and an early
or repeated decision has no live wait to land in — all of those are refused
rather than stored for some later, unseen write.

A decision is spent when it is delivered: a repeated `approve` is refused
rather than staging a second signal that a later call could consume.

A write can carry up to 64 KiB, and `status` trims a long one to stay readable.
It says so when it does, and `status <id> --full` prints every byte — so nothing
is ever approved sight unseen.

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
  `write_file`, each confined to the workspace directory. The confinement is
  not only lexical: a symbolic link at the final component is refused, and the
  deepest existing ancestor is resolved through every link and must stay under
  the real workspace root. The 64 KiB read cap is checked before the file is
  allocated, and the 200-entry listing cap stops the directory read rather than
  trimming its result, so neither one huge file nor one huge directory can take
  the daemon down. A write lands
  atomically, through a scratch file renamed over the target, with the file and
  every directory from there up to the workspace root flushed, so an approved
  file is never left half-written and the replacement survives a host crash.
  The daemon flushes the path above the workspace once at startup, because the
  entry that names the workspace lives there. It keeps the mode of
  the file it replaces — a content change is not a permission change. A file the agent
  creates starts `0600`. `write_file` is the one tool the workflow gates on
  approval.
- **[`src/daemon.rs`](src/daemon.rs)** — the socket, the drive tick, and the
  single-writer main loop.
- **[`src/inspect.rs`](src/inspect.rs)** — a second, **read-only** connection
  for the reads the runtime does not expose. Each one asks for what it needs: a
  `status` reads one row, the startup check and the drive tick read the running
  rows, and only `list` reads them all.
- **[`src/guard.rs`](src/guard.rs)** — the exclusive lock on the database file
  that keeps "one writer" true across processes.

## Tests

```bash
cargo test -p claude-agent-daemon
```

Twenty-eight tests, all offline: the happy path, a denied tool call, the restart
proof, the workspace sandbox (two symlink escapes, the read cap, and a named
pipe), an atomic write, a truncated turn, a turn that says nothing, a stale
approval, the full approval view, a session bound to another workspace and to
another model, the single-writer lock through every alias, the database and
socket permissions, a refused hard-linked database, a turn whose tool calls
share an id, a repeated decision, a write that keeps its target's mode
(including one the umask would strip) and never deletes a file on a scratch
name, the drive interval, which API failures may be retried, a billed response
that is not a message, and one end-to-end run through the daemon socket.

## What this example does not do

Honest limits, so nothing here reads as a promise:

- **One writer.** The daemon owns the database file, and holds an exclusive
  `flock` on that file to prove it. The lock is on the file itself rather than
  on a name derived from its path, so a symbolic link or a different spelling
  reaches the same lock. A **hard-linked** database is refused outright:
  `SQLite` derives its write-ahead log from the path, so two names would read
  two different logs and lose each other's committed sessions. The lock is
  taken **before** the database
  is opened, because opening reclaims every task left `RUNNING` by a dead
  process: a second daemon that opened the file first would reclaim a *live*
  task and run its activity twice. The kernel releases the lock when the holder
  dies, so a crash strands nothing. A fleet wants the Postgres core.
- **The runtime is serialised.** A control command waits while a model call is
  in flight, because both need the runtime mutably. That is the single-writer
  model, made visible. The daemon holds at most 32 control connections while it
  waits, and the kernel queues the rest on the listening socket, so a polling
  script cannot spend its descriptors.
- **The transcript rides in history.** Each turn stores the whole conversation
  as its activity input, which is simple and replay-exact but grows with the
  session. A long-running agent should store the transcript outside the engine
  and pass a handle instead. The 2 MiB payload cap is the hard bound.
- **Polling, not push.** `SQLite` has no `LISTEN`/`NOTIFY`, so progress comes
  from the `--tick-ms` poll. The poll reads no database at all: the daemon is
  the only writer, so it holds the live sessions in memory, seeded once at
  startup from the rows a previous process left `RUNNING`. A query on every
  tick would visit every session that ever ran, because `harvest_executions`
  is indexed on `(workflow_name, workflow_id)` and not on `state`. The engine
  owns that schema, and an example does not add an index to it. A production
  daemon would sleep until the next timer deadline.
- **No streaming.** One turn is one non-streaming request, because an activity
  result is a value, not a stream. Token-by-token output needs a side channel.
- **One turn can be paid for twice.** Activity execution is at-least-once, and
  the Messages API takes no idempotency key. If the daemon dies, or the
  connection drops, after the API accepts a request but before the result is
  committed, that one turn is sent again on resume. The window is one turn
  wide, and only the uncommitted turn: every turn already in history replays
  for free, which is the property the restart proof shows. Every unambiguous
  case — the request accepted, and its response then lost or unparseable — is
  not retried at all, since a retry there would be charged again for certain. A
  rate limit or a server fault produced no turn, so those still back off and
  retry as the policy says.
- **The workspace is confined, not sandboxed.** The toolbox refuses a path
  that escapes, a symbolic link at the final component (`O_NOFOLLOW` on the
  open, so a link appearing after the check loses too), and anything that is
  not an ordinary file. What it does not do is traverse through opened
  directory descriptors, so a *concurrent local process* that swaps a parent
  directory for a symlink mid-call can still win that race. Closing it needs
  `openat`-based traversal, which is more machinery than an example should
  carry. The model is the untrusted party here, and it cannot win that race;
  another process running as you already can do worse directly.
- **A session is bound to its workspace path, not to the directory object.**
  The path is recorded at submit time. A daemon serving a different one refuses
  the call. The check cannot see a directory deleted and recreated at the same
  path between an approval and the write. Binding to the inode instead would
  refuse every legitimate recreation: a fresh clone, a restore, a rebuilt
  container. That turns a resumable session into a dead one. It would also miss
  the simpler substitution, where the contents change and the inode does not.
  The path is the honest guarantee here.
- **Startup paths are not race-free against a local attacker.** The daemon
  locks the database and then opens it by path, and it reclaims a stale socket
  by checking it and then replacing it. A local process racing either sequence
  can defeat it. Both would need an identity the pathname cannot carry — and
  `SQLite` must be handed a path, since that is how it names its write-ahead
  log. The same reasoning applies: whoever can win these races already runs as
  you.
- **Two daemons must not share one socket path.** Pointed at the same
  `--socket` with different databases, two daemons starting at once can both
  find the socket stale, and the loser ends up running but unreachable. Give
  each daemon its own `--socket`.
- **An atomic write replaces the file, so it replaces its owner.** The rename
  that makes a write all-or-nothing installs a new inode, which the daemon
  owns. Its mode is carried over, but an unprivileged process cannot give a
  file back to another user, so a workspace shared between users is not a good
  fit. Writing in place would keep the owner and lose the atomicity; this
  example keeps the atomicity.
- **The socket file outlives the daemon.** Shutdown does not unlink it: no
  check can prove a public pathname still names *this* daemon's socket, and
  deleting someone else's is worse than leaving a stale one. The next start
  reclaims it, once it has proved the entry is a socket that nobody answers on.
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
