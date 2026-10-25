# agent-mesh: an always-on support desk

This example runs a support desk with three durable agents. It is the
capstone of [epic #2262](https://github.com/autumn-foundation/autumn-harvest/issues/2262).
Each agent is a keyed entity in one SQLite file. A restart, a crash between
steps, or a duplicate message changes nothing that the mesh already did.

| Agent | Does | Tools | Wakes |
|---|---|---|---|
| `triage/desk` | Reads the inbox. Answers a ticket when a person asks. | `check_inbox`, `file_reply`, and the peers `researcher` and `critic` | A heartbeat every 15 minutes |
| `researcher/kb` | Finds facts in the notes. | `search_notes` | A peer message |
| `critic/qa` | Checks a draft reply. | none | A peer message |

The desk shows each feature of the epic:

- **Always on.** The triage agent wakes on its own heartbeat. A scheduled
  check is unattended, so it may only read (#2266).
- **Durable peers.** A `delegate` call to `researcher` sends a peer message
  to `researcher/kb` and waits for its reply. Each message runs once, across
  restarts (#2269).
- **Mandates.** Each message carries a mandate that only narrows. The
  researcher may only search, and the critic may use no tool.
- **Approvals.** `file_reply` writes, so the policy asks a person first. The
  agent waits durably, so the approval can come from another process. A
  reply waits one day of desk clock. After that, the agent reads it as
  denied and files nothing.
- **Live tokens.** The triage agent streams its turns. The CLI prints the
  text as the model writes it (#2268).
- **A mesh budget.** The three agents share one daily token cap, read from
  the cost ledger of the SQLite file (#2272, #2275). A spent budget parks
  the mesh until the next day.
- **Lifecycle.** An idle agent is a row in the file. Each command opens a
  new runtime, so every step below is a restart (#2267).

## Run it

The default model is offline and needs no key:

```console
$ cargo run -p agent-mesh -- --dir /tmp/desk demo
```

Or step by step:

```console
$ cargo run -p agent-mesh -- --dir /tmp/desk init
$ cargo run -p agent-mesh -- --dir /tmp/desk start
$ cargo run -p agent-mesh -- --dir /tmp/desk tick --minutes 16
[report Heartbeat] 2 open tickets: T-1 (Export fails after the upgrade), T-2 (How do I rotate my API key?).
$ cargo run -p agent-mesh -- --dir /tmp/desk answer T-1
The reply waits for approval. Run `agent-mesh approve`, or `--reject <reason>`.
$ cargo run -p agent-mesh -- --dir /tmp/desk approve
[...] I filed the reply to T-1. The researcher found the facts, and the critic checked the draft.
$ cargo run -p agent-mesh -- --dir /tmp/desk status
```

`tick` moves the desk clock on, so the heartbeat fires at once. The clock
never runs back.

To run the agents on Claude, set a key and pass `--model claude`:

```console
$ export ANTHROPIC_API_KEY=...
$ cargo run -p agent-mesh -- --dir /tmp/desk --model claude answer T-2
```

The Claude adapter is `autoresearch::claude::ClaudeModel`. It streams over
Server-Sent Events, so the text shows while the model writes it.

## The files

| Path | Holds |
|---|---|
| `inbox/<id>.json` | One ticket. |
| `notes/<name>.md` | One note of the knowledge base. |
| `replies/<id>.md` | The filed reply to a ticket. |
| `filed.log` | One line for each run of `file_reply`. |
| `reports.log` | One line for each report of the triage agent. |
| `desk.db` | The SQLite file of the mesh: histories, states and the cost ledger. |
| `clock.txt` | How far `tick` moved the desk clock, in minutes. |

## The tests

| Suite | Proves |
|---|---|
| `tests/e2e.rs` | A check reports the inbox and files nothing. An answer asks both peers once, waits for approval across a restart, and files the reply once. A rejected reply is never filed. A spent budget parks the mesh until the next day. |
| `tests/simulation.rs` | A seeded sweep with restarts, reordered steps, duplicate ops, clock jumps and a model that tries a write in a check. Four invariants hold on every seed: no lost or duplicate message, no widened mandate, no budget overrun on any day, and no write in an unattended tick. Each nemesis fires on some seed: a restart and a reorder while an agent waits for a reply, a heartbeat and a passed approval deadline after a clock jump. |
| `tests/postgres.rs` | The agent loop on a Postgres worker. Three children of one turn overlap on the wall clock, and one slot runs them one after another (#2270). The live text of a turn reaches a progress listener while the turn runs (#2268). A durable peer on another shard gets one message and answers once (#2269). An erased agent keeps no memory, stream chunk or transcript text (#2267). |

Run one seed of the sweep with `AGENT_MESH_SEED=<n>`. The Postgres suite
starts a container, or uses `HARVEST_TEST_DATABASE_URL` when it is set.

## Limits

- The desk runs on SQLite, in one process at a time. The same agents run
  on Postgres with `autumn_harvest_agent::workflow::workflows()` and
  `activities()`, and the harness in `HarvestBuilder::state`.
  `tests/postgres.rs` shows the setup.
- The budget check runs before each model turn, so a day can pass its cap
  by one turn. A cap that is smaller than one answer can stop an answer in
  the middle. The answer then ends, and the agent parks until the next day.
- A reply waits for approval for one day of desk clock. Set
  `MeshConfig::approval_timeout` to change it. Each `tick` moves the clock
  16 minutes, so a reply outlasts about 90 ticks. After that, `approve`
  says that nothing waits.
- A peer must run before a message can reach it. `start` starts all three.
