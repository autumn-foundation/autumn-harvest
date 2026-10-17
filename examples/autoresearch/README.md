# autoresearch on Harvest

Karpathy's [autoresearch](https://github.com/karpathy/autoresearch) gives an
agent a small LLM training setup. The agent edits `train.py`, trains for five
minutes, keeps the change if `val_bpb` goes down, and repeats all night.

This example runs that loop as **durable Harvest workflows**, on the embedded
[SQLite backend](../../docs/sqlite-backend.md). Each step is a workflow, a
child workflow or an activity. A crash costs at most the step in flight. A
restart replays the night and does not pay for a model turn or a training run
twice.

By default it needs **no GPU, no API key, no network and no Docker**. A
simulated trainer scores `train.py` on a fixed loss landscape, and a scripted
researcher plays the agent. Point it at a real checkout and a key, and the
same workflows run the real thing.

## Try it in one minute

Run these from the repository root:

```bash
cargo run -p autoresearch -- init /tmp/ar-lab
cargo run -p autoresearch -- run --lab /tmp/ar-lab --tag demo --experiments 8
cargo run -p autoresearch -- status --lab /tmp/ar-lab
```

```text
started autoresearch/demo on branch autoresearch/demo
  #0   keep    0.997795   44.0 GB  c1dc0d6  baseline
  #1   discard 0.997884   49.5 GB  d2ff4ed  much deeper model: 8 -> 12
  #2   keep    0.995173   44.0 GB  ac42df0  raise MATRIX_LR: 0.04 -> 0.05
  #3   keep    0.993402   44.0 GB  baae43b  longer warmdown: 0.5 -> 0.7
  #4   keep    0.992512   44.0 GB  b82abb8  halve WEIGHT_DECAY: 0.2 -> 0.1
  #5   keep    0.986595   68.8 GB  3287aa7  deeper model: 8 -> 10
  #6   crash   0.000000    0.0 GB  8f6495b  ten times MATRIX_LR: 0.05 -> 0.5
  #7   keep    0.983772   68.8 GB  2972e15  halve TOTAL_BATCH_SIZE: 2**19 -> 2**18
  #8   discard 0.985371   68.8 GB  14e1329  full attention: "SSSL" -> "L"
done: autoresearch/demo: 8 experiments, best val_bpb 0.983772 at 2972e15 (6 kept, 2 discarded, 1 crashed)
```

Experiment 1 ran out of memory, and a fix proposal halved the device batch.
Experiment 6 diverged, and the fix agent gave up, so the row is a crash. The
branch `autoresearch/demo` holds the baseline and the six kept commits only.
`results.tsv` and a morning report sit in the lab.

## How upstream maps to Harvest

| Upstream autoresearch | Harvest primitive |
|---|---|
| `program.md`, which the human edits | The system prompt of each `agent_loop` run. `eval` tests a new one against recorded runs. |
| The agent edits `train.py` only | The `edit_train` tool writes that one file. The policy denies every unknown tool. |
| `prepare.py` is read-only | `read_file` serves a fixed list of files. No tool writes `prepare.py`. |
| The branch `autoresearch/<tag>` must not exist | The session workflow id is `autoresearch/<tag>`, started with `RejectDuplicate`. |
| Commit, train, then keep or `git reset` | A saga: the commit is a step, and `git reset` is its compensation. |
| Five minutes of training, kill a run at ten | The `train` activity, `start_to_close = "15m"`. The trainer kills the process group at its timeout. |
| Fix a dumb crash, or log `crash` | One fix proposal as a child `agent_loop`, then a `crash` row. |
| `results.tsv`, untracked | The `record` activity appends once per row. Git excludes the file. |
| NEVER STOP | `continue_as_new` every N experiments, so the history stays small. A `stop` signal ends the night. |
| "You wake up to a log of experiments" | A nightly schedule, a detached report agent, and a heartbeat that speaks only on trouble. |
| Several GPUs, several branches | A workflow mutex per GPU. A kept idea goes to sibling sessions as a signal. |
| Rewind "very very sparingly" | The `rewind` tool waits for a person's approval. |
| The simplicity criterion | The keep rule charges each added line. A `reviewer` peer reads each diff. |

## The workflows

```text
schedule "0 23 * * *" ─► research_session  (continue-as-new every N)
                          │  signals: hint, stop, policy · query: status · update: whatif
                          │  activity setup · search attributes · memo
                          ├─► experiment (child, one per idea)
                          │     ├─► agent_loop (child: propose one edit)
                          │     │     read_file, read_paper, show_diff, edit_train, rewind
                          │     │     reviewer peer · memory · loop guard · response cache
                          │     ├─ saga: commit ─► compensation: reset
                          │     ├─ mutex gpu:<id> ─► activity train
                          │     ├─ local activity parse_summary
                          │     └─ activity record (results.tsv)
                          ├─► signal_external_workflow_by_id: sibling sessions
                          └─► detached agent_loop: the morning report
schedule every 30m ─► agent_heartbeat (precheck: new rows) ─► delivery
```

- [`src/workflows.rs`](src/workflows.rs): `research_session` and `experiment`.
- [`src/activities.rs`](src/activities.rs): git, the trainer and `results.tsv`.
  Each input carries the lab path, so the bodies run on SQLite and Postgres.
- [`src/agent.rs`](src/agent.rs): the tools, the policy, the flow rule, the
  reviewer peer and the stores.
- [`src/rules.rs`](src/rules.rs): the keep rule, one pure function.
- [`src/runtime.rs`](src/runtime.rs): registration, the drive, the inbox
  commands, `verify`, `eval`, the schedules and the Postgres registration.
- [`src/scripted.rs`](src/scripted.rs) and [`src/claude.rs`](src/claude.rs):
  the two models.

## The keep rule

With `gain = best - val_bpb` and `net = lines_added - lines_removed`:

- no `val_bpb` is a `crash`;
- peak memory over `max_memory_gb` (80 GB) is a `discard`;
- `gain - line_cost * net > min_improvement` is a `keep`;
- anything else is a `discard`.

The defaults are `min_improvement = 0` and `line_cost = 0.0001`. So a change
that adds 20 lines needs a gain over 0.002, and an equal result from less code
is a keep. Equal or worse is a discard, as upstream. Change the rule of a live
session with the `policy` signal.

## Steer a running night

`run` and `serve` hold the database as its one writer. The other commands
write a file to the inbox, and the runner turns it into a keyed signal. A
resend after a crash does not deliver twice.

```bash
cargo run -p autoresearch -- hint --lab /tmp/ar-lab --tag demo "paper:warmdown"
cargo run -p autoresearch -- stop --lab /tmp/ar-lab --tag demo
```

A hint of the form `paper:<name>` makes the agent read `papers/<name>.md`.
That text comes from outside the lab, so a flow rule makes the next edit wait
for a person. A hint of the form `rewind:<commit>` asks for a rewind, which
always waits. The runner prints the call and its token:

```text
waiting for a person: edit_train {"new":"WARMDOWN_RATIO = 0.7","old":"WARMDOWN_RATIO = 0.5"}
  approve: autoresearch approve --lab /tmp/ar-lab '<run id>/tool_approval:1:0:call_1_0'
  deny:    autoresearch deny --lab /tmp/ar-lab '<run id>/tool_approval:1:0:call_1_0'
```

A wait has a deadline, 15 minutes by default (`--approval-timeout`). When it
passes, the call does not run, and the agent reads that no approval came.

Kill the runner at any time. Run the same command again, and it resumes the
session where it stopped.

## Run it on a GPU

1. Clone [karpathy/autoresearch](https://github.com/karpathy/autoresearch)
   and run `uv run prepare.py` once.
2. Copy this example's [`program.md`](program.md) over the checkout's. The
   upstream one tells the agent to run git and the trainer itself. Here the
   workflows do that, and the agent only edits.
3. Run a night with the real trainer and Claude:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
cargo run -p autoresearch -- run --lab ~/autoresearch --trainer command \
  --data-dir ~/.cache/autoresearch --model claude --experiments 100
```

`--trainer command` runs `uv run train.py` in the lab and kills it at ten
minutes. `--model claude` uses `claude-opus-5-5` with adaptive thinking and
the server-side refusal fallback. The key comes from the environment only.
`serve` runs the nightly schedule and the heartbeat instead of one night.

## Commands

| Command | What it does |
|---|---|
| `init <dir>` | Create an offline lab with one commit on `master`. |
| `run --lab <dir>` | Start or resume a session and drive it to its end. |
| `serve --lab <dir>` | Run a session at 23:00 UTC each night and a heartbeat every 30 minutes. |
| `status --lab <dir>` | List the sessions and the last rows, with a read-only handle. |
| `hint`, `stop` | Queue a signal for a session. |
| `approve`, `deny` | Queue a decision for a gated tool call. |
| `eval --candidate <file>` | Re-drive a recorded proposal with a new `program.md`. Prints the first turn that differs. |
| `verify` | Replay every stored history with this build. Run it before a deploy. |
| `purge --older-than-days <n>` | Remove old finished runs. |

The database is `<lab>/.autoresearch/harvest.db` unless `--db` names one.
The memory notes, the response cache, the inbox and the reports live beside it.

## Tests

```bash
cargo test -p autoresearch
```

All tests are offline. They need `git` and a Unix shell.

- Unit tests cover the keep rule (with property tests), `results.tsv`, the
  summary, both trainers, the lab, the tools, the stores and both models.
- [`tests/simulation.rs`](tests/simulation.rs) sweeps both workflows with the
  seeded simulation of issue #2033. Each seed runs twice, replays, and runs
  again with a crash. The sweeps reorder results, signals and timers, and
  inject faults. Required `sometimes!` targets prove that the sweeps reach a
  keep, a discard, a crash, a fixed crash, a stop and a hand-off. Widen a
  sweep with `HARVEST_SIM_SEEDS=10000`.
- [`tests/e2e.rs`](tests/e2e.rs) runs each acceptance criterion on SQLite with
  a real git lab: the restart proof, approvals, signals, the query and the
  update, two sessions on one GPU, continue-as-new, the schedules, `eval`, the
  response cache, memory, the inbox, retention and the Postgres registration.
- [`tests/cli.rs`](tests/cli.rs) runs the binary as a reader does.

The simulation found a real bug while this example was written: the
experiment set the search attribute `status`, which the engine reserves.

## Limits

- **The simulated trainer is not a GPT.** It scores the hyperparameter block
  on a fixed landscape, so the loop is testable offline.
- **The scripted researcher is not Claude.** It tries a fixed list of ideas.
- **Training runs on the host.** The sandbox activities have no GPU.
- **One writer.** One process drives the database, as the SQLite backend
  requires. A fleet wants the Postgres core, which the same code registers on.
- **The Claude model replays no thinking.** The agent adapter's messages carry
  text, tool calls and tool results only, so each turn plans again. The
  [`claude-agent-daemon`](../claude-agent-daemon) example replays thinking
  verbatim.
- **LLM budgets and the cost ledger need Postgres.** `runtime::postgres`
  builds that registration with a budget on `agent_loop`.
- **The response cache index lives in memory.** A restart starts it empty.
  The blobs stay on disk.

See [`DESIGN-autoresearch.md`](../../DESIGN-autoresearch.md) for the plan and
the acceptance criteria.
