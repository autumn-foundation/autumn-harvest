# autoresearch on Harvest

Karpathy's [autoresearch](https://github.com/karpathy/autoresearch) gives an
agent a small LLM training setup. The agent edits `train.py`, trains for five
minutes, keeps the change if `val_bpb` goes down, and repeats all night.

This example runs that loop as **durable Harvest workflows**, on the embedded
[SQLite backend](../../docs/sqlite-backend.md). Each step is a workflow, a
child workflow or an activity. A crash costs at most the step in flight. A
restart replays the night. It does not pay again for a model turn or a
training run that completed.

By default it needs **no GPU, no API key, no network and no Docker**. A
simulated trainer scores `train.py` on a fixed loss landscape, and a scripted
researcher plays the agent. Point it at a real checkout and a key, and the
same workflows run the real thing.

## Try it in one minute

Run these from the repository root:

```bash
cargo run -p autoresearch -- init /tmp/ar-lab
cargo run -p autoresearch -- run --lab /tmp/ar-lab --tag demo --experiments 8 --explore 100
cargo run -p autoresearch -- status --lab /tmp/ar-lab
```

```text
started autoresearch/demo on branch autoresearch/demo
  #0   keep    0.997795   44.0 GB  2a342ca  baseline
  #1   discard 0.997884   49.5 GB  afb841e  much deeper model: 8 -> 12
  #2   crash   0.000000    0.0 GB  24e87d1  ten times MATRIX_LR: 0.04 -> 0.4
  #3   keep    0.995173   44.0 GB  35bff6a  raise MATRIX_LR: 0.04 -> 0.05
  #4   keep    0.993402   44.0 GB  728b5cb  longer warmdown: 0.5 -> 0.7
  #5   keep    0.992512   44.0 GB  11c1f21  halve WEIGHT_DECAY: 0.2 -> 0.1
  #6   keep    0.986595   68.8 GB  d593bb0  deeper model: 8 -> 10
  #7   keep    0.983772   68.8 GB  8c38a11  halve TOTAL_BATCH_SIZE: 2**19 -> 2**18
  #8   discard 0.985371   68.8 GB  f4227e1  full attention: "SSSL" -> "L"
done: autoresearch/demo: 8 experiments, best val_bpb 0.983772 at 8c38a11 (6 kept, 2 discarded, 1 crashed)
report: /tmp/ar-lab/.autoresearch/reports
```

`--explore 100` makes each experiment try its boldest idea first, so the
night is the same each time. Only the commit hashes differ. With the default
of 10, each experiment tries a bold idea by chance, and the night varies.

Experiment 1 ran out of memory. A fix proposal halved the device batch, and
the fixed run lost to the base, so it is a discard. Experiment 2 diverged,
and the fix agent gave up, so the row is a crash. The six kept rows count the
baseline. The branch `autoresearch/demo` holds the baseline and the five
kept commits only. The baseline is an empty start commit, so two sessions in
one lab never share a commit. `results.tsv` and a morning report sit in the
lab.

## How upstream maps to Harvest

| Upstream autoresearch | Harvest primitive |
|---|---|
| `program.md`, which the human edits | The system prompt of each `agent_loop` run. `eval` tests a new one against recorded runs. |
| The agent edits `train.py` only | The `edit_train` tool writes that one file. The policy denies every unknown tool. |
| `prepare.py` is read-only | `read_file` serves a fixed list of files. No tool writes `prepare.py`. |
| The branch `autoresearch/<tag>` must not exist | The session workflow id is `autoresearch/<tag>`, started with `RejectDuplicate`. Setup also refuses a branch that another session started, such as one that `purge` left. |
| Commit, train, then keep or `git reset` | A saga: the commit is a step, and `git reset` is its compensation. |
| Five minutes of training, kill a run at ten | The `train` activity, `start_to_close = "15m"`. The trainer kills the process group at ten minutes. |
| A run over ten minutes is a failure | A crash row. The run gets one fix proposal first, as a crash does. |
| Fix a dumb crash, or log `crash` | One fix proposal as a child `agent_loop`, then a `crash` row. |
| `results.tsv`, untracked | The `record` activity appends one row per experiment, keyed on its idempotency key. Git excludes the file. |
| NEVER STOP | `continue_as_new` every N experiments, so the history stays small. A `stop` signal ends the night. |
| "You wake up to a log of experiments" | A nightly schedule, a detached report agent with the session's own summary, and a heartbeat that speaks only on trouble. |
| Several GPUs, several branches | A workflow mutex per GPU. A kept idea goes to sibling sessions as a signal. |
| Rewind "very very sparingly" | The `rewind` tool waits for a person's approval. It only goes back along the branch. |
| The simplicity criterion | The keep rule charges each added line. A `reviewer` peer reads each diff. |

## The workflows

```text
schedule "0 23 * * *" ─► research_session  (continue-as-new every N)
                          │  signals: hint, stop, policy · query: status · update: whatif
                          │  activity setup · search attributes · memo
                          ├─ mutex lab:<path> ─► experiment (child, one per idea)
                          │     ├─ activity head: check out the session branch
                          │     ├─► agent_loop (child: propose one edit)
                          │     │     read_file, read_paper, show_diff, edit_train, rewind
                          │     │     reviewer peer · memory · loop guard · response cache
                          │     ├─ saga: commit ─► compensation: reset
                          │     ├─ mutex gpu:<id> ─► activity train
                          │     ├─ local activity parse_summary
                          │     └─ activity record (results.tsv)
                          ├─► signal_external_workflow_by_id: sibling sessions
                          └─► detached agent_loop: the morning report
schedule every 30m ─► agent_heartbeat (precheck: a new crash streak) ─► delivery
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

Two sessions can share one lab. The session holds the mutex `lab:<path>` for
each experiment, so the sessions take turns. Each experiment checks out its
own branch first. Each step that writes the lab names that branch and refuses
another one.

## The keep rule

With `gain = best - val_bpb` and `net = lines_added - lines_removed`:

- no `val_bpb` is a `crash`;
- peak memory over `max_memory_gb` (80 GB) is a `discard`;
- `gain - line_cost * net > min_improvement` is a `keep`;
- anything else is a `discard`.

The defaults are `min_improvement = 0` and `line_cost = 0.0001`. So a change
that adds 20 lines needs a gain over 0.002, and an equal result from less code
is a keep. Equal or worse is a discard, as upstream. A proposal that changes
nothing is a discard row with the numbers of its base.

## Steer a running night

`run` and `serve` hold the database as its one writer. The other commands
write a file to the inbox, and the runner turns it into a keyed signal. A
resend after a crash does not deliver twice. A command that can never land,
such as a stop for a session that ended, moves to `inbox/rejected`.

```bash
cargo run -p autoresearch -- hint --lab /tmp/ar-lab --tag demo "paper:warmdown"
cargo run -p autoresearch -- policy --lab /tmp/ar-lab --tag demo --line-cost 0.0002
cargo run -p autoresearch -- stop --lab /tmp/ar-lab --tag demo
```

The commands find a running session by its tag. A nightly session gets a tag
from its start time, such as `20261017-2300`, and `status` prints it. The
year keeps the night of a later year off an old branch.

The heartbeat runs a model call only for a new streak of three crashes. Its
check reads the rows and writes nothing. Only a delivered alert marks the
rows, so a tick that fails leaves them for the next tick.

A hint of the form `paper:<name>` makes the agent read `papers/<name>.md`.
That text comes from outside the lab, so a flow rule makes the next edit wait
for a person. The text can live on in a memory note or an answer line. So
the paper taints the rest of its session: each later edit asks too. A new
session starts clean.

A hint of the form `rewind:<commit>` asks for a rewind, which always waits.

The runner prints each call that waits, and its token. An edit after a paper
prints:

```text
waiting for a person: edit_train {"new":"WARMDOWN_RATIO = 0.7","old":"WARMDOWN_RATIO = 0.5"}
  approve: autoresearch approve --lab /tmp/ar-lab '<run id>/tool_approval:1:0:call_1_0'
  deny:    autoresearch deny --lab /tmp/ar-lab '<run id>/tool_approval:1:0:call_1_0'
```

A wait has a deadline, `--approval-timeout` seconds (900 by default). When it
passes, the call does not run, and the agent reads that no approval came.
The first decision for a wait counts. A second one is not sent.

Kill the runner at any time. Run the same command again, and it resumes the
session where it stopped. With no `--tag`, `run` resumes the one running
session, so a night that crosses midnight resumes too.

## Run it on a GPU

1. Clone [karpathy/autoresearch](https://github.com/karpathy/autoresearch)
   and run `uv run prepare.py` once.
2. Copy this example's [`program.md`](program.md) over the checkout's, and
   commit it. The upstream one tells the agent to run git and the trainer.
   Here the workflows do that, and the agent only edits.
3. Run a night with the real trainer and Claude:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
cargo run -p autoresearch -- run --lab ~/autoresearch --tag oct17 --trainer command \
  --data-dir ~/.cache/autoresearch --model claude --experiments 100
```

`--trainer command` runs `uv run train.py` in the lab and kills its process
group at ten minutes. Read the limits below before you run it.

`--model claude` uses `claude-opus-5-5` with adaptive thinking, `high`
effort, automatic prompt caching and the server-side refusal fallback. The
key comes from the environment only. The agent adapter keeps text and tool
calls, but the API wants each thinking block back. So the model keeps each
raw turn in `.autoresearch/claude-turns` and sends it back unchanged.

`serve` runs the nightly schedule and the heartbeat instead of one night.

## Commands

| Command | What it does |
|---|---|
| `init <dir>` | Create an offline lab with one commit on `master`. |
| `run --lab <dir>` | Start or resume a session and drive it to its end. |
| `serve --lab <dir>` | Run a session at 23:00 UTC each night and a heartbeat every 30 minutes. |
| `status --lab <dir>` | List the sessions and the last rows, with a read-only handle. |
| `hint`, `stop`, `policy` | Queue a signal for the running session with a tag. |
| `approve`, `deny` | Queue a decision for a gated tool call. |
| `eval --candidate <file>` | Re-drive the newest recorded proposal with a new `program.md`. Prints the first turn that differs. |
| `verify` | Replay every stored history with this build. Run it before a deploy. |
| `purge --older-than-days <n>` | Remove old finished runs from the database. |

The database is `<lab>/.autoresearch/harvest.db` unless `--db` names one.
The memory notes, the response cache, the inbox and the reports stay in
`<lab>/.autoresearch`. Only the owner can read that directory.

## Tests

```bash
cargo test -p autoresearch
```

All tests are offline. They need `git` and a Unix shell.

- Unit tests cover the keep rule, `results.tsv`, the summary, both trainers,
  the lab, the tools, the stores and both models. Property tests check the
  keep rule against an oracle. A crash at each idempotent write lands once:
  a row, an edit, a commit and a training run.
- [`tests/simulation.rs`](tests/simulation.rs) sweeps both workflows with the
  seeded simulation of issue #2033. Each seed runs twice, replays, and runs
  again with a crash. The sweeps reorder results, signals and timers, and
  inject faults. Seed-driven mocks take the rewind, no-change, time-out and
  failure paths. Required `sometimes!` targets prove that each path ran.
  Widen a sweep with `HARVEST_SIM_SEEDS=10000`.
- [`tests/e2e.rs`](tests/e2e.rs) runs each acceptance criterion on SQLite with
  a real git lab. It covers the restart proof, approvals and the taint, and
  the signals, the query and the update. It covers two sessions on one GPU
  and in one lab, and continue-as-new. It covers the schedules, commands by
  tag, `eval`, the response cache, memory, the inbox, retention and the
  Postgres registration.
- [`tests/cli.rs`](tests/cli.rs) runs the binary as a reader does.

The simulation found a real bug while this example was written: the
experiment set the search attribute `status`, which the engine reserves.

## Limits

- **The simulated trainer is not a GPT.** It scores the hyperparameter block
  on a fixed landscape, so the loop is testable offline.
- **The scripted researcher is not Claude.** It tries a fixed list of ideas.
- **The command trainer is not a sandbox.** It runs code that the model wrote,
  as your user. Its environment is a fixed list. The list holds the shell
  basics, the locale, and the CUDA, PyTorch, Python, uv and Hugging Face
  settings. A name that looks like a secret stays out, and so does a URL
  with login details, such as a private package index. Pass another with
  `--pass-env NAME`. On Linux the runner is not dumpable, so a run cannot
  read the Claude key from the runner. A shell that exported the key can
  still leak it, so set the key for the runner alone:
  `ANTHROPIC_API_KEY=... autoresearch run ...`. Git runs with hooks off. The code can still read your
  files, write `.git` and `.autoresearch`, and escape the kill with
  `setsid`. Run it in a container, or as a user with no other rights.
- **The score is what `train.py` prints.** As upstream, the last `val_bpb:`
  line counts. An edit can print a false score. Read each kept diff.
- **Activity inputs are trusted.** The lab path and the trainer command come
  from the workflow input. On Postgres, let only people you trust with a
  shell on the GPU host start a session.
- **One writer.** One process drives the database, as the SQLite backend
  requires. `eval`, `verify` and `purge` need the writer lock, so stop `run`
  or `serve` first. `status` reads beside them.
- **The GPU mutex lives in one database.** The CLI serves one lab per process
  and per database. So two labs on one GPU take turns only when one process
  serves both, as `tests/e2e.rs` does, or on Postgres.
- **On SQLite, one activity runs at a time.** The GPU mutex orders the grants
  there. Its exclusion matters on a Postgres fleet.
- **A taint covers one session.** Rows in `results.tsv` stay visible to
  later sessions in the lab.
- **LLM budgets and the cost ledger need Postgres.** `runtime::postgres`
  builds that registration with a budget on `agent_loop`.
- **`purge` cleans the database only.** The response cache, the memory notes,
  the reports, the saved runs and the Claude turns stay in `.autoresearch`.
  Delete `.autoresearch/cache` when no process runs.
- **The response cache index lives in memory.** A restart starts it empty.
  The blobs stay on disk.
- **A runner killed as its run starts leaves the run alive.** The lock file
  then names no group, so the next run cannot kill it. That run waits ten
  seconds and fails, and the activity retries. Stop the old run by hand,
  or let it end at its own time budget.
- **A heartbeat alert holds the rows at its delivery.** Crash rows that land
  while the alert is written get no alert of their own. The next crash
  alerts again.

See [`DESIGN-autoresearch.md`](../../DESIGN-autoresearch.md) for the plan and
the acceptance criteria.
