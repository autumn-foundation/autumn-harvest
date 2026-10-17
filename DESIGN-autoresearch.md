# Design — autoresearch on Harvest: a capstone example

Karpathy's [autoresearch](https://github.com/karpathy/autoresearch) lets an
agent edit `train.py`, train for five minutes, keep or discard the change,
and repeat all night. This example runs that loop as durable Harvest
workflows. It uses the agent primitives and as much of the engine surface as
fits the problem. It runs on the embedded SQLite backend, offline, with no
GPU, no API key and no Docker.

The crate is `examples/autoresearch`. The guide is its `README.md`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Upstream has three files that matter. `prepare.py` is fixed. `train.py` is
  the one file that the agent edits. `program.md` is the instructions that
  the human edits.
- Upstream setup: agree a run tag, create the branch `autoresearch/<tag>`,
  read the files, check the data, start `results.tsv`, run a baseline.
- Upstream loop: edit, commit, train, read `val_bpb` and `peak_vram_mb`, log
  a row, keep the commit on a lower `val_bpb`, else `git reset`. A run over
  ten minutes is a failure. A crash gets a fix attempt or a `crash` row. The
  loop never stops on its own.
- `results.tsv` has five tab-separated columns: `commit`, `val_bpb`,
  `memory_gb`, `status` and `description`. It stays untracked.
- The simplicity criterion weighs the gain against the lines added.
- Several GPUs run several branches, for example `autoresearch/mar5-gpu0`.
- PR #2135 gives the SQLite backend child workflows, detached children, and
  signal, cancel and await between runs. A child cannot continue as new.
- PR #2179 gives `WorkflowTestEnv::simulation()`: seeded delivery orders,
  `always!`, `sometimes!`, faults, crash and resume, and a replay check.
  It mocks children and activities. It sends no query and no update.
- On SQLite, an update handler cannot change the state of the body.
  `ctx.state::<T>()` is `None`. `heartbeat_timeout` is refused.
- `autumn-harvest-agent` runs `agent_loop` as a workflow. A tool keys its
  idempotency on `(run_id, step, call_id)`. The run id is the execution id.
  So two loops inline in one run share a run id. A child loop does not.
- The workflow macro refuses the wall clock, `rand`, `std::env` and sleeps
  in a workflow body.
- `harvest-verify` classifies each `ctx.<method>(` call in `examples/*/src`.

### 0.2 Brainstorm — how can Harvest run autoresearch?

| # | Idea | Verdict |
|---|------|---------|
| B1 | One workflow per session. The loop runs `agent_loop` inline for each experiment. | Rejected. Inline loops share a run id, so tool keys and memory keys collide. |
| B2 | A session parent. Each experiment is a child. Each proposal is an `agent_loop` grandchild. | **Adopted.** Each agent run gets its own run id. Each experiment has its own history. |
| B3 | Loop forever with `continue_as_new` every N experiments. Carry the best result. | **Adopted.** It bounds the history of the parent. |
| B4 | Commit as a saga step. A discard or a crash runs the compensation: `git reset`. | **Adopted.** It is the upstream keep-or-reset rule. |
| B5 | A workflow mutex per GPU around training. | **Adopted.** Two sessions on one GPU do not train at once. |
| B6 | A simulated trainer with a known loss landscape, and a command trainer for a real GPU. | **Adopted.** CI runs the simulated one. A GPU host runs `uv run train.py`. |
| B7 | A scripted researcher model offline, and a Claude model with an API key. | **Adopted.** The scripted model makes every test deterministic. |
| B8 | A `reviewer` peer that judges a diff against the simplicity criterion. | **Adopted.** It uses the mesh with a mandate. |
| B9 | A `read_paper` tool labelled `untrusted`, and a flow rule: an edit after it needs approval. | **Adopted.** Untrusted text must not steer code that runs on the GPU host unseen. |
| B10 | A `rewind` tool that needs approval. | **Adopted.** Upstream says to rewind "very very sparingly". |
| B11 | Hints, stop and policy as signals. Status as a query. A what-if check as an update. | **Adopted.** It respects the SQLite update rule. |
| B12 | Sibling sessions share a kept idea through an external signal by workflow id. | **Adopted.** It is "more agents in the mix". |
| B13 | A nightly schedule starts a session. A heartbeat agent watches the results. | **Adopted.** |
| B14 | A detached agent child writes the morning report when a session ends. | **Adopted.** The session ends without a wait for the report. |
| B15 | `eval::evaluate` re-drives a recorded proposal with a candidate `program.md`. | **Adopted.** The human programs `program.md`, so the human needs a regression check. |
| B16 | A file inbox for `hint`, `stop`, `approve` and `deny` while a runner holds the writer lock. | **Adopted.** Keyed signals dedupe a resend after a crash. |
| B17 | Search attributes and a memo on each session, and `list_executions` by tag. | **Adopted.** |
| B18 | Run training in a sandbox activity. | Rejected. The sandbox needs KVM and has no GPU. |
| B19 | Run on Postgres in CI. | Rejected. It needs Docker. A test builds the Postgres registration instead. |
| B20 | Use a DAG for one night. | Rejected. SQLite has no DAGs, and the loop is not a static graph. |
| B21 | A follow-up timer on the agent. | Rejected. The session loop already owns the cadence. |

### 0.3 Reverse brainstorm — how can this example do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A restart trains the same commit again, or pays for a model turn again. | Each step is an activity or a child. A restart test counts model calls and training runs in each process. |
| R2 | A discard leaves the bad commit on the branch. | The saga compensation resets to the base commit. A sweep checks that each non-keep row has a reset. |
| R3 | A row lands twice in `results.tsv` after a crash. | The record activity checks for the row before it appends. A test runs it twice. |
| R4 | The agent edits `prepare.py` or another file. | The edit tool writes `train.py` only. Unknown tools are denied. A test calls the tool with other paths. |
| R5 | Text in a paper plants code that runs on the GPU host. | A flow rule gates an edit after `read_paper`. A test waits on that approval. |
| R6 | Two sessions train on one GPU at once. | A workflow mutex per GPU. A test runs two sessions on one key. |
| R7 | A crashed run reads as a kept one. | A crash row has `0.000000` and `0.0`. A sweep checks it. |
| R8 | The workflow reads the clock or the environment, so replay diverges. | The macro lint refuses it. Each sweep seed replays. Each SQLite history replays on `WorkflowReplayer`. |
| R9 | An update tries to change the policy and silently does nothing on SQLite. | The policy is a signal. The update only answers a what-if. |
| R10 | A stale approval releases a later rewind. | Each wait has its own signal name. The test approves a token after its deadline and expects no rewind. |
| R11 | The history of the session grows without bound. | `continue_as_new` every N experiments. A test checks the carry. |
| R12 | The example rots. | CI runs its tests, its sweeps, its clippy and a demo run of the binary. |
| R13 | A test needs a GPU, Python or the network. | The simulated trainer and the scripted model are the defaults. Tests use a temporary git repository. |
| R14 | The command trainer hangs. | `start_to_close` is ten minutes. The trainer kills the child process at its timeout. |
| R15 | The sim proves nothing. | Each sweep names `sometimes!` targets for keep, discard, crash and fix. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. SQLite now has children and external
  commands. The sim mocks children. The agent crate registers on SQLite.
- **Red (feelings).** A reader wants one command that runs a night of
  research offline. A reader wants to see the engine save them when the
  process dies.
- **Black (risks).** Scope creep: the surface is large. Git and file I/O in
  tests can be slow or flaky. The scripted model can hide bugs that a real
  model shows. Many CI gates apply to a new crate.
- **Yellow (benefits).** One example shows children, sagas, mutexes,
  signals, queries, updates, schedules, continue-as-new, search attributes,
  the agent mesh, approvals, flow rules, memory, delivery, heartbeats, the
  response cache, eval and the new DST. It also maps to a story that many
  readers know.
- **Green (ideas).** A sibling-session hint channel. A file inbox. A what-if
  update. A morning report as a detached child.
- **Blue (process).** Red, then green, then refactor. Pure types and rules
  first. Then activities, then workflows under the sim, then SQLite
  end-to-end tests, then the CLI, docs and CI. Then a review from several
  angles.

---

## 1. Architecture

```text
schedule "0 23 * * *" ─► research_session  (parent, continue-as-new every N)
                          │  signals: hint, stop, policy · query: status · update: whatif
                          │  activity setup · search attributes · memo
                          ├─► experiment (child, one per idea)
                          │     ├─► agent_loop (grandchild: propose an edit)
                          │     │     tools: read_file, read_paper, show_diff, edit_train, rewind
                          │     │     peer: reviewer · memory · loop guard · response cache
                          │     ├─ saga: commit ─► compensation: reset
                          │     ├─ mutex gpu:<id> ─► activity train (start_to_close 10m)
                          │     ├─ local activity parse_summary
                          │     └─ activity record (results.tsv)
                          ├─► signal_external_workflow_by_id: sibling sessions
                          └─► detached agent_loop: the morning report
schedule every 30m ─► agent_heartbeat (precheck: new rows) ─► delivery
```

The keep rule is one pure function. With `gain = best - val_bpb` and
`net = lines_added - lines_removed`:

- no `val_bpb`: `crash`;
- `memory_gb > max_memory_gb`: `discard`;
- `gain - line_cost * net > min_improvement`: `keep`;
- else `discard`.

A baseline that does not crash is always `keep`. The defaults are
`min_improvement = 0` and `line_cost = 0.0001`, so 20 added lines need a gain
over 0.002. Equal or worse is a discard, as upstream.

---

## 2. Acceptance criteria

| AC | Criterion |
|----|-----------|
| AC1 | A session runs setup, a baseline and N experiments, and completes. `results.tsv` has one row per experiment in the upstream format. |
| AC2 | The branch head is the best kept commit. Each discard and crash resets to the base commit. |
| AC3 | The keep rule: lower `val_bpb` keeps, equal or worse discards, a crash logs `0.000000` and `0.0`, the line cost and the VRAM cap apply. Property tests cover it. |
| AC4 | A crash gets one fix proposal and one retry. A run over its time budget is a crash. |
| AC5 | The agent edits `train.py` only. `prepare.py` and other paths are refused. |
| AC6 | An edit after `read_paper` waits for approval. A `rewind` waits for approval. A deny or a timeout runs nothing. |
| AC7 | The proposal agent delegates to a `reviewer` peer under a mandate. |
| AC8 | The agent keeps notes in a durable memory scope per tag. A restart keeps them. |
| AC9 | Signals `hint`, `stop` and `policy` steer a live session. Query `status` reads it. Update `whatif` answers a what-if with a validator. |
| AC10 | A workflow mutex per GPU serialises training across sessions. |
| AC11 | The session continues as new every N experiments and carries the best result, the count and the pending hints. |
| AC12 | A kept idea reaches sibling sessions through an external signal by workflow id. An unknown sibling does not fail the session. |
| AC13 | Each session has search attributes (tag, best, count, phase) and a memo. `status` lists sessions by tag. |
| AC14 | A nightly schedule starts a session. A heartbeat agent with a precheck delivers a report only when it has something to say. |
| AC15 | A detached agent child writes the morning report when a session ends. |
| AC16 | A restart mid-session resumes by replay. No completed model call or training run runs again. |
| AC17 | Every SQLite history of the example replays on the core `WorkflowReplayer`. |
| AC18 | DST: seeded sweeps of `experiment` and `research_session` hold the invariants under reordering, faults, crashes, signals and replay. Each `sometimes!` target is reached. |
| AC19 | `eval` re-drives a recorded proposal with a candidate `program.md` and reports the first divergence. |
| AC20 | The response cache serves an identical model call across runs. |
| AC21 | A file inbox carries `hint`, `stop`, `approve` and `deny` to a running session. A resend after a crash is deduped. |
| AC22 | The command trainer runs a real `train.py` command, kills it at the timeout, and parses the summary. A test uses a fake command. |
| AC23 | A Claude model implements `AgentModel` over the Messages API. Offline tests cover the request and the response mapping. |
| AC24 | The same workflows and activities build a Postgres `HarvestBuilder` registration, with an LLM budget on `agent_loop`. |
| AC25 | `retention`: `purge` removes old terminal sessions. |
| AC26 | The CLI: `init`, `run`, `serve`, `status`, `hint`, `stop`, `approve`, `deny`, `eval`, `purge`. A demo runs offline in one command. |
| AC27 | CI runs the tests, the sweeps, clippy and the demo, so the example cannot rot. |
| AC28 | Comments and docs are short and in ASD-STE100 style. The comment-hygiene gate passes. |

---

## 3. Limits

- Training runs on the host, not in a sandbox. The sandbox has no GPU.
- The simulated trainer is a model of a loss landscape, not a model of a
  GPT. It exists so the loop is testable offline.
- The scripted researcher is not Claude. It tries a fixed list of ideas.
- One writer process owns the database, as the SQLite backend requires.
- The cost ledger and LLM budgets need Postgres. AC24 builds that
  registration but does not run it.

---

## 4. Test plan

The evidence table for each AC is in section 5 once the work lands.
