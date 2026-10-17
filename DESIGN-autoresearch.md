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
| B5 | A workflow mutex per GPU around training. | **Adopted.** Two sessions in one database do not train on one GPU at once. |
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
| B17 | Search attributes and a memo on each session. Commands find a session by its `tag` attribute. | **Adopted.** A nightly session has an id that the engine picks. |
| B18 | Run training in a sandbox activity. | Rejected. The sandbox needs KVM and has no GPU. |
| B19 | Run on Postgres in CI. | Rejected. It needs Docker. A test builds the Postgres registration instead. |
| B20 | Use a DAG for one night. | Rejected. SQLite has no DAGs, and the loop is not a static graph. |
| B21 | A follow-up timer on the agent. | Rejected. The session loop already owns the cadence. |

### 0.3 Reverse brainstorm — how can this example do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A restart trains the same commit again, or pays for a model turn again. | Each step is an activity or a child. A restart test counts model calls and training runs in each process. |
| R2 | A discard leaves the bad commit on the branch. | The saga compensation resets to the base commit. A sweep checks that each non-keep row has a reset. |
| R3 | A row lands twice in `results.tsv` after a crash, or an equal row of another experiment is lost. | The record activity keys each row on its idempotency key. Tests crash it before and after the write. |
| R4 | The agent edits `prepare.py` or another file. | The edit tool writes `train.py` only. Unknown tools are denied. A test calls the tool with other paths. |
| R5 | Text in a paper plants code that runs on the GPU host. | A flow rule gates an edit after `read_paper`. The labels follow the session, so a later run that reads the text through memory or a row asks too. Tests wait on both approvals. |
| R6 | Two sessions train on one GPU at once. | A workflow mutex per GPU, in one database. A test runs two sessions on one key. |
| R7 | A crashed run reads as a kept one. | A crash row has `0.000000` and `0.0`. A run past its time budget is a crash. A sweep checks both. |
| R8 | The workflow reads the clock or the environment, so replay diverges. | The macro lint refuses it. Each sweep seed replays. Each SQLite history replays on `WorkflowReplayer`. |
| R9 | An update tries to change the policy and silently does nothing on SQLite. | The policy is a signal. The update only answers a what-if. |
| R10 | A stale approval releases a later rewind. | Each wait has its own signal name. The test approves a token after its deadline and expects no rewind. |
| R11 | The history of the session grows without bound. | `continue_as_new` every N experiments. A test checks the carry. |
| R12 | The example rots. | CI runs its tests, its sweeps, its clippy and a demo run of the binary. |
| R13 | A test needs a GPU, Python or the network. | The simulated trainer and the scripted model are the defaults. Tests use a temporary git repository. |
| R14 | The command trainer hangs. | The trainer kills the process group at ten minutes. `start_to_close` is 15 minutes. |
| R15 | The sim proves nothing. | Each sweep requires its `sometimes!` targets. A mutation run checks that a broken session fails a sweep. |
| R16 | Two sessions share one lab, and one commits or resets on the branch of the other. | A mutex per lab for each experiment. Each write step names its branch and refuses another one. |
| R17 | A training run outlives its runner and shares the GPU with the next run. | The process group dies on every exit path. A lock file finds a group that a dead runner left, and the next run kills it. A free lock clears the old group id, so a reused id is never killed. |
| R18 | A tool call or an activity runs again after a crash and writes twice. | Each write is keyed: a commit by its trailer, an edit by its tool call, a row and a run by the activity idempotency key. |
| R19 | Code that the model wrote reads a secret or plants a git hook. | The run gets a fixed list of environment variables, and more only by name. Git runs with hooks off. The README says that the command trainer is not a sandbox. |
| R22 | A heartbeat tick fails, and its crash rows are never checked again. | The precheck reads and writes nothing. Only a delivered alert marks the rows. |
| R23 | The morning report counts the rows of another session in the lab. | The report reads the session's own summary, not `results.tsv`. |
| R24 | A nightly tag repeats a year later and reuses the old branch. | A derived tag holds the year. |
| R20 | `hint` or `stop` misses a nightly session, and the command is lost. | Commands find a session by tag. A command that can never land moves to `inbox/rejected`. |
| R21 | The Claude model drops thinking blocks in a tool loop. | The model keeps each raw turn by hash and sends it back unchanged. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. SQLite now has children and external
  commands. The sim mocks children. The agent crate registers on SQLite.
- **Red (feelings).** A reader wants one command that runs a night of
  research offline. A reader wants to see the engine save them when the
  process dies.
- **Black (risks).** Scope creep: the surface is large. Git and file I/O in
  tests can be slow or flaky. The scripted model can hide bugs that a real
  model shows. Many CI gates apply to a new crate.
- **Yellow (benefits).** One example shows many engine primitives: children,
  sagas, mutexes, signals, queries, updates, schedules, continue-as-new and
  search attributes. It shows the agent mesh, approvals, flow rules, memory,
  delivery, heartbeats, the response cache, eval and the new DST. It also
  maps to a story that many readers know.
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
                          ├─ mutex lab:<path> ─► experiment (child, one per idea)
                          │     ├─ activity head: check out the session branch
                          │     ├─► agent_loop (grandchild: propose an edit)
                          │     │     tools: read_file, read_paper, show_diff, edit_train, rewind
                          │     │     peer: reviewer · memory · loop guard · response cache
                          │     ├─ saga: commit ─► compensation: reset
                          │     ├─ mutex gpu:<id> ─► activity train (start_to_close 15m)
                          │     ├─ local activity parse_summary
                          │     └─ activity record (results.tsv)
                          ├─► signal_external_workflow_by_id: sibling sessions
                          └─► detached agent_loop: the morning report
schedule every 30m ─► agent_heartbeat (precheck: a new crash streak) ─► delivery
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
| AC4 | A crash gets one fix proposal and one retry. A run over its time budget is a crash, even with a summary. |
| AC5 | The agent edits `train.py` only. `prepare.py` and other paths are refused. |
| AC6 | An edit after `read_paper` waits for approval, and so does each later edit of the session. A `rewind` waits for approval. A deny or a timeout runs nothing. |
| AC7 | The proposal agent delegates to a `reviewer` peer under a mandate. |
| AC8 | The agent keeps notes in a durable memory scope per tag. A restart keeps them. |
| AC9 | Signals `hint`, `stop` and `policy` steer a live session. Query `status` reads it. Update `whatif` answers a what-if with a validator. |
| AC10 | A workflow mutex per GPU orders the training of the sessions in one database. |
| AC11 | The session continues as new every N experiments and carries the best result, the count and the pending hints. |
| AC12 | A kept idea reaches sibling sessions through an external signal by workflow id. An unknown sibling does not fail the session. |
| AC13 | Each session has search attributes (`tag`, `phase`, `experiments`, `best_bpb`, `best_commit`) and a memo. `status` lists the sessions with their tags. |
| AC14 | A nightly schedule starts a session. A heartbeat agent with a precheck delivers a report only when it has something to say. |
| AC15 | A detached agent child writes the morning report of the session, from the session's own summary, when it ends. |
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
| AC26 | The CLI: `init`, `run`, `serve`, `status`, `hint`, `stop`, `policy`, `approve`, `deny`, `eval`, `verify`, `purge`. A demo runs offline in one command. |
| AC27 | CI runs the tests, the sweeps, clippy and the demo, so the example cannot rot. |
| AC28 | Comments and docs are short and in ASD-STE100 style. The comment-hygiene gate passes. |
| AC29 | Two sessions in one lab take turns. Each experiment commits and resets on its own branch. |
| AC30 | Commands find a running session by its tag, a nightly session included. `run` with no tag resumes the one running session. |
| AC31 | Each write runs once across a crash: a commit per key, an edit per tool call, a row per record call, a training per train call. |
| AC32 | A training run never outlives its runner. Its process group dies at the end, at the timeout and on a drop. The next run kills a group that a dead runner left. |
| AC33 | The Claude model sends each thinking block back unchanged, and redacts its key. |

---

## 3. Limits

- Training runs on the host, not in a sandbox. The sandbox has no GPU.
  The command trainer runs code that the model wrote, as the user.
- The simulated trainer is a model of a loss landscape, not a model of a
  GPT. It exists so the loop is testable offline.
- The scripted researcher is not Claude. It tries a fixed list of ideas.
- One writer process owns the database, as the SQLite backend requires.
  On SQLite one activity runs at a time, so the GPU mutex orders grants.
  Its exclusion matters on a Postgres fleet.
- The cost ledger and LLM budgets need Postgres. AC24 builds that
  registration but does not run it.
- Activity inputs are trusted. Whoever can start a session picks the lab
  path and the trainer command.

The README lists the rest.

---

## 4. Test plan

| Layer | Where | What it proves |
|---|---|---|
| Unit and property | `src/*.rs` | The pure parts: the keep rule, the TSV, the summary, the trainers, the lab, the tools, the stores, both models. Each idempotent write is crashed before and after its write. |
| Simulation | `tests/simulation.rs` | Both workflows under seeded orders, faults, crashes, signals and replay. Seed-driven mocks take every path. Each target is required. |
| SQLite end to end | `tests/e2e.rs` | Each AC with a real git lab, the real activities and the scripted model. |
| CLI | `tests/cli.rs` | The binary as a reader runs it. |
| CI | `ci.yml`, `dst-nightly.yml` | The suite, clippy, the README demo and `verify` on each PR. 5000 seeds per sweep each night. |

---

## 5. Acceptance criteria evidence

Test names are in `examples/autoresearch`. `e2e::` is `tests/e2e.rs`,
`sim::` is `tests/simulation.rs`, and `cli::` is `tests/cli.rs`.

| AC | Evidence |
|----|----------|
| AC1 | `e2e::a_night_of_research_keeps_only_improvements`: setup, a baseline and six experiments, seven rows, the upstream header. `cli::a_night_runs_offline_from_one_command`. |
| AC2 | The same test: `HEAD` is the best commit, the branch holds only the kept commits, the tree is clean. `sim::an_experiment_holds_its_invariants_on_every_seed`: a discard or a crash resets to the base last. |
| AC3 | `rules::tests` (seven cases and three property tests, one against an oracle from the README). `e2e::a_crash_gets_one_fix_and_a_broken_idea_logs_a_crash_row` checks the zero crash row. |
| AC4 | `e2e::a_crash_gets_one_fix_and_a_broken_idea_logs_a_crash_row`. The sim targets "a fix recovers a crash", "a run crashes for good" and "a run passes its time budget". |
| AC5 | `agent::tests::edit_train_writes_train_py_only`, `the_policy_gates_rewind_and_denies_unknown_tools`, `lab::tests::the_agent_reads_only_known_files_and_papers`. |
| AC6 | `e2e::an_edit_after_a_paper_waits_and_a_deny_changes_nothing`, `e2e::a_paper_taints_every_later_run_of_the_session`, `e2e::an_approved_rewind_moves_the_base_to_an_older_kept_commit`, `lab::tests::a_short_id_names_one_commit_of_the_branch`, `kept_refuses_a_row_whose_id_names_two_commits`, `e2e::a_late_approval_never_releases_the_call`. |
| AC7 | `scripted::tests::the_researcher_reads_edits_asks_the_reviewer_and_answers`, `agent::tests::the_reviewer_is_read_only_with_two_tools`. The e2e nights delegate in every proposal. |
| AC8 | `e2e::memory_notes_survive_a_restart`, `stores::tests::memory_survives_a_new_store_and_dedupes_a_key`. |
| AC9 | `e2e::signals_a_query_and_an_update_steer_a_live_session`. `sim::a_session_holds_its_invariants_under_signals_and_faults` requires the hint, policy and stop targets. `sim::a_stop_at_the_hand_off_ends_the_night` requires a stop that meets the hand-off. |
| AC10 | `e2e::two_sessions_share_one_gpu_and_a_kept_idea`: every training takes `gpu:gpu0`, and each grant has its own fencing token. The sim checks one grant per training. |
| AC11 | `e2e::the_session_continues_as_new_and_carries_its_best`: three runs, the carried count and best. `sim::a_session_hands_off_its_carry_past_a_failed_experiment`: the count, the best, each hint and the labels. `sim::a_continued_session_finishes_from_its_carry`. |
| AC12 | `e2e::two_sessions_share_one_gpu_and_a_kept_idea`: the hint reaches B, and the unknown sibling fails only its own signal. |
| AC13 | `e2e::a_night_of_research_keeps_only_improvements`: the attributes, the memo, and a list by the `tag` attribute. |
| AC14 | `e2e::the_nightly_schedule_starts_a_session_and_the_heartbeat_speaks_up`. `stores::tests::the_precheck_runs_a_tick_only_for_a_new_crash_streak`, `a_redelivered_alert_marks_its_rows`. |
| AC15 | `e2e::a_finished_night_leaves_a_morning_report`, `scripted::tests::the_reporter_reports_the_summary_of_its_own_session`. |
| AC16 | `e2e::a_restart_resumes_without_paying_for_a_turn_or_a_run_twice`: the model calls across two processes equal those of one process, and each row has one `train` activity. `activities::tests::a_train_call_that_runs_again_returns_its_saved_run`: a train call that runs again does not train again. |
| AC17 | `e2e::a_night_of_research_keeps_only_improvements` and `e2e::the_session_continues_as_new_and_carries_its_best` run `verify`. The CI demo runs `verify`. |
| AC18 | `sim::*`: nine sweeps. Each seed runs twice, replays, and runs again with a crash. Each target is required. The mocks give full commit ids, so a sweep sees a short id where a full one belongs. |
| AC19 | `e2e::eval_names_the_first_turn_where_a_new_program_differs`, `cli::eval_and_purge_work_on_a_finished_night`. |
| AC20 | `e2e::the_response_cache_serves_an_identical_report_turn`. |
| AC21 | `e2e::the_inbox_delivers_a_command_once`, `cli::inbox_commands_queue_for_the_runner`, `inbox::tests`. |
| AC22 | `e2e::the_command_trainer_runs_a_real_command_per_commit`, `trainer::tests::the_command_trainer_*`. |
| AC23 | `claude::tests` (nine cases). |
| AC24 | `e2e::the_workflows_register_on_a_postgres_builder_with_an_llm_budget`. |
| AC25 | `e2e::purge_removes_old_finished_runs`, `cli::eval_and_purge_work_on_a_finished_night`: the tag of a purged night stays used. |
| AC26 | `cli::*` and the CI demo step. |
| AC27 | `ci.yml`: the test step, the demo step and the clippy step. `dst-nightly.yml`: the `autoresearch` job. |
| AC28 | `docs/audits/comment-hygiene.py --base origin/trunk-dev` passes. |
| AC29 | `e2e::two_sessions_in_one_lab_take_turns_on_their_own_branches`, `lab::tests::checkout_moves_back_to_the_session_branch_only_when_clean`, `two_sessions_never_share_a_commit`, `a_tag_that_another_session_used_is_refused`, `activities::tests::train_refuses_a_moved_branch_and_a_change_no_commit_holds`, `setup_refuses_a_tag_that_another_session_used`. |
| AC30 | `e2e::a_nightly_session_takes_commands_by_its_tag`, `e2e::a_nightly_session_that_waits_for_its_lab_takes_commands_by_its_tag`. |
| AC31 | `lab::tests::a_commit_is_made_once_per_key`, `an_edit_runs_once_per_call`, `an_edit_whose_write_was_lost_runs_on_its_retry`, `a_row_is_recorded_once_per_key`, `a_row_whose_write_was_lost_lands_on_its_retry`. `activities::tests::a_train_call_that_runs_again_returns_its_saved_run`. `agent::tests::an_edit_call_that_runs_again_changes_nothing`. |
| AC32 | `trainer::tests::a_dropped_run_kills_its_whole_group`, `the_next_run_kills_a_run_that_outlived_its_runner`, `a_timeout_kills_the_children_of_the_run_too`, `a_finished_run_names_no_group`, `a_free_lock_forgets_the_group_of_an_earlier_run`. |
| AC33 | `claude::tests::a_thinking_block_goes_back_unchanged_in_the_next_request`, `a_turn_survives_in_the_turn_directory`, `the_key_stays_out_of_debug_output`. |

---

## 6. Review record

Five agents reviewed the first green build from five angles: durability,
security, tests and CI, API fidelity and docs, and code quality. Codex
reviewed the PR. A workflow then tried to refute each finding against the
reviewed commit, with probes. Most findings held. One did not: a train
call that runs again after its result is lost has a window of under a
millisecond on SQLite. The saved run stays, because it is cheap and it
covers the completion call on a Postgres worker.

The fixes:

- **Durability.** Rows keyed on the activity idempotency key. Edits keyed on
  the tool call. A saved run per train call. The process group dies with the
  run. A mutex per lab and a branch on each write step. A `train.py` change
  that no commit holds is refused. No execution timeout on `experiment`,
  because it counted the downtime of the runner. Workflow state keeps full
  commit ids. A short id can become ambiguous as the lab grows, so only
  people and the model see it: `results.tsv`, the console, the prompts,
  the hints, the report and the `best_commit` attribute. A rewind resolves a short id on
  the session branch only. A row of `results.tsv` whose short id names two
  commits is refused, so a lookup never gives the score of the wrong
  commit. A trainer run clears its group id from the lock file when it
  ends, and a free lock clears an old one. A
  redelivered heartbeat alert marks its rows. Each session starts with an
  empty commit that holds the key of its setup. So two sessions in one lab
  never share a commit, and setup refuses a branch that another session
  started, such as one that `purge` left. A scheduled session publishes its
  tag before it waits for its lab, so commands reach it from the start.
  A trainer whose run cannot end inside the budget of a `train` activity
  is refused at start, at the schedule and in the session.
- **Steering.** Commands by tag, `run` resumes across midnight, rejected
  commands kept, one decision per approval wait.
- **Security.** Flow labels follow the session. No secrets in the trainer
  environment. Git hooks off. Symlinks refused and reads capped. The state
  directory is private. Model text loses control and bidirectional
  characters in `results.tsv`, the console and each commit message. A URL
  with login details stays out of the trainer environment. The Claude key
  is redacted.
- **Claude client.** Thinking blocks go back unchanged. A timeout, a body
  cap, automatic caching, and retries on 408 and 409.
- **Tests.** The tests review ran mutants. Several survived every test: a
  stop lost at the hand-off, a crash that moved the base, a lost carried
  best, and lost hints. The sweeps now check the base, the carry, the
  hints, the stop, the policy and the taint on every seed. A no-stop sweep
  reaches the hand-off and carries a hint. A sweep with one experiment per
  run lands a stop at the hand-off. Seed-driven mocks take the rewind, no-change,
  time-out and failure paths. The review also found a product bug: a run
  past its budget was scored. It is a crash now.

One mutant cannot fail a test on SQLite: a GPU mutex released before the
training. The release writes no event, and one activity runs at a time.
Section 3 states it.

Two findings stay as limits in the README, because a full fix costs more
than it buys:

- A runner killed between the start of its run and the write of the group
  leaves a run that the next run cannot kill. No await sits in that window,
  so only an outside kill can hit it. The next run fails safe: it waits and
  retries, and never shares the GPU.
- A heartbeat alert marks the rows at its delivery. Crash rows that land
  while the alert is written get no alert of their own, and the next crash
  alerts again. An exact mark needs the rows that each tick read.
