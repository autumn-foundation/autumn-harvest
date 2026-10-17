# Design — Issue #2030: cross-backend conformance suite

No test runs one workflow on both backends and compares the results. The
cross-backend test replays a SQLite history on the core replayer. That
proves the encoding, not the behaviour. A gap such as #2020 passed every
test.

This change adds a corpus of workflows. Each scenario runs on Postgres and on
SQLite. The suite asserts the same outcome and replay-equivalent histories.

**No runtime change. No migration. No new `WorkflowEvent` variant.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `WorkflowCommand` has 27 variants (`context.rs`). SQLite accepts 24 of
  them. It rejects `ScheduleExternalActivity` by name. No context method
  emits `Complete` or `Fail`. The executor turns `ContinueAsNew` into an
  outcome.
- SQLite also rejects four fields by name: `ScheduleActivity.session_id`,
  `.schedule_to_start_override`, `.result_writer` and
  `ContinueAsNew.new_workflow_type`. Each seals the run `FAILED` with
  `error_type = "UnsupportedFeature"` and `details.feature`.
- SQLite rejects two batch shapes by name: `AcquireMutex` beside another
  awaitable, and `RunLocalActivity` beside a durable awaitable.
- The SQLite terminal drain has no arm for `ScheduleActivity`, `StartTimer`
  and `WaitForSignal`. A last cycle that starts one and returns without an
  await is rejected by the command name.
- `WaitForActivity`, `WaitForSignal`, `SetCurrentDetails`, `PublishProgress`,
  `PublishDurableProgress` and `RecordLog` write no event on either backend.
  `UpsertSearchAttributes` and `ReleaseMutex` write a side table only.
  `CancelRaceLosers` writes a failure event only for a loser that it cancels.
- `runtime::command_name` in SQLite matches every variant, but nothing lists
  which variants a test covers.
- Postgres writes `ActivityStarted` when a worker claims an activity. SQLite
  does not. The core matcher skips it.
- Postgres has no test clock. Timers fire on the database clock, with a
  one-second floor for `ctx.timer`.
- Postgres ingests every pending signal at task preparation. A signal sent
  before the worker starts lands right after `WorkflowStarted`.
- The fix for #2020 is `92add6f`. Its parent is the "trunk-dev today" that
  the issue names.
- `autumn-harvest-sqlite` builds the core without `db`. A dev-dependency
  with `db` unifies features, so every SQLite test would build Diesel.
- The `test-db-linux` job runs each `linux` row of
  `.github/ci/integration-suites.txt`. It runs for every PR that changes a
  non-docs file. `ci_run_coverage` rejects a row for an unknown crate.

### 0.2 Experiments run before the plan

A spike ran three scenarios on both backends before the corpus was written.
Section 0.6 lists what the spike and the first full corpus run found.

### 0.3 Brainstorm — where does the suite live, and how does it compare?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A test target in `autumn-harvest-sqlite` with a `db` dev-dependency. | Rejected. Feature unification builds Diesel into every SQLite test. |
| B2 | A test target in the core crate with a dev-dependency on SQLite. | Rejected. A dev-dependency cycle gives two copies of the core types. |
| B3 | Share a fixture file with `#[path]`, as #2024 does, and compare each backend with a golden report. | Rejected. Two processes cannot compare histories. Goldens drift. |
| B4 | A new crate, `autumn-harvest-conformance`, with `publish = false`. One test target depends on both backends. | **Adopted.** It isolates the features. One process compares both runs. |
| B5 | One scenario = one workflow, an input, initial signals and a script of steps. | **Adopted.** The script is data, so both drivers run the same inputs. |
| B6 | Steps wait on wall-clock sleeps. | Rejected. Postgres timing then decides the history. |
| B7 | Steps wait on a history event (`AwaitEvent`). A signal batch commits in one transaction. | **Adopted.** One decision cycle sees the whole batch on both backends. |
| B8 | Compare raw history bytes. | Rejected. Ids and instants differ per run. |
| B9 | Compare normalized histories: drop backend-only events, mask volatile values, keep order and every other field. | **Adopted.** Each rule is a named list in `compare.rs`. |
| B10 | Also replay each history on the core `WorkflowReplayer`. | **Adopted.** It is the issue's "replay-equivalent". |
| B11 | Read the emitted commands with the replay debugger (`debugger` feature). It records the pending commands at each step of a prefix replay. | **Adopted in review.** A scenario must emit each kind that it covers, on both backends. |
| B11a | Check a witness event for each covered kind. | Replaced by B11. One event can stand for two kinds, and signal ingest writes `SignalReceived` with no wait. It stays only for the kinds that a replay does not emit. |
| B12 | An exhaustive `match` maps each command to a kind. A second one gives SQLite support. | **Adopted.** A new variant fails to compile until someone classifies it. |
| B13 | A guard test, with no Docker, that every accepted kind has a scenario. | **Adopted.** It runs in the same target. |
| B14 | Run a rejected primitive on both backends. | Rejected. Postgres needs extra wiring, such as an external completer. SQLite claims no parity. The suite asserts the loud rejection only. |
| B15 | One container per test binary. | Rejected. A static container leaks when the process exits. One container per test is the repo convention. |

### 0.4 Reverse brainstorm — how can the suite do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | It is flaky, so people ignore it. | Steps wait on events, not on time. A step that needs a parked run waits for a marker of the first cycle. Initial signals go in before the worker starts. |
| R2 | Normalization hides a real gap. | Each rule is narrow and named. Ids get aliases, so the relations stay. Instants are masked only at the top of the event data. |
| R3 | A scenario claims a command that it never emits. | Rule C1 replays each history with the debugger. Each covered kind must appear on both backends. |
| R4 | A new command lands with no scenario. | The exhaustive `match` fails to compile. The guard test fails on a kind with no scenario. |
| R5 | SQLite silently skips an unsupported primitive. | Each rejected primitive has a scenario. It asserts `UnsupportedFeature` and the feature name. |
| R6 | The row is deleted, so CI stops running the suite. | A `ci_run_coverage` guard requires the row. |
| R7 | The suite is slow. | Short timers. A container per test, in parallel outside CI. |
| R8 | Tests share a database and claim each other's tasks. | Each test starts its own container. With `HARVEST_TEST_DATABASE_URL`, the README says to run serially. A panic still stops the worker. |
| R9 | The suite passes on the old code, so it proves nothing. | Run the #2020 scenarios on `92add6f^`. They must fail there. See section 3. |
| R10 | The suite is flaky because Postgres has a defect. | A scenario that meets a known defect moves to the `defects` list. Its test carries `#[ignore]` with the reason, and the README names it. |
| R11 | A named rejection in the SQLite source has no scenario. | The coverage guard scans `runtime.rs` for `"<Feature> — ` messages. A second guard checks the last-cycle list against the terminal drain. |
| R12 | Postgres writes an event after the terminal event, and the read misses it. | The driver stops the worker before it reads any history. |

### 0.5 Six thinking hats

- **White (facts).** Sections 0.1 and 0.6.
- **Red (feelings).** A suite that compares two engines feels brittle. Each
  normalization rule must be named and justified.
- **Black (risks).** Section 0.4. Timer scenarios add seconds of wall clock.
  The suite also costs one more `test-db-linux` row.
- **Yellow (benefits).** A behavioural gap fails one named scenario with a
  side-by-side diff. A new command cannot land without a decision.
- **Green (ideas).** Run the chaos and DST seeds through both drivers later.
  Add a generated corpus from the lifecycle proptest model.
- **Blue (process).** Red: the suite, the guards and the CI guard, failing.
  Green: the drivers, the row and the clippy step. Refactor: docs. Then a
  multi-angle review.

### 0.6 Experiment results

| # | Finding | Action |
|---|---------|--------|
| E1 | Two signals sent in one Postgres transaction share `received_at`. Postgres then orders them by a random id. | The driver sends each signal in its own transaction, as a client does. Section 2 lists it as a Postgres defect. |
| E2 | Postgres writes a cycle's external signal, cancel and await requests before its other command events. SQLite keeps command order. Both replay. | Rule N4. |
| E3 | `random_u64` and `new_uuid` values differ per backend. They seed from the execution id. | Rule N3. |
| E4 | `MutexGranted.acquired_at` is an instant. | Rule N2 masks each `*_at` key at the top of the event data. |
| E5 | The core replayer reports a mismatch at an operator `WorkflowCancelled`, on both backends. | Rule R2 compares the verdicts. |
| E6 | A signal or a cancel to a detached child in the cycle that spawns it fails on Postgres in about half of the runs. The child can get the signal twice, or the parent fails on a duplicate `event_id`. A cancel can leave a second `ExternalCancelDelivered` after `WorkflowCompleted`. SQLite is correct each time. | The CI corpus sends in a later cycle. Two `#[ignore]`d tests reproduce the defect. |
| E7 | A fan-out with a result writer fails on the context before SQLite sees the field. Both backends give the same error. | A `Same` scenario. `ScheduleActivity.result_writer` is in `UNREACHABLE_REJECTIONS`. |
| E8 | A session acquire fails on `schedule_to_start_override` before any activity carries `session_id`. | `ScheduleActivity.session_id` is in `UNREACHABLE_REJECTIONS`. |
| E9 | The bookkeeping commands write no event on either backend. | Rule C1 sees each command in the replay. The docs list all six no-ops. |
| E10 | The suite on `92add6f^`, the trunk-dev before the #2020 fix. | See section 3. |
| E11 | Postgres finishes parallel activities in any order. | Rule N5. |
| E12 | Postgres writes a race loser's `ActivityFailed` after the cycle's other command events. SQLite keeps command order. Both replay. | Rule N6. |
| E13 | Postgres fails a run that waits in `await_condition` with no command: "workflow suspended without emitted commands". SQLite keeps the run and resumes it on a signal. | A third `#[ignore]`d defect test. |
| E14 | The logger emits no `RecordLog` in a replay. An update result runs outside the body replay. | `NOT_SEEN_IN_REPLAY` lists both, with the event that shows the command, if any. |
| E15 | With a 5 s retry backoff, the losing activity of a 1 s race once finished first on Postgres, in 1 of 13 runs. The cause is not known. A 60 s backoff gave 11 of 11 equal runs, on the typed and on the raw path. | The corpus uses a 60 s backoff. Section 2 lists the open question. |

---

## 1. Change

1. New crate `autumn-harvest-conformance`, `publish = false`.
2. `tests/conformance/`: the corpus, two drivers, the comparison rules, the
   command table and the coverage guard.
3. `.github/ci/integration-suites.txt`: one `linux` row.
4. `ci_run_coverage.rs`: the crate is valid, and its target needs a row.
5. `ci.yml`: a clippy step for the crate.
6. `docs/sqlite-backend.md` and `autumn-harvest-sqlite/src/lib.rs`: the six
   no-op commands, and a pointer to the suite.

## 2. Out of scope

- Fixing a Postgres defect that the suite finds (E1, E6, E13). Each one
  needs its own issue.
- The cause of E15.
- Client paths with no workflow command: signal-with-start, keyed signals,
  signals by workflow id, queries, the workflow retry policy and the
  execution timeout. Each one is a follow-up scenario.
- Comparing side tables: progress chunks, log lines and search attributes.
- Schedules and registration-time panics. They are not workflow commands.
- The chaos suite, the lifecycle model and DST on SQLite.

## 3. Evidence

**The #2020 cases fail on the code before the fix.** The suite cannot build
at `92add6f^`, because that SQLite has no update, cancel, terminate,
composition or mutex API. A reduced copy kept the harness, the comparison
and the eight scenarios that this SQLite can run. The only other change was
the two-line `remote_task.rs` compile fix from #2131. The result:

| Scenario | Result on `92add6f^` |
|----------|----------------------|
| `signal_drain` | Fails. Postgres `2`, SQLite `0`. |
| `signal_try_receive` | Fails. Postgres `"hello"`, SQLite `null`. |
| `signal_handler` | Fails. Postgres `"ship"`, SQLite `NotEnded` ("made no progress … (stuck)"). |
| `activity_completes`, `signal_wait`, `signal_before_first_cycle`, `signal_beats_deadline`, `deadline_beats_signal` | Pass. |

**The red phase.** With stub drivers, each of the 35 scenarios failed and
the five coverage guards passed (commit `7097276`).

**The green phase.** On this branch, every scenario passes on Postgres 16
and SQLite. The three defect tests carry `#[ignore]`.
