# Design — Issue #2030: cross-backend conformance suite

No test runs one workflow on both backends and compares the results. The
cross-backend test replays a SQLite history on the core replayer. That
proves the encoding, not the behaviour. A gap such as #2020 passes every
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
- `WaitForActivity`, `SetCurrentDetails`, `PublishProgress`,
  `PublishDurableProgress`, `RecordLog`, `UpsertSearchAttributes`,
  `CancelRaceLosers` and `ReleaseMutex` write no event on either backend.
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
| B11 | Capture the emitted commands at run time. | Rejected. The core exposes no command hook. |
| B12 | An exhaustive `match` maps each command to a kind. A table gives SQLite support and a witness event. | **Adopted.** A new variant fails to compile until someone classifies it. |
| B13 | A guard test, with no Docker, that every accepted kind has a scenario. | **Adopted.** It runs in the same target. |
| B14 | Run a rejected primitive on both backends. | Rejected. Postgres needs extra wiring, such as an external completer. SQLite claims no parity. The suite asserts the loud rejection only. |
| B15 | One container per test binary. | Rejected. A static container leaks when the process exits. One container per test is the repo convention. |

### 0.4 Reverse brainstorm — how can the suite do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | It is flaky, so people ignore it. | Steps wait on events, not on time. A signal batch is one transaction. Initial signals go in before the worker starts. |
| R2 | Normalization hides a real gap. | It masks values, never keys. It drops only `ActivityStarted`. Order and every other field stay. |
| R3 | A scenario claims a command that it never emits. | Each accepted kind with a witness event must leave that event in both histories. |
| R4 | A new command lands with no scenario. | The exhaustive `match` fails to compile. The guard test fails on a kind with no scenario. |
| R5 | SQLite silently skips an unsupported primitive. | Each rejected primitive has a scenario. It asserts `UnsupportedFeature` and the feature name. |
| R6 | The row is deleted, so CI stops running the suite. | A `ci_run_coverage` guard requires the row. |
| R7 | The suite is slow. | Short timers. A container per test, in parallel outside CI. |
| R8 | Tests share a database and claim each other's tasks. | Each test starts its own container. With `HARVEST_TEST_DATABASE_URL`, the README says to run serially. |
| R10 | The suite is flaky because Postgres has a defect. | A scenario that meets a known defect moves to the `defects` list. It is `#[ignore]`d with the reason, and the README names it. |
| R11 | A named rejection in the SQLite source has no scenario. | The coverage guard scans `runtime.rs` for `"<Feature> — ` messages. |
| R9 | The suite passes on the old code, so it proves nothing. | Run it on `92add6f^`. The #2020 scenarios must fail there. |

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
| E1 | Two signals sent in one Postgres transaction share `received_at`. Postgres then orders them by a random id. | The driver sends each signal in its own transaction, as a client does. Recorded as a Postgres finding. |
| E2 | Postgres writes a cycle's external signal, cancel and await requests before its other command events. SQLite keeps command order. Both replay. | Rule N4. |
| E3 | `random_u64` and `new_uuid` values differ per backend. They seed from the execution id. | Rule N3. |
| E4 | `MutexGranted.acquired_at` is an instant. | Rule N2 masks each `*_at` key. |
| E5 | The core replayer reports a mismatch at an operator `WorkflowCancelled`, on both backends. | Rule R2 compares the verdicts. |
| E6 | A signal or a cancel to a detached child in the cycle that spawns it fails on Postgres in about half of the runs. The child can get the signal twice, or the parent fails on a duplicate `event_id`. A cancel can leave a second `ExternalCancelDelivered` after `WorkflowCompleted`. SQLite is correct each time. | The CI corpus sends in a later cycle. Two `#[ignore]`d tests reproduce the defect. |
| E7 | A fan-out with a result writer fails on the context before SQLite sees the field. Both backends give the same error. | A `Same` scenario. `ScheduleActivity.result_writer` is in `UNREACHABLE_REJECTIONS`. |
| E8 | A session acquire fails on `schedule_to_start_override` before any activity carries `session_id`. | `ScheduleActivity.session_id` is in `UNREACHABLE_REJECTIONS`. |
| E9 | The bookkeeping commands write no event on either backend. | Covered by the outcome. The docs list all six no-ops. |
| E10 | The suite on `92add6f^`, the trunk-dev before the #2020 fix. | See section 3. |

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

- Fixing a Postgres defect that the suite finds (E1, E6). Each one needs its
  own issue.
- Comparing side tables: progress chunks, log lines and search attributes.
- Schedules and registration-time panics. They are not workflow commands.
- The chaos suite, the lifecycle model and DST on SQLite.

## 3. Evidence

Filled in after the red run.
