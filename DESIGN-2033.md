# Design — Issue #2033: Seeded interleaving simulation for user workflow code

`WorkflowTestEnv` resolves each suspension in one fixed order. It records
every result of a batch in one task, in command order. Production can deliver
the same results across many tasks, in any order. This change adds a seeded
mode that explores those orders and checks each one.

**No migration. No new `WorkflowEvent` variant. No engine change.** The new
code is `autumn_harvest::testing::sim`, behind the `testing` feature. The
default `WorkflowTestEnv` keeps its order.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Each harness cycle builds a new context from the history and runs the body
  from the start. The body holds no state between cycles.
- `process_command` appends the scheduling event at once. It defers the
  terminal event to the end of the batch. So a batch always resolves in the
  same task.
- A probe shows the race rule. With both race results in one task, branch 0
  wins. With only branch 1 in a task, branch 1 wins and the body emits
  `CancelRaceLosers` for branch 0. The default env can never show branch 1.
- An in-progress activity replays as `WaitForActivity`. An in-progress classic
  timer and an in-progress child emit `StartTimer` and `StartChildWorkflow`
  again. The worker dedups them by id.
- `worker::merge_wake_events` orders one ingest. A signal received strictly
  before a timer deadline goes first. A tie goes to the timer. The SQL loads
  due timers by `(fires_at, timer_id)`.
- Activity and child terminals are appended when they happen. Signals and
  timer fires are appended only at task-prep ingest.
- On Postgres a cancel is terminal. `cancel_workflow_execution` appends
  `WorkflowCancelled`, sets `CANCELLED` and fails the open tasks. The body does
  not run again.
- The virtual clock is derived from the fired timers in history
  (`virtual_now`). Activities take no virtual time.
- `ctx.new_uuid()` and `random_u64()` draw real randomness on a live cycle.
  Two runs of one seed can record different values.
- `dst` owns `SplitMix64`, `SeedPlan`, `first_divergence` and `Nondeterminism`.
  `run_twice` and `repro_command` are typed on the claim-protocol config.
- The body is polled inline in the caller's task. A `tokio::task_local!`
  scope reaches it.

### 0.2 Brainstorm — how can a test explore delivery orders?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Wrap the env in a seeded scheduler that holds results in a pending set and lands a seeded subset per task. | **Adopted.** It reuses the real cycle and command code. |
| B2 | Permute the events of a finished history and replay each permutation. | Rejected. Most permutations are illegal, and the body never reacts to the new order. |
| B3 | Run the real worker under the #2002 world simulation. | Rejected. It needs Postgres and runs no user code without a database. |
| B4 | Seed the poll order of the body futures, as autumn's `Sim::interleave` does. | Rejected. The engine decides a race from history, not from poll order. |
| B5 | Let the seed advance the virtual clock only to the next timer deadline. | **Adopted.** A timer then cannot fire early, and the existing clock stays valid. |
| B6 | Model signals as seeded arrivals and order each ingest with the worker's rule. | **Adopted.** |
| B7 | Model a cancel as a seeded, terminal arrival, as on Postgres. | **Adopted.** |
| B8 | `always!` and `sometimes!` record into a task-local probe of the current run. | **Adopted.** They work in the body and in the check closure. |
| B9 | A trace of normalized events per task. Ids become ordinals. Payloads stay out. | **Adopted.** A random id or `new_uuid` value cannot break run-twice. |
| B10 | Crash at a seeded task: drop the cycle's commands before they persist, then resume from the history. | **Adopted.** |
| B11 | A `#[sim_test]` attribute macro. | Deferred. A sweep call in a plain `#[tokio::test]` does the same job. |
| B12 | A proptest `Op` strategy sweep. | Deferred. Out of scope for the first cut. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The sim produces an order the worker cannot produce, so it reports a false bug. | The rules are a second model. A differential test ties them to `merge_wake_events`. A Postgres test ties them to the real ingest. Each explored history passes a legality check. |
| R2 | The refactor changes the order of the default env. | The default env flattens the new delivery units in the old order. The existing harness tests must stay green. |
| R3 | A green sweep proves nothing. | A sweep fails when a `sometimes!` label is never true. |
| R4 | A failing seed does not replay. | Each seed runs twice and the traces must match. The failure prints a one-line command with `HARVEST_SIM_SEED`. |
| R5 | A mock reads the wall clock or `rand`, so run-twice fails. | `mock_activity_seeded` gives the mock a seeded RNG and the virtual clock. |
| R6 | A re-emitted `StartTimer` or `StartChildWorkflow` doubles an event. | The harness dedups by id, as the worker does. |
| R7 | A race loser's held result lands after the loser was cancelled. | `CancelRaceLosers` drops the held result before it appends the synthetic terminal. `CancelTimer` drops the held fire. |
| R8 | The sweep is slow in CI. | The default is 64 seeds. `HARVEST_SIM_SEEDS` changes it. |
| R9 | A cancel or block leaves a history that strict replay rejects. | An unfinished history replays in canary mode, as the deploy canary does. |
| R10 | Users trust the sim for things it does not model. | The guide lists what it does not cover: activity bodies, the worker loop, Postgres, updates and queries. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The harness already owns the cycle, the
  mocks and the virtual clock. `dst` owns the seed tools.
- **Red (feelings).** A user wants one call that runs many seeds and prints a
  command on failure. Builders with many knobs feel heavy.
- **Black (risks).** The rules are a model, not the worker. Random side
  effects stay random. Shared state in `with_state` survives a crash.
- **Yellow (benefits).** Race, fan-out, signal and timer orders become
  testable without a database. Each seed also checks replay for free.
- **Green (ideas).** A by-ordinal fault plan. A seeded transient fault lane.
  Seeded mocks.
- **Blue (process).** Red, then green, then refactor. Pure model first, then
  the run loop, then sweep, docs and review.

---

## 1. Scope

In scope:

1. `WorkflowTestEnv::simulation()` returns a `Simulation`. `run(seed, ..)` runs
   one seed. `sweep(..)` runs a seed plan.
2. Held deliveries: activity results (with retries), child results, timer
   fires, mid-run signals and a cancel.
3. The legal-order model, its legality check and two tests that tie it to the
   worker.
4. Faults: a seeded transient lane and a by-ordinal `FaultPlan`.
5. Crash and resume at a seeded task.
6. A replay check of each explored history.
7. `always!`, `sometimes!` and the non-vacuity rule.
8. Run-twice, a normalized trace and a one-line replay command.
9. `mock_activity_seeded`.
10. A guide in `docs/testing/` and a changelog fragment.

Out of scope: `#[sim_test]`, proptest strategies, updates and queries in a
sim run, and external signal, cancel and await orders.

---

## 2. Legal-order rules

At each task boundary the sim holds a pending set. It picks a seeded subset:

1. **Results.** Any subset of held activity and child results, in any order.
   A result exists only after its scheduling event is in history.
2. **Clock.** Optionally advance to the next timer deadline. Every timer with
   a deadline at or before the new clock is then due. All due timers fire in
   this task, ordered by `(deadline, timer_id)`.
3. **Signals.** Zero or more of the next scripted signals arrive, in script
   order. Each is received before or at the new deadline.
4. **Ingest order.** The task appends the results first. It then appends the
   signals and the due fires, merged as `merge_wake_events` merges them.
5. **Cancel.** A scripted cancel can arrive. It appends `WorkflowCancelled`
   after the results and ends the run. No ingest runs.
6. **Losers.** `CancelRaceLosers` drops each loser's held result and appends
   the synthetic terminal at once. `CancelTimer` drops a held fire.

An awaited cancellable timer is held only on a bookkeeping-only batch, as in
the default env. A local activity resolves in its own task.

---

## 3. Test plan

| AC | Evidence |
|----|----------|
| Seeded, legal delivery across tasks | `workflow_sim_tests`: activities, children, timers, signals and cancel each land in more than one order over a sweep. Each explored history passes the legality check. |
| Rules match the worker | `worker::tests::sim_wake_order_matches_merge_wake_events` (pure, differential). `signal_tests::sim_wake_order_matches_the_real_ingest` (Postgres). |
| Worked example | `workflow_sim_tests::the_sim_finds_the_race_bug_the_default_env_misses`. |
| Run-twice and replay command | A tampered runner fails run-twice. A failure message holds `HARVEST_SIM_SEED=`. |
| Replay check | Each sweep seed passes `WorkflowReplayer`. A nondeterministic body fails the sweep. |
| `always!`, `sometimes!`, sweep | An unsatisfied `sometimes!` fails a sweep. A false `always!` fails it. |
| Crash and resume | Each sweep seed also runs with a crash. The outcome must match. A body that depends on its run count fails. |
| Guide | `docs/testing/workflow-simulation.md`. |
| Changelog | `docs/changelog.d/pr-2033-workflow-sim.md`. |
