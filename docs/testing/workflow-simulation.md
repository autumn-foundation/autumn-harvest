# Seeded simulation of workflow code

Issue #2033 adds a seeded mode to `WorkflowTestEnv`. It tests your workflow
code against the delivery orders that production can produce.

The default env lands every result of a suspension in one task, in command
order. A timer fires as soon as the body waits on it. A queued signal lands
before the first task. So a test sees one order only. In production, the
same results can land across many tasks and in any order.

A simulation holds each result in a pending set. At each task boundary, a
seed picks which results land and in what order. The rest wait for a later
task. A seed fixes every choice, so a failing seed replays exactly.

The module is `autumn_harvest::testing::sim`. It needs the `testing`
feature. It needs no database.

## A worked example

This workflow asks two carriers for a quote and books the first one. It has
a bug: it always books the primary carrier.

```rust
type Fut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn book_first_quote<'a>(ctx: &'a WorkflowContext, _: Value) -> Fut<'a> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("quote_primary", json!({}), "q")
            .activity_raw("quote_backup", json!({}), "q")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let booking = json!({"carrier": "primary", "price": winner.value});
        ctx.execute_activity_raw("book", booking, "q")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner": winner.index, "carrier": "primary"}))
    })
}
```

The default env lands both quotes in one task. When both results are in
history, the race picks branch 0. So `env.run` always books the primary and
the test passes.

A sweep finds the bug:

```rust
use autumn_harvest::{always, sometimes};

#[tokio::test]
async fn the_booked_carrier_won() {
    let env = WorkflowTestEnv::new()
        .mock_activity("quote_primary", |_| Ok(json!(100)))
        .mock_activity("quote_backup", |_| Ok(json!(90)))
        .mock_activity("book", Ok);
    env.simulation()
        .sweep(book_first_quote, json!(null), |run| {
            if let Ok(output) = run.result() {
                let won = ["primary", "backup"][output["winner"].as_u64().unwrap() as usize];
                always!(output["carrier"] == won, "the booked carrier won");
                sometimes!(won == "backup", "the backup quote wins");
            }
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}
```

On some seed the backup quote lands alone in a task. The race picks branch 1,
and the `always!` is false. The failure prints the seed, a replay command and
the end of the trace. This is the output of the version in this repository
when this guide was written:

```text
seed 5 failed: always!("the booked carrier is the one that won") is false in the check
reproduce: HARVEST_SIM_SEED=5 cargo test -p autumn-harvest --no-default-features --features testing --test integration workflow_sim_tests::the_sim_finds_the_race_bug_the_default_env_misses -- --exact --nocapture
last trace lines:
0000 +0s WorkflowStarted
0001 +0s MarkerRecorded race:1
0001 +0s ActivityScheduled a1 quote_primary
0001 +0s ActivityScheduled a2 quote_backup
0002 +0s ActivityStarted a2
0003 +0s ActivityStarted a1
0003 +0s ActivityCompleted a2
0003 +0s MarkerRecorded race_winner:1
0003 +0s ActivityScheduled a3 book
0003 +0s ActivityFailed a1 attempt 1
0004 +0s ActivityStarted a3
0005 +0s ActivityCompleted a3
0005 +0s WorkflowCompleted
end completed
```

Each line starts with the task and the virtual time. In task 3 the backup
result `a2` lands alone, so branch 1 wins. The race then cancels the primary
`a1`, which records `ActivityFailed a1`. `race_winner:1` names the first race of the run, not its winner.

`autumn-harvest/tests/integration/workflow_sim_tests.rs` holds a version of
this example and the fixed workflow.

## What a sweep checks

`Simulation::sweep` runs each seed of its plan. For each seed it:

1. runs the seed and calls your check closure on the outcome;
2. runs the seed again and compares the two traces;
3. checks that the history obeys the legal-order rules;
4. fails on a false `always!`;
5. replays the history with `WorkflowReplayer`;
6. runs the seed again with a crash at a seeded task, and compares the
   outcome with the uncut run.

After the last seed, each `sometimes!` label must have been true at least
once. A label counts only after a run evaluates it. For a label on a path
that no seed may reach, call `Simulation::require_reached(label)`. A harness
error, such as a missing mock, fails the sweep at once.

`always!` and `sometimes!` work in the workflow body and in the check
closure. They are in every build, so a workflow body can keep them. Outside a
simulation run, they only evaluate the condition.

## What the simulation delivers

| Delivery | Rule |
|---|---|
| Activity result | Lands at any later task. Each attempt's `ActivityStarted` lands first, in its own step. A start wakes no task, so it lands with the next waking delivery. |
| Child result | Lands at any later task. |
| Timer fire | Lands when the virtual clock reaches its deadline. Due timers fire by deadline, then by timer id. |
| Signal | `Simulation::signal` sends it at a seed-chosen task. Signals arrive in call order. |
| Cancel | `Simulation::cancel` lands it at a seed-chosen task. The run ends `Cancelled`. |

At one task boundary, the results land first, in a seeded order. The
task-prep ingest follows: the arrived signals and the due fires. A signal
received strictly before a deadline lands before that fire. A tie goes to the
timer. This is the order of `worker::ingest_due_timers_and_signals`.

The virtual clock moves only when a timer fires. The seed can move it to the
next deadline at any boundary. A timer never fires before its deadline.

Equal deadlines from different tasks fire in the order of the tasks that
armed them, as distinct `NOW()` values order them. Equal deadlines from one
task fire by timer id.

The simulation holds every awaited cancellable timer, because the worker
stores a timer row for each one. A task that parks on an activity, a signal
or a child, with no classic timer, sets no wake-up time. Its awaited timers
then fire only when another delivery wakes the task.

The simulation drops a race loser's held result when the race cancels it. It
also drops the held fire of a cancelled timer.

A cancel is terminal, as on Postgres. The body does not run again. To test a
body that reacts to a cancel, use `WorkflowTestEnv::with_cancellation`.

Two tests tie these rules to the worker:

- `worker::tests::sim_wake_order_matches_merge_wake_events` compares the
  model with `merge_wake_events` on 2,000 seeded inputs.
- `workflow_sim_ingest_tests::sim_wake_order_matches_the_real_ingest` runs
  the real ingest on Postgres and compares the events.

## Faults

- `Simulation::fault_percent(p)` fails each activity attempt with chance
  `p` percent. The worker retries it. The attempts come from the retry policy
  override of the call, or `DEFAULT_MAX_ATTEMPTS` (3).
- `FaultPlan` fails a call by ordinal. `FaultPlan::new().fail("send_invoice",
  2, "down")` fails the second call. `transient(name, n, k)` fails the first
  `k` attempts of call `n`.

The mock does not run for a failed attempt. A mock that returns `Err` ends
the call, as in the default env. `SimOutcome::faults` lists each fault that a
run applied.

These limits apply:

- Faults do not apply to local activities.
- A `FaultPlan` entry for a call turns off the fault lane and the retry
  sequence for that call.
- The simulation does not read the retry policy that the activity registers.

## Crash and resume

A crash drops the commands of one cycle before they persist. The run then
resumes from the history so far, as a new worker does. A correct body gives
the same trace as the uncut run.

A body that keeps state outside the history fails this check. An example is a
counter in `ctx.state` that picks a branch. `Simulation::run_with_crash` runs
one seed with a crash at a given task.

## Determinism

Each seed runs twice, and the traces must match. A trace line names the task,
the virtual time and the event. Ids become ordinals such as `a1` and `c1`.
The trace holds no payload, so the two runs also compare a digest of each
event with its payloads. The digest uses the same ordinals. The crash check
compares both too.

A run seeds the live values of `ctx.new_uuid()`, the `ctx.random_*` helpers
and the `ctx.system_*` clock reads. A clock read returns the virtual time. A
run starts at a fixed time, Monday 2024-01-01 00:00 UTC, unless the env calls
`with_frozen_anchor`. So a body that branches on these values replays from
its seed on any machine and day.

A mock that reads `rand` or the wall clock can still break this check. Use
`mock_activity_seeded`. Its mock gets a seeded generator and the virtual
clock:

```rust
let env = WorkflowTestEnv::new()
    .mock_activity_seeded("flip", |_, mock| Ok(json!(mock.rng().chance(50))));
```

## Run and replay

| Variable | Meaning |
|---|---|
| `HARVEST_SIM_SEED` | Run this one seed only. It prints the trace and skips the `sometimes!` check. |
| `HARVEST_SIM_SEEDS` | The number of seeds. The default is 64. |
| `HARVEST_SIM_SEED_BASE` | The first seed. The default is 0. |

`Simulation::seeds(plan)` sets the plan in code. Each variable overrides its
part of the plan, so the replay command works for every test and a nightly
job can widen a sweep. Set `HARVEST_SIM_SEED` for one test at a time. A test
that expects a sweep to fail can pass on one seed.

The replay command names the package and the test target. Add any other
cargo arguments with `Simulation::cargo_args`, for example
`--features testing`. The command uses POSIX shell syntax. On Windows, set
the variable first.

`Simulation::replay_with(handler)` replays each history with another
handler. Use it to check a new build against the orders of the old one.

## Wire the sweep into your own CI

A sweep is a normal `#[tokio::test]`. Enable the `testing` feature of
`autumn-harvest` in your `[dev-dependencies]`. Your CI runs the sweep with
`cargo test`. Run more seeds on a schedule than on each pull request. For example, set
`HARVEST_SIM_SEEDS=10000` in a nightly job. Change `HARVEST_SIM_SEED_BASE` to
explore new seeds without a code change.

## What it does not cover

- **Activity bodies.** Mocks replace them. The simulation tests the order of
  results, not the work that makes them.
- **The worker loop.** The simulation is a second model of the worker. Use
  the world simulation in [`simulation.md`](simulation.md) for the worker
  itself.
- **Postgres.** No SQL runs. Locks, claims and leases are out of scope.
- **Updates and queries.** A simulation run sends neither. Use
  `WorkflowTestEnv::start` for those.
- **External workflows.** External signals, cancels and awaits resolve in
  their own task, as in the default env.
- **Local activities.** They resolve in the task that runs them, as in
  production. The body then runs again at once, with no delivery between.
- **Shared state.** A crash does not reset `with_state` values.
- **Timer id collation.** The model sorts tied timer ids by bytes. Postgres
  sorts them by collation. Keep tied ids to plain ASCII letters and digits.
- **Cancellable timer deadlines.** The simulation counts a cancellable
  timer's deadline from its arm, as the harness clock does. The worker counts
  it from the first await.
- **A cancel before the first task.** A scripted cancel lands only after the
  first task. Production can cancel a run before its first task.
- **Several deadlines in one task.** One task fires the timers of one
  deadline. The worker can ingest several deadlines in one task.
- **A cancellable timer beside a running activity.** The worker appends
  `ActivityStarted` when an attempt starts. A timer await scan stops at that
  event, so `join!(activity, timer.await_fire())` can report a false
  non-determinism once the activity runs. This is engine behaviour, and the
  simulation shows it.
- **A race that a queued signal wins at once.** When the signal is in
  history before the first task, the race never dispatches its other
  branches. A strict replay of that history then reports a divergence, in
  the default env too. This is engine behaviour, and the simulation shows it.
- **In-flight starts in a replay.** A canary replay of an unfinished history
  leaves out the `ActivityStarted` of each activity with no terminal. The
  matcher does not consume such an event at the frontier.

autumn's `#[sim_test]` attribute and its proptest sweep are not part of this
change.
