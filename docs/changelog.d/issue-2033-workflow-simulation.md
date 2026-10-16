## Testing — seeded interleaving simulation of workflow code (issue #2033)

`WorkflowTestEnv::simulation()` tests user workflow code against the
delivery orders that production can produce. The default env lands every
result of a suspension in one task, in command order. A simulation holds
activity results, child results, timer fires, mid-run signals and a cancel
in a pending set. At each task boundary a seed picks which ones land, and in
what order.

**Legal orders only.** Results land after their scheduling event, and each
attempt's `ActivityStarted` lands first. A timer fires only at its deadline
on the virtual clock. Equal deadlines from different tasks fire in arm
order. Each task-prep ingest orders signals and due fires as
`worker::merge_wake_events` does. A cancel is
terminal, as on Postgres. A pure differential test ties the model to
`merge_wake_events`. A Postgres test ties it to the real
`ingest_due_timers_and_signals`. Each explored history also passes
`check_legal`.

**Sweeps.** `Simulation::sweep` runs a seed plan. Each seed runs twice with
equal traces, replays through `WorkflowReplayer`, and runs again with a crash
at a seeded task. The crash must not change the outcome. `always!` is a hard
invariant. `sometimes!` is a reachability target, and a sweep fails when a
label is never true. A failure prints `HARVEST_SIM_SEED=<seed> cargo test …`.

**Determinism.** A run seeds `ctx.new_uuid()`, `ctx.random_*` and the
`ctx.system_*` clock reads, and it starts at a fixed time. The context gains
a `testing`-only seam for this. Production leaves it unset.

**Faults and mocks.** `fault_percent` fails activity attempts at a seeded
rate, and the worker retries them. `FaultPlan` fails a call by ordinal.
`mock_activity_seeded` gives a mock a seeded generator and the virtual clock.
`replay_with` replays each history with another build.

**Worked examples.** `workflow_sim_tests` holds a `ctx.race()` workflow that
always books the primary carrier. The default env passes it. A sweep finds a
seed where the backup quote wins, and the replay command reproduces it. A
second test races a cancellable timer against a later classic timer. The
default env lets the later timer win, which production cannot do. The sim
fires the earlier one, and the sweep shows that the history then does not
replay.

**Also.** `dst::SeedPlan` gains `parse_named` and `from_env_named`, which
take a `SeedVars`, so both harnesses share one seed parser. `always!` and
`sometimes!` live in `sim_probe`, so they compile in every build. The
default env now dedups a re-emitted `StartTimer` and `StartChildWorkflow` by
id, as the worker does. Its order is unchanged.

**Engine findings.** The sim surfaced three engine behaviours that this
change does not alter. A canary replay of a running activity reports its
trailing `ActivityStarted` as unconsumed. A cancellable timer await stops its
scan at an in-flight `ActivityStarted`. A strict replay of a race that a
queued signal wins at once diverges. The guide lists all three.

The harness now records a race loser's terminal only for a branch that
started, as `worker::apply_race_loser_cancellations` does.

The guide is `docs/testing/workflow-simulation.md`.

No new `WorkflowEvent` variant, no migration, no production behaviour change.
The simulation and the context seam are behind the `testing` feature.
