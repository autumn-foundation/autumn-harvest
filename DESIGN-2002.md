# Design — Issue #2002: Run the real worker loop under deterministic simulation

Issue #2002 asks for a seeded simulation that runs `worker.rs`, not only
the oracle store of issue #1830. The resident path, timers and the
scheduler fire claim must be in scope. A failure must replay from its seed.

**No migration. No new `WorkflowEvent` variant. No route change.**
`Worker` gains two `#[doc(hidden)]` hooks. `autumn_harvest::dst` gains the
`world` module.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `src/dst` drives store statements on an in-memory oracle. It never calls
  `worker.rs`. ADR 0004 names this limit.
- `worker.rs` writes through `AsyncPgConnection` in about 600 places. An
  in-memory store behind it is a rewrite, not a change.
- One poll-loop iteration is `Worker::poll_once`. It claims one task and
  spawns the task body on `dispatched.tracker`.
- The resident path needs one `WorkflowCache` across decisions and a
  non-zero `sticky_timeout`. `Worker` owns both.
- Timers fire inside the next decision. `ingest_due_timers_and_signals`
  reads `fires_at <= NOW()`.
- The scheduler fire claim is `claim_and_fire_workflow_schedule`. It takes a
  due-list snapshot, so a stale snapshot can race a fresh one.
- Time comes from Postgres `NOW()`, `clock_timestamp()` and host
  `Utc::now()`. No clock seam exists.
- Only `harvest_events` has an update guard. The other tables accept an
  `UPDATE` of their timestamp columns.

### 0.2 Brainstorm — how can the worker loop run under a seed?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Put a store trait under `worker.rs` and back it with the oracle. | Rejected. About 600 SQL sites change. The oracle becomes a second engine. |
| B2 | Run real workers concurrently and seed only the faults. | Rejected. The interleaving is not seeded, so a failure does not replay. ADR 0004 rejects this for the same reason. |
| B3 | Run each actor step to completion on one thread, against Postgres. The seed picks the actor. | **Adopted.** Each step calls real code. The order of steps is a function of the seed. |
| B4 | Add a clock trait to the crate and inject a virtual clock. | Rejected. Three clock sources and about 90 call sites change. |
| B5 | Keep a virtual clock. Advance it by shifting every stored instant back by one tick. | **Adopted.** No production change. Section 2.3 gives the determinism argument. |
| B6 | Use the chaos `kill_at` points for crashes inside a step. | Deferred. It needs the `chaos` feature. Section 5 lists it as next scope. |
| B7 | Fresh database per run, cloned from a migrated template. | **Adopted.** Two runs of one seed then start from equal state. |
| B8 | A planted, seeded bug proves that a failure replays from its seed. | **Adopted.** A workflow reads worker-local state. Resident state hides it until a fault moves the workflow. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Real time leaks into a decision, so two runs of one seed differ. | Every seed runs twice on fresh databases. The traces must be equal. The tick is one hour, and a run takes seconds. |
| R2 | A random id leaks into the trace. | The trace uses labels: `c0`, `s1`, `w2.3`. Activity ids become ordinals. |
| R3 | The time warp misses a column, so a deadline never comes due. | The warp shifts every `timestamptz` column of the in-scope tables, read from `information_schema`. The convergence check fails on a stuck run. |
| R4 | The hooks change production behaviour. | `dst_poll_once` calls `poll_once`, then waits for the tracker. `dst_register` calls `register_in_fleet`. Neither runs on a production path. |
| R5 | A test-side query copies production SQL and drifts. | Only the due-list scan is test-side. The claim itself is production code. |
| R6 | The sweep is too slow for CI. | PR CI runs 4 seeds. The nightly runs 2,000 in release mode. |
| R7 | A planted bug leaks into the default run. | The plant is off by default. Only `HARVEST_DST_WORLD_PLANT` turns it on. |
| R8 | The harness passes vacuously. | A sweep asserts coverage counters: warm resumes, declines, cold loads, activities, timer fires, signals, lost fire claims, stalls and crashes. |
| R9 | The warp maps two virtual instants to one stored instant. | Found in the first trace: two schedule slots got one workflow id, and a run was lost. Each advance now shifts by one tick plus 1 ms. `FireStartsRun` catches a lost run. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The oracle harness has 6 invariants, 8
  operations and a golden test. The nightly runs 4,000,000 oracle seeds.
- **Red (feelings).** A Postgres-backed run feels slow and fragile. A
  strict run-twice check and fixed labels answer the fragility. Release
  mode answers the speed.
- **Black (risks).** Coarse steps hide a race between two statements of one
  cycle. Host-clock code inside a step could still see real time. The warp
  is a model of time, not time itself.
- **Yellow (benefits).** The real claim, decision, resident resume, timer
  ingest, sticky failover, fire claim, reclaimer and sweeper run under a
  seed. A failure prints one command.
- **Green (ideas).** Use chaos holds to split a cycle into steps. Run the
  oracle and the world from one seed. Add PCT-style priorities.
- **Blue (process).** Red, then green, then refactor. Pure logic first,
  with no database. The Postgres world next. Docs, nightly and review last.

---

## 1. Scope

| In scope | Code that runs |
|---|---|
| Claim and decision | `Worker::poll_once`, `process_task`, `process_workflow_task` |
| Activities | `process_activity_task` through the same poll |
| Resident path | `WorkflowCache` with resident state, sticky routing |
| Timers | `persist_started_timer`, `ingest_due_timers_and_signals` |
| Signals | `signal::send_signal`, signal ingest in the decision |
| Scheduler fire claim | `scheduler::claim_and_fire_workflow_schedule` |
| Orphan reclaimer | `poison_pill::reclaim_orphaned_tasks` |
| Timeout sweeper | `timeout::enforce_timeouts_once` |
| Liveness | `workers::heartbeat_worker` |

Faults: a worker stall and a worker crash. A crash drops the `Worker`, so
its resident state goes. A stall keeps it, so a stale resident must
decline.

## 2. Design

### 2.1 Split

- `autumn_harvest::dst::world` (no database). The config, the seeded
  planner, the virtual clock, the driver loop, the invariants, the trace,
  the run-twice check and the replay command. A `World` trait applies
  each action.
- `tests/integration/dst_world_tests.rs`. `PgWorld` implements `World` with
  real `Worker`s on a fresh database.
- `tests/dst/main.rs` tests the driver with an in-memory fake world.

### 2.2 One step

The planner draws one enabled action from fixed weights. The world applies
it and awaits it to completion. The driver then reads a snapshot: each
execution's status, block state and history facts. It logs the new facts
and checks the invariants.

### 2.3 The virtual clock

The clock moves in ticks of one hour. `Advance` subtracts one tick plus
1 ms from every `timestamptz` column of each `harvest_*` table, except the
event log. A stored instant is then `virtual time + e`, where `e` is the
real time at the write.

- Every workload duration is a whole number of ticks.
- Two instants with different virtual times differ by at least one tick.
  `e` is less than a run's real length, which is far below one tick. So
  the virtual parts decide the comparison.
- Two instants with equal virtual times compare by `e`. `e` follows the
  order of the writes, and the seed fixes that order.
- The extra millisecond keeps two virtual instants apart. The engine builds
  ids from instants, such as the workflow id of a schedule slot.

### 2.4 Invariants

| Invariant | Meaning |
|---|---|
| `OneTerminal` | An execution has at most one terminal event, and it is the last event. |
| `StatusMatchesHistory` | A terminal status has its terminal event, and the other way round. |
| `ActivityResultOnce` | An activity has at most one result event. |
| `TimerFiresOnce` | A timer fires at most once. |
| `TimerNotEarly` | A timer fires no earlier than its start plus its duration. |
| `ScheduleSlotOnce` | A schedule slot fires at most once. |
| `ScheduleNotEarly` | A slot fires no earlier than its due time. |
| `FireStartsRun` | A fire claim that wins starts exactly one execution. |
| `Deterministic` | No execution fails or blocks on non-determinism. |
| `ExpectedOutput` | A completed execution returns the expected value. |
| `Converges` | After the drain phase, every execution is complete. |

### 2.5 Replay

A failure prints a command with the seed, the plant and the checks. The
replay runs the seed twice on fresh databases and prints the trace.

## 3. Test plan (red, green, refactor)

1. Red: no-database tests for the clock, the planner, the invariants and
   the driver against a fake world. Green: `dst::world`.
2. Red: integration tests for the hooks and one Postgres world seed.
   Green: the hooks and `PgWorld`.
3. Red: the planted failure and its replay from the seed alone. Green: the
   plant.
4. Refactor. Then the nightly job, the PR CI row and the docs.

## 4. Acceptance criteria

| Criterion | Evidence |
|---|---|
| The nightly DST run drives `worker.rs`. | The `world` job in `dst-nightly.yml`. |
| The resident path, timers and the fire claim are in scope. | Coverage counters asserted by `a_world_sweep_covers_the_scope`, and `dst_poll_once_runs_the_claimed_task_to_completion`. |
| A seeded failure reproduces from its seed alone. | `a_planted_failure_replays_from_its_seed_alone`. Seed 0 of the plant fails `Deterministic`. |

## 5. Next scope

- A crash inside a step, through the chaos `kill_at` points.
- Statement-level interleaving inside one cycle, through chaos holds.
- Quarantine and the `FAILED` state, with failing activities.
