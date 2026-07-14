## R&D Spike — SQLite edge/local-first harvest feasibility (issue #966)

**Throwaway R&D feasibility spike — never merged to `trunk-dev` as-is.** A
timeboxed study answering one question: *can the backend-neutral determinism
core (`run_workflow`, `WorkflowEvent`, deterministic replay) be driven by a
non-Postgres, embedded, single-writer persistence layer?* The answer:
**feasible, but do not port in-crate** — recommend a separate companion crate
that depends on the core; decline the in-crate `StorageBackend` trait (its cost
falls entirely on the shipped Postgres hot path).

**What shipped:**

- **The primary deliverable — the feasibility report** `docs/rnd/sqlite-feasibility.md`
  (executive summary + costed go/no-go up front; exhaustive, grep-verified
  Postgres-coupling inventory classifying every coupled surface as
  trivially-trait-able / needs-a-semantic-substitute / fundamentally-Postgres-shaped;
  `StorageBackend` seam sizing with the honest cost to the Postgres path;
  prototype evidence; test-portability analysis; edge→hub sync scoping incl. the
  `ExecutionId` shard-byte problem and libSQL/Turso + DuckDB alternatives;
  prior-art scan vs. Temporal/DBOS/Inngest/Hatchet/Restate/Oban). The prior-art
  section is fact-checked against primary sources (accessed 2026-07-14): **DBOS
  ships embedded SQLite as its *default* system database** (recommending Postgres
  for production/multi-server) — the closest in-process-library precedent to
  Harvest, and its own "can't be used in a distributed setting" guidance
  *corroborates* this spike's single-writer finding; **Oban ships a built-in
  SQLite engine** (`Oban.Engines.Lite`), not Postgres-only; Restate is a separate
  single-binary server (not an in-process library); Temporal's dev server runs on
  embedded SQLite for local dev only. `docs/comparison.md` (issue #963) was
  aligned to the same corrected DBOS facts. The correction **strengthens** the
  verdict (feasibility rests on demand + zero core cost, not on being first) and
  leaves it unchanged.
- **A disposable, feature-gated prototype** at `autumn-harvest/src/sqlite_spike/`
  (`mod`/`schema`/`store`/`queue`/`worker`) behind the new `sqlite-spike =
  ["dep:rusqlite"]` Cargo feature. It reuses the backend-neutral determinism
  core **wholesale** (zero core changes) and reimplements only persistence on
  embedded SQLite: single-writer `BEGIN IMMEDIATE` claim substitutes for
  `SELECT … FOR UPDATE SKIP LOCKED`; polling substitutes for LISTEN/NOTIFY;
  app-minted UUIDs substitute for `gen_random_uuid()`; explicit epoch `fire_at`
  columns substitute for `INTERVAL` deadline arithmetic. **Every event append is
  committed atomically with its companion durable state mutation** (Codex P1
  durability fixes, closed convergently via a full audit of the append+state
  class): a cycle's derived event and its paired task/timer row
  (`apply_commands`); a drained activity's terminal event + task-state flip
  (`drain_ready`); the staged-signal `delivered` flag + `SignalReceived` append;
  each due timer's `TimerFired` event + `fired = 1` flag (`drain_ready` — a later
  Codex P1: separate autocommits previously re-fired a timer into a stray
  duplicate `TimerFired` after a crash); the terminal `WorkflowCompleted`/
  `WorkflowFailed` event + execution-state flip (`drive_one_cycle` — separate
  autocommits previously re-ran a run into a duplicate terminal); and the start
  execution-row + `WorkflowStarted` event (`start_workflow`/`import_execution`).
  **The virtual clock is itself durable** (Codex P1): persisted on every advance
  (`spike_meta`) and restored on `open` (belt-and-braces: never below any
  already-fired timer's deadline) — timers store an *absolute* `fire_at`, so the
  prior reset-to-0-on-reopen left a timer armed at a non-zero logical time
  permanently not-yet-due. This closes the append-before-enqueue,
  result-before-finish, fire-before-flag, terminal-event-before-state-flip, and
  clock-reset windows; the sole residual window is a crash *mid activity-body*,
  which a productized runtime would recover via the engine's heartbeat-timeout +
  poison-pill reclaimer (out of a single-process spike's scope). **The import path
  materializes derived state for in-flight open work** (Codex P1, round 4): when an
  imported cross-backend history carries an *open* (un-terminated)
  `ActivityScheduled`/`TimerStarted`, `import_execution` rebuilds the companion
  `spike_tasks`/`spike_timers` row inside the import transaction, so a handoff of a
  *partially-complete* execution drains rather than wedging at `SpikeError::Stuck`
  (open timers re-arm relative to import time, since a source backend's absolute
  deadline isn't portable). **Deterministic side-effect commands are persisted, not
  dropped** (Codex P2, round 4): `apply_commands` appends `RecordSideEffect`/
  `RecordMarker` as `SideEffectRecorded`/`MarkerRecorded` in command order (so a
  `ctx.system_now`/`new_uuid`/`random_*`/`side_effect` before a suspending activity
  replays faithfully rather than diverging), and its match is now **exhaustive**
  (no `_` wildcard) — every command is persisted, a single documented no-op
  (`WaitForActivity`), or an explicit `Unsupported` that rolls the batch back, so no
  command is ever silently dropped. **Terminal-cycle bookkeeping is persisted before
  the seal, and a sealed import derives its terminal state** (Codex P2, round 5):
  `drive_one_cycle` now drives through the no-DB `run_workflow_with_state` (which
  surfaces the terminal cycle's pending command vector the bare `run_workflow`
  drops) and persists a same-cycle `RecordSideEffect`/`RecordMarker` as
  `SideEffectRecorded`/`MarkerRecorded` **before** the terminal
  `WorkflowCompleted`/`WorkflowFailed` event (mirroring the engine's
  `worker::persist_terminal_outcome_commands`) — so a workflow that mints a value
  and returns *without suspending* no longer re-mints/diverges on replay; and
  `import_execution`, when handed an already-**sealed** history (tail event a
  terminal `WorkflowCompleted`/`WorkflowFailed`), initializes the row directly in
  that terminal state with the imported output/error instead of creating it RUNNING
  and re-driving it into a duplicate terminal. **One documented known limitation
  (not fixed, per timebox):** reusing a *classic* timer id after a prior same-id
  timer has fired wedges at `SpikeError::Stuck` — the prototype's whole-history
  bare-id arm-guard drops the engine's legitimately-new second `StartTimer`, and the
  `spike_timers` PK `(exec_id, timer_id)` cannot represent a second occurrence; a
  faithful fix is occurrence-paired arming plus a surrogate-PK schema change plus
  occurrence-aware fire/introspection queries — real surgery to disposable code that
  the productized engine already handles natively (occurrence-based matcher over the
  real `harvest_timers`). These three round-5 edges (sealed import, terminal-cycle
  bookkeeping, timer-id reuse) are documented in report §5.1/§5.2 as *evidence for*
  the go/no-go: they are exactly the deep coordination/matcher-coupling surface that
  motivates the "separate companion crate reusing the core, not a trivial port"
  recommendation.
- **A 15-test smoke suite** `autumn-harvest/tests/integration/sqlite_spike_tests.rs`
  (15/15 pass, no Docker via `rusqlite`'s `bundled` feature): activity retry,
  durable timer across process restart, signal delivery, deterministic replay
  after a simulated crash, **cross-backend replay executed in both directions** —
  a SQLite-written history (success-path *and* a retry-produced history) replays
  cleanly on the engine's own `WorkflowReplayer` path, and a genuinely PG-shaped
  `ActivityStarted`-bearing history drives the SQLite runtime's own reload path to
  the identical outcome with no duplicate activity execution (both backends
  serialize the same `WorkflowEvent` via `serde_json`; histories are
  replay-equivalent, not byte-identical event streams, because the matcher skips
  `ActivityStarted`) — **plus four persistence-durability regression tests**: the
  event/queue-timer-row invariant survives a reload
  (`scenario_atomic_persist_event_and_row_never_out_of_sync`); a batch that fails
  partway rolls back *both* the event and its row
  (`scenario_atomic_persist_rolls_back_the_whole_batch_on_unsupported_command` —
  a both-or-neither proof that fails on the pre-fix per-command-autocommit code);
  a fired timer's `TimerFired` event and `fired = 1` row agree across a reload so
  it is never re-fired (`scenario_timer_fire_is_atomic_no_double_fire_across_reload`);
  and a timer armed at a non-zero logical time fires after a restart via the
  durable clock (`scenario_two_timers_second_fires_across_restart_via_durable_clock`
  — a part-way advance isolates the persisted-clock fix from the fired-timer floor;
  fails on the pre-fix clock-reset-to-0-on-reopen code) — **plus two round-4
  regression tests**: an imported in-flight open activity materializes its task row
  and drains to `Completed`
  (`scenario_import_in_flight_activity_materializes_task_and_drains`; fails pre-fix,
  wedging at `Stuck`); and a `ctx.new_uuid()` side effect emitted before an activity
  is frozen into history and recovered verbatim across a restart
  (`scenario_side_effect_command_persists_and_replays_across_restart`; fails pre-fix
  where the `RecordSideEffect` was silently dropped) — **plus two round-5 regression
  tests**: a `ctx.new_uuid()` minted and returned in the *same terminal cycle* is
  frozen into history before the seal and recovered verbatim on a fresh runtime
  (`scenario_terminal_cycle_side_effect_persists_before_seal`; fails pre-fix — no
  `SideEffectRecorded` reaches history, so the `.expect` panics and the restart
  re-mints); and a fully-sealed imported history is derived to its terminal state
  and grows by no second terminal event
  (`scenario_import_sealed_history_derives_terminal_state`; fails pre-fix — the row
  is created RUNNING, re-driven, and the log grows 5 → 6 with a duplicate terminal).

**Honest fidelity caveat (disclosed in report §5.3):** the prototype records
*retryable* activity failures in an audit table + the task-row `attempt`
counter, not in the replayable log — **verified to match the PG engine**
(`queue::requeue_for_retry` stores the error on the task-queue row, never in
`harvest_events`; `ActivityFailed` reaches the event log only on terminal
retry-exhaustion via `finalize_activity_failure`). The original test-coverage
caveat is now **closed by executed tests in both directions**: a retry-produced
history round-trips through the engine's `WorkflowReplayer`
(`scenario_cross_backend_replay_retry_history`), and a genuinely PG-shaped
`ActivityStarted`-bearing history drives the SQLite runtime's reload path
(`scenario_pg_shaped_history_replays_on_sqlite`). The one residual code-read
(not executed) item is the PG engine's *internal* retry-recording mechanism —
unavoidable without a Postgres to exercise.

**Invariants:** No new `WorkflowEvent` variant, no migration, feature-gated
behind `sqlite-spike` so the default Postgres build is byte-unaffected (`cargo
tree -i rusqlite` on default features → no match; single `libsqlite3-sys
v0.36.0`, no duplicate; no-DB suite 1565 passed / 0 failed). R&D spike — never
merged to `trunk-dev` as-is. **Go/no-go verdict: feasible; recommend a separate
companion crate reusing the core, not an in-crate `StorageBackend` trait.**
