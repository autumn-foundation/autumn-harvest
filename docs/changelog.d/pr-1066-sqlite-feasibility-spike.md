## R&D Spike — SQLite edge/local-first harvest feasibility (issue #966)

**Throwaway R&D feasibility spike — never merged to `trunk-dev` as-is.** A
timeboxed study answering one question: *can the backend-neutral determinism
core (`run_workflow`, `WorkflowEvent`, deterministic replay) be driven by a
non-Postgres, embedded, single-writer persistence layer?* The answer:
**feasible, but do not port in-crate** — recommend a separate companion crate
that depends on the core; decline the in-crate `StorageBackend` trait (its cost —
a wide-but-half-empty trait plus a ~doubled integration test matrix, **not**
hot-path dispatch, which a monomorphized generic makes zero-cost — falls entirely
on the shipped Postgres path).

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
  clock-reset windows. **Any post-claim error requeues the task to `PENDING`, never
  strands it `RUNNING`** (Codex P2, round 6): the claim commits `RUNNING` in a
  *separate* transaction from the post-body persistence (the body runs between
  them), so `drain_ready` now requeues the task on **any** subsequent error — an
  *unregistered* activity (a recoverable application condition: register it and
  re-run) or a *transient* post-body persistence error — via `queue::mark_pending`
  before surfacing the error, so a re-run re-drains it rather than wedging at
  `SpikeError::Stuck` (later claims select only `PENDING`). The **sole residual
  window** is now a *hard process crash* between the claim and that requeue — no
  in-process handler runs — which a productized runtime would recover via the
  engine's heartbeat-timeout + poison-pill reclaimer (out of a single-process
  spike's scope). **The import path
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
  and re-driving it into a duplicate terminal. **The import terminal-recognition
  helpers recognize the FULL terminal-variant set** (Codex P2, round 7): the
  open-vs-closed activity check now recognizes `ActivityTimedOut` (and the
  external-completion terminals `ActivityCompletedExternally`/
  `ActivityFailedExternally`) alongside `ActivityCompleted`/`ActivityFailed`,
  mirroring the authoritative engine `HistoryMatcher::scan_activity_terminal` — so
  importing a PG history whose scheduled activity **timed out** no longer misreads
  it as *open*, re-materializes a stale `spike_tasks` row, and (once the workflow
  swallows the replayed timeout and reaches the `drain_ready` pass) corrupts the
  log with a duplicate `ActivityCompleted`; the timer check recognizes
  `TimerCancelled` (issue #768) as terminal alongside `TimerFired`; and the sealed
  workflow-terminal scan recognizes the engine's full `is_terminal_lifecycle` seal
  set — `WorkflowCompleted`/`WorkflowFailed` are represented directly, while a
  terminal the COMPLETED/FAILED-only prototype cannot model (`WorkflowCancelled`/
  `WorkflowExecutionTimedOut`/`WorkflowContinuedAsNew`/`WorkflowResetTerminated`)
  is **rejected** outright with `SpikeError::Unsupported` before any DB write
  (never silently mis-materialized as a RUNNING import a later re-drive would
  append a second terminal to), consistent with the driven path already rejecting a
  `ContinueAsNew` command. **One documented known limitation
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
  recommendation. **Three further prototype edges surfaced by later review rounds —
  activity-body panic containment (`worker.rs:156`), timer-vs-activity drain-order
  replay divergence (`worker.rs:108`), and its **multi-timer variant**
  (`queue.rs:214`; two-plus classic timers armed in one suspension batch fire
  by `fire_at` deadline, not command order, so a later-command earlier-deadline
  timer's `TimerFired` interleaves ahead and `match_timer_strict` for the first
  timer wedges at `Stuck`) — are likewise documented in report §5.1 and
  deliberately declined (not fixed) per the spike's timebox: all are inherent to
  the minimal single-process prototype, are unexercised by the four required
  scenarios (none arms multiple classic timers in one batch — `scenario_two_timers`
  arms them sequentially), do not touch the default Postgres build, and are handled
  natively by the productized engine (issue #782 panic containment; command-order
  persistence + a `HistoryMatcher` that tolerates an interleaved `TimerFired` for a
  *different* timer id).**
- **One further review edge FIXED (Codex P1) — engine-detected non-determinism is
  now DETECT-AND-HELD (non-sealing, recoverable), not sealed FAILED.** After
  round-5 switched `drive_one_cycle` to `run_workflow_with_state`, whose `Failed`
  outcome carries `non_deterministic_details`, the `Failed` arm still appended a
  durable `WorkflowFailed` unconditionally — so a replay divergence (e.g. a
  mismatched cross-backend import) permanently sealed the run FAILED, violating
  harvest's core replay-safety invariant (issue #603 / Phase 3.45: engine-detected
  non-determinism must never terminally fail a workflow — it blocks non-terminally
  so a rollback / re-import / code-fix can recover). The arm now branches on
  `non_deterministic_details`: on a divergence it opens no persist transaction at
  all (no `WorkflowFailed`, no divergent-cycle bookkeeping persisted, execution
  left non-terminal) and surfaces the distinct, recoverable
  `SpikeError::NonDeterministic` — a scoped analogue of the engine's non-terminal
  ND-block gate (the throwaway prototype captures only the non-sealing/recoverable
  property, not the engine's backoff + `nd_blocked_*` columns). Regressed by
  `scenario_nondeterministic_import_is_recoverable_not_sealed` (an
  `other_activity`-named imported schedule vs. the handler's `work` diverges →
  recoverable ND, no `WorkflowFailed`, no bookkeeping, re-drive re-detects; fails
  pre-fix, which sealed FAILED).
- **A 20-test smoke suite** `autumn-harvest/tests/integration/sqlite_spike_tests.rs`
  (20/20 pass, no Docker via `rusqlite`'s `bundled` feature): activity retry,
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
  is created RUNNING, re-driven, and the log grows 5 → 6 with a duplicate terminal)
  — **plus one round-6 regression test**: a workflow scheduling an *unregistered*
  activity requeues the claimed task to `PENDING` and, once the handler is
  registered, drains it to `Completed`
  (`scenario_unregistered_activity_requeues_and_drains_after_registration`; fails
  pre-fix — the claimed task is stranded `RUNNING`, later claims select only
  `PENDING`, so the re-run wedges at `Stuck`) — **plus two round-7 regression
  tests** closing the import terminal-variant completeness gap: an imported
  timed-out activity (`Scheduled → Started → ActivityTimedOut`) is recognized as
  terminal, materializes no stale task row, and drives without appending a
  duplicate `ActivityCompleted`
  (`scenario_import_timed_out_activity_is_terminal_not_re_materialized`; fails
  pre-fix — the timed-out activity reads as *open*, a stale task is materialized,
  and `drain_ready` corrupts the log with a second terminal); and a sealed import
  whose terminal the prototype cannot model (`WorkflowCancelled`,
  `WorkflowExecutionTimedOut`) is rejected with `SpikeError::Unsupported` before any
  DB write while a representable sealed import still succeeds afterward
  (`scenario_import_sealed_unrepresentable_terminal_is_rejected`; fails pre-fix —
  the unrepresentable seal falls through to the in-flight branch and is seeded
  `RUNNING`) — **plus one round-9 regression test (Codex P2, AC4 fidelity)**: the
  SQLite runtime now threads the stored `workflow_name` into the full executor
  entry point (`run_workflow_with_state_history_policy_and_caps`) instead of the
  thin `run_workflow_with_state` (which hardcoded an empty name), so
  `ctx.info().workflow_type` / `ctx.workflow_type()` observe the real registered
  handler name and stay **cross-backend-consistent** with the engine's own
  `WorkflowReplayer` (which threads `workflow_name` from the history snapshot)
  (`scenario_ctx_info_workflow_type_is_cross_backend_faithful`; fails pre-fix —
  the SQLite backend observed `""` and recorded `""` into the activity input, so a
  PG-shaped import would diverge against the engine's recomputed real name).

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
