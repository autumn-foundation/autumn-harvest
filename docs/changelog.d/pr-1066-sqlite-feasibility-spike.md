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
  columns substitute for `INTERVAL` deadline arithmetic. **Per-decision-cycle
  persistence is transactional** (Codex P1 durability fix): a cycle's derived
  event and its paired task/timer row commit in one `SQLite` transaction
  (`apply_commands`), and a drained activity's terminal event + task-state flip
  commit in one transaction (`drain_ready`), mirroring the PG engine, which
  persists an event and its task-queue row together. This closes the
  append-before-enqueue and result-before-finish crash windows (which previously
  wedged the reload at `SpikeError::Stuck`); the sole residual window is a crash
  *mid-body*, which a productized runtime would recover via the engine's
  heartbeat-timeout + poison-pill reclaimer (out of a single-process spike's
  scope).
- **A 9-test smoke suite** `autumn-harvest/tests/integration/sqlite_spike_tests.rs`
  (9/9 pass, no Docker via `rusqlite`'s `bundled` feature): activity retry,
  durable timer across process restart, signal delivery, deterministic replay
  after a simulated crash, **cross-backend replay executed in both directions** —
  a SQLite-written history (success-path *and* a retry-produced history) replays
  cleanly on the engine's own `WorkflowReplayer` path, and a genuinely PG-shaped
  `ActivityStarted`-bearing history drives the SQLite runtime's own reload path to
  the identical outcome with no duplicate activity execution (both backends
  serialize the same `WorkflowEvent` via `serde_json`; histories are
  replay-equivalent, not byte-identical event streams, because the matcher skips
  `ActivityStarted`) — **plus two persistence-atomicity regression tests**: the
  event/queue-timer-row invariant survives a reload
  (`scenario_atomic_persist_event_and_row_never_out_of_sync`), and a batch that
  fails partway rolls back *both* the event and its row
  (`scenario_atomic_persist_rolls_back_the_whole_batch_on_unsupported_command` —
  a both-or-neither proof that fails on the pre-fix per-command-autocommit code).

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
