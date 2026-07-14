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
  prior-art gap analysis vs. Temporal/DBOS/Inngest/Hatchet/Restate/Oban).
- **A disposable, feature-gated prototype** at `autumn-harvest/src/sqlite_spike/`
  (`mod`/`schema`/`store`/`queue`/`worker`) behind the new `sqlite-spike =
  ["dep:rusqlite"]` Cargo feature. It reuses the backend-neutral determinism
  core **wholesale** (zero core changes) and reimplements only persistence on
  embedded SQLite: single-writer `BEGIN IMMEDIATE` claim substitutes for
  `SELECT … FOR UPDATE SKIP LOCKED`; polling substitutes for LISTEN/NOTIFY;
  app-minted UUIDs substitute for `gen_random_uuid()`; explicit epoch `fire_at`
  columns substitute for `INTERVAL` deadline arithmetic.
- **A 5-test smoke suite** `autumn-harvest/tests/integration/sqlite_spike_tests.rs`
  (5/5 pass, no Docker via `rusqlite`'s `bundled` feature): activity retry,
  durable timer across process restart, signal delivery, deterministic replay
  after a simulated crash, and a cross-backend replay check (a SQLite-written
  history replays cleanly on the engine's own `WorkflowReplayer` path because
  both backends serialize the same `WorkflowEvent` via `serde_json`).

**Honest fidelity caveat (disclosed in report §5.3):** the prototype records
*retryable* activity failures in an audit table + the task-row `attempt`
counter, not in the replayable log — **verified to match the PG engine**
(`queue::requeue_for_retry` stores the error on the task-queue row, never in
`harvest_events`; `ActivityFailed` reaches the event log only on terminal
retry-exhaustion via `finalize_activity_failure`). The genuine caveat is test
coverage: the cross-backend replay test only exercised a success-path,
single-attempt history, so retry-history round-tripping is argued from source,
not proven by an executed test.

**Invariants:** No new `WorkflowEvent` variant, no migration, feature-gated
behind `sqlite-spike` so the default Postgres build is byte-unaffected (`cargo
tree -i rusqlite` on default features → no match; single `libsqlite3-sys
v0.36.0`, no duplicate; no-DB suite 1565 passed / 0 failed). R&D spike — never
merged to `trunk-dev` as-is. **Go/no-go verdict: feasible; recommend a separate
companion crate reusing the core, not an in-crate `StorageBackend` trait.**
