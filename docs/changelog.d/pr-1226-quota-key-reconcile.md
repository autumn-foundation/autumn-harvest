## Phase — Backfill quota_key for pre-upgrade executions (issue #1226)

Follow-up from a Codex automated-review finding on PR #1221 (issue #946,
per-tenant resource quotas): the migration that added
`harvest_workflow_executions.quota_key` shipped with no backfill, by
design — the key-resolution expression is Rust application code, not
something a pure-SQL migration can evaluate. The practical effect: an
execution already `RUNNING`/`PAUSED` before its workflow type declared a
`QuotaPolicy` kept `quota_key = NULL` forever, invisible to
`quota::load_quota_usage`'s counting. A tenant with enough such
pre-existing runs could admit a full new quota limit on top of usage the
engine could not see — a genuine quota-bypass-during-upgrade-window gap.

**The fix.** A new module, `autumn-harvest/src/quota_reconcile.rs`, adds a
periodic, shard-local sweep. It finds non-terminal rows with
`quota_key IS NULL` and backfills them using the exact same
`quota::resolve_quota_key` function the live admission path calls, so a
backfilled value can never drift from what a fresh admission would
compute. A row with no declared policy, an unresolvable key, or a
resolved key exceeding `quota::MAX_QUOTA_KEY_BYTES` is left `quota_key =
NULL` — the same fail-open outcome admission itself would produce.

**Design decision: periodic, not startup-once.** A one-time startup pass
would only close the rollout-window gap the migration describes. It would
miss a `QuotaPolicy` declared on an already-running workflow type
mid-uptime, with no accompanying restart. A periodic sweep (mirroring
`poison_pill.rs`/`sessions.rs`) closes both cases with one mechanism, and
never touches the startup path, so it cannot delay boot. The sweep is
wired into `worker.rs`'s `spawn_monitoring_tasks`, one per assigned shard
pool, on the existing `worker_heartbeat_interval` cadence. The batch size
(`QUOTA_RECONCILE_DEFAULT_BATCH = 200`) is a fixed internal constant
rather than a new `WorkerConfig` knob — `WorkerRuntimeConfig` is
constructed as a bare struct literal with no `Default`/spread at dozens
of call sites across the test suite, and this value only needs to be
"bounded", not operator-tunable.

**Migration `20260910192721_harvest_quota_reconcile_candidate_index`.** A
code-review pass caught the candidate query's `state IN ('RUNNING',
'PAUSED')` silently falling back to a full sequential scan of
`harvest_workflow_executions` on every tick: the existing
`idx_harvest_we_state` index covers only `state = 'RUNNING'`, not
`PAUSED`. This migration adds a dedicated partial index,
`idx_harvest_we_quota_reconcile_candidates`, on the sweep's exact
predicate (`quota_key IS NULL AND state IN ('RUNNING', 'PAUSED')`). The
index self-shrinks: a row leaves it the moment its `quota_key` is
backfilled, so it always covers exactly the current candidate set, never
the table's full history.

**Review.** Developed red/green/refactor: a deliberate bug in the
over-cap branch (skipping the `quota_key_over_cap` bound check) was
confirmed to make its test fail before the real implementation was
restored. Reviewed from four independent angles:

- **Correctness** — no bugs in the core logic; confirmed `resolve_backfill`
  matches the live admission path's resolution exactly, including
  stamping a key even when the policy declares no caps. Fixed a cosmetic
  metric over-count: `summary.backfilled` is now only incremented when
  the UPDATE's `rows_affected` is nonzero, closing a rare double-count
  under two concurrent sweeps racing the same row.
- **Concurrency/ops** — shutdown, error-handling, idempotency, and
  boot-path claims all hold and mirror `poison_pill.rs`/`sessions.rs`.
  Found and fixed the sequential-scan issue above.
- **Security/data-integrity** — no exploitable issues. The over-cap bound
  check, bind-parameter discipline (no string-interpolated SQL), and "no
  second resolver" guarantee all verified to hold.
- **Test-coverage** — the three acceptance-criteria tests are
  non-vacuous and each genuinely isolates the effect it claims. Found and
  fixed a CRITICAL process gap: the new integration test file was never
  added to `.github/ci/integration-suites.txt`, so it compiled but never
  actually ran anywhere; the repo's own `ci_run_coverage` guard now
  passes. Added a test proving `batch_size` actually bounds one sweep's
  work (previously unexercised).

**Scope.** `harvest_dead_letters.quota_key` backfill is explicitly out of
scope for this pass, per issue #1226's own draft acceptance criteria,
which call it lower priority; documented as a deliberate follow-up in
`quota_reconcile.rs`'s module doc, not silently dropped.

**Tests.** Unit tests for the pure `resolve_backfill` decision logic
(no DB). Integration suite `quota_reconcile_tests.rs` (real Postgres via
testcontainers) proving: a pre-upgrade active execution gets correctly
backfilled; a tenant's combined pre- and post-upgrade usage is correctly
capped once reconciliation has run (before: `check_quota` sees no
violation at usage 1 of 3; after: a real violation at usage 3 of 3); a
second sweep is a no-op; terminal rows are never touched; an over-cap key
is left `NULL`; a workflow type with no declared policy is left `NULL`;
`batch_size` bounds one sweep and the remainder finishes on a later one.

No new `WorkflowEvent` variant, no replay impact. Doc updates:
`quota.rs`'s module doc, the original migration's comment,
`schema.rs`'s `quota_key` column doc, `docs/upgrading/0.5.0.md`'s
migration inventory table, and `docs/rnd/sqlite-feasibility.md`'s
live-audited Postgres-coupling inventory (recomputed counts).
