## Testing — Seeded deep-backlog fixture and stats snapshot before drop (issue #1956)

Issue #1956 found that no committed fixture was production-shaped. The e2e
bench also dropped its databases at exit, so `pg_stat_user_tables` was lost.
This change adds the fixture and the harness, and re-runs Ledger against them.
The findings are in `docs/performance-deep-backlog.md`.

**Fixture.** `tests/integration/deep_backlog_support.rs` seeds
`harvest_task_queue` from one `u64` seed.

- 1,000,000 live task rows at Ledger scale. `MAX_LIVE_ROWS` caps a spec at 1e9.
- Queues and concurrency keys use a power skew, `idx = floor(n * u^k)`. Rank 0
  gets `(1/n)^(1/k)` of the rows: 25% of 64 queues, and 12.5% of 4096 keys.
- RUNNING rows fit the seeded fleet (64 workers x 16 slots) and each key's cap.
- The churn follows the task lifecycle: released claims and reclaimed tasks.
  Every update changes `state`, so none is HOT. The default dead ratio is 10%,
  which models autovacuum lag behind the 2% scale factor of the hygiene
  migration.
- Every value comes from `md5` of the seed. The SQL calls no volatile
  function, and rows go in hash order. One seed gives one fixture, byte for
  byte.

**Harness.** `tests/integration/pg_stats_snapshot.rs` reads
`pg_stat_statements` (scoped to the dbid) and `pg_stat_user_tables`. It waits
for other sessions to flush, then names any that did not as `PARTIAL`.
`FixtureDb::snapshot_and_drop` takes `self`, so the snapshot always comes
before the drop. The e2e bench gets the same hook behind
`HARVEST_BENCH_STATS_DIR`. A failed or slow snapshot there is a reported
failure, and the drop still runs.

**Ledger.** `autumn-harvest/scripts/deep_backlog_ledger_repro.sh` seeds a
shallow (4k) and a deep (1M) fixture on one seed. It drives the engine claim
path on each and writes `docs/perf-artifacts/deep-backlog/`.

No engine code, query, migration or `WorkflowEvent` variant changes.

Tests: `deep_backlog_fixture_tests` (new `linux` manifest row) checks the
generator and renderer with no database. On a database it checks:

- one seed gives one fixture;
- the skew, the exact live count and the dead ratio;
- that no churn update is HOT;
- the RUNNING caps;
- that the snapshot comes before the drop.

`e2e_bench_support` tests cover the hook's env parsing and labels.
`benchmarks_docs` checks that the new variable is documented.
