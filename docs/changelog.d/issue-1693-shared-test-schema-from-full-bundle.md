## Fix — shared test database uses the full migration bundle (issue #1693)

`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
timed out on CI. The source workflow stayed `RUNNING` and its task row stayed
queued. The task looked unclaimed, but the claim path was correct.

The shared test fixture in `integration_e2e.rs` built its database from a
hand-kept list of migrations. When the issue was filed, the list lacked
migration `20260920215812`. The completion transaction of the source then
failed on every retry and rolled back. PR #1713 added that one migration.
The list still lacked many others. Earlier CI failures (issues #1074, #1142
and #1685) had the same cause.

The fixture now applies `autumn_harvest::test_init_sql()`, which `build.rs`
generates from `migrations/`. The hand-kept list is deleted, and so is its
claim-path string guard. The three `drop_dag_runs` migration tests recreate
the `harvest_dag_runs` table, which the full bundle drops.

Two new tests guard the fix:

- `shared_test_database_schema_matches_full_migrations` builds a reference
  database from each `migrations/*/up.sql` file. It compares the columns,
  types and nullability with the shared database.
- `wait_timeout_reports_the_stuck_task_error` checks that a timed-out state
  wait names the stuck task and its last error.

Test evidence: both new tests failed before the change. `integration_e2e`
(120 tests) and the suites that use the shared fixture pass. The one
failure, `child_policy_tests::terminal_detached_spawn_setup_error_fails_workflow`,
uses its own fixture and fails on the previous commit too. That suite is
listed as not run in CI.

Test code only. No engine change and no migration.
