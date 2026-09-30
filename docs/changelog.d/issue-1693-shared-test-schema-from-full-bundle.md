## Fix — shared test database uses the full migration bundle (issue #1693)

`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
timed out on CI. The source workflow stayed `RUNNING` and its task row stayed
queued. That looked like a task that no worker claims. The claim was fine.

The shared test fixture in `integration_e2e.rs` built its database from a
hand-kept list of migrations. The list lacked one migration. The completion
transaction then failed on every retry and rolled back the source. Migration
`20260920215812` was the trigger in this case. The same omission caused
issues #767, #1074, #1142 and #1685.

The fixture now applies `autumn_harvest::test_init_sql()`, which `build.rs`
generates from `migrations/`. The hand-kept list is deleted. The three
`drop_dag_runs` migration tests recreate the `harvest_dag_runs` table, which
the full bundle drops.

Two new tests guard the fix:

- `shared_test_database_schema_matches_full_migrations` compares every column
  of the shared database with a database built from the full bundle.
- `wait_timeout_reports_the_stuck_task_error` checks that a timed-out state
  wait names the stuck task and its last error.

Test code only. No engine change and no migration.
