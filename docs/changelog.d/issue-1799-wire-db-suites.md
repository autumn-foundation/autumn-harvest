## Fix — wire five resilience DB suites into CI and lock the allowlist (issue #1799)

Five DB suites from issue #1799 now have `linux` manifest rows:
`scheduler_ha_tests`, `poison_pill_tests`, `signal_tests`,
`replayer_integration_tests` (with `testing`), and the plugin suite
`erase_payloads_integration`. Before this change, CI compiled them but never
ran them.

The run-coverage guard in `ci_run_coverage.rs` has four fixes:

- `ALLOWLIST_MAX_LEN` must equal the allowlist length. A removal must also
  lower the cap, so the list cannot grow back without a reviewed change.
- An allowlisted suite that a manifest row covers is now a stale entry. Before
  this change, the guard skipped covered suites before it read the allowlist.
  Two dead entries hid there: `core:cross_workflow_signal_tests` and
  `plugin:batch_operations_integration`.
- A row filter credits each module whose name contains it. libtest matches
  filters as substrings, so this matches what CI runs. The `pause_tests` row
  already runs `scheduler_auto_pause_tests`, so that entry is gone too.
- A core row covers a module only when it enables every feature gate of that
  module. Before, the guard checked only `db` and `testing`. A row without
  `chaos` could credit `chaos_tests` and run zero tests.

Three manifest rows ran a suite a second time, because a shorter filter
already selects it. They are removed: `cross_workflow_signal_tests` (run by
`signal_tests`), and `activity_pause_tests` and `queue_pause_tests` (run by
`pause_tests`). Each suite now runs once.

The allowlist drops from 63 to 55 entries. The cap drops from 75 to 55.

Shard balance: `docs/audits/shard-weight-drift.py` reports the heaviest
`test-db-linux` shard at 307 tests. It was 306 before. Without the three row
removals, the new rows raise it to 444.

No migration, no route change, and no `harvest_events` change.

Tests: four new guard tests, one rewritten cap test, one test ported to
`core_required_features`, and a stricter stale check in
`every_db_gated_test_has_a_ci_run_step_or_is_allowlisted`. The red commits
show each failure before its fix. The five suites pass locally against Docker
Postgres with `--test-threads=1`, with and without the `testing` feature.
