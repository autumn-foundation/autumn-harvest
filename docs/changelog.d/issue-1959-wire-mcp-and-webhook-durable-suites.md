## Fix — run the MCP tools and durable webhook suites in CI (issue #1959)

`mcp_tools_integration` and `webhook_durable_integration` now run in CI from
`linux` manifest rows. Before this change, every test in them was
`#[ignore]`d, so CI never ran them. The `compileonly` row for
`mcp_tools_integration` goes, because the `linux` row also compiles it.

Three harness defects stopped both suites. Each was in the test, not in the
engine:

- Current-thread runtime: `TestApp::plugin` blocks on plugin startup, and the
  test hangs. Both suites now use the multi-thread flavor.
- Postgres 11: `TestDb` starts the image default. The worker claim query uses
  `MATERIALIZED`, which needs Postgres 12+, so no task ran. The outbound
  webhook was not delivered for this reason. Both suites now start Postgres 16.
- Migration version collision: six Harvest migrations share a version with an
  autumn-web framework migration. A second `run_pending` call skipped them, so
  the schema had no `harvest_schedules.paused_at`. Both suites now load the
  Harvest schema with `test_init_sql()`. In production, `plugin_migrations`
  resolves such collisions.

Three `mcp_tools_integration` tests had stale assertions:

- The watch test read `state` from the JSON-RPC envelope. It now reads the
  terminal frame from the `tools/call` result.
- The continue-as-new test sent its call while the predecessor still ran.
  It now waits until status reports the successor.
- The DAG start test sent no `body`. autumn-web returned a JSON-RPC error,
  which `call_tool` read as success. The test now sends `body: null`, and
  `call_tool` treats a JSON-RPC error as an error.

One engine race showed up once the suite ran: a manual or MCP DAG trigger
near startup returned 503 in 2 of 6 runs. The trigger calls
`ensure_dag_schedule` while the scheduler registers the same DAG. Both found
no row, and both inserted. The second insert failed on
`harvest_schedules_dag_name_key`. `upsert_schedule` now inserts with
`ON CONFLICT (dag_name) DO NOTHING` and uses the committed row. A new test,
`a_concurrent_dag_registration_uses_the_first_row`, holds the first insert
open until the second blocks on it.

The engine admits a declarative `#[update]` but never runs it, so
`wait=completed` times out (issue #2035). Two tests that wait for an update
stay `#[ignore]`d with that issue number. The other 9 tests run.

The allowlist drops from 6 to 4 entries, and `ALLOWLIST_MAX_LEN` to 4. No debt
entry is left. A new guard test, `issue_1959_suites_run_in_ci`, keeps both
suites wired and keeps the three harness fixes.

Tests: `mcp_tools_integration` 9 passed, 2 ignored (#2035), in 5 runs.
The DAG start test passed 8 of 8 runs after the race fix.
`webhook_durable_integration` 1 passed. `scheduler_registration_tests` 21
passed. The `ci_run_coverage` guard passes.

No migration, no route change, and no `harvest_events` change.
