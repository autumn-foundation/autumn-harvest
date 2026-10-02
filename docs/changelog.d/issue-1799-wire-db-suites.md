## Fix — wire five resilience DB suites into CI and lock the allowlist (issue #1799)

Five DB suites from issue #1799 now have `linux` manifest rows:
`scheduler_ha_tests`, `poison_pill_tests`, `signal_tests`,
`replayer_integration_tests` (with `testing`), and the plugin suite
`erase_payloads_integration`. Before this change, CI compiled them but never
ran them.

The run-coverage guard in `ci_run_coverage.rs` has three fixes:

- `ALLOWLIST_MAX_LEN` must equal the allowlist length. A removal must also
  lower the cap, so the list cannot grow back without a reviewed change.
- An allowlisted suite that a manifest row covers is now a stale entry. Before
  this change, the guard skipped covered suites before it read the allowlist.
  Two dead entries hid there: `core:cross_workflow_signal_tests` and
  `plugin:batch_operations_integration`.
- A row filter credits each module whose name contains it. libtest matches
  filters as substrings, so this matches what CI runs. The `pause_tests` row
  already runs `scheduler_auto_pause_tests`, so that entry is gone too.

The `signal_tests` row also runs `cross_workflow_signal_tests`. Its own row is
removed, so CI runs that suite once, not twice.

The allowlist drops from 63 to 55 entries. The cap drops from 75 to 55.

No migration, no route change, and no `harvest_events` change.

Tests: four new guard tests, red before the fix and green after. The five
suites pass locally against Docker Postgres with `--test-threads=1`.
