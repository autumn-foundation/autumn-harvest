# CI DB suite allowlist

A DB suite must run in CI from a `linux` or `allos` row in
`.github/ci/integration-suites.txt`. The guard
`autumn-harvest/tests/integration/ci_run_coverage.rs` checks this. A suite
without a row must be on its `ALLOWLIST`, with a reason.

This page is the tracking table for that allowlist (issue #1799). Each entry
has an owner, or "won't wire" and a reason:

- **Owner** (`@login`, `#issue`): the suite is debt. The owner wires it, then
  removes its entry here and in `ALLOWLIST`.
- **won't wire**: CI does not run the suite from the manifest by design. The
  reason tells where or how it runs.

The guard test `tracking_table_lists_every_allowlist_entry_with_an_owner_or_reason`
compares this table with `ALLOWLIST`. Change both in the same commit.

## Rules

- To wire a suite, add its manifest row. Then remove its `ALLOWLIST` entry and
  its row here, and lower `ALLOWLIST_MAX_LEN` by one.
- To add an entry, raise `ALLOWLIST_MAX_LEN` and add a row here, in the same
  change. Give the reason in both places.
- A debt entry needs an owner. A reason that `BY_DESIGN_REASONS` does not
  list is debt, so the guard asks for an owner.

## Table

<!-- allowlist-tracking:begin -->
| Suite | Owner | Reason |
|---|---|---|
| `core:audit_log_unexported_idx_write_cost_perf` | won't wire | Manual evidence harness (issue #1272). Its one test is `#[ignore]`d by design (500k-row fixture, `VACUUM FULL`). Run it by hand with `--ignored`. |
| `core:chaos_tests` | won't wire | Runs nightly in `.github/workflows/chaos.yml` with the `chaos` feature (issue #940). It is too slow for each PR. |
| `plugin:connector_kafka_broker` | won't wire | Runs in CI from its own Linux step in `ci.yml` (issue #944). The step installs `librdkafka` build dependencies first. |
| `plugin:outbox_start_relay_perf` | won't wire | Manual `pg_stat_statements` evidence (issue #1620). Run it by hand, as `docs/performance-outbox-start-relay.md` tells. |
<!-- allowlist-tracking:end -->
