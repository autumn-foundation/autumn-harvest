## Phase — Rate-limit bucket GC failure reports keep `dry_run` (issue #1316)

`RateLimitBucketGcOutcome::failed` filled every field but `error` from
`Default`. `dry_run` came back `false` on every failure, even a failed
`dry_run: true` preview. `GET /admin/retention` then showed a failed
preview as a failed REAL pass — the exact conflation
`RateLimitBucketGcOutcome` (issue #1127) exists to prevent.

**The fix.** `failed` now takes `dry_run` as an argument, the way
`collected` already does. Both call sites in the janitor loop —
connection acquisition and the sweep/preview query — pass
`config.dry_run`.

**Test evidence.**
`a_failed_dry_run_pass_still_reports_dry_run_true` in
`autumn-harvest/tests/integration/rate_limit_bucket_gc_tests.rs` hides
`harvest_rate_limit_buckets` (a rename, restored before any assertion
runs) to fail the sweep query on an otherwise-healthy connection, under
`dry_run: true`, and asserts the reported outcome still says
`dry_run: true`. This covers the call site the pre-existing
`a_shard_whose_sweep_fails_reports_the_failure_instead_of_a_silent_zero`
test (connection acquisition) does not.
