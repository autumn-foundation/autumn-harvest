## Fix — Postgres deep activity result (issue #2225)

An activity result nested past `MAX_PAYLOAD_DEPTH` (100) is no longer stored.
The Postgres worker turns it into a non-retryable `ActivityFailed` with
`error_type = "PayloadTooDeep"`. The workflow sees the failure and can handle
it. This matches the SQLite fix from issue #2216.

Before this fix, a result nested 126 levels or more was stored. `jsonb` accepts
any depth, but `serde_json` reads at most 128 levels. So every later load of
the run history failed with "Invalid Json". The workflow could not handle the
error, because it happened in the driver.

The check covers every path that records an activity result:

- A regular activity result. The check runs before the byte cap, the
  offloader and the fan-out result writer. So a deep value never reaches the
  payload store. The check does not depend on the byte cap, so an uncapped
  worker refuses a deep result too.
- A `run_transactional` result. The check runs before the commit. The
  transaction rolls back, and no user write commits.
- A local activity result. The activity ends on its first attempt with
  `LocalActivityFailed` and `LocalActivityExhausted`. A local failure keeps
  only its message, as every local failure does.

An activity heartbeat checkpoint nested past the cap is dropped with a warning.
The flush still refreshes the heartbeat time, so the timeout scanner sees a live
activity. The last good checkpoint stays. Before this fix, a deep checkpoint made
every later claim of the task fail.

This fix stops new deep results. It does not repair a history that already
holds one.

The cap and the depth walk now live in `autumn_harvest::payload_depth`. The
SQLite backend uses the same code and keeps its public names. The walk uses a
heap stack, so a very deep value cannot overflow the call stack. No migration.
No new `WorkflowEvent` variant.

Tests: `tests/integration/payload_depth_tests.rs` covers a remote result at 126
levels and at the cap, an uncapped worker, the offloader, a local activity, a
transactional activity and a deep heartbeat. Unit tests in `payload_depth.rs`
check the walk against a recursive oracle.

Breaking: an activity result nested 101 to 125 levels used to complete on
Postgres. It now fails with `PayloadTooDeep`, as on SQLite. Postgres still has
no depth cap at ingress. Follow-up work covers workflow input, signals and
external completions.
