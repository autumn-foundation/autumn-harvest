## Fix — Postgres deep activity result (issue #2225)

Breaking: an activity result nested 101 to 125 levels used to complete on
Postgres. It now fails with `PayloadTooDeep`, as on SQLite. A heartbeat
checkpoint nested past 100 levels is now dropped. No migration. No new
`WorkflowEvent` variant. The upgrade guide has a new section 1.12.

The Postgres worker no longer stores an activity result nested past
`MAX_PAYLOAD_DEPTH` (100). It records a non-retryable `ActivityFailed` with
`error_type = "PayloadTooDeep"` instead. The workflow sees the failure and can
handle it. This matches the SQLite fix from issue #2216.

Before this fix, a result nested 126 levels or more was stored. `jsonb` stores
any depth, but `serde_json` reads at most 128 levels, and event storage adds
two. So every later load of the run history failed with "Invalid Json". The
workflow could not handle that error, because the load fails before workflow
code runs.

The check covers these paths:

- A regular activity result. The check runs before the byte cap, the
  offloader and the fan-out result writer. It does not depend on the byte cap,
  so an uncapped worker refuses a deep result too. A payload codec cannot hide
  the depth, because the check reads the plain result.
- A `run_transactional` result. The check runs before the commit. The
  transaction rolls back, and no user write commits.
- A local activity result. The activity ends on its first attempt. A local
  failure keeps only its message, as every local failure does.
- An external completion, from the HTTP route or from a remote task poller.
  The token fails with the same message, and the run history stays readable.
  An HTTP body of 126 levels passes the JSON parser, so this path was open to
  any holder of the token.

A heartbeat checkpoint nested past the cap is dropped with a warning. The
write still refreshes the heartbeat time, so the timeout scanner sees a live
activity. The stored checkpoint stays. The flusher does not retry a deep
checkpoint, because it never becomes valid. Before this fix, a checkpoint of
128 levels or more made every later claim of the task fail.

This fix stops new deep results. It does not repair a history that already
holds one.

The cap and the depth walk now live in `autumn_harvest::payload_depth`. The
SQLite backend uses the same code and keeps its public names. The walk uses a
heap stack, so a deep value cannot overflow the call stack.

Not covered yet: Postgres still has no depth cap at ingress (workflow input,
signal, memo, update) or on the `details` of an activity failure. Follow-up
work covers them.
