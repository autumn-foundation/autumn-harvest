## Fix — workflow-side payload depth (issue #2224)

A value that workflow code hands to the engine can no longer nest past
`MAX_PAYLOAD_DEPTH` (100). Before this fix, such a value was stored. Then every
later drive of the run failed with "recursion limit exceeded".

`WorkflowContext` now refuses the value at the call site. It returns the new
`HarvestError::PayloadTooDeep`, as it does for the size caps. The workflow can
handle the error. No command and no event is written. The check covers:

- activity and local activity input, including races and fan-outs
- child workflow input, including detached, timeout and fan-out spawns
- continue-as-new input
- external signal payloads
- side-effect values

An update result has no call site. A result nested past the cap fails the
update with a message. A workflow result has no call site either. On SQLite, a
result nested past the cap fails the run with a non-retryable `PayloadTooDeep`
failure. The run does not retry, because the result is the same on every
attempt. The terminal event is `WorkflowFailed`, and the history reads back.
Postgres workflow results are not capped yet. The depth walk does not recurse, so the check cannot overflow the stack.

The cap and its walk moved to `autumn_harvest::payload_depth`. The SQLite crate
re-exports both names. A `PayloadTooDeep` refusal is deterministic, so a saga
unwind treats it as it treats `PayloadTooLarge`. No migration. No new
`WorkflowEvent` variant.

Breaking: a workflow-side value nested 101 levels or more used to be accepted.
It now fails with `PayloadTooDeep`. This applies to Postgres too, which had no
depth cap.

Tests: `payload_depth_tests` in `context.rs` covers the call sites. The SQLite
`workflow_payload_depth.rs` covers each path in the issue at the cap and over
it.
