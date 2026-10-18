## Fix — SQLite deep activity result (issue #2216)

An activity result nested past `MAX_PAYLOAD_DEPTH` (100) is no longer stored.
The SQLite worker turns it into a non-retryable `ActivityFailed` with
`error_type = "PayloadTooDeep"`. A local activity ends on its first attempt
with `LocalActivityExhausted`. The workflow sees the failure and can handle it.

Before this fix, a result nested 126 levels or more was stored, and then every
drive of the run failed with "recursion limit exceeded". `run_until_idle`
returned that error on every call. The workflow could not handle it, because
the failure was in the driver.

The check runs before the size cap and before the audit row is written. So the
audit row holds the failure and reads back too. The depth walk uses a heap
stack, so a very deep result cannot overflow the call stack. No migration. No
new `WorkflowEvent` variant.

Tests: `tests/integration/payload_depth.rs` covers a remote and a local result
at the cap, one level over it, and far past it. Unit tests in `worker.rs` cover
the finalize path and `local_attempt_events`.

Breaking: an activity result nested 101 to 125 levels used to complete. It now
fails with `PayloadTooDeep`, as an ingress payload of that depth does since
issue #2157. Postgres has no depth cap.
