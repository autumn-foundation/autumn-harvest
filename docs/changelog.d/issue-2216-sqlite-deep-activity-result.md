## Fix — SQLite deep activity result (issue #2216)

An activity result nested past `MAX_PAYLOAD_DEPTH` (100) is no longer stored.
The SQLite worker turns it into a non-retryable `ActivityFailed` with
`error_type = "PayloadTooDeep"`, the new `PAYLOAD_TOO_DEEP_ERROR_TYPE`. A local
activity ends on its first attempt with `LocalActivityExhausted`. It keeps only
the message, as every local failure does. The workflow sees the failure and can
handle it.

Before this fix, a result nested 126 levels or more was stored. Then every
drive of the run failed with "recursion limit exceeded", and `run_until_idle`
returned that error on every call. The workflow could not handle it, because
the failure was in the driver.

An activity heartbeat nested past the cap is dropped with a warning. The last
good checkpoint stays. Before this fix, a heartbeat of 128 levels or more made
every claim of the task fail.

The depth check runs before the size cap and before the audit row is written.
So a result that is too deep and too large fails as `PayloadTooDeep`, and the
audit row reads back. The depth walk does not recurse, so the check itself
cannot overflow the call stack. No migration. No new `WorkflowEvent` variant.

Tests: `tests/integration/payload_depth.rs` covers a remote result at the cap,
one level over it, at the issue depth and far past it, with arrays and
objects. It covers a local result at the cap and over it, and a deep
heartbeat. Unit tests in `worker.rs` cover the finalize path, the check order
and `local_attempt_events`.

Breaking: an activity result nested 101 to 125 levels used to complete. It now
fails with `PayloadTooDeep`, as an ingress payload of that depth does since
issue #2157. Postgres has no depth cap.
