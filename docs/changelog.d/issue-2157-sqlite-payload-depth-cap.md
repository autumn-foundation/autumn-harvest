## Fix — SQLite payload depth cap (issue #2157)

The SQLite backend refuses a start input, memo, signal payload or update
input that nests deeper than `MAX_PAYLOAD_DEPTH` (100). Ingress returns the new
`SqliteError::PayloadTooDeep` before it persists anything.

`serde_json` writes any depth but reads at most 128 levels. Before this fix, a
deep payload was stored and then every drive failed with "corrupt stored
value". The run stayed `Running`. The cap leaves room for the event wrapping.

The depth walk uses a heap stack, so a very deep value cannot overflow the
call stack. No migration. No new `WorkflowEvent` variant.

Tests: `tests/integration/payload_depth.rs` covers each ingress at and over the
cap, `signal_with_start`, and a 10,000-level value.

Breaking: ingress now refuses nesting from 101 to 127 levels too. Those
values worked before when the wrapper stayed under 128. The cap is stricter
than Postgres, which has no depth cap. The error enum gains one variant.
