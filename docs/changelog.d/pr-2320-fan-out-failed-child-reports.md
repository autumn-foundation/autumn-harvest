## Phase 2262.2320 — Failed agent children keep their reports (issue #2320)

A failed child of a fan-out no longer drops the reports of its later siblings.
`fan_out` merges every successful report, then fails the batch with the first
error in input order. The error text does not change.

An inline `delegate` that fails now adds the usage of the child to the parent.
A durable agent therefore keeps the tokens and the flow labels of a failed op,
as `fold_segment` promises.

The fix issues no command and adds no patch gate. It changes only the
in-memory usage and labels of the parent.

Tests: `fan_out.rs` (fan-out, inline, two failures) and `durable_peers.rs`
(peer segment).

Known limit: the failed slot itself still loses its usage. Issue #2328 tracks it.
