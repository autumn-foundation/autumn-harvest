## Phase — `cancel` on a terminal execution now answers 409, not 400 (issue #1444)

`docs/openapi.json` documents `409 Conflict` — "Execution is already in a
terminal state" — for both `POST /workflows/{id}/cancel` and
`POST /workflows/by-id/{workflow_name}/{workflow_id}/cancel`. The sibling
mutating routes, `pause_workflow` and `rerun_workflow`, honor that: both
route their `HarvestError::Config("… already terminal …")` through the
shared `conflict_from()` helper (issue #383's own state-conflict
convention) and answer 409. `cancel_workflow` instead fell through to the
generic `map_error()`, which sends `Config` to `400 Bad Request` — so
cancelling a workflow that had already completed, failed, or was cancelled
answered 400, contradicting the route's own published contract and its
sibling routes' behavior on the identical error.

Found by Snag (exploratory QA): an interrupt-tour drive against a live
`cargo dev` instance — cancel issued after the target had already reached
a terminal state — surfaced the mismatch, then a differential check against
`pause` on the same execution (same `Config` message, different status
code) pinned the cause to `cancel_workflow`'s error arm skipping
`conflict_from`.

Fix: `cancel_workflow` (`autumn-harvest-plugin/src/api.rs`) now maps its
error through `conflict_from(e)` instead of `map_error(e)` — a one-line
change. `conflict_from` already falls through to `map_error` for every
non-`Config` variant, so `NotFound` (404), `Database` (500), and
`ShardUnavailable` are unaffected; only the "already terminal" `Config`
case changes, from 400 to the documented 409. `cancel_workflow_by_id`
delegates to `cancel_workflow`, so both routes are fixed together.

No new `WorkflowEvent` variant, no migration — a pure error-mapping fix.
Regression test:
`cancel_by_id_on_terminal_execution_returns_conflict_not_bad_request`
(`autumn-harvest-plugin/tests/by_id_integration.rs`), which seeds a
`COMPLETED` execution, asserts the by-id cancel route answers 409 with
`X-Harvest-Execution-Id` set, and confirms the rejected cancel left the
execution's state untouched.
