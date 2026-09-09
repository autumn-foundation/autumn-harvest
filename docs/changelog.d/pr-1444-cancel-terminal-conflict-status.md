## Phase — `cancel` on a terminal execution now answers 409, not 400 (issue #1444)

`docs/openapi.json` documents `409 Conflict` — "Execution is already in a
terminal state" — for both `POST /workflows/{id}/cancel` and
`POST /workflows/by-id/{workflow_name}/{workflow_id}/cancel`. The sibling
mutating routes, `pause_workflow` and `rerun_workflow`, honor that: both
route their `HarvestError::Config("… already terminal …")` through the
shared `conflict_from()` helper (issue #383's own state-conflict
convention) and answer 409. `cancel_workflow` instead fell through to the
generic `map_error()`, which sends `Config` to `400 Bad Request` — so
cancelling a workflow that had already reached a non-cancelled terminal
state (`COMPLETED`, `FAILED`, `TIMED_OUT`, `TERMINATED`, …) answered 400,
contradicting the route's own published contract and its sibling routes'
behavior on the identical error. Re-cancelling an already-`CANCELLED`
execution is unaffected either way: `cancel_workflow_execution_collect`
treats that case as an idempotent no-op success (202, `newly_cancelled:
false`) before it ever reaches this error arm.

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

Codex review on this PR (issue #1445) caught that `conflict_from` itself was
imprecise: it mapped *every* `HarvestError::Config` to 409, not only the
"already terminal" state conflict — so a cancel that instead hit
`resolve_live_attempt_id`'s retry-chain max-depth guard (issue #843, also
surfaced as `Config`, an operational failure unrelated to execution state)
would have been mislabeled a 409 conflict too. `conflict_from` now matches
on the "is already terminal" message content — the same content-matching
convention `concurrency.rs` and `worker.rs` already use to classify this
exact error — before mapping to 409; every other `Config` message keeps the
normal mapping. Unit tests:
`conflict_from_maps_already_terminal_config_to_409` and
`conflict_from_leaves_other_config_errors_at_their_normal_status`
(`autumn-harvest-plugin/src/api.rs`).
