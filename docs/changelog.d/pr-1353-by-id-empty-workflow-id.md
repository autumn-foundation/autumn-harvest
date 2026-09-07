## Phase — Reject an empty `workflow_id` across the by-id route family (issue #1353)

`workflow_id: ""` was accepted by `POST /workflows/{name}/start` with no
validation, as a value distinct from omitting the field (omitting it
auto-generates a UUID; an explicit empty string was taken literally as the
business id). Once such a run existed, `GET
/workflows/by-id/{workflow_name}/{workflow_id}` -- the base "describe by
business id" route (issue #805) -- 404'd for it ("no route matches"), while
every sibling by-id route (`/stack`, `/children`, `/result`, `/signal`,
`/query`, `/cancel`, `/pause`, `/resume`) resolved it fine and returned 200.

**Root cause.** `matchit` (the router `axum` uses, pinned at 0.8.4) will bind
an empty path segment to a named parameter everywhere except the FINAL
segment of a route. `GET .../{workflow_name}/{workflow_id}` ends on the
`{workflow_id}` capture, so a trailing-slash request (`.../order_flow/`)
never reaches a handler at all -- axum answers a structural 404 before any
application code runs. The sibling routes carry a further literal segment
after `{workflow_id}` (`.../{workflow_id}/stack`), so the SAME empty id lands
on a non-final segment there and matches. Confirmed against the pinned
`matchit` version directly (a minimal reproduction outside this crate); a
later `matchit` patch release stops matching the empty segment in either
position, which would have silently broken the sibling routes too -- this
router quirk is not a contract this codebase can lean on either way it goes.

**The fix rejects the value at both ends, not just the one route the issue
reported.**

1. **Creation.** `start_workflow` and `rerun_workflow`'s `workflow_id`
   override both reject an explicit empty string with `400` (`"workflow_id
   must not be empty"`), via a shared `reject_empty_workflow_id` helper.
   Omitting the field is untouched. This is the root-cause fix: no new run
   can be created with an unaddressable business id going forward.
2. **Resolution.** `resolve_workflow_by_business_id` -- the resolver shared
   by every by-id route -- now rejects an empty `workflow_id` with `400`
   before any shard fan-out. This makes the whole ten-route family answer
   uniformly for an empty id, independent of which router-matching quirk let
   a particular request through, and covers any row that predates this
   change.
3. **Routing.** A new literal route, `GET /workflows/by-id/{workflow_name}/`
   (the trailing-slash form), delegates to the existing `get_workflow_by_id`
   handler with `workflow_id = ""`, which the resolver fix above now turns
   into the same `400` every sibling route gives. Without this registration
   the request still never reaches a handler -- only 400 is answered, never
   silently the wrong success shape.

**Scope.** No new `WorkflowEvent` variant, no migration, no `harvest_events`
write -- pure input validation plus one additional route registration. The
new route is wired into every manifest a by-id route touches: `CLASSIFIED_ROUTES`
/ `EXCLUDED_ROUTES` / `ALL_MUTATION_ROUTES` (`autumn-harvest/src/audit.rs`,
classified `ReadOnly`, never audited -- it never reads or writes, mirroring
the existing name-only guard route from issue #805/#776), `management_api_routes()`,
and `docs/api-contract.json` (regenerated `docs/openapi.json` /
`autumn-harvest-plugin/openapi.json` to match).

**Behavior change to note.** `workflow_id: ""` is no longer a startable
value; a caller that relied on the empty string being accepted now gets a
`400` at start time instead of a silently-degraded run. This is the point of
the fix, not an accident -- an identifier the by-id route family could not
address consistently was never a safe value to hand out as "the real id" of
a live, controllable workflow.

Tests, red → green → refactor: `autumn-harvest-plugin/tests/by_id_integration.rs`
(`start_with_empty_workflow_id_is_rejected_400`,
`by_id_base_route_trailing_slash_rejects_empty_workflow_id` -- reproduces the
issue's exact repro path, `by_id_sibling_route_rejects_empty_workflow_id`,
`start_with_omitted_workflow_id_still_auto_generates` as a regression guard)
and `autumn-harvest-plugin/tests/workflow_rerun_integration.rs`
(`rerun_with_empty_workflow_id_override_is_rejected_400`, R-66).
