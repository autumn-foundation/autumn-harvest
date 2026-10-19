## Feature — Durable routes: an Autumn route that runs as a keyed workflow (PR #2223)

The new `durable_route` module of `autumn-harvest-plugin` adds the
`DurableRoute` extractor. A route hands its work to a registered workflow,
keyed by the request's `Idempotency-Key`. A retry with the same key attaches
to the same run and gets its stored result, so side effects do not run again.
A request that outlives its wait budget answers `202` with `Retry-After`, or
`409` when Autumn's idempotency layer is on, because that layer caches every
2xx. This fills the "durable HTTP endpoints" gap of the October 2026
feature-gap report.

- The dedupe store is the start-idempotency claim (issue #808), reached
  through the keyed transactional start (issue #763). The claim key is a
  SHA-256 digest of the workflow, the tenant, the mandatory scope and the
  raw key. The raw key never reaches the database.
- A fingerprint of the canonical input rides in the `workflow_id`. A reused
  key with another body answers `422` and starts nothing.
- The start runs in its own task and drops its connection before the wait.
  The wait takes a permit from `HarvestPlugin::durable_route_max_waiters`
  (default 32) and follows continue-as-new chains. A same-type chain takes
  one business-id lookup. `HarvestPlugin::durable_route_max_chain_steps`
  (default 1024) caps the changes of workflow type that one request follows.
- The wait ends 2 s, and every read after the start ends 1 s, before
  Autumn's request deadline, counted from when the route reads the request.
  Under Autumn's idempotency layer the route counts half the deadline.
- An input over the workflow's input cap answers `413` before any I/O.
- Problem bodies use fixed text. They never hold the workflow error message,
  the key, the scope or the input. `failure_status` maps a typed
  `WorkflowFailure` class to a final 4xx or 5xx status.

No migration. No new `WorkflowEvent` variant. No `harvest_events` write. No
management API route. `sha2` becomes a plain dependency of the plugin.

Tests: unit and property tests in `durable_route.rs`; Postgres tests in
`autumn-harvest-plugin/tests/durable_route_integration.rs`; docs guards in
`autumn-harvest-plugin/tests/durable_route_docs.rs`. Guide:
`docs/durable-routes.md`. Design: `DESIGN-durable-routes.md`.
