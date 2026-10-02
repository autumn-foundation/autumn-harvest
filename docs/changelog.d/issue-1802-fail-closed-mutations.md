## Security — Mutating routes fail closed outside `dev` (issue #1802)

**Breaking default.** Outside the `dev` profile, a management API with no
declared auth boundary now refuses every mutating route with `401`. Before,
only admin-gated routes did. Workflow start, signal, reset and update, DAG
trigger, schedule changes, external-activity callbacks and worker drain were
open to any caller. This maps to OWASP API Security Top 10 2023, API2 and API5.

**What shipped.**

- `require_classified_mutation_auth`, a route layer on `harvest_api_router`.
  It reads the class from `CLASSIFIED_ROUTES` through `classify_route`, so an
  unclassified route fails closed. It runs on matched routes only, so an
  unknown path still answers `404`.
- `require_mutation_auth_by_method` on every Vantage `/ui` route and every
  mutating MCP tool route. Any method except `GET`, `HEAD` and `OPTIONS`
  counts as a mutation.
- The gate admits a request in the `dev` profile, under a declared auth
  boundary, with a verified scoped token, with an admin session, or under the
  opt-out.
- The opt-out: `HarvestPlugin::allow_unauthenticated_mutations()`,
  `StandaloneAdminAuth::allow_unauthenticated_mutations()` and
  `HarvestApiState::set_allow_unauthenticated_mutations`. Startup logs a
  `tracing::warn!` while it opens the routes.
- `boot::unauthenticated_mutations_open` is the one predicate for the gate,
  the warning and `preflight`. The `admin_auth_boundary` preflight check gains
  an `unauthenticated_mutations` field.
- The `dev` warnings now name the mutating routes too.

**Docs.** `docs/security-posture.md` gains a "Fail-closed mutations" section.
`docs/upgrading/0.7.0.md` describes the migration path. `docs/mcp-tools.md`
reflects the new MCP default.

**Invariants.** No new `WorkflowEvent` variant, no migration, no new route, no
`harvest_events` write.

**Tests.** `tests/security.rs` pins start, signal, reset, DAG, schedule,
external-activity and drain routes at `401` with no auth. A sweep over every
`Mutating` entry in `CLASSIFIED_ROUTES` pins the same on the live router. The
suite also pins the opt-out, `dev`, a declared boundary, admin and plain
sessions, the `404` on an unknown path, and Vantage posts.
`tests/mcp_tools_http_tests.rs` pins the MCP tool routes. Unit tests pin the
predicate truth table and the preflight field. Suites that exercise handlers
rather than auth set the opt-out.
