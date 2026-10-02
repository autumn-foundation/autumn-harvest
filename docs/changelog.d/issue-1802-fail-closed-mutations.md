## Security — Mutating routes fail closed outside `dev` (issue #1802)

**Breaking default.** Outside the `dev` profile, a management API with no
declared auth boundary now refuses every mutating route with `401`. Before,
only admin-gated routes did. Workflow start, signal and update (with the
with-start variants), reset, DAG trigger, patch and retry, schedule changes,
external-activity callbacks, worker drain, Vantage form posts and MCP tool
mutations were open to any caller. This maps to OWASP API Security Top 10
2023, API2 and API5.

**What shipped.**

- `require_classified_mutation_auth`, a route layer on `harvest_api_router`.
  It reads the class from `CLASSIFIED_ROUTES` through `classify_route`, so an
  unclassified route fails closed. It runs on matched routes only, so an
  unknown path still answers `404`.
- `require_mutation_auth_by_method` on every Vantage `/ui` route and every
  mutating MCP tool route. Any method except `GET`, `HEAD` and `OPTIONS`
  counts as a mutation.
- The gate admits a request when one of five conditions holds. The
  conditions are the `dev` profile, a declared auth boundary, a verified
  scoped token, an admin session and the opt-out.
- The opt-out: `HarvestPlugin::allow_unauthenticated_mutations()`,
  `StandaloneAdminAuth::allow_unauthenticated_mutations()` and
  `HarvestApiState::set_allow_unauthenticated_mutations`. Outside `dev`,
  startup logs a `tracing::warn!` while it opens the routes.
  `StandaloneAdminAuth::mount` sets the state from its declaration.
- `boot::unauthenticated_mutations_open` is the one predicate for the gate,
  the opt-out warning and `preflight`. The `admin_auth_boundary` preflight
  check gains an `unauthenticated_mutations` field. It fails when the opt-out
  opens the routes outside `dev`.
- The `dev` warnings now name the mutating routes too.

**Design decision.** External-activity callbacks stay behind the gate. The
task token in the path is not a secret, because `GET /admin/external-handoffs`
lists tokens with no admin gate. A remote worker outside `dev` needs a
credential.

**Docs.** `docs/security-posture.md` gains a "Fail-closed mutations" section.
`docs/upgrading/0.7.0.md` describes the migration path. `docs/mcp-tools.md`,
`docs/operator-role.md`, `docs/management-api.md`, the getting-started
chapters, the external-handoff runbook and `docs/api-contract.json` reflect
the new default.

**Invariants.** No new `WorkflowEvent` variant, no migration, no new route, no
`harvest_events` write.

**Tests.**

- `tests/security.rs` pins every data-plane route at `401` with no auth. A
  sweep over every `Mutating` entry in `CLASSIFIED_ROUTES` pins the same on
  the live router.
- A second sweep proves the opt-out opens exactly the data plane, and every
  admin gate stays.
- The suite also pins `dev`, a declared boundary, admin and plain sessions,
  destructive start policies under the opt-out, the `404` on an unknown path,
  and every Vantage form post.
- `tests/mcp_tools_http_tests.rs` pins the MCP tool routes.
  `tests/standalone_admin_auth.rs` pins the opt-out through a nested mount.
- Unit tests pin the predicate, the preflight field and status, a scoped
  token, and the opt-out warning text.
- Legacy admin-gate tests set the opt-out, so their `401` still proves the
  gate they name. Suites that exercise handlers rather than auth set it too.
