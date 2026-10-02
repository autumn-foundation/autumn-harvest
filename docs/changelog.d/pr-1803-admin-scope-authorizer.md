## Phase — Admin token scope and authorizer hook (issue #1803)

**Problem.** API tokens had two scopes, `read` and `mutate`. A `mutate` token
could mint and revoke tokens, so one leaked token could create others.
Authorization had no tenant or shard input, and `shard_id` was not a
capability.

**What shipped.**

- **`admin` token scope.** `TokenScope::Admin` includes `mutate`. Only `admin`
  reaches `audit::ADMIN_SCOPE_ROUTES`: `POST /admin/tokens` and
  `DELETE /admin/tokens/{id}`. A `mutate` token gets `403` there. No
  management route publishes a workflow module today. A guard test fails if a
  mutating `/admin/tokens` or `/modules` route is not admin-only.
- **Authorizer hook.** `HarvestPlugin::with_authorizer` and
  `StandaloneAdminAuth::with_authorizer` install a `HarvestAuthorizer`. It sees
  the principal, route class, tenant key (`x-harvest-tenant`) and shard. The
  hook runs after the built-in gates, so it can only deny. A plain closure is
  an authorizer. With no hook, the router is unchanged.
- **Shard sources.** The hook reads a shard only where the handler reads it:
  an execution id in the path (decoded, also under `/ui`, with a rebalanced
  run checked on both its entry and live shard), a shard query
  parameter on the routes in `authz::SHARD_SOURCES`, or a shard field in their
  body. A shard named anywhere else is ignored, so a caller cannot show the
  hook one shard while the handler reads all of them.
- **Admin prefixes.** Any mutation under `/admin/tokens`, `/admin/modules` or
  `/modules` is admin-only, even if `ADMIN_SCOPE_ROUTES` does not list it.
- **Deny audit.** Every token scope deny and every hook deny writes an
  `authz.deny` row with status `failed`. The reason is in `error_summary`. The
  caller gets a generic `403`. The audit export ships the row to the SIEM.
- **CLI.** `harvest token bootstrap` defaults to `--scope admin` and accepts
  `admin`.

**Invariants.** Migration `20261002033903_harvest_api_token_admin_scope`
widens the `scope` CHECK only. No `WorkflowEvent` variant, no change to
`harvest_events`, no replay impact.

**Behavior change.** A `mutate` token that minted or revoked tokens now gets
`403`. Mint an `admin` token for that caller.

**Tests.** `authz_integration.rs` covers these cases. A `mutate` mint or
revoke gets `403`, is audited, and changes nothing. A `mutate` token is denied
exactly the admin routes. An `admin` token mints and revokes. A hook denies by
tenant key, and by shard from the path, `/ui`, query, start body, residency key
and other bodies. A rebalanced run is checked on its live shard. A query shard on a route that ignores it is not read. A hook
cannot widen a scope. Deny rows reach an audit export claim. Unit tests cover the scope decision, the admin route matcher,
execution-id decoding, query and body shard parsing, and the guard tests in
`audit.rs`.
