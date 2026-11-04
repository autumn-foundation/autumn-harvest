## Plugin — SQLite app: API tokens, roles, OIDC and audit (issue #2469, epic #2466)

A `HarvestPlugin` app on `autumn-web/sqlite` now builds with
`enable_api_tokens`, `api_with_role_auth`, `with_roles`, `with_authorizer`,
`with_api_rate_limit` and `api_with_oidc`. Slice 1 refused each one at build.
The Postgres default build does not change. The design record is
`docs/design/DESIGN-2469.md`.

- **One layer stack.** The SQLite router goes through
  `apply_admin_auth_layers`, then host auth, then the OIDC session boundary,
  as on Postgres. No layer has a SQLite copy.
- **One store seam.** `crate::auth_store::AuthStore` has a Postgres and a
  SQLite variant. The token layer, the token routes, `GET /admin/audit` and
  each deny audit (token scope, role, authorizer, tenant binding, rate limit)
  use it. A SQLite call runs on the blocking pool.
- **Tables in the Harvest file.** `autumn_harvest_sqlite::SqliteAuthStore`
  adds `harvest_api_tokens` and `harvest_audit_log`. It opens its own
  connection, so a token check never waits for the driver. A trigger refuses
  each `UPDATE` of an audit row.
- **Routes.** The SQLite router serves the Postgres handlers of
  `GET`/`POST /admin/tokens`, `DELETE /admin/tokens/{id}` and
  `GET /admin/audit`.
- **Fail closed on tenants.** The SQLite store keeps no run tenant. A token
  mint with a `tenant` is a `400`. A verified tenant gets `501` on each
  non-public route, with one `authz.deny` row.
- **Shared tests.** The bodies of the token, role and OIDC suites moved to
  `tests/support/{token,role,oidc}_contract.rs`. `token_auth_integration`,
  `custom_roles` and `oidc_login` run them on Postgres. The `auth_parity`
  module of `tests/sqlite_plugin.rs` runs them on a SQLite app. CI's
  `sqlite-plugin` job turns on `oidc`.
- **One connection per call on Postgres.** An `AuthSession` holds one
  connection, so a lookup or a change and its audit row share it. The
  token layer gives it back before the handler runs (#1827).
- **Two writers on one file.** The store reads on a read-only connection.
  Each `SqliteRuntime` transaction now begins `IMMEDIATE`. The runtime and
  the store then wait for each other on the busy timeout, and neither fails
  with `SQLITE_BUSY`. The store opens before the driver starts.
- **Audit retention.** The store honours `audit_retention_days`, at boot
  and after each 256 audit writes.
- **Tenant routes.** The stack wraps the `/tenants/{key}` routes inside the
  nest, with one shared rate limiter. Each layer sees the core route. The
  authorizer sees `{key}` as the tenant, and a header that names another
  tenant is a `400`.
- **No new `WorkflowEvent` variant. No Postgres migration.** The store
  creates the SQLite tables when it opens the file.
- **Limits.** The workflow routes on SQLite write no audit row yet.
