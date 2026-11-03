# DESIGN-2469: API tokens, roles, OIDC and audit on the SQLite plugin

Issue #2469. Parent epic #2466. Follows #2467 and #2489.

## Problem

Slice 1 refuses these builder options at build on SQLite:
`enable_api_tokens`, `api_with_role_auth`, `with_roles`, `with_authorizer`,
`with_api_rate_limit` and `api_with_oidc`. Each one reads or writes a
Postgres table. The SQLite router also skips the admin-auth layer stack.

Acceptance: the existing token, role and OIDC security tests also run
against a SQLite app.

## Facts

- `apply_admin_auth_layers` in `api.rs` is the one place that orders the
  layers. Postgres and the standalone mount both call it.
- Storage is reached in six places, all through `storage_pool()`:
  - the token layer looks up a token and writes `last_used_at`;
  - the token routes create, list and revoke tokens;
  - the authorizer, the role layer, the tenant binding layer and the rate
    limiter each write one audit row on a deny;
  - `GET /admin/audit` reads the audit rows.
- The OIDC login stores no rows. It keeps its principal in the autumn-web
  session. Its denies go through the role layer.
- `SqliteRuntime` holds an exclusive lock on `<file>.lock`, not on the
  database. `SqliteOverlayLog` already opens a second connection to the
  Harvest file. WAL mode lets the two connections work side by side.
- The driver runs activity bodies inline, outside any transaction. A
  command sent to the driver waits for the activity in progress.
- The SQLite store has no run tenant. The tenant binding layer (#1977)
  needs one to check a run, and the engine needs one to refuse a start of
  another tenant.
- `TestClient::into_router()` gives the full app router, session layer
  included. A test helper that drives an `axum::Router` can then drive a
  SQLite app with no change.

## Brainstorm

1. Send each token and audit call to the driver task as a command.
2. Open a second connection for the auth tables, as the overlay log does.
3. Put the auth tables in a third file, `app.harvest-auth.db`.
4. Write a SQLite copy of each layer.
5. Add a store seam, `AuthStore`, with a Postgres and a SQLite variant.
   The layers call the seam, so one layer stack serves both backends.
6. Copy each Postgres test into the SQLite suite.
7. Move each test body into a shared contract file. Each backend gives a
   harness. The Postgres file and the SQLite suite run the same body.

## Reverse brainstorm: how could this fail?

- A token check waits behind a long activity. Then every API call stalls.
  Option 1 does this. Option 2 does not.
- A copy of a layer drifts from the original. Then SQLite enforces a
  weaker rule. Option 4 does this. Option 5 does not.
- A tenant-bound token starts a run on SQLite. The run has no tenant, so
  the token later gets `503` on its own run. Worse, a start can attach to
  a run of another tenant, because SQLite has no engine tenant check.
- A copied test passes on SQLite while its Postgres original changes.
  Option 6 does this. Option 7 does not.
- A blocking SQLite call runs on an async worker thread and starves the
  runtime.
- An audit row is rewritten after the fact.
- A store that is not open yet answers as "no token". A claimed token would
  then pass through unverified.

## Six hats

- **White (facts).** Six storage call sites. Two tables. Three suites:
  `token_auth_integration`, `custom_roles` and `oidc_login`.
- **Red (feel).** A third file is simple but surprising. The issue asks for
  the Harvest file.
- **Black (risk).** Tenant binding is a data leak risk on SQLite. A seam
  touches hot Postgres code, so the Postgres suites must stay green.
- **Yellow (value).** One layer stack on both backends. Each later layer
  works on SQLite with no extra work.
- **Green (ideas).** `GET /admin/audit` makes the audit rows visible on
  both backends, so a contract test reads rows through the API.
- **Blue (process).** Write the contract tests first (red). Add the store,
  the seam and the routes (green). Then refactor and review.

## Decision

- **Tables in the Harvest file.** `autumn_harvest_sqlite::SqliteAuthStore`
  opens its own connection and creates `harvest_api_tokens` and
  `harvest_audit_log`. A trigger refuses each `UPDATE` of an audit row.
- **One seam.** `crate::auth_store::AuthStore` has a `Postgres` and a
  `Sqlite` variant. Each storage call site uses it. A SQLite call runs on
  the blocking pool.
- **One layer stack.** The SQLite router goes through
  `apply_admin_auth_layers`, then the host auth layer, then the OIDC
  layer, as on Postgres.
- **The same routes.** The SQLite router mounts the Postgres handlers of
  `GET`/`POST /admin/tokens`, `DELETE /admin/tokens/{id}` and
  `GET /admin/audit`.
- **No tenant binding on SQLite.** A token mint with a `tenant` is a
  `400`. A request with a verified tenant is a `501` that names the epic.
  Both fail closed.
- **No store, no pass.** Until the startup hook opens the store, a claimed
  token gets `503`, as on Postgres.
- **Shared tests.** `tests/support/token_contract.rs`,
  `role_contract.rs` and `oidc_contract.rs` hold the test bodies. The
  Postgres suites and `tests/sqlite_plugin.rs` run them.

## Out of scope

- The audit rows of the core workflow mutations (start, signal, cancel and
  the others). Postgres writes them in each handler. SQLite does not yet.
  `GET /admin/audit` on SQLite lists the token and deny rows only.
- Run tenants on SQLite.

## Acceptance

1. Each refused option builds and boots on SQLite.
2. The token, role and OIDC suites run against a SQLite app.
3. An authorizer deny, a role deny and a token scope deny each write one
   `authz.deny` row to the Harvest file. `GET /admin/audit` lists it.
4. Postgres behaviour does not change. Its suites stay green.
