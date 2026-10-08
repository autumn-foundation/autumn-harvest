# Design — Issue #1978: OIDC login and custom roles

Issue #1978 asks where SSO lives. It also asks for custom roles and optional
mTLS on the management API. The decision record is
[ADR 0006](docs/adr/0006-oidc-sso-and-custom-roles.md).

**No migration. No new `WorkflowEvent` variant. No change to
`harvest_events`. No new route in `CLASSIFIED_ROUTES`.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- autumn-web 0.8 has an OIDC client behind its `oauth2` feature:
  `oauth2_authorize_url` and `oauth2_finish_login`. It does PKCE (S256),
  checks `state` and `nonce`, verifies the ID token against the JWKS, pins the
  algorithm to the key, and checks `iss`, `aud` and `exp`.
- `jsonwebtoken` 11 is already in the lockfile, through autumn-web.
- autumn-web 0.8 has mTLS (`[server.tls.client_auth]`, `required_paths`,
  `ClientCert`). Harvest does not own the listener.
- Harvest has three route classes (`PublicSafe`, `ReadOnly`, `Mutating`) and
  one admin-only list (`ADMIN_SCOPE_ROUTES`). Token scopes `read`, `mutate`
  and `admin` already map onto them (`token_scope_denies`).
- Vantage routes are not in `CLASSIFIED_ROUTES`. The #1802 gate treats a
  Vantage `GET`/`HEAD` as a read and every other method as a mutation.

### 0.2 Brainstorm — where does SSO live?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A host-app recipe only. | Rejected. Each host writes claim mapping and role checks again. The issue asks for custom roles, which need code in Harvest. |
| B2 | A new OIDC client in Harvest, on `openidconnect` or `jsonwebtoken`. | Rejected. It adds a second JWT stack and new crypto code to review. |
| B3 | Thin OIDC login in Harvest on the autumn-web client, behind an `oidc` feature. | **Adopted.** Harvest adds the routes, the claim-to-role map and the session boundary. autumn-web does the protocol and the crypto. |
| B4 | Accept OIDC access tokens as API bearers. | Deferred. Machine clients have scoped `hvst_` tokens. A JWT bearer needs a JWKS cache and audience rules. See the ADR. |
| B5 | Custom roles as a set of route classes only. | Rejected alone. Three classes give only three roles, the same as the token scopes. |
| B6 | Custom role = a base scope plus named extra routes. | **Adopted.** A `dlq-operator` can read all and replay the DLQ, and do nothing else. |
| B7 | mTLS in Harvest. | Rejected. autumn-web owns the listener and already has mTLS. Harvest documents the config and maps a certificate to roles through `RoleGrant`. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Accept a caller-supplied role (header, query). | Roles come only from the server-side session or a `RoleGrant` request extension. A client cannot set either. Test: an `x-harvest-roles` header grants nothing. |
| R2 | An unknown role name or a typo in a route grant grants access. | `HarvestRoles::build` refuses an unknown route template, an empty name and a duplicate name. The layer ignores an unknown role name. Tests for each. |
| R3 | An unclassified route is open to a narrow role. | The role check uses `classify_route`, which fails closed to `Mutating`. Test: a `read` role gets `403` on an unclassified path. |
| R4 | A user with no matching claim gets a session. | The callback refuses a login that maps to no role (`403`) and writes no session principal. Test. |
| R5 | A forged ID token, a replay, or a wrong audience logs in. | autumn-web checks signature, `kid`, algorithm, `iss`, `aud`, `exp`, `state` and `nonce`. Tests: a bad `state`, a token signed with an unknown key, and a wrong audience each fail. |
| R6 | The login page is an open redirect. | No `return_to` parameter. The callback redirects only to the configured page. |
| R7 | Login routes sit behind the boundary, so nobody can log in. | The login router is merged outside the boundary and the role layer. The round-trip test proves it. |
| R8 | A spoofed `x-harvest-actor` hides who acted. | The OIDC boundary overwrites the actor with `oidc:{subject}` for a session principal. It strips an inbound `oidc:` actor on every other request. Test. |
| R9 | Declaring the boundary opens the MCP tool routes. | `api_with_oidc` applies the same OIDC boundary and role check to each generated MCP tool route. |
| R10 | A token caller loses access. | A verified `hvst_` token skips the role check. Its scope still applies. The boundary passes an `hvst_` bearer only when tokens are on. |
| R11 | A stale session keeps old roles forever. | The session stores the login time. After `max_session_age` (default 12 h) the user must log in again. Test. |
| R12 | Plain HTTP to the identity provider. | `OidcLogin::build` refuses a non-`https` endpoint unless the host is loopback. Test. |
| R13 | A discovery document names another issuer (mix-up). | `discover` refuses a document whose `issuer` differs from the one asked for. Test. |
| R14 | A deployment without the feature changes. | No layer is installed unless asked for. Existing suites stay green. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | autumn-web 0.8 ships the OIDC client and mTLS. Harvest has token scopes over three route classes plus an admin list. Vantage uses the autumn-web session. No Harvest login page exists. |
| Red | Operators want "log in with Okta, see Vantage, act by group". A long recipe feels like no feature. |
| Black | A new auth path is a new attack surface. The session boundary must not open MCP routes. Roles fixed at login can go stale. autumn-web fetches the JWKS on each login, which costs one request per login. An IdP that puts groups only in userinfo is not covered by the ID-token claims. |
| Yellow | No new crypto. One pure function decides each request, so unit and property tests cover it. Custom roles also work with the host's own auth and with mTLS through `RoleGrant`. |
| Green | B4 (JWT bearer) and SCIM sync are follow-ups. A `RoleGrant` lets mTLS map a certificate to a role with ten lines of host code. |
| Blue | Red: tests for the role decision, the claim map, the boundary and the OIDC round trip fail. Green: `roles.rs`, `oidc.rs`, the plugin and standalone wiring. Refactor: docs, ADR, comment hygiene, clippy. Then a multi-angle review. |

---

## 1. Custom roles (`roles.rs`, no feature)

- `HarvestRole::new(name).with_scope(TokenScope).allow_route("POST /dead-letters/replay")`.
- `HarvestRoles::builder().role(..).build()` validates names and route
  templates against `CLASSIFIED_ROUTES`. `HarvestRoles::builtin()` holds
  `harvest-viewer` (read), `harvest-operator` (mutate) and `harvest-admin`
  (admin).
- The decision is `HarvestRoles::allows(roles, method, path)`:
  - `PublicSafe` is always allowed.
  - A role allows a route when its scope allows it (`token_scope_denies`) or
    when it names the route.
  - A Vantage path (`/ui/...`) is a read for `GET`/`HEAD` and a mutation
    otherwise, as the #1802 gate decides.
- The role layer reads roles from a `RoleGrant` extension, else from the
  session key `harvest_roles` (comma-separated). A verified token skips it.
  A deny is `403` and writes one `authz.deny` row.
- An allowed request carries a `RolePrincipal` extension.
  `require_harvest_admin` and the #1802 gate admit it, as they admit a token.

## 2. Claim-to-role map (`roles.rs`, no feature)

- `ClaimRule::new("groups", "harvest-admins", "harvest-admin")`. The claim
  path is dot-separated (`realm_access.roles`). A string claim matches by
  equality. An array claim matches when it holds the value.
- `ClaimRoleMap::roles_for(&claims)` returns the role names, sorted and
  without duplicates. Its build refuses a rule that names an undefined role.

## 3. OIDC login (`oidc.rs`, feature `oidc`)

- `OidcLogin::new(provider, roles, claim_map)` and `OidcLogin::discover(..)`.
- Routes, outside the boundary: `GET /auth/oidc/login`,
  `GET /auth/oidc/callback`, `POST /auth/oidc/logout`.
- The boundary layer admits a session principal, a `PublicSafe` route and an
  `hvst_` bearer when tokens are on. Else a Vantage `GET` gets `302` to login
  and any other request gets `401`.
- Wiring: `HarvestPlugin::api_with_oidc(path, login)` and
  `StandaloneAdminAuth::with_oidc(login)`. Both declare the auth boundary.

## 4. mTLS (docs only)

`docs/security-posture.md` shows `[server.tls.client_auth]` with
`required_paths = ["/api/harvest"]`, and a host middleware that maps the
`ClientCert` common name to a `RoleGrant`.

## 5. Tests

| Test | Where |
|------|-------|
| Role decision, builder validation, claim map, property test (a role never allows more than its scope plus its routes) | `roles.rs` unit tests |
| Role layer: grant, session roles, header spoof, token skip, deny audit status | `tests/custom_roles.rs` (no DB) |
| OIDC round trip against a local mock IdP: login, callback, claim map, role enforcement, logout, bad state, bad key, wrong audience, no role, discovery, session age | `tests/oidc_login.rs` (feature `oidc`, no DB) |
