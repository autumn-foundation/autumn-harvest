## Security — OIDC login and custom roles for Vantage and the API (issue #1978)

Issue #1978 asks where SSO lives. [ADR 0006](../adr/0006-oidc-sso-and-custom-roles.md)
records the decision: SSO lives in Harvest as a thin opt-in layer, custom
roles are built in, and mTLS stays in autumn-web.

**Custom roles (`roles.rs`, no feature).** A role has a base scope (`read`,
`mutate` or `admin`) and a list of extra `CLASSIFIED_ROUTES` entries.
`HarvestRolesBuilder::build` refuses bad names, duplicates, empty roles and
unknown routes. Three built-in roles exist: `harvest-viewer`,
`harvest-operator` and `harvest-admin`. Roles come from a `RoleGrant`
request extension or the session key `harvest_roles`. A Vantage `GET` is a
read, so a viewer can now use Vantage. A deny is `403` and writes one
`authz.deny` row. An allowed request carries a `RolePrincipal`, which the
admin gate and the #1802 gate admit. Under the role layer, the admin
checks inside handlers pass only for an `admin`-scope role, so a declared
boundary cannot widen a narrow role. Turn it on with
`HarvestPlugin::with_roles` or `StandaloneAdminAuth::with_roles`. With
`mcp_tools()`, the tool routes get a role gate by method.

**OIDC login (`oidc.rs`, feature `oidc`).** The feature turns on the
autumn-web OIDC client. It adds no crate to the lockfile.
`HarvestPlugin::api_with_oidc` and `StandaloneAdminAuth::with_oidc` mount
`GET /auth/oidc/login`, `GET /auth/oidc/callback` and
`POST /auth/oidc/logout` under the API mount. autumn-web checks PKCE,
`state`, `nonce`, the JWKS signature, `iss`, `aud`, `exp` and `nbf`.
Harvest refuses a `userinfo_url` (that path is unsigned) and a Microsoft
multi-tenant issuer.
`ClaimRoleMap` maps claims to roles. An identity with no role cannot log
in. The session boundary sends Vantage to the login page and answers `401`
elsewhere. The audit actor is `oidc:{subject}`. Sessions expire after
`max_session_age` (default 12 hours). `discover_provider` reads the
discovery document and checks its issuer.

**mTLS.** Documented in `docs/security-posture.md`: autumn-web's
`[server.tls.client_auth]` plus a host middleware that maps the certificate
to a `RoleGrant`.

**No migration. No new `WorkflowEvent` variant. No change to
`harvest_events`.** A deployment that sets nothing is unchanged.

**API changes.** New public modules `roles` and `oidc` (feature `oidc`). New
builders `HarvestPlugin::with_roles`, `HarvestPlugin::api_with_oidc`,
`StandaloneAdminAuth::with_roles` and `StandaloneAdminAuth::with_oidc`.

**Tests.** `roles.rs` unit and property tests. `tests/custom_roles.rs` (11
tests, no database). `tests/oidc_login.rs` (21 tests, feature `oidc`, no
database) runs the full login round trip against a local mock identity
provider that signs Ed25519 ID tokens. It covers claim mapping, role
enforcement, logout, a forged `state`, a replayed callback, a wrong nonce,
issuer or audience, an expired token, a foreign signing key, session-id
rotation, a cross-site logout, token pass-through, an identity with no
role, session expiry, discovery and bad configurations. `oidc.rs` and
`plugin.rs` unit tests cover actor attribution, the admin-check scope and
the MCP tool gates. `ci_run_coverage.rs` pins the two manifest rows.
