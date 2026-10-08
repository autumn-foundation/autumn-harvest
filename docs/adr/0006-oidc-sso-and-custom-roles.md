# ADR 0006 — SSO, custom roles and mTLS for Vantage and the management API

**Status**: Accepted
**Date**: 2026-10-08
**Issue**: [#1978](https://github.com/autumn-foundation/autumn-harvest/issues/1978)
(parent [#1969](https://github.com/autumn-foundation/autumn-harvest/issues/1969))

---

## Context

Before this change, the host app owned all user authentication. Harvest had three
coarse tiers: admin, read-only and anonymous. It had no OIDC login, no
custom roles and no mTLS. Enterprise buyers expect SSO and RBAC.

The issue asks where SSO lives: in Harvest, or in a host-app recipe. It also
proposes custom roles and optional mTLS.

Facts that decide it:

- autumn-web 0.8 has an OIDC client behind its `oauth2` feature. It does
  PKCE (S256), `state`, `nonce`, the JWKS signature check with the algorithm
  pinned to the key, and the `iss`, `aud` and `exp` checks.
- autumn-web 0.8 has mTLS on its listener (`[server.tls.client_auth]`).
- Harvest already has three route classes, an admin-only route list and
  token scopes over them.

---

## Decision

### 1. SSO lives in Harvest, as a thin opt-in layer

**Decision: built.** The `oidc` feature of `autumn-harvest-plugin` adds OIDC
login for Vantage and the management API.

- Harvest owns three routes: login, callback and logout. It also owns the
  claim-to-role map and the session boundary.
- autumn-web owns the protocol and the crypto. Harvest adds no JWT code and
  no new crate.
- Harvest requires a signed ID token. It refuses a `userinfo_url`, because
  the autumn-web userinfo path checks no signature, audience or nonce.
- Harvest refuses a Microsoft multi-tenant issuer. autumn-web would accept
  the unverified issuer of any tenant.
- `HarvestPlugin::api_with_oidc` and `StandaloneAdminAuth::with_oidc` mount
  it. Each declares the auth boundary.
- The callback maps ID-token claims to role names. An identity that maps to
  no role cannot log in. A failed callback does not change the session.
- The session keeps the subject, the roles and the login time. After
  `max_session_age` (default 12 hours) the user must log in again.
- The audit actor of a session user is `oidc:{subject}@{issuer}`. The boundary strips
  an inbound `oidc:` actor from every other request.
- A verified `hvst_` token still works for machine clients.
- The generated MCP tool routes get the same session check.

### 2. Custom roles over the existing route classes

**Decision: built.** `roles.rs` adds custom roles. They need no feature.

- A role has a base scope (`read`, `mutate` or `admin`) and a list of extra
  routes. An extra route is a `CLASSIFIED_ROUTES` entry.
- Three built-in roles exist: `harvest-viewer`, `harvest-operator` and
  `harvest-admin`.
- The build refuses a bad name, a duplicate, an empty role and an unknown
  route.
- Roles come from a `RoleGrant` request extension or the session key
  `harvest_roles`. A client cannot set either one.
- A Vantage `GET` is a read. Any other Vantage method is a mutation.
- An unclassified API path is a mutation, so the check fails closed.
- A deny is `403` and writes one `authz.deny` audit row. A caller with no
  role names writes no row.
- Under the role layer, the admin checks inside handlers pass only for a
  role with the `admin` scope. A declared auth boundary cannot widen a
  narrower role.
- The role layer sits between the read-only layer and the authorizer hook.
  The hook can only narrow what a role allows.

### 3. mTLS stays in autumn-web

**Decision: documented, not built.** autumn-web owns the listener and already
verifies client certificates. Harvest documents the configuration. A host
middleware maps the certificate identity to a `RoleGrant`. See
[`docs/security-posture.md`](../security-posture.md#mtls-on-the-management-api).

---

## Limits

- Roles are fixed at login. A group change at the identity provider takes
  effect at the next login.
- The claim map reads the signed ID token only. An identity provider that
  puts groups only in userinfo needs a claim mapper on its side.
- autumn-web fetches the JWKS on each login. That costs one request per
  login. It does not cost a request per API call.
- Logout removes the Harvest keys from the session and rotates its id. It
  does not end the session at the identity provider.
- An extra route covers the API only. A Vantage form post needs the `mutate`
  scope.
- The OIDC login needs an autumn-web session layer. `HarvestPlugin` has one.
  A standalone mount must add one.

---

## Declined

- *A host-app recipe only.* Each host would write the claim map and the role
  checks again. Custom roles need code in Harvest.
- *A new JWT stack in Harvest.* autumn-web already has a reviewed one. A
  second stack doubles the crypto to review.
- *OIDC access tokens as API bearers.* Machine clients have scoped `hvst_`
  tokens. A JWT bearer needs a JWKS cache and audience rules. It can follow
  later.
- *SCIM user and group sync.* Roles come from claims at login. A sync adds a
  store and a second source of truth.
- *mTLS in Harvest.* Harvest does not own the listener.

---

## Consequences

- No migration. No new `WorkflowEvent` variant. No change to
  `harvest_events`.
- A deployment that sets nothing sees no change. No layer is installed unless
  asked for.
- `StandaloneAdminAuth` and `AdminAuthLayers` gain fields. Code that builds
  `StandaloneAdminAuth` through its builder does not change.
- Tests: `roles.rs` unit and property tests, `tests/custom_roles.rs`, and the
  OIDC round trip in `tests/oidc_login.rs` against a local mock identity
  provider.
