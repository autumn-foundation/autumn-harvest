# Harvest Management API Security Posture

This document defines the supported security postures for the Harvest management
API, explains how to mount it safely in an Autumn application, and provides a
production-readiness checklist.

Harvest does not ship its own identity provider, session system, or RBAC engine.
Authentication and authorization are **delegated to the host Autumn application**,
exactly as Oban Web and Sidekiq Web are mounted behind Plug/Rack authentication
in their respective ecosystems. The responsibility of this document is to make
the API surface explicit so embedders can make informed decisions and verify
their posture before deployment.

---

## Route classification

Every route in `harvest_api_router` belongs to exactly one of three security
classes, declared in `autumn_harvest::audit::CLASSIFIED_ROUTES`:

| Class | Description | Examples |
|---|---|---|
| `PublicSafe` | Always safe to expose without authentication | `GET /health` |
| `ReadOnly` | Reads operator state, no workflow side effects | `GET /workflows`, `GET /admin/audit` |
| `Mutating` | Modifies workflow execution or system configuration | `POST /workflows/{name}/start`, `POST /dead-letters/replay` |

### Mutating routes (must be protected in production)

The following categories carry production risk and **must** be behind
authentication middleware in any non-local deployment. Outside the `dev`
profile, Harvest refuses them with `401` when the mount declares no auth layer.
See [Fail-closed mutations](#fail-closed-mutations-issue-1802).

- **Workflow lifecycle** — `start`, `signal`, `cancel`, `reset`
- **DLQ replay/discard** — single and bulk
- **Schedule mutation** — create, pause, resume, delete
- **Batch operations** — submit fleet-wide cancel/signal jobs
- **Retention** — `run-now` forces immediate data deletion
- **External activity callbacks** — `complete`, `fail`
- **Worker drain** — triggers graceful shutdown on a live worker process

### PublicSafe routes

The `PublicSafe` routes are `GET /health`, `GET /health/live`,
`GET /health/ready` and `GET /openapi.json`. Kubernetes probes and
load-balancer health checks must reach the health paths without credentials.
Exposing them is an explicit product decision. Put all other routes behind
your authentication boundary in production.

The `/health/live` and `/health/ready` bodies hold only booleans, the shard
verdict and reason codes. Under `require_shard_readiness`, `GET /health` also
returns the full shard report, which includes database error text. See
[`operations/kubernetes-probes.md`](operations/kubernetes-probes.md).

---

## Supported postures

### Local / development (no auth)

Mount the API without middleware **and run under `AUTUMN_PROFILE=dev`**. All
routes — including every `/admin` route, every mutating route and the Vantage
dashboard — are then reachable by any caller that can open a socket to the
process, with no session, cookie or token. Suitable for local development and CI environments where the
network boundary already limits access.

```rust
HarvestPlugin::new()
    .api("/api/harvest")
```

Both halves are load-bearing (issue #1284). The admin gate
(`has_harvest_admin_access`) admits an unauthenticated caller **only** when the
deployment profile is exactly `dev` *and* no auth boundary was declared. Under
any other profile — including the default `unknown` a standalone integration
gets when it never sets one — the same mount is fail-closed and every `/admin`
route and every mutating route answers `401`, which is why `harvest preflight` against a non-dev
deployment needs a [scoped token](#scoped-api-tokens-built-in-opt-in--issue-942)
or an admin session.

A process in this posture says so, twice, so it is never a silent surprise:

- a `tracing::warn!` at startup naming the two ways to close it
  (`HarvestPlugin::api_with_auth(..)`, or a non-dev profile);
- `unauthenticated_access: true` on the `admin_auth_boundary` check in
  `GET /admin/preflight`, so a release script can gate on the field rather than
  parse a message.

A caller that *does* present an established (cookie-backed) session is still
judged by `admin_auth_session_key` even in `dev` — so an embedder running its
own auth middleware without going through `api_with_auth` keeps that gate.

### Fail-closed mutations (issue #1802)

Outside the `dev` profile, a mount with no declared auth boundary refuses
every `Mutating` route with `401`. Before this change, only the admin-gated
routes did. Workflow start, signal and update (with the with-start variants),
reset, DAG trigger, patch and retry, schedule changes, external-activity
callbacks, worker drain, Vantage form posts and MCP tool mutations were open
to any caller.

The gate covers three surfaces:

- every `Mutating` route of `harvest_api_router`, by its
  `CLASSIFIED_ROUTES` class (an unclassified route counts as `Mutating`, and
  an `OPTIONS` preflight passes);
- every Vantage `/ui` route, for any method except `GET`, `HEAD` and
  `OPTIONS`;
- every mutating MCP tool route (`start_{wf}`, `start_{dag}`, `signal_{wf}`
  and `{wf}_update_{name}`).

A request passes the gate when one of these is true:

| Condition | How to set it |
|---|---|
| The profile is `dev` | `AUTUMN_PROFILE=dev` |
| An auth boundary is declared | `HarvestPlugin::api_with_auth`, `api_with_role_auth`, or `StandaloneAdminAuth::with_admin_auth_boundary` |
| The request carries a verified scoped token | [`enable_api_tokens`](#scoped-api-tokens-built-in-opt-in--issue-942) or `StandaloneAdminAuth::with_api_tokens`, and a `mutate` token. Tokens do not cover the MCP tool routes. |
| The session carries an admin marker | the same markers the `/admin` gate reads |
| The opt-out is set | see below |

Read routes do not change. A route that already had an admin gate keeps it.
The gate runs on a matched route only, so an unknown path still answers `404`.

**Which profile is `dev`.** autumn-web reads `AUTUMN_ENV`, then
`AUTUMN_PROFILE`, then `--profile`. With none of them, a release build from
`#[autumn_web::main]` resolves to `prod`, and any other build resolves to
`dev`. So a debug build with no profile set runs with the gate open. Set the
profile explicitly in every deployment. `HarvestEmbedding` and a raw router
default to `unknown`, which is not `dev`.

**The opt-out.** `HarvestPlugin::allow_unauthenticated_mutations()`,
`StandaloneAdminAuth::allow_unauthenticated_mutations()` or
`HarvestApiState::set_allow_unauthenticated_mutations(true)` restores the
pre-#1802 posture. Outside `dev`, `HarvestPlugin` and `HarvestEmbedding` then
log a `tracing::warn!` at startup that names the open routes. A raw router
mount logs nothing. `StandaloneAdminAuth::mount` sets the state from its own
declaration, so a mount without the opt-out clears it. Use the opt-out only
while you add an auth layer.

**Visibility.** The `admin_auth_boundary` check in `GET /admin/preflight`
reports `unauthenticated_mutations: true` whenever a caller with no credential
can reach a mutating route. That is the `dev` profile, or the opt-out, with no
declared boundary. When the opt-out opens the routes outside `dev`, the check
fails and names the opt-out.

**Remote callers.** External-activity callbacks (`complete`, `fail`,
`heartbeat`) are mutating routes. Outside `dev`, a remote worker needs a
credential. A `mutate` token also admits every admin route, so prefer an auth
layer that admits the worker to the callback routes only.

**MCP callers.** A `tools/call` through `secure_mcp` replays against the tool
route with the caller's headers. Outside `dev`, a mutating tool then needs
`api_with_auth` or an admin session. A scoped token does not authorize these
routes, because the token layer wraps only the nested management router.

**Not covered.** A raw `harvest_api_router` mount has no startup hook, so it
logs no warning. Its gate still fails closed.

### Read-only operator tier (least-privilege triage)

For a support/on-call/status-dashboard principal that should **read but not
mutate**, mount with `api_with_role_auth` instead of `api_with_auth` — a single
call that adds a class-aware enforcement layer giving `403 Forbidden` on every
mutating management route (and every mutating [MCP tool](./mcp-tools.md), when
`mcp_tools()` is enabled) to any principal your middleware marks read-only,
while leaving 100% of the read surface reachable. See **[the read-only operator
role guide](./operator-role.md)** for the Session claim contract, the
fail-closed guarantee, the MCP-tool coverage, and the `/ui` limitation.

### Production (host-app authentication)

Mount the API with the host application's authentication middleware. The
`api_with_auth` method applies any Tower middleware layer to the **entire**
router — every management API route, the embedded Vantage UI (`/ui/*`), and
all CLI-compatible endpoints are wrapped together because `harvest_ui_router` is
nested into the same Axum router before the middleware layer is added (see
`HarvestPlugin::build` in the plugin source). The same layer is also applied to
every generated MCP tool route.

Pass any Tower `Layer`-compatible middleware. Two common shapes are shown below.

**Session-based (web UI users)**

`autumn_web::auth::RequireAuth` checks for a named key in the session cookie.
It does **not** read the `Authorization` header and will not admit CLI bearer
tokens.

```rust
use autumn_web::auth::RequireAuth;

HarvestPlugin::new()
    // Rejects requests whose session does not contain "harvest-admin"
    .api_with_auth("/api/harvest", RequireAuth::new("harvest-admin"))
```

**Bearer-token (CLI / API clients)**

The Harvest CLI sends `Authorization: Bearer <token>` (via `--token` /
`HARVEST_TOKEN`). To validate that header, supply a Tower middleware that reads
`Authorization` rather than the session:

```rust
use axum::{extract::Request, middleware::Next, response::IntoResponse, response::Response};
use http::StatusCode;

async fn bearer_auth(req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    // Fail closed: reject if the env var is unset or empty, or if the token
    // doesn't match. unwrap_or_default() would make "" a valid token.
    let expected = std::env::var("HARVEST_ADMIN_TOKEN").unwrap_or_default();
    if expected.is_empty() || token != Some(expected.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(req).await
}

HarvestPlugin::new()
    .api_with_auth("/api/harvest", axum::middleware::from_fn(bearer_auth))
```

Replace the static token comparison with your actual validation logic (JWT
verification, database lookup, etc.).

**Unauthenticated health paths for probe traffic**

The health handlers are internal to the plugin and cannot be re-mounted
separately. To let probes reach them without credentials, use a selective
middleware that skips auth for those exact paths. `api_with_auth` applies
the layer inside the nest, so the layer sees the path without the mount
prefix:

```rust
async fn harvest_auth(req: Request, next: Next) -> Response {
    // Exact match — ends_with("/health") would also bypass /workers/health,
    // which is ReadOnly, not PublicSafe. The path has no mount prefix here.
    if matches!(
        req.uri().path(),
        "/health" | "/health/live" | "/health/ready"
    ) {
        return next.run(req).await;  // allow probe traffic through
    }
    // your bearer or session check here
    // ...
    StatusCode::UNAUTHORIZED.into_response()
}

HarvestPlugin::new()
    .api_with_auth("/api/harvest", axum::middleware::from_fn(harvest_auth))
```

Alternatively, configure your reverse proxy or ingress controller to bypass
authentication for the three health paths at the infrastructure layer.

---

## Scoped API tokens (built-in, opt-in — issue #942)

The postures above delegate authentication entirely to the host application. As
an alternative or complement, Harvest ships a first-class, least-privilege token
layer for the management API: create / list / revoke scoped, optionally-expiring,
individually-revocable API tokens, with every mutating operation attributable to
a named actor. It is a `autumn-harvest-plugin` auth layer plus two additive
config-table migrations (`20260713000000_harvest_api_tokens`,
`20261002033903_harvest_api_token_admin_scope`) — no new
`WorkflowEvent` variant, no change to `harvest_events`, no replay-determinism
impact. The deterministic execution core is untouched.

### Enabling it

Token auth is **off by default** (byte-for-byte identical to today) and turned on
with a single builder call:

```rust
let plugin = HarvestPlugin::new(/* … */)
    .enable_api_tokens();
```

This installs a token verification + scope-enforcement layer on the nested
`harvest_api_router`.

### Composed vs. standalone mode

- **Composed** — layer tokens *on top of* your existing `api_with_auth` admin
  boundary. An already-admin caller mints the first token normally through the
  API. Token auth **composes with**, never replaces, `api_with_auth`.
- **Standalone (tokens-only)** — scoped API tokens are the *only* auth, with no
  embedder admin boundary. See "First-token bootstrap" below for the
  chicken-and-egg case.

### Auth ordering

The layer inspects the `Authorization: Bearer` value:

- A bearer that **begins with `hvst_`** is treated as a Harvest token and
  verified (hash the presented secret, one indexed `SELECT`). A claimed `hvst_`
  bearer that cannot be verified because the store is unavailable is rejected
  `503` — never trusted unverified.
- A bearer that **does not begin with `hvst_`** (or an absent bearer) is passed
  through untouched, so `api_with_auth` still runs. Token auth composes with the
  embedder's own middleware rather than short-circuiting it.

### Scopes (read / mutate / admin)

A token carries `read`, `mutate` or `admin`. Each scope includes the one before
it.

| Scope | Reaches | Denied (`403`) |
|---|---|---|
| `read` | Every `ReadOnly` and `PublicSafe` route. | Every mutating route. |
| `mutate` | Every route except the admin-only ones. | `POST /admin/tokens`, `DELETE /admin/tokens/{id}`. |
| `admin` | Every route. | Nothing. |

The `read` gate uses the same `audit::CLASSIFIED_ROUTES` taxonomy as the
read-only operator tier. It **fails closed**: an unclassified path resolves to
`Mutating`, so a `read` token gets `403`.

The admin-only routes are `audit::ADMIN_SCOPE_ROUTES` (issue #1803), plus
any mutation under `/admin/tokens`, `/admin/modules` or `/modules`. Token
management is there because a token that mints tokens can copy itself. No
management route publishes a workflow module today. A future publish route
runs code, so it goes in the same list. A guard test fails if a mutating
`/admin/tokens` or `/modules` route is missing from it.

Every scope deny writes an `authz.deny` audit row. See
[Deny audit](#deny-audit-issue-1803).

### Routes (admin-gated, audited)

| Route | Method | Effect |
|---|---|---|
| `/api/harvest/admin/tokens` | `POST` | Create a token → `201`, audited `token.create`. A token caller needs `admin` scope. The plaintext secret is returned **exactly once** and never persists. |
| `/api/harvest/admin/tokens` | `GET` | List tokens (metadata-only DTO — structurally cannot hold the hash/secret). |
| `/api/harvest/admin/tokens/{id}` | `DELETE` | Revoke a token → audited `token.revoke`. A token caller needs `admin` scope. |

Wire format: a secret is opaque `hvst_<base64url(32 random bytes)>`. Only
`token_hash = hex(SHA256(secret))` is stored (UNIQUE-indexed).

### First-token bootstrap (standalone mode)

In pure standalone mode there is a chicken-and-egg gap: `POST /admin/tokens` is
an admin-gated mutation, so minting a token requires a previously-minted token.
`harvest token bootstrap` closes it — an **offline seed** CLI that opens no DB
connection and issues no HTTP request. It prints a fresh secret **once** and the
exact `INSERT INTO harvest_api_tokens (...)` statement (embedding only the hash,
never the secret) for the operator — who already holds DB access, the trust
anchor — to run out-of-band. It defaults `--scope` to `admin` so the seed token
can mint the rest through the API. Among tokens, only an `admin` token can
mint (issue #1803). A bootstrap-seeded token authenticates byte-for-byte
identically to a route-minted one (shared core hashing helper).

### Rotation, expiry, and actor attribution

- Optional `expires_at`; an expired or revoked token is rejected `401` on the
  **next request** (there is no grant cache — every request re-queries the
  table). Rotate by create-replacement → cut over → revoke old.
- On a verified token, the layer strips any inbound `x-harvest-actor` and injects
  `token:{id}`, so every audited mutation is attributed to the token (never the
  secret/hash) and a caller cannot spoof a different actor. The `token:` actor
  namespace is reserved.

### Operational caveats

- **Standalone-token mode should sit behind a rate-limiting proxy.** With
  `enable_api_tokens()` as the only auth, any `hvst_` bearer triggers one indexed
  lookup before authentication (inherent to any bearer scheme). Front the API
  with a per-source rate-limiting proxy to bound unauthenticated lookup floods.
- **Rotation needs `admin`.** `harvest token rotate` mints through
  `POST /admin/tokens`, so only an `admin` token can rotate.
- **A compromised `admin` token can mint replacement tokens.** Give `admin` to
  as few callers as you can. Give CI and services `mutate` or `read`: neither
  can mint (issue #1803). When an `admin` token leaks, revoking it is not
  enough. Also audit the `created_by` provenance and revoke every token it
  minted.
- **Upgrading from the two-scope model.** A `mutate` token that minted or
  revoked tokens before issue #1803 now gets `403` on those routes. Mint an
  `admin` token for that caller, through the embedder boundary or
  `harvest token bootstrap`.

CLI: `harvest token create | list | revoke` (plus a client-side `rotate`
convenience) and the offline `harvest token bootstrap`.

---

## Authorizer hook (issue #1803)

The built-in gates decide by verb only. The authorizer hook adds a policy by
principal, route class, tenant and shard. It is off by default. With no hook,
the router is byte-for-byte unchanged.

```rust
use autumn_harvest::types::ShardId;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::authz::{AuthzDecision, AuthzPrincipal, AuthzRequest};

let plugin = HarvestPlugin::new()
    .enable_api_tokens()
    .with_authorizer(|req: &AuthzRequest<'_>| match req.principal {
        AuthzPrincipal::Token { .. } if req.shard == Some(ShardId::new(2)) => {
            AuthzDecision::deny("tokens may not reach shard 2")
        }
        _ => AuthzDecision::Allow,
    });
```

A standalone mount uses `StandaloneAdminAuth::with_authorizer`. A policy that
needs I/O implements `HarvestAuthorizer` directly and returns a boxed future.

### What the hook sees

| Field | Source |
|---|---|
| `principal` | `Token { id, scope }` for a verified `hvst_` token. `Embedder` for every other caller; read its claims from `extensions`. |
| `route_class` | `CLASSIFIED_ROUTES`. An unclassified path is `Mutating`. |
| `tenant_key` | The `x-harvest-tenant` header, trimmed. A repeated header, a blank value, a non-ASCII byte or more than 128 bytes gets `400`. |
| `shard` | Only a source the route's handler uses. See the table below. |
| `method`, `path`, `extensions` | The request. |

| Route | Shard source |
|---|---|
| A path with an execution id, e.g. `GET /workflows/{id}`, also under `/ui` | The entry shard of the id and the shard the run lives on now. On a route that acts on the live attempt, also the shard of each later attempt in the retry chain. On `/result`, also every shard of each continued-as-new successor. A percent-encoded id is decoded first. |
| `GET /workflows/{id}/children`, `GET /workflows/{id}/tree`, `POST /workflows/{id}/erase-payloads` | The execution's shards, and also `None`, because the handler reads, or erases, every shard for descendants. |
| `GET /admin/history/exports`, `GET /admin/history/export-sample` | `shard_id`, `shard-id` or `shard` query parameter. |
| `GET /admin/external-handoffs` | `shard_id` or `shard` query parameter. |
| `POST /workflows/{name}/start` | Body `shard_id`, or the shard a body `residency_key` resolves to. |
| `POST /dead-letters/replay`, `POST /dead-letters/discard` | Body `shard_id`, JSON or form. |
| `POST /dlq/redrive`, `POST /admin/queues/{name}/pause`, `POST /admin/queues/{name}/resume` | Body `shard_id`. |
| `POST /admin/audit-export/redrive`, `.../decommission`, `.../reactivate` | Body `shard`. |

A shard named anywhere else is ignored. So `GET /workflows?shard_id=3` gives
`None`, because that handler lists every shard. To read a body, the hook
buffers it under the app's own body limit, then hands it to the handler. A body
over the limit gets `413` before the hook runs.

Harvest calls the hook once for each distinct shard a request names. With no
shard, it calls it once with `shard: None`. `None` means Harvest cannot name
the shard before the handler runs. A list route then reads every shard. A by-id
route (`/workflows/by-id/...`) and a start with no placement each reach one
shard by hash. To confine a caller to some shards, deny `None` too.

### Rules

- **The hook can only deny.** It runs after the token layer and the read-only
  layer, so it cannot grant what a token scope withholds.
- **A deny is a generic `403`.** The body is
  `{"error":"forbidden by authorization policy"}`. The reason goes only to the
  audit row, so the policy is not an oracle.
- **The tenant key is caller-declared.** Harvest does not bind it to stored
  executions. The hook decides if the principal may act for that tenant. To
  confine a caller to its own executions, also check the target in `path`.
- **An execution id gives every shard its handler can reach.** That is the
  entry shard and the live shard after a rebalance. Some routes act on the
  live attempt: `/result`, `cancel`, `terminate`, `pause`, `resume`, `signal`,
  `query`, `queries` and `update`. On those, the hook also checks the shard of
  each later attempt in the retry chain (`authz::RETRY_CHAIN_ROUTES`).
  `/result` also follows continued-as-new successors
  (`authz::CONTINUED_AS_NEW_ROUTES`), so the hook also checks every shard of
  each successor. The hook uses the same walks as the handlers. This costs a few indexed lookups per request. If a walk fails, the
  request gets `503`, because the handler would fail the same walk.
- **A cutover after the check is fenced.** The hook fences the handler to the
  shards the policy allowed (`autumn_harvest::shard_fence`). A rebalance
  cutover between the check and the handler moves a run to a new shard. The
  handler then names that shard at its first checkout, and the fence refuses
  the checkout before any read or write there. The caller gets `503` with a
  retry hint. The retry resolves the run on its new shard, and the policy
  decides on that shard. An SSE stream whose run moves after the check ends
  with an `error` frame (`outside_shard_fence`, `retry: true`), and the
  reconnect is authorized against the live shard. The fence never widens
  access. A fenced miss writes no `authz.deny` row, because the policy did
  not deny. A request the policy saw with `shard: None` gets no fence,
  because its handler reads every shard by design. Control rows (audit,
  tokens, gates) are reached through the default pool, which names no shard,
  so the fence does not apply to them.
- **The hook does not cover the app-level MCP tool routes or webhook routes.**
  They live outside the management router.
- **A panic in the hook aborts the request.** It never lets it through.

### Deny audit (issue #1803)

Every token-scope deny and every hook deny writes one `harvest_audit_log` row
on the control shard. That includes the read-scope deny on a generated MCP tool
route. The write is best effort: a failed write is logged, and
the caller still gets `403`. A read-only-role deny and a tenant-header `400`
write no row. The row holds these values:

| Column | Value |
|---|---|
| `operation` | `authz.deny` |
| `target_type` | `route` |
| `status` | `failed`, so the SIEM export marks it `ERROR` |
| `actor` | `token:{id}` for a token. Otherwise, the value of the actor extractor. By default, that is the `x-harvest-actor` header or `anonymous`. A caller with no credential can set it. Cut to 256 bytes. |
| `request_id` | The `x-request-id` header, cut to 256 bytes |
| `route_or_command` | `METHOD path`, with the path cut to 256 bytes |
| `shard_id` | The denied shard, if any |
| `error_summary` | `token scope '<scope>' does not allow this route`, or `authorizer denied (tenant="…", shard=…): <reason>`. The tenant is quoted and escaped. Cut to 512 bytes. |

The [audit export](./audit-export.md) ships these rows like any other.

A deny row costs one insert. A token scope deny comes only from a valid token,
so its author is known. The hook can also deny a caller with no credential.
For such a request, return `Allow` and let `require_admin` answer `401` with
no audit write. Keep the rate-limiting proxy advice above.

---

## Data residency and shard placement (issue #697)

`POST /workflows/{name}/start` accepts an optional `shard_id` or `residency_key`
that pins the new workflow (and, by shard inheritance, its whole descendant
tree) to a specific database. See [`sharding.md`](./sharding.md#explicit-shard-placement-and-data-residency-issue-697)
for the mechanism. Security-relevant properties:

- **`shard_id` is not a capability by default.** Any caller authorised to start
  a workflow can pin it to any *placeable* shard. Placement selects a database
  within the deployment; it does not grant access to data already there.
  Harvest validates that a requested shard exists and accepts writes, not that
  *this* caller is entitled to it. To confine a caller to one region, install
  an [authorizer hook](#authorizer-hook-issue-1803). It sees the shard a start
  body pins, including one a `residency_key` resolves to.
- **Rejections do not enumerate the deployment.** A refused placement names only
  what the caller asked for (`shard N is not a placeable shard for this
  deployment`, `residency key 'K' is not declared for this deployment`). The
  shard set, drain state, and declared key list are never returned to the
  caller — they go to the server log via `tracing::warn!`. This keeps the start
  route from being a topology-discovery oracle for a lower-trust caller.
- **Residency keys are opaque labels, not secrets.** They are operator-declared
  at boot and appear in caller requests, CLI invocations, and audit rows. Do not
  encode tenant identifiers or anything sensitive in them; use region /
  jurisdiction names.
- **A pin failing closed is a `503`, not a silent redirect.** A shard the router
  accepts but has no pool for is refused rather than written to the default
  database, so a residency obligation cannot be violated by a configuration gap.

---

## Business-key targeting for signal/cancel (issue #751)

`WorkflowContext::signal_external_workflow_by_id` and
`request_cancel_external_workflow_by_id` let a running workflow address another
by its stable `(workflow_name, workflow_id)` business key instead of its
`ExecutionId`. Security-relevant properties:

- **Same trust boundary as `ExecutionId`-targeted signal/cancel.** Neither
  primitive is reachable from outside the engine — both are called from
  already-running, already-trusted server-side workflow code, exactly like the
  pre-existing `ExecutionId`-targeted methods (issue #244/#492). There is no
  new HTTP or network-facing surface here; the HTTP business-id read surface
  (issue #805) already exposes at least as much information to any
  read-authenticated caller.
- **A business key is easier to guess than an `ExecutionId`.** A `workflow_id`
  is often predictable (`order-42`, `tenant-7`), unlike a random `ExecutionId`.
  This does not widen what the *engine* allows — there is no ACL on either
  addressing mode, matching Harvest's "no built-in RBAC engine" design — but it
  does lower the practical guessing bar for embedder-supplied inputs. **Do not
  build `workflow_name`/`workflow_id` targeting strings from
  attacker-influenced data inside a workflow** without your own
  authorization check; treat this exactly like the [shard-placement caveat
  above](#data-residency-and-shard-placement-issue-697) — the string is an
  address, not a secret, and reaching it should be gated by your embedding
  application, not by Harvest.
- **Shard resolution is placement-aware (issue #1146, closing the #697
  interaction).** Resolving which shard owns a `(workflow_name, workflow_id)`
  target used to re-derive the rendezvous hash a fresh start would use
  (`shard::external_target_owning_shard`), which cannot see an explicit shard
  pin applied at start time (`ShardPlacement::Shard`/`ShardPlacement::ResidencyKey`)
  or a shard drained out of `writable_shards` since placement — so an
  explicitly pinned workflow could be unreachable by business-key targeting.
  Delivery now resolves by observation instead
  (`external_target_location::resolve_location_by_workflow_id` fans out
  across every expected shard and merges the per-shard answers), so any
  placement is addressable.

  Two properties matter for posture rather than correctness. Resolution
  **reads at most two rows per shard** for the addressed key — an
  authorization-neutral read: the management API's by-id endpoints (issue #805)
  already fan the identical query across every shard for any read-authenticated
  caller, and it returns only the target's own execution id/state/start time,
  never another tenant's data. And a shard that cannot be inspected yields a
  **retry**, never a `target_unknown`, so an operator watching a partial outage
  sees stalled by-id deliveries rather than silent false failures. Business-key addressing
  remains an *address, not a secret*: the caveat above about building
  `workflow_name`/`workflow_id` from attacker-influenced data is unchanged and
  is now the only gate, since placement no longer accidentally hides a pinned
  workflow.

---

## CLI token semantics

The Harvest CLI supports `--token <value>` and the `HARVEST_TOKEN` environment
variable. **This only sends credentials — it does not secure the server.**

When the CLI sends a request with `--token`, it sets the `Authorization: Bearer
<token>` header on every request via `reqwest::RequestBuilder::bearer_auth`.
Whether that header is validated depends entirely on the middleware the embedder
configures on the server.

Without authentication middleware:

- The CLI token is sent but ignored by the server (unless it is a
  [scoped `hvst_` token](#scoped-api-tokens-built-in-opt-in--issue-942), which
  the built-in layer validates on its own).
- Read routes with no admin gate stay reachable without credentials.
- Every **mutating** route and every **admin-gated** route depends on the
  deployment profile. It is reachable without credentials under
  `AUTUMN_PROFILE=dev`. It answers `401` under every other profile (issues
  #1284 and #1802). The `allow_unauthenticated_mutations()` opt-out reopens
  the mutating routes that have no admin gate. So a bare `harvest preflight`
  or `harvest workflow start` works against a local dev app and fails closed
  against anything else.

With `RequireAuth` (session guard):

- **CLI bearer tokens are not validated** — `RequireAuth` checks a session
  cookie, not the `Authorization` header. CLI calls will always get `401`.
- Use the bearer-token middleware recipe above if CLI access is required.

With a bearer-token middleware:

- The server validates the `Authorization: Bearer` value.
- Unauthenticated requests (no token or wrong token) receive `401 Unauthorized`.
- CLI calls with a valid `--token` are admitted.

---

## Authentication and audit trail composition

Authentication (issue #174) and the audit trail (issue #158) are complementary,
not substitutes:

- **Authentication** decides whether a caller *may* act.
- **Audit** records *who* acted and *what* happened, including failures.

The audit trail (`harvest_audit_log`) records the `X-Harvest-Actor` header as
the `actor` field. When the host application populates this header after
successful authentication (e.g., from a validated JWT subject claim), the audit
trail reflects the real operator identity. When no auth is configured, `actor`
defaults to `"anonymous"`.

Data-governance operations follow the same posture: **per-execution legal hold**
(`POST /workflows/{id}/legal-hold` / `…/legal-hold/release`, issue #747) and
**targeted PII erasure** (`POST /workflows/{id}/erase-payloads`, issue #495) are
admin-gated mutating routes, audited under `legal_hold.set` / `legal_hold.release`
and `workflow.erase_payloads`. A legal hold exempts a single execution's history
from the retention janitor and from PII erasure until released — see
[`docs/archival.md`](archival.md) for the retention/erasure lifecycle.

---

## Payload encryption at rest (issue #1825)

By default, Harvest stores workflow payloads in `harvest_events` as plain
JSON. A workflow that carries PII or secrets must encrypt them. Use
`autumn_harvest::aead_codec::AeadCodec`.

### What the codec does

- It uses AES-256-GCM from the RustCrypto `aes-gcm` crate. Harvest implements
  no cipher of its own.
- Each encode reads a fresh 96-bit nonce from the operating system RNG.
- Each payload starts with a header that holds the format version and the
  key id. The header is AEAD associated data, so it is authenticated.
- Decode fails for a wrong key, a changed byte, a changed header or a
  truncated payload.
- `DataKey` clears its bytes on drop. The AES round keys and the GHASH state
  are also cleared on drop. On aarch64, upstream `polyval` does not yet clear
  the GHASH state.
- `Debug` output and error text never hold key material or plaintext.

### What the codec does not cover

The codec encrypts the payload fields of `harvest_events.event_data` only:
`input`, `output`, `payload`, `details`, `value` and
`last_completion_result`. ADR-0003 and the current schema keep these columns
in clear, so that operators can query them:

- `harvest_workflow_executions.input`, `.output`, `.memo` and `.search_attrs`;
- `harvest_task_queue.input`, `.output` and `.heartbeat_details`;
- `harvest_signals.payload` and `harvest_dead_letters.input`;
- other denormalized copies, for example schedule inputs and outbox rows.

Failure text also stays in clear (issue #1920). The codec does not encrypt
these free-form strings in `harvest_events.event_data`:

- `error` in `WorkflowFailed`, `ActivityFailed`, `ActivityFailedExternally`,
  `LocalActivityFailed`, `LocalActivityExhausted`, `ChildWorkflowFailed` and
  `UpdateFailed`.
- `last_error` in `WorkflowStarted`.
- `reason` in `WorkflowCancelled`, `WorkflowResetFork`,
  `WorkflowResetTerminated`, `WorkflowExecutionPaused` and `WorkflowRedriven`.
- `message` in `ExternalAwaitFailed`.
- `error_type` and `reason_code`, which name a failure class.

The engine also writes the `error` columns of `harvest_workflow_executions`,
`harvest_task_queue` and `harvest_dead_letters` as plain text. The dead-letter
`failure_signature` derives from the error text. The history export puts the
`WorkflowFailed` error text in a diagram note. A completion callback sends the
error text to its URL.

Erasure (issue #495) does not erase error text. Operators need it to diagnose
failures. A validation message often quotes the bad value, for example an email
address or a social security number.

Keep PII out of error strings. Only `WorkflowFailed`, `ActivityFailed`,
`ChildWorkflowFailed` and `ExternalAwaitFailed` carry a `details` field, which
the codec encrypts. Set it with `WorkflowFailure::with_details` or
`ActivityFailure::with_details`. A plain `Err(String)` sets no `details`. The
other variants have no encrypted field for failure data.

Event types, ids, timestamps and workflow names also stay in clear. Do not put
PII in a memo, a search attribute, a workflow id or a workflow name. If these
columns must not hold PII, encrypt the value in workflow code before Harvest
sees it. Also use Postgres disk encryption.

The associated data binds the version and the key id, not the row. A writer
with access to `harvest_events` can copy a ciphertext to another field, event
or execution under the same key, and it decodes. Restrict write access to the
Harvest database.

### Key providers

Load each data key once, at startup, through a `KeyProvider`:

| Provider | Key source |
|----------|------------|
| `EnvKeyProvider` | An environment variable that holds base64. The key stays in the process environment. |
| `FileKeyProvider` | `<dir>/<key_id>.key` that holds base64, for example a secret volume. |
| `KmsKeyProvider` | A wrapped data key that a KMS unwraps (envelope encryption). |

The `autumn-harvest-plugin` `aws-kms` feature adds `aws_kms::AwsKms`, which
implements `KmsDecrypt` for AWS KMS. The core crate has no cloud dependency. To
make a wrapped key, call `GenerateDataKey` with the codec key id as the
encryption context:

```sh
aws kms generate-data-key --key-id "$KMS_KEY_ARN" --key-spec AES_256 \
  --encryption-context harvest_codec_key_id=2026-10 \
  --query CiphertextBlob --output text > 2026-10.wrapped.b64
```

The CLI writes the wrapped key as base64. Load it with
`with_wrapped_key_base64`. Never store the `Plaintext` field. KMS refuses to
unwrap the key under another key id or another KMS key.

The application needs the plugin feature, and `aws-config` to load AWS
credentials. The plugin re-exports `aws_sdk_kms`.

```toml
[dependencies]
autumn-harvest-plugin = { version = "0.7", features = ["aws-kms"] }
aws-config = { version = "1", features = ["behavior-version-latest"] }
```

```rust,ignore
use autumn_harvest::aead_codec::{AeadCodec, KmsKeyProvider};
use autumn_harvest_plugin::aws_kms::{AwsKms, aws_sdk_kms};

let kms = AwsKms::new(aws_sdk_kms::Client::new(&aws_config::load_from_env().await));
let wrapped = std::fs::read_to_string("2026-10.wrapped.b64")?;
let keys = KmsKeyProvider::new(kms, kms_key_arn).with_wrapped_key_base64("2026-10", &wrapped)?;
let harvest = HarvestBuilder::new()
    .aead_payload_codec_key(AeadCodec::load(&keys, "2026-10").await?)
    .try_build()?;
```

Use `aead_payload_codec_key`, not `payload_codec`, from the first deployment.
It writes the key id into each envelope, so a later rotation needs no
`legacy` key.

The first key registered becomes active at once. That is safe for one
process. In a fleet with more than one process, an upgraded process could
then write keyed envelopes before every reader can decode them. So, for a
fleet, do the rollout in two steps:

1. Register `IdentityCodec` under `CODEC_LEGACY_KEY_ID` before the AEAD
   codec. The legacy key stays active, so writes do not change. Deploy this
   build to every process.
2. Call `codec_rotation::activate_codec_key` for the AEAD key id. That call
   checks that every live worker can read the key, records it, and then
   switches new writes to it.

Activation encrypts new writes only. Payloads that an existing deployment
already stored stay in clear. The rotation sweep re-keys ciphertext only, so
it never encrypts them, and its census does not count them. A sweep can
therefore report completion while old plaintext remains. To remove old
plaintext, let retention delete it, or erase it with
`POST /workflows/{id}/erase-payloads` (terminal executions only). Harvest has
no plaintext-to-ciphertext migration. That migration would be a third
in-place mutation of `harvest_events`, and the engine invariants in
`CLAUDE.md` allow two.

```rust,ignore
use autumn_harvest::payload_codec::{CODEC_LEGACY_KEY_ID, IdentityCodec};

let harvest = HarvestBuilder::new()
    .payload_codec_key(CODEC_LEGACY_KEY_ID, IdentityCodec)
    .aead_payload_codec_key(AeadCodec::load(&keys, "2026-10").await?)
    .try_build()?;
```

A deployment that already uses `payload_codec(AeadCodec)` has history with no
`kid`. Keep that `payload_codec` call when you add keyed codecs. The kid-less
history decodes through it.

### Rotation and the nonce limit

A random 96-bit nonce can repeat. NIST SP 800-38D limits each key to 2^32
encodes with random nonces. At that limit, the chance of any repeated nonce is
about 2^-33. Each payload field is one encode. The rotation sweep re-encodes
each stored field, so the sweep also counts against the new key. Rotate each
key well before the limit, and at once after a key leak.

To rotate, load a codec for the new key id and register it. Activate it with
`codec_rotation::activate_codec_key`. The issue #948 sweep then re-encrypts
stored ciphertext under the new key. It does not touch stored plaintext.

The sweep does not re-encrypt offloaded blobs (issue #524). Retirement
removes the old codec from the registry, so the read path can no longer
decode a blob that the old key encrypted. If you use payload offloading,
re-key the offloaded blobs before you retire the old key. Then retire the old
key. Destroy the old key material only after that. See "What zero does and
does not authorise" in
[`operations/codec-key-rotation.md`](operations/codec-key-rotation.md).
`replay_fidelity_is_byte_identical_across_a_sweep` proves that replay stays
byte-identical across a sweep with this codec.

---

## Production-readiness checklist

Before deploying the Harvest management API to a production environment, verify
the following:

### 1. Authentication middleware is configured

```rust
// Confirm api_with_auth (not api) is used in production
HarvestPlugin::new()
    .api_with_auth("/api/harvest", /* your middleware */)
```

### 2. Unauthenticated mutating requests are rejected

Run each command **without credentials**. Every request must return `401` or
`403` before any workflow, DLQ, schedule, batch, or retention side effect occurs.
Outside the `dev` profile, Harvest returns `401` here even with no middleware
(issue #1802). A `2xx` or a handler error means one of three things. The
profile is `dev`, the opt-out is set, or your declared auth layer admits
anonymous callers.

```bash
BASE="https://your-app.example.com/api/harvest"

# Workflow start (mutating)
curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/workflows/my-workflow/start" \
  -H "Content-Type: application/json" -d '{}'
# Expected: 401 or 403

# DLQ bulk replay (mutating)
curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/dead-letters/replay" \
  -H "Content-Type: application/json" -d '{"ids":[]}'
# Expected: 401 or 403

# Schedule creation (mutating)
curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/admin/schedules/workflow" \
  -H "Content-Type: application/json" -d '{}'
# Expected: 401 or 403

# Batch submission (mutating)
curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/batch-operations" \
  -H "Content-Type: application/json" -d '{}'
# Expected: 401 or 403

# Retention run-now (mutating)
curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/admin/retention/run-now" \
  -H "Content-Type: application/json" -d '{}'
# Expected: 401 or 403
```

A 100% rejection rate from these five representative endpoints is the minimum
bar. The Harvest security test suite (`tests/security.rs`) covers all 20
mutating routes with `RequireAuth` applied and serves as the canonical
regression suite.

### 3. Read-only routes are appropriately protected

`ReadOnly` routes do not mutate state but may expose sensitive operational data
(execution IDs, payload previews, schedule definitions). Protect them with the
same middleware layer as mutating routes unless your threat model explicitly
permits unauthenticated read access.

### 4. Actor header is populated post-authentication

```http
X-Harvest-Actor: alice@example.com
```

Set this header from your authentication middleware after the caller is
identified. The audit trail stores it as the `actor` field on every mutation
record. Without it, records default to `"anonymous"`.

### 5. Multi-shard deployments

Authentication middleware applies uniformly across all shards because it wraps
the router layer, not individual handlers. No extra configuration is needed for
multi-shard deployments.

### 6. Payloads that carry PII are encrypted

If a workflow carries PII or secrets, register an `AeadCodec` with
`aead_payload_codec_key`. Load the key from a `KeyProvider`, never from
source code. Keep PII out of error text. Read
[what the codec does not cover](#what-the-codec-does-not-cover).

---

## Route classification regression test

The exhaustiveness guard in `autumn_harvest::audit` ensures that no route can
be added to `harvest_api_router` without being explicitly classified. The test
`audit::tests::route_classification_covers_all_known_routes` fails if any route
is present in `ALL_MUTATION_ROUTES` but missing from `CLASSIFIED_ROUTES`.

When adding a new management route:

1. Register it in `harvest_api_router` (`autumn-harvest-plugin/src/api.rs`).
2. Add it to `ALL_MUTATION_ROUTES` (`autumn-harvest/src/audit.rs`) with the
   appropriate audit operation or `None`.
3. Add it to `CLASSIFIED_ROUTES` (`autumn-harvest/src/audit.rs`) with the
   correct `RouteClass` (`PublicSafe`, `ReadOnly`, or `Mutating`).
4. If it is `Mutating`, either wire an audit record in the handler or add an
   explicit entry to `EXCLUDED_ROUTES` with a justification comment.

Running `cargo test -p autumn-harvest --features db -- audit::tests` will catch
any omission before merge.
