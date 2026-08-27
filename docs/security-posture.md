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
authentication middleware in any non-local deployment:

- **Workflow lifecycle** — `start`, `signal`, `cancel`, `reset`
- **DLQ replay/discard** — single and bulk
- **Schedule mutation** — create, pause, resume, delete
- **Batch operations** — submit fleet-wide cancel/signal jobs
- **Retention** — `run-now` forces immediate data deletion
- **External activity callbacks** — `complete`, `fail`
- **Worker drain** — triggers graceful shutdown on a live worker process

### PublicSafe routes

`GET /health` is the only `PublicSafe` route. Kubernetes liveness/readiness
probes and load-balancer health checks commonly require this path to be
reachable without credentials. Exposing it is an explicit product decision; all
other routes should be behind your authentication boundary in production.

---

## Supported postures

### Local / development (no auth)

Mount the API without middleware. All routes are reachable. Suitable for local
development and CI environments where the network boundary already limits access.

```rust
HarvestPlugin::new()
    .api("/api/harvest")
```

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

**Unauthenticated `/health` for probe traffic**

The `health` handler is internal to the plugin and cannot be re-mounted
separately. To let load-balancer probes reach `/health` without credentials,
use a selective middleware that skips auth for that path:

```rust
async fn harvest_auth(req: Request, next: Next) -> Response {
    // Exact match — ends_with("/health") would also bypass /workers/health,
    // which is ReadOnly, not PublicSafe. Update the literal if you change
    // the HarvestPlugin mount point.
    if req.uri().path() == "/api/harvest/health" {
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
authentication for `GET /api/harvest/health` at the infrastructure layer.

---

## Scoped API tokens (built-in, opt-in — issue #942)

The postures above delegate authentication entirely to the host application. As
an alternative or complement, Harvest ships a first-class, least-privilege token
layer for the management API: create / list / revoke scoped, optionally-expiring,
individually-revocable API tokens, with every mutating operation attributable to
a named actor. It is a `autumn-harvest-plugin` auth layer plus one additive
config-table migration (`20260713000000_harvest_api_tokens`) — no new
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

### Scopes (read / mutate)

A token carries `read` or `mutate`, derived from the same
`audit::CLASSIFIED_ROUTES` taxonomy the read-only operator tier uses (no second
route taxonomy). The gate **fails closed**: an unclassified path resolves to
`Mutating` and is denied to a `read` token. A `read` token is rejected `403` on
every mutating route and admitted on every read-only route.

### Routes (admin-gated, audited)

| Route | Method | Effect |
|---|---|---|
| `/api/harvest/admin/tokens` | `POST` | Create a token → `201`, audited `token.create`. The plaintext secret is returned **exactly once** and never persists. |
| `/api/harvest/admin/tokens` | `GET` | List tokens (metadata-only DTO — structurally cannot hold the hash/secret). |
| `/api/harvest/admin/tokens/{id}` | `DELETE` | Revoke a token → audited `token.revoke`. |

Wire format: a secret is opaque `hvst_<base64url(32 random bytes)>`. Only
`token_hash = hex(SHA256(secret))` is stored (UNIQUE-indexed).

### First-token bootstrap (standalone mode)

In pure standalone mode there is a chicken-and-egg gap: `POST /admin/tokens` is
an admin-gated mutation, so minting a token requires a previously-minted token.
`harvest token bootstrap` closes it — an **offline seed** CLI that opens no DB
connection and issues no HTTP request. It prints a fresh secret **once** and the
exact `INSERT INTO harvest_api_tokens (...)` statement (embedding only the hash,
never the secret) for the operator — who already holds DB access, the trust
anchor — to run out-of-band. It defaults `--scope` to `mutate` so the seed token
can mint the rest through the API. A bootstrap-seeded token authenticates
byte-for-byte identically to a route-minted one (shared core hashing helper).

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
- **A compromised `mutate` token can mint replacement tokens** (the 2-level
  read/mutate model has no scope that grants mutation but withholds token
  management — fine-grained RBAC is out of scope). Revoking a leaked `mutate`
  token is insufficient on its own: also audit the `created_by` provenance and
  revoke the entire lineage of tokens it minted.

CLI: `harvest token create | list | revoke` (plus a client-side `rotate`
convenience) and the offline `harvest token bootstrap`.

---

## Data residency and shard placement (issue #697)

`POST /workflows/{name}/start` accepts an optional `shard_id` or `residency_key`
that pins the new workflow (and, by shard inheritance, its whole descendant
tree) to a specific database. See [`sharding.md`](./sharding.md#explicit-shard-placement-and-data-residency-issue-697)
for the mechanism. Security-relevant properties:

- **`shard_id` is not a capability.** Any caller authorised to start a workflow
  can pin it to any *placeable* shard. Placement selects a database within the
  deployment; it does not grant access to data already there, and there is no
  per-shard authorisation tier. If a caller must be confined to one region,
  enforce that in your own auth layer before delegating to the start route —
  Harvest validates that a requested shard exists and accepts writes, not that
  *this* caller is entitled to it.
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
- **Shard resolution assumes the default (`Auto`) placement (issue #697
  interaction).** Resolving which shard owns a `(workflow_name, workflow_id)`
  target re-derives the same rendezvous hash a fresh start would use
  (`shard::external_target_owning_shard`) — it does not, and cannot without a
  directory lookup, see an explicit shard pin applied at start time
  (`ShardPlacement::Shard`/`ShardPlacement::ResidencyKey`). A workflow started
  with an explicit pin can therefore be unreachable — or, in a genuinely
  pathological multi-tenant layout, misresolved to a shard hosting an
  unrelated `(workflow_name, workflow_id)` pair — via business-key targeting.
  This is a correctness limitation, not a privilege-escalation vector (a
  misresolution surfaces as `target_unknown`/`NoRunFound`, never as access to
  a target the caller could not otherwise reach), but it means
  business-key-addressed signal/cancel should be reserved for workflows known
  to use the default placement; address an explicitly shard-pinned workflow
  by `ExecutionId` instead. A shard-placement-aware directory lookup for
  business-key targeting is a documented follow-up, out of scope for issue
  #751.

---

## CLI token semantics

The Harvest CLI supports `--token <value>` and the `HARVEST_TOKEN` environment
variable. **This only sends credentials — it does not secure the server.**

When the CLI sends a request with `--token`, it sets the `Authorization: Bearer
<token>` header on every request via `reqwest::RequestBuilder::bearer_auth`.
Whether that header is validated depends entirely on the middleware the embedder
configures on the server.

Without authentication middleware:

- The CLI token is sent but ignored by the server.
- Any caller can reach mutating endpoints without credentials.

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
