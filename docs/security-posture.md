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
