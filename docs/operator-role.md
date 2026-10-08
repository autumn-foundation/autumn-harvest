# Read-only operator role (least-privilege triage access)

The Harvest management API historically had exactly one authorization tier:
every protected route was gated by the same admin check, so a principal either
held full admin — able to terminate, cancel, reset, pause, force-open circuit
breakers, bulk-replay the DLQ, and run fleet-wide batch mutations — or held
nothing at all. There was no read-only tier.

The **read-only operator role** (issue #776) closes that gap. It lets you expose
triage and status dashboards to support engineers, on-call responders, or
internal status pages with **read but not mutate** access, in **one builder
call** — without hand-rolling a reverse proxy that allowlists GET routes.

---

## The single opt-in call

```rust
use autumn_harvest_plugin::HarvestPlugin;

let plugin = HarvestPlugin::new()
    // ... .workflows(...).activities(...).worker(...) ...
    .api_with_role_auth("/api/harvest", my_auth_middleware);
```

`api_with_role_auth(path, middleware)` behaves exactly like
[`api_with_auth`](./security-posture.md) — `middleware` is your application's
authentication layer, applied to the whole management router — and additionally
installs a class-aware enforcement layer that gives the least-privilege
read-only tier. The default `api(path)` and `api_with_auth(path, mw)` mounts are
**unchanged**: the enforcement layer is installed only under
`api_with_role_auth`.

---

## The Session claim contract

The read-only restriction applies **only** to a principal that your
authentication `middleware` explicitly marks read-only on the autumn-web
`Session`. Set one of the following:

| Session field | Read-only when value is |
|---|---|
| `role` | `harvest_readonly`, `harvest_operator`, `operator`, `readonly`, or `read_only` |
| `is_harvest_readonly` | `"true"` or `"1"` |
| `is_harvest_operator` | `"true"` or `"1"` |

A principal is treated as **full-access** (never verb-restricted) when the
Session carries an **admin** marker — `role` ∈ {`admin`, `harvest_admin`}, or a
truthy `is_harvest_admin` / `is_admin` — or when it carries **no marker at all**.
The admin marker always wins over a concurrent read-only marker.

> **Opt-in per principal.** A principal your middleware lets through with no
> marker keeps today's boundary semantics (full access). The read-only tier
> restricts only the principals you deliberately tag. This preserves backward
> compatibility for existing single-admin deployments.

Example autumn-web middleware setting the claim (illustrative):

```rust
// After your identity provider resolves the caller, before Harvest sees it:
if caller.is_support_engineer() {
    session.set("role", "harvest_readonly").await;
}
```

---

## What a read-only principal can and cannot do

- **Reaches 100% of `RouteClass::ReadOnly` routes** — list/describe/history/
  export/query/preview/stack/timeline/eligibility/schedule-read/etc. This
  includes read routes that are otherwise admin-gated, and the SSE event stream.
- **Receives `403 Forbidden` on every `RouteClass::Mutating` management route**
  — terminate, cancel, reset, pause/resume, signal, update, batch start/cancel/
  signal/reset, DLQ replay/discard/redrive, circuit force-open/close, schedule
  create/update/pause/resume/backfill/trigger/delete, retention run-now,
  rate-limit setters, PII erasure, legal hold, build-routing policy/ramp
  changes, task reprioritize, worker drain, calendar/completion-trigger CRUD,
  external-activity completion, and any route not yet classified (see
  *fail-closed* below).
- **Receives `403 Forbidden` on every mutating MCP tool** — when
  [`mcp_tools()`](./mcp-tools.md) is enabled, the generated `start_{wf}`,
  `signal_{wf}`, `{wf}_update_{name}`, and `start_{dag}` tools are covered by
  the same read-only class gate (the `{wf}_status` / `{wf}_watch` **read** tools
  stay reachable). The gate applies to both the tool's own HTTP path and the
  `/mcp` JSON-RPC `tools/call` envelope (autumn-web re-dispatches the envelope
  through the same route). See *MCP tool routes* below.

The `403` is distinct from the anonymous `401` the admin gate returns, so an
authenticated-but-insufficient read-only principal is distinguishable from an
unauthenticated one. Its body carries a stable marker:

```json
{ "error": "read-only principal: mutation not permitted" }
```

### Verb restriction, not field or row restriction

This slice restricts **verbs** (read vs mutate), not **fields** or **rows**. A
read-only principal sees every row an admin would, and — because the read-only
tier is mounted behind your auth boundary — gets admin-level **reads**,
including payload decoding
([issue #608](./operations/read-path-decode.md)) and the SSE event stream. If
you need field-level redaction or per-tenant row scoping, that is a separate
concern outside this role.

---

## Single source of truth + fail closed

Enforcement is driven entirely by
`autumn_harvest::audit::CLASSIFIED_ROUTES` — the same table that classifies
every route for the audit trail — so there is **no second hand-maintained list
of mutating routes to drift out of sync**. A `contract_regression` test asserts
every route in the mounted route inventory has a classification entry.

The enforcement layer **fails closed**: a request whose method + path matches no
`CLASSIFIED_ROUTES` entry resolves to `Mutating` and is denied to read-only
principals. A route a contributor forgot to classify can never be silently
exposed to a read-only principal — the worst case is that it is over-restricted
(a read that returns `403` until it is classified `ReadOnly`).

### How the match works

The layer keys on `request.method()` and `request.uri().path()` (the
nest-stripped, mount-relative path), matched via `matchit` — the same radix-tree
matcher axum uses — so static-beats-param precedence is correct
(`POST /workflows/batch_start` is never mistaken for the `ReadOnly`
`/workflows/{id}` route). It deliberately does **not** use axum's `MatchedPath`,
which is unreliable when read from a layer on a nested router (axum #1441).
Classification keys on the route class, **never the HTTP verb**: the two
`ReadOnly` routes that use POST (`POST /workflows/{id}/query/{query_name}` and
`POST /admin/build-routing/retire`) are correctly reachable by a read-only
principal.

---

## MCP tool routes

The generated [MCP tool routes](./mcp-tools.md) are registered app-level (via
`AppBuilder::routes`), **outside** the nested management router that carries the
`enforce_read_only_class` layer. Under `api_with_role_auth` they therefore carry
their **own** class gate (`enforce_read_only_mcp_mutation`), installed on every
**mutating** tool (`start_{wf}` / `signal_{wf}` / `{wf}_update_{name}` /
`start_{dag}`) and driven by `ToolKind::is_mutation()`. A read-only principal is
`403`'d on those and reaches the **read** tools (`{wf}_status` / `{wf}_watch`)
unchanged. Because autumn-web re-dispatches the `/mcp` JSON-RPC `tools/call`
envelope through the same route with the caller's forwarded credential, this one
gate closes both the direct-HTTP and the envelope invocation paths. When
`api_with_role_auth` is not used, the gate is not installed and the MCP routes
are byte-for-byte their pre-#776 shape.

## Known limitation: the Vantage UI (`/ui`)

The nested `/ui` sub-router's paths are not part of `CLASSIFIED_ROUTES`, so under
fail-closed enforcement a read-only principal receives `403` on `/ui`. Admins
reach `/ui` unchanged. Classifying the UI routes so read-only principals can view
(but not act through) the dashboard is a documented follow-up.

## Known limitation: read-capability breadth

This slice restricts **verbs**, not **fields** or **rows** (see above): a
read-only principal, mounted behind your auth boundary, gets admin-level
**reads** — including payload decoding ([#608](./operations/read-path-decode.md))
and the SSE event stream. Field-level redaction and per-tenant row scoping are
out of scope for this role.

---

## Out of scope

- Fine-grained per-workflow-type, per-queue, or per-namespace RBAC.
- Multi-tenant data isolation / row-level scoping.
- Arbitrary custom per-route permission policies beyond the binary read/mutate
  classification.
- Changing the default single-admin boundary or the `has_harvest_admin_access`
  semantics for existing callers.
