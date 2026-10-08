# Design — Issue #1977: bind the tenant to the principal, per-tenant retention

Issue #1977 lists three gaps:

1. The authorizer hook reads the tenant from `x-harvest-tenant`. The caller
   declares that header, so a caller can claim any tenant.
2. Harvest refuses nothing when the declared tenant is not the caller's.
3. Retention has a per-type override (#737), but no per-tenant override.

**One migration. No new `WorkflowEvent` variant. No new route.**

---

## 0. Planning record

### 0.1 Scope decision — is hostile multi-tenancy in scope?

ADR 0004 (#1837) said no. It said a cell bounds load, not access. This issue
asks for the decision again. The decision, recorded in ADR 0004 as an
amendment:

- **In scope at the management API.** A credential carries a tenant. A
  tenant-bound caller reaches only its own executions, through a fixed set of
  routes. Harvest enforces this without an authorizer.
- **Out of scope inside the engine.** Harvest adds no namespaces and no row
  filters on list routes. Workflow code is trusted. Cells still bound load.

### 0.2 Brainstorm — where does the tenant come from, and where does it go?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Keep the header. Document that the authorizer must check it. | Rejected. That is the defect. |
| B2 | A `tenant` column on `harvest_api_tokens`. The token layer reads it. | **Adopted.** One column on a small control-shard table. |
| B3 | A public `VerifiedTenant` request extension. The embedder's auth layer sets it. | **Adopted.** It is the host-app principal claim the issue asks for. |
| B4 | Give the authorizer the verified tenant in `tenant_key`, and a `tenant_verified` flag. | **Adopted.** A policy can refuse an unverified tenant. |
| B5 | Refuse a header that names another tenant. | **Adopted.** `403`, and one `authz.deny` audit row. |
| B6 | Stamp the tenant in `context_headers`. | Rejected. PII erasure sets `context_headers` to NULL, so an erased run loses its tenant retention. A caller can also write the key. |
| B7 | Stamp the tenant in `search_attrs`. | Rejected. Workflow code can change it, and children do not inherit it. |
| B8 | A `tenant` column on `harvest_workflow_executions`. Children, retries, continue-as-new, reset and re-run copy it. | **Adopted.** Retention and the row check read it. Rebalance copies every column, so it moves with the run. |
| B9 | Filter every list route by tenant. | Rejected. That is the namespace design ADR 0004 refused. |
| B10 | Give a tenant-bound caller a fixed route allowlist. Refuse every other route. | **Adopted.** Fail closed. A list route, an admin route and token mint are refused. |
| B11 | Per-tenant retention keyed on the new column, beside the per-type override. | **Adopted.** See §1.5. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Trust the header when a credential has a tenant. | The verified tenant wins. A different header is `403`. Red test first. |
| R2 | Let a tenant-bound admin token mint an untenanted token. | Token routes are not in the allowlist. A test mints with a tenant admin token and expects `403`. |
| R3 | Read another tenant's run by id. | The row check compares the stored tenant. A mismatch is `404`, the same as an unknown id. |
| R4 | Read an untenanted run by id. | A NULL tenant is a mismatch. Fail closed. |
| R5 | Reach another tenant's data through a list route or Vantage. | Only allowlisted templates pass. `/ui` paths are not in the list. |
| R6 | Start through a path that drops the tenant (debounce, batch, signal-with-start, MCP tools). | Those routes are not in the allowlist. A deferred start body is refused. Each MCP tool route refuses a bound caller. |
| R7 | Lose the tenant across a retry, a continue-as-new, a child, a reset, a re-run or a rebalance. | Each path copies the column. A DB test per path. |
| R8 | Spoof the tenant in a start body. | The start body has no tenant field. Only a verified tenant is stamped. |
| R9 | Delete a run too early through a tenant override. | Validated bounds, as for type overrides. The SQL cutoff is a superset check, and Rust re-checks each candidate. Legal hold still wins. |
| R10 | Break an existing deployment. | No tenant means no change. The binding layer passes every request with no verified tenant. |
| R11 | Make the hot table migration lock for long. | `ADD COLUMN` with no default is a catalog change. `lock_timeout = '5s'` first. |
| R12 | Learn about, or change, a run of another tenant through a start collision. | The engine refuses a tenant start that meets a run of another tenant, under the row lock, before it attaches, cancels or replaces. Every `409` of a bound start names no run. |

### 0.4 Six thinking hats — tenant binding (B2, B3, B8, B10)

| Hat | Notes |
|-----|-------|
| White | The token layer looks up each request in `harvest_api_tokens`. The header is read only in `authz.rs`. Rebalance copies rows with `jsonb_populate_record`. Erasure NULLs `context_headers`. |
| Red | "A tenant token sees its own runs and nothing else" is easy to explain and to audit. |
| Black | The allowlist is narrow. A tenant cannot list its runs. A tenant client must keep the ids it gets from start. A collision on `workflow_id` across tenants still shows that an id is in use. Document: prefix workflow ids with the tenant. |
| Yellow | Fail closed. No authorizer is needed for the base rule. The authorizer still adds policy on top. The same column drives retention. |
| Green | B9 (list filters) can come later. The allowlist is one constant, so a later change can add routes. |
| Blue | Red: the cross-tenant header test, the row test and the retention tests fail. Green: migration, token column, binding layer, stamping, propagation, retention. Refactor: docs, ADR, review. |

### 0.5 Six thinking hats — per-tenant retention precedence

| Hat | Notes |
|-----|-------|
| White | Today: type override, then global `max_age`. Legal hold exempts a run. |
| Red | A tenant contract ("delete my data after 30 days") is the strongest claim on a run. |
| Black | A tenant override can shorten a type that a team wants to keep. A legal hold is the tool to keep a run. |
| Yellow | Precedence is strict and simple: tenant, then type, then global. One SQL `COALESCE`. |
| Green | A `(tenant, type)` pair override. Deferred: no request for it. |
| Blue | Unit tests for resolution and validation. A DB test where one tenant expires and another tenant of the same type stays. |

---

## 1. Design

### 1.1 Migration

`YYYYMMDDHHMMSS_harvest_tenant_binding`:

- `harvest_api_tokens.tenant TEXT NULL`, with a `CHECK` on length 1 to 128.
- `harvest_workflow_executions.tenant TEXT NULL`. No index. No default.
- `SET LOCAL lock_timeout = '5s'` before each `ALTER`.

The executions `table!` had 64 columns, the limit of diesel's
`64-column-tables` feature. The new column needs `128-column-tables`. The
other way keeps the column out of `table!` and writes it with raw SQL on each
insert path. That spreads the copy rule over eight paths, and the next column
meets the same limit. The cost of the chosen way is a slower cold build of
diesel. CI caches dependencies, so it pays that cost once per lock file.

### 1.2 Verified tenant

- `TokenPrincipal` gets `tenant: Option<String>`. The token layer reads it
  from the row.
- `autumn_harvest_plugin::tenant::VerifiedTenant(String)` is a public request
  extension. The embedder's auth layer inserts it.
- The binding layer resolves one verified tenant. A token tenant and an
  embedder tenant that differ is `403`.
- The mint route takes `tenant`. The list route shows it. The CLI bootstrap
  takes `--tenant`.

### 1.3 Binding layer

`enforce_tenant_binding` runs directly inside the token layer, on every
mount. With no verified tenant, it passes the request on unchanged. With a
verified tenant `T`:

1. A header that is not `T` is `403` and an `authz.deny` row.
2. A route not in `TENANT_SCOPED_ROUTES` is `403` and an `authz.deny` row.
3. On a route with an execution id, the live row must have tenant `T`.
   Otherwise `404`.
4. It inserts `VerifiedTenant(T)` for the authorizer and the start handler.

### 1.4 Stamping and propagation

- `StartWorkflowParams.tenant` stamps `harvest_workflow_executions.tenant`.
  `SignalWithStartParams`, `UpdateWithStartParams` and the three typed start
  option structs get the same field, for in-process code.
- The HTTP start route sets it from `VerifiedTenant`. The start core refuses
  a tenant start that meets a run of another tenant with
  `HarvestError::TenantConflict`. The API answers `409`.
- Children, cross-shard children, retries, continue-as-new, reset, re-run
  and completion-trigger targets copy the source row's tenant.

### 1.5 Per-tenant retention

- `RetentionConfig.tenant_overrides: BTreeMap<String, u64>`, with
  `with_tenant_override` and `with_tenant_overrides`.
- Precedence: tenant override, then type override, then global `max_age`.
- `effective_max_age_for(workflow_name, tenant)` resolves it.
- The candidate SQL resolves the cutoff per row with one more `COALESCE` arm.
- `validate` applies the type-override bounds. A tenant key is 1 to 128 bytes.

### 1.6 Authorizer

`AuthzRequest.tenant_key` is the verified tenant when one exists. Otherwise
it is the header, as before. `AuthzRequest.tenant_verified` tells them apart.
