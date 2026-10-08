## Security — bind the tenant to the principal, per-tenant retention (issue #1977)

ADR 0004 (`docs/adr/0004-tenant-isolation-cells.md`) records the decision in
a new amendment. Hostile multi-tenancy is now in scope at the management
API. It stays out of scope inside the engine.

**The defect.** The authorizer hook read the tenant from
`x-harvest-tenant`, which the caller declares. A caller with a tenant-A
token set the header to B. A hook that trusted the header let it read and
cancel B's runs. The red test
`tenant_a_credential_cannot_reach_tenant_b_by_setting_the_header` got `200`
before the fix and gets `403` after it.

**Tenant-bound credentials.** `POST /admin/tokens` takes `tenant`. The list
route shows it. `harvest token create --tenant` and
`harvest token bootstrap --tenant` set it. An embedder attaches
`autumn_harvest_plugin::tenant::VerifiedTenant` in its own auth layer. One
rule, `autumn_harvest::tenant::validate_tenant`, checks every tenant key: 1
to 128 bytes of visible ASCII, with no spaces.

**The binding layer.** `tenant::enforce_tenant_binding` runs directly inside
the token layer, on every mount. A request with no verified tenant passes
unchanged. For a bound caller:

- a header that names another tenant gets `403`;
- a route outside `TENANT_SCOPED_ROUTES` gets `403`, so list routes, admin
  routes, token mint, Vantage and the deferred start paths are refused;
- a by-id request for a run of another tenant, or a run with no tenant, gets
  `404`;
- a start whose workflow id is in use by a run of another tenant gets
  `409`, and every `409` of a bound start names no run;
- a throttled, debounced or batched start gets `400`.

Each `403`, `404` and `409` refusal writes one `authz.deny` audit row. The
rate limiter now runs before the binding layer, so it bounds refusals too.
The authorizer hook sees the verified tenant in `tenant_key`, and the new
`tenant_verified` flag. Every generated MCP tool route refuses an embedder
tenant. The optional `enforce_token_scope_mcp_mutation` refuses a bound
token.

**The engine guard.** A start with a tenant never attaches to, cancels,
replaces or seals a run of another tenant. The start core checks the prior
row under its lock, and also before the `terminate_if_running` pre-check.
It returns the new `HarvestError::TenantConflict`, which the API maps to
`409`. The review found that without this guard, `terminate_existing` from
tenant A cancelled tenant B's run.

**The run's tenant.** A new nullable column,
`harvest_workflow_executions.tenant`, holds the verified tenant. The HTTP
start route sets it for a bound caller. `StartWorkflowParams::tenant`,
`SignalWithStartParams::tenant`, `UpdateWithStartParams::tenant` and the
typed start options set it in-process. Children, cross-shard children,
retries, continue-as-new successors, reset forks, re-runs and
completion-trigger targets copy it. A
rebalance copies it with the row. Erasure does not touch it. The column is
the 65th of the table, so the workspace now enables diesel's
`128-column-tables` feature.

**Per-tenant retention.** `RetentionConfig::with_tenant_override` and
`with_tenant_overrides` keep every run of one tenant for its own age.
Precedence is the tenant override, then the type override (#737), then the
global `max_age`. A tenant override alone turns on history retention. The
candidate scan resolves the cutoff per row with one more `COALESCE` arm, and
the per-candidate check uses `effective_max_age_for`. `validate` applies the
type-override bounds and the tenant key rule.

**Migration.** `20261008041103_harvest_tenant_binding` adds the two nullable
columns. No index, no data migration, no `WorkflowEvent` variant, no replay
impact.

**Tests.** `autumn-harvest-plugin/tests/tenant_binding_integration.rs` covers
the red test, the row check, the allowlist and the header mismatch. It also
covers start stamping, the start guard, the `409` body, the embedder tenant
and the mint round trip. `tenant_propagation_tests.rs` covers derived runs.
`retention_overrides_tests.rs` adds two DB tests for the tenant override.
Unit tests cover tenant validation, retention resolution and bounds, the
allowlist matcher and the CLI flags.
