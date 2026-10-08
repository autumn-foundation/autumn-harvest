-- Tenant binding and per-tenant retention (issue #1977).
--
-- `harvest_api_tokens.tenant` is the tenant claim of a token. NULL means the
-- token is not bound to a tenant. A bound token reaches only the runs of its
-- tenant, through the tenant-scoped routes.
--
-- `harvest_workflow_executions.tenant` is the verified tenant of a run. A
-- start by a bound caller sets it. Children, retries, continue-as-new, reset
-- and re-run copy it. The retention janitor reads it for tenant overrides.
-- NULL means the run has no tenant.
--
-- No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.
--
-- A nullable column with no default needs no table rewrite. ADD COLUMN takes
-- an ACCESS EXCLUSIVE lock for a moment. A held lock fails the migration after
-- 5 s, so the step path does not queue behind it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_api_tokens
    ADD COLUMN IF NOT EXISTS tenant TEXT
        CONSTRAINT harvest_api_tokens_tenant_len
        CHECK (tenant IS NULL OR char_length(tenant) BETWEEN 1 AND 128);

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS tenant TEXT;

COMMENT ON COLUMN harvest_api_tokens.tenant IS
    'Tenant claim of the token (issue #1977). NULL means not tenant-bound.';

COMMENT ON COLUMN harvest_workflow_executions.tenant IS
    'Verified tenant of the run (issue #1977). NULL means no tenant.';
