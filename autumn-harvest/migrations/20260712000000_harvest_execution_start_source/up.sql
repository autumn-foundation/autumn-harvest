-- Workflow-start provenance classifier (issue #740).
--
-- Three additive, nullable columns on harvest_workflow_executions recording
-- HOW an execution was started (start_source), an optional correlation
-- reference (start_source_ref, e.g. the triggering execution id or schedule
-- id), and an optional human/operator attribution (started_by). Distinct from
-- the `origin` column (issue #534), which classifies scheduled dispatch
-- sub-kinds; the two coexist.
--
-- NO backfill: pre-upgrade rows keep start_source = NULL, which the read model
-- reports as "unknown". A start-source column is never read during replay, so
-- this is replay-safe by construction.
ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source TEXT NULL;
ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source_ref TEXT NULL;
ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS started_by TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_harvest_wfx_start_source
    ON harvest_workflow_executions (start_source)
    WHERE start_source IS NOT NULL;
