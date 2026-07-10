-- Continue-as-new run-chain back-links (issue #701).
-- Additive, nullable columns so pre-migration rows and all non-continued runs
-- are simply NULL. `continued_from_exec_id` links a successor to its immediate
-- predecessor; `first_exec_id` links every successor directly to the chain
-- origin so any chain member resolves the head in one lookup.
ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS continued_from_exec_id UUID NULL;
ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS first_exec_id UUID NULL;

CREATE INDEX IF NOT EXISTS idx_harvest_wfx_first_exec_id
    ON harvest_workflow_executions (first_exec_id)
    WHERE first_exec_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_continued_from
    ON harvest_workflow_executions (continued_from_exec_id)
    WHERE continued_from_exec_id IS NOT NULL;

-- Legacy backward reconstruction (issue #701, Codex P2) gathers a chain's
-- members by (workflow_name, workflow_id). The only existing index on that pair
-- is the PARTIAL uniqueness index (`harvest_we_workflow_name_workflow_id_active_key`,
-- `WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED')`), which does NOT cover
-- terminal CONTINUED_AS_NEW predecessors — exactly the rows the legacy candidate
-- query loads. Add a full (non-partial) index so that gather is an index scan
-- instead of a sequential scan on large legacy tables.
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_workflow_identity
    ON harvest_workflow_executions (workflow_name, workflow_id);
