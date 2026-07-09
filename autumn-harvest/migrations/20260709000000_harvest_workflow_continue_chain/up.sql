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
