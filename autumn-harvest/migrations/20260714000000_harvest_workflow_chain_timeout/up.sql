-- Add chain-scoped lifetime cap columns for continue-as-new chains (issue #617).
--
-- Distinct from the per-run execution_timeout (#243): the chain cap is anchored
-- at the FIRST run's start and CARRIED VERBATIM across every continue-as-new, so
-- a runaway loop cannot escape it by continuing-as-new. Enforced by the same
-- timeout scanner and the same WorkflowExecutionTimedOut event (no new event
-- variant).
--
-- chain_execution_timeout: the chain-cap DURATION (mirrors execution_timeout).
-- chain_deadline_at:       the absolute enforcement deadline
--                          (first_run.started_at + chain_execution_timeout),
--                          copied VERBATIM into every continue-as-new successor
--                          (NOT recomputed as now + timeout).
--
-- NULL means no chain cap is configured (legacy behaviour preserved).
-- The scanner performs: WHERE state = 'RUNNING' AND chain_deadline_at < NOW()
-- (OR'd with the per-run deadline_at check), served by the partial index below.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS chain_execution_timeout INTERVAL    NULL,
    ADD COLUMN IF NOT EXISTS chain_deadline_at        TIMESTAMPTZ NULL;

-- Partial index for efficient chain-timeout scanner queries.
-- Only RUNNING executions with a configured chain deadline need to be scanned.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_chain_deadline
    ON harvest_workflow_executions (chain_deadline_at)
    WHERE state = 'RUNNING' AND chain_deadline_at IS NOT NULL;
