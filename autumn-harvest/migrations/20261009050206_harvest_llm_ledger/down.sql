-- Revert the LLM ledger (issue #1997). This deletes the recorded usage, so
-- the budgets start from zero after a later upgrade.
--
-- The drop also removes the FK triggers on `harvest_workflow_executions`. The
-- timeout bounds that lock wait (issue #1810).
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_llm_ledger;
