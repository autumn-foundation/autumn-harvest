-- Rollback: agent cost ledger (issue #1996).
--
-- The drop removes the foreign key triggers on
-- `harvest_workflow_executions`, so the timeout bounds the wait.

SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_llm_ledger;
