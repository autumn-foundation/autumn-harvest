-- Reverse of the covering index for the tenant LLM spend read (issue #1997).
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS harvest_llm_ledger_run_recorded;
