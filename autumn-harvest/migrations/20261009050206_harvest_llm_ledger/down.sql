-- Revert the LLM ledger (issue #1997). This deletes the recorded usage, so
-- the budgets start from zero after a later upgrade.
DROP TABLE IF EXISTS harvest_llm_ledger;
