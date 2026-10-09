-- The LLM ledger: one row for each recorded model call (issue #1997).
--
-- The budgets of issue #1997 sum these rows. A run cap sums the rows of one
-- execution. A tenant cap sums the rows of one `(workflow_name, quota_key)`
-- pair inside a rolling window.
--
-- The usage is in clear columns, outside the encrypted payload. SQL can sum
-- it without the codec key. The model id and the token counts are therefore
-- visible to anyone who can read the table. See `docs/security-posture.md`.
--
-- A side table, not an event: no `WorkflowEvent` variant, no change to
-- `harvest_events`, no replay impact. The FK drops the rows with their run.
--
-- `quota_key` is copied from the run at write time. The tenant read then
-- needs no join. Both indexes include the summed columns, so each read can
-- be an index-only scan.
--
-- The foreign key locks `harvest_workflow_executions` while it is added. The
-- timeout bounds that wait, so a busy table fails the migration fast instead
-- of stalling every start behind it (issue #1810).
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS harvest_llm_ledger (
    id            INT8        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id  UUID        NOT NULL
                              REFERENCES harvest_workflow_executions (id) ON DELETE CASCADE,
    workflow_name TEXT        NOT NULL,
    quota_key     TEXT        NULL,
    activity_name TEXT        NOT NULL,
    activity_id   UUID        NOT NULL,
    attempt       INT4        NOT NULL CHECK (attempt >= 1),
    model         TEXT        NOT NULL,
    input_tokens  INT8        NOT NULL CHECK (input_tokens >= 0),
    output_tokens INT8        NOT NULL CHECK (output_tokens >= 0),
    cost_micros   INT8        NOT NULL CHECK (cost_micros >= 0),
    latency_ms    INT8        NOT NULL CHECK (latency_ms >= 0),
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_harvest_llm_ledger_run
    ON harvest_llm_ledger (execution_id)
    INCLUDE (input_tokens, output_tokens, cost_micros);

CREATE INDEX IF NOT EXISTS idx_harvest_llm_ledger_tenant
    ON harvest_llm_ledger (workflow_name, quota_key, recorded_at)
    INCLUDE (input_tokens, output_tokens, cost_micros)
    WHERE quota_key IS NOT NULL;
