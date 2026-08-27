-- Durable per-execution workflow logs (issue #790).
--
-- Stores the author-emitted log lines a workflow produces through
-- `ctx.logger()` / `ctx.log_*` (issue #379) so an operator can reconstruct what
-- a run was doing from the management API and Vantage UI, without pivoting to
-- external log aggregation and correlating by execution id.
--
-- Deliberately a SEPARATE table, NOT a new `WorkflowEvent` variant and NOT part
-- of `harvest_events`: log lines are OBSERVATIONAL ONLY. They carry no
-- determinism guarantee, are never replayed, and are never read back into
-- workflow logic. The append-only event contract is untouched.
--
-- Shard-local: rows live on the owning shard alongside the execution.

CREATE TABLE IF NOT EXISTS harvest_workflow_logs (
    id               BIGSERIAL   PRIMARY KEY,
    -- ON DELETE CASCADE ties log retention to workflow-history retention: the
    -- lines can never outlive the execution they describe, with no bespoke
    -- delete in the retention janitor (issue #790 AC4).
    workflow_exec_id UUID        NOT NULL
        REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    -- Deterministic logical-position identity minted by the workflow context
    -- (`encode_progress_seq`: epoch-prefixed, per-cycle local index). Doubles
    -- as the total emission order AND the exactly-once dedup key -- a re-driven
    -- decision cycle re-mints the SAME seq, so the writer's ON CONFLICT DO
    -- NOTHING collapses it instead of duplicating the line.
    seq              BIGINT      NOT NULL,
    level            TEXT        NOT NULL,
    message          TEXT        NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_workflow_logs_level_check
        CHECK (level IN ('info', 'warn', 'error'))
);

-- The exactly-once dedup constraint AND the read path's ordering/pagination
-- index in one: `WHERE workflow_exec_id = $1 ORDER BY seq ASC` is an index scan.
CREATE UNIQUE INDEX IF NOT EXISTS harvest_workflow_logs_exec_seq
    ON harvest_workflow_logs (workflow_exec_id, seq);
