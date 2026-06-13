-- Add soft SLA tracking columns for non-fatal breach signal (issue #487).
--
-- sla:             Optional declared SLA budget (mirrors execution_timeout).
--                  Stored so continue-as-new and reset can re-anchor per run.
-- sla_deadline_at: Computed at start as started_at + sla. NULL = no SLA.
-- sla_breached:    Server-side observation flag set exactly once by the scanner.
--                  Never written by the workflow engine; leaves harvest_events
--                  untouched (zero replay footprint, like query handlers).
-- sla_breached_at: Wall-clock instant the breach was first detected (UTC).
--
-- The scanner query is:
--   WHERE state = 'RUNNING'
--     AND sla_deadline_at IS NOT NULL
--     AND sla_deadline_at < NOW()
--     AND sla_breached = FALSE
-- which is served by the partial index below without a sequential scan.
-- Only RUNNING is scanned (mirrors the #243 execution-timeout scanner): a
-- PAUSED run must not breach mid-pause, and SUSPENDED is not a persisted state
-- (the harvest_workflow_executions state CHECK constraint forbids it).

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS sla              INTERVAL    NULL,
    ADD COLUMN IF NOT EXISTS sla_deadline_at  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS sla_breached     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sla_breached_at  TIMESTAMPTZ NULL;

-- Partial index: only indexes rows the scanner will actually visit.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_sla_deadline
    ON harvest_workflow_executions (sla_deadline_at)
    WHERE state = 'RUNNING'
      AND sla_deadline_at IS NOT NULL
      AND sla_breached = FALSE;
