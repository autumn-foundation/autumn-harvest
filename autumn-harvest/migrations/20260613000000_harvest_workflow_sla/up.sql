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
--   WHERE state <> 'PAUSED'
--     AND sla_deadline_at IS NOT NULL
--     AND sla_breached = FALSE
--     AND sla_deadline_at < COALESCE(completed_at, NOW())
-- which is served by the partial index below without a sequential scan.
-- RUNNING rows (completed_at NULL) compare against NOW(); already-terminal rows
-- compare against their actual completion instant, so a run that finished before
-- its deadline never breaches, while a run that crossed the deadline just before
-- going terminal is still caught. PAUSED rows are excluded: pause suspends the
-- SLA clock.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS sla              INTERVAL    NULL,
    ADD COLUMN IF NOT EXISTS sla_deadline_at  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS sla_breached     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sla_breached_at  TIMESTAMPTZ NULL;

-- Partial index: only indexes rows the scanner can still flip. On-time terminal
-- rows (completed_at <= sla_deadline_at) and PAUSED rows are excluded so the
-- index does not accumulate every SLA-bearing completed run.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_sla_deadline
    ON harvest_workflow_executions (sla_deadline_at)
    WHERE sla_deadline_at IS NOT NULL
      AND sla_breached = FALSE
      AND state <> 'PAUSED'
      AND (completed_at IS NULL OR completed_at > sla_deadline_at);
