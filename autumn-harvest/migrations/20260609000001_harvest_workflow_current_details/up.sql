-- Durable workflow status breadcrumb (issue #473).
--
-- Workflow authors call ctx.set_current_details("Step 3/5: awaiting approval")
-- to push a last-write-wins human-readable string that operators can read
-- directly from the execution list (GET /workflows) or detail (GET /workflows/{id})
-- without a live worker or per-row Query fan-out.
--
-- Properties:
--   - NULL when the workflow author never called set_current_details.
--   - Frozen at terminal state: completed/failed/cancelled/timed_out executions
--     retain the last-set value so post-mortem inspection is possible.
--   - Replay-safe: the worker suppresses the DB write during replay cycles
--     (same zero-footprint contract as Query handlers, #234).
--   - Capped at 1 KiB at the application layer; TEXT has no additional DB limit.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS current_details TEXT NULL;
