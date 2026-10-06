-- Retired ramp ids for the build routing writers (issue #1814).
--
-- A keyed ramp request derives its `ramp_id` from the request, so a late
-- retry derives the same id. An operator can clear or replace that ramp
-- first. Each ramp writer and the manual clear record the `ramp_id` that they
-- remove here, on the pool that held it. A later write of a recorded id is
-- refused, so a stale retry cannot undo the operator's change.
--
-- The guard's own clears are recorded in `ramp_aborted` and in
-- `harvest_ramp_abort_reports`. This table holds only ids that a writer
-- removed. It holds one small row per removed ramp.
CREATE TABLE IF NOT EXISTS harvest_ramp_retired_ids (
    ramp_id    UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    retired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
