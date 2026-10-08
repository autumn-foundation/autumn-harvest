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
--
-- A stored `ramp_id` derives from the request's own id and the base build.
-- A base change re-ids a kept ramp, so a retry derives another stored id.
-- `ramp_caller_id` keeps the request's own id on the row, and the writers
-- retire it too. A retry is then refused whatever the base is now.
ALTER TABLE harvest_build_policies
    ADD COLUMN IF NOT EXISTS ramp_caller_id UUID NULL;

-- The key holds the queue. A library caller can reuse one caller id on two
-- queues, so a retire on one queue must not refuse the id on another.
CREATE TABLE IF NOT EXISTS harvest_ramp_retired_ids (
    queue_name TEXT NOT NULL,
    ramp_id    UUID NOT NULL,
    retired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, ramp_id)
);

-- The guard's abort retires the caller id of the cleared ramp on every pool.
-- A writer from before the `ramp_id` column changes a ramp but keeps the old
-- caller id. That id would then let an old abort clear the new ramp. The
-- reset trigger already drops a stale `ramp_id` on such a write. It now
-- drops the stale `ramp_caller_id` too.
--
-- An id-aware writer sets `harvest.ramp_id_aware` for its transaction. It
-- can keep its id on purpose, for example to change only the percentage
-- under the same caller id. The trigger leaves such a write alone.
CREATE OR REPLACE FUNCTION harvest_build_policies_reset_ramp_id()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF current_setting('harvest.ramp_id_aware', true) = 'on' THEN
        RETURN NEW;
    END IF;
    IF NEW.ramp_id IS NOT DISTINCT FROM OLD.ramp_id
       AND (NEW.target_build_id IS DISTINCT FROM OLD.target_build_id
            OR NEW.ramp_percent IS DISTINCT FROM OLD.ramp_percent
            OR NEW.updated_at IS DISTINCT FROM OLD.updated_at) THEN
        NEW.ramp_id := NULL;
        NEW.ramp_caller_id := NULL;
    END IF;
    RETURN NEW;
END;
$$;
