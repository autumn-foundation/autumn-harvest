-- Outcome index for the build ramp guard (issue #1814).
--
-- Each guard pass counts the runs of two builds on one queue. It reads
-- `queue_name = $q AND assigned_build_id IN ($base, $target) AND
-- created_at >= $step`. No index led with the queue. The planner had to read
-- every run of both builds, or every run of every queue since the step began.
-- This index serves the read as two range scans. The work per pass is
-- bounded by the runs of the ramp step.
--
-- The index holds only rows with an assigned build. A queue with no build
-- policy adds no rows to it.
--
-- On a live deployment, use the concurrent form. It cannot run inside
-- Diesel's migration transaction. Build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_ramp_guard_outcome
--       ON harvest_workflow_executions (queue_name, assigned_build_id, created_at)
--       WHERE assigned_build_id IS NOT NULL;
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
--
-- The plain build waits at most 5 s for its lock, then the migration fails.
-- Run it again, or prebuild the index as shown above.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_index_oid oid;
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_class.oid, pg_get_indexdef(pg_class.oid), pg_index.indisvalid
      INTO existing_index_oid, existing_def, existing_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_we_ramp_guard_outcome'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #1814 operators prebuild it CONCURRENTLY
        CREATE INDEX idx_harvest_we_ramp_guard_outcome
            ON harvest_workflow_executions (queue_name, assigned_build_id, created_at)
            WHERE assigned_build_id IS NOT NULL;
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (queue_name, assigned_build_id, created_at) WHERE (assigned_build_id IS NOT NULL)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_ramp_guard_outcome already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_ramp_guard_outcome already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
