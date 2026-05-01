-- Track per-target dispatch durably so resume after crash excludes
-- already-dispatched workflows by identity instead of by counter offset.
-- Necessary for Signal batches because signaled workflows stay RUNNING
-- and would otherwise be re-dispatched (or skipped incorrectly when the
-- RUNNING set shifts due to natural completions during recovery).
ALTER TABLE harvest_batch_jobs
    ADD COLUMN processed_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
