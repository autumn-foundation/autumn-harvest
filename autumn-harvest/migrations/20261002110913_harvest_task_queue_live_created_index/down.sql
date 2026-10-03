-- Issue #1795: drop the live-row index for timeout sweeps.
DROP INDEX IF EXISTS idx_harvest_tq_live_created;
