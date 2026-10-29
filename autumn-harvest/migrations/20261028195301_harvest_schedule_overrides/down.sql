-- Issue #2385: drop the operator schedule overrides.
--
-- A dropped override returns each DAG to its code schedule on the next tick.
DROP TABLE IF EXISTS harvest_schedule_overrides;
