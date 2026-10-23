-- Issue #2255: drop the owner decider scan index and the fused fence.
--
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` on a hot table. `lock_timeout` fails
-- the revert after 5 s instead of queueing every task write behind it.
SET LOCAL lock_timeout = '5s';

DROP FUNCTION IF EXISTS harvest_decision_fence(INTEGER, INTEGER, TEXT);
DROP INDEX IF EXISTS idx_harvest_tq_owner_scan;
