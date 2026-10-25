-- Issue #2308: drop the ordered owner scan index.
--
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` on a hot table. `lock_timeout` fails
-- the revert after 5 s instead of queueing every task write behind it.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_tq_owner_order;
