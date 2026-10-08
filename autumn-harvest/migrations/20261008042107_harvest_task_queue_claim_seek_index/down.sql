-- Issue #1971: drop the claim seek index.
--
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` on a hot table. `lock_timeout` fails
-- the revert after 5 s instead of queueing every claim behind it.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_tq_claim_seek;
