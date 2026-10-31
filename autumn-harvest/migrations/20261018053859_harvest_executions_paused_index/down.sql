-- Issue #2212: drop the PAUSED-run index.
--
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` on a hot table. `lock_timeout` fails
-- the revert after 5 s instead of queueing every state change behind it.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_we_paused;
