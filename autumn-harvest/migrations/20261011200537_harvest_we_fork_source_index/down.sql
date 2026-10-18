-- Issue #2000: drop the fork source index.
--
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` on a hot table. `lock_timeout` fails
-- the revert after 5 s instead of queueing every start behind it.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_we_fork_source;
