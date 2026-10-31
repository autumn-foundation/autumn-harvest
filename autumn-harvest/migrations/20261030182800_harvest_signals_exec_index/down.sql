-- Revert issue #2367. Drop the signal index.
--
-- After this revert, a workflow retry, a PII erasure, a shard rebalance and
-- each execution delete read the whole signals table again.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2367 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_signals_exec;
