-- Index the (workflow_name, workflow_id) lookup used by every throttled
-- admission's idempotent-retry shortcut (issue #607 code review).
--
-- `existing_pending_throttle_for_workflow_id` and its `skip_size_check`
-- caller both filter harvest_start_throttle by (workflow_name, workflow_id).
-- Without this index, that lookup is a sequential scan of the whole pending
-- backlog on every admission (reserve, defer, or size-check pre-check),
-- making admission latency O(backlog) once a key's queue grows.
--
-- Non-unique: an idempotent retry landing on the same row is enforced in
-- application code (reserve_or_defer's own check-then-insert), not by a
-- database constraint -- a benign race between two concurrent admissions for
-- a brand-new workflow_id can (rarely) insert two rows; the second one to
-- fire simply hits AlreadyExists and refunds its token. A unique index would
-- require additional conflict-handling logic and is a separate design
-- decision from the performance fix this migration addresses.
CREATE INDEX IF NOT EXISTS idx_harvest_start_throttle_workflow_id
    ON harvest_start_throttle (workflow_name, workflow_id);
