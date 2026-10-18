-- Cancel reason for a cross-shard child (issue #2194).
--
-- `cancel_reason` is the text that the parent gives its cancel. The relay
-- records it on the child's `WorkflowCancelled` event, as a same-shard cancel
-- does. NULL means no reason: the relay uses the generic parent-cancel text.
--
-- No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.
--
-- A nullable column with no default needs no table rewrite.
-- A held lock fails the migration after 5 s.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_cross_shard_children
    ADD COLUMN IF NOT EXISTS cancel_reason TEXT;
