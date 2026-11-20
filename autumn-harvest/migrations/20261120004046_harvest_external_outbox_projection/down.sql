-- Revert: drop the trigger of the external outbox. The table stays until the
-- previous migration reverts.
SET LOCAL lock_timeout = '5s';
DROP TRIGGER IF EXISTS harvest_external_outbox_projection_trg ON harvest_events;
