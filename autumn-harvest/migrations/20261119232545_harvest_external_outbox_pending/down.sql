-- Revert: drop the pending table of the external outbox and its function.
-- The trigger revert runs first, so nothing calls the function.
SET LOCAL lock_timeout = '5s';
DROP FUNCTION IF EXISTS harvest_external_outbox_project();
DROP TABLE IF EXISTS harvest_external_outbox_pending;
