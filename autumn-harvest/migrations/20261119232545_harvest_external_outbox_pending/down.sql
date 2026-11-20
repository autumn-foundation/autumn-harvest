-- Revert: drop the pending table of the external outbox. The claims of the
-- previous release read `harvest_events` and need no data from it.
SET LOCAL lock_timeout = '5s';
DROP TRIGGER IF EXISTS harvest_external_outbox_projection_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_external_outbox_project();
DROP TABLE IF EXISTS harvest_external_outbox_pending;
