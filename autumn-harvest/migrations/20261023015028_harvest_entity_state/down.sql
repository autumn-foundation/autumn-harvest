-- Revert: drop the keyed-entity state projection (issue #2259).
SET LOCAL lock_timeout = '5s';
DROP TRIGGER IF EXISTS harvest_entity_state_projection_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_entity_state_project();
DROP TABLE IF EXISTS harvest_entity_state;
