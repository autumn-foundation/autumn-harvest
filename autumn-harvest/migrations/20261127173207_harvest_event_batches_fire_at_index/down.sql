-- Reverse of the fire_at index for the unsharded event-batch scanner.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_event_batches_fire_at;
