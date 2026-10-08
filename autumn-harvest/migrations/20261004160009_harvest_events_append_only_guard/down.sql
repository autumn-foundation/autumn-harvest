-- Remove the append-only guard on `harvest_events` (issue #1817).
SET LOCAL lock_timeout = '5s';

DROP TRIGGER IF EXISTS harvest_events_append_only_trg ON harvest_events;
DROP FUNCTION IF EXISTS harvest_events_guard_append_only();
