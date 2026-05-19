-- Revert calendar-aware schedules (issue #337)

ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS calendar_name,
    DROP COLUMN IF EXISTS skip_policy;

DROP TABLE IF EXISTS harvest_calendar_exclusions;
DROP TABLE IF EXISTS harvest_calendars;
