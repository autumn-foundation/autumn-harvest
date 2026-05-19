-- Calendar-aware schedules (issue #337)
--
-- Introduces two new tables:
--   harvest_calendars            - named exclusion-set registries
--   harvest_calendar_exclusions  - per-calendar date exclusions
--
-- Adds two columns to harvest_schedules:
--   calendar_name  - references a harvest_calendars.name (nullable)
--   skip_policy    - what to do when the fire date is excluded (default 'skip')
--
-- Three built-in calendars are seeded with holidays for 2025-2026.
-- Built-in calendars have built_in = TRUE; operators may not delete them.

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS harvest_calendars (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    description TEXT,
    built_in    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT harvest_calendars_name_unique UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS harvest_calendar_exclusions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    calendar_name TEXT NOT NULL REFERENCES harvest_calendars(name) ON DELETE CASCADE,
    excluded_date DATE NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT harvest_calendar_exclusions_unique UNIQUE (calendar_name, excluded_date)
);

CREATE INDEX IF NOT EXISTS harvest_calendar_exclusions_name_idx
    ON harvest_calendar_exclusions (calendar_name);

-- ── Schedule columns ──────────────────────────────────────────────────────────

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS calendar_name TEXT
        REFERENCES harvest_calendars(name) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS skip_policy TEXT NOT NULL DEFAULT 'skip';

-- ── Built-in calendars ────────────────────────────────────────────────────────

INSERT INTO harvest_calendars (name, description, built_in) VALUES
    ('weekends-off',          'Excludes Saturdays and Sundays', TRUE),
    ('us-federal-holidays',   'US federal public holidays (observed dates, 2025-2026)', TRUE),
    ('nyse',                  'NYSE market holidays (2025-2026)', TRUE)
ON CONFLICT (name) DO NOTHING;

-- weekends-off: uses a function-based approach; operators load specific dates
-- via PUT /calendars/weekends-off. The built-in seed only registers the calendar
-- with a flag so the scheduler can enumerate it; actual weekend exclusion is
-- handled at query time in the scheduler by inspecting DOW when calendar_name =
-- 'weekends-off'. No static dates are seeded for this calendar.

-- US federal holidays 2025
INSERT INTO harvest_calendar_exclusions (calendar_name, excluded_date) VALUES
    ('us-federal-holidays', '2025-01-01'),
    ('us-federal-holidays', '2025-01-20'),
    ('us-federal-holidays', '2025-02-17'),
    ('us-federal-holidays', '2025-05-26'),
    ('us-federal-holidays', '2025-06-19'),
    ('us-federal-holidays', '2025-07-04'),
    ('us-federal-holidays', '2025-09-01'),
    ('us-federal-holidays', '2025-10-13'),
    ('us-federal-holidays', '2025-11-11'),
    ('us-federal-holidays', '2025-11-27'),
    ('us-federal-holidays', '2025-12-25'),
    -- 2026
    ('us-federal-holidays', '2026-01-01'),
    ('us-federal-holidays', '2026-01-19'),
    ('us-federal-holidays', '2026-02-16'),
    ('us-federal-holidays', '2026-05-25'),
    ('us-federal-holidays', '2026-06-19'),
    ('us-federal-holidays', '2026-07-03'),
    ('us-federal-holidays', '2026-09-07'),
    ('us-federal-holidays', '2026-10-12'),
    ('us-federal-holidays', '2026-11-11'),
    ('us-federal-holidays', '2026-11-26'),
    ('us-federal-holidays', '2026-12-25')
ON CONFLICT (calendar_name, excluded_date) DO NOTHING;

-- NYSE holidays 2025
INSERT INTO harvest_calendar_exclusions (calendar_name, excluded_date) VALUES
    ('nyse', '2025-01-01'),
    ('nyse', '2025-01-20'),
    ('nyse', '2025-02-17'),
    ('nyse', '2025-04-18'),
    ('nyse', '2025-05-26'),
    ('nyse', '2025-06-19'),
    ('nyse', '2025-07-04'),
    ('nyse', '2025-09-01'),
    ('nyse', '2025-11-27'),
    ('nyse', '2025-12-25'),
    -- 2026
    ('nyse', '2026-01-01'),
    ('nyse', '2026-01-19'),
    ('nyse', '2026-02-16'),
    ('nyse', '2026-04-03'),
    ('nyse', '2026-05-25'),
    ('nyse', '2026-06-19'),
    ('nyse', '2026-07-03'),
    ('nyse', '2026-09-07'),
    ('nyse', '2026-11-26'),
    ('nyse', '2026-12-25')
ON CONFLICT (calendar_name, excluded_date) DO NOTHING;
