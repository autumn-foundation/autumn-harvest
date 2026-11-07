-- Operator overrides for the defaults of an activity type.
--
-- `ActivityInfo` declares a retry policy, `start_to_close`,
-- `heartbeat_timeout`, `schedule_to_start` and `schedule_to_close` in
-- code. This table holds the values that an operator sets at runtime,
-- keyed by the activity name. Precedence: call site, then override, then
-- code, then builder default. A NULL column inherits the code value. A row
-- that sets no column is never stored.
--
-- The override applies at schedule time. The task row stores the resolved
-- values, and a retry reuses the row. So a scheduled activity keeps its
-- values. A local activity freezes its values in `LocalActivityScheduled`.
--
-- The table is fleet-wide and lives on the default shard. Each process loads
-- it into the run-defaults cache, so a schedule pays no query for it.
--
-- The table is new, so no other session holds a lock on it.
--
-- Each bound matches the decode of the engine. One row that the engine
-- cannot read would fail the load of every override. A timeout stops at
-- 100 years (3153600000000 ms). A retry interval stops at 365 days
-- (31536000000 ms), as for the workflow retry override.
CREATE TABLE harvest_activity_default_overrides (
    activity_name             TEXT        PRIMARY KEY,
    retry_max_attempts        INTEGER     CHECK (retry_max_attempts >= 1),
    retry_initial_interval_ms BIGINT
        CHECK (retry_initial_interval_ms BETWEEN 1 AND 31536000000),
    retry_max_interval_ms     BIGINT
        CHECK (retry_max_interval_ms BETWEEN 1 AND 31536000000),
    start_to_close_ms         BIGINT
        CHECK (start_to_close_ms BETWEEN 1 AND 3153600000000),
    heartbeat_timeout_ms      BIGINT
        CHECK (heartbeat_timeout_ms BETWEEN 1 AND 3153600000000),
    schedule_to_start_ms      BIGINT
        CHECK (schedule_to_start_ms BETWEEN 1 AND 3153600000000),
    schedule_to_close_ms      BIGINT
        CHECK (schedule_to_close_ms BETWEEN 1 AND 3153600000000),
    reason                    TEXT,
    updated_by                TEXT        NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_activity_default_overrides_retry_interval_check
        CHECK (retry_max_interval_ms >= retry_initial_interval_ms),
    CONSTRAINT harvest_activity_default_overrides_check
        CHECK (
            retry_max_attempts IS NOT NULL
            OR retry_initial_interval_ms IS NOT NULL
            OR retry_max_interval_ms IS NOT NULL
            OR start_to_close_ms IS NOT NULL
            OR heartbeat_timeout_ms IS NOT NULL
            OR schedule_to_start_ms IS NOT NULL
            OR schedule_to_close_ms IS NOT NULL
        )
);

COMMENT ON TABLE harvest_activity_default_overrides IS
    'Operator overrides for the retry policy and the timeouts of an activity type. A NULL column inherits the code value.';
