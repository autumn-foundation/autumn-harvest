-- Schedule backfill log (issue #177)
--
-- Durable record of each backfill request: requested window, actor, outcome
-- counts, and timestamps. Queried by GET /admin/schedules to surface recent
-- backfill activity per schedule without a full audit log scan.

CREATE TABLE IF NOT EXISTS harvest_backfill_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id     UUID        NOT NULL,
    actor           TEXT        NOT NULL DEFAULT 'unknown',
    source          TEXT        NOT NULL DEFAULT '',
    from_ts         TIMESTAMPTZ NOT NULL,
    to_ts           TIMESTAMPTZ NOT NULL,
    dry_run         BOOLEAN     NOT NULL DEFAULT FALSE,
    total           INT         NOT NULL DEFAULT 0,
    dispatched      INT         NOT NULL DEFAULT 0,
    skipped         INT         NOT NULL DEFAULT 0,
    failed          INT         NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL,
    error_summary   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

-- Index to efficiently fetch the most recent backfill for a given schedule.
CREATE INDEX IF NOT EXISTS harvest_backfill_log_schedule_id_idx
    ON harvest_backfill_log (schedule_id, started_at DESC);
