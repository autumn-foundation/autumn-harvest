-- Persist scheduler decisions (issue #325)
--
-- Adds a new harvest_schedule_decisions table to record scheduler fire/skip actions.

CREATE TABLE IF NOT EXISTS harvest_schedule_decisions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id   UUID REFERENCES harvest_schedules(id) ON DELETE CASCADE,
    schedule_name TEXT NOT NULL,
    target_kind   TEXT NOT NULL CHECK (target_kind IN ('workflow', 'dag')),
    decision      TEXT NOT NULL CHECK (decision IN ('fired', 'skipped', 'suppressed_paused', 'backfilled')),
    reason_code   TEXT NOT NULL,
    detail        JSONB,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_fire_at  TIMESTAMPTZ NOT NULL,
    shard_id      SMALLINT NOT NULL
);

-- Recency-ordered queries for a single schedule
CREATE INDEX IF NOT EXISTS harvest_sched_dec_schedule_id_occurred_at_idx
    ON harvest_schedule_decisions (schedule_id, occurred_at DESC)
    WHERE schedule_id IS NOT NULL;

-- Recency-ordered fleet-wide queries
CREATE INDEX IF NOT EXISTS harvest_sched_dec_occurred_at_idx
    ON harvest_schedule_decisions (occurred_at DESC);
