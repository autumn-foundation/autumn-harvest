-- Operator overrides for code-declared schedules (issue #2385, epic #2384).
--
-- A DAG declares its schedule in code. The scheduler converges each
-- `harvest_schedules` row to that code value on every tick, so a direct edit
-- of the row does not last. This table holds the values that an operator sets
-- at runtime. The scheduler applies them to its catalog before registration,
-- so the row converges to the override instead of the code value.
--
-- Precedence: override, then code. A NULL column inherits the code value. A
-- row with every column NULL is never stored; the clear path deletes it.
--
-- The row lives on the shard that owns the DAG, next to its schedule row.
-- `kind` is `dag` only. Code-declared workflow schedules (issue #2386) add
-- their value to the check.
--
-- `schedule_expr` uses the stored form of `harvest_schedules.schedule_expr`:
-- `cron:<expr>`, `cron_tz:<tz>:<expr>`, `interval:<secs>` or `manual`.
--
-- The table is new, so no other session holds a lock on it.
CREATE TABLE harvest_schedule_overrides (
    kind            TEXT        NOT NULL CHECK (kind IN ('dag')),
    name            TEXT        NOT NULL,
    schedule_expr   TEXT,
    catchup         BOOLEAN,
    max_active_runs INTEGER     CHECK (max_active_runs >= 0),
    jitter_secs     BIGINT      CHECK (jitter_secs >= 0),
    overlap_policy  TEXT,
    buffer_all_max  INTEGER     CHECK (buffer_all_max >= 0),
    reason          TEXT,
    updated_by      TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, name)
);

COMMENT ON TABLE harvest_schedule_overrides IS
    'Issue #2385: operator overrides for code-declared schedules. A NULL column inherits the code value.';
