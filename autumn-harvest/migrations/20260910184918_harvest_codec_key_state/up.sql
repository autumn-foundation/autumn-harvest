-- Durable fleet-wide codec key state (issue #1244).
--
-- Closes the two write-fence gaps issue #1244 identified in #948 / #1242:
-- PayloadCodecs::set_active_key flips only the calling process's in-memory
-- registry, so retire_codec_key had no durable, fleet-wide signal that every
-- writer had observed a rotation before it destroyed a decoder.
--
-- One row per key id. `state` tracks the key through its lifecycle:
--   active   -- new writes encode under this key. At most one row at a time.
--   retiring -- superseded by a newer active key; still decodable, not yet
--               proven writer-free.
--   retired  -- retire_codec_key finalized retirement.
--
-- `harvest_codec_key_state` lives on every shard, mirroring `harvest_workers`
-- and `harvest_codec_rotation_cursor`: this deployment has no global control
-- plane, so a fleet-wide fact is a per-shard row that every writer's scanner
-- tick reads on a bounded interval (see `codec_rotation::refresh_active_codec_key`).
CREATE TABLE IF NOT EXISTS harvest_codec_key_state (
    key_id          TEXT PRIMARY KEY,
    state           TEXT NOT NULL
                    CONSTRAINT harvest_codec_key_state_state_check
                    CHECK (state IN ('active', 'retiring', 'retired')),
    activated_at    TIMESTAMPTZ,
    retiring_since  TIMESTAMPTZ,
    retired_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE harvest_codec_key_state IS
    'Durable fleet-wide codec key lifecycle (issue #1244). Read by every '
    'writer''s scanner tick and by retire_codec_key''s structural write fence.';

-- At most one active key at a time. The indexed expression is constant under
-- the partial predicate, so a second row with state = 'active' collides with
-- the first on the same index entry.
CREATE UNIQUE INDEX IF NOT EXISTS harvest_codec_key_state_one_active_idx
    ON harvest_codec_key_state ((state = 'active'))
    WHERE state = 'active';
