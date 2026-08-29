-- Durable resume cursor for the lazy payload-codec re-encryption sweep
-- (issue #948).
--
-- The sweep walks `harvest_events` in `id` order on one shard, decoding
-- payload fields that still carry a retired codec key id and re-encoding them
-- under the active key. This table is the only bookkeeping it needs: where the
-- last batch stopped, so a restart resumes instead of rescanning from zero.
--
-- The primary key is `(shard_id, active_key_id)`, not `(shard_id)`, and that is
-- load-bearing. Progress is only meaningful relative to the key being rotated
-- ONTO: flipping the active key must start a fresh pass over the whole shard,
-- and a rotation that is later rolled back must resume its own earlier pass.
-- Keying the row on the target key id makes both fall out for free, with no
-- reset step an operator can forget to run.
--
-- This table holds ONLY sweep bookkeeping. Nothing here is ever written to
-- `harvest_events`, and the sweep itself changes only the ciphertext bytes
-- inside payload fields -- decoded plaintext, event `type`, event ids, ordering
-- and timestamps are untouched -- so replay of any execution swept under any
-- key is byte-identical. No new `WorkflowEvent` variant.
CREATE TABLE IF NOT EXISTS harvest_codec_rotation_cursor (
    -- The shard this cursor tracks. Shard-local, exactly like every other
    -- background sweep's state.
    shard_id INTEGER NOT NULL,
    -- The key id being rotated ONTO, i.e. the active key at the time the pass
    -- ran.
    active_key_id TEXT NOT NULL,
    -- Highest `harvest_events.id` examined by a completed batch. The next batch
    -- resumes strictly above it.
    last_event_id BIGINT NOT NULL DEFAULT 0,
    -- Total rows whose bytes this pass actually rewrote. Monotonic per pass;
    -- surfaced by `GET /admin/codec/rotation`.
    rows_reencrypted BIGINT NOT NULL DEFAULT 0,
    -- First time this pass observed a batch that reached the end of the shard's
    -- events. Informational: later events are written under the active key
    -- already, so there is nothing left to convert behind the cursor.
    completed_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, active_key_id)
);
