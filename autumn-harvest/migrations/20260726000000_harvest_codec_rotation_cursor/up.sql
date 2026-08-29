-- Durable resume cursor for the lazy payload-codec re-encryption sweep
-- (issue #948).
--
-- The sweep walks `harvest_events` in `id` order on one shard, decoding
-- payload fields that still carry a retired codec key id and re-encoding them
-- under the active key. This table is the bookkeeping it needs: which key the
-- current pass is converting ONTO, where the last batch stopped, and whether
-- that pass left anything unconverted behind it.
--
-- ONE ROW PER SHARD, with the target key stored as a column rather than as
-- part of the key. That is load-bearing. Progress is only meaningful relative
-- to the key being rotated onto, so ANY change of active key -- a rotation, a
-- second rotation, or a ROLLBACK to a key that already had a pass of its own --
-- must start a fresh scan of the whole shard. Keying the row on
-- `(shard_id, active_key_id)` instead would resume a rolled-back-to key's old,
-- already-completed cursor and silently skip every row written under the key
-- being rolled back FROM.
--
-- `unresolved_rows` is what makes the sweep eventually consistent. A row the
-- pass could not convert -- an unregistered key id, corrupt ciphertext, or a
-- compare-and-swap lost to a concurrent PII erasure -- is skipped and counted
-- here, and the cursor still advances so one bad row cannot wedge the pass.
-- When the pass reaches the end of the shard with a non-zero count, the cursor
-- resets to 0 and the pass runs again instead of being marked complete, so a
-- transient failure (a key registered late, a lost race) always gets another
-- chance. `completed_at` is therefore only ever stamped on a pass that
-- converted everything it saw -- it means "this key is safe to gate on", not
-- merely "the scan ran off the end".
--
-- This table holds ONLY sweep bookkeeping. Nothing here is ever written to
-- `harvest_events`, and the sweep itself changes only the ciphertext bytes
-- inside payload fields -- decoded plaintext, event `type`, event ids, ordering
-- and timestamps are untouched -- so replay of any execution swept under any
-- key is byte-identical. No new `WorkflowEvent` variant.
CREATE TABLE IF NOT EXISTS harvest_codec_rotation_cursor (
    -- The shard this cursor tracks. Shard-local, exactly like every other
    -- background sweep's state.
    shard_id INTEGER PRIMARY KEY,
    -- The key id the CURRENT pass is converting onto. When the process's
    -- active key no longer matches this, the pass is stale and restarts.
    active_key_id TEXT NOT NULL,
    -- Highest `harvest_events.id` examined by the current pass. The next batch
    -- resumes strictly above it.
    last_event_id BIGINT NOT NULL DEFAULT 0,
    -- Rows whose bytes this pass actually rewrote. Surfaced by
    -- `GET /admin/codec/rotation`.
    rows_reencrypted BIGINT NOT NULL DEFAULT 0,
    -- Rows this pass examined but could not convert. Non-zero at the end of a
    -- pass forces another pass rather than a completion stamp.
    unresolved_rows BIGINT NOT NULL DEFAULT 0,
    -- Set only when a pass reached the end of the shard having converted
    -- everything it saw. NULL while a pass is in flight or had unresolved rows.
    completed_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
