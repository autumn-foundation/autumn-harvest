-- Cross-region DR write-authority fencing (issue #954).
--
-- One row per shard, in that shard's own database: a monotonic epoch for "who
-- is allowed to write here". A worker reads the epoch once at startup and pins
-- it; the claim query cross-joins this row and the persist path asserts it, so
-- a worker still pinned to a pre-failover epoch structurally cannot claim tasks
-- or append events after a standby is promoted and its epoch bumped.
--
-- Deliberately NOT seeded here. A shard's database cannot know its own shard
-- id, and seeding a speculative `(0, 0)` into every shard's database would put
-- a row with the wrong identity in shards 1..N. The row is provisioned by
-- `replication::ensure_generation_row` at worker startup, which does know.
-- Until then the table is empty and fencing is simply off — the pre-#954
-- behaviour, byte for byte.
--
-- No new `WorkflowEvent` variant, no change to any existing table, no replay
-- impact: this is additive fencing metadata read by the claim/persist SQL, not
-- workflow history.

CREATE TABLE IF NOT EXISTS harvest_shard_generation (
    shard_id      INTEGER     PRIMARY KEY,
    generation    BIGINT      NOT NULL DEFAULT 0,
    fenced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fenced_by     TEXT        NULL,
    fenced_reason TEXT        NULL,

    -- Monotonicity is enforced by `bump_generation`'s `generation + 1` UPDATE
    -- under the row lock; this only rules out a hand-written negative epoch,
    -- which would sort below every pinned value and silently unfence the shard.
    CONSTRAINT harvest_shard_generation_non_negative CHECK (generation >= 0)
);

COMMENT ON TABLE harvest_shard_generation IS
    'Per-shard write-authority epoch for cross-region DR fencing (issue #954). '
    'Bumped by an operator at the promoted primary during failover; workers pin '
    'the value they read at startup and are structurally unable to claim or '
    'persist once it moves. See docs/runbooks/cross-region-failover.md.';

-- ── Measured RPO: the replication watermark trail (issue #954) ─────────────
--
-- Why this exists rather than just reading `pg_stat_replication.replay_lag`:
-- for LOGICAL replication that column is computed from the subscriber's reply
-- messages, and a subscriber whose apply worker is *stuck* stops replying —
-- so `replay_lag` stays NULL or frozen at its last value exactly in the
-- incident where the RPO matters. Measured directly (see the drill in
-- docs/runbooks/cross-region-failover.md): with the apply worker blocked,
-- byte lag grew while `replay_lag` never left NULL.
--
-- This table is the fix, and it is still "sourced from replication positions":
-- each row stamps a wall-clock instant against the WAL position current at
-- that instant, so the standby's own `confirmed_flush_lsn`/`restart_lsn` —
-- read from `pg_replication_slots` on the primary — can be translated into a
-- time. The RPO is then the age of the newest watermark the standby has
-- actually consumed, which is exactly the definition of "how much acknowledged
-- work would failing over right now lose".
--
-- The beat also keeps WAL moving on an idle primary, so an idle deployment
-- reports a live RPO instead of a spuriously growing one.
--
-- Primary key is `(shard_id, beat_lsn)` deliberately: no sequence, because
-- logical replication does not replicate sequence values and a promoted
-- standby would otherwise inherit a sequence that hands out already-used ids.

CREATE TABLE IF NOT EXISTS harvest_replication_heartbeat (
    shard_id INTEGER     NOT NULL,
    beat_lsn PG_LSN      NOT NULL,
    beat_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (shard_id, beat_lsn)
);

-- The RPO query is `MAX(beat_at) WHERE shard_id = $1 AND beat_lsn <= $2`, and
-- the prune is `beat_at < cutoff`. One index serves both.
CREATE INDEX IF NOT EXISTS harvest_replication_heartbeat_shard_at_idx
    ON harvest_replication_heartbeat (shard_id, beat_at DESC);

COMMENT ON TABLE harvest_replication_heartbeat IS
    'Wall-clock/WAL-position watermarks used to translate a standby replication '
    'position into a measured RPO in seconds (issue #954). Pruned to a bounded '
    'trailing window by the sampler that writes it.';
