-- Durable mutual-exclusion locks for workflow code (issue #691).
--
-- Backs `ctx.mutex(key).acquire()`, a WorkflowContext author primitive that
-- durably suspends the calling workflow until it holds exclusive access to
-- `key`, then releases on guard drop / scope exit / holder terminal transition
-- / lease-TTL backstop. At most one holder per key per shard; grants are FIFO
-- by request time.
--
-- Scoping: mutex coordination is shard-local (lock + waiter rows on the
-- contending workflows' own shard), consistent with per-key concurrency
-- (issue #247). Contending workflows must resolve to the same shard;
-- cross-shard global locks are out of scope.
--
-- Crash safety: `lease_expires_at` is a TTL backstop and `lock_seq` is a
-- monotonic fencing token. A lease scanner reclaims an expired lease and wakes
-- the next waiter; release/reclaim are fenced by `(holder_exec_id, lock_seq)`
-- so a reclaimed-then-resumed stale holder's release is a 0-row no-op and the
-- lock table never corrupts.

-- One row per currently-held key. Absence of a row means the key is free.
CREATE TABLE IF NOT EXISTS harvest_mutex_locks (
    lock_key         TEXT        PRIMARY KEY,
    -- Execution that currently holds the lock.
    holder_exec_id   UUID        NOT NULL,
    -- Monotonic fencing token minted from harvest_mutex_lock_seq on each grant.
    -- release/reclaim match on (holder_exec_id, lock_seq) so a stale holder can
    -- never free a lock a newer holder has since acquired.
    lock_seq         BIGINT      NOT NULL,
    acquired_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- TTL backstop: an expired lease is reclaimable by the lease scanner. The
    -- holder's own decision cycles renew this forward.
    lease_expires_at TIMESTAMPTZ NOT NULL
);

-- Terminal auto-release: release every lock a now-terminal execution holds.
CREATE INDEX IF NOT EXISTS idx_harvest_mutex_locks_holder
    ON harvest_mutex_locks (holder_exec_id);

-- Lease scanner: find expired leases to reclaim.
CREATE INDEX IF NOT EXISTS idx_harvest_mutex_locks_lease
    ON harvest_mutex_locks (lease_expires_at);

-- FIFO waiter queue: one row per execution waiting on a key. BIGSERIAL `id`
-- encodes request order; grants go to the smallest id for the key.
CREATE TABLE IF NOT EXISTS harvest_mutex_waiters (
    id             BIGSERIAL   PRIMARY KEY,
    lock_key       TEXT        NOT NULL,
    waiter_exec_id UUID        NOT NULL,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Re-parks re-enqueue with ON CONFLICT DO NOTHING so the original id (and
    -- thus FIFO position) is preserved across suspension cycles.
    UNIQUE (lock_key, waiter_exec_id)
);

-- Head-of-line probe: the smallest id for a given key wins the next grant.
CREATE INDEX IF NOT EXISTS idx_harvest_mutex_waiters_key_id
    ON harvest_mutex_waiters (lock_key, id);

-- Fencing-token source. Every grant mints the next value.
CREATE SEQUENCE IF NOT EXISTS harvest_mutex_lock_seq;
