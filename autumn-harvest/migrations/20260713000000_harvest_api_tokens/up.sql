-- Scoped API tokens for the management API (issue #942).
--
-- A small, Postgres-only bearer-credential table for the Harvest management
-- API. One row per issued token. Only a cryptographic hash of the secret is
-- stored — the plaintext secret is returned exactly once at creation and never
-- persists at rest.
--
-- This is CONFIG STATE, not event history: no `WorkflowEvent` variant, no
-- change to `harvest_events`, zero replay-determinism impact. The table is a
-- control-plane concern verified on every management request against the
-- default (control) shard, so — unlike `harvest_audit_log` — its rows carry no
-- `shard_id`.
--
-- Scope is drawn from the existing route classification
-- (`autumn_harvest::audit::CLASSIFIED_ROUTES`): a `read` token reaches every
-- `RouteClass::ReadOnly`/`PublicSafe` route and is denied (403) every mutating
-- route; a `mutate` token reaches everything. Enforcement reuses the #776
-- read-only mechanism rather than forking a second route taxonomy.

CREATE TABLE IF NOT EXISTS harvest_api_tokens (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Human-readable label for the caller (CI job, dashboard, on-call, SDK).
    name         TEXT        NOT NULL,
    -- Hex-encoded SHA-256 of the full `hvst_...` secret. The plaintext is
    -- NEVER stored. Verification hashes the presented secret and looks it up
    -- here (indexed exact match); no in-Rust hash comparison is performed.
    token_hash   TEXT        NOT NULL,
    -- Verb-level scope: 'read' (ReadOnly routes only) or 'mutate' (everything).
    scope        TEXT        NOT NULL    CHECK (scope IN ('read', 'mutate')),
    created_at   TIMESTAMPTZ NOT NULL    DEFAULT NOW(),
    -- Optional expiry. An expired token is rejected 401 on the next request.
    expires_at   TIMESTAMPTZ,
    -- Best-effort last-use timestamp, updated off the request critical path.
    last_used_at TIMESTAMPTZ,
    -- Revocation timestamp. A revoked token is rejected 401 on the next
    -- request — revocation takes effect immediately because there is no grant
    -- cache (every request re-queries this table).
    revoked_at   TIMESTAMPTZ,
    -- Actor who minted this token (audit provenance).
    created_by   TEXT        NOT NULL    DEFAULT 'anonymous'
);

-- The verification lookup key. UNIQUE so a hash collision (or a re-mint of an
-- identical secret, astronomically unlikely) can never produce two rows.
CREATE UNIQUE INDEX IF NOT EXISTS harvest_api_tokens_hash_uidx
    ON harvest_api_tokens (token_hash);

-- List endpoint read pattern: recency-ordered scan of active tokens.
CREATE INDEX IF NOT EXISTS harvest_api_tokens_active_idx
    ON harvest_api_tokens (created_at DESC)
    WHERE revoked_at IS NULL;
