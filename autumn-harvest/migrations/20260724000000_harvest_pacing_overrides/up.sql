-- Pacing overrides are configuration, not workflow history. The management API
-- replicates each row to every shard; enforcement and token state stay local.
CREATE TABLE harvest_pacing_overrides (
    policy_kind TEXT NOT NULL CHECK (policy_kind IN ('activity', 'workflow')),
    declared_name TEXT NOT NULL,
    refill_per_sec DOUBLE PRECISION NULL CHECK (refill_per_sec > 0 AND refill_per_sec < 'Infinity'::float8),
    burst DOUBLE PRECISION NULL CHECK (burst > 0 AND burst < 'Infinity'::float8),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_kind, declared_name)
);

CREATE INDEX harvest_pacing_overrides_expiry_idx
    ON harvest_pacing_overrides (expires_at);
