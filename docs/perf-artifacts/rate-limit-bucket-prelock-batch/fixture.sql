-- Reproduction fixture for the rate-limit-bucket pre-lock N+1 evidence
-- (throttle::pre_lock_rate_limit_buckets_for_claimed_batch, issue #1230
-- Finding 2). Run against a scratch database, not a real deployment.
--
--   createdb ledger_evidence
--   psql ledger_evidence -f docs/perf-artifacts/rate-limit-bucket-prelock-batch/fixture.sql
--
-- Schema mirrors harvest_rate_limit_buckets exactly (migrations
-- 20260522000001, 20260724000000, 20260902133132).
CREATE TABLE harvest_rate_limit_buckets (
    key VARCHAR PRIMARY KEY,
    refill_rate DOUBLE PRECISION NOT NULL,
    burst DOUBLE PRECISION NOT NULL,
    tokens DOUBLE PRECISION NOT NULL,
    last_refilled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    override_refill_rate DOUBLE PRECISION NULL,
    override_burst DOUBLE PRECISION NULL,
    override_expires_at TIMESTAMPTZ NULL,
    last_registered_at TIMESTAMPTZ NULL,
    baseline_set_at TIMESTAMPTZ NULL
);

-- 5,000 start-throttle tenant buckets: the "small deployment" data size.
INSERT INTO harvest_rate_limit_buckets
    (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at,
     override_refill_rate, override_burst, override_expires_at,
     last_registered_at, baseline_set_at)
SELECT
    'start-throttle:throttled_flow:tenant-' || i,
    10, 100, 100 - (random() * 100)::int,
    now() - (random() * 3600)::int * interval '1 second',
    now() - interval '30 days',
    now() - (random() * 3600)::int * interval '1 second',
    NULL, NULL, NULL,
    -- ~90% recently re-registered, ~10% NULL ("never registered since the
    -- upgrade" -- realistic density per the GC migration's own comment).
    CASE WHEN random() < 0.9
        THEN now() - (random() * 86400)::int * interval '1 second'
        ELSE NULL END,
    -- ~2% carry an operator-set permanent baseline (exempt from GC).
    CASE WHEN random() < 0.02 THEN now() - interval '2 days' ELSE NULL END
FROM generate_series(0, 4999) i;

-- Run the "5k rows" before/after captures here, then extend to 50,000 total
-- (a busier deployment's activity-rate-limit buckets alongside the
-- start-throttle ones) for the "50k rows" captures.
INSERT INTO harvest_rate_limit_buckets
    (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at)
SELECT
    'dyn-rate:some_activity:tenant-' || i, 10, 100, 100 - (random() * 100)::int,
    now(), now() - interval '30 days', now()
FROM generate_series(5000, 49999) i;

ANALYZE harvest_rate_limit_buckets;
