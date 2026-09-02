-- Idle rate-limit-bucket GC (issue #1127): two additive nullable columns plus
-- one supporting index. No data migration, no `WorkflowEvent` variant, nothing
-- written to `harvest_events`.

-- 1. `last_registered_at` — when `queue::ensure_rate_limit_bucket` last
--    (re-)registered this bucket, refreshed at most once per
--    `RATE_LIMIT_BUCKET_TOUCH_INTERVAL_SECS`.
--
--    It is part of the GC's idleness clock, and it is a SEPARATE column rather
--    than a reuse of `updated_at` for two reasons. First, `updated_at` today
--    means "an operator or config write changed this bucket" (only
--    `set_rate_limit` and the pacing-override routes write it) and
--    `GET /admin/rate-limits` reports it as such; making ordinary dispatch
--    traffic move it would destroy that answer. Second, the touch is the
--    concurrency interlock that stops the sweep collecting a bucket out from
--    under an enqueue that has not committed yet, so it must be writable
--    without touching any operator-facing field.
--
--    NULL on every pre-upgrade row, which reads as "never registered since the
--    upgrade" — the first `ensure` stamps it. `GREATEST` ignores NULLs, so an
--    un-stamped row simply ages out on its other timestamps.
ALTER TABLE harvest_rate_limit_buckets
    ADD COLUMN IF NOT EXISTS last_registered_at TIMESTAMPTZ NULL;

-- 2. `baseline_set_at` — when an operator last wrote this bucket's PERMANENT
--    baseline through `POST /admin/rate-limits/{key}` (issue #332).
--
--    A bucket carrying this is exempt from collection. That route writes
--    `refill_rate`/`burst` straight onto the row and validates nothing against
--    the registry, so an operator can (and, to clamp a noisy tenant, would)
--    target a per-tenant `dyn-rate:`/`start-throttle:` key. Collecting such a
--    row would silently revert the clamp to the code-declared rate the next
--    time that tenant appeared -- destroying deliberate operator intent, the
--    same reason the sweep refuses a bucket under a live TTL'd pacing override
--    (issue #945). Exempt rows are bounded by how many keys an operator has
--    explicitly written, and `DELETE /admin/rate-limits/{key}`-style cleanup
--    remains the operator's own call.
ALTER TABLE harvest_rate_limit_buckets
    ADD COLUMN IF NOT EXISTS baseline_set_at TIMESTAMPTZ NULL;

-- 2a. Preserve operator intent that predates this column.
--
--     An operator who already clamped a per-tenant key through
--     `POST /admin/rate-limits/{key}` has no `baseline_set_at`, so without this
--     the very first sweep would treat that row as ordinary, collect it once
--     idle, and let the next registration restore the code-declared rate --
--     silently undoing a deliberate production clamp during the upgrade.
--
--     `updated_at` is the available evidence: before this migration NOTHING but
--     an operator or config write moved it (the admin baseline route and the
--     TTL'd pacing-override routes set it; debits and refunds write only
--     `tokens`/`last_refilled_at`), while a bucket the engine registered was
--     inserted with `created_at` and `updated_at` from the same statement clock.
--     So `updated_at > created_at` means "an operator touched this row", and
--     marking those is deliberately CONSERVATIVE: it also captures rows whose
--     only operator write was a pacing override that has since expired, which
--     merely retains a few extra buckets -- the safe direction.
--
--     Residual, documented in docs/upgrading/0.5.0.md §3.8: a bucket the admin
--     route CREATED (rather than modified) has `updated_at == created_at` and is
--     indistinguishable from an engine-registered one. Re-apply any such clamp
--     after upgrading, or preview a tick with `dry_run` first -- it lists exactly
--     what would be collected and deletes nothing.
UPDATE harvest_rate_limit_buckets
   SET baseline_set_at = updated_at
 WHERE updated_at > created_at;

-- 2b. One-time upgrade grace for the rolling-deploy window.
--
--     The GC's protection against collecting a bucket out from under an
--     in-flight enqueue is an interlock the WRITER takes (see
--     `queue::RATE_LIMIT_BUCKET_TOUCH_INTERVAL_SECS`). A worker running the
--     previous binary does not take it: its dynamic enqueue is a bare
--     `INSERT ... ON CONFLICT DO NOTHING`, and its throttle backlog append
--     ensured no bucket at all. The janitor, however, starts collecting as soon
--     as the FIRST upgraded instance runs -- so during a migrate-then-rolling-
--     restart deployment a new janitor would be sweeping while old workers still
--     write unprotected.
--
--     Stamping every existing row's `last_registered_at` with the migration's
--     own clock closes that window with no extra column, predicate or fleet
--     probe: `last_registered_at` is one of the terms in the sweep's
--     `GREATEST(...)` idleness test, so every pre-upgrade bucket is ineligible
--     for a FULL retention window from the moment the migration runs (at least
--     one hour, seven days by default). A rolling restart that outlasts that is
--     not a rolling restart; one that genuinely will should disable the pass
--     (`RetentionConfig::without_rate_limit_bucket_gc()`) until it completes.
--
--     Deliberately not permanent: after that one window the rows age normally
--     and abandoned per-tenant buckets -- the bulk of the growth this issue is
--     about -- are still reclaimed.
UPDATE harvest_rate_limit_buckets
   SET last_registered_at = NOW()
 WHERE last_registered_at IS NULL;

-- 3. The sweep refuses to collect a bucket that any NON-TERMINAL task still
--    references: both the claim-time gate and
--    `queue::try_consume_rate_limit_token` fail CLOSED on a missing bucket row,
--    and nothing re-registers a bucket for an already-enqueued task, so
--    collecting one would strand that task forever.
--
--    The pre-existing `idx_harvest_task_queue_rate_limit_key` is partial on
--    `state = 'PENDING'`, which cannot serve that anti-join: a RUNNING task (a
--    circuit-breaker activity debits its token at dispatch, not at claim) pins
--    its bucket just as hard, and a retry puts a RUNNING task back to PENDING.
--    Without this index the anti-join degenerates to a sequential scan of
--    `harvest_task_queue` per candidate bucket.
--
--    The predicate is written as a NOT IN over the three TERMINAL states rather
--    than an IN over the non-terminal ones so that a future state added to
--    `harvest_task_queue` is covered by both the index and the sweep's matching
--    WHERE clause automatically -- failing safe toward RETAINING a bucket.
--
--    On a live deployment prefer the concurrent form, which cannot run inside
--    Diesel's migration transaction -- a plain CREATE INDEX holds SHARE on
--    `harvest_task_queue` for the length of the build, blocking every enqueue,
--    claim and completion on the busiest table in the engine. Run it ahead of
--    the migration and the statement below becomes a no-op via IF NOT EXISTS:
--
--      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_task_queue_rate_limit_key_live
--          ON harvest_task_queue (rate_limit_key)
--          WHERE rate_limit_key IS NOT NULL
--            AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
CREATE INDEX IF NOT EXISTS idx_harvest_task_queue_rate_limit_key_live
    ON harvest_task_queue (rate_limit_key)
    WHERE rate_limit_key IS NOT NULL
      AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
