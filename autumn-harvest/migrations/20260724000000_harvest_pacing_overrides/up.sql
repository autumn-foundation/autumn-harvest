-- TTL'd runtime overrides for per-activity rate limits and workflow-start
-- throttles (issue #945).
--
-- Both `#[activity(rate_limit(...))]` (issues #88/#332/#699) and
-- `#[workflow(throttle(...))]` (issue #607) pace dispatch through the SAME
-- token-bucket table, `harvest_rate_limit_buckets` -- an activity's bucket key
-- is the (possibly dynamic, issue #699) activity name, a throttle's bucket key
-- is `start-throttle:{workflow}:{key}` (issue #607). Both are declared once at
-- code-deploy time and previously required a worker restart (to change the
-- author-declared value) or a *permanent* database write (the pre-existing
-- `POST /admin/rate-limits/{key}` route, issue #332) to change.
--
-- These three additive nullable columns let an operator layer a TEMPORARY,
-- self-expiring override on top of an EXISTING bucket's declared baseline
-- (`refill_rate`/`burst`, unchanged) without touching either. While
-- `override_expires_at` is set and in the future, the *effective* refill rate
-- and burst are `COALESCE(override_x, x)` per field -- so an operator may
-- override just one of the two and the other still reads from the declared
-- baseline. Once `override_expires_at` elapses, every consumer reverts to the
-- baseline automatically: expiry is enforced by a plain `> NOW()` comparison
-- evaluated fresh on every claim/debit/read, so there is NO background sweeper
-- to run and NO operator action required to revert.
--
-- Scoping is per-shard, exactly like the bucket rows themselves: a multi-shard
-- deployment's bucket for the same key exists independently on every shard
-- database, so setting an override fans out to every shard (mirroring the
-- pre-existing `set_rate_limit`/`declare_compat` fan-out pattern) and a partial
-- fan-out failure can leave the override live on some shards and not others
-- until retried.
--
-- These columns hold ONLY dispatch/admission pacing state. Nothing here is
-- ever written to `harvest_events` -- an override changes how fast work is
-- admitted or dispatched, never what a workflow's recorded history contains --
-- so replay of any execution that ran under an active override is byte-
-- identical to one that never had one. No new `WorkflowEvent` variant.
ALTER TABLE harvest_rate_limit_buckets
    ADD COLUMN IF NOT EXISTS override_refill_rate DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS override_burst DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS override_expires_at TIMESTAMPTZ NULL;
