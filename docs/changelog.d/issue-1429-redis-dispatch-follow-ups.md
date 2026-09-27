## Phase 6.5 — Redis dispatch follow-ups (issue #1429)

**Opt-in. Additive. No new `WorkflowEvent` variant and no migration.** This
slice closes seven of the ten follow-ups that issue #1312 left. The plan, with
its brainstorm, reverse brainstorm and six hats, is
`docs/plans/2026-09-27-redis-dispatch-follow-ups.md`.

**What shipped.**

- **Tail latency (item 1), closed by evidence.** Assay #9 re-ran the paced
  shape after the sampler fix of #1428. The channel p99 is 50.02 ms against
  the 250 ms line. No code change.
- **Buffering scopes (item 2).** More transaction owners now publish their
  hints after they commit. The owners are 9 sites in `timeout.rs` plus the
  mutex reclaim of the timeout scanner, and 7 top-level sites in
  `execution.rs`. The `poison_pill.rs` quarantine and `context.rs`
  `run_transactional` are owners too. The fire paths of debounce, throttle,
  event batch and the completion-trigger outbox own their nested starts, so
  they are wrapped as well. Each site chains the new
  `dispatch::BufferedSettledExt::buffered_settled()` before its `.await`, so
  no transaction body moves. A wrapped owner under an outer scope leaves its
  hints with that scope, as before. The start, cancel and terminate collect
  helpers are not wrapped. They often run as a SAVEPOINT, and a scope there
  would publish inside the caller's open transaction. The standalone cancel
  and terminate wrappers carry the scope instead. A wrapped owner awaits its
  publish after commit, so a stalled Redis delays that caller by up to the
  5 s response timeout.
- **Effective config (item 6).** `GET /admin/config` has a `redis` section:
  `installed`, the credential-free `endpoint`, the effective `key_prefix`, and
  every `[harvest.redis]` tuning key. `installed` is the install result, not
  the config. The endpoint drops any userinfo and replaces any query or
  fragment, because a unix-socket URL can carry `?pass=`.
- **Metrics (item 7).** Three counters: `harvest.dispatch.hints_dropped`,
  `harvest.dispatch.fallbacks{reason}` and `harvest.dispatch.recovered`. Two
  ticket alerts, runbook sections, a dashboard row, and metrics-rs bridges.
  The background publisher counts each dropped hint at the drop site, through
  `dispatch::set_dropped_hint_recorder`. The built-in scrape endpoint does not
  export the three counters yet.
- **Batched acks (item 8).** `TaskDispatch::ack_many`, with a default body
  that acks each lease. Redis overrides it with one atomic pipeline. The
  worker claims every lease of a read, acks them in one call, and only then
  starts the claimed tasks. No task starts before its ack, so the crash matrix
  does not change. A wrapper `TaskDispatch` must forward `ack_many` and the
  two lease methods. Otherwise it falls back to one ack per lease and a sweep
  on every worker.
- **Recovery round trips (item 9).** One step reads every pending entries
  list, and a second claims every idle entry, for all queues at once. The
  commands go out together on the multiplexed connection, and each keeps its
  own result. One bad queue does not block the rest, and no claim runs twice.
- **Sweep fan-out (item 10).** `[harvest.redis] reconcile_batch` (default
  1000, range 1 to 10000, env `AUTUMN_HARVEST_REDIS__RECONCILE_BATCH`). A
  per-queue sweep lease in Redis (`<prefix>:dispatch:<queue>:reconcile`) lets
  one worker sweep each queue. The holder renews the lease at the start of
  each sweep. Its TTL is three times the larger of the reconcile and poll
  intervals. The lease fails open. A failed lease call sweeps every queue. A
  sweep that fails on Postgres gives its leases back. After a Redis failure,
  the lease expires. A stopping worker releases its leases, best effort. Slow
  page reads renew the lease before the publish, and a queue a peer took is
  dropped from that publish. A failed renewal publishes nothing, and the
  worker claims through Postgres for a cooldown. The holder saves each
  queue's cursor in Redis after a successful sweep, and a new holder resumes
  from it (`TaskDispatch::save_reconcile_cursors`).

**Deferred.** Items 3 (multi-shard), 4 (priority and sticky streams) and 5
(Redis Cluster) change where a reference lives. Each needs its own design and
upgrade note, so each stays open on #1429.

**Tests.** Red first, then green:

- `dispatch_tests.rs`: `a_resume_publishes_its_wake_after_commit` and
  `the_mutex_reclaim_publishes_its_wake_after_commit` failed before the wrap.
  A commit-checking channel saw the wake leave through the background
  publisher. `the_worker_acks_each_read_in_one_batch` failed with nine
  single acks. All three pass now. The fallback, recovery and sweep-lease
  cases pass too.
- `dispatch_redis.rs`: `ack_many`, cross-queue recovery, a malformed entry
  in one queue, and the lease hold, renew, release and expiry cases, against a
  real Redis.
- Config, runner, effective-config, HTTP, contract, dashboard and alert-pack
  guard tests cover the new keys, section and series.
