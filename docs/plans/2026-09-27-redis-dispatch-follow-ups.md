# Redis dispatch follow-ups (issue #1429)

**Status:** implementation plan. Follows
`docs/plans/2026-09-07-redis-dispatch-worker-integration.md` (issue #1312).

## 1. Decision in one paragraph

Issue #1429 lists ten follow-ups. None of them loses or duplicates work. This
change does the seven items that fit the current design: item 1 by evidence,
items 2, 6, 7, 8, 9 and 10 in code. Items 3, 4 and 5 change the key layout or
the stream topology. Each one needs its own design and its own upgrade story,
so each one gets its own issue.

## 2. Scope

| # | Item | Decision | Reason |
|---|------|----------|--------|
| 1 | Tail latency | Close by evidence | Assay #9 re-ran the paced shape after the #1428 fix. The Redis arm p99 is 50.02 ms against the 250 ms line. |
| 2 | Owners with no buffering scope | Do | Wrap each top-level owner. It never publishes earlier than today. |
| 3 | Multi-shard runtimes | Defer | Needs per-shard installs and a shard segment in every key. |
| 4 | Priority and sticky affinity | Defer | Needs one stream per priority band or per worker. Changes the read. |
| 5 | Redis Cluster | Defer | The blocking read spans all queues in one `XREADGROUP`. Different hash tags make it fail with `CROSSSLOT`. |
| 6 | Effective config | Do | Add a `redis` section to the admin config view. |
| 7 | Metrics | Do | Three counters, two alerts, runbook sections and panels. |
| 8 | Batched acks | Do | `TaskDispatch::ack_many` with a default body. |
| 9 | Recovery round trips | Do | Send `XPENDING` and `XCLAIM` for all queues at once. |
| 10 | Sweep fan-out | Do | Make `reconcile_batch` tunable, 1 to 10000. Add a per-queue sweep lease. |

## 3. Brainstorm

| Item | Options | Chosen |
|------|---------|--------|
| 2 | (a) wrap each owner in `buffered_settled`; (b) a post-commit hook in the connection; (c) publish from the caller after return | (a). Diesel-async has no commit hook. (c) spreads one rule over many callers. The owners are 9 sites in `timeout.rs` plus the mutex reclaim of its scanner, 7 in `execution.rs`, the `poison_pill.rs` quarantine and `context.rs` `run_transactional`. The fire paths of debounce, throttle, event batch and the completion-trigger outbox own the nested starts, so they are wrapped too. The start, cancel and terminate collect helpers are not wrapped. They often run as a SAVEPOINT, so a scope there publishes inside the caller's open transaction. The standalone cancel and terminate wrappers carry the scope instead (a Codex finding). The cross-connection completion-trigger relay and cross-shard outbox deliveries keep their current behaviour. They belong to item 3. |
| 6 | (a) a `redis` section in core, filled by the plugin; (b) a plugin wrapper view; (c) a generic `dispatch` section | (a). The key names match `[harvest.redis]`, so an operator finds them. The endpoint is the redacted URL. |
| 7 | (a) counters at the event sites; (b) a sampler over process-global atomics; (c) gauges | (a). The worker counts fallbacks and recoveries. The background publisher counts each dropped hint at the drop site, through a process-global recorder. The plugin runner and `Worker::new` set it with `dispatch::set_dropped_hint_recorder`. |
| 8 | (a) ack each lease after its claim (today); (b) claim all, ack all, then start the tasks; (c) start each task after its claim, ack all at the end | (b). The ack still comes before the task starts, so the crash matrix does not change. The cost is that the first task waits for the other claims in the read. |
| 9 | (a) one pipeline for all `XPENDING`, one for all `XCLAIM`; (b) `XAUTOCLAIM`; (c) concurrent commands on the multiplexed connection | (c). It costs about one round trip, like (a), and each command keeps its own result. A pipeline fails as a whole, and retrying it would repeat claims that already ran. `XAUTOCLAIM` needs Redis 6.2. |
| 10 | (a) a tunable batch only; (b) a lease per queue so one worker sweeps; (c) a shared cursor in Redis | All three. The cursor travels as an opaque string, so the channel trait never sees the Postgres cursor type. The lease TTL is three times the larger of `reconcile_interval` and `poll_interval`. |

## 4. Reverse brainstorm: how to make this lose work or mislead an operator

| Attack | Defence |
|--------|---------|
| A wrapped owner runs as a SAVEPOINT with no outer scope, and publishes before the outer commit | Today the same hint goes to the background publisher at once, which is earlier still. The wrap is never worse. A reference to a row that is not visible gets three short releases, and the sweep is the floor. |
| A batched ack runs after a task starts, so the task can wake its own row and a stale ack deletes the new marker | The worker acks the whole read before it starts any task. No task of this read runs before its ack. |
| A crash after the claims and before the batched ack | The references stay in the pending entries list. Recovery redelivers them, each row reads `RUNNING`, and each reference is acked. This is row 2 of the crash matrix. |
| A sweep lease holder stops sweeping but keeps its lease | The holder renews the lease at the start of each sweep. A sweep that fails on a Postgres error gives the lease back. A lease that is not renewed expires after its TTL, three times the larger of `reconcile_interval` and `poll_interval`. The TTL covers the real renewal period, which a blocking read can stretch past one reconcile interval. |
| Redis fails in the middle of a sweep | The worker makes no further Redis call for that sweep. The lease expires after its TTL, and a peer takes the queue. |
| A lease holder shuts down | The worker releases its leases on stop, best effort. A lease it fails to release expires after its TTL. |
| A lease moves to a peer, and the sweep walk restarts at the head of the backlog | The holder saves each cursor in Redis after a successful sweep. A new holder resumes from it. |
| Slow page reads outlast the lease, and a peer sweeps the same queue | A sweep whose reads take longer than one renewal period renews its leases before it publishes. It publishes only the queues the renewal still holds. A failed renewal publishes nothing and sends the worker to the Postgres claim path. |
| A worker gets a lease back and resumes from its own stale cursor | The hold reply says whether the lease changed hands. A new holder adopts the saved cursor, and an empty one clears the local cursor. |
| Redis refuses the lease call | The worker sweeps anyway. The floor fails open. |
| A peer serves a different queue set | The lease is per queue, so each queue has its own holder among the workers that poll it. |
| `reconcile_batch = 0` stalls the cursor, or a huge batch reads a whole deep backlog in one query | Validation requires 1 to 10000. |
| The effective config view leaks the Redis password | The view stores `redacted_url()`, which fails closed to `<redacted>`. A test asserts that no password reaches the JSON. |
| The view says "installed" when the connect failed | A failed connect stops startup, so a served view always follows a successful install. The view records the install result, not the config. |
| Two workers in one process export the dropped-hint count twice | One process-global recorder counts each drop at the drop site. The last setter wins, so a drop counts once. |
| A stalled Redis blocks a wrapped owner | The owner awaits its publish after commit. A stalled Redis delays that caller by up to the 5 s response timeout. `signal.rs` and `sessions.rs` make the same trade-off. |
| A wrapper `TaskDispatch` drops the new methods | The default bodies acknowledge each lease alone and let every worker sweep. A wrapper must forward `ack_many`, `hold_reconcile_leases` and `release_reconcile_leases`. |

## 5. Six hats

- **White (facts).** Assay #9 clears item 1. The other facts describe the
  tree before this change. The consume loop does one Redis round trip per
  lease. Recovery does two per queue. Every worker sweeps every queue each
  interval. The admin view has five sections and no Redis data. No metric
  exports the `dropped_hints()` count.
- **Red (feelings).** `worker.rs` is the hottest file in the tree. Keep each
  change there small and local.
- **Black (risks).** The sweep lease touches the durability floor. It must fail
  open, and a failed sweep must give it back or let it expire. Deferred acks
  widen the crash window from one claim to one read, and delay the first task
  by the other claims of the read. Both need tests against a real Redis.
- **Yellow (benefits).** A fleet of N workers sweeps each queue once per
  interval, not N times. The operator can see the channel in the admin view
  and in metrics. Acks and recovery cost one round trip each per pass.
- **Green (alternatives).** Per-priority streams and hash-tagged keys fit
  the same trait later.
- **Blue (process).** One commit pair per item: a red test, then the fix.
  Refactor last. Run the Redis and Postgres suites locally before each push.

## 6. Tests first

| Item | Red test |
|------|----------|
| 2 | A channel that checks, at publish time, that the row is committed and that the publish runs on the owner's task. Drive `resume_workflow_execution` and the mutex reclaim of the timeout scanner through it. The tests do not drive `force_fail_activity`. |
| 6 | The view serializes a `redis` section. The password never reaches the JSON. The HTTP key set includes `redis`. |
| 7 | A recording recorder sees a fallback count, a recovery count and the dropped-hint count. The metrics-rs bridge maps each one. |
| 8 | `ack_many` on Redis clears every entry, marker and pending entry in one call. The default body acks each lease. |
| 9 | One `maintain` pass recovers idle entries from several queues. |
| 10 | `reconcile_batch` parses, has an env override and rejects 0. A second worker skips the sweep while a peer holds the lease. A failed sweep gives the lease up. |

## 7. Deferred items

Items 3, 4 and 5 stay open on issue #1429. Each one changes where a reference
lives, so each one needs a key-migration note and an upgrade test that the
reconcile sweep converges.

## 8. Review outcomes

A multi-angle review of the first cut changed these points:

- **Item 2.** The fire paths of debounce, throttle, event batch and the
  completion-trigger outbox are now wrapped. The start-collect transaction
  stays unwrapped, because it often runs as a SAVEPOINT.
- **Item 2 test.** The commit check reads the transaction state of the owner
  connection.
- **Item 6.** The admin view endpoint drops the userinfo and replaces any
  query or fragment. A unix-socket URL can carry `?pass=`.
- **Item 7.** A process-global recorder counts each dropped hint at the drop
  site. The sampler and its atomic swap are gone.
- **Item 8.** The end-to-end wrappers forward `ack_many` and the two lease
  methods.
- **Item 9.** Recovery sends each queue's command at once on the multiplexed
  connection, not in one pipeline. Each command has its own result. A failed
  queue never forces a retry of claims that already ran, so one bad queue does
  not block the rest.
- **Item 10.** The range of `reconcile_batch` is now 1 to 10000. The lease TTL
  is three times the larger of the two intervals. The holder renews at sweep
  start and releases its leases on stop. `hold_reconcile_leases` returns the
  held leases, each with its saved cursor. A later Codex review added the saved
  cursor and the renewal after slow page reads. A failed renewal publishes
  nothing, so a lease a peer took is never swept twice.
- **API contract.** The OpenAPI document is regenerated.
