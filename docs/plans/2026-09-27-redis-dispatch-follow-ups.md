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
| 2 | Owners with no buffering scope | Do | A mechanical wrap. It never publishes earlier than today. |
| 3 | Multi-shard runtimes | Defer | Needs per-shard installs and a shard segment in every key. |
| 4 | Priority and sticky affinity | Defer | Needs one stream per priority band or per worker. Changes the read. |
| 5 | Redis Cluster | Defer | The blocking read spans all queues in one `XREADGROUP`. Different hash tags make it fail with `CROSSSLOT`. |
| 6 | Effective config | Do | Add a `redis` section to the admin config view. |
| 7 | Metrics | Do | Three counters, two alerts, runbook sections and panels. |
| 8 | Batched acks | Do | `TaskDispatch::ack_many` with a default body. |
| 9 | Recovery round trips | Do | Pipeline `XPENDING` and `XCLAIM` across queues. |
| 10 | Sweep fan-out | Do | Make `reconcile_batch` tunable. Add a per-queue sweep lease. |

## 3. Brainstorm

| Item | Options | Chosen |
|------|---------|--------|
| 2 | (a) wrap each owner in `buffered_settled`; (b) a post-commit hook in the connection; (c) publish from the caller after return | (a). Diesel-async has no commit hook. (c) spreads one rule over many callers. |
| 6 | (a) a `redis` section in core, filled by the plugin; (b) a plugin wrapper view; (c) a generic `dispatch` section | (a). The key names match `[harvest.redis]`, so an operator finds them. The endpoint is the redacted URL. |
| 7 | (a) counters at the event sites; (b) a sampler over process-global atomics; (c) gauges | (a) for fallbacks and recoveries, which the worker sees. (b) for dropped hints, because the drop site has no telemetry handle. |
| 8 | (a) ack each lease after its claim (today); (b) claim all, ack all, then start the tasks; (c) start each task after its claim, ack all at the end | (b). The ack still comes before the task starts, so the crash matrix does not change. The cost is that the first task waits for the other claims in the read. |
| 9 | (a) one pipeline for all `XPENDING`, one for all `XCLAIM`; (b) `XAUTOCLAIM` | (a). `XAUTOCLAIM` needs Redis 6.2, and it is still one call per queue. |
| 10 | (a) a tunable batch only; (b) a lease per queue so one worker sweeps; (c) a shared cursor in Redis | (a) plus (b). (c) leaks the Postgres cursor type into the channel trait. |

## 4. Reverse brainstorm: how to make this lose work or mislead an operator

| Attack | Defence |
|--------|---------|
| A wrapped owner runs as a SAVEPOINT with no outer scope, and publishes before the outer commit | Today the same hint goes to the background publisher at once, which is earlier still. The wrap is never worse. A reference to a row that is not visible gets three short releases, and the sweep is the floor. |
| A batched ack runs after a task starts, so the task can wake its own row and a stale ack deletes the new marker | The worker acks the whole read before it starts any task. No task of this read runs before its ack. |
| A crash after the claims and before the batched ack | The references stay in the pending entries list. Recovery redelivers them, each row reads `RUNNING`, and each reference is acked. This is row 2 of the crash matrix. |
| A sweep lease holder stops sweeping but keeps its lease | The holder renews only after a sweep succeeds. A failed sweep deletes the lease. A lease that is not renewed expires after three intervals. |
| Redis refuses the lease call | The worker sweeps anyway. The floor fails open. |
| A peer serves a different queue set | The lease is per queue, so each queue has its own holder among the workers that poll it. |
| `reconcile_batch = 0` stalls the cursor | Validation requires at least 1. |
| The effective config view leaks the Redis password | The view stores `redacted_url()`, which fails closed to `<redacted>`. A test asserts that no password reaches the JSON. |
| The view says "installed" when the connect failed | A failed connect stops startup, so a served view always follows a successful install. The view records the install result, not the config. |
| Two workers in one process export the dropped-hint count twice | The sampler takes the unreported count with an atomic swap, so each drop is exported once. |

## 5. Six hats

- **White (facts).** Assay #9 clears item 1. The consume loop does one Redis
  round trip per lease. Recovery does two per queue. Every worker sweeps
  every queue each interval. The admin view has five sections and no Redis
  data. Nothing exports `dropped_hints()`.
- **Red (feelings).** `worker.rs` is the hottest file in the tree. Keep each
  change there small and local.
- **Black (risks).** The sweep lease touches the durability floor. It must fail
  open, and it must never outlive a failed sweep. Deferred acks widen the
  crash window from one claim to one read, and delay the first task by the
  other claims of the read. Both need tests against a real Redis.
- **Yellow (benefits).** A fleet of N workers sweeps each queue once per
  interval, not N times. The operator can see the channel in the admin view
  and in metrics. Acks and recovery cost one round trip each per pass.
- **Green (alternatives).** A shared sweep cursor, per-priority streams, and
  hash-tagged keys all fit the same trait later.
- **Blue (process).** One commit pair per item: a red test, then the fix.
  Refactor last. Run the Redis and Postgres suites locally before each push.

## 6. Tests first

| Item | Red test |
|------|----------|
| 2 | A channel that checks, at publish time, that the row is committed and that the publish runs on the owner's task. Drive `force_fail_activity`, `resume_workflow_execution` and the timeout scanner through it. |
| 6 | The view serializes a `redis` section. The password never reaches the JSON. The HTTP key set includes `redis`. |
| 7 | A recording recorder sees a fallback count, a recovery count and the dropped-hint count. The metrics-rs bridge maps each one. |
| 8 | `ack_many` on Redis clears every entry, marker and pending entry in one call. The default body acks each lease. |
| 9 | One `maintain` pass recovers idle entries from several queues. |
| 10 | `reconcile_batch` parses, has an env override and rejects 0. A second worker skips the sweep while a peer holds the lease. A failed sweep gives the lease up. |

## 7. Deferred items

Items 3, 4 and 5 stay open on issue #1429. Each one changes where a reference
lives, so each one needs a key-migration note and an upgrade test that the
reconcile sweep converges.
