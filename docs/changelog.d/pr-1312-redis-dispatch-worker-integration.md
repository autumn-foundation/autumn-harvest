## Phase 6.4 — Redis dispatch worker integration (issue #1312)

**Opt-in. Additive. No new `WorkflowEvent` variant and no migration.** The
`autumn-harvest-redis` crate has existed and been tested since Phase 4, but it
was never wired into the worker: its own module doc named the required
transactional-boundary refactor as unbuilt, so an operator could not turn it on
at any throughput. `docs/rnd/2026-09-03-redis-queue-worker-integration-deferral.md`
recorded that state and asked for a dated, owned trigger. Issue #1312 is that
trigger, and this slice closes it.

**The decision.** Postgres keeps every `harvest_task_queue` row and stays the
only source of truth. Redis Streams becomes a **dispatch channel**, never a
queue: after a row becomes claimable the engine publishes a small reference
(task id, queue, due time) to a per-queue stream, a worker reads the reference,
claims the named row in Postgres with the **full existing claim predicate**
(`queue::claim_task_query()` plus one predicate on `id`), and then acks the
reference. All thirteen claim gates, the park/wake cycle, the
`(worker_id, crash_strikes)` claim token and the orphan reclaim path are
untouched, because the row they act on is untouched. This is the shape Temporal
uses: the matching service holds tasks in memory and the persistence layer is
the durable record. The alternative shapes — a full envelope in the stream, a
wake-signal-only Redis, a new outbox table, acking after the completion commit —
are recorded with their rejection reasons in
`docs/plans/2026-09-07-redis-dispatch-worker-integration.md` §3.

**Design decisions worth naming.**

- **Ack right after the Postgres claim commit**, not after completion. The
  claim commit is the only Postgres write the reference exists to trigger.
  Acking later would couple pending-entry visibility to activity duration and
  add no durability, because Postgres already owns the lease.
- **The reconcile sweep is the durability floor.** Every
  `reconcile_interval` a worker republishes due `PENDING` rows in
  `(priority DESC, scheduled_at ASC)` order, bounded by a batch size. A lost
  reference, a dropped hint, a crash between commit and publish, and a full
  Redis restart all converge through it, so Redis persistence is not required
  for correctness.
- **Fallback, not failure.** When the channel errors the worker uses the
  existing Postgres claim path for that iteration. Availability with Redis down
  equals availability with Redis absent.
- **Gated rows release with capped exponential backoff**; absent rows get three
  short releases before an ack, which covers a publish that raced its own
  transaction; `RUNNING`, terminal and still-absent rows are acked.
- **A publish is idempotent per task id**, through a marker key with a
  ten-minute TTL. An earlier due time moves a parked delayed reference forward.

**Operator surface.** New `[harvest.redis]` config section with `url`,
`key_prefix` (default `harvest`), `consumer_group` (default `harvest_workers`),
`visibility_timeout_ms` (60000), `poll_interval_ms` (20) and
`reconcile_interval_ms` (1000), each with an environment override
(`AUTUMN_HARVEST_REDIS__URL` and one per remaining key). `url` unset means
Redis dispatch is off, which is the default and is byte-identical to the
pre-#1312 behaviour. New `redis` cargo
feature on `autumn-harvest-plugin` carrying the optional
`autumn-harvest-redis` dependency; a build without the feature **rejects** a
configured URL at validation rather than ignoring it. `HarvestRunner::start`
installs the channel before the worker is constructed and in every mode, so an
API-only process publishes too. A connect failure fails startup with an error
naming the endpoint; the endpoint is credential-free in both the error and the
one startup `INFO` line, via `HarvestRedisConfig::redacted_url`.

**Invariant notes.** No new `WorkflowEvent` variant. No migration. No new
table. No change to the `harvest_events` append-only invariant or to its two
sanctioned in-place writers. No change to the Postgres claim path when the
channel is off. The channel is a latency and throughput optimization and never
a durability store, which is stated in the `dispatch.rs` module doc and proven
by the reconcile sweep above.

**v1 limits, stated rather than discovered later.** Single-shard runtimes only;
a sharded runtime rejects Redis dispatch at validation, and the reference
carries a shard slot for the follow-up. Priority order and sticky affinity
degrade to best effort, because a stream delivers in publish order and only the
reconcile sweep publishes in priority order.

**Test evidence.** Core unit tests for hint buffering and release backoff, plus
DB tests against `dispatch::MemoryDispatch` (feature `testing`). Redis tests
for dedupe, reschedule, batch read, release and pending-entry recovery. The
end-to-end suite runs a real workflow through the channel against a live
Postgres and a live Redis (`workflow_completes_through_the_channel`), and the
process-kill test
`crash_between_claim_commit_and_ack_neither_loses_nor_duplicates` aborts a child
worker at the new `DISPATCH_AFTER_CLAIM_BEFORE_ACK` chaos point — after the
claim transaction commits and before the ack — then asserts exactly one
execution and a clean stream from the parent. Plugin unit tests cover the
config section: defaults, TOML parse, the six environment overrides, interval
and timeout validation, the missing-feature rejection, and credential-free URL
redaction. CI gains per-crate clippy for `autumn-harvest-redis`, a plugin
clippy step for the `redis` feature, `cargo test -p autumn-harvest-redis --lib`
on the no-database leg, and three manifest rows that run the Redis suites on
the Docker-backed Linux job.

**Throughput.** Assay #1 measured the standalone adapter at 12,004 claims/sec
draining a 1,000-entry backlog with 8 claim-only workers, against 640/sec and
29/sec for the Postgres claim path at 1,000-row and 10,000-row backlogs. That
comparison is not matched and does not measure the wired path, which also pays
a Postgres claim per reference. The deployment-shaped assay #6 reports the
integrated number, against a pre-registered kill line and a matched Postgres
control in the same run.

**Documentation.** New operator guide
[`docs/operations/redis-dispatch.md`](../operations/redis-dispatch.md) (what it
is, when to use it, configuration, key layout, consumer group, the crash
matrix, failure modes, limits, how to turn it off).
`docs/autumn-workflow-architecture.md` §9.1 now describes the wired channel and
its limits instead of the "not yet wired" note, §14 documents the config
section and the six environment variables, and the executive summary, the
Phase 4 list and the comparison table no longer claim Postgres is the only
possible infrastructure dependency. `docs/architecture.md` gains a
`dispatch.rs` module-guide row and a corrected workspace tree.
`docs/rnd/2026-09-03-redis-queue-worker-integration-deferral.md` is marked
superseded.
