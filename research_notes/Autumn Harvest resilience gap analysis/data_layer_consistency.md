# Data-layer consistency, coordination and resilience: best practices and Autumn Harvest gap analysis

Scope: leases, fencing, leader election, Postgres work-queue claiming, outbox, isolation, clock/timer correctness, partitioning/rebalancing, replication and DR. It does not cover the execution model, retries/overload, testing or observability.

Codebase citations point at the local checkout `/home/user/autumn-harvest` (branch `claude/happy-ride-f5e7hg`, HEAD `937b655`, 2026-09-30). They use the form `path:line`, relative to the repo root. External sources carry their publication date where the page gave one. Evidence labels used below: **absent**, **present but untested**, **present but not wired**, **present and tested**.

---

## Q1 (external). Leases, fencing tokens, leader election and clock-skew hazards

### Takeaway
A lease or lock with a timeout does not give mutual exclusion by itself. A process pause, a network delay or a clock jump can let two holders act at once. The accepted remedy has two parts. The coordinator issues a monotonic fencing token (an epoch or generation) on every acquisition. The storage layer then rejects writes that carry a stale token. Expiry decisions should use one clock, ideally the storage server's own clock, not the clocks of the clients.

### Cited Findings
- A lease holder can pause (for example in GC) past its lease expiry, fail to notice, and "go ahead" with unsafe writes. HBase hit this bug in production. — [Kleppmann, "How to do distributed locking" (2016-02-08; older but canonical)](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- Clock jumps make lease expiry unpredictable, because `gettimeofday` "is subject to discontinuous jumps in system time". Network packets can also be delayed for a long time. The article cites a 90-second packet delay at GitHub. — [Kleppmann 2016](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- How fencing works: every lock grant returns a monotonically increasing token, and every write carries it. The storage "remembers that it has already processed a write with a higher token number" and rejects older writes. Redlock has no monotonic token, so it is "not sufficiently safe for situations in which correctness depends on the lock". — [Kleppmann 2016](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- Kafka fences zombie producers with an epoch. `initTransactions()` "increments an epoch associated with the transactional.id", and "any producers with same transactional.id and an older epoch are considered zombies and are fenced off". — [Confluent, "Transactions in Apache Kafka" (2017-11-17)](https://www.confluent.io/blog/transactions-apache-kafka/)
- Temporal gives each history shard a `RangeID`, "a monotonically increasing generation number used for fencing". Shard ownership is coordinated by a `ShardController` over a Ringpop membership ring. — [temporalio/temporal docs/architecture/history-service.md](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- CockroachDB has replaced epoch-based leases (tied to node-liveness records) with **leader leases**, where "the Raft leader is always the range's leaseholder, except briefly during lease transfers". Failover "should complete within a few seconds". — [CockroachDB replication layer docs](https://docs.cockroachlabs.com/docs/stable/architecture/replication-layer)
- CockroachDB bounds clock skew explicitly. `--max-offset` defaults to 500 ms. "When a node detects that its clock is out of sync with at least half of the other nodes in the cluster by 80% of the maximum offset allowed, it spontaneously shuts down." — [CockroachDB operational FAQs](https://docs.cockroachlabs.com/docs/stable/operational-faqs)
- River (a Go Postgres queue) had to guard its job rescuer against stale snapshots. A rescue fetched before a job completed and was re-claimed could otherwise overwrite the job's new owner. — [riverqueue/river PR #1373](https://github.com/riverqueue/river/pull/1373); [River maintenance services docs](https://riverqueue.com/docs/maintenance-services)

### Inferences
- Any worker-side "I still hold this task" check must compare a **per-claim epoch**. That can be an attempt counter, a claim token or a generation. It must be checked in the same SQL statement or transaction as the write. Comparing only `state = 'RUNNING'`, or only a worker id, is not a fence, because the same worker can win the row again.
- Lease expiry computed on a database with `now()` removes client clock skew from the decision. Where a client-stamped timestamp is later compared against the database's `NOW()`, skew comes back.

### Gaps
- The fetched Temporal architecture page names `RangeID` fencing but does not describe the conditional-write mechanics (for example `UPDATE ... WHERE range_id = ?`), task-ID range allocation, or how a stale activity task token is rejected. I did not obtain a primary source for those details.

---

## Q2 (external). Postgres work queues: `SKIP LOCKED`, MVCC and vacuum, LISTEN/NOTIFY, XID wraparound, timeouts, isolation, outbox

### Takeaway
`FOR UPDATE SKIP LOCKED` is the standard Postgres claim primitive. Its weak points are operational. Long transactions hold back vacuum, and dead tuples then slow the claim query. High-churn tables need vacuum attention. `NOTIFY` serializes commits under a global lock and fails commits when its queue fills. Unbounded statements or transactions pin `xmin`. Isolation below SERIALIZABLE allows write skew unless code takes explicit locks. The transactional outbox is the standard way to make a database write and an external side effect atomic, and it gives at-least-once delivery.

### Cited Findings
- **MVCC and queue degradation.** Deleted or updated queue rows stay as dead tuples while any older snapshot is open. The locking query must walk them, so lock time went from <0.01 s to 0.1+ s (about 15x) and 60,000 jobs backed up within an hour behind one long transaction. The backlog "evaporates" as soon as that transaction ends. Mitigations the article lists: tighter predicates, locking several jobs per query, and supervisors that kill over-age transactions. — [brandur.org, "Postgres Job Queues & Failure By MVCC" (2015-05-18; older, mechanism unchanged)](https://brandur.org/postgres-queues)
- **XID wraparound.** Postgres warns at about 40 M XIDs remaining. At about 3 M remaining it stops assigning XIDs ("database is not accepting commands that assign new transaction IDs"). `autovacuum_freeze_max_age` defaults to 200 M. Three things hold back xmin and prevent cleanup: long-running transactions, old prepared transactions, and replication slots with an old `xmin`/`catalog_xmin`. — [PostgreSQL docs, Routine Vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html)
- **Timeouts are off by default.** `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` and `transaction_timeout` all default to 0 (disabled). The docs recommend setting them per session or role, not in `postgresql.conf`. `idle_in_transaction_session_timeout` "can be used to ensure that idle sessions do not hold locks for an unreasonable amount of time". — [PostgreSQL docs, Client Connection Defaults](https://www.postgresql.org/docs/current/runtime-config-client.html)
- **NOTIFY queue limits.** The queue is 8 GB by default. "If this queue becomes full, transactions calling NOTIFY will fail at commit." A session that executes `LISTEN` and then stays in a long transaction blocks queue cleanup. — [PostgreSQL docs, NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
- **NOTIFY global commit lock.** A committing transaction that issued `NOTIFY` takes a database-wide lock, which "effectively ensures that only a single COMMIT query can be handled at a time". Recall.ai had three outages with this signature: CPU and I/O fell while waiting sessions spiked. They moved the wake-up logic to the application layer. — [Recall.ai, "Postgres LISTEN/NOTIFY does not scale" (2025)](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale). The page says a later Postgres commit resolved the bottleneck. A secondary search summary attributes that fix to PostgreSQL 19 (GA expected around September 2026). I did not verify this against Postgres release notes. DBOS published a counterpoint, "Postgres LISTEN/NOTIFY Actually Scales" ([dbos.dev](https://www.dbos.dev/blog/postgres-listen-notify-scalability)), which I did not read in full.
- **Isolation.** Jepsen (2020-06-12) found G2-item anomalies under PostgreSQL SERIALIZABLE, caused by a bug in SSI conflict detection that dated from 2011. The fix was scheduled for the August 2020 minor release. Jepsen also notes that Postgres "repeatable read" is snapshot isolation, which allows G2-item (write skew). — [Jepsen, PostgreSQL 12.3](https://jepsen.io/analyses/postgresql-12.3)
- **Transactional outbox.** Write the message to an outbox table in the same transaction as the state change, and have a relay publish it later. "Messages are guaranteed to be sent if and only if the database transaction commits." The relay may publish a message more than once, so consumers must be idempotent. — [microservices.io, Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
- **Fast orphan rescue.** River Pro uses queue heartbeats to rescue jobs orphaned by crashed clients within about heartbeat interval + margin (30 s + 60 s defaults), instead of job timeout + `RescueStuckJobsAfter` (up to 1 h). — [River blog / docs search summary](https://riverqueue.com/blog/active-job-rescue)

### Inferences
- A Postgres-queue engine should do four things. Pin session timeouts on its own connections: `statement_timeout`, `idle_in_transaction_session_timeout`, and `transaction_timeout` where available. Keep claim transactions short. Treat NOTIFY as an optional hint that runs outside critical write transactions. Monitor `age(datfrozenxid)`, replication-slot xmin and dead-tuple counts on the queue table.
- With READ COMMITTED as the working isolation level, correctness has to come from explicit row locks, advisory locks and unique constraints, not from the isolation level. Every read-then-write invariant then needs its own lock argument.

### Gaps
- I found no Jepsen analysis of a Postgres-backed job queue specifically (River, Oban, graphile-worker, PgQueuer, Solid Queue). I did not fetch the Crunchy Data or 37signals Solid Queue posts. Their specific recommendations are therefore not cited here.

---

## Q3 (external). Ownership transfer and rebalancing (Temporal, Kafka, CockroachDB), and multi-region DR patterns

### Takeaway
Mature systems attach an epoch to every ownership change and reject writes that carry an older epoch: Temporal `RangeID`, Kafka producer epoch, CockroachDB lease epochs and leader leases. Multi-region DR on async replication always has a non-zero RPO. The main split-brain risk is a still-writing old primary, so failover must fence the old side before promoting the new one.

### Cited Findings
- Temporal shard acquisition bumps the `RangeID` in the database, and the `RangeID` is the fencing generation. — [temporalio/temporal history-service.md](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md); [pkg.go.dev shard package (search summary)](https://pkg.go.dev/go.temporal.io/server/service/history/shard)
- Kafka zombie fencing by epoch bump on `initTransactions()`. — [Confluent 2017](https://www.confluent.io/blog/transactions-apache-kafka/)
- CockroachDB lease transfer goes: store-liveness detection, then Raft election, then lease acquisition, "within a few seconds". Leader leases removed the liveness range as a single point of failure. — [CockroachDB replication layer](https://docs.cockroachlabs.com/docs/stable/architecture/replication-layer)
- Temporal delivers activity cancellation only through heartbeats: "Activities that don't Heartbeat can't receive a Cancellation." On heartbeat timeout, "the Activity Task fails and a retry occurs if a Retry Policy dictates it." — [Temporal docs, Detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
- A search snippet attributes a Temporal NotFound message, "invalid activityID or activity already timed out or invoking workflow is completed", to late activity interactions. This is snippet-level evidence only. — [Temporal Go SDK failure detection (search result)](https://docs.temporal.io/develop/go/failure-detection)

### Inferences
- A Harvest shard is a whole Postgres database, and any worker may claim from any shard. In-memory shard ownership does not exist, so Temporal-style shard-ownership fencing is not needed for normal operation. Postgres row locks and the `(workflow_exec_id, event_id)` unique constraint serialize history writers. Epoch fencing still matters in two places: per-claim task ownership (Q4) and cross-region failover (Q5).

### Gaps
- I did not verify from a primary source whether the Temporal server rejects a completion from an attempt that already timed out (task-token attempt matching). Treat the comparison in Q4 as Harvest-internal reasoning backed by River's analogous fix, not as a verified Temporal behaviour.
- I did not fetch Kafka consumer-group generation-ID fencing of offset commits or KIP-848 details.

---

## Q4 (codebase). Claiming, ownership, fencing, timers, history/queue atomicity, races, lock ordering, pools and timeouts, and the redis/sqlite backends

### Takeaway
Harvest's Postgres core is carefully engineered. Claims are one-statement `SKIP LOCKED` CTEs that bump `attempt`. History appends are serialized by an execution-row lock and a unique `(exec, event_id)` constraint. Terminal **workflow-task** writes, schedule fire-claims and the durable mutex are all fenced. Timers and retry deadlines use the database clock. Five holes remain:
1. **Activity** completion, heartbeat and failure writes are fenced only by `state = 'RUNNING'`, with no worker or attempt check.
2. The #1184 workflow-task guard omits `attempt`.
3. Per-task heartbeats are stamped with the host clock but judged against the DB clock.
4. Engine connections set no `statement_timeout` or idle-in-transaction timeout, and single-shard pool acquisition is unbounded.
5. Every history append and enqueue issues `pg_notify` inside the write transaction.

### Cited Findings

**Claiming (present and tested)**
- The claim is a single CTE statement. `candidate` selects with `ORDER BY` sticky/priority-aging/`scheduled_at`, `LIMIT 1 FOR UPDATE SKIP LOCKED`. `claimed` sets `state='RUNNING', worker_id=$1, started_at=NOW(), attempt = attempt + 1`. Capped concurrency keys are gated by `pg_try_advisory_xact_lock(hashtext(key))` plus a `COUNT(*)` recheck. — `autumn-harvest/src/queue.rs:1099`, `:1110-1135`
- Claim transactions pin READ COMMITTED explicitly, so an operator's `default_transaction_isolation = REPEATABLE READ` cannot turn contention into 40001 aborts. The same convention appears in the scheduler, timeout and pause paths. — `queue.rs:1414-1434`, `:1767`, `:7200`; `scheduler.rs:741-748`; `timeout.rs:1241-1249`; `queue_pause.rs:1310-1312`. One path deliberately uses REPEATABLE READ. — `build_routing.rs:811-817`. SERIALIZABLE is used nowhere (grep).
- Hot-table indexes are partial on `state = 'PENDING'` or `'RUNNING'` (for example `idx_harvest_tq_poll`, `idx_harvest_tq_running`). Terminal rows therefore do not bloat the claim index. — `autumn-harvest/migrations/20260409000000_harvest_initial/up.sql:87-96`
- Claim cost "grows superlinearly with backlog depth" per the project's own measurements, and sharding is the documented remedy. — `docs/sharding.md:9-14`
- A batched seek-and-refine claim exists (`claim_task_batched`, issue #1340) but is called only from tests. It is **present but not wired**. It also has **no DR-fence splice**: its body at `queue.rs:7186-7215` has no `fence_binding`, unlike `claim_task_on_shard` (`queue.rs:1444`) and the by-id claim (`queue.rs:1774`). Callers were found only in `autumn-harvest/tests/integration/claim_batched_tests.rs:158,2076,2210`.

**Atomicity of history append and queue mutation (present and tested)**
- Activity completion runs in one transaction. It locks the execution row and loads history (`lock_workflow_execution_and_load_history`), checks the activity is still pending, takes the task row `FOR UPDATE`, appends `ActivityCompleted`, then calls `queue::complete_task`. — `autumn-harvest/src/worker.rs:13043-13083`
- Workflow completion does the same in one transaction: `claim_still_held_for_update`, then append `WorkflowCompleted`, then execution-row update, then `complete_task`, then the parent-close cascade and completion-trigger evaluation. — `worker.rs:8028-8060`. The child-workflow variant is at `worker.rs:13525-13545`.
- Split-brain detection is the unique `(workflow_exec_id, event_id)` constraint. Under partitioning it becomes `UNIQUE(..., cohort)` plus a cross-partition `EXISTS` trigger. — `migrations/20260901115500_harvest_event_partitioning/up.sql:161-207`
- **Outbox pattern (present and tested).** Completion-trigger and external outboxes are claimed `FOR UPDATE SKIP LOCKED`, and the claim is held across the relay decision. — `completion_trigger.rs:902`, `:919-927`, `:978`; `completion_callback.rs:2395`, `:2418`. Cross-shard signal, cancel and await delivery is documented as at-least-once, with dedupe on the target. — `docs/cross-region-dr.md:367-376`

**Fencing of stale owners: mixed**
- Workflow-task terminal writes are fenced (issue #1184, "Terminal workflow writes ... have no worker-ownership check", now closed). `claim_still_held_for_update` requires `state='RUNNING' AND worker_id=$2 AND crash_strikes=$3 FOR UPDATE SKIP LOCKED`. **Present and tested** (`capability_miss_tests.rs:2883,4993,5172`). — `queue.rs:4181-4188`, `:4266-4287`; [issue #1184](https://github.com/autumn-foundation/autumn-harvest/issues/1184)
- **The #1184 guard omits `attempt`.** The workflow-task timeout reset explains why `crash_strikes` alone is not enough. `poison_pill::requeue_stuck_task` "deliberately leaves `crash_strikes` untouched", so a same-worker re-claim still matches, and "this reset must also check `attempt`". — `worker.rs:31260-31277`; `poison_pill.rs:352-364`. `claim_still_held_for_update_query` (`queue.rs:4181-4188`) does **not** check `attempt`. So after `requeue_stuck_task` and a re-claim by the same worker, a stale dispatcher from the earlier claim still passes the guard. This is an inference from reading the code and has not been reproduced. Its impact is bounded by the execution-row lock and the event-id uniqueness.
- **Activity completion is not ownership-fenced (absent).** `finalize_activity_completion` checks only that the task row's `state == "RUNNING"`, via `task_state_for_update` (`worker.rs:13068-13073`, `queue.rs:2459-2473`). `queue::complete_task` filters only on `id` and `state='RUNNING'` (`queue.rs:2476-2505`). `queue::fail_task` accepts any `PENDING` or `RUNNING` row "regardless of `worker_id`", as the code's own comments admit (`queue.rs:2517-2545`; `worker.rs:7259`, `:23785`). Nothing compares `worker_id` or `attempt`. Worker A's activity can be requeued by a heartbeat or start-to-close timeout, or by the orphan reclaimer when A's worker heartbeat goes stale (`poison_pill.rs:103-114`). Worker B then claims it (attempt+1, `RUNNING`). If A now finishes, A's result is accepted as the activity's outcome, and B's own later `complete_task` fails with `NotFound` ("task queue item ... is not running").
- **Heartbeat is not fenced either.** `record_heartbeat` filters only on `id` and `state='RUNNING'` (`queue.rs:2853-2878`). A zombie A's heartbeat therefore refreshes `last_heartbeat_at` and overwrites `heartbeat_details` on B's live attempt. That masks B's heartbeat timeout and corrupts B's checkpoint. When a heartbeat flush fails because the row is not running, the flusher only logs a warning. The activity is not cancelled, so a fenced-off attempt keeps running. — `heartbeat.rs:74-90`
- Schedule fire-claims use a token plus a `fire_claimed_until` lease, and the write is fenced on `fire_claim_token = $token`. **Present.** — `scheduler.rs:2462-2565`
- The durable mutex uses a monotonic `lock_seq` from `nextval('harvest_mutex_lock_seq')` as its fencing token. `lease_expires_at` is computed with the DB `now()`, and takeover happens only `WHERE lease_expires_at < now()`. **Present and tested** (`tests/integration/mutex_tests.rs`, `mutex_lease_reclaim_perf.rs`). — `mutex.rs:202-215`, `:285-341`

**Timers and clocks**
- Retry deadlines are computed on the Postgres clock (`clock_timestamp() + make_interval`, issue #1389) to remove host/DB skew "by construction". — `queue.rs:40-60`, `:94-96`
- Timeout enforcement compares `COALESCE(last_heartbeat_at, started_at) + heartbeat_timeout < NOW()`, which is the DB clock. — `timeout.rs:84-90`. But `record_heartbeat` stamps `last_heartbeat_at` with the **host** `Utc::now()` (`queue.rs:2866`), and `complete_task` / `fail_task` stamp `completed_at` with the host clock too (`queue.rs:2491`, `:2533`). Worker liveness is stamped with DB `now` (`workers.rs:343`, `:471`). Per-task heartbeat is therefore the one lease signal still exposed to skew: a lagging host clock causes false heartbeat timeouts, and a leading one hides real ones.
- Timers are DB-polled, not a timer wheel. A dedicated `timer_fires_at` column and index were added recently. — `migrations/20260921011505_harvest_task_queue_timer_fires_at/`. The checker holds one pooled connection for a whole `enforce_timeouts_once` pass. — `codec_rotation.rs:1369-1378`

**Leader election**
- No leader election was found (grep for leader/elect/singleton in `worker.rs`, `scheduler.rs`, `retention.rs`, `scanner_health.rs`, `lifecycle.rs`). Every worker runs the background scanners, and each scanner makes itself safe under concurrency with `SKIP LOCKED`, compare-and-swap updates, or a try-advisory "registration lock" (`scheduler.rs:749-756`). That is a valid design, but it costs N times the scan load.

**Lock ordering (present, enforced per path, not globally)**
- The conventions are documented per path. The mutex takes the advisory lock first (`mutex.rs:62-72`). Queue work goes execution row, then task row, and code that inverts that order uses `SKIP LOCKED` so it never waits (`queue.rs:4247-4262`). The child-timeout materializer goes execution row, then `harvest_timers`, and a test pins it with a `NOWAIT` probe. — `docs/architecture.md:571`
- Some tests pin individual orderings (`tests/integration/quota_lock_ordering_tests.rs`, `child_timeout_tests.rs`). No global lock-order checker or linter was found.
- One known cycle is accepted as a liveness hazard: `cancel_running` combined with `ctx.mutex` can raise `40P01` on start. — `docs/sharding.md:70-76`. There is no generic retry-on-`40P01`/`40001` wrapper. Detection helpers exist only in `partition.rs:1122-1128`. Elsewhere a deadlock surfaces as `HarvestError::Database` (`concurrency.rs:331`).

**Pool exhaustion, failover and timeouts**
- Two deadpool pools share a `max_total_connections` ceiling (default 95, worker default 10). `worker_pool_size` is **per shard**, so N shards need N times as many connections. — `pool.rs:1-40`
- Shard pools are built with `.max_size()` only: no wait, create or recycle `Timeouts`. — `shard.rs:1759-1761`. The code says so itself: "Harvest configures no deadpool `Timeouts`, so every `pool.get().await` is an **unbounded** wait". — `codec_rotation.rs:1372-1374`
- A bounded acquire exists only for multi-shard workers. `shard_acquire_bound` returns `None` for a single shard, which means a plain `pool.get().await`. — `worker.rs:5961-5979`. Audit export uses a 5 s `SHARD_ACQUIRE_BOUND`. — `audit_export.rs:734`, `:2587-2589`
- Related closed issues: [#1426](https://github.com/autumn-foundation/autumn-harvest/issues/1426) (monitor loops hang shutdown on an exhausted shard pool) and [#1459](https://github.com/autumn-foundation/autumn-harvest/issues/1459) (task wedged `RUNNING` when the timeout reset races pool contention). Open: [#1552](https://github.com/autumn-foundation/autumn-harvest/issues/1552) (heartbeat task detaches rather than aborts under external cancellation).
- **Session timeouts are absent.** No `statement_timeout`, `idle_in_transaction_session_timeout` or `transaction_timeout` is set on engine connections. The only `SET LOCAL statement_timeout` / `lock_timeout` is in partition maintenance (`partition.rs:1010-1014`, `:4250`). A GitHub issue search for statement_timeout, pool timeout and failover returned no results.
- Failover handling: no explicit primary-failover detection was found. TCP keepalive DSN keys appear only in the dev DSN helper (`autumn-harvest-plugin/src/dev/dsn.rs:76-79`). The HA runbook warns that session-scoped pins break under PgBouncer transaction pooling. — `docs/runbooks/ha-deployment.md:214-218`

**LISTEN/NOTIFY**
- Every history append calls `notify_workflow_events_appended`, which does `SELECT pg_notify(...)` on one global channel inside the append's transaction, and propagates errors with `?`. — `store.rs:237-245`; `notify.rs:229-245`
- Every enqueue, requeue and wake also notifies. — `queue.rs:558`, `:833`, `:3490`, `:3605`, `:3660`, `:4982`
- Only the retry-requeue path tolerates a notify failure. — `queue.rs:2995-3002`

**Redis and SQLite backends**
- Redis is a **dispatch hint channel only**. Postgres keeps every `harvest_task_queue` row as the source of truth, workers claim the named row in Postgres with the full predicate, and a reconcile sweep republishes due `PENDING` rows. If Redis is unreachable the worker falls back to the Postgres claim path. — `autumn-harvest-redis/src/lib.rs:62-71`
- Redis v1 limits: single-node client (not Cluster-aware), **no TLS** (`rediss://` rejected, so a plain URL sends the password in cleartext), best-effort priority and stickiness. — `autumn-harvest-redis/src/lib.rs:73-120`. The standalone `RedisTaskQueue` adapter uses visibility-timeout recovery (`lib.rs:15-17`, `:51-52`).
- SQLite is single-writer by contract. `BEGIN IMMEDIATE` replaces `SKIP LOCKED`, polling replaces LISTEN/NOTIFY, and on `open` every `RUNNING` row is reclaimed as an orphan. — `autumn-harvest-sqlite/src/lib.rs:21-38`; `queue.rs:8-15`. Pragmas are `busy_timeout=5000`, `journal_mode=WAL`, `synchronous=FULL` (`runtime.rs:262-264`). No `locking_mode=EXCLUSIVE` or lockfile enforces single-writer (grep found none). SQLite timers use the host wall clock (`lib.rs:39-49`).

### Inferences
- The activity-completion gap is the most consequential finding here. History cannot fork, because the execution-row lock and event-id uniqueness still hold. The hazard is ownership: a timed-out attempt's result is recorded over a live attempt, and a zombie's heartbeats keep a possibly hung live attempt looking healthy. The fix is small and matches existing code. Carry `(worker_id, attempt)` from the claim and add `AND worker_id = $w AND attempt = $a` to `complete_task`, `fail_task` (on activity paths) and `record_heartbeat`. Make a heartbeat `NotFound` cancel the activity's token. `worker.rs:31260-31277` already applies this pattern to the workflow-task reset.
- `pg_notify` inside every append transaction puts two things on the write hot path: Postgres's global commit-serializing NOTIFY lock, and a queue-full failure mode that would fail appends. At the high append rates sharding targets, this may be the throughput ceiling before row contention is.
- Without `statement_timeout` and `idle_in_transaction_session_timeout`, one hung connection can hold `xmin` back. In a single-shard deployment, an unbounded pool wait can also hang loops. This is the brandur failure mode applied to `harvest_task_queue`.

### Gaps
- I did not run the code (no build or test was allowed). The two fencing gaps are derived from reading the code, and I found no test that asserts a stale activity completion after re-claim is rejected. `grep` for zombie/stale-completion tests found only workflow-task-level tests.
- I did not determine whether deadpool's default recycle check detects a connection broken by a failover before handing it out.

---

## Q5 (codebase). Migration safety, retention and vacuum, backup/restore verification, cross-region DR, partitioning and rebalancing

### Takeaway
DR, backup verification, shard rebalancing and event partitioning are unusually thorough and honestly documented, and each has integration tests. Migration safety relies on convention and hand-written recipes rather than tooling:
- Only 2 of 113 migrations bound their lock wait.
- No migration actually runs `CREATE INDEX CONCURRENTLY`.
- One recent migration rebuilds a unique index on the hot executions table inside a transaction.

Vacuum is not tuned for the hot queue table, and terminal task rows are never deleted under default retention.

### Cited Findings

**Migrations**
- There are 113 migration directories (`ls autumn-harvest/migrations | wc -l`).
- Only two set `SET LOCAL lock_timeout = '5s'`: `20260901115500_harvest_event_partitioning/up.sql:24` and `20260902131705_harvest_shard_rebalancing/up.sql:49`. The first explains why this matters: `ADD COLUMN` takes ACCESS EXCLUSIVE, and later statements queue behind a lock *waiter*, so all appends stall behind one idle-in-transaction reader. — `20260901115500.../up.sql:14-23`
- `migrate.rs` supports `run_in_transaction = false` via `metadata.toml` for `CONCURRENTLY` builds (`migrate.rs:31-35`, `:94`, `:110-151`). But no migration ships a `metadata.toml`, and every occurrence of `CONCURRENTLY` in `up.sql` is inside a comment or error string (grep).
- Large-table indexes are handled with a manual "build it out of band first" recipe plus a guard that accepts a pre-built valid index. Otherwise plain `CREATE INDEX` takes `SHARE` on `harvest_events`, "which blocks every append, claim and completion". — `migrations/20260919144514_harvest_events_recent_by_timestamp_index/up.sql:14-30` (the same pattern is cited for `20260905181020`, `20260911213344`, `20260913010332`)
- `20260915231809_harvest_migrated_seal_terminal_at/up.sql:48-54` does `DROP INDEX` then `CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key ON harvest_workflow_executions ...` inside the migration transaction, with no `lock_timeout` and no out-of-band recipe. Plain `DROP INDEX` takes ACCESS EXCLUSIVE on the table until commit, so executions reads and writes block for the whole unique-index build.
- Constraint changes sometimes use `NOT VALID` (`20260430000000.../up.sql:12-19`, `20260902131705.../up.sql:43-73`). Earlier ones replace CHECK constraints with full validation (`20260503000000_harvest_workflow_reset/up.sql:9-37`).
- CI migration hygiene guards cover duplicate timestamp prefixes and test-bundle drift, **not** lock-safety. — `autumn-harvest/tests/integration/migration_hygiene.rs:1-20`. `migrate.rs` offers no `down.sql` rollback. — `migrate.rs:44-47`

**Retention and vacuum**
- History retention is **off by default** (`max_age_secs: None`). Audit (90 d) and rate-limit-bucket GC are on by default. — `retention.rs:484-495`; `docs/autumn-workflow-architecture.md:992`
- No retention pass deletes terminal `harvest_task_queue` rows directly. `retention.rs` only reads them (`retention.rs:3465-3467`). Rows go when the execution is deleted, through the `ON DELETE CASCADE` FK (`migrations/20260409000000_harvest_initial/up.sql:40,55,135`). With default settings, COMPLETED and FAILED task rows therefore accumulate indefinitely (inference from the defaults plus the cascade).
- No `autovacuum_*` or `fillfactor` storage parameters are set on any table (grep of migrations and `src`). `harvest_task_queue` carries about 15 indexes, several of them partial on `state`. Each state transition therefore moves index entries and cannot be a HOT update.
- Doc drift: `docs/autumn-workflow-architecture.md:988` says `harvest_task_queue` "is also partitioned by `queue_name` using list partitioning". No migration contains `PARTITION BY` (grep), so this is inaccurate.
- Event partitioning (issue #958) is opt-in. Partitions are dropped rather than rows deleted, which avoids vacuum churn. **Present and tested** (`tests/integration/event_partitioning_tests.rs`). A residual duplicate-event window is documented: two concurrent in-flight appends of the same `event_id` whose insert instants straddle a cohort boundary. — `migrations/20260901115500.../up.sql:197-207`; `docs/partitioned-events.md:110-130`

**Backup and restore (present and tested)**
- `backup_verify.rs` does read-only post-restore verification with a three-tier severity model. It pins every connection to a read-only session and reuses the scanners' selection predicates. — `backup_verify.rs:1-45`. It is wired into the CLI (`autumn-harvest-cli/src/lib.rs`) and the 30-minute drill runbook, whose exit codes are 0 resumable, 1 incoherent, 2 undetermined. — `docs/runbooks/backup-restore.md:138-161`. Tests: `tests/integration/backup_verify_tests.rs` and two perf suites.

**Cross-region DR (issue #954; present and tested, opt-in)**
- Replication is per shard, using stock Postgres logical or physical replication. Failover is operator-initiated. There is no automatic promotion, no active-active mode and no zero-RPO mode. — `docs/cross-region-dr.md:3-8`, `:39-53`
- The fence is a per-shard `harvest_shard_generation` epoch.
  - **Claim gate:** a `CROSS JOIN` in the claim CTE (`queue.rs:1149-1165`, `:1444`).
  - **Persist assert:** `FOR SHARE` on the generation row in the same transaction as every event INSERT, which acts as a commit-order barrier against `bump_generation` (`store.rs:217-235`; `replication.rs:1171-1189`).
  - The worker pins the epoch once and "must **never** adopt a newer epoch". — `docs/cross-region-dr.md:185-228`
- Stated limits:
  - The fence cannot stop a partitioned old-region worker writing to its own old primary, so "isolating the old primary's database is a mandatory operator step".
  - Fencing is **opt-in per process** and off by default, so admin scripts, migration jobs and workers without `dr_fencing` are not fenced.
  - A bump fences everyone.
  - — `docs/cross-region-dr.md:241-276`
- RPO is measured through a WAL-LSN watermark trail rather than `replay_lag`, which "goes blind" when the apply worker is stuck. — `docs/cross-region-dr.md:278-316`. A starter alert fires at `harvest_replication_lag_seconds > 60`. — `docs/alerts/starter-pack-v0.1.0.json:1239`
- Multi-shard skew is acknowledged, with no cross-shard consistent snapshot. Named hazards: duplicate outbox deliveries, parent/child skew (bounded by `child_timeout`), and schedule re-fires (absorbed only if runs carry an idempotency key). The discipline is "fence all shards, verify all shards, and only then start workers". — `docs/cross-region-dr.md:356-397`
- Tests: `tests/integration/cross_region_dr_tests.rs`, `cross_region_dr_docs.rs`.

**Shard rebalancing (issue #964; present and tested)**
- Only quiescent executions move: copy, replay-verify, one atomic cutover on the source, then the sealed source acts as a forwarding pointer. `ExecutionId` never changes. — `docs/sharding.md:529-560`
- A crash-safe phase table lives on the source. The cutover re-checks quiescence **and** the verified history high-water mark. — `docs/sharding.md:652-680`
- A documented liveness gap: between cutover and target activation, the run is claimable on neither shard. — `docs/sharding.md:682-687`
- Tests: `tests/integration/shard_rebalance_db_tests.rs`, `shard_rebalance_unit.rs`.

**Codec-key re-encryption (exception #3 to append-only)**
- It fences with `assert_fence` before its writes (`codec_rotation.rs:1068`, `:1165`, `:1276`). CLAUDE.md documents a CAS against erasure and a replay-fidelity test.

### Inferences
- DR, backup and rebalancing are ahead of most self-hosted engines. Their residual risks are operator-procedure risks, and the docs name them explicitly.
- Migration lock-safety is the weakest data-layer discipline. It depends on each author remembering `lock_timeout` and the out-of-band recipe. A CI lint could enforce it: require `SET LOCAL lock_timeout` in any `up.sql` that ALTERs, DROPs or indexes an existing hot table, and forbid non-concurrent `CREATE INDEX` or `DROP INDEX` on `harvest_task_queue`, `harvest_events` and `harvest_workflow_executions` unless the out-of-band guard is used.

### Gaps
- I did not inspect `docs/runbooks/cross-region-failover.md` or `docs/runbooks/shard-decommission.md` in detail.
- I did not verify whether the `dr_fencing` config is plumbed through `autumn-harvest-plugin` for embedded deployments.
- I did not measure actual table bloat or autovacuum behaviour. No benchmark in `docs/` covers long-run `harvest_task_queue` growth under default retention.

---

## Q6. Rated gap register

| # | Gap | Evidence status | Rating | Reasoning | Suggested remedy |
|---|---|---|---|---|---|
| 1 | Activity `complete_task`, `fail_task` and `record_heartbeat` are not fenced by claim epoch (`worker_id`, `attempt`). A zombie attempt's result or heartbeat lands on the live re-claimed attempt. A heartbeat `NotFound` does not cancel the activity. | Absent (`queue.rs:2476-2545`, `:2853-2878`; `worker.rs:13068-13083`; `heartbeat.rs:74-90`) | **High** | Reclaim is designed to happen (heartbeat or start-to-close timeout, orphan reclaim after worker-liveness loss, `poison_pill.rs:103-114`), so this is a normal-path race, not a corner case. The history cannot fork, but ownership semantics break: stale results are accepted and a hung live attempt is masked. This is the Kleppmann / River class of bug. | Carry `(worker_id, attempt)` from the claim into every activity write, and add both to the `WHERE` clause. Treat 0 rows as "lease lost" and cancel the activity's token. Add an integration test for timeout, re-claim, then a late completion. |
| 2 | No `statement_timeout`, `idle_in_transaction_session_timeout` or `transaction_timeout` on engine connections. No deadpool `Timeouts`, and single-shard `pool.get()` is unbounded. | Absent (`shard.rs:1759-1761`; `codec_rotation.rs:1372-1374`; `worker.rs:5965-5967`) | **High** | One stuck transaction pins `xmin` (queue bloat, brandur) and holds locks. Pool exhaustion has already produced wedges (#1426, #1459). Postgres defaults are 0 (disabled). | Set per-session timeouts on pool connection creation, sized per role (short for claim and heartbeat, longer for scanners). Configure deadpool `wait`/`create`/`recycle` timeouts. Alert on `age(backend_xmin)`. |
| 3 | Unconditional `pg_notify` inside every history append and enqueue transaction, on a global channel. Errors propagate. | Present, on hot path (`store.rs:237-245`; `queue.rs:558`, `:833`, ...) | **High at scale / Medium otherwise** | The NOTIFY commit path serializes commits instance-wide (Recall.ai), and a full queue fails commits (PG docs). The wake-up hint thereby becomes a write-availability dependency. | Make NOTIFY opt-in or send it post-commit on a separate connection, and never fail an append on notify. Consider coalescing. Reassess once Postgres ships the upstream fix (version unverified). |
| 4 | The #1184 workflow-task guard `claim_still_held_for_update` checks `worker_id + crash_strikes` but not `attempt`. The code's own comments say `requeue_stuck_task` makes that insufficient. | Present but incomplete (`queue.rs:4181-4188` vs `worker.rs:31260-31277`, `poison_pill.rs:352-364`) | **Medium** | A same-worker re-claim after a stuck requeue lets a stale dispatcher's terminal write pass. Deterministic replay plus event-id uniqueness limit the damage. | Add `AND attempt = $4`, as the timeout-reset path already does. Add a regression test. |
| 5 | Per-task heartbeat is stamped with the host clock (`Utc::now()`) but judged against DB `NOW()`. | Present, inconsistent (`queue.rs:2866` vs `timeout.rs:90`) | **Medium** | Host/DB skew causes false heartbeat timeouts (spurious retries, double execution) or hides real ones. #1389 fixed exactly this class of bug for retry deadlines. | Stamp `last_heartbeat_at = NOW()` in SQL. Do the same for `completed_at`. |
| 6 | Online-migration safety is not enforced. 2 of 113 migrations set `lock_timeout`. No migration uses `CONCURRENTLY`. `20260915231809` rebuilds a unique index on `harvest_workflow_executions` in-transaction. | Present by convention only (`migration_hygiene.rs:1-20`; `20260915231809.../up.sql:48-54`) | **Medium** | Upgrades can block the executions table or queue writes behind a lock waiter for the length of an index build. The recipes exist but are manual. | Add a CI lint (lock_timeout required; no non-concurrent index DDL on hot tables without the out-of-band guard). Ship `metadata.toml` `run_in_transaction=false` migrations for concurrent builds. |
| 7 | Queue-table hygiene: no autovacuum or fillfactor tuning. About 15 indexes on `harvest_task_queue`. Terminal task rows are retained forever under default retention. | Absent (grep; `retention.rs:484-495`) | **Medium** | Constant `state` churn plus unbounded terminal rows means table and index growth, vacuum load and freeze work that rise over time. Partial indexes soften the claim path but not the heap or vacuum cost. | Add a terminal-task-row janitor that is independent of history retention. Set table-level autovacuum scale factors and a lower fillfactor for `harvest_task_queue`. Document XID-age and dead-tuple monitoring. |
| 8 | Lock ordering is per-path conventions plus targeted tests, with no global checker. There is no generic retry on `40P01`/`40001`. | Present and partially tested (`docs/architecture.md:571`; `mutex.rs:62-72`; `docs/sharding.md:70-76`) | **Medium-Low** | New code can add an inversion that only shows up under load. The one known cycle is a liveness issue only. | Write down a single global lock hierarchy (advisory, execution, task, timers, outbox). Add a debug-mode lock-order assertion or a test harness. Add a bounded retry wrapper for deadlock and serialization errors on idempotent transactions. |
| 9 | DR fencing is opt-in and off by default. Admin tooling is unfenced. There is no automatic failover, and multi-shard skew is accepted. | Present and tested, opt-in (`docs/cross-region-dr.md:166-183`, `:241-276`, `:356-397`) | **Medium (operational)** | Correct by design, but safety depends on operators performing old-primary isolation and fencing in the right order. | Default `dr_fencing` on when a standby is configured. Have the CLI and admin tools pin epochs as well. Add a drill that checks the old primary is isolated. |
| 10 | `claim_task_batched` (public API) lacks the DR fence splice. | Present but not wired (`queue.rs:7186-7215`) | **Low** (latent) | Harmless today, but promoting it to the default claim path as issue #1340 intends would silently bypass the fence. | Add a fence splice and a test that asserts every claim query variant contains `harvest_shard_generation` when fenced. |
| 11 | Partitioned-events residual duplicate-append window at cohort boundaries. | Present, documented (`up.sql:197-207`) | **Low** | Needs a split-brain plus a microsecond boundary overlap. Honestly disclosed. | Optionally serialize appends per execution when partitioned. Most paths already hold the execution-row lock, so auditing the paths that do not may close the window cheaply. |
| 12 | SQLite single-writer contract is not enforced, and `open()` reclaims every `RUNNING` row. | Absent enforcement (`autumn-harvest-sqlite/src/lib.rs:21-38`; `runtime.rs:262-264`) | **Low** | Two processes on one file would steal each other's tasks and double-execute. The docs state it as a contract. | Take an exclusive lockfile or `PRAGMA locking_mode=EXCLUSIVE` at open. |
| 13 | Redis dispatch: no TLS, not Cluster-aware. | Documented limit (`autumn-harvest-redis/src/lib.rs:73-103`) | **Low** (data layer) | Postgres stays the source of truth, so there is no consistency risk. The exposure is credential and availability. | Track the follow-up issues already named in the crate docs. |
| 14 | Shard-rebalance cutover-to-activation window, where the run is claimable on neither shard. | Documented (`docs/sharding.md:682-687`) | **Low** | Liveness only, and closed by `rebalance-resume`. | Auto-run resume from a scanner. |
| 15 | Doc drift: `harvest_task_queue` described as list-partitioned. | Inaccurate doc (`docs/autumn-workflow-architecture.md:988`) | **Low** | Misleads operators planning vacuum strategy. | Correct the doc. |

### Inferences
- The two High fencing and timeout gaps are cheap to fix and follow patterns already present elsewhere in the tree: the `attempt` guard at `worker.rs:31277`, DB-clock stamping from #1389, and bounded acquisition in `SHARD_ACQUIRE_BOUND`.
- The strengths should be credited in the final report, because they are rare in comparable engines:
  - explicit READ COMMITTED pinning;
  - DB-clock leases for the mutex and retries;
  - `lock_seq` and fire-claim tokens;
  - a generation fence with a `FOR SHARE` commit barrier;
  - an LSN-watermark RPO metric;
  - read-only restore verification;
  - copy, verify and cutover rebalancing.

### Gaps
- No runtime reproduction was possible (no builds or tests allowed). Ratings for gaps 1, 4 and 5 rest on reading the code.
- I did not check open GitHub issues exhaustively. Issue searches for activity-completion fencing, statement_timeout and NOTIFY scalability returned no open issue that tracks gaps 1–3.
