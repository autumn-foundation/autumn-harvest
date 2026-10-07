# Storage Architecture, Scalability and Performance Engineering for Durable Workflow Engines (Postgres emphasis, October 2026)

Scope note: research was done with web search and page fetches on 2026-10-07. Almost every number below is a **vendor-published** figure unless it says otherwise. The only multi-system independent benchmark found is hardbyte/postgresql-job-queue-benchmarking (May 2026). Several pricing figures come from aggregator sites and are marked as unverified.

## How do Temporal, Restate, DBOS, Inngest, Hatchet, Absurd, pgflow, PGMQ, River, Oban, graphile-worker and Solid Queue persist and scale? What numbers are published, and under what conditions?

### Takeaway
There are two main designs. In the first, an engine owns a log or shard and keeps hot state close to compute: Temporal uses history shards with in-memory mutable state and a transactional outbox, and Restate uses the Bifrost replicated log with RocksDB partition processors that snapshot to S3. In the second, the engine is a thin library over Postgres tables: DBOS, Absurd, Hatchet, River and graphile-worker. The best published Postgres numbers are tens of thousands of workflows or jobs per second on one large node. DBOS reports 43K no-op workflows/s on a 96-vCPU RDS instance. The limits are WAL flush and lock contention on the head of the queue, not CPU. Restate reports about 94K steps/s on a 3-node replicated cluster.

### Cited Findings
**Temporal**
- Workflow executions are split into History Shards. The shard count is fixed when the cluster is created and cannot be changed later. Each shard maps to one persistence partition, and a shard allows only one concurrent operation inside its partition at a time — [Temporal docs: Temporal Server](https://docs.temporal.io/temporal-service/temporal-server); [deepwiki summary](https://deepwiki.com/temporalio/temporal/3-history-service)
- Owning a shard means "being responsible for the lifecycle of every workflow execution in that shard by (synchronously) handling incoming requests ... and (asynchronously) processing background tasks" — [temporal/docs/architecture/history-service.md](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- Temporal persists Mutable State, a per-workflow summary of open activities, timers and similar items, instead of recomputing it from history on each request. Recently used Mutable State is cached in memory — [history-service.md](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- The Transfer and Timer task queues use the Transactional Outbox Pattern. History events, mutable state and history tasks commit atomically. A history event "is only valid if it is in Mutable State", and on a failed persist the engine reloads from persistence — [history-service.md](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- Supported persistence: Cassandra, MySQL, PostgreSQL or SQLite, with Elasticsearch as an option for advanced Visibility — [Temporal docs](https://docs.temporal.io/temporal-service/temporal-server)
- CHASM is a new durable state-machine framework inside the server. Standalone Activities are now enabled by default (`activity.enableStandalone`), and Standalone Nexus Operations need CHASM. Requests that carry a `component_ref` go to the CHASM engine instead of the mutable-state workflow engine. CHASM persistence supports separate businessID spaces per archetype — [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) (via search snippet; exact release version not confirmed)

**Restate**
- Bifrost is a segmented, distributed durable log. It gives "fast primary durability for events" such as invocations, journal entries, state updates and durable promises. Reconfiguration uses external consensus from the control plane — [Restate blog, Feb 20 2025](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- Partition processors tail the log, invoke handlers over a bidirectional stream, and keep a materialized state cache in embedded RocksDB. The cache holds journals, idempotency metadata, virtual-object state and timer indices — [Restate architecture docs](https://docs.restate.dev/references/architecture)
- Partition processors periodically snapshot RocksDB to S3 and trim the log at the snapshot point. On takeover, a new processor downloads the latest snapshot and replays only the log suffix. Snapshots happen "a few times per hour", so object-store performance does not affect cluster performance — [Restate snapshots docs](https://docs.restate.dev/server/snapshots); [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- One replication option is "quorum replication to nodes with async batch writes to S3" — [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- Vendor benchmark: 3-way replicated cluster on AWS c6id.8xlarge nodes, 1,200 concurrent clients, 9 intermediate steps per workflow: **94,286 steps/s = 8,571 workflows/s**. Latency at low load (10 clients): single step p50 5 ms; 3 steps p50 15 ms, p99 69 ms. At high load: 3 steps p50 58 ms, p99 98 ms; 9 steps p50 116 ms, p99 163 ms. Median per step is about 10 ms under high load — [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)

**DBOS**
- DBOS is a library. The only overhead per step is one Postgres write that checkpoints the step output, "typically 1-2ms". DBOS contrasts this with Temporal, where a step needs an async dispatch from the central server ("tens to hundreds of ms") — [DBOS docs: Comparing DBOS and Temporal](https://docs.dbos.dev/explanations/comparing-temporal)
- Vendor benchmark (Apr 23 2026), RDS db.m7i.24xlarge (96 vCPU, 384 GB RAM, 120K provisioned IOPS io2), async Python clients, one row per transaction:
  - raw inserts: **144K writes/s**
  - durable no-op workflows (2 writes each): **43K workflows/s**
  - queued workflows (4 writes each): **12.1K/s on a single queue** and **30.6K/s across multiple partitioned queues**
  - Bottlenecks: for writes and workflows, "exactly one process was flushing the WAL to disk" while others waited on the WAL lock. For queues, the limit was "lock contention in the workflow_status table" because all clients hit "the same few rows at the head"
  - Source: [DBOS blog](https://www.dbos.dev/blog/benchmarking-workflow-execution-scalability-on-postgres)
- DBOS publishes an open benchmark harness — [dbos-inc/durable-execution-benchmark](https://github.com/dbos-inc/durable-execution-benchmark) (repo not fetched; contents not verified)
- Third-party commentary says DBOS throughput is "5,000 to 20,000 workflow steps per second on a well-tuned instance" and puts DBOS latency at 5–20 ms against 1–5 ms for Restate. These are unsourced estimates from a blog and are not measurements — [Kanopy Labs](https://kanopylabs.com/blog/restate-vs-temporal-vs-dbos-durable-execution)

**Hatchet**
- Hatchet v1 grew to over 20k tasks per minute, which is more than 1 billion per month. Bursts reach over 5k tasks/s, about 25k transactions/s, because each task needs at least 5 Postgres transactions. Plain `FOR UPDATE SKIP LOCKED` showed pathological CPU spikes when there were "many tasks in the backlog, many workers, workers long-polling at approximately the same time" — [Show HN: Hatchet v1](https://news.ycombinator.com/item?id=43572733)
- Six v1 changes:
  1. range partitioning of time-series tables
  2. hash partitioning of task events
  3. monitoring tables separated from the queue
  4. buffered reads and writes, flushed every 10 ms
  5. identity columns instead of UUIDs, to reduce index bloat
  6. heavy use of triggers
  - Source: [Show HN: Hatchet v1](https://news.ycombinator.com/item?id=43572733)
- An HN commenter flagged a risk of multixact ID exhaustion and problems with long-lived transactions under heavy `FOR UPDATE` use — [Show HN thread](https://news.ycombinator.com/item?id=43572733)
- Hatchet's published Postgres scaling posts include partitioning pitfalls, the events table, multi-tenant fair queues and a startup survival guide — [hatchet.run/blog/postgres-partitioning](https://hatchet.run/blog/postgres-partitioning); [postgres-events-table](https://hatchet.run/blog/postgres-events-table); [multi-tenant-queues](https://hatchet.run/blog/multi-tenant-queues); [postgres-survival-guide](https://hatchet.run/blog/postgres-survival-guide)

**Absurd, PGMQ, pgflow**
- Absurd is a SQL-only library with a thin SDK. It stores both the queue and the workflow state in Postgres and needs no extension (Nov 2025) — [Armin Ronacher, Absurd Workflows](https://lucumr.pocoo.org/2025/11/3/absurd-workflows/)
- After five months in production, Absurd had SDKs for TypeScript and Python, an experimental Go SDK, a CLI (absurdctl) and a dashboard — [Absurd in production, Apr 2026](https://lucumr.pocoo.org/2026/4/4/absurd-in-production/)
- PGMQ is an SQS-like queue in Postgres with visibility timeouts and delete/archive semantics — [mfyz.com](https://mfyz.com/durable-queue-workers-with-just-postgres/)
- I found no pgflow-specific numbers.

**Job queues (River, graphile-worker, Oban)**
- River (vendor): about 46k jobs/s on an 8-core M2 MacBook Air with 2,000 worker goroutines — [River benchmarks](https://riverqueue.com/docs/benchmarks)
- graphile-worker (vendor): about 196k jobs/s with 4 instances at concurrency 24. Performance goes down, not up, once Postgres saturates and more workers are added — [graphile-worker performance](https://worker.graphile.org/docs/performance); [scaling tips](https://worker.graphile.org/docs/scaling)
- **Independent benchmark** (hardbyte, May 9 2026). Eight systems on the same Postgres 18.6 config: awa, pgque, pgmq, pg-boss, river, absurd, oban and procrastinate. It uses a closed-loop harness with steady-state, sustained-pressure and chaos scenarios.
  - Peak throughput: pgque (event bus) **39.9k/s**; awa (fastest full job queue) **14.2k/s**; pgmq **11.3k/s**
  - pgmq "anti-scales past 16 workers"
  - pg-boss, river, absurd and procrastinate timed out under sustained pressure
  - Append-only designs (awa, pgque) resisted bloat better than row-mutating designs
  - Hardware is not stated in the summary
  - Source: [hardbyte/postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

### Inferences
- Vendor numbers from 46k to 196k jobs/s fall to about 10–40k/s under the independent sustained, multi-system harness. A rough rule: discount vendor peak queue numbers by 3–10x for sustained, bloat-affected production loads.
- On Postgres, the common ceiling is (a) WAL flush serialization for one-row-per-transaction writes and (b) hot-row contention at the head of the queue. Both point at batching (group writes or buffered flush, as Hatchet does at 10 ms) and at sharding the queue head (DBOS's partitioned queues gave 2.5x).
- Append-only state, where state changes are inserted events instead of updated rows, did measurably better on bloat in the independent benchmark. A Postgres-first engine that already keeps an append-only event log can aim to keep its queue append-only too.

### Gaps
- I did not fetch current Inngest architecture or numbers. Its persistence design (state store and queue) is not covered here.
- I found no published scaling numbers for Solid Queue, Oban (beyond the hardbyte run), pgflow or Temporal-on-Postgres. Temporal does not publish official throughput per persistence backend.
- Details of Temporal history archival and CHASM's persistence schema were not fetched.

## Postgres queue scaling limits and techniques

### Takeaway
The main Postgres-native problems are now well documented: SKIP LOCKED collapse under many concurrent pollers, the NOTIFY global commit lock, missing parent-table statistics on partitioned tables, and WAL-flush serialization. Each has a known workaround: batching or buffering, partitioned queue heads, notification coalescing, manual `ANALYZE`, and time partitions dropped for retention. Postgres 18 adds AIO, uuidv7 and skip scan; Postgres 19 is in beta. Neither changes the write-path bottleneck at its root.

### Cited Findings
- **NOTIFY global lock:** committing a transaction that called NOTIFY takes a global exclusive lock held through commit and fsync. This serializes such commits — [Recall.ai](https://recall.ai/blog/postgres-listen-notify-does-not-scale)
- DBOS explains why: Postgres delivers notifications in commit order, so commits that contain NOTIFY are serialized, which "disabl[es] optimizations like group commit". Naive throughput was **2.9K writes/s**. With in-memory buffering, batched NOTIFY flushes and a fallback poll, it reached **60K writes/s** at 15–100 ms latency (Jul 24 2026). DBOS did not state the hardware — [DBOS blog](https://dbos.dev/blog/postgres-listen-notify-scalability)
- A pgsql-hackers proposal suggests an "Out-of-Order NOTIFY" GUC to remove the ordering constraint — [pgsql-hackers](https://www.postgresql.org/message-id/ab1b986a-8ae2-469e-a680-11c1ce8fd4e8%40app.fastmail.com); [hackorum](https://hackorum.dev/topics/51945). Its status in PG19 is not confirmed.
- **SKIP LOCKED collapse:** Hatchet dropped plain `FOR UPDATE SKIP LOCKED` after CPU spikes when many workers long-polled a large backlog at once — [Show HN: Hatchet v1](https://news.ycombinator.com/item?id=43572733)
- **Head-of-queue contention:** DBOS's single queue was capped at 12.1K/s by lock contention on the head rows. Partitioned queues reached 30.6K/s — [DBOS blog](https://www.dbos.dev/blog/benchmarking-workflow-execution-scalability-on-postgres)
- **Time partitioning with drop-based retention** (Hatchet, Dec 2025):
  - daily range partitions with compound PK `(id, inserted_at)`
  - partitions created 2 days ahead
  - retention by `DETACH PARTITION ... CONCURRENTLY` then `DROP TABLE`, plus `DETACH ... FINALIZE` for orphaned detaches
  - Source: [Hatchet: pitfalls of partitioning](https://hatchet.run/blog/postgres-partitioning)
- **Partition statistics pitfall:** "autovacuum does not run ANALYZE on partitioned tables". Hatchet saw row estimates off by **6,100,000x** and queries **10x slower** (to 20 ms+). The fix is a manual `ANALYZE` on parent tables. Single-partition load tests hid the problem — [Hatchet](https://hatchet.run/blog/postgres-partitioning)
- In Postgres 19, `vacuumdb --analyze-only` analyzes partitioned tables by default. This is partial tooling help for the problem above — [PostgreSQL 19 Beta 1 announcement](https://postgresql.org/about/news/postgresql-19-beta-1-released-3313/)
- Hatchet notes that partitions vacuum independently and that deleting old data is near-instant by dropping a partition — [Hatchet postgres-events-table](https://hatchet.run/blog/postgres-events-table) (via search snippet)
- **Postgres 18:**
  - asynchronous I/O for reads: `io_method` is sync, worker (default) or io_uring; reported 2–3x gains on sequential and bitmap heap scans, most on network-attached storage
  - `uuidv7()` for time-ordered keys
  - skip scan on multicolumn B-tree indexes
  - Sources: [noqta.tn guide](https://noqta.tn/blog/postgresql-18-async-io-uuidv7-developer-guide-2026); [linuxiac](https://linuxiac.com/postgresql-18-released-with-up-to-3-faster-io-and-easier-upgrades/)
- **Postgres 19:** Beta 1 shipped June 4 2026 and Beta 2 July 16 2026. It brings "performance, developer experience, security, monitoring, and logical replication improvements" — [PG19 Beta 1](https://postgresql.org/about/news/postgresql-19-beta-1-released-3313/); [release notes](https://www.postgresql.org/docs/19/release-19.html)

### Inferences
- PG18 AIO speeds up reads, such as history replay scans and retention scans. It does not help the commit and WAL-flush path that bounds enqueue and checkpoint throughput. Expect little benefit for queue throughput, but some for cold-history reads on cloud volumes.
- `uuidv7` and identity keys both address index-locality bloat. Hatchet's move away from random UUIDs is the motivating evidence.
- Notification coalescing, a buffered NOTIFY with a polling fallback, is now a proven pattern. An engine that issues NOTIFY per enqueue inside the business transaction can lose up to 20x in throughput.

### Gaps
- I did not research unlogged tables, advisory-lock queues, logical-replication or CDC fan-out, or OrioleDB (an undo-log storage engine that addresses MVCC bloat). I found no benchmarks of OrioleDB on queue workloads in this pass.
- I did not research Neon or serverless branching for workflow engines, or distributed Postgres (Citus, Aurora Limitless, Yugabyte, CockroachDB). I found no source showing a durable-execution engine published on these with numbers.
- I could not confirm which PG19 features matter specifically for queues, beyond the vacuumdb change. The full release notes were not fetched.

## Object-storage-backed or disaggregated designs and their fit for workflow history

### Takeaway
Restate is the leading example of a durable-execution engine that uses object storage as its snapshot and backup tier, with a replicated log as the hot tier. "Zero-disk" designs such as WarpStream and SlateDB batch writes to S3 to manage the cost of each PUT. That adds latency, so they suit cold or tiered history better than the per-step commit path.

### Cited Findings
- Restate: RocksDB snapshots go to S3 and the log is trimmed. Recovery means downloading a snapshot and replaying the log suffix. Snapshot frequency is "a few times per hour" — [Restate snapshots docs](https://docs.restate.dev/server/snapshots); [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- SlateDB is an embedded LSM storage engine that writes to object storage. It batches writes and flushes MemTables as SSTs to manage write API cost. `put()` resolves once the data is durable. Its stated use cases include "durable execution, workflow orchestration" — [The New Stack](https://thenewstack.io/slatedb-bottomless-databases-built-on-cloud-object-stores/); [slatedb README](https://docs.rs/crate/slatedb/0.15.0/source/README.md)
- WarpStream started the "Zero-Disk Architecture" idea: a Kafka-compatible engine that runs entirely on S3. Confluent acquired it — [The New Stack](https://thenewstack.io/slatedb-bottomless-databases-built-on-cloud-object-stores/)
- A third-party benchmark compares SlateDB with RocksDB — [nixiesearch substack](https://nixiesearch.substack.com/p/benchmarking-slatedb-vs-rocksdb) (not fetched)

### Inferences
- For a Postgres-first engine, a practical tiered design keeps hot history in Postgres partitions. When a time partition ages out, it is exported to Parquet on S3 for archival and analytics instead of being dropped. Temporal Cloud's ~40x price gap between active and retained storage (below) shows the economic case for this.
- A write path on object storage alone adds batching latency, so it does not fit per-step commits that need low latency. That is why Restate puts a quorum-replicated log in front of S3.

### Gaps
- I found no published case of workflow history stored in Iceberg or Parquet with a replay path. I also found no Temporal archival performance data.
- I did not research S2 (s2.dev) streams or Restate's newer S3-only or "diskless" deployment modes.

## Cost models: cost per million actions or steps, and what drives cost

### Takeaway
List prices per million billable operations range from about $8 (Lambda durable functions) through $25 (Step Functions Standard, Temporal at volume, Restate overage) to $50 (Temporal pay-as-you-go). The billing unit differs a lot between vendors: actions, state transitions, step runs or durable operations. Storage is billed separately. Temporal charges about 40x more for open-workflow storage than for closed-workflow storage.

### Cited Findings
- **Temporal Cloud:**
  - $50 per million actions pay-as-you-go, self-service tiers down to $25/M, with deeper discounts by commit
  - Active storage $0.042/GB-hr; retained storage $0.00105/GB-hr
  - Billable action types include Workflow, Activity, Timer, Signal, Query and Schedule operations
  - Business support costs $500/month or 10% of usage
  - Source: [Temporal pricing](https://temporal.io/pricing)
- **AWS Lambda durable functions:**
  - $8.00 per million durable operations (start execution, step completion, wait)
  - $0.25/GB of data written; $0.15/GB-month of data retained
  - Lambda compute is billed separately
  - AWS's example: 1M executions x 4 operations = $32/month
  - Source: [AWS Lambda pricing](https://aws.amazon.com/lambda/pricing/)
- **AWS Step Functions Standard:** $25 per million state transitions ($0.000025 each), 4,000 free per month — [cloudburn.io](https://cloudburn.io/blog/aws-lambda-pricing) (aggregator; matches the widely published AWS list price)
- **Inngest:** billed per step run. Free tier has 50K steps/month and 7-day history; Basic $20/month for 200K steps; Pro $50/month for 500K steps; Advanced is custom — [automationatlas.io](https://automationatlas.io/answers/inngest-pricing-explained-2026/) (aggregator); [Inngest pricing](https://inngest.com/pricing) (not fetched directly)
- **Restate Cloud** (unverified aggregator): Free 50K actions/month; Starter $75/month for 5M; Business $300 for 20M; Premium $1,000 for 50M. Overage is $25/M up to 100M, then $10/M up to 200M — [devtune.ai](https://devtune.ai/verticals/workflow-orchestration-and-durable-execution/restate/pricing). The official pricing page at [restate.dev/pricing](https://restate.dev/pricing) did not render for extraction.
- **DBOS:** Pro $75/month and Teams $99/month. Managed-execution metering is not public — [toolradar](https://toolradar.com/compare/dbos-vs-inngest) (aggregator)

### Inferences
- What drives cost: (1) how many billable operations a vendor counts per logical step (Temporal counts timers, signals and more as actions; AWS counts starts and waits); (2) storage of open-workflow state; (3) retention. For a self-hosted Postgres engine, marginal cost is roughly Postgres write IOPS and WAL volume per step. DBOS's 144K writes/s on one large RDS node implies a cost per million steps far below $8–$50 when well used.
- A Postgres engine that needs 4–5 writes per task (Hatchet: 5 transactions; DBOS queued: 4 writes) pays 2–2.5x the write cost of a 2-write design. Cutting writes per step is the main cost lever.

### Gaps
- I could not verify Restate Cloud and DBOS Cloud prices against primary pages.
- I did not find Temporal's multi-region (replicated namespace) price multiplier on the pricing page.
- I found no independent TCO comparison.

## Multi-region and active-active workflow execution

### Takeaway
Temporal Cloud's multi-region namespaces are active/standby with asynchronous replication, not true active-active. Temporal publishes an RPO under 1 minute (P95 replication lag) and a 20-minute RTO under a 99.99% SLA, and it has a conflict-resolution step for divergence after failover.

### Cited Findings
- A multi-region namespace is a single logical endpoint over two regions, one active and one standby. History events replicate asynchronously to the standby — [Temporal Cloud HA docs](https://docs.temporal.io/cloud/high-availability.md); [multi-region namespace](https://docs.temporal.io/evaluate/development-production-features/multi-region-namespace)
- RPO is under one minute, with a target P95 replication lag under 1 minute. RTO is 20 minutes. The SLA is 99.99% — [Temporal RPO/RTO docs](https://docs.temporal.io/cloud/rpo-rto)
- If the regions are not in sync at failover, "Temporal's conflict resolution process reconciles discrepancies" — [Temporal blog: HA and DR](https://temporal.io/blog/high-availability-and-disaster-recovery-with-temporal-cloud)
- Multi-region namespaces became available in Temporal Cloud in June 2024 — [Temporal changelog](https://temporal.io/changelog/new-feature-multi-region-namespaces-in-temporal-cloud)
- Restate replicates the log by quorum across nodes in a cluster — [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)

### Inferences
- For a Postgres-first engine, the comparable baseline is Postgres physical replication to another region (async, RPO of seconds) plus failover. Temporal's sub-minute RPO and 20-minute RTO are not a high bar to match operationally.
- True active-active needs per-workflow ownership, a "home region", which is how Temporal's namespace-level active/standby works, or conflict-free history merge. Neither is native to Postgres.

### Gaps
- I did not fetch Restate's multi-region or geo-replication claims. I also found no RPO/RTO claims for DBOS, Inngest or Hatchet, and no detail on Temporal's conflict-resolution algorithm.

## Latency: per-step overhead, caching, eager start and co-location

### Takeaway
Library-in-process engines (DBOS) claim 1–2 ms per step, which is one Postgres write. Restate claims about 5 ms p50 per step at low load and about 10 ms under load, with 3-way replication. Temporal's server-dispatch model costs tens of ms per hop. It reduces this with sticky worker caches, local activities and Eager Workflow Start, which measured p50 16.7 ms against 29.3 ms.

### Cited Findings
- DBOS: about 1–2 ms per step checkpoint, against "tens to hundreds of ms" per Temporal step dispatch (vendor comparison) — [DBOS docs](https://docs.dbos.dev/explanations/comparing-temporal)
- Restate: single step p50 5 ms at low load; median per step about 10 ms at high load. Under low load with 0 intermediate steps, p50 is 5 ms, p90 34 ms and p99 54 ms — [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- Temporal sticky execution sends workflow tasks to the worker that already caches the workflow state. That worker also polls a worker-specific sticky queue, which avoids a full replay of history — [Temporal: Sticky Execution](https://docs.temporal.io/sticky-execution)
- Eager Workflow Start schedules the first workflow task straight to a local worker and skips the Matching round-trip. This saves about 30–50 ms per start; measured p50 was 16.7 ms eager against 29.3 ms non-eager (43%). The starter and the worker must share a client in the same process. It is recommended for short workflows with local activities that run near the server — [Temporal docs: Eager Workflow Start](https://docs.temporal.io/design-patterns/eager-workflow-start); [Temporal blog](https://temporal.io/blog/improving-latency-with-eager-workflow-start)
- DBOS reports 15–100 ms notification latency at max throughput with batched NOTIFY. Batching trades latency for throughput — [DBOS blog](https://dbos.dev/blog/postgres-listen-notify-scalability)
- Hatchet buffers writes and flushes every 10 ms, which puts a floor under per-operation latency in exchange for throughput — [Show HN](https://news.ycombinator.com/item?id=43572733)

### Inferences
- The latency floor for a Postgres-first engine is one synchronous commit, about 1–2 ms on a co-located instance. Running the next step inline in the same worker after the checkpoint, as DBOS does and as Temporal's eager start does, avoids a queue hop that costs 10–50 ms. "Run the next step locally after commit" is the largest single latency lever.
- Both buffered writes and batched notifications add 10–100 ms. An adaptive design is better than a fixed one: write synchronously at low load and batch at high load.

### Gaps
- I found no independent measurement of per-step latency across engines in a comparable setup.

## Benchmarking methodology: is there a standard benchmark for durable execution?

### Takeaway
No. Each vendor runs its own harness and workload: Temporal uses omes, DBOS has its durable-execution-benchmark, and Restate uses its own setup. Those workloads (no-op steps, different step counts, different hardware and replication) are not comparable. The closest independent effort covers Postgres job queues, not durable workflows.

### Cited Findings
- Omes is Temporal's load generator, used "primarily ... by the Temporal team to benchmark features and situations" — [temporalio/omes on pkg.go.dev](https://beta.pkg.go.dev/github.com/temporalio/omes)
- Manetu found that omes and Maru "offer a very flexible way to define various loads on Temporal, but are lacking in gathering and reporting analytics". It wrote its own tool focused on the workflow-creation rate — [manetu/temporal-benchmark](https://github.com/manetu/temporal-benchmark)
- DBOS publishes an open benchmark repository — [dbos-inc/durable-execution-benchmark](https://github.com/dbos-inc/durable-execution-benchmark)
- Vendor setups differ:
  - DBOS: no-op workflows, 96-vCPU single-node RDS, one row per transaction — [DBOS blog](https://www.dbos.dev/blog/benchmarking-workflow-execution-scalability-on-postgres)
  - Restate: 9-step workflows on a 3-node replicated c6id.8xlarge cluster — [Restate blog](https://restate.dev/blog/building-a-modern-durable-execution-engine-from-first-principles)
- hardbyte's harness is independent and covers queues only. It runs a closed loop with steady, sustained-pressure and chaos scenarios, uses public APIs only, and measures long-horizon bloat. It shows how short-burst vendor numbers can hide timeouts and anti-scaling under sustained load — [hardbyte benchmark](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

### Inferences
- A credible durable-execution benchmark would fix:
  - steps per workflow
  - payload size
  - replication and durability level (fsync, quorum)
  - hardware
  - open-loop versus closed-loop load
  - run length long enough to expose vacuum and bloat
  - recovery time after a crash
- Publishing such a benchmark would be an opportunity in its own right.

### Gaps
- I found no cross-engine durable-execution bake-off that a third party has reproduced. I also found no published critique of the Restate or DBOS benchmarks beyond general vendor-number caveats.
