# Actor/shard ownership prior art: Temporal, Durable Functions + Netherite, Orleans, Akka, Rivet

Scope: how each system assigns single ownership of shards or keys, fences stale owners, keeps durable truth, delivers cross-owner messages, serves global listing, and handles failover. The consumer is Autumn Harvest, which is weighing (1) Postgres shard leases with an epoch, (2) a Postgres event log with a local SQLite cache, and (3) per-entity user SQLite. Research date: 2026-10-19. Source labels: "primary" means vendor docs, source code or the authors' paper. "Secondary" means forums, mirrors or third-party write-ups. Anything not marked older is current as of the 2026 doc revisions cited.

## Temporal (history shards, range_id fencing, matching, visibility)

### Takeaway
Temporal uses the design closest to option (1). It has a fixed shard count chosen at cluster creation. Ringpop membership decides which history host owns each shard. A monotonically increasing `RangeID` on the shard row fences writers: every persistence write carries it, and a stale owner gets `ShardOwnershipLostError` and unloads the shard. Global listing comes from a separate, asynchronously updated, eventually consistent visibility store.

### Cited Findings
**Ownership and shard map**
- "The total number of History Shards is fixed at cluster creation and cannot be changed later." Each history instance "owns a subset of History Shards." — [temporalio/temporal docs/architecture/history-service.md (primary)](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- Shard ownership is coordinated by a `ShardController` that "uses a membership protocol (implemented via the Ringpop library)." The in-memory shard metadata includes `RangeID`, "a monotonically increasing generation number used for fencing." — [history-service.md (primary)](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- The Frontend routes by Workflow Id: a request "must have a Workflow Id, which is hashed for routing purposes." Services learn of each other "through a membership protocol" (Ringpop), and the Frontend reads the hash rings. — [Temporal Server docs (primary)](https://docs.temporal.io/temporal-service/temporal-server)
- Sizing: "Temporal recommends starting at a ratio of 1 History Service process for every 500 History Shards." The service "has operated successfully using anywhere from 1 to 128K History Shards." The history process count can range from 1 up to the shard count. "Each History Shard maps to a single persistence partition." — [Temporal Server docs (primary)](https://docs.temporal.io/temporal-service/temporal-server)
- On Cassandra, history shards "map 1:1 to partitions of the `executions` table." — [history-service.md (primary)](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)
- Background on why per-shard throughput is bounded by database operation latency, so too few shards raise history latency without raising storage latency (secondary, 2021, older). — [mikhail.io, "Choosing the Number of Shards in Temporal History Service" (secondary, 2021)](https://mikhail.io/2021/05/choose-the-number-of-shards-in-temporal-history-service/)

**Fencing mechanics, from `service/history/shard/context_impl.go` (primary, main branch, fetched 2026-10)**
- Each write path, including `CreateWorkflowExecution`, `UpdateWorkflowExecution`, `ConflictResolveWorkflowExecution`, `SetWorkflowExecution` and adding tasks, stamps the observed range on the request (`request.RangeID = s.getRangeIDLocked()`). The persistence write is therefore conditional on the shard's current range. — [context_impl.go (primary)](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)
- `renewRangeLocked` is the only place the range advances. It first drains in-flight task requests. The comment explains: "requests are conditioned on rangeID, if rangeID is updated before draining them, those requests could fail." It then does `RangeId++` and calls `UpdateShard` with `PreviousRangeID`, which is a compare-and-swap on the shard row. It also counts `StolenSinceRenew` when stealing. — [context_impl.go (primary)](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)
- `acquireShard` calls `renewRangeLocked(true)` on every acquisition attempt, so every acquisition produces a new range ID. The task-key manager is constructed with a callback that calls `renewRangeLocked(false)`. This is consistent with task IDs being allocated from a range block, but the arithmetic, `rangeSizeBits`, is not in this file. — [context_impl.go (primary)](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)
- On `ShardOwnershipLostError` from a read or write, the context transitions to stop (`stopReasonOwnershipLost`), cancels its lifecycle, and calls back so the controller drops the shard. Acquisition does not retry on this error. Other acquisition errors retry for up to 5 minutes (`WithExpirationInterval(5 * time.Minute)`). An error on a write whose request range is older than the current range is returned without changing shard state. Other ambiguous write errors trigger re-acquisition, which yields a new RangeID. — [context_impl.go (primary)](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)

**Per-shard queues and the outbox pattern**
- At startup, "a per-shard `QueueProcessor` is started for each of the queue types." Each processor checkpoints its ack level. Transfer tasks are immediately executable and issue an RPC to Matching. Timer tasks fire at a stored time, and a fired user timer creates a Transfer task. Visibility tasks update "workflow metadata records in the visibility storage." The doc names this the Transactional Outbox pattern. — [history-service.md (primary)](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)

**Durable truth and cache**
- Mutable State is "persisted as a single row" and loaded into `MutableStateImpl`. "Mutable State for recently accessed workflow executions are cached in memory." If persisting dirty mutable state fails, history reloads from persistence. Durable truth is therefore both the history event rows and the mutable-state row, with an owner-side cache. — [history-service.md (primary)](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)

**Matching service**
- Matching hosts "user-facing Task Queues for Task dispatching" and "can scale internally by having multiple instances." — [Temporal Server docs (primary)](https://docs.temporal.io/temporal-service/temporal-server)
- "By default each Task Queue has 4 partitions," and "each task is assigned to a random partition." Sync match delivers a task to a waiting worker without writing it to the database. Otherwise tasks are persisted, and the backlog (async match) is served first. Task queues are "lightweight, dynamically allocated" and need no registration. — [Task Queue docs (primary)](https://docs.temporal.io/task-queue)

**Visibility (global listing and search)**
- "Visibility is a search index that Temporal updates asynchronously." The docs say: "Treat Visibility as eventually consistent and avoid depending on a fixed or guaranteed delay." For the current state of one execution, use `DescribeWorkflowExecution`. Temporal Cloud publishes no visibility-lag SLA. — [Visibility docs (primary)](https://docs.temporal.io/visibility)
- Backends: MySQL 8.0.17+ and PostgreSQL 12+ (Server v1.20 and later), Elasticsearch 7 and 8, and OpenSearch 2+ (Server v1.30.1 and later). Dual Visibility, from v1.21, migrates between stores. — [Visibility docs (primary)](https://docs.temporal.io/visibility)

**Rebalancing and churn**
- Changing the history node count reassigns shards and forces in-memory state to be rebuilt. Temporal staff say latency can rise during rebalancing until the cluster settles. — [community.temporal.io, rolling-update thread (secondary)](https://community.temporal.io/t/will-the-history-service-get-unsteady-in-rolling-update-of-deployment/6580)
- Staff describe shard movement as normal during scaling and restarts. Shard-created and shard-closed metric spikes can coincide with `ResourceExhausted` / `BusyWorkflow` errors. — [community.temporal.io, shard operations thread (secondary)](https://community.temporal.io/t/how-to-investigate-or-solve-occasional-shard-operations-causing-resourceexhausted-errors/15857)
- One forum post shows a shard's range ID advancing during movement, with queue processors shutting down and timer ack-level update errors. Staff attribute this to shard movement. — [community.temporal.io, matching QPS thread (secondary)](https://community.temporal.io/t/matching-service-high-qps-on-persistence/2628)
- One write-up measured shard-lock p95 near 50 ms against a target under 5 ms and blamed too few shards (secondary). — [Temporal on DEV, "Scaling Temporal: the basics" (secondary)](https://dev.to/temporalio/scaling-temporal-the-basics-31l5)

### Inferences
- Temporal fences in two layers. Ringpop gives a soft, best-effort view of who should own a shard. The `range_id` CAS on the shard row, plus `RangeID` carried on every write, is the hard guarantee. Two hosts may both believe they own a shard during membership churn, but only the one holding the latest range can commit. This maps directly onto proposal (1): a Postgres `shards(shard_id, owner, epoch)` row and `WHERE epoch = $mine` on every write. In Postgres, one transaction can check the epoch and append events, so the check need not be a separate round trip.
- The "drain in-flight requests before bumping the range" rule is a subtle requirement worth copying. Without it, an owner renewing its own range fails its own in-flight writes.
- Temporal does not route task *claims* through the shard owner. Matching is a separate partitioned service, and history pushes tasks to it via the transfer-queue outbox. A Harvest design that routes "every decision" to the shard owner still needs an outbox-like path to workers.
- Visibility is deliberately decoupled and eventually consistent. Temporal accepts list/search staleness to keep the shard write path narrow.

### Gaps
- No primary number found for shard-movement latency, meaning the time from host loss to the shard serving on a new owner. Ringpop failure-detection timeouts were not retrieved.
- The `task_queues` table also carries a `range_id` used to fence Matching task-queue partition owners. That is general knowledge of the Temporal schema and was not verified in this session, so do not cite it without a check.
- No public Temporal postmortem specifically about shard ownership churn or split brain was found.
- Sticky task queue mechanics and their schedule-to-start timeout were not covered on the fetched page.

## Azure Durable Functions / Durable Entities on Netherite

### Takeaway
Netherite has a fixed partition count and storage leases that keep each partition on at most one host. Each partition is event-sourced into a FASTER commit log plus checkpoints on Azure Page Blobs. Partitions talk over ordered Event Hubs queues, with an outbox and a per-sender deduplication vector, which together give exactly-once internal messaging. Microsoft ends Durable Functions support for Netherite on 31 March 2028 and recommends the managed Durable Task Scheduler instead.

### Cited Findings
**Papers**
- "Netherite: Efficient Execution of Serverless Workflows," Burckhardt, Chandramouli, Gillum, Justo, Kallas, McMahon, Meiklejohn, Zhu, PVLDB 15(8):1591–1604, 2022. An extended version appeared in the VLDB Journal "Best of VLDB 2022" issue. — [PVLDB PDF (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf); [MSR extended version (primary)](https://www.microsoft.com/en-us/research/publication/netherite-efficient-execution-of-serverless-workflows-extended-journal-version/)
- An earlier arXiv version, "Serverless Workflows with Durable Functions and Netherite," was written in 2020 (older) and uses 32 partitions by default. — [arXiv 2103.00033 (primary, 2020/2021)](https://arxiv.org/abs/2103.00033)

**Ownership and fencing**
- "To ensure that the same partition is running on at most one host, we use storage leases." Partitions are the unit of distribution and recovery. — [PVLDB §5 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- The older Azure Storage backend is described the same way: "Instance queues are partitioned statically by computing a hash of the key... a single instance worker to process work items. This is enforced by using storage leases." — [PVLDB §3 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf); the current docs say the Azure Storage provider uses "blob leases to manage partitions" — [Microsoft Learn storage providers (primary, 2026-04)](https://learn.microsoft.com/en-us/azure/durable-task/common/durable-task-storage-providers)
- The partition count is fixed. The default is 12, and it "cannot be changed after the task hub has been created." — [durabletask-netherite README (primary)](https://github.com/microsoft/durabletask-netherite). Conflict: the arXiv paper's evaluation uses "32 by default" — [arXiv HTML (primary, 2020)](https://arxiv.org/html/2103.00033v1). The docs say Netherite "supports up to 32 data partitions with Event Hubs Basic and Standard SKUs." Maximum orchestration and entity scale-out is 32 nodes. — [Microsoft Learn (primary)](https://learn.microsoft.com/en-us/azure/durable-task/common/durable-task-storage-providers)

**Durable truth**
- Each partition is event-sourced: "a dual persistence model using a combination of a commit log and checkpoints." The partition state is a deterministic function of the processed events. FasterLog persists the commit log, and FasterKV hosts partition state "on top of Azure PageBlob Storage." — [PVLDB §5.2 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- FasterKV "allows the partition state to be larger than memory," "acts as a LRU cache for instance states," supports incremental asynchronous checkpoints, "allows instant recovery (lazy loading)," and "can index the instances, to support analytical DF queries over state." — [PVLDB §5.2 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- Partition state components are an instance map, the input queue position plus a deduplication vector, per-instance inbound buffers, an outbox, and pending tasks. A `StepCompleted` event atomically updates instance state, consumes messages, adds outgoing messages and adds tasks. "The transition commits at the moment this event is persisted in the log." — [PVLDB §5.2.1–5.2.3 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- Group commit: "when under load, a partition can group commit, i.e. commit multiple events at once using a single storage access." — [PVLDB (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)

**Exactly-once cross-partition messaging**
- "Each partition has an ordered persistent input queue, provided by EventHubs... An input queue can receive messages even when its corresponding partition is not loaded on any host, for example, while it is being moved." — [PVLDB §5 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- Messages to other partitions "are first stored in the outbox. Only after the producing event has been persisted in the commit log are they actually sent, and only after the send has been acknowledged are they removed from the outbox." A partition may re-send after a crash. "Receivers use a deduplication vector, which records the position of the last-processed incoming message from each remote partition." In-order Event Hubs delivery plus a checkpointed receive position "is sufficient to guarantee reliable, ordered delivery." — [PVLDB §5.2.3 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- "Each individual workflow step commits like a serializable transaction. This implies that messages appear to be processed exactly once." External effects remain at-least-once. — [PVLDB §1, §3.3 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- Speculation: local pipelining executes causally dependent steps before the earlier step is persisted, but never commits them out of order. The arXiv version also describes global speculation with recovery broadcasts that make receivers rewind. — [PVLDB §1.1 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf); [arXiv HTML (primary, 2020)](https://arxiv.org/html/2103.00033v1)

**Failover and rebalancing**
- Continuous persistence "allows easy movement of a partition to a different host, as it can be quickly shut down and then recovered on the destination host." On recovery, a partition resumes at the stored input queue position. Instant recovery "is particularly important... because unlike most databases, partitions must frequently move between hosts." — [PVLDB §5 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)
- In the arXiv scale-out experiment, throughput reached peak "after around 7 seconds" after a rebalance at 70 s. On 4 nodes, 24 of 32 partitions moved; on 8 nodes, 28 of 32. — [arXiv HTML (primary, 2020)](https://arxiv.org/html/2103.00033v1)

**Benchmarks (PVLDB)**
- Throughput gains over the original DF engine were up to x12.2 for Hello5, x7.8 for the Bank application, x18.6 for Word Count, and x2.35 on a fourth benchmark. Storage accesses fell x4.4–x71.6. Storage triggers had x1k–x10k higher latency than Netherite. — [PVLDB §6 (primary)](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)

**Global query and status**
- Netherite "supports almost all of the DT and DF APIs." Its known gaps are that queries and purges run through the function app and rewind is unavailable. The current version is 2.1.0. — [durabletask-netherite README (primary)](https://github.com/microsoft/durabletask-netherite)
- "Support for using the Netherite storage backend with Durable Functions ends March 31, 2028." Microsoft recommends the Durable Task Scheduler, which the comparison table lists as GA, "Very high" throughput, and "recommended." Data cannot be migrated between providers. — [Microsoft Learn storage providers (primary, updated 2026-06)](https://learn.microsoft.com/en-us/azure/durable-task/common/durable-task-storage-providers); [Azure update AZ489009 (secondary mirror)](https://mc.merill.net/message/AZ489009)

### Inferences
- Netherite is the closest published analogue to combining proposals (1) and (2). It has a fixed partition set, a lease for single ownership, a partition-local event log as truth with an in-memory/LRU state cache, and a transactional outbox plus per-source high-water-mark dedup for cross-partition messages. Harvest could use a per-(source shard → destination shard) sequence high-water mark stored in the destination shard's state, committed in the same transaction as consumption.
- Netherite's fencing relies on Azure blob leases. The fetched text does not say whether every log append is conditioned on the lease. If Harvest uses Postgres, an epoch check inside the append transaction is a stronger and simpler form of the same guarantee.
- Netherite's retirement, with Microsoft moving to a managed scheduler, suggests the bring-your-own-storage, high-throughput design carried notable operational cost. That is an inference; Microsoft gives no stated reason.

### Gaps
- Exact blob-lease timings, the lease renewal period, and whether commit-log writes are fenced by the lease were not found in the readable text.
- No measured failover time after a host crash, as opposed to planned movement, was found.
- How instance list/query APIs are served across partitions on Netherite, for example fan-out to every partition, was not documented in the sources fetched.
- The Durable Task Scheduler's internal ownership and fencing design was not researched.

## Microsoft Orleans (virtual actors, grain directory)

### Takeaway
Orleans maps grain identity to location through a grain directory, not a fixed shard map. The default in-cluster directory is an eventually consistent DHT that "allows occasional duplicate activations during cluster instability." Orleans 10 adds a strongly consistent in-cluster directory, still in preview, that prevents duplicates using versioned range locks. External directories on Redis, Azure Table or ADO.NET are also available, including PostgreSQL. Grain state persistence is a separate, pluggable provider.

### Cited Findings
- "At most one activation of a grain exists at any point in time... When a grain activates in the cluster, it registers itself in the grain directory." The directory maps grain identity to the silo of its current activation. — [Orleans grain directory docs (primary, 2026-01)](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)
- Directory options in Orleans 9/10: Distributed In-Cluster, the default ("Eventually consistent, partitioned across silos using a distributed hash table. Allows occasional duplicate activations during cluster instability"); Strongly-Consistent In-Cluster ("versioned range locks. Prevents duplicate activations but requires more coordination"); ADO.NET (SQL Server, PostgreSQL, MySQL, Oracle); Azure Table; and Redis. — [grain directory docs (primary)](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)
- The strongly consistent directory "is available starting with Orleans 10.0. It is currently in preview." It uses "a distributed hash table with virtual nodes (similar to Amazon Dynamo and Apache Cassandra)." Each silo manages 30 virtual nodes. It has normal and view-change phases and "Recovers automatically when silos crash without completing handoff." — [grain directory docs (primary)](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)
- Microsoft's guidance is to start with the default. It is "self-sufficient... and has been used successfully in production since the beginning." For "a stronger single-activation guarantee," Microsoft suggests a storage-backed directory such as Redis, adopted for a few grain types first. External directories also let registrations "persist across full cluster restarts." — [grain directory docs (primary)](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)
- Pluggable directories date from Orleans 3.2.0 (older). — [grain directory docs, 3.x–8.x pivot (primary)](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)
- The Orleans v10.2.0 release notes, about four months before the fetch (2026), list many directory reliability fixes: "Fixed distributed directory recovery handoff for grains activating during ownership transfer" (#10082), "Fixed LocalGrainDirectory membership reconciliation using snapshot-based processing" (#10086), membership-version-aware stale cleanup (#10087), "Fixed shutdown activations leaving stale grain directory entries" (#10206), stuck deactivation recovery (#10046), and bounded retries after directory activation failures (#10094, breaking). — [newreleases.io mirror of dotnet/orleans v10.2.0 (secondary mirror of primary release notes)](https://newreleases.io/project/github/dotnet/orleans/release/v10.2.0)

### Inferences
- With the default directory, Orleans gives up strict single activation to gain availability, and the docs say so. The fence is not in the directory at all. A grain that needs safety must rely on storage-level optimistic concurrency, the ETag on grain state writes, to reject the losing duplicate's writes. Like Temporal, this makes the storage CAS the real fence.
- The v10.x stream of handoff and race fixes shows how hard a correct ownership handoff is in a DHT directory under membership change, even after more than a decade in production.
- Orleans' ADO.NET/PostgreSQL directory is evidence that a database-backed key→owner directory is a supported production pattern. It is the per-key alternative to Harvest's per-shard lease.

### Gaps
- The grain-state ETag / `InconsistentStateException` behaviour was not fetched this session. It is general knowledge of Orleans persistence, so verify before citing.
- No primary Orleans postmortem or GitHub issue quantifying duplicate-activation frequency was found.
- Orleans has no built-in global listing or search of grains. No primary statement was fetched, so this is an absence of evidence.
- Membership-protocol failure-detection timings (the silo membership table and probes) were not retrieved.

## Akka Cluster Sharding + Akka Persistence

### Takeaway
Akka hashes entity IDs into a fixed number of shards (default 1000, about 10x the maximum node count). A shard coordinator allocates shards to nodes, and its state is replicated with Distributed Data. The single-writer property for a persistence ID depends on membership correctness, which the Split Brain Resolver (SBR) provides. Without SBR, two partitions can each run the same sharded persistent entity and corrupt its journal. Default messaging is at-most-once. Stronger guarantees need Reliable Delivery or persistence plus deduplication.

### Cited Findings
**Sharding**
- By default the shard ID is "the absolute value of the `hashCode` of the entity identifier modulo the total number of shards." `number-of-shards` defaults to 1000 and must match on all nodes. "The number of shards should be a factor ten greater than the planned maximum number of cluster nodes." — [Akka Cluster Sharding docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- In the default `ddata` mode, "the state of the `ShardCoordinator` is replicated across the cluster but is not stored to disk." The `persistence` mode is deprecated. — [Cluster Sharding docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- `LeastShardAllocationStrategy` moves shards from the most-loaded regions, with per-round limits. `ExternalShardAllocationStrategy` allows explicit placement. During handoff or passivation the shard sends a local stop message, and messages arriving during shutdown "are buffered by the shard and delivered to the new incarnation." "Remember entities" restarts entities after a rebalance or crash and disables automatic passivation. Its store is ddata (LMDB-durable by default) or event-sourced. — [Cluster Sharding docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- "Make sure to not use a Cluster downing strategy that may split the cluster into several separate clusters," because multiple shards and entities could then start, one in each partition. — [Cluster Sharding docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)

**Persistence (durable truth)**
- "For a particular `persistenceId` only one persistent actor instance should be active at one time." The docs recommend Cluster Sharding to enforce this. Concurrent writers can journal different events under the same sequence number, and the replay filter (`repair-by-discard-old`, `fail`, `warn`, `off`) exists to handle that corruption. — [Akka Persistence docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)
- "An event sourced actor is automatically recovered on start and on restart by replaying journaled events." Snapshots can "dramatically reduce recovery times." Commands are stashed during recovery. — [Akka Persistence docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)

**Split brain**
- With Persistence and no correct downing, "two instances of a persistent actor with the same `persistenceId` are running and writing concurrently," with possibly fatal consequences on replay. — [Split Brain Resolver docs (primary)](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html)
- SBR strategies are keep-majority (the default), static-quorum, keep-oldest, down-all (not recommended above 10 nodes), and lease-majority. Lease-majority uses a distributed lease, for example a Kubernetes CRD lease with `release-after = 40s`, and delays the minority's lease attempt by `acquire-lease-delay-for-minority = 2s`. — [SBR docs (primary)](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html)
- Timing: `stable-after` defaults to 20 s (recommended about 7 s for 5 nodes, 10 s for 10, 20 s for 100, 30 s for 1000). `down-removal-margin` defaults to `stable-after`. With defaults, failover of a singleton or sharded entity takes "around 45 seconds": about 5 s of detection, 20 s stable-after and 20 s removal margin. About 25 s is achievable at 10 nodes. — [SBR docs (primary)](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html)

**Messaging**
- Akka's default is "at-most-once delivery, i.e. no guaranteed delivery." Ordering holds per sender-receiver pair with direct tell, and "is not transitive." At-least-once needs ACK-RETRY. The Reliable Delivery feature provides "an ACK-RETRY protocol with business-level acknowledgements and de-duplication using identifiers." — [Akka Message Delivery Reliability (primary)](https://doc.akka.io/libraries/akka-core/current/general/message-delivery-reliability.html)
- Persistence side effects run "on an at-most-once basis and will not be executed if the persist fails." — [Akka Persistence docs (primary)](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)

### Inferences
- Akka's single-writer guarantee comes from membership, through downing and SBR, plus wall-clock margins such as `down-removal-margin`. It does not come from a storage-level fence. The journal replay filter is a cleanup tool for when that fails. This is the cautionary contrast with Temporal's range_id CAS: a Harvest lease must be checked at write time in Postgres, not only trusted from a heartbeat.
- The default 45 s failover gives a useful comparison point for Harvest's lease timeout.
- There is no built-in global entity listing. Akka Projections over the journal's event stream is the usual route, but that was not researched here.

### Gaps
- Whether specific journal plugins, such as akka-persistence-r2dbc, enforce a uniqueness constraint on `(persistence_id, seq_nr)` that would act as a storage fence was not verified.
- No public Akka split-brain postmortem was retrieved.
- Akka's licence change to BSL in 2022 (older) is general knowledge and was not re-verified.

## Rivet Actors (rivet.dev)

### Takeaway
Rivet is an Apache-2.0, self-hostable orchestrator for keyed, long-lived "Actors," written in Rust. Each actor has in-memory durable state and a per-actor SQLite database. The SQLite pages are persisted through a custom VFS to a replicated "hot tier" on FoundationDB, Postgres or RocksDB, with cold chunks tiered to S3. Rivet says actors are "single-writer by design" with a structural dual-write guard, but it does not publicly document the fencing mechanism.

### Cited Findings
- Actors are "the primitive for agent orchestration, realtime, and per-tenant databases." They are "addressed by key and scheduled by the control plane." They sleep when idle and wake "with state intact." Placement uses Rivet Cloud or a self-hosted control plane. — [rivet.dev/docs/actors (primary, 2026)](https://rivet.dev/docs/actors)
- The engine is a single Rust binary. Its components are Pegboard (actor scheduling and networking), Gasoline (a durable execution engine), Guard (a routing proxy) and Epoxy (a "Multi-region KV store (EPaxos)"). Persistence uses Postgres or FoundationDB, with tiering to S3. Each actor gets "a SQLite database or a POSIX filesystem, tiered to S3." Rivet cites a 12 ms cold start and says a failed worker's actors are rescheduled elsewhere "with their state intact." The licence is Apache 2.0, and the repo has about 6.3k stars. — [github.com/rivet-dev/rivet (primary)](https://github.com/rivet-dev/rivet)
- SQLite for Rivet Actors launched on 2026-02-26 as "one database per agent, tenant, or document." — [Rivet changelog (primary)](https://rivet.dev/changelog/2026-02-26-sqlite-for-rivet-actors/). A search summary says SQLite runs in-process and a custom VFS persists writes to FoundationDB or Postgres, and that FoundationDB is recommended for production but needs an enterprise licence. These details come from a search snippet of [Rivet SQLite docs](https://rivet.dev/actors/docs/sqlite) and [Rivet FoundationDB self-hosting docs](https://rivet.dev/docs/self-hosting/foundationdb) and were not fetched (secondary).
- The July 2026 storage-engine post describes 4 KiB pages grouped into 256 KiB chunks. The hot tier "can run on RocksDB, Postgres, or FoundationDB." Commits "must return in low milliseconds while still maintaining at least triple redundancy." S3 is kept off the write path because PUTs take about 30–45 ms. Chunks untouched for 7 days are offloaded to S3 and rehydrated on read. Point-in-time recovery is optional. Actors "must be able to start on any machine with no data on it." Actors are "single-writer by design," "there can only be one instance of a given Actor running at a time," and "a guard preventing dual writes" is "enforced structurally." — [Rivet blog, 2026-07-31 (primary)](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/)
- Durable `c.state` is saved automatically, but "Rapid mutations are batched into a single write on a throttle (`stateSaveInterval`, default 1 second)." State is also flushed on sleep or shutdown. `c.saveState({ immediate: true })` waits for the write. — [Rivet persistence docs (primary)](https://rivet.dev/docs/actors/persistence)
- The persistence docs say "`c.db` is a SQLite database scoped to each actor and stored on disk... queries run locally with no network round trips." This conflicts with the July 2026 "zero disk" post, where pages live in a remote hot tier and are fetched on demand with caching. The docs may describe the developer mental model or an older or local mode. — [Rivet persistence docs (primary)](https://rivet.dev/docs/actors/persistence); [Rivet blog (primary)](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/)

### Inferences
- Rivet is an existence proof of proposal (3), per-entity SQLite at scale. Its durability comes from a replicated KV underneath a VFS, not from local disk. Local disk is only a cache, so actors can move to empty machines. Proposal (3) with SQLite files on worker-local disk would not match this model.
- The default 1 s state-save throttle means an un-awaited `c.state` mutation can be lost on crash. That is a weaker durability default than a workflow engine's commit-before-ack.
- Rivet does not describe how it fences a stale actor instance, for example a generation number checked by the VFS on each page write. Its claim that the guard is "enforced structurally" is unverified.

### Gaps
- No public documentation was found for Rivet's fencing or generation mechanism, actor-to-node assignment algorithm, failover timing, or any global actor list/search API.
- How the Low-Level KV Storage API relates to the SQLite VFS was not retrieved.
- No Rivet postmortems or third-party evaluations were found.

## Cross-system comparison

### Takeaway
Every system that gives a strong single-writer guarantee enforces it at the storage write. Temporal uses a range_id CAS on every write, Netherite uses storage leases, Orleans' safe mode relies on storage ETags or a consistent directory, and Rivet claims a structural guard. Membership or heartbeats alone, as in Akka without SBR or Orleans' default directory, allow duplicates during instability. Global listing is always a separate, eventually consistent index (Temporal), a fan-out or index over partitions (Netherite), or left to the application (Orleans, Akka).

### Cited Findings
| Dimension | Temporal | DF + Netherite | Orleans | Akka Sharding + Persistence | Rivet |
|---|---|---|---|---|---|
| Key → owner map | Fixed shard count, WorkflowId hash; Ringpop ring assigns shards to hosts ([docs](https://docs.temporal.io/temporal-service/temporal-server)) | Fixed partitions (12 default in README, 32 in paper), hash of instance ID ([README](https://github.com/microsoft/durabletask-netherite); [PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | Per-grain directory entry; DHT by default, 30 vnodes per silo in strong mode ([docs](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)) | hashCode mod `number-of-shards` (default 1000); coordinator allocates shards ([docs](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)) | Per-actor key, placed by the Pegboard control plane ([repo](https://github.com/rivet-dev/rivet)) |
| Acquisition | Membership plus `UpdateShard` CAS bumping RangeID ([source](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)) | Storage lease per partition ([PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | Directory registration on activation ([docs](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)) | Coordinator allocation, coordinator state in ddata ([docs](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)) | Control-plane scheduling; mechanism undocumented |
| Stale-owner fence | RangeID on every write → `ShardOwnershipLostError` → unload ([source](https://github.com/temporalio/temporal/blob/main/service/history/shard/context_impl.go)) | Lease ("at most one host") ([PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | None in the default directory (duplicates possible); range locks in the preview strong directory ([docs](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory)) | SBR / downing; journal replay filter as cleanup ([SBR](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html); [Persistence](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)) | "Structural" dual-write guard, unspecified ([blog](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/)) |
| Durable truth | History events plus mutable-state row in DB; owner-side mutable-state cache ([arch doc](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)) | Per-partition commit log plus FASTER checkpoints on Page Blobs ([PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | Grain state provider (pluggable) | Event journal plus snapshots ([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html)) | Per-actor SQLite via VFS on replicated hot tier plus S3 ([blog](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/)) |
| Cross-owner messaging | Transfer-queue outbox → Matching RPC ([arch doc](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md)) | Outbox plus ordered Event Hubs plus per-source dedup vector = exactly-once ([PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | Not covered this session | At-most-once by default; Reliable Delivery for ack/retry/dedup ([docs](https://doc.akka.io/libraries/akka-core/current/general/message-delivery-reliability.html)) | Durable queues per actor ([docs](https://rivet.dev/docs/actors)); semantics undocumented |
| Global listing | Visibility store, async, eventually consistent ([docs](https://docs.temporal.io/visibility)) | DF query APIs via the function app; FasterKV indexes instances ([README](https://github.com/microsoft/durabletask-netherite); [PVLDB](https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf)) | None found | None built in (Projections not researched) | Inspector per actor; no list API found ([docs](https://rivet.dev/docs/actors)) |
| Failover timing | Not published; rebalance raises latency (forum) ([forum](https://community.temporal.io/t/will-the-history-service-get-unsteady-in-rolling-update-of-deployment/6580)) | Peak throughput about 7 s after a planned rebalance ([arXiv](https://arxiv.org/html/2103.00033v1)) | Not found | About 45 s with defaults, about 25 s tuned ([SBR](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html)) | Claims "no delay" on crash or migrate ([blog](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/)) |
| Status (2026) | Active | Support ends 2028-03-31; DTS recommended ([MS Learn](https://learn.microsoft.com/en-us/azure/durable-task/common/durable-task-storage-providers)) | v10.x; strong directory in preview | Active | Active; SQLite since 2026-02 |

### Inferences
- **For proposal (1), shard leases with an epoch:** Temporal is the direct template. Use a fixed shard count sized well above the expected owner count (Temporal: about 500 shards per process; Akka: 10x nodes). Bump the epoch with a CAS on acquisition. Carry the epoch on every write and reject a stale write in the same Postgres transaction. Drain in-flight writes before an owner renews its own epoch. Have the owner unload on a lost-ownership error instead of retrying. Membership or heartbeats only decide who *should* own a shard; the write-time check decides who *can* write.
- **For cross-shard signals, child workflows and similar:** Netherite's outbox plus per-source sequence dedup vector is the published exactly-once recipe. Temporal's transfer queue is the same outbox idea applied between history and matching.
- **For proposal (2), a Postgres log plus local SQLite cache:** Temporal's in-memory mutable-state cache and Netherite's FasterKV LRU cache both treat the owner-local copy as disposable and rebuild it from the durable log or checkpoint on movement. Movement cost is therefore cache warm-up, which Netherite cuts with lazy "instant recovery."
- **For proposal (3), per-entity user SQLite:** Rivet is the only surveyed system doing this, and it stores SQLite pages in a replicated KV through a VFS, not on local disk. Its state-save default is throttled to 1 s. A Harvest version would need an explicit fence on the page store and commit-before-ack semantics, plus a separate list/search index, because none of the per-key systems provides one.
- **Global listing:** Every system that offers listing does it outside the owner write path, asynchronously. Harvest can likely keep a Postgres-indexed visibility table updated by an outbox, with documented eventual consistency, as Temporal does.

### Gaps
- No primary postmortem for any of the five systems was found that quantifies a split-brain or duplicate-owner incident.
- Comparable, primary-sourced failover latencies after a crash are missing for Temporal, Orleans and Rivet.
- The Durable Task Scheduler (Netherite's successor) and Temporal's newer features, such as any change to Matching partition ownership, were not researched.
