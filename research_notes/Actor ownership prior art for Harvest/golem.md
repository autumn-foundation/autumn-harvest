# Golem (golem.cloud, golemcloud/golem) as prior art for owner-per-key with colocated embedded storage

Research date: 2026-10-19. That is the scheduled release day of Golem 1.6 itself, so "current" here means the 1.6 release post (dated 2026-10-09) plus `main` PRs merged up to about 2026-10-08.

Source classes used below:
- **[P-code]**: a primary source in GitHub PRs or issues on golemcloud/golem. These are the most reliable on internals.
- **[P-docs]**: vendor docs at learn.golem.cloud. Pages are split into `/v1.5/` and unversioned (1.6-era) copies.
- **[V-blog]**: a vendor blog post. Treat it as a vendor claim.
- **[S]**: a secondary aggregator.

Method caveat: the web fetches came back as model-summarized text, not raw HTML. Exact numbers and quoted phrases were kept as returned. Spot-check anything load-bearing before relying on it. DeepWiki (golemcloud/golem) returned HTTP 429 and could not be used. The GitHub MCP connector has no access to golemcloud/golem in this session, so issues and PRs were read through the public web.

## Components, sharding, assignment, and stale-executor fencing

### Takeaway
Golem shards agents across worker executors. A shard manager hands out shards, and production uses 1024 shards. Before September 2026 a stale executor was **not** fenced at the storage layer, and only an index primary key decided which writer won. Golem 1.6 adds three things: executor-held shard leases, a CAS-persisted shard manager with leader election and an etcd option, and **per-oplog epoch fencing** that is checked inside the append transaction. The fence covers only the oplog. KV records, status blobs, and blob payloads remain unfenced.

### Cited Findings
- **Services (1.6-era deploy docs) [P-docs]:**
  - The registry service manages accounts, apps, environments, components, plugins, tools, and policies, backed by a relational DB plus blob storage.
  - The worker service provides the APIs and HTTP gateway, and it routes requests to executors.
  - The worker executor runs agents on its assigned shards. Each oplog lives in indexed storage (PostgreSQL, SQLite, or Redis), alongside KV and blob storage.
  - The shard manager assigns shards and rebalances them when executors join or leave.
  - Other pieces are an optional component compilation service that warms the compiled-code cache, and an Envoy "Golem router".
  - A single binary, `golem server run`, runs everything in one process. `GOLEM_LITESTREAM=true` replicates its SQLite DBs.
  - — [Deployment docs](https://learn.golem.cloud/deploy)
- **Shard manager singleton rule [P-docs]:** "Run exactly one instance with PostgreSQL or SQLite persistence." With etcd, "run multiple replicas with one elected leader." Older docs described it as a single node, with "only one instance can be alive at any given time" (older info). — [Deployment docs](https://learn.golem.cloud/deploy); older wording via search snippet of the same page
- **Shard count is 1024 [P-code]:** A chaos run on golem-dev on 2026-08-18 left "460 of 1024 shards had no owner for 43 minutes", and "the cluster never repaired itself". — [PR #3766](https://github.com/golemcloud/golem/pull/3766)
- **Revoke protocol (pre-redesign) [P-code]:**
  - The shard manager sends `RevokeShards`, and the executor drains the agents on those shards.
  - Drains ran one agent at a time against a 5 s deadline. On timeout, `Rebalance::remove_shards` dropped the shards from the plan, and the routing table kept crediting the old executor.
  - PR #3766 proposed several fixes: parallel drain, `Worker::is_loaded()` as the barrier, a single-flight drain, raising `revoke_shards_timeout` from 5 s to 60 s, and an authoritative `SetShardAssignment` before reassigning.
  - vigoo's review found that durable writes from RPCs already past their ownership check could still land after the drain. Failed-assignment reconciliation could also allow dual ownership.
  - The author called an oplog epoch fence "the structural fix". The PR was closed unmerged on 2026-09-28 as "fixed by GOL-448,449 redesign".
  - — [PR #3766](https://github.com/golemcloud/golem/pull/3766)
- **Lease protocol [P-code]:** PR #3834, "Implement Shard lease protocol: RenewShardLease, Deregister, epoch in AssignShards", is referenced as merged. Lease durations were not found. — referenced from [PR #3766](https://github.com/golemcloud/golem/pull/3766) and [PR #3845](https://github.com/golemcloud/golem/pull/3845)
- **Oplog epoch fencing [P-code], PR #3845:** opened 2026-09-09 and merged around 2026-09-28.
  - **Problem statement:** a lease bounds how long a lost shard is served. It cannot stop an executor that already lost the shard from finishing a write it had started, for example after a pause, VM migration, or partition. Before this PR, only the `index_storage` primary key protected the oplog. It "rejected whichever executor wrote second, often the rightful owner."
  - **Mechanism:** a per-oplog ownership record, `oplog_metadata`, later renamed `indexed_key_epoch` with column `writer`. It holds the shard epoch allowed to write. The record only moves forward, through a CAS upsert.
  - **Check inside the append transaction:** every append asserts the epoch in the same transaction that inserts the entries. PostgreSQL uses `SELECT ... FOR UPDATE`. SQLite relies on its single-connection write pool. A missing record counts as fenced. A refused batch writes nothing.
  - **Backends:** all five backends implement it. Redis does it with one atomic Lua script. Redis Cluster is unsupported because the two keys must share a slot. Redis epochs are "only as durable as the last write the server kept".
  - **Unchecked writes:** `None` skips the check. That covers single-shard mode, the debugging service, forks into remote targets, ephemeral oplogs, and archive layers.
  - **Relinquish on a refused write:** the refusal becomes `OplogFenced` and then `InterruptKind::ShardLost`.
    - The agent is given up, not restarted, because a restart would reopen the oplog at the stale epoch.
    - The handle refuses all later writes locally.
    - A result is not published to waiters until its completion record commits.
    - Pending invocations get a retriable error and no cached result, so the new owner runs them.
    - The worker service retries routing misses.
  - **Epoch hygiene:** the manager mints epochs above any epoch an executor reports from a refused write. Re-registration after `LeaseNotFound` sends the shards the executor held.
  - **Limits (stated in the PR):** "The fence covers only the oplog." KV records, status blobs, and blob payloads carry no epoch, so a stale status write can be visible until the next status change. `drop_prefix` was not epoch-checked. The shipped default storage stayed `KVStoreRedis`, and changing it was deferred. The migration is roll-forward only.
  - **Test:** a sharding end-to-end test SIGSTOPs an executor, moves its shards, wakes it, and checks that no duplicate write happens.
  - — [PR #3845](https://github.com/golemcloud/golem/pull/3845)
- **1.6 release framing [V-blog]:** the shard manager was "reworked around executor-held shard leases, compare-and-swap persistence, leader election, and an etcd backend for distributed deployments". Oplog writes are fenced by shard epoch, so "an executor that has lost ownership of an agent cannot write to it." — [Golem 1.6 post (2026-10-09)](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Ownership rule [V-blog]:** "Only one node owns a shard at a time." A stateless gateway routes calls to the owner. Agents on a failed node recover from their oplogs elsewhere. — [What's This Golem Cloud Thing About? (De Goes, dated 2026-10-11)](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **Executor internals [V-blog]:**
  - Agents are Wasmtime instances inside a shared executor process.
  - TS and Effect agents run on QuickJS compiled to Wasm. Rust, Go, and MoonBit compile directly to Wasm.
  - Compiled code is loaded once and shared read-only.
  - — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **Runtime and later PRs [V-blog/P-code]:** 1.6 moves to WASI P3 and Wasmtime 46. A later `main` merge mentions a "fenced blob oplog archive", which suggests the archive layer gained fencing after #3845. This is known from the PR title only. — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/); [PR #4070 title](https://github.com/golemcloud/golem/pull/4070)

### Inferences
- Golem's ownership history matches the failure mode Harvest would face with owner-local SQLite. Leases alone were insufficient, and Golem's own team said so in review. The real fix was a monotonic per-key writer epoch, checked atomically with the append.
- For Harvest, the analogue is an owner epoch stored and checked in the same transaction as the history append. With colocated SQLite, the epoch would have to live in that SQLite file, or in Postgres in the same transaction. Otherwise the fence does not cover the write.
- Golem's fence deliberately stops at the oplog, and side stores still have a stale-write window. This is a concrete warning for any design that commits a "user SQLite" next to history: unless both share one transaction and one fence, the user DB is the unfenced side store.
- Golem routes by shard (1024) rather than by key. Fencing granularity is still per oplog, keyed by the shard epoch.

### Gaps
- Lease TTL and renewal intervals, and the leader-election details, were not found. PR #3834 was not read directly.
- The agent-ID-to-shard hash function was not confirmed. Earlier aggregator claims of "consistent hashing" are unverified.
- Whether `number_of_shards` is configurable, and its default, was not found in docs. 1024 comes from the golem-dev chaos run.

## Oplog: contents, storage layers, archiving, recovery, bounding replay

### Takeaway
Each durable agent has an append-only oplog. It records every host call that touches the outside world, with request and response, plus invocations, updates, snapshots, and permission events. Storage is layered:
- a primary indexed store (Redis streams, PostgreSQL, or SQLite);
- compressed secondary indexed layers;
- blob storage for larger compressed chunks.

Replay length is bounded by snapshots. Since 1.5, snapshot entries are written into the oplog on a policy (`every(N)` calls or `periodic`). Recovery replays only what follows the latest usable snapshot.

### Cited Findings
- **Oplog contents [V-blog]:** the oplog records "each host call that touches the outside world": clock reads, randomness, HTTP requests, agent-to-agent calls, and tool calls, each with request and response. It also records invocations, updates, snapshots, and permission events. — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **Write-ahead claim [S]:** "Every operation performed by a worker is recorded as an OplogEntry before execution." — DeepWiki snippet via search ([deepwiki](https://deepwiki.com/golemcloud/golem)), not verified directly.
- **Durable function types (1.5 WIT) [P-docs]:** `read-local`, `write-local`, `read-remote`, `write-remote`, `write-remote-batched`, and `write-remote-transaction`. The batched and transaction variants take an optional `OplogIndex`. The WIT exposes `get-oplog` and `search-oplog` resources for agents to read their own or others' oplogs. WIT versions are `golem:api/host@1.5.0` and `golem:durability/durability@1.5.0`. — [Additional APIs (v1.5)](https://learn.golem.cloud/v1.5/develop/additional)
- **Persistence levels [P-docs]:** 1.5 has `persist-nothing`, `persist-remote-side-effects`, and `smart`. These are user-settable to turn off persistence for sections of code. `isLive` reports whether the agent is replaying. — [Additional APIs (v1.5)](https://learn.golem.cloud/v1.5/develop/additional). **In 1.6 these are removed:** "Durable agents no longer have persistence levels" [V-blog]. — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Opting out [P-docs]:** "You cannot opt out of oplog writes for a durable agent." Ephemeral agents (`mode: "ephemeral"`) keep no oplog and discard state after each invocation. — [Configuring durability (TS, v1.5)](https://learn.golem.cloud/v1.5/how-to-guides/ts/golem-configure-durability-ts)
- **Storage traits [P-docs]:** the executor needs three backends.
  - Blob storage: S3, file system, or in-memory.
  - KV storage with set/sorted-set operations: Redis or in-memory.
  - Indexed append-only storage for oplogs: Redis streams or in-memory.
  - Custom backends implement the `BlobStorage`, `KeyValueStorage`, and `IndexedStorage` traits.
  - The newer deploy docs and PR #3845 add PostgreSQL, SQLite, and "multi-SQLite" for indexed storage.
  - — [Persistence (v1.5)](https://learn.golem.cloud/v1.5/operate/persistence); [Deployment](https://learn.golem.cloud/deploy); [PR #3845](https://github.com/golemcloud/golem/pull/3845)
- **Layering and defaults [P-docs]:**
  - Primary is indexed storage that running agents append to. Secondary is indexed storage where each entry is a compressed set of oplog entries. Tertiary is blob storage with larger compressed chunks.
  - "Oplogs are continuously moved towards the lower layers to prevent the primary oplog storage (Redis) from getting full." Agents inactive for a while live fully in blob storage.
  - Defaults: `archive_interval = "1day"`, `indexed_storage_layers = 2`, `blob_storage_layers = 1`, `entry_count_limit = 1024`, `max_operations_before_commit = 128`, `max_payload_size = 65536`.
  - Payloads over the limit go to blob storage, and the oplog holds a reference.
  - — [Persistence (v1.5)](https://learn.golem.cloud/v1.5/operate/persistence)
- **Metadata cache [P-docs]:** aggregated agent metadata is cached in KV storage. It may lag the oplog and records the last oplog index it was computed from. Without primary oplog data, metadata is rebuilt by reading the whole archived oplog. Promises and schedules, such as wake-after-sleep, live in KV. — [Persistence (v1.5)](https://learn.golem.cloud/v1.5/operate/persistence)
- **Ephemeral archiving [P-code]:** PR #3802, "Archive ephemeral oplogs from a sweep instead of a scheduled action", exists. It was seen as a search title only. — [PR #3802](https://github.com/golemcloud/golem/pull/3802)
- **Snapshots bound replay [P-docs]:** TS uses `snapshotting: { every: 10 }` or `{ periodic: "30s" }`. "With snapshots, recovery starts from the latest snapshot instead of replaying the full oplog." — [Configuring durability (TS, v1.5)](https://learn.golem.cloud/v1.5/how-to-guides/ts/golem-configure-durability-ts). 1.5 [V-blog] says the oplog has snapshot entries that "capture full agent state at recovery checkpoints". — [Golem 1.5 post (2026-05-08)](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)
- **1.6 snapshot changes [V-blog]:** agents "restore without re-running initialization". Custom multipart snapshots carry structured state plus named binary parts. — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Inline retries [V-blog]:** 1.5 retries many transient errors inline "instead of discarding the instance and replaying the oplog". 1.6 adds "local retries". — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/); [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)

### Inferences
- Golem's oplog is a record of non-deterministic effects. Replay re-runs Wasm code and substitutes recorded results. This is conceptually the same as Harvest's strict replay over an event history, but at host-call granularity rather than command/event granularity.
- In practice, Golem's layered primary → compressed → blob design is "hot tail in a fast store, cold body in object storage". Combined with snapshots, recovery cost is roughly snapshot load plus the tail.
- Golem rebuilds metadata from the full archived oplog when the primary is gone. That is a real cold-path cost, and Harvest should avoid it by persisting metadata durably.

### Gaps
- The exact on-disk serialization format of oplog entries was not confirmed. The OplogEntry enum list for 1.6 was not retrieved.
- The commit and replication semantics of `max_operations_before_commit` were not read.
- 1.6-era oplog layer defaults were not confirmed. Defaults above are v1.5 docs.

## Exactly-once external effects, idempotency keys, agent-to-agent RPC

### Takeaway
Golem's "exactly-once" is scoped tightly. It covers agent invocations per idempotency key, agent-to-agent calls, and tool calls. External HTTP is exactly-once **only if the remote honors an auto-injected idempotency key**, and at-least-once otherwise. RDBMS got special in-flight-transaction recovery (issue #1514). It uses Postgres transaction-ID introspection, or a `golem_transactions` marker table where introspection is not available. There is no automatic rollback.

### Cited Findings
- **Scope [V-blog]:**
  - Invocations execute exactly once per idempotency key, and agent-to-agent and tool calls execute exactly once. Golem derives idempotency keys from oplog position.
  - Outgoing HTTP gets an automatic `idempotency-key` header. Services must honor the key for exactly-once to hold.
  - "Accepted work runs to completion, but there is no automatic rollback." Atomic regions re-execute from their start, which is safe only with key-honoring services.
  - — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **Docs version [P-docs]:** exactly-once for systems supporting idempotency keys, at-least-once otherwise. `generateIdempotencyKey()` returns a UUID that "will always be the same (per occurrence)" across crash restarts. The CLI `-i` flag sets an explicit invocation key. — [Additional APIs (v1.5)](https://learn.golem.cloud/v1.5/develop/additional); [agents concept page](https://learn.golem.cloud/concepts/agents) (search snippet)
- **RDBMS [P-code], issue #1514:** "Implement Exactly-Once Semantics for golem:rdbms".
  - Opened 2025-04-10 by jdegoes and closed 2025-09-16 via PR #1741 (vigoo). It sat in the Golem 1.3 milestone with a $1,500 bounty.
  - **Problem:** a DB commit followed by a crash before the oplog records the outcome would re-run the transaction on replay.
  - **Design with introspection (Postgres):** log the transaction ID at begin and an `end_transaction` marker. On replay with no marker, query the DB for the transaction status. Skip if committed, replay if aborted or never completed, and fail the instance if the status is unknown.
  - **Design without introspection (MySQL):** keep a `golem_transactions` table with a UUID row written inside the transaction, and delete it after the end marker is logged.
  - — [Issue #1514](https://github.com/golemcloud/golem/issues/1514)
- **Concurrent durability [V-blog]:** 1.6 adds it. "host calls can overlap durably, and agent-to-agent and tool calls execute exactly once." — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **RPC durability [V-blog]:** agent-to-agent calls carry the same key across retries and replays. Scheduled calls are durable and cancellable. `get` creates the target if it is absent. — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **RPC variants [V-blog]:** 1.5 RPC offers `get`, `getPhantom`, and `newPhantom`, with invoke, trigger, and schedule variants. — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)
- **Results after failover [P-code]:** with fencing, a result is not published to waiters until its completion record commits. A relinquished agent's pending invocations get a retriable error and no cached result, so the new owner executes them. — [PR #3845](https://github.com/golemcloud/golem/pull/3845)

### Inferences
- The #1514 design is the in-doubt-commit problem that Harvest avoids by construction if user SQLite and history commit in one transaction on the same SQLite or Postgres. Golem had to build introspection and marker tables because its oplog and the user's DB are separate stores.
- "Exactly-once" in Golem marketing means exactly-once *recording and dedup* inside Golem. For external effects it is effectively-once only when the remote cooperates.

### Gaps
- No independent verification (Jepsen-style) of Golem's exactly-once claims was found.
- Whether RPC dedup state survives oplog archiving or deletion of the callee was not found.

## Per-agent filesystem and per-agent SQLite

### Takeaway
Each agent has a private filesystem. TS agents have had embedded SQLite through `node:sqlite` on QuickJS, and 1.6 documents it for Effect and Rust, with experimental in-memory Turso for Rust. SQLite is **not** journaled at the SQL level. A file-backed DB is a file on the agent's filesystem. Its durability comes from **filesystem snapshots**, added in October 2026, plus replay that compares recorded file reads and stats. File snapshots need XFS with reflink.

### Cited Findings
- **Language support [V-blog]:** "TypeScript agents already had embedded SQLite". In 1.6, Effect and Rust gain documented SQLite support, and Rust gets "an experimental in-memory Turso integration". Scala and MoonBit are not covered. Golem "can now snapshot an agent's filesystem with its state". — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **SQLite was planned for 1.6 [V-blog]:** the 1.5 post lists "per-agent SQLite/Turso for trivially-deployed local agent state" under 1.6 plans. This conflicts mildly with "TS already had" it in the 1.6 post: TS probably gained `node:sqlite` during the 1.5.x line. — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)
- **Filesystem snapshots [P-code], PR #4061:** merged around 2026-10-08, 112 commits.
  - **Problem fixed:** before it, "a start from a snapshot record lost the files that the agent wrote before the record".
  - **Mechanism:** at a snapshot boundary the executor makes a **reflink copy** of the agent's files and writes a snapshot record naming it. A background job uploads the copy in **restic format**, through a `rustic_core` fork, to the executor's `BlobStorage`, with one repository per agent incarnation. On upload completion the agent writes a `SnapshotConfirmed` oplog entry. A start uses only confirmed records and restores files before loading the application snapshot.
  - **Rules:** an application snapshot is used only if files can be restored with it. Agents whose files changed take no automatic snapshot, and snapshot-based manual updates of them fail. Ephemeral agents take no snapshots. Filesystem snapshots require XFS with reflink.
  - **WIT:** `golem:api/oplog@1.5.0` adds `snapshot-confirmed`.
  - — [PR #4061](https://github.com/golemcloud/golem/pull/4061)
- **SQLite and snapshots [P-code], PR #4019:** merged 2026-10-02 into the `filesystem-snapshot` branch.
  - **Before:** a typed TS snapshot stored the bytes of every `DatabaseSync` field, which "cut it off from its file".
  - **After:** file-backed DBs are recorded **by location only** in a `fileDatabases` field. In-memory DBs are still stored as `db:<field>` byte parts.
  - **Load and fallback:** load reopens the file and fails if it is missing. Automatic startup then falls back to an older snapshot or full replay.
  - **Warm-up:** after load, the SDK warms the page cache (schema, plus every page if the DB fits) "so replay's file-read comparisons match and recovery does not fall back to full replay."
  - **Save rules:** an open transaction fails the save. `StatementSync`, `Session`, and `SQLTagStore` fields fail the save. PRAGMAs and functions are not preserved.
  - **Without filesystem snapshots:** "files written before a snapshot are not restored". A test checks full-replay recovery for that case.
  - — [PR #4019](https://github.com/golemcloud/golem/pull/4019)
- **Related bug [P-code]:** PR #3938, "Prevent idle durable stream maintenance from exhausting SQLite resources" (seen as a title only). — [PR #3938](https://github.com/golemcloud/golem/pull/3938)
- **Limits [V-blog]:** per-agent memory and disk default to 1 GB each in open source. 1.6 meters storage and memory with per-agent limits. — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/); [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Initial filesystem [V-blog]:** an agent's "initial filesystem" is configurable per agent (1.5). — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)

### Inferences
- Golem's per-agent SQLite is effectively local state reconstructed by deterministic replay. Agent file writes are re-executed on replay rather than journaled. Snapshot and restore of the filesystem is the newer optimization that shortens replay. This differs from "SQLite committed atomically with history". In Golem, SQLite page writes and oplog appends are not in one transaction. Consistency is restored by replay, and snapshots are gated on confirmed file uploads.
- The reflink-plus-restic approach, with `SnapshotConfirmed` written only after upload, is a two-phase "capture locally, confirm in log" pattern. Harvest could reuse it for shipping per-owner SQLite to object storage.
- The open-transaction-fails-save rule and the page-cache warm-up show how fragile replay comparisons over file reads can be. They are a caution for Harvest's strict-determinism model if user SQLite reads were to be journaled.

### Gaps
- The exact mechanism for SQL writes during replay (re-executed vs recorded) was not stated in PR text. The inference above is not confirmed.
- How the reflink copy treats a SQLite file open in WAL mode, which bears on consistency, was not addressed in the sources read.
- Whether Rust Turso is persisted at all (it is "in-memory") was not found.

## Oplog processor plugins (1.5)

### Takeaway
Oplog processor plugins are ordinary agents with a special interface. They receive every oplog entry of agents that activate them, with **exactly-once delivery per processor** as of 1.5, authored in Rust only. The flagship use is the built-in OTLP exporter. Instance count and placement are best-effort.

### Cited Findings
- **Maturity and delivery [V-blog]:** before 1.5 the feature was "unfinished … we did not have exactly-once delivery semantics". 1.5 "guarantees that every oplog entry is sent exactly once to a given oplog processor". There are no guarantees on how many processor instances exist. By default it tries to keep one per executor node. Only Rust is supported for authoring. — vigoo blog via search snippet: [Part 14: OpenTelemetry](https://blog.vigoo.dev/posts/golem15-part14-otlp/), [golem.cloud mirror](https://golem.cloud/blog/golem-1-5-features-part-14-opentelemetry/)
- **OTLP exporter [V-blog]:** the built-in `golem-otlp-exporter` exports traces, logs, and metrics over OTLP/HTTP. About two dozen runtime metrics are tagged with agent type, ID, component, and version. Plugins install per component and activate per agent. — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/); [Observability docs (v1.5)](https://learn.golem.cloud/v1.5/develop/observability)
- **Republished for 1.6 [P-code]:** PR #4071 republishes four built-ins against the new oplog WIT. Plugins are tied to the oplog WIT version. — [PR #4071](https://github.com/golemcloud/golem/pull/4071)

### Inferences
- This is a log-tailing CDC consumer with per-consumer exactly-once. It is the natural Golem analogue for building secondary indexes or search over agents. Golem did not apparently use it for its own agent listing.

### Gaps
- How exactly-once is implemented (cursor stored where, batch size, ack protocol) was not retrieved. Fetching the full vigoo Part 14 post would close this.
- Indexing use cases beyond OTel were not found in sources.

## Suspend-to-zero, interruption, resumption, cold start, memory

### Takeaway
Agents waiting on sleeps or promises are unloaded after about 10 seconds. Only the oplog, files, and snapshots remain. Idle agents are evicted under memory pressure. The vendor claims a warm call takes a few milliseconds and the first call to a never-seen component takes "well under a second". No independent benchmarks were found.

### Cited Findings
- **Suspension and latency [V-blog]:** an agent waiting on a timer, promise, or slow agent call is unloaded "after about ten seconds for sleeps and promises". Idle resident agents are evicted under memory pressure. "A warm call to an agent takes a few milliseconds end to end." The first call to an unseen component "completes in well under a second". Golem Cloud meters allocated Wasm memory over time. — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **Waits and throttling [V-blog]:** webhook waits suspend the agent so that it "suspends fully — consuming no compute, no memory". Quota throttle suspends until the resource is available (1.5). 1.6: idle agents "still suspend to zero with their full state", and the invocation hot path "does less copying and less allocation". No figures are given. — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/); [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Compiled-code cache [P-docs]:** compiled native code is cached in executor blob storage. The optional compilation service warms it, and otherwise the executor compiles on first use. — [Persistence (v1.5)](https://learn.golem.cloud/v1.5/operate/persistence); [Deployment](https://learn.golem.cloud/deploy)
- **Config rename [V-blog]:** `[active_workers]` is renamed `[active_agents]` in 1.6. This is the executor's resident-agent limit section. — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)

### Inferences
- Golem's resume path is: load the compiled component, then the latest confirmed snapshot (application state plus restored files), then replay the oplog tail. That is the same shape Harvest would have with per-owner SQLite: download or restore the file, then replay the history tail.

### Gaps
- No measured resume-from-snapshot latency was found, nor any numbers for replay throughput or per-agent memory overhead.

## Listing and search over agents

### Takeaway
Enumeration is a paged scan per component with metadata filters (name, status, version, creation time, env), and cached or "precise" metadata. Golem's docs explicitly call it slow and not for application logic. There is no general secondary index or search service.

### Cited Findings
- **CLI [P-docs]:** `golem agent list` takes `--filter "status = Running"`, ANDed. It pages with `--max-count` and `--scan-cursor`, and cursor paging requires a single component. `--precise` gives fresher status but is slower. — [Listing and Filtering Agents](https://learn.golem.cloud/how-to-guides/common/golem-list-and-filter-agents)
- **WIT and warning [P-docs]:** an enumeration resource takes a component ID, a filter, and a precise flag, and returns pages from `get-next`. Filters are any-of-all-of property filters. Precise mode recomputes metadata, and the default uses cached values. "Enumerating agents of a component is a slow operation and should not be used as part of the application logic." — [Additional APIs (v1.5)](https://learn.golem.cloud/v1.5/develop/additional); [Listing and Filtering Agents](https://learn.golem.cloud/v1.5/how-to-guides/common/golem-list-and-filter-agents)
- **Metadata location [P-docs]:** the cached metadata lives in KV and may lag the oplog. — [Persistence (v1.5)](https://learn.golem.cloud/v1.5/operate/persistence)

### Inferences
- Golem pays for owner-local state with weak enumeration. Listing is a scan over KV metadata, not an index. With per-owner local SQLite, Harvest would face the same trade-off unless it keeps a central index in Postgres, or builds a projection fed by the log (Golem's oplog-processor pattern).

### Gaps
- The scan's internal implementation was not read: per-shard fan-out vs a central KV scan.

## Upgrades of running agents

### Takeaway
There are two modes. **Automatic** replays the oplog against the new component and fails safely on divergence. **Manual** uses snapshots: the old version's `save-snapshot` and the new version's `load-snapshot`. In 1.6, automatic updates can use snapshots, reverts work across snapshot-based updates, and filesystem snapshots are restored as part of the update.

### Cited Findings
- **Modes [P-docs]:**
  - Automatic is the default and works when the new version is compatible. It fails if replay diverges.
  - Manual needs `saveSnapshot` and `loadSnapshot` and is required for incompatible changes.
  - Commands: `golem agent update '<id>' manual` and `golem component update-agents --update-mode manual`. The API is `updateAgent(agentId, targetRevision, "automatic")`.
  - Rust generates snapshot exports only when `snapshotting` is set. MoonBit uses `Snapshottable`, and Scala uses `Snapshotted[S]`.
  - — [Updating Running Agents](https://learn.golem.cloud/how-to-guides/common/golem-update-running-agents); [Additional APIs (v1.5)](https://learn.golem.cloud/v1.5/develop/additional); custom-snapshot guides ([Rust](https://learn.golem.cloud/how-to-guides/rust/golem-custom-snapshot-rust), [TS v1.5](https://learn.golem.cloud/v1.5/how-to-guides/ts/golem-custom-snapshot-ts))
- **Safe failure [V-blog]:** "An automatic update fails safely if new code diverges from the recorded history." — [De Goes post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)
- **1.6 changes [V-blog]:** automatic updates can use snapshots to move agents to a new revision, and reverts work across successful snapshot-based updates. — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)
- **Update with files [P-code]:** snapshot-assisted automatic updates restore the files of the selected record. A failure writes `FailedUpdate`, and the agent stays on its source revision. — [PR #4061](https://github.com/golemcloud/golem/pull/4061)

### Inferences
- Golem's automatic update means "replay the old history under new code and detect divergence". That is the same as Harvest's strict-replay versioning problem. Golem's escape hatch is a user-defined state snapshot, not code-level patch markers like Temporal's `patched`.

### Gaps
- How 1.6 picks the snapshot for automatic updates, and the semantics of revert, were not detailed in the sources read.

## Known limits, bugs, complaints, performance numbers

### Takeaway
Golem's public issue tracker shows real distributed-ownership bugs in 2026. Shards went ownerless for 43 minutes in a chaos run, and the pre-fencing design allowed the wrong executor to win writes. No independent performance benchmarks were found. The vendor gives only "few ms warm call" and "sub-second first call".

### Cited Findings
- **Ownership bugs [P-code]:** 460/1024 shards had no owner for 43 minutes, and the cluster never repaired itself (golem-dev chaos run, 2026-08-18). Possible dual ownership and late RPC writes were found in review. — [PR #3766](https://github.com/golemcloud/golem/pull/3766)
- **Fence limits [P-code]:** KV, status, and blob writes are unfenced. Redis Cluster is unsupported. The default `KVStoreRedis` storage fails at startup in distributed mode until reconfigured. — [PR #3845](https://github.com/golemcloud/golem/pull/3845)
- **File snapshot limits [P-code]:** filesystem snapshots need XFS with reflink. Agents whose files changed without filesystem snapshots get no automatic snapshot. — [PR #4061](https://github.com/golemcloud/golem/pull/4061)
- **Partial durability [V-blog]:** WebSocket durability is "partial today" (1.5). — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)
- **Breaking changes in 1.6 [V-blog]:**
  - SDK rebuild is required, and TS moves from decorators to `defineAgent`.
  - Scala 2.13 is dropped.
  - Persistence levels, environment shares, and the debugging service are removed.
  - The WIT contract and manifest format changed.
  - — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)

### Inferences
- A useful negative lesson for Harvest: Golem built leases first and found them insufficient within weeks. Fencing should be designed in from day one.

### Gaps
- Community complaints (forums, Discord, HN) were not surveyed within the tool budget.

## Version timeline (1.3 → 1.6)

### Takeaway
The "agent" model and RDBMS exactly-once arrived in the 1.3 line (2025). 1.5 (2026-05-14) made the agent the primary configurable entity, made oplog processors exactly-once, and added snapshots-in-oplog, quotas, Scala, and MoonBit. 1.6 (2026-10-19) brings ownership fencing, leases, etcd, filesystem snapshots, SQLite for Rust and Effect, WASI P3, and the removal of persistence levels.

### Cited Findings
- **1.3 [P-code]:** issue #1514 (RDBMS exactly-once) was in the Golem 1.3 milestone and closed 2025-09-16. — [Issue #1514](https://github.com/golemcloud/golem/issues/1514)
- **1.5 [V-blog]:** announced 2026-05-08 for release on "Thursday, May 14th". A vigoo series post had said the end of April 2026, so the date slipped. Changes:
  - Rib removed, and the TS REPL replaces it.
  - Agent IDs are the type name plus constructor parameters.
  - Per-agent config and secrets.
  - Predicate-driven retry policies.
  - Quotas (Rate, Capacity, Concurrency).
  - Scala via Scala.js on QuickJS, and MoonBit.
  - — [1.5 post](https://golem.cloud/blog/golem-1-5-the-agent-runtime/); [Part 5 mirror](https://blogs.apievangelist.com/blogs/golem-cloud-2026-04-14-golem-1-5-features-part-5-scala-support/) [S]
- **1.6 [V-blog]:** post dated 2026-10-09: "On Monday, October 19th, we will release Golem 1.6." Changes:
  - Leases, CAS persistence, leader election, etcd, and epoch fencing.
  - Filesystem snapshots, and snapshot restore without re-running init.
  - SQLite for Effect and Rust, and Turso (experimental).
  - Concurrent durability, Durable Streams, and streaming methods.
  - WASI P3 and Wasmtime 46.
  - Storage and memory metering.
  - — [1.6 post](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)

### Inferences
- The fencing and filesystem-snapshot code is weeks old at release, merged from September to October 2026. Treat its production maturity as unproven.

### Gaps
- No 1.4 release notes were retrieved, so what changed in 1.4 specifically is unknown.
- The 1.3 release date was not confirmed.
