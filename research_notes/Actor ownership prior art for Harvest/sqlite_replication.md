# Durable / replicated SQLite and many-SQLite-per-node: prior art for Autumn Harvest

Scope: Litestream, LiteFS, Turso/libSQL (incl. the Rust "Turso Database" rewrite), rqlite, dqlite, Cloudflare D1 / Durable Objects (briefly), mvSQLite. Research date 2026-10-19. Every finding has an inline source. Secondary sources are marked **[secondary]**. Facts that may be stale are marked **[possibly stale]**.

## Litestream: WAL shipping, RPO/RTO, the 2025 revival (v0.5, LTX, VFS), single-writer rule and fencing

### Takeaway
Litestream is asynchronous WAL-to-object-storage shipping. The default sync interval is 1 s, so the RPO is roughly the last second of commits on a crash. Synchronous replication does not exist. Since v0.5.8 an S3 conditional-write lease ("CASAAS") keeps a second *Litestream process* from replicating to the same path. Nothing in the docs says it fences the *application's* writes on a node that has lost the lease. Litestream is a backup and restore tool. It is not a failover-safe replicated source of truth.

### Cited Findings
- Mechanics: "By default, Litestream will replicate new changes to an S3 replica every second." Replication is asynchronous, so a crash can lose the changes inside the latest sync window. Synchronous replication is "on the roadmap" but not implemented. On a clean shutdown signal it tries to flush outstanding WAL first. — [Litestream Tips & Caveats](https://litestream.io/tips/)
- Litestream owns checkpointing and holds a read lock between its checkpoints. If the app checkpoints in between (for example, through autocheckpoint under many small transactions), Litestream can miss WAL frames and must take a fresh full snapshot. The recommendations are `PRAGMA wal_autocheckpoint = 0` and `busy_timeout = 5000`. Litestream "requires periodic but short write locks on the database when checkpointing occurs." — [Litestream Tips](https://litestream.io/tips/)
- Two writers: "Multiple applications replicating into the same bucket & path can cause situations where you will be unable to restore." — [Litestream Tips](https://litestream.io/tips/)
- Why the old design broke: Litestream is a sidecar process, so it can die or move and miss WAL changes. It detected breaks and started a new "generation" (snapshot plus WAL stream). Multiple generations made failover and read replicas hard. Two overlapping instances "could confuse each other." The fix is a time-based lease built on object-store conditional writes ("CASAAS"), so "you can run Litestream with ephemeral nodes, with overlapping run times." LiteFS needed Consul for the same job. — [Fly.io blog, "Litestream: Revamped" (2025, undated in page; cites Nov-2024 S3 conditional writes)](https://fly.io/blog/litestream-revamped/)
- v0.5.0 (announced on the Fly blog, ~Oct 2025 per Simon Willison's link post): generations are removed. A WAL-continuity break now writes a snapshot into the next LTX file, which keeps "a monotonically incrementing transaction ID". Restores use TXIDs. Each database gets **one** replica destination ("a single source of truth"). LTX is borrowed from LiteFS, with per-page compression and an end-of-file index. Compaction runs in levels: L1 30 s windows, L2 5 min, L3 hourly. Point-in-time restore needs "a dozen or so files on average." A NATS JetStream replica type is new. v0.5 cannot restore v0.3.x WAL-segment backups. CGO is removed (modernc.org/sqlite). — [Fly.io blog, "Litestream v0.5.0 is Here"](https://fly.io/blog/litestream-v050-is-here/); date: [Simon Willison, 3 Oct 2025](https://feeds.simonwillison.net/2025/Oct/3/litestream/) **[secondary for the date]**
- Distributed leasing (v0.5.8): "This ensures only one Litestream instance actively replicates a database at a time." The lease is a `lock.json` next to the replica holding `generation` (a monotonically increasing counter), `expires_at`, and `owner` (`hostname:pid`). Acquire is read, check expiry, then a conditional write using `If-Match`/`If-None-Match` on the ETag. Contention returns `412 Precondition Failed`. Failover time "depends on the lease duration." It requires a provider with conditional writes (AWS S3 supports them). It is "currently available only through the Go library API". The page gives no TTL defaults and does not say what the old leader does on lease loss. — [Litestream S3 Advanced Configuration](https://litestream.io/guides/s3-advanced/)
- The Litestream author said on HN that single-writer coordination was previously left to the operator outside Litestream. — [HN thread via hn.nuxt.dev](https://hn.nuxt.dev/item/44045292) **[secondary/forum]**
- An analysis of S3-CAS coordination notes that a hot manifest key contested by many writers livelocks into 409/412 retry storms. The pattern suits one intended writer with occasional failover. — [mohakchugh blog](https://mohakchugh.is-a.dev/blog/s3-conditional-writes-distributed-coordination) **[secondary]**
- Restore time: "a restore replays every LTX file recorded since the most recent snapshot". Frequent writes make it slower. Lower `snapshot.interval` (for example to `1h`) to shorten restores. Verify with `PRAGMA integrity_check`. — [Litestream Tips](https://litestream.io/tips/)
- Read-replica VFS: at v0.5.0 it was "the next major feature" with a working PoC. It serves reads from S3 at once while it hydrates in the background. — [Fly blog v0.5.0](https://fly.io/blog/litestream-v050-is-here/). The v0.5.3 beta ships it as SQLite loadable extensions for read-only access to replicas, marked **experimental**, plus point-in-time querying via PRAGMAs. — [newreleases.io v0.5.3-beta1](https://newreleases.io/project/github/benbjohnson/litestream/release/v0.5.3-beta1) **[secondary mirror of GitHub release]**. The VFS builds a page index from LTX files, fetches pages on demand, and polls for new LTX files every 1 s by default. It rewrites page 1 so the replica appears as a read-only rollback-journal DB. — [litestream.io: Read Replicas with VFS](https://litestream.io/how-it-works/vfs/) (via search snippet). The Fly post admits that VFS reads are "not as efficient as a local SQLite database." — [Litestream: Revamped](https://fly.io/blog/litestream-revamped/)
- The revamp targets replicating many DBs per process (`/data/*.db`, "hundreds or thousands" of files). — [Litestream: Revamped](https://fly.io/blog/litestream-revamped/)
- The latest release seen in search results is v0.5.13. — [search summary of litestream releases](https://newreleases.io/project/github/benbjohnson/litestream/release/v0.5.3-beta3) **[secondary; not verified on GitHub]**

### Inferences
- RPO is about 1 s of commits plus any WAL that Litestream missed while down. Litestream recovers from missed WAL by re-snapshotting, so a gap means extra restore cost, not silent loss of *older* data. RTO is roughly lease TTL plus a full restore (snapshot plus LTX replay). It scales with DB size and write rate, and no published numbers exist.
- The lease fences *the replicator against the object store*. It is a CAS on `lock.json`, and the `generation` counter behaves like a fencing token for uploads. The docs do not say that a deposed Litestream stops the application from committing locally. If Harvest made a local SQLite file the source of truth with Litestream, two Harvest processes on different hosts could both commit locally. Only one stream would reach S3, and the other host's commits would be silently orphaned. Harvest would still need its own ownership fencing (for example, a Postgres-side epoch or lease) *in front of* the SQLite writer.
- The single-writer, per-file OS lock in Harvest's SQLite backend fences only within one host. It does not cover a second host restoring the same replica.

### Gaps
- No published RTO or restore-time benchmarks for v0.5 (MB/s or seconds per GB).
- The lease TTL and renewal defaults are undocumented on the page. Whether a deposed leader halts replication immediately or on the next failed renew was not found.
- Whether the leasing feature is exposed in the CLI/YAML config (beyond the Go API) as of Oct 2026 is unverified.
- The exact GA status of the VFS in v0.5.x as of Oct 2026 was not verified on GitHub directly.

## LiteFS: FUSE, Consul leases, LTX, lease loss/fencing, project status, failure modes

### Takeaway
LiteFS is a FUSE passthrough filesystem that captures each transaction's page set as LTX and ships it **asynchronously** from a single Consul-lease primary to replicas. Nothing fences a deposed primary with a token. A rolling whole-database checksum detects divergence, and the replica re-snapshots from the new primary, which **discards** any unreplicated writes. Fly.io has "refocused effort" away from LiteFS: it gets limited updates and no significant planned work, and LiteFS Cloud was shut down on 15 Oct 2024.

### Cited Findings
- LiteFS is "a passthrough file system which intercepts these API calls" and copies out each transaction's page set into LTX. It supports rollback-journal and WAL modes. — [Fly docs: How LiteFS Works](https://docs.fly.io/litefs/how-it-works)
- Leases: "Distributed leases work by obtaining a lock on a key within Consul". The primary renews a TTL. A clean shutdown destroys the lease for immediate handover. After a crash, "the TTL must expire before a new node will become primary." — [superfly/litefs ARCHITECTURE.md](https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md)
- Data-loss window: "Typically, this window is subsecond as transactions can quickly be shuttled from the primary to the replicas". "A catastrophic crash on the primary would cause these transactions to be lost". "replicas which are further ahead could potentially lose some transactions" after failover. Synchronous and time-bounded async replication are "planned for future development". — [ARCHITECTURE.md](https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md); [How LiteFS Works](https://docs.fly.io/litefs/how-it-works)
- Divergence handling (instead of fencing): the position is `TXID/checksum`. The rolling checksum XORs each page's CRC64 in and out on every transaction. A former primary reconnecting may have the same TXID but a different checksum. "If it naively began applying transactions from the new leader, it could corrupt its database state". Instead "it will resnapshot the database from the new primary to ensure consistency." Writes made during a partition that never replicated are discarded. The design deliberately avoids Raft-style consensus and targets ephemeral deployments. — [ARCHITECTURE.md](https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md)
- Neither doc describes fencing tokens, or what a primary does with in-flight writes when its Consul lease lapses. — [ARCHITECTURE.md](https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md); [How LiteFS Works](https://docs.fly.io/litefs/how-it-works)
- A remote HALT lock (used for write forwarding from replicas) expires automatically and is released when the fd closes. — [litefs-go source](https://github.com/superfly/litefs-go/blob/main/litefs.go); [pkg.go.dev litefs](https://pkg.go.dev/github.com/superfly/litefs)
- Known issue: "Primary does not consistently release Consul lease" on SIGINT, which causes ungraceful shutdown and a TTL wait. — [superfly/litefs #171](https://github.com/superfly/litefs/issues/171). A community report covers a node unable to become primary after restart. — [Fly community](https://community.fly.io/t/litefs-cannot-become-primary-after-restart/15425) **[forum]**
- Status: quoted from Ben Johnson (LiteFS author) in a Fly community thread, re-quoted 13 Feb 2025: "LiteFS hasn't been discontinued but we've refocused effort towards other parts of the company". "it's been getting limited updates". "There's not a plan at the moment to do significant work on LiteFS". It "has been fairly stable for a while." — [Fly community: What is the status of LiteFS?](https://community.fly.io/t/what-is-the-status-of-litefs/23883) **[forum re-quote; original post undated on page]**
- LiteFS Cloud (managed backups) was wound down and available until 15 Oct 2024 because most users did not use it. "We're not sunsetting LiteFS itself!" The recommended DR is LiteFS plus Litestream. The LiteFS Cloud code was donated as the MIT `litefs-backup` project (self-hosted). — [Fly community: Sunsetting LiteFS Cloud](https://community.fly.io/t/sunsetting-litefs-cloud/20829)
- Fly says Litestream is more popular because it is easier to deploy. The Litestream revamp ports LiteFS ideas (LTX, compaction, VFS) and replaces Consul with object-store CAS. — [Litestream: Revamped](https://fly.io/blog/litestream-revamped/)

### Inferences
- LiteFS's split-brain story is "detect and overwrite the loser", not "prevent the loser from committing". That suits a cache or read-replica topology. It is unacceptable for a workflow history, where an acknowledged commit (an activity completion, say) could be silently rolled back after failover.
- Given the "limited updates" status, LiteFS is a poor dependency for a 2026 design. Its ideas (LTX, TXID plus rolling checksum for divergence detection) are reusable.

### Gaps
- No published FUSE overhead or write-throughput numbers were found in the primary docs.
- Default Consul lease TTL not found in the pages fetched.
- No specific GitHub issue documenting confirmed acknowledged-write loss in production was found. The loss is by design (async), per the architecture doc.

## Turso / libSQL: embedded replicas, "millions of databases", per-DB cost, schema migrations, 2025–2026 changes, sync semantics

### Takeaway
Turso has pivoted hard. Edge replicas, Multi-DB Schemas, and `ATTACH` were removed for new users (Jan 2025). libSQL embedded replicas are now "legacy" and "strongly discouraged" in favour of the new `turso` crate's push/pull sync, where reads *and writes* are local. The Rust rewrite (ex-Limbo, now "Turso Database") is in beta, with MVCC `BEGIN CONCURRENT` (row-level conflict detection at commit) in early preview on Turso Cloud since 3 Aug 2026. "Millions of databases" remains the marketing pitch, but schema fan-out across tenants is now the user's job.

### Cited Findings
- 21 Jan 2025 roadmap post: edge replicas, Multi-DB Schemas, and `ATTACH` are removed for new users (existing paid users keep them). "70% of Turso users never create geographical replicas". The new server is closed source while "everything that runs on the client will remain strictly open source". libSQL stays self-hostable. Limbo will "simply be called 'Turso'". The free tier moves from Fly.io to AWS ("no cold starts!"). There are layoffs, because the team was not suited for "writing a database from scratch". — [Turso: Upcoming changes to the Turso Platform and Roadmap](https://turso.tech/blog/upcoming-changes-to-the-turso-platform-and-roadmap)
- Multi-DB Schemas docs page titled "(Deprecated)": deprecated for all new users, existing paid users continue. A parent schema DB propagates to children, and a schema DB cannot be deleted while it has children. — [Turso docs: Multi-DB Schemas (Deprecated)](https://docs.turso.tech/features/multi-db-schemas); [Attach Database (Deprecated)](https://docs.turso.tech/features/attach-database). Conflicting: the concepts page still describes Multi-DB Schemas as current. — [docs.turso.tech/concepts](https://docs.turso.tech/concepts) **[docs inconsistent]**. The startup-program post still advertises "schema propagation across millions of databases". — [Turso Startup Program](https://turso.tech/blog/turso-for-startups-the-many-database-architecture) **[possibly stale]**
- Embedded replicas: "Usage of embedded replicas is strongly discouraged." "With embedded replicas, reads are local but writes are sent to the remote database". The newer `turso` crate sync API keeps reads and writes local with explicit push and pull. TS/Python docs recommend `@tursodatabase/sync` / `turso.sync` for new projects. — [Turso Rust SDK docs](https://docs.turso.tech/sdk/rust); [TS reference](https://docs.turso.tech/sdk/ts/reference.md); [Python reference](https://docs.turso.tech/sdk/python/reference.md); [Embedded replicas](https://docs.turso.tech/features/embedded-replicas)
- Homepage: "Millions of Databases. One Architecture." — one per user, agent, or tenant. — [turso.tech](https://turso.tech/)
- Per-tenant migrations become a fan-out job that must be idempotent and resumable. There are no cross-DB queries because ATTACH is deprecated. — [Softwr comparison](https://www.softwr.com/compare/meilisearch-vs-turso) **[secondary]**
- Concurrent writes, early preview on Turso Cloud from 3 Aug 2026: `BEGIN CONCURRENT` "allows multiple writers to proceed in parallel instead of serializing behind one write lock". The MVCC is adapted from Microsoft Hekaton onto SQLite's B-tree. "Conflicts are detected at commit time at row granularity" (versus page granularity in SQLite's unmerged `begin-concurrent` branch, which "has never been merged into mainline"). Hot-row workloads still conflict and need retry logic. It must be explicitly enabled and requires `tursodb`-type databases. The open-source engine "has supported concurrent writes with MVCC for a couple of months now." — [Turso: SQLite Concurrent Writes Are Here](https://turso.tech/blog/concurrent-writes-on-turso-cloud)
- Turso Database is still described as **beta** in a June 2026 AMA ("currently in beta, but it's where we're headed"). — [Reddit AMA snapshot 2026-06-12](https://reddit.sentinel-team.org/posts/1tvz2dm/snapshots/2026-06-12T03%3A41%3A07.2239Z) **[secondary]**; [The New Stack](https://thenewstack.io/why-we-created-turso-a-rust-based-rewrite-of-sqlite) **[secondary]**
- Reported: Supabase acquired Turso (dated 2 Oct 2026). — [datastudios.org](https://www.datastudios.org/post/supabase-acquires-turso-ai-agents-millions-databases-on-demand) **[single secondary source, UNVERIFIED — treat as rumour until confirmed on turso.tech/supabase.com]**

### Inferences
- Turso's move from "remote primary + local read replica" to "local read/write + push/pull sync" is a move toward local-first with *eventual* reconciliation. That is a conflict-resolution model, not a single-writer fenced log. It does not fit a workflow engine that needs one authoritative ordered history per execution.
- Turso dropped server-side schema propagation, which is a signal. Even the vendor whose pitch is "millions of DBs" found that cross-DB schema management belongs to the application. Harvest would need its own idempotent per-file migrator (already implied by its Diesel/rusqlite split).
- MVCC concurrent writes in a beta Rust engine are not a reason to choose it for durable history in 2026. Harvest's single-writer-per-shard model does not need concurrent writers anyway.

### Gaps
- No published per-database cost or footprint (bytes or RAM per idle DB, cold-open latency) from Turso was found.
- Durability semantics of Turso Sync push (what is acknowledged when, and conflict resolution on pull) were not covered in the sources fetched.
- Whether Multi-DB Schemas were rebuilt on the new architecture (as promised Jan 2025) is unknown.

## rqlite and dqlite: Raft-replicated SQLite, throughput, determinism

### Takeaway
rqlite replicates **SQL statements** through Raft and so must rewrite non-deterministic functions. Its rewrite misses defaults, triggers, generated columns, CHECK constraints, views, index expressions, and `RANDOM()` in `ORDER BY`. dqlite replicates **WAL page frames** through Raft via a custom VFS: deterministic by construction, more bytes on the wire, and the write lock is held until the Raft entry commits. Both are CP and synchronously replicated, so the RPO is effectively 0 for acknowledged writes and every write pays a quorum round trip.

### Cited Findings
- rqlite: "every SQL statement is usually stored in the Raft log exactly in the form it was received". Non-deterministic functions could make nodes diverge. The receiving node rewrites `RANDOM()`, `RANDOMBLOB(N)`, `now`/`CURRENT_TIMESTAMP`/`CURRENT_DATE`/`CURRENT_TIME` into literals before logging. There is no rewrite inside `ORDER BY` ("may result in different data on each node"), column defaults, CHECK constraints, generated columns, index expressions, triggers, or views. Those are evaluated per node with each node's own clock. Rewrites can be turned off per request (`norwrandom`, `norwtime`). — [rqlite docs: Non-deterministic functions](https://rqlite.io/docs/api/non-deterministic/)
- dqlite: the modified pages of a write transaction are encoded into a Raft log entry. "the Raft log is therefore what gets persisted on disk, rather than regular SQLite database files". The WAL is updated only after commit, and the write lock is held during replication, so no new write transaction can start meanwhile. — [Canonical dqlite docs: Replication](https://canonical.com/dqlite/docs/explanation/replication)
- dqlite replicates pages, not statements: "more data flowing between nodes, so expect lower performance". On partition it "chooses consistency and sacrifices availability", with a configurable timeout. dqlite dropped its earlier patched-SQLite approach for a custom VFS because upstream changes were hard to merge. — [go-dqlite on pkg.go.dev](https://pkg.go.dev/github.com/CanonicalLtD/go-dqlite) **[possibly stale; older package README]**

### Inferences
- Page or frame replication (dqlite, LiteFS, Litestream, mvSQLite) avoids rqlite's determinism problem entirely. A Harvest design that replicates SQLite should ship pages or frames, never statements. Harvest already logs determinism-sensitive data as events, so a Postgres event log is effectively "logical replication of already-decided facts". It has no determinism hazard either.
- Per-write Raft quorum on top of SQLite's single write lock gives one serialized replicated write path per database. Running one Raft group per shard or entity file multiplies heartbeats and log state per file. That is the classic multi-Raft cost, and neither project targets thousands of groups per node.

### Gaps
- No primary throughput numbers for rqlite (bulk or queued writes pages not fetched) or dqlite were obtained.
- No source on running many dqlite or rqlite clusters per node.

## mvSQLite / SQLite on FoundationDB

### Takeaway
mvSQLite is a SQLite VFS over FoundationDB, with an `mvstore` server colocated with FDB. It provides optimistic, fine-grained `BEGIN CONCURRENT`-like MVCC, namespaces as the tenancy unit, and lifts FDB's 5 s transaction limit. It inherits FDB's strict serializability and replication, so it is a real source of truth. But it is a one-person experimental project (v0.2.x) that requires running FoundationDB.

### Cited Findings
- It "integrates with SQLite as a custom VFS layer". `mvstore` is "the server-side half that should be colocated with the FoundationDB cluster in production". It offers "Optimistic fine-grained concurrency with BEGIN CONCURRENT-like semantics" and no distributed lock in the data plane. It claims the FDB 5 s transaction limit no longer applies and that a SQLite transaction can be ~39x larger than a native FDB one. Tenancy unit: namespaces created via the admin API. Experimental "commit groups" commit across ATTACHed DBs atomically (conflict surfaces as `SQLITE_PERM`) with no shared cross-DB snapshot. Experimental features "should not be used in production without validation." The repo shows install instructions for v0.2.1 with FDB 7.3.69, Apache-2.0, 234 commits, ~1.6k stars, not archived. — [losfair/mvsqlite README](https://github.com/losfair/mvsqlite)

### Inferences
- mvSQLite shows that "page-level MVCC over a transactional KV store" gives SQLite semantics with real multi-writer fencing (FDB transactions conflict-check). The cost is that the local file is no longer local: every page read can be a network fetch. That fails Harvest's goal of "local embedded SQLite".

### Gaps
- Last-commit date and maintenance activity in 2026 could not be confirmed from the fetched page.
- No published throughput or latency benchmarks.
- Whether versioning is per page is asserted in the task brief and folklore, but the README defers internals to the wiki (not fetched).

## Cloudflare D1 / Durable Objects SQLite (brief)

### Takeaway
Cloudflare is the strongest production example of "one SQLite per entity as source of truth". It makes that safe only by wrapping each local SQLite in a platform-level synchronous quorum log (reported: 5 replicas, ack after 3) plus a single-owner actor runtime. The SQLite file is not durable on its own.

### Cited Findings
- Durable Object SQLite storage forwards each write to five replicas in nearby data centres and acknowledges after three confirm. It offers 30-day point-in-time recovery. — [Simon Willison, 13 Oct 2024](https://simonwillison.net/2024/Oct/13/zero-latency-sqlite-storage-in-every-durable-object/) **[secondary summary of Cloudflare's blog; possibly stale]**
- A SQLite-backed Durable Object has a 10 GB max on Workers Paid (1 GB free). It returns `SQLITE_FULL` when full. — [Cloudflare DO FAQ (preview mirror)](https://8f23c8e1.previews.developers.cloudflare.com/durable-objects/reference/faq/index.md) **[preview mirror]**
- D1: 10 GB per database, not raisable. It is designed to scale out over many small per-user or per-tenant DBs. Each DB is single-threaded and processes queries one at a time. — [Cloudflare D1 limits](https://developers.cloudflare.com/d1/platform/limits/index.md)

### Inferences
- The Durable Objects design matches Harvest's "shard owner + local SQLite" idea almost exactly: a single-threaded owner with colocated SQLite. Cloudflare still does not treat the local file as durable. The durable thing is the replicated output log, and the SQLite file is effectively a materialization of it. This supports "Postgres log = truth, local SQLite = rebuildable cache".

### Gaps
- Cloudflare's own primary blog post on DO SQLite storage ("Zero-latency SQLite storage in every Durable Object") was not fetched directly. The 5/3 quorum figure is via a secondary source.
- The per-account D1 database count limit was not captured.

## Many SQLite databases per node: fds, open/close cost, checkpoints, inodes, backup

### Takeaway
I found no rigorous published benchmark of thousands to millions of SQLite files per node. The binding constraints cited by practitioners are the OS per-process fd limit (each WAL-mode DB holds the main, `-wal`, and `-shm` files), filesystem metadata and inodes at roughly 50k+ tenants, per-file page cache memory, and the fact that each file has its own single write lock and its own checkpoint. The universal mitigation is open-on-demand with an LRU close of idle handles.

### Cited Findings
- SQLite has no cap of its own on open database files per process. The practical ceiling is the OS per-process fd limit, which a fleet of thousands of tenants hits without tuning. `ATTACH` defaults to 10 per connection (compile-time max 125, `SQLITE_MAX_ATTACHED`), and each attached DB has its own page cache. — [DEV: Per-user database files vs row-level](https://dev.to/helperx/multi-tenant-data-isolation-in-sqlite-per-user-database-files-vs-row-level-5glm) **[secondary]**; [coddy.tech ATTACH docs](https://coddy.tech/docs/sqlite/attach-database) **[secondary]**
- A practitioner reports that at 50,000+ tenants you start hitting filesystem limits (inode counts, directory size). One writer per file bounds each tenant's peak write concurrency. — [DEV: Per-user database files](https://dev.to/helperx/multi-tenant-data-isolation-in-sqlite-per-user-database-files-vs-row-level-5glm) **[secondary]**
- A Rails per-tenant SQLite ("shardines") write-up and HN discussion cover the operational weight of many tenant shards. — [Julik Tarkhanov, Apr 2025](https://blog.julik.nl/2025/04/a-can-of-shardines); [HN 43811400](https://news.ycombinator.com/item?id=43811400) **[secondary, not fetched in full]**. A counter-argument: "You probably don't need a database per tenant." — [ollieread.com](https://ollieread.com/articles/you-probably-dont-need-a-database-per-tenant) **[secondary, opinion, not fetched]**
- Litestream v0.5's revamp explicitly targets replicating "hundreds or thousands" of DBs from one process (`/data/*.db`). — [Litestream: Revamped](https://fly.io/blog/litestream-revamped/)
- Related prior art: an `autumn-foundation/autumn` PR proposes "SQLite database fleet: one database per tenant or per slot (ADR 0019)", with files opened lazily on first use. — [autumn-foundation/autumn PR #3226](https://github.com/autumn-foundation/autumn/pull/3226) **[search snippet only; not fetched — the report writer should check whether this is the same org as Autumn Harvest]**

### Inferences
- In WAL mode each open DB costs about 3 fds (db, `-wal`, `-shm`) plus an mmap of the shm. This is a SQLite design fact, not from a fetched source. Per-file page cache (default about 2 MB) adds up, so 10k open DBs means about 30k fds and tens of GB of potential cache unless `cache_size` is cut. Harvest's own OS file lock per file adds one more fd.
- Every file checkpoints independently. With Litestream controlling checkpoints (autocheckpoint = 0), a fleet needs a scheduler. Otherwise WALs grow unbounded on idle-then-busy tenants.
- Backup of millions of files: object-store PUT counts scale with files × sync interval (1 s default). Idle files cost nothing in Litestream's design, but a burst across many tenants multiplies requests. Restoring a whole node means N independent restores.

### Gaps
- No published numbers for open/close latency of a SQLite WAL DB at scale, per-idle-DB RAM, or Turso's internal per-DB footprint.
- No primary source on inode exhaustion at a specific count (only the 50k+ practitioner remark).

## General lesson: local DB as rebuildable cache vs replicated source of truth

### Takeaway
Every system surveyed that treats a local SQLite file as *authoritative* either (a) accepts an async loss window and has no write fencing (Litestream, LiteFS: about 1 s or "subsecond", loser overwritten), (b) pays a synchronous quorum per write and becomes a distributed database (rqlite, dqlite, Durable Objects' 3-of-5), or (c) moves the pages off-box (mvSQLite on FDB, Litestream VFS). For Harvest, which already has an append-only Postgres event log with fencing, the evidence favours **local SQLite as a rebuildable cache or materialization** over the Postgres log. Treat the SQLite file as truth only for the existing single-node embedded backend.

### Cited Findings
- Async replicators state the loss window plainly: Litestream "changes made within the most recent sync window" can be lost — [Litestream Tips](https://litestream.io/tips/). LiteFS: "A catastrophic crash on the primary would cause these transactions to be lost", and diverged writes are discarded by re-snapshot. — [LiteFS ARCHITECTURE.md](https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md)
- The Litestream lease exists to serialize replicators. Before it, single-writer coordination was the operator's problem. — [S3 Advanced](https://litestream.io/guides/s3-advanced/); [Litestream: Revamped](https://fly.io/blog/litestream-revamped/)
- The synchronous alternatives hold the write lock across a Raft round trip (dqlite) — [Canonical dqlite replication](https://canonical.com/dqlite/docs/explanation/replication). Or they ack after 3 of 5 replicas (Durable Objects) — [Simon Willison](https://simonwillison.net/2024/Oct/13/zero-latency-sqlite-storage-in-every-durable-object/) **[secondary]**.
- Turso deprecated remote-primary embedded replicas for local-write plus push/pull sync. That is an eventual-consistency model. — [Turso Rust SDK docs](https://docs.turso.tech/sdk/rust)
- Fly.io deprioritized LiteFS, the most sophisticated "replicated local SQLite as primary" system, and steers DR users to Litestream backups. — [Fly community: status of LiteFS](https://community.fly.io/t/what-is-the-status-of-litefs/23883); [Sunsetting LiteFS Cloud](https://community.fly.io/t/sunsetting-litefs-cloud/20829)

### Inferences
- **Cache model (recommended for multi-node Harvest):** the Postgres append-only `harvest_events` stays the fenced source of truth. A shard owner materializes a local SQLite file for its executions and rebuilds it from the log after ownership moves. RPO = 0 (inherited from Postgres). RTO is the rebuild time for the shard's live executions, which can be bounded by snapshotting to object storage with a Litestream-style LTX plus TXID high-water mark, then catching up from the log. No split-brain risk lives in SQLite, because a stale owner's local writes are never authoritative and Postgres ownership fencing rejects them. LiteFS's TXID-plus-rolling-checksum idea is a cheap way to detect a stale or diverged cache.
- **Source-of-truth model:** Harvest would need (1) synchronous replication, or acceptance of about 1 s loss of acknowledged workflow progress, which breaks durable-execution guarantees, (2) its own ownership fencing token checked at commit, because none of Litestream or LiteFS stop a deposed host from committing locally, and (3) a per-file migration, checkpoint, and backup fleet manager. That rebuilds dqlite or Durable Objects inside Harvest.
- The existing embedded single-writer SQLite backend (OS file lock, `synchronous=FULL`) is a legitimate single-node source of truth. Litestream could serve as an *optional* DR backup for it (≈1 s RPO), with `wal_autocheckpoint=0` and Litestream controlling checkpoints. It conflicts with any Harvest-driven checkpointing, which must be checked.

### Gaps
- I found no primary-source essay specifically arguing "local DB as cache rebuilt from a log" versus "local replicated DB" for workflow engines. The comparison above is synthesized from the systems' own stated guarantees.
- No published rebuild-time numbers for materializing SQLite from an external event log.
