# Cloudflare Durable Objects as prior art for owner-per-key with colocated embedded storage

Scope note: research done 2026-10-19. "Primary" = Cloudflare docs or Cloudflare blog. "Secondary" = third-party trackers or search summaries. Docs pages fetched were live on the fetch date. The limits page showed "last updated June 1, 2026". Blog posts are dated and may describe behavior that has since changed; flagged where relevant.

## 1. Identity and placement (addressing, single live instance, migration)

### Takeaway
A DO is addressed by a `DurableObjectId` derived from a name (`idFromName`/`getByName`) or minted randomly (`newUniqueId`). Placement is chosen at first access, near the caller or by a best-effort hint; jurisdictions are hard constraints. Objects currently do not relocate after creation, although the 2020 launch promised auto-migration. Single-instance is enforced by checks at event start and at storage access, backed by a storage-layer fence, not by a perfect runtime guarantee.

### Cited Findings
- Launch: closed beta announced 2020-09-28 by Kenton Varda. Pitch: each object has a globally unique ID and "exists in only one location worldwide at a time". `idFromName("my-counter")` maps to "exactly one instance in the whole world". Millions or billions of objects are "not a problem" (primary). — [Cloudflare blog, Introducing Workers Durable Objects (2020-09-28)](https://blog.cloudflare.com/introducing-workers-durable-objects/)
- The 2020 post said Cloudflare can move objects between locations and that objects would auto-migrate toward their users (primary, **dated, superseded**). — [Cloudflare blog 2020](https://blog.cloudflare.com/introducing-workers-durable-objects/)
- Current docs: "Objects do not currently move after creation." Dynamic relocation is planned but not available (primary). — [Data location docs](https://developers.cloudflare.com/durable-objects/reference/data-location/)
- Namespace API (primary). — [Namespace API docs](https://developers.cloudflare.com/durable-objects/api/namespace/)
  - `idFromName(name)` gives a deterministic ID.
  - `newUniqueId({jurisdiction?})` gives a random ID. The caller must store the ID string, for example in KV, another DO or a cookie.
  - `idFromString(str)` throws if the ID came from another namespace.
  - `get(id, {locationHint})` and `getByName(name, {locationHint})` return a stub at once, before any connection exists.
  - `jurisdiction("eu")` returns a jurisdiction-restricted subnamespace.
- First access to a name-derived ID "may require a global check", which "can take up to a few hundred milliseconds". `newUniqueId` skips this check. After first use, the object's location is cached globally (primary). — [Namespace API docs](https://developers.cloudflare.com/durable-objects/api/namespace/)
- Creating a stub does not instantiate the object. The first method call runs `constructor()` and then the handler (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)
- Jurisdictions are `eu`, `us` and `fedramp`. They constrain where the object runs and where its data persists (primary). — [Data location docs](https://developers.cloudflare.com/durable-objects/reference/data-location/)
  - The same name yields different IDs in different jurisdictions.
  - IDs are logged outside the jurisdiction for billing and debugging.
  - `ctx.id.jurisdiction` is readable inside the object, including in alarm handlers for alarms scheduled on or after 2026-03-15.
- Location hints are `wnam`, `enam`, `sam`, `weur`, `eeur`, `apac`, `apac-ne`, `apac-se`, `oc`, `afr` and `me`. Only the first `get()` honors the hint, and it is "not a guarantee". `sam`, `afr` and `me` do not host objects directly; for example, `sam` maps to `enam`. Without a hint, the object is created near the first `get()` (primary). — [Data location docs](https://developers.cloudflare.com/durable-objects/reference/data-location/)
- Global uniqueness caveat (known issues). Uniqueness is checked when an event starts and when storage is accessed (primary). — [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/)
  - An event that runs long without touching storage may belong to a stale instance while a newer instance runs elsewhere.
  - The stale instance's next storage access throws. If it never touches storage, it "may never detect this".
  - Replacement happens on network partition or a software update. That includes class code updates, Workers runtime updates, and turning on `wrangler tail` or dashboard logs.
- Objects can restart at any time: on deploys, on inactivity, on runtime updates, and on "runtime decisions about where to host objects" (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)

### Inferences
- The guarantee is "at most one instance can commit storage writes", not "at most one instance runs code". This matches a lease/fence model. For Harvest, ownership must be fenced at the storage-commit point, and side effects outside storage need their own idempotency.
- Placement is sticky and decided at creation. That is close to a "home node" per key. Cloudflare has not shipped live relocation, which suggests that moving an owner together with its colocated storage is the hard part.
- The name-to-location lookup is a global directory with caching. A self-hosted analogue is a directory table (key → owner node + epoch) with a local cache, which costs a round trip on first contact.

### Gaps
- The ID format, and whether `newUniqueId` IDs encode a home region or jurisdiction, is not documented on the pages fetched. The 64-hex-char format is folklore here and unverified.
- The internal design of the placement directory (the "global check") is not published in the sources found.

## 2. Storage: SQLite backend, legacy KV, SQL API, PITR, durability and replication

### Takeaway
Each SQLite-backed DO has a private SQLite database in the same thread. Since GA on 2025-04-07 it holds 10 GB per object, with unlimited objects and unlimited account storage on Paid. Durability comes from the Storage Relay Service (SRS). SRS hooks the SQLite VFS in WAL mode and streams WAL frames to 5 followers in different data centers. A write is confirmed after 3 of the 5 acknowledge it. Logs and snapshots are batched to object storage, which gives a 30-day PITR window.

### Cited Findings
- Source post: "Zero-latency SQLite storage in every Durable Object", Kenton Varda, 2024-09-26 (primary). — [Cloudflare blog](https://blog.cloudflare.com/sqlite-in-durable-objects/)
  - SRS also powers D1. It uses "local disk as a cache on top of object storage". It "always configures SQLite to use WAL mode" and intercepts WAL writes by hooking SQLite's VFS. The design was inspired by Litestream, but the implementation differs.
  - Replication: "SRS will immediately forward the change log to five 'follower' machines across our network. Once at least three of these followers respond that they have received the change, SRS informs the application that the write is confirmed."
  - Each follower is in a different physical data center. Losing a confirmed write would take about four machines in three buildings failing together.
  - Followers buffer changes on local disk. If they get no "persisted" notice before a timeout, they upload the change to object storage themselves.
  - Object-storage upload batching: "up to 10 seconds, or up to 16 MB worth, whichever happens first". A full snapshot is uploaded when the logs since the last snapshot exceed the database size. That bounds storage and reconstruction downloads at about 2x the database size.
  - Object-storage latency is "tens or hundreds of milliseconds". Local queries "can complete in microseconds". Example: 101 queries at 5 ms network latency would take about 505 ms with a remote database.
  - DOs "scale out, not up". Each object is one thread on one machine.
  - At the time of the post, the storage backend was chosen per class at creation and "you cannot change it later". Migrating existing KV-backed DOs was planned for 2025. **Current status of that migration path was not verified.**
  - The beta limit was 1 GB per object, with 10 GB promised at GA.
- GA: on 2025-04-07, SQLite-backed DOs and `sql.exec` moved to general availability. Storage per object rose to 10 GB. SQLite DOs became available on the Workers Free plan (primary changelog, read via search summary). — [Changelog 2025-04-07](https://developers.cloudflare.com/changelog/2025-04-07-durable-objects-free-tier/)
- Current limits, SQLite backend (primary). — [Limits page, updated 2026-06-01](https://developers.cloudflare.com/durable-objects/platform/limits/)
  - Unlimited objects. 500 classes on Paid, 100 on Free.
  - Account storage: unlimited on Paid, 5 GB on Free.
  - 10 GB per object on Paid. The FAQ says 1 GB on Free, which the page itself contradicts.
  - Key + value ≤ 2 MB. String, BLOB or row ≤ 2 MB. SQL statement ≤ 100 KB.
  - 100 columns per table, 100 bound parameters, 32 function args, 50-byte LIKE/GLOB pattern.
  - At the size limit, writes fail with `SQLITE_FULL`; reads and `DELETE` still work. Storage is measured in decimal GB.
- Legacy KV backend (primary). Free plans get SQLite only. Paid accounts can use KV only if they already have a KV-backed namespace. KV limits: 50 GB per account (raisable), 2 KiB keys, 128 KiB values. — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- SQL API (primary). — [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
  - `ctx.storage.sql.exec()` returns a cursor and is synchronous. It cannot run `BEGIN TRANSACTION` or `SAVEPOINT`; use `transactionSync(cb)` instead. The callback must be synchronous and rolls back if it throws.
  - Cursors held across an `await` have **no snapshot isolation**. They can see later writes, including uncommitted ones.
  - Numbers are limited to JS 52-bit precision.
  - `rowsRead`/`rowsWritten` feed billing. `databaseSize` gives the size in bytes.
  - A synchronous KV API (`ctx.storage.kv`) stores data in a hidden `__cf_kv` table.
  - `sync()` resolves when all pending writes, including those sent with `allowUnconfirmed`, are persisted.
- PITR (primary). Restores the whole SQLite database (SQL + KV) to any point in the last 30 days. It is on by default and needs no opt-in. Old logs and snapshots are marked for deletion 30 days later instead of being deleted. — [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/); [blog 2024-09-26](https://blog.cloudflare.com/sqlite-in-durable-objects/)
  - API: `getCurrentBookmark()`, `getBookmarkForTime(ts)` and `onNextSessionRestoreBookmark(bookmark)`, followed by `ctx.abort()` to restart into the restored state.
  - Restore returns a bookmark for the moment before recovery, so a restore is undoable.
  - Bookmarks sort lexically in time order. PITR does not work in local development.
- Pricing at the time of the 2024 post, Workers Paid: 5 GB-month included, then $0.20/GB-month. SQL billing was planned for H1 2025 (primary, **dated**). — [blog 2024-09-26](https://blog.cloudflare.com/sqlite-in-durable-objects/)

### Inferences
- The SRS design maps directly onto Harvest's embedded single-writer SQLite. It runs SQLite in WAL mode with a VFS hook, ships WAL frames to N followers with quorum acks, and batches cold upload to object storage with size-triggered snapshots. Confirmation latency is one quorum round trip to nearby data centers, while reads stay local.
- The 30-day PITR comes almost for free from "delay deletion of logs and snapshots". It is a cheap feature if Harvest ever ships WAL to object storage.
- The 10 GB per-object cap is a hard bound that pushes users to model one key = one bounded state. For a workflow engine, per-run history fits easily. Per-namespace aggregates do not.

### Gaps
- How the legacy KV backend stored data internally was not covered by the sources fetched.
- No public figure for follower placement (same metro or cross-region) beyond "different physical data center".
- Whether KV-backed objects can now be migrated to SQLite was not verified as of 2026.

## 3. Input gates, output gates, implicit transactions

### Takeaway
DOs give serializable-looking behavior without explicit transactions. Input gates block delivery of new events while a storage operation is in flight. Output gates hold every outgoing message (responses and outgoing `fetch`) until the writes before it are durable. Writes with no `await` between them coalesce into one atomic batch. If a write fails, the queued output is replaced with errors and the object restarts.

### Cited Findings
- Source: "Durable Objects: Easy, Fast, Correct — Choose three", Kenton Varda, 2021-08-03 (primary). — [Cloudflare blog](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/)
  - Input gate: "While a storage operation is executing, no events shall be delivered to the object except for storage completion events." Deferred events wait until the object is idle. Concurrent storage calls inside one handler are still allowed.
  - Output gate: "any new outgoing network messages will be held back until the write has completed." This covers client responses and outgoing `fetch()`. If the write fails, the messages are replaced with errors and "the Durable Object will be shut down and restarted from scratch."
  - Implicit transactions: writes with no intervening `await` "are automatically grouped together and stored atomically". Coalescing also applies to awaited writes, so the output gate waits about O(1) round trips, not O(n).
  - In-memory cache of "several megabytes". `put()` "simply writes to cache". Operations complete "in exactly the order in which they were initiated".
  - Caveats: same-event concurrency, such as two unawaited calls to the same counter, can still race. Opt-outs are `allowConcurrency`, `allowUnconfirmed` and `noCache`.
- Current docs: "Input gates only protect during storage operations." Non-storage I/O such as `fetch()` lets other requests interleave. Output gates hold responses until writes persist (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- Each storage method runs in an implicit transaction. A run of reads then writes with no other I/O between them "behaves like a transaction". On SQLite objects, the `txn` object from `transaction()` is obsolete (primary). — [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- With SRS, responses are held "until all storage writes relevant to the response have been confirmed". A failed write replaces the response with an error and restarts the DO (primary). — [blog 2024-09-26](https://blog.cloudflare.com/sqlite-in-durable-objects/)
- `blockConcurrencyWhile()` is for initialization and migrations only. It must not span I/O, since it blocks all events (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)

### Inferences
- The output gate is the key idea for Harvest. Run the owner's state machine against local SQLite at memory speed, and gate externally visible effects (task dispatch, RPC replies, client acks) on the commit watermark. This is "group commit + hold replies until durable". It removes the need for the application to await each write.
- "On write failure, crash and restart from durable state" is a clean failure model. It fits deterministic replay well: after a restart the owner rebuilds from committed history only.
- Input gates do not cover external I/O. Interleaving across `await fetch()` is still the user's problem. Harvest's deterministic, command-based workflow code already avoids this.

### Gaps
- No public description of how SRS orders multiple outstanding write batches, or of output-gate behavior under partial quorum, beyond "replace with error + restart".

## 4. Alarms

### Takeaway
One alarm per object, at millisecond precision. Execution is at-least-once, with up to 6 retries from 2 s exponential backoff. Users build multi-timer schedulers in SQL on top of the single alarm.

### Cited Findings
- `setAlarm()` overrides any existing alarm. Only one `alarm()` runs per object at a time (primary). — [Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)
- Retries (primary). — [Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)
  - "Alarms have guaranteed at-least-once execution." A throwing handler is retried with exponential backoff from 2 s, up to 6 retries. After that, the alarm needs a new `setAlarm()`.
  - The handler gets `retryCount` and `isRetry`. `deleteAlarm()` inside `alarm()` prevents retries only on a best-effort basis.
  - The constructor runs before the alarm handler on wake. Check `getAlarm()` first so the constructor does not overwrite the pending alarm.
- "In rare cases, alarms may fire more than once." Make `alarm()` idempotent, and reschedule before retries run out (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- An alarm handler gets 15 minutes of wall time per invocation and the 30 s default CPU budget (primary). — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- The Agents SDK stores many schedules in SQL (`cf_agents_schedules`) and drives them from the single alarm, because "Durable Objects only allow one alarm at a time" (primary). — [Agents SDK, Agent class](https://developers.cloudflare.com/agents/concepts/agent-class/)

### Inferences
- "One durable wake-up per owner + a local timer table" is a clean primitive. Harvest's timers per run could live in the run's own store, with one next-wake entry per owner in a global wake index. That is the scheduler a self-hosted system needs to wake a cold owner.

### Gaps
- The maximum backoff interval and the total retry window are not documented.

## 5. Fencing, split-brain and RPC guarantees

### Takeaway
Split-brain writes are prevented at the storage layer. SRS will not let a new instance start until the old one is known dead. If the old host is unreachable, a quorum of 3 of the 5 followers tells the old instance to stop confirming writes. A stale instance can still run code until it touches storage. RPC between DOs has no documented exactly-once or at-least-once guarantee. Errors carry `.retryable`/`.overloaded`/`.remote` flags, and retry is safe only for idempotent calls.

### Cited Findings
- Fencing: "We cannot start up a new instance of the DO until we know for sure that the previous instance is dead." Otherwise "the old and new instances could then confirm contradictory writes." If the host is unreachable, SRS contacts the followers. Once at least 3 of 5 agree, the old instance is told to stop confirming writes, and a new instance can start (primary). — [blog 2024-09-26](https://blog.cloudflare.com/sqlite-in-durable-objects/)
- A stale instance detects staleness only on storage access, which throws (primary). — [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/)
- On shutdown, in-flight HTTP/RPC requests may finish unless they access storage, which is stopped at once with an error. Runtime updates give requests up to 30 s. WebSockets are terminated (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)
- Error flags (primary). — [Error handling](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/)
  - `.retryable` means a retry is suggested, "but only if the requests are idempotent". Use exponential backoff.
  - `.overloaded` means do not retry, because "retrying will worsen the overload".
  - `.remote` means the error came from user code, or from exceeding memory or CPU limits.
  - Many errors leave the stub broken, so create a new stub per retry. The doc example uses 100 ms base, a 20 s cap and 3 attempts.
- Infrastructure error strings (primary). — [Troubleshooting](https://developers.cloudflare.com/durable-objects/observability/troubleshooting/)
  - "Durable Object reset because its code was updated." Here, "Reset in error messages refers to in-memory state", and persisted data is unaffected.
  - "Durable Object storage operation exceeded timeout which caused object to be reset." This can come from `deleteAll()` on large KV objects. Each call makes progress, so retrying is safe.
  - "Your account is generating too much load on Durable Objects. Please back off and try again later." This is a stub-creation rate limit.
  - "Your account is doing too many concurrent storage operations…"
- Code rollouts are eventually consistent. For seconds to minutes, a new Worker version may call a DO still on old code. Keep the Worker↔DO API compatible both ways (primary). — [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/)

### Inferences
- The protocol is a lease with quorum-based revocation. The quorum that confirms writes is also the quorum that fences an old writer. Harvest can copy this: the replica set that acks WAL frames also holds the owner epoch, so a new owner first collects a quorum of "stop accepting epoch N" responses. In a Postgres-backed mode, a CAS on an `owner_epoch` column in the same transaction as each commit gives the same property.
- RPC outcomes after a connection error are ambiguous (the call may or may not have run). That forces idempotency keys for owner-to-owner calls. Harvest should treat cross-owner signals as at-least-once delivery with dedup at the receiver, recorded in the receiver's history.

### Gaps
- The exact semantics of "Network connection lost" and of DO-to-DO RPC delivery (at-most-once vs at-least-once, ordering) are not in the docs fetched.
- No public detail on how long a fenced instance takes to be replaced, that is, the unavailability window during failover.

## 6. Hibernation, eviction, cold start

### Takeaway
An idle, hibernateable object can be hibernated after 10 s. Server-side WebSockets stay connected and the object is not billed for duration. A non-hibernateable idle object is evicted after about 70–140 s. Every wake re-runs the constructor. No cold-start latency figure is published.

### Cited Findings
- Lifecycle docs (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)
  - States are: active; idle non-hibernateable; idle hibernateable; hibernated; inactive.
  - Hibernation can happen after 10 s idle. It requires no pending timers, no unfinished I/O or `waitUntil`, no outbound connections, and no standard (non-hibernation) WebSocket API in use.
  - Eviction happens after about 70–140 s idle when the object cannot hibernate.
  - Pending outbound operations block eviction, each for up to 15 minutes.
  - No shutdown hooks exist, so the docs say to "write progress to storage incrementally".
- WebSocket Hibernation (primary). — [WebSockets docs](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
  - `ctx.acceptWebSocket(ws)` allows hibernation. `serializeAttachment` holds at most 16,384 bytes per socket.
  - Automatic ping/pong replies do not wake the object. Close frames get auto-replies with compatibility date 2026-04-07 or later.
  - "Billable Duration (GB-s) charges do not accrue during hibernation."
  - The constructor re-runs on wake. Only incoming WebSockets hibernate. Deploys disconnect all WebSockets. The docs claim "thousands of clients per instance".
- The cold-start delay is not quantified. The docs only say an inactive object may need to cold start. The first `idFromName` access may add "up to a few hundred milliseconds" for the global check (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/); [Namespace API docs](https://developers.cloudflare.com/durable-objects/api/namespace/)

### Inferences
- In-memory state is a cache that can vanish at any time, and the constructor is the rehydration hook. This matches a workflow owner that keeps a sticky replay cache and rebuilds from history on wake.

### Gaps
- No published cold-start latency (isolate start + SQLite open + SRS fetch) was found.

## 7. Global listing and search

### Takeaway
There is no in-Worker API to enumerate or query across objects. Cloudflare guides users to keep their own index: a parent DO that tracks children in its SQL, or an external store such as D1 for `newUniqueId` IDs. Workers KV is suggested for global read-mostly config. A REST management API can page through the object IDs in a namespace, but it is not a query layer.

### Cited Findings
- Parent-child pattern: the parent stores child references in its own SQL and "knows about all matches without waking up each child". For `newUniqueId()` objects: "You must store this ID somewhere (e.g., D1) to find it again later" (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- Do not build a single global DO; "the single Durable Object becomes a chokepoint". Use Workers KV for frequently read global configuration (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- The Cloudflare REST API has an endpoint that lists the objects in a namespace with cursor pagination. It returns `id` and `hasStoredData` per object (primary API reference, read via search summary). — [API: namespace list objects](https://developers.cloudflare.com/api/operations/durable-objects-namespace-list-objects)
- Workflows keeps a per-account "Account Controller" DO that stores workflows, versions and instances. That is a first-party example of the index-DO pattern (primary). — [Workflows blog 2024-10-24](https://blog.cloudflare.com/building-workflows-durable-execution-on-workers/)

### Inferences
- Owner-per-key with colocated storage gives up cross-key queries. Any visibility or listing feature, like Temporal's "list workflows", needs a separate secondary index. That index is either written synchronously as part of the owner's commit (an extra round trip) or fed asynchronously (eventually consistent). Harvest would need to decide which.

### Gaps
- No primary guidance was found on using Analytics Engine or Logpush as a search index for DOs. Treat that pattern as unverified.

## 8. Limits (as of the 2026 docs)

### Takeaway
The per-object envelope is: one thread, about 1,000 req/s soft limit, 30 s default CPU per request (up to 5 min), 10 GB SQLite, 128 MB isolate memory per Workers docs, and 15 min alarm wall time.

### Cited Findings
- Throughput: soft limit of 1,000 req/s per object. Overloaded objects return "overloaded" errors (primary). — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- Rule-of-thumb throughput tiers (primary). — [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
  - About 1,000 req/s for pass-through work.
  - About 500–750 req/s with JSON parsing.
  - About 200–500 req/s with storage writes.
  - About 200 req/s when using a 5 ms `blockConcurrencyWhile`.
- CPU: 30 s per request by default, configurable to 5 min (`limits.cpu_ms = 300000`). Each request may hold 6 outgoing connections. HTTP/RPC to a DO has no wall-time limit while the caller stays connected. Alarms get 15 min wall time (primary). — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- Memory: the DO limits page states **no** memory limit. The Workers limits page gives 128 MB per isolate (JS heap + Wasm). An isolate can serve many concurrent requests, and over-limit isolates are replaced. The Workers page does not say explicitly that this applies to DOs (primary; DO applicability inferred). — [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- Storage per object: 10 GB on Paid for SQLite. See section 2 for the rest (primary). — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- Four documented overload variants (primary). — [Troubleshooting](https://developers.cloudflare.com/durable-objects/observability/troubleshooting/)
  - "Too many requests queued"
  - "Too much data queued"
  - "Requests queued for too long"
  - "Too many requests for the same object within a 10 second window"

### Inferences
- About 1k simple ops/s per key is the planning number for a single-threaded owner with local SQLite and quorum-replicated commits. Harvest's per-run owner would see far fewer events than that, so the real bottleneck is hot shared keys (namespaces, task queues), not runs.

### Gaps
- An explicit DO memory limit is not on the current DO limits page. Older docs and folklore say 128 MB per object. Whether several objects share one isolate's 128 MB was not confirmed.

## 9. How Cloudflare Workflows and the Agents SDK build on DOs

### Takeaway
Each Workflow instance is an "Engine": an SQLite-backed DO that caches step results by step name and resumes via alarms. This is memoization, not Temporal-style deterministic replay of commands. Each Agent is a DO that uses `ctx.storage.sql`, keeps state and schedules in SQL tables, and multiplexes schedules onto the single alarm.

### Cited Findings
- Workflows architecture, published 2024-10-24 by Sid Chatterjee, Matt Silverlock and Celso Martinho (primary). — [Workflows blog](https://blog.cloudflare.com/building-workflows-durable-execution-on-workers/)
  - "every workflow instance is an Engine behind the scenes, and every Engine is an SQLite-backed Durable Object."
  - A per-account Account Controller DO stores workflows, versions and instances. Components talk over JS-native RPC.
  - The Engine is modeled as a "game loop". `step.do()` checks a cache keyed by step name and runs the callback only on a miss.
  - `step.sleep()` and retries use `scheduler.wait()` plus DO alarms. After eviction, pending steps "get rerun on the next lifetime of the Engine", which hydrates the cache from storage.
  - Steps should be "stateful and idempotent".
- Workflow steps have unlimited wall time, but CPU limits still apply (primary). — [Limits page](https://developers.cloudflare.com/durable-objects/platform/limits/)
- Agents SDK (primary). — [Agent class docs](https://developers.cloudflare.com/agents/concepts/agent-class/)
  - "agents *are* Durable Objects" (Agent → partyserver `Server` → `DurableObject`). `this.sql` uses the synchronous `ctx.storage.sql`.
  - State lives in `cf_agents_state` and is broadcast over WebSockets. Schedules live in `cf_agents_schedules` behind one alarm.
  - Hibernation is on by default. `keepAlive()` uses alarm heartbeats.

### Inferences
- Cloudflare's own durable-execution product shows that one run = one owner with private embedded SQLite works at scale for step-memoized workflows. Harvest's strict command-matching replay is stricter than Workflows' name-keyed cache. The storage and ownership substrate carries over; the replay semantics do not.

### Gaps
- Current Workflows limits (max steps, payload size, instance retention) were not fetched in this pass.

## 10. Failure modes, incidents and complaints

### Takeaway
The documented failure modes are the hot-key or single-thread ceiling, resets on code or runtime updates, storage timeouts on large `deleteAll`, stub-creation rate limits, and stale instances. Public DO-specific postmortems are rare. DOs appear as a dependency in larger Cloudflare outages, and the 2026 status entries are short regional incidents with no published root cause.

### Cited Findings
- Hot key: one object "cannot do more work than is possible on a single thread". The fix is sharding (primary). — [Troubleshooting](https://developers.cloudflare.com/durable-objects/observability/troubleshooting/); [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- Code updates: the rollout shuts down running objects and disconnects all WebSockets, and an old/new version mix lasts seconds to minutes. Turning on `wrangler tail` also triggers an update that replaces objects (primary). — [Lifecycle docs](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/); [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/)
- 2025-02-06 R2 incident: R2's metadata layer is built on DOs. It saw load problems as clients reconnected, and DO calls in North America saw a 0.09% error-rate increase (primary postmortem, read via search summary; not fetched directly). — [Cloudflare blog, incident 2025-02-06](https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025)
- 2025-06-12 outage: Workers KV's dependency failed. DOs depended on KV in that path, which hit products such as Access SCIM (primary postmortem, read via search summary). — [Cloudflare blog, outage 2025-06-12](https://blog.cloudflare.com/cloudflare-service-outage-june-12-2025/)
- 2026 incidents (secondary trackers):
  - 2026-07-28: DO issues in WNAM from 14:29 to 14:55 UTC.
  - 2026-09-25: a "Minor" DO incident listed at 14h 45m with elevated errors, plus a Workflows issue in the same window.
  - 2026-10-01: about 19 min of elevated DO and R2 errors tied to Amsterdam, with no postmortem.
  - Sources: [uptimerobot](https://uptimerobot.com/is-it-down/cloudflare/2026-10-01/); [pulsetic mirror of Cloudflare status](https://pulsetic.com/status/cloudflare/incidents/141209/)
- "Durable Objects availability degraded in Asia Pacific" appears as a status-history title on a mirror. Its date was not verified (secondary). — [thenote.app mirror](https://thenote.app/post/en/durable-objects-availability-degraded-in-asia-pacific-wunbzbuk9a)

### Inferences
- Because objects do not relocate, a regional problem hits every key homed there (the WNAM and Amsterdam incidents fit this). A self-hosted owner-per-key design needs a failover path that rebuilds an owner elsewhere from the replicated WAL. That is exactly the fencing path in section 5.

### Gaps
- No HN or user-forum threads were retrieved. Searches did not surface specific complaint threads, so user sentiment is unsourced here.
- The root causes of the 2026 incidents are unpublished.
