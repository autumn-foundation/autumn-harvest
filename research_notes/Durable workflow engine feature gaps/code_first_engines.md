# Code-first durable execution engines: capability catalog (as of October 2026)

Scope: Restate, DBOS (Transact + Conductor), Inngest, Hatchet, Trigger.dev, Resonate, Golem, Cloudflare Workflows, Vercel Workflows (Workflow SDK / "use workflow"), LittleHorse, Postgres-native libraries (Absurd, pgflow), and Rust-native engines (Obelisk, Duroxide, Restate Rust SDK). The emphasis is on what differs from Temporal's model.

Research date: 2026-10-07. Status labels (GA / beta / preview / roadmap) are given where a primary source states them. Each engine's facts are grouped under the key question they answer. The last key question is the cross-engine summary.

---

## Execution model: journaling vs replay, push vs pull, embedded library vs server, Postgres-only designs

### Takeaway
The newer engines split along three axes that Temporal does not offer as options:
- **Push vs pull.** Push engines (Restate, Inngest, Vercel, Cloudflare) call your HTTP or serverless handler. Pull engines (Hatchet, DBOS, Absurd, Trigger.dev, LittleHorse) run workers that poll or hold connections.
- **Embedded vs server.** Some are an embedded library over Postgres (DBOS, Absurd, pgflow). Others are a separate server (Restate, Hatchet, LittleHorse, Resonate, Obelisk).
- **What persists.** Most use step memoization or replay. Golem instead records an oplog of the whole WASM process. Trigger.dev instead checkpoints the whole container with CRIU.

### Cited Findings

**Restate**
- Restate journals every step. Durable steps wrap non-deterministic operations, and "Restate persist[s] its result". The primitives are Services (stateless), Virtual Objects (keyed, single-writer) and Workflows (run-once) — [Restate docs: durable building blocks](https://docs.restate.dev/concepts/durable_building_blocks)
- Durable RPC, one-way messages and delayed messages all get exactly-once semantics through journaling — [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- Restate 1.7 rebuilt the internal scheduler around Virtual Queues (vqueues). The release blog calls it "the biggest release since introducing distributed Restate" — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- Release dates for 1.7 conflict. The server changelog lists v1.7.0 on 2026-06-18; the blog summary gives 2026-07-07 — [server changelog](https://docs.restate.dev/changelog/server.md); [1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- 1.7 adds native Google Cloud Run OIDC authentication, so Restate pushes invocations to Cloud Run without long-lived bearer tokens (GA) — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- 1.7 adds HTTP/2 connection pooling for service invocations and a bounded invoker memory pool (1.5 GiB default) — [server changelog](https://docs.restate.dev/changelog/server.md)

**DBOS**
- DBOS is "an open-source library you integrate directly into your application", not a separate orchestration server. "There's no separate orchestration server and no infrastructure required besides Postgres." — [DBOS architecture](https://docs.dbos.dev/architecture)
- Recovery has three phases: detect interrupted workflows, re-execute with the original inputs while skipping steps with cached outputs, then resume from the first uncompleted step. Workflows must be deterministic and steps must be idempotent — [DBOS architecture](https://docs.dbos.dev/architecture)
- Conductor stays "off your workflows orchestration path". It connects outbound by websocket from the app servers, so it needs no direct database access, and apps keep running if the connection drops — [DBOS architecture](https://docs.dbos.dev/architecture)
- Throughput claim (May 2026): one Postgres server sustains 144K writes/s, which DBOS equates to 43K workflows/s ("4 billion workflows/day"). The optimizations named are in-memory worker concurrency tracking, READ COMMITTED isolation where global limits are not needed, and partial indexes — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)
- August 2026: several DBOS applications, including ones in different languages, can share one system database. They are isolated by default but can coordinate explicitly — [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- DBOS Go v1.0 (August 2026) is production-ready, with database-backed queues and schedules and "SQLite is now optional for Postgres-only applications" — [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- DBOS partners with Cockroach Labs to use CockroachDB as the durable execution state store, for multi-region deployment — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)

**Inngest**
- Inngest is push-based and event-driven, with step memoization. Billing reflects the model: "An execution is one function run or one step execution. A function run with five steps uses six executions." — [Inngest usage limits](https://www.inngest.com/docs/usage-limits/inngest)
- **Checkpointing** became the default in TypeScript SDK v4 (v4 GA 2026-03-17). It gives "near-zero inter-step latency" and about 50% shorter workflow duration in internal tests — [releases.sh: checkpointing](https://releases.sh/release/rel_ynTYYDEU4o1SthmySlCwO-checkpointing-near-zero-latency-for-durable-workflows); [Inngest changelog](https://www.inngest.com/changelog)
- **Durable Endpoints** (public beta, 2026-02-10) wrap API handler code in `inngest.endpoint()` and `step.run()`, without a separate queue. Supported on Next.js and Bun — [Inngest changelog](https://www.inngest.com/changelog); [releases.sh](https://releases.sh/release/rel_ejfuprn3hgWq8D5x2fo_b-durable-endpoints-durability-beyond-workflows)
- **Connect** (2025-06-20) gives persistent outbound worker connections, a pull-like option for containers without load balancers — [Inngest changelog](https://www.inngest.com/changelog)
- Self-hosting got an experimental Postgres backend (2025-01-20) and a Helm chart v0.3.0 (2026-02-18) — [Inngest changelog](https://www.inngest.com/changelog)

**Hatchet**
- Hatchet is an open-source (MIT) orchestrator built on Postgres. It serves as "a general-purpose queue, a DAG-based orchestrator, a durable execution engine, or all three" — [Hatchet GitHub (v1)](https://github.com/hatchet-dev/hatchet-v1); [Hatchet docs](https://docs.hatchet.run/home)
- Durable tasks use an event log with replay. Each completed operation "creates a new checkpoint (an entry in a durable event log), from which we can replay" — [Hatchet durable execution](https://docs.hatchet.run/home/durable-execution)
- Each task costs at least 5 Postgres transactions. Hatchet Cloud bursts above 5k tasks/s, which is about 25k transactions/s — [Hatchet v1 GitHub / Show HN](https://github.com/hatchet-dev/hatchet-v1)
- The separate `hatchet-v1` repository was archived on 2025-10-28. Development continues in the main repository, with Go module versions through v0.101.x in 2026 — [hatchet-v1 repo](https://github.com/hatchet-dev/hatchet-v1); [pkg.go.dev](https://pkg.go.dev/github.com/hatchet-dev/hatchet@v0.101.32)

**Trigger.dev**
- Trigger.dev suspends at await points by checkpointing the container: "the platform serializes the task's state using CRIU (Checkpoint/Restore In Userspace) and suspends the container." Code outside steps needs no determinism — [indepth.dev: how Trigger.dev checkpoints containers](https://indepth.dev/posts/1020/en/how-trigger-dev-checkpoints-containers)
- v4 went GA on 2025-08-18 with Run Engine 2. Warm starts take 100-300 ms, against several seconds for a cold start — [Trigger.dev v4 GA changelog](https://trigger.dev/changelog/trigger-v4-ga)
- Max run duration is "Unlimited (no timeouts)" — [Trigger.dev pricing](https://trigger.dev/pricing)

**Vercel Workflows / Workflow SDK**
- `'use workflow'` marks the orchestrator and `'use step'` marks retryable units. Vercel Functions run the code, Vercel Queues dispatch it, and managed persistence stores the event log. Durability comes from "deterministic replays" — [Vercel Workflows docs](https://vercel.com/docs/workflow)
- Persistence uses event sourcing. A normal step produces `step_created`, `step_started` and `step_completed` events, plus `step_retrying` for each retry — [Vercel pricing & limits](https://vercel.com/docs/workflows/pricing.md)
- "Worlds" are pluggable backends: Local, Postgres, Vercel or custom. The Postgres World does not work on Vercel deployments — [Vercel pricing page links](https://vercel.com/docs/workflows/pricing.md); [useworkflow.dev/worlds](https://useworkflow.dev/worlds/vercel)
- Multi-region: each run is pinned to one region for life (state, queue dispatch and streams), and `start(..., { region })` can choose it. This needs `workflow` 5.0.0-beta.33 or later; 4.x runs always live in iad1 — [Vercel Workflows docs](https://vercel.com/docs/workflow)
- "Dynamic workflows" (experimental) start runs from workflow source assembled after deployment, over steps already deployed — [Vercel Workflows docs](https://vercel.com/docs/workflow)

**Cloudflare Workflows**
- The API is step-based: `step.do()`, `step.sleep()`/`step.sleepUntil()` and `step.waitForEvent()`. Instances can be triggered, paused, resumed and terminated through APIs — [Cloudflare Workflows docs](https://developers.cloudflare.com/workflows/)
- GA in April 2025 — [Cloudflare blog: Workflows GA](https://blog.cloudflare.com/workflows-ga-production-ready-durable-execution/)
- Sleeping or waiting instances do not count toward concurrency limits, "enabling millions of waiting instances simultaneously" — [Cloudflare limits](https://developers.cloudflare.com/workflows/reference/limits/index.md)

**Golem**
- Golem records an operation log (oplog) for automatic durability of WebAssembly components. After a crash, agents resume at the last successful point without explicit step annotations — [Golem: why Golem](https://learn.golem.cloud/why-golem)
- Golem promises "exactly-once external effects", durable agent-to-agent messages and suspend-to-zero: idle agents park at zero compute cost — [Golem: why Golem](https://learn.golem.cloud/why-golem)
- Current version 1.5, released end of April 2026 — [Golem v1.5 docs](https://learn.golem.cloud/v1.5/concepts); [API Evangelist summary](https://blogs.apievangelist.com/blogs/golem-cloud-2026-04-14-golem-1-5-features-part-5-scala-support/)

**Resonate**
- The model is "Distributed Async Await": durable promises persist in storage with a unique identity that outlives the function execution. After premature termination the runtime restarts and deduplicates on completed promises. Functions in different processes can await each other — [Resonate docs: durable promise](https://docs.resonatehq.io/concepts/durable-promise); [search summary of distributed-async-await page](https://docs.resonatehq.io/concepts/distributed-async-await)
- The server is a single Rust binary. SDKs exist for TypeScript (v0.10.1), Python (0.6.x), Go and Rust (0.4.0, in active development) — [Resonate develop docs](https://docs.resonatehq.io/develop); [Resonate Rust SDK guide](https://docs.resonatehq.io/develop/rust)

**LittleHorse**
- Workflows are defined in code as WfSpecs, which compile to a server-side spec. Task Workers pull from task queues per TaskDef — [LittleHorse server docs](https://docs.littlehorse.io/server)
- Storage is Kafka Streams with Kafka as the WAL and RocksDB/Speedb as the index. A request is recorded to a Kafka topic and then processed by a Streams processor — [LittleHorse architecture](https://docs.littlehorse.io/server/architecture-and-guarantees)
- Licensed under the SSPL ("free for production use") — [LittleHorse server docs](https://littlehorse.io/docs/server)

**Absurd (Postgres-native)**
- The core is "a single SQL file (absurd.sql)" of stored procedures for tasks, checkpoints, events and claim-based scheduling. It needs no Postgres extension — [Ronacher, Nov 2025](https://lucumr.pocoo.org/2025/11/3/absurd-workflows/)
- Five months in production (April 2026): "code outside step boundaries needn't be deterministic", and pull scheduling removed coordinator complexity. SDKs: TypeScript (~1,400 lines), Python (~1,900 lines), Go (experimental). Ronacher contrasts this with Temporal's Python SDK at about 170,000 lines — [Ronacher, Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)
- Limitations: no built-in scheduler, no push/webhook model, and no partitioning support. Ronacher calls "partition lifecycle management under real workloads" hard — [Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)

**pgflow (Postgres-native)**
- The SQL core manages workflow state in Postgres. A TypeScript DSL compiles DAGs into SQL migrations. Workers are Supabase Edge Functions (`@pgflow/edge-worker`) with retries, concurrency control and auto-restart around Edge Function CPU/memory limits. "No Redis, no Temporal" — [pgflow (gittrend summary)](https://gittrend.io/repo/pgflow-dev/pgflow); [@pgflow/edge-worker README](https://jsr.io/@pgflow/edge-worker/0.16.0/README.md)

**Rust-native engines**
- **Obelisk** is "an open-source deterministic workflow engine that runs, stores, and replays WASM-based workflows using SQLite". It uses the WASM Component Model and WIT IDL for type-safe workflows and activities — [Obelisk 0.24.1](https://obeli.sk/blog/introducing-obelisk-0-24-1/); [Obelisk workflows docs](https://obeli.sk/docs/v0.22.0/concepts/workflows/)
- Obelisk 0.24.1 (2025-09-09) added heterogeneous join sets, scoped join-set close (structured concurrency) and stub activities for external or human work. It also added experimental process spawning, disk IO, and JavaScript and Go support. The project is pre-release — [Obelisk 0.24.1](https://obeli.sk/blog/introducing-obelisk-0-24-1/)
- **Duroxide** is "a lightweight and embeddable durable execution runtime for Rust" inspired by the Durable Task Framework and Temporal. It has deterministic replay, durable timers, external events, fan-out/fan-in, and a durable per-instance KV store that clients and other instances can read. A Python SDK writes workflows as generators over the Rust runtime. Latest crate: 0.1.29 — [docs.rs duroxide](https://docs.rs/crate/duroxide/latest); [PyPI duroxide](https://pypi.org/p/duroxide)

### Inferences
- "Postgres as the only dependency" is now a mainstream selling point: DBOS, Hatchet, Absurd, pgflow, and Inngest self-host with a Postgres backend. DBOS and Absurd go further: no server process at all, only a library plus SQL.
- Three engines escape Temporal-style determinism constraints: Trigger.dev (CRIU snapshots), Golem (WASM oplog) and Absurd (checkpoint-only, with non-deterministic code allowed outside steps). That is a buyer-visible developer-experience difference.
- Push/serverless invocation (Restate, Inngest, Vercel, Cloudflare) wins on serverless platforms. Temporal assumes long-lived pull workers.

### Gaps
- I did not confirm Restate's current Rust SDK feature parity or version. The search results did not cover it.
- The Hatchet docs pages I reached did not say whether durable tasks need deterministic code or how they version code.
- LittleHorse's 2026 release history and version number were not found.
- pgflow's current version beyond edge-worker 0.16.0, and its production status, were not verified.

---

## Distinctive primitives (keyed state, awakeables, promises, flow control, queues, DAGs, waitpoints, fork)

### Takeaway
Flow control is the biggest competitive axis in 2026. Inngest, Hatchet, DBOS and now Restate 1.7 all offer per-key concurrency, rate limits, fairness and priority as declarative function or queue settings. Temporal mostly leaves these to the application. Other distinctive primitives: Restate's keyed Virtual Objects, durable promises and awakeables (Restate, Resonate), DBOS fork and rewind, Trigger.dev waitpoints, and Inngest's event-native debounce, batching, singleton and cancelOn.

### Cited Findings

**Restate**
- Virtual Objects are "keyed, stateful handlers with exclusive single-writer semantics and shared read access". State uses get/set/clear on persistent KV — [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- Awakeables are one-shot primitives with unique-ID completion, for Services and Virtual Objects. Durable Promises are named promises for Workflows only. Signals are reusable named notifications per invocation — [Restate awakeables](https://docs.restate.dev/develop/ts/awakeables); [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- Idempotency comes from idempotency keys and journaling. Timers "consume no resources while sleeping" — [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- **1.6 (2026-01-30):** pause and resume of running invocations, and restart of an invocation from a journal prefix so completed work is kept — [server changelog](https://docs.restate.dev/changelog/server.md)
- **1.7 flow control.** Concurrency limits are GA, hierarchical (organization → team → user) and declarative. They update through the admin API without restarts, and every limit must be met before dispatch — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- Scopes are set by HTTP header or by SDK (`ctx.scope()`, `rpc.opts({ limitKey })`). There are also concurrency limits on deployment endpoints — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- Rate limits, priorities and capacity/backlog limits are on the roadmap, not shipped — [Restate OSS roadmap](https://docs.restate.dev/roadmap/oss.md)
- Also on the roadmap: native shareable, resumable streams; cron schedules; sticky workers — [Restate OSS roadmap](https://docs.restate.dev/roadmap/oss.md)

**DBOS**
- Queues control concurrency (global and per worker), rate limiting, priority and partitions. Partitioned queues apply concurrency limits per partition, which gives tenant fairness — [DBOS queues (TS reference)](https://docs.dbos.dev/typescript/reference/transactapi/workflow-queues); [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- August 2026: partitioned queues got about 10x more throughput from better indexes, batched dequeues and faster SELECT DISTINCT alternatives — [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- **Dynamic queue configuration** (May 2026, TS/Python; Go/Java "coming soon"): `register_queue` persists queue definitions in the system DB. Concurrency, rate limiters, priority, partitions and polling settings change at runtime, and workers pick them up on the next poll without a restart — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)
- **Fork** "generates a new workflow with a new workflow ID, copies… the original workflow's inputs and all its steps up to the selected step". It can target a new application version to fix bugs — [DBOS workflow management](https://docs.dbos.dev/python/tutorials/workflow-management)
- **Rewind** keeps the workflow ID and re-executes from a chosen step, which matters when the ID is an idempotency key — [DBOS workflow management](https://docs.dbos.dev/python/tutorials/workflow-management)
- Cancel preempts an executing workflow, and async steps can be marked preemptible. Resume restarts from the last completed step. Workflow attributes are custom KV metadata for search — [DBOS workflow management](https://docs.dbos.dev/python/tutorials/workflow-management)
- Go v0.14 and Java v0.8 (May 2026) added delayed workflow scheduling with deadlines. They also added dynamic schedules stored in Postgres with runtime create/update, backfill and manual trigger — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)

**Inngest flow control**
- Concurrency (per key, user or resource), throttling, rate limiting, debounce, priority and multi-tenancy grouping. Throttling limits throughput; rate limiting skips excess events; debounce de-duplicates over a sliding window; priority uses "any data". Multi-tenancy grouping reduces "head-of-line blocking" — [Inngest flow control](https://www.inngest.com/docs/guides/flow-control.md)
- **Singleton** (2025-06-06): one run per key, `mode: "skip"` or `mode: "cancel"` — [Inngest singleton](https://www.inngest.com/docs/guides/singleton.md); [Inngest changelog](https://www.inngest.com/changelog)
- **cancelOn** stops a running function when a matching event arrives — [Inngest cancelOn](https://www.inngest.com/docs/reference/typescript/v4/functions/cancel-on.md)
- **Event batching:** 5 / 100 / 500 events (Free / Pro / Business), 10 MiB hard cap per batch — [Inngest usage limits](https://www.inngest.com/docs/usage-limits/inngest)
- `step.fetch` (2025-05-09) makes durable HTTP calls — [Inngest changelog](https://www.inngest.com/changelog)
- **Deferred Functions** (2026-06-05) are fire-and-forget, fully independent runs with typed payloads — [Inngest changelog](https://www.inngest.com/changelog)
- **Experiments** (2026-06-23) compare versions of a step on live production traffic, with durable selection — [Inngest changelog](https://www.inngest.com/changelog)

**Hatchet**
- Concurrency strategies: GROUP_ROUND_ROBIN (fair across groups), CANCEL_IN_PROGRESS, CANCEL_NEWEST, CANCEL_QUEUED_EXCEPT_NEWEST and CANCEL_QUEUED_EXCEPT_OLDEST — [Hatchet concurrency](https://docs.hatchet.run/home/concurrency)
- Keys are CEL expressions over `input` and `additional_metadata`. Max runs can be dynamic CEL, for example `input.tier == 'premium' ? 10 : 1` — [Hatchet concurrency](https://docs.hatchet.run/home/concurrency)
- Strategies can be shared and tenant-scoped (`is_tenant_scoped: true`) or chained for hierarchical limits. "Task slot cost" weights worker capacity per task — [Hatchet concurrency](https://docs.hatchet.run/home/concurrency)
- Queue disciplines: FIFO, LIFO, round robin and priority. Priority runs from 1 to 3 — [Hatchet repo](https://github.com/hatchet-dev/hatchet-v1); [Hatchet Python SDK](https://docs.hatchet.run/sdks/python-sdk)
- **Sticky assignment:** SOFT prefers the same worker and falls back; HARD requires the same worker — [Hatchet worker assignment](https://docs.hatchet.run/home/features/worker-assignment/overview)
- **Worker affinity labels** are key/value pairs such as model loaded, memory or region, matched against task requirements — [Hatchet worker affinity](https://www.mintlify.com/hatchet-dev/hatchet/workers/worker-affinity)
- Rate limits include dynamic rate limits — [Hatchet worker assignment / rate limits](https://docs.hatchet.run/home/features/worker-assignment/overview)
- Durable tasks offer durable sleep, durable event waits, and child spawning. Conditional waits compose through "or groups" — [Hatchet durable execution](https://docs.hatchet.run/home/durable-execution)

**Trigger.dev**
- Waitpoints pause a run for human approval, HTTP callbacks and idempotency — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga)
- v4 also added run priority, queue pause and queue stats — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga)
- v4.4.2 added bidirectional, typed input streams — [newreleases: v4.4.5](https://newreleases.io/project/github/triggerdotdev/trigger.dev/release/v4.4.5)
- Machine presets run from Micro (0.25 vCPU / 0.25 GB) to Large 2x (8 vCPU / 16 GB), chosen per task — [Trigger.dev pricing](https://trigger.dev/pricing)
- Bulk Actions API (v4.5.2) re-runs runs by filter or IDs. Large batch payloads over 128 KB offload to object storage — [releases.sh v4.5.x](https://releases.sh/trigger-dev/trigger-dev/highlights)

**Cloudflare Workflows**
- `waitForEvent` handles human-in-the-loop and webhooks. Its default timeout is 24 h, configurable up to 7 days; a timeout throws and can be caught — [Cloudflare limits/skills summary](https://developers.cloudflare.com/workflows/reference/limits/index.md); [CF Workflows GA blog](https://blog.cloudflare.com/workflows-ga-production-ready-durable-execution/)

**Vercel Workflows**
- Sleep has no maximum duration. Hooks wait for external events (hook token up to 255 bytes) — [Vercel pricing & limits](https://vercel.com/docs/workflows/pricing.md)
- Streams pass data in and out of runs with managed persistence — [Vercel Workflows docs](https://vercel.com/docs/workflow)
- Runs carry up to 64 attributes (`setAttributes`) — [Vercel pricing & limits](https://vercel.com/docs/workflows/pricing.md)

**Golem**
- Agent-to-agent RPC with guaranteed delivery, and forking agents for parallel work — [Golem: why Golem](https://learn.golem.cloud/why-golem)

**LittleHorse**
- User Tasks are a first-class human-in-the-loop primitive — [LittleHorse server docs](https://docs.littlehorse.io/server)

**Absurd**
- `beginStep()` and `completeStep()` split a step, so the code can inspect it before committing. Tasks can spawn and await child tasks — [Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)

### Inferences
- Restate's vqueues close its largest gap against Inngest, Hatchet and DBOS. But at October 2026 only concurrency limits are GA; rate limits and priorities are roadmap.
- Inngest has the richest event-native flow-control set: debounce, batching, singleton, cancelOn, throttling and rate limiting. Hatchet's CEL-based dynamic keys and limits, and DBOS's runtime-reconfigurable partitioned queues, are the closest equivalents.
- DBOS fork and rewind onto a new app version is a distinctive operations primitive. It is a supported way to "fix code, then re-run a failed workflow from step N". Temporal's reset is the nearest analog.

### Gaps
- The Inngest debounce window limits and the throttle/rate-limit period ranges were not captured.
- No Hatchet page I reached gave rate-limit configuration details.
- DBOS queue deduplication IDs were not verified from a fetched primary page in this session.

---

## Developer experience: local dev, UIs, observability, replay/time-travel debugging, testing, languages

### Takeaway
Every engine ships a local dev server or CLI and a web UI. The 2025-2026 differentiators are timeline/trace views built for long agentic runs (Restate UI 1.0, DBOS Conductor timeline, Vercel's trace viewer), SQL querying of run data (Inngest Insights), MCP servers and "agent skills" so coding agents can debug workflows, and replay or fork from any step.

### Cited Findings
- **Restate:** `restate up` for local development (1.6) — [server changelog](https://docs.restate.dev/changelog/server.md)
- **Restate UI:** a live timeline of steps, retries, nested RPCs, awakeables, promises and cancellation signals (1.5, 2025-10-01) — [Restate 1.5 blog](https://restate.dev/blog/announcing-restate-1-5)
- **Restate UI 1.0** (GA in 1.7) handles thousands of invocations/s and shows vqueue blocking conditions — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- **Restate tracing** was rebuilt into start, per-attempt and end spans, with steps as events. A `@restatedev/restate-sdk-opentelemetry` package adds trace enrichment — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- **Restate SDK languages:** TypeScript, Java, Python, Go; Rust also exists (not verified in this session) — [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- **DBOS Conductor timeline** (May 2026) is "optimized for deeply nested workflows, multi-day executions, and workflows with millions of steps" — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)
- **DBOS Conductor API and CLI** (Aug 2026): a public OpenAPI 3.1 HTTP API and the `dbosctl` CLI. A metrics preview exports to Prometheus, Grafana, Datadog, OTel, Honeycomb and New Relic — [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- **DBOS migrations** can print SQL with `--print-migrations` and `--print-user-role` — [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- **DBOS MCP server:** list, get and fork workflows, and list steps, for agentic troubleshooting — [DBOS Aug 2026 (search summary)](https://dbos.dev/blog/new-in-dbos-august-2026)
- **DBOS languages:** Python, TypeScript, Go (v1.0), Java (v0.8, with a Spring Boot starter) — [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026); [DBOS Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)
- **Inngest Dev Server MCP** (2025-10-27) — [Inngest changelog](https://www.inngest.com/changelog)
- **Inngest Insights** (SQL over events, 2025-09-23) gained runs, steps and trace-span datasources and AI-assisted queries (2026-05-13) — [Inngest changelog](https://www.inngest.com/changelog)
- **Inngest Sessions and Propagated Sessions** (Jun/Aug 2026) group related runs. Metrics export to Prometheus and Datadog; six "Agent Skills" for coding agents (2026-02-18) — [Inngest changelog](https://www.inngest.com/changelog)
- **Inngest TS v4** supports Standard Schema (Zod 4, Valibot, ArkType). Other SDKs: Python and Go — [Inngest changelog](https://www.inngest.com/changelog)
- **Hatchet** has Python, TypeScript, Go and Ruby SDKs, plus observability, alerting and logging. Every task is "durably persisted… allowing for debugging, retries and replays" — [Hatchet docs](https://docs.hatchet.run/home)
- **Trigger.dev v4.4.3:** an Errors page with fingerprint grouping and bulk replay — [releases.sh](https://releases.sh/trigger-dev/trigger-dev/highlights)
- **Trigger.dev v4.5:** `trigger init` sets up an MCP server and agent skills for AI coding assistants — [releases.sh](https://releases.sh/release/rel_Yiomqo3606kpS-f7BiUoM-trigger-dev-v4-5-0-rc-6)
- **Trigger.dev runtimes:** OpenTelemetry export; Node 22/24/26 and Bun — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga)
- **Vercel Workflows:** "Every step, input, output, sleep, and error inside a workflow is recorded automatically" in Vercel Observability. Languages: TypeScript/JavaScript, with Python in beta at GA — [Vercel Workflows docs](https://vercel.com/docs/workflow); [dutchitchannel GA report](https://www.dutchitchannel.nl/news/730354/vercel-workflows-nu-officieel-beschikbaar)
- **Absurd:** the `absurdctl` CLI and the "Habitat" web dashboard — [Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)
- **Obelisk:** "you can replay any past workflow execution exactly as it happened" for debugging. Languages: any with wit-bindgen (Rust, Go); JS experimental — [Obelisk docs](https://obeli.sk/docs/v0.22.0/concepts/workflows/); [Obelisk 0.24.1](https://obeli.sk/blog/introducing-obelisk-0-24-1/)
- **Golem:** "searchable traces with safe replay". Languages: TypeScript, Rust, Scala, MoonBit — [Golem: why Golem](https://learn.golem.cloud/why-golem)

### Inferences
- "MCP or agent skills for debugging workflows" became table stakes in 2025-2026 (DBOS, Inngest, Trigger.dev, Absurd). An engine without it looks dated to AI-first buyers.
- Fork or replay from step N (DBOS, Hatchet, Trigger.dev bulk replay, Obelisk exact replay) is a common repair primitive.

### Gaps
- I found no testing-utility details (time-skipping test servers or mocks) for any engine in this session.
- I did not confirm Cloudflare's local-dev story for Workflows (wrangler dev).

---

## AI and agent features: durable agent loops, LLM streaming, tool calling, human-in-the-loop, MCP, AI SDK integrations

### Takeaway
Nearly every engine now positions itself as "durable execution for agents". The concrete differentiators:
- **Framework integrations:** DBOS with Pydantic AI, OpenAI Agents and Google ADK; Restate with the Vercel AI SDK and OpenAI Agents SDK; Vercel's native WorkflowAgent in AI SDK v7.
- **Resumable LLM output streams:** Vercel, Trigger.dev and Inngest Realtime.
- **AI-specific observability:** Inngest AI Overview and Scoring.
- **Agent-native runtimes:** Golem.

### Cited Findings
- **DBOS + Pydantic AI:** `DBOSAgent` wraps the agent run loop as a workflow, and model requests and MCP calls as steps (`pip install pydantic-ai[dbos]`) — [DBOS Pydantic AI](https://docs.dbos.dev/integrations/pydantic-ai); [Pydantic docs](https://pydantic.dev/docs/ai/capabilities/durable_execution/dbos/)
- **DBOS + OpenAI Agents SDK:** `DBOSRunner.run` replaces `Runner.run` — [DBOS OpenAI Agents](https://docs.dbos.dev/integrations/openai-agents)
- **DBOS other integrations:** LlamaIndex, Google ADK, Vercel AI — [DBOS AI quickstart](https://docs.dbos.dev/ai/ai-quickstart); [DBOS Vercel AI](https://docs.dbos.dev/integrations/vercel-ai)
- **Restate:** `@restatedev/vercel-ai-middleware` journals LLM calls, tool executions and session state. Native OpenAI Agents SDK integration for Python; works with any LLM SDK — [Restate AI docs](https://docs.restate.dev/ai/index); [Restate + Vercel blog](https://restate.dev/blog/building-durable-agents-with-vercel-and-restate); [Restate + OpenAI blog](https://restate.dev/blog/durable-orchestration-for-ai-agents-with-restate-and-openai-sdk)
- **Restate pitch:** Restate frames 1.7 flow control around agents, for example "how many inference calls a team is allowed to do in a certain time frame" — [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- **Restate funding:** a secondary Japanese report says Restate raised a $20M Series A on 2026-09-30, led by Singular, with Redpoint and Capital One Ventures. Not verified against a primary source — [xenospectrum](https://xenospectrum.com/restate-ai-agent-durable-execution-funding/)
- **Vercel Workflow SDK** integrates with the AI SDK for durable agents. AI SDK v7 adds a fully native `WorkflowAgent`. `getWritable()` gives a persistent stream that clients can disconnect from and resume at any point — [Vercel search summary / docs](https://vercel.com/docs/workflow); [workflow-sdk.dev/docs/ai](https://workflow-sdk.dev/docs/ai)
- **Vercel streams limits:** unlimited storage, 10 MB per chunk, 1,000 chunks/s per stream — [Vercel pricing & limits](https://vercel.com/docs/workflows/pricing.md)
- **Inngest:** AgentKit `useAgent`/`useChat` React hooks stream agent updates (2025-09-24). Realtime was rewritten in TS SDK v4 (2026-03-25); Python realtime is beta — [Inngest changelog](https://www.inngest.com/changelog)
- **Inngest AI observability:** AI Overview monitors AI calls, cost and latency (2026-08-12). Scoring attaches quality signals to runs (2026-06-30) — [Inngest changelog](https://www.inngest.com/changelog)
- **Trigger.dev** turns tasks into AI SDK tools (v4) — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga)
- **Trigger.dev `chat.agent`** gives each conversation "a stateful machine that sleeps between turns, with durable streaming and AI SDK integration". Realtime is GA with LLM streaming. Secondary source, not verified on trigger.dev — [automationatlas](https://automationatlas.io/answers/trigger-dev-pricing-explained-2026/)
- **Golem 1.5** added code-first routes, webhooks, MCP, Node.js compatibility and Scala. Agents are typed ("Agent Type" is a versioned code + config definition), with per-agent sandbox capabilities — [Golem v1.5](https://learn.golem.cloud/v1.5/concepts); [Golem: why Golem](https://learn.golem.cloud/why-golem)
- **LittleHorse** User Tasks are positioned for correcting LLM hallucinations — [LittleHorse docs](https://docs.littlehorse.io/server)
- **Absurd** ships agent skills for debugging workflow state, plus patterns for durable agent turns — [Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)
- **Obelisk** blog: "Taming AI-Assisted Code with Deterministic Workflows" — [Obelisk blog](https://obeli.sk/blog/taming-ai-assisted-code/)

### Inferences
- Resumable streams of agent output are a 2026 differentiator: Vercel `getWritable`, Trigger.dev realtime and input streams, and Inngest Realtime. Restate lists native streams only as roadmap.
- The integration pattern of "wrap an existing agent framework's loop as a workflow and its LLM/tool calls as steps" (DBOS, Restate) is the low-friction adoption path buyers cite.

### Gaps
- Cloudflare's agent story (Agents SDK on Durable Objects) was not researched in this session.
- Hatchet's agent-specific features beyond "state checkpointing for error recovery" were not found.

---

## Operations: self-hosting, managed cloud, pricing models, multi-tenancy, limits

### Takeaway
Pricing models differ sharply:
- **Per execution or step:** Inngest (each run and step counts); Cloudflare (steps).
- **Per event and data:** Vercel (events + GB written/retained).
- **Per compute-second plus per run:** Trigger.dev.
- **Per executor:** DBOS Conductor.

Limits differ by orders of magnitude, from Inngest's 1,000 steps/function to Cloudflare's 25,000 steps and Vercel's 10,000 steps.

### Cited Findings

**Cloudflare Workflows (limits updated 2026-09-21)**

| Limit | Free | Paid |
|---|---|---|
| Steps per instance | 1,024 | 10,000 default, configurable to 25,000 |
| CPU per step | 10 ms | 30 s, configurable to 5 min |
| Wall clock per step | Unlimited | Unlimited |
| Step result size | 1 MiB | 1 MiB |
| Event payload size | 1 MiB | 1 MiB |
| Persisted state | 100 MB | 1 GB |
| Max sleep | 365 days | 365 days |
| Concurrent instances | 100 | 50,000 |
| Instance creation rate | 100/s | 300/s per account, 100/s per workflow |
| Queued instances | 100,000 | 2,000,000 |
| State retention | 3 days | 30 days |

- Source for the table: [Cloudflare limits](https://developers.cloudflare.com/workflows/reference/limits/index.md)
- The step limit was raised to 25k on 2026-03-03 — [CF changelog](https://developers.cloudflare.com/changelog/post/2026-03-03-step-limits-to-25k/)
- Pricing: $0.30 per million step executions after 10M free per month. This figure comes from a search summary and was not verified on the pricing page — [CF GA blog (search summary)](https://blog.cloudflare.com/workflows-ga-production-ready-durable-execution/)

**Vercel Workflows (GA ~April 2026)**
- Beta launched October 2025. By GA it had processed 100M+ runs and 500M+ steps for 1,500+ customers, with 200K weekly npm downloads — [createwith summary](https://www.createwith.com/tool/vercel/updates/vercel-workflows-reaches-general-availability-for-long-running-processes); [dutchitchannel, 2026-04-23](https://www.dutchitchannel.nl/news/730354/vercel-workflows-nu-officieel-beschikbaar)
- Pricing: Events at $0.02/1K (Hobby includes 50K/mo), Data Written at $0.50/GB (1 GB included), Data Retained at $0.50/GB-month. Function compute and Queues are billed separately — [Vercel pricing](https://vercel.com/docs/workflows/pricing.md)
- Run limits: 10,000 steps/run, 25,000 events/run, 50 MB max payload, 2 GB entity storage per run, 1,000 run creations/s. Run duration and sleep have no limit — [Vercel pricing](https://vercel.com/docs/workflows/pricing.md)
- Replay limits: 240 s max replay duration. Replay slows past 2,000 events or 1 GB, so Vercel advises child workflows or batching — [Vercel pricing](https://vercel.com/docs/workflows/pricing.md)
- Retention after completion: 1 day (Hobby), 7 days (Pro), 30 days (Enterprise). Concurrency up to 100,000 — [Vercel pricing](https://vercel.com/docs/workflows/pricing.md)
- Step data is encrypted automatically. A "Workflow Run Data Viewer" RBAC permission controls access to decrypted run data — [dutchitchannel](https://www.dutchitchannel.nl/news/730354/vercel-workflows-nu-officieel-beschikbaar); [Vercel docs](https://vercel.com/docs/workflow)

**Inngest**
- Platform limits: 1,000 steps/function, 4 MiB per step output, 32 MiB run state, 1-year max sleep, 2 h step timeout (bounded by the host), 5,000 events per send — [Inngest usage limits](https://www.inngest.com/docs/usage-limits/inngest)
- Plan limits by Free / Pro / Business: concurrent steps 5 / 100 / 500; max event size 256 KiB / 3 MiB / 3 MiB; max run length 30 / 90 / 366 days; queue depth 100K / 1M / 10M — [Inngest usage limits](https://www.inngest.com/docs/usage-limits/inngest)

**Trigger.dev**
- Plans: Free $0 ($5 credit), Hobby $10, Pro $50, Enterprise custom — [Trigger.dev pricing](https://trigger.dev/pricing)
- Compute bills per second by machine, from $0.0000169/s (Micro) to $0.00068/s (Large 2x), plus $0.25 per 10K runs — [Trigger.dev pricing](https://trigger.dev/pricing)
- Concurrency: 20 / 50 / 200+ concurrent runs. Unlimited max duration — [Trigger.dev pricing](https://trigger.dev/pricing)
- Self-hosting is Apache 2.0 — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga); [automationatlas](https://automationatlas.io/answers/trigger-dev-pricing-explained-2026/)
- Conflict: a secondary source lists different tiers (Pro $50 with 250K runs, Team $200). The official page shows Free/Hobby/Pro, so prefer it — [automationatlas](https://automationatlas.io/answers/trigger-dev-pricing-explained-2026/) vs [Trigger.dev pricing](https://trigger.dev/pricing)

**DBOS**
- Transact is open source; Conductor is the paid control plane at $99/mo per additional self-hosted executor — [DBOS pricing](https://dbos.dev/dbos-pricing)
- DBOS Cloud bills compute time, RAM and requests — [DBOS pricing](https://dbos.dev/dbos-pricing)

**Restate**
- Restate Cloud exists, and 1.5 included changes "important to build Restate Cloud" — [Restate 1.5 blog](https://restate.dev/blog/announcing-restate-1-5)
- Self-hosted operations: snapshots to Azure Blob/GCS (1.6), and a Kubernetes operator v2.6.1 (2026-06-12) — [server changelog](https://docs.restate.dev/changelog/server.md); [operator changelog](https://docs.restate.dev/changelog/operator)

**Hatchet**
- Hatchet Cloud or self-hosted (MIT) — [Hatchet docs](https://docs.hatchet.run/home)

**Golem**
- Golem Cloud (managed) or self-hosted — [Golem: why Golem](https://learn.golem.cloud/why-golem)

### Inferences
- Multi-tenant fairness is now a first-class product feature: DBOS partitioned queues, Hatchet tenant-scoped round robin, Inngest multi-tenancy grouping and Restate hierarchical scopes. Buyers running SaaS on these engines cite it.
- The serverless engines (Vercel, Cloudflare) cap per-run history (10k-25k steps, replay-time caps) much as Temporal caps history. Vercel's 240 s replay cap shows the cost of full replay on serverless.

### Gaps
- Pricing for Restate Cloud, Hatchet Cloud, Golem Cloud and Inngest plan dollar prices was not captured.
- Multi-tenancy isolation models (namespaces or projects) per engine were not researched in depth.

---

## Versioning and upgrade strategies

### Takeaway
The engines avoid Temporal-style `getVersion` patching in several ways:
- **Immutable deployments with invocation pinning:** Restate, Vercel skew protection, Golem's side-by-side versions.
- **App-version-scoped recovery, patching, and fork-to-new-version:** DBOS.
- **Step-ID memoization that tolerates code edits around stable step IDs:** Inngest, Cloudflare.
- **No replay at all:** Trigger.dev CRIU snapshots of the running process; Golem live migration with snapshot-based manual updates.

### Cited Findings
- **Restate** uses immutable deployments with invocation pinning. Note: the page summary listed this as a requirement item and did not quote it, so it needs confirmation — [Restate docs](https://docs.restate.dev/concepts/durable_building_blocks)
- Restate 1.6 made deployment registration idempotent and safer. 1.7 UI adds deployment pruning — [server changelog](https://docs.restate.dev/changelog/server.md); [Restate 1.7 blog](https://restate.dev/blog/announcing-restate-1-7)
- **DBOS** offers "patching (using conditional logic) and versioning (isolating workflows to compatible code versions)". Workflows recover only on processes running compatible code — [DBOS architecture](https://docs.dbos.dev/architecture)
- DBOS forks can target a new application version to fix bugs. Java v0.8 persists application versions in the database for runtime observability and control — [DBOS workflow management](https://docs.dbos.dev/python/tutorials/workflow-management); [DBOS May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)
- **Vercel** offers "Skew Protection" against version skew. Runs keep their region; "Upgrading the SDK does not migrate runs created before the upgrade" — [Vercel Workflows docs](https://vercel.com/docs/workflow)
- Vercel step and workflow names "are derived from the file path and function name", so names form the memoization identity — [Vercel pricing & limits](https://vercel.com/docs/workflows/pricing.md)
- **Golem** offers "zero-downtime upgrades: run versions side-by-side or migrate live agents forward", with automatic and manual (snapshot-based) update paths — [Golem: why Golem](https://learn.golem.cloud/why-golem)
- **Inngest Experiments** (2026-06-23) roll out step versions on live traffic with "durable selection", a versioning primitive at step level — [Inngest changelog](https://www.inngest.com/changelog)
- **Absurd** keeps non-deterministic code outside step boundaries. This reduces versioning hazards to step naming and ordering — [Absurd in production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)
- **Restate 1.6** can restart an invocation from a journal prefix, keeping completed work. This is an operator tool for recovering from bad deployments — [server changelog](https://docs.restate.dev/changelog/server.md)

### Inferences
- Pinning in-flight work to the old deployment (Restate, Vercel, Golem) and "recover only on the same app version" (DBOS) are the dominant patterns. Both trade code-level patching for running old and new code side by side.
- DBOS fork-to-new-version and Restate restart-from-prefix offer a "fix forward from step N" escape hatch. Buyers cite that as an operational advantage over Temporal's reset.

### Gaps
- I did not fetch Inngest's step-ID versioning doc ("changing functions" guidance) in this session, so the precise step-ID rules are unconfirmed.
- Hatchet, Trigger.dev and Cloudflare upgrade semantics for in-flight runs were not verified.
- I did not confirm Restate's exact deployment-pinning wording from a primary page.

---

## Cross-engine summary of distinctive capabilities (versus Temporal)

### Takeaway
Buyers choose these engines for:
- **Infrastructure minimalism:** Postgres-only or library-only (DBOS, Absurd, pgflow, Hatchet).
- **Serverless push execution:** Restate, Inngest, Vercel, Cloudflare.
- **Declarative flow control and fairness:** Inngest, Hatchet, DBOS, Restate 1.7.
- **No determinism burden:** Trigger.dev CRIU, Golem oplog, Absurd checkpoints.
- **Keyed stateful actors:** Restate Virtual Objects, Golem agents.
- **First-class agent support:** streams, framework adapters, MCP debugging.
- **Operator repair tools:** DBOS fork/rewind, Restate pause and restart-from-prefix, Trigger.dev bulk replay.

### Cited Findings

| Capability | Engines with it (evidence) |
|---|---|
| Embedded library, Postgres only, no server | DBOS ([arch](https://docs.dbos.dev/architecture)), Absurd ([post](https://lucumr.pocoo.org/2026/4/4/absurd-in-production.md)), pgflow ([summary](https://gittrend.io/repo/pgflow-dev/pgflow)) |
| Server on Postgres | Hatchet ([docs](https://docs.hatchet.run/home)), Inngest self-host (experimental PG) ([changelog](https://www.inngest.com/changelog)), Vercel Postgres World ([useworkflow.dev](https://useworkflow.dev/worlds/vercel)) |
| Push to HTTP/serverless | Restate (Cloud Run OIDC) ([1.7](https://restate.dev/blog/announcing-restate-1-7)), Inngest ([limits](https://www.inngest.com/docs/usage-limits/inngest)), Vercel ([docs](https://vercel.com/docs/workflow)), Cloudflare ([docs](https://developers.cloudflare.com/workflows/)) |
| Process/container snapshot, no replay determinism | Trigger.dev CRIU ([indepth.dev](https://indepth.dev/posts/1020/en/how-trigger-dev-checkpoints-containers)), Golem oplog ([Golem](https://learn.golem.cloud/why-golem)) |
| Keyed single-writer state | Restate Virtual Objects ([docs](https://docs.restate.dev/concepts/durable_building_blocks)), Golem agents ([Golem](https://learn.golem.cloud/why-golem)), Duroxide per-instance KV ([docs.rs](https://docs.rs/crate/duroxide/latest)) |
| Durable promises / awakeables | Restate ([awakeables](https://docs.restate.dev/develop/ts/awakeables)), Resonate ([durable promise](https://docs.resonatehq.io/concepts/durable-promise)) |
| Per-key concurrency and fairness | Inngest ([flow control](https://www.inngest.com/docs/guides/flow-control.md)), Hatchet CEL keys + round robin ([concurrency](https://docs.hatchet.run/home/concurrency)), DBOS partitioned queues ([Aug 2026](https://dbos.dev/blog/new-in-dbos-august-2026)), Restate hierarchical scopes ([1.7](https://restate.dev/blog/announcing-restate-1-7)) |
| Debounce, batching, singleton, cancel-on-event | Inngest ([flow control](https://www.inngest.com/docs/guides/flow-control.md), [singleton](https://www.inngest.com/docs/guides/singleton.md), [cancelOn](https://www.inngest.com/docs/reference/typescript/v4/functions/cancel-on.md)), Hatchet cancel strategies ([concurrency](https://docs.hatchet.run/home/concurrency)) |
| Runtime-reconfigurable limits without restart | DBOS `register_queue` ([May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)), Restate admin API limits ([1.7](https://restate.dev/blog/announcing-restate-1-7)) |
| Sticky workers and affinity labels | Hatchet SOFT/HARD + labels ([assignment](https://docs.hatchet.run/home/features/worker-assignment/overview)); Restate roadmap only ([roadmap](https://docs.restate.dev/roadmap/oss.md)) |
| Fork, rewind, restart-from-step | DBOS fork/rewind ([mgmt](https://docs.dbos.dev/python/tutorials/workflow-management)), Restate restart from journal prefix and pause/resume ([changelog](https://docs.restate.dev/changelog/server.md)), Trigger.dev bulk replay ([releases](https://releases.sh/trigger-dev/trigger-dev/highlights)) |
| Resumable output streams | Vercel `getWritable` ([docs](https://vercel.com/docs/workflow)), Trigger.dev realtime and input streams ([v4.4.x](https://newreleases.io/project/github/triggerdotdev/trigger.dev/release/v4.4.5)), Inngest Realtime ([changelog](https://www.inngest.com/changelog)); Restate roadmap ([roadmap](https://docs.restate.dev/roadmap/oss.md)) |
| Durable HTTP endpoints, not background jobs | Inngest Durable Endpoints, beta ([changelog](https://www.inngest.com/changelog)) |
| Agent framework adapters | DBOS (Pydantic AI, OpenAI Agents, ADK, LlamaIndex) ([AI](https://docs.dbos.dev/ai/ai-quickstart)), Restate (Vercel AI SDK, OpenAI Agents) ([AI](https://docs.restate.dev/ai/index)), Vercel WorkflowAgent ([docs](https://workflow-sdk.dev/docs/ai)) |
| MCP or agent skills for operating workflows | DBOS MCP, Inngest Dev Server MCP + skills, Trigger.dev MCP + skills, Absurd skills (sources above) |
| Per-run region pinning | Vercel ([docs](https://vercel.com/docs/workflow)); DBOS via CockroachDB multi-region ([May 2026](https://dbos.dev/blog/new-in-dbos-may-2026)) |
| WASM sandbox determinism | Obelisk ([0.24.1](https://obeli.sk/blog/introducing-obelisk-0-24-1/)), Golem ([Golem](https://learn.golem.cloud/why-golem)) |
| Live A/B of step versions | Inngest Experiments ([changelog](https://www.inngest.com/changelog)) |
| AI cost and quality observability | Inngest AI Overview and Scoring ([changelog](https://www.inngest.com/changelog)) |

### Inferences
- Features most often absent from a Temporal-style engine and present in several competitors:
  1. declarative per-key concurrency with fair round-robin across tenants;
  2. throttle, debounce, batching and singleton as function config;
  3. runtime-reconfigurable queue limits;
  4. fork or rewind from step N onto new code;
  5. resumable streams to frontends;
  6. pause and resume of a single execution;
  7. MCP tooling for operating workflows;
  8. agent-framework adapters.
- Restate 1.7 and DBOS May/Aug 2026 show that flow control and multi-tenant fairness are where incumbents invested most during 2026.

### Gaps
- The October 2026 versions of Hatchet, Inngest Python/Go SDKs and LittleHorse were not pinned down.
- No independent benchmark compares these engines head to head. Throughput claims (DBOS 43K workflows/s, Hatchet 5k tasks/s bursts) are vendor-reported.
- HN and buyer-sentiment sources were not gathered in this session, so the "reasons buyers cite" come from vendor positioning, not third-party surveys.
