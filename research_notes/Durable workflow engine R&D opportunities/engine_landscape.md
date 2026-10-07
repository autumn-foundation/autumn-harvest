# Competitive Landscape of Durable Workflow / Durable Execution Engines (as of October 2026)

Scope note: research date 2026-10-07. Items dated before 2026 are marked with their date. Second-group engines (Resonate, LittleHorse, pgflow, Windmill, Prefect, Airflow 3, Dapr) got only light coverage; see the Gaps lists.

---

## Q1. What execution model does each engine use, and what trade-offs do vendors claim?

### Takeaway
The market has split into three execution families. (1) **Deterministic event-sourced replay** of a workflow function: Temporal, Cadence, Azure Durable Functions/DTS, Vercel Workflow, AWS Lambda durable functions, Obelisk. (2) **Journaled step memoization**, where only step results are checkpointed and the code between steps re-runs or simply continues: DBOS, Inngest, Restate, Absurd, Cloudflare Workflows. (3) **Process/VM state capture**: Trigger.dev uses CRIU container checkpoints, and Golem uses WASM with an oplog. In 2026 the visible trend is that the replay vendors are working to reduce the cost of replay (Inngest checkpointing, Temporal Worker Versioning GA, Restate "restart from journal prefix"). Meanwhile the memoization vendors pitch "no determinism tax" and "just Postgres".

### Cited Findings
**Temporal (event-sourced replay)**
- Workflow code must be deterministic: no `Date.now()`, no random, no direct HTTP. Non-deterministic work moves into Activities, and a workflow can run fine for a month and then fail on its first replay. — [HackerNoon, "You Don't Need Temporal Yet"](https://hackernoon.com/you-dont-need-temporal-yet-durable-execution-for-ai-agents-in-150-lines)
- Critics describe the "determinism tax" as the biggest hidden cost. Reading system time, spawning threads, iterating unordered maps, RNG and bare HTTP calls all become bugs inside workflow functions. — [foojay.io, "Durable Execution Is a Property, Not a Product"](https://foojay.io/today/durable-execution-is-a-property-not-a-product/); [Kanopy Labs comparison](https://kanopylabs.com/blog/restate-vs-temporal-vs-dbos-durable-execution)
- Worker Versioning (GA at Replay 2026) "pins running Workflows to the Worker version that started them". Temporal claims it removes the need for patching and enables progressive rollouts. This is Temporal's main answer to the replay-vs-deploy problem. — [Temporal Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements)
- Standalone Activities (Public Preview for Go, Python and .NET; pre-release for Java and TypeScript) run a single Activity as an independent durable job with no Workflow. This moves Temporal toward the job-queue use case that Inngest, Hatchet and Trigger.dev serve. — [Temporal Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)

**AWS Lambda durable functions (checkpoint + replay; launched re:Invent, Dec 2025)**
- AWS calls the model "checkpoint/replay". On resume, "your code runs from the beginning but skips over completed checkpoints, using stored results instead of re-executing completed operations." The primitives are `context.step()` (retries + checkpoint) and `context.wait()` (suspend with no compute charge). — [AWS Lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html); [AWS What's New, Dec 2025](https://aws.amazon.com/about-aws/whats-new/2025/12/lambda-durable-multi-step-applications-ai-workflows/)
- AWS positions it against Step Functions as follows. Durable functions run inside Lambda in standard languages, for logic tightly coupled with business code. Step Functions is a graph DSL/visual designer with native integrations to 220+ services. — [AWS Lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)

**Vercel Workflow / Workflow SDK (event sourcing + deterministic replay via directives)**
- The `"use workflow"` / `"use step"` directives compile async JS/TS into durable workflows. State changes are stored as events and replayed, so workflow code must be deterministic. — [Vercel Workflow examples](https://examples.vercel.com/workflow); [Vercel docs](https://vercel.com/docs/workflow)
- Each step produces `step_created`, `step_started` and `step_completed` events, plus `step_retrying` on each retry. — [Vercel Workflows pricing/limits](https://vercel.com/docs/workflows/pricing)
- Inngest published a rebuttal of the directive approach, "Explicit APIs vs Magic Directives" (Oct 24, 2025). — [Inngest blog index](https://www.inngest.com/blog-markdown)

**Inngest (step memoization over HTTP, now with checkpointing)**
- In the classic model, each step returns an HTTP response and Inngest then enqueues another job for the next step, which adds inter-step latency. Checkpointing (public beta, Jan 9, 2026) runs synchronous steps immediately and saves their output through the API. It falls back to async for `step.sleep()` or when a checkpoint fails. Inngest reports about 50% faster execution, tens-of-ms inter-step latency over 100K executions, and a Shopify order flow cut from about 18 s to about 5 s. — [Inngest: Introducing Checkpointing](https://www.inngest.com/blog/introducing-checkpointing)
- In serverless mode, checkpointing has a tunable maximum lifetime. After it, execution switches to async to reset the function timeout. — [Inngest checkpointing](https://www.inngest.com/blog/introducing-checkpointing)

**Restate (journaled execution + virtual objects, single binary)**
- Restate keeps a full invocation history (journals) in the single binary's RocksDB tables by default. — [Restate 1.5 announcement](https://restate.dev/blog/announcing-restate-1-5)
- 1.6 added "Restart Invocation from Journal Prefix", which keeps completed calls, sleeps and side effects instead of re-executing from the start. It also added pause/resume, and invocations now pause by default when retries run out. — [Restate v1.6.0 release notes](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md)
- A docs issue confirms the at-least-once edge: a `ctx.run` action re-executes if the process dies after the action but before its result is journaled. — [restatedev/docs-restate #410](https://github.com/restatedev/docs-restate/issues/410)

**DBOS (library-embedded step checkpointing in Postgres)**
- DBOS Transact stores all workflow state only in PostgreSQL. Conductor is an out-of-band control plane that "never accesses your Postgres directly". It talks to workers over an outbound WebSocket, so workflows keep running during a Conductor outage. — [What is DBOS Conductor? (Aug 20, 2026)](https://www.dbos.dev/blog/what-is-dbos-conductor)

**Absurd (Postgres-only, stored procedures)**
- The core is one SQL file of stored procedures for tasks, checkpoints, events and claim-based scheduling, plus an SDK of about 1,400–1,900 lines. Each step is a checkpoint. The author contrasts it with Temporal's roughly 170,000-line Python SDK. — [letsdatascience summary of Ronacher's report](https://letsdatascience.com/news/absurd-delivers-durable-workflows-on-postgres-cb349d8d); [Armin Ronacher blog](https://lucumr.pocoo.org/tags/announcements)

**Trigger.dev (container checkpoint/restore)**
- On a wait for a subtask or a pause, Trigger.dev checkpoints the task's entire state (memory, CPU registers, open file descriptors) with CRIU, releases resources, and restores the task in a new environment. It uses Docker's checkpoint API and Buildah. — [indepth.dev on Trigger.dev checkpoints](https://indepth.dev/posts/1020/en/how-trigger-dev-checkpoints-containers)
- v4 (GA) adds warm starts of 100–300 ms vs several seconds cold, and "waitpoints" for HITL, HTTP callbacks and idempotency. — [Trigger.dev v4 GA changelog](https://trigger.dev/changelog/trigger-v4-ga)

**Golem Cloud (WASM + oplog, transparent durability)**
- Agents are WASM components written in TypeScript, Rust, Scala or MoonBit. An append-only oplog records inputs, messages, effects and decisions for replay. The runtime mediates external calls, guarantees agent-to-agent delivery, and parks idle agents at zero compute. — [Golem concepts](https://learn.golem.cloud/concepts); [Golem docs intro](https://learn.golem.cloud/docs/intro)

**Obelisk (WASM Component Model, deterministic)**
- Obelisk is an open-source deterministic workflow engine. It runs, stores and replays WASM-based workflows using SQLite, with schema-first WIT IDL type safety. — [Introducing Obelisk](https://obeli.sk/blog/introducing-obelisk/)

**Azure Durable Functions / Durable Task Scheduler (DTF replay)**
- DTS is a purpose-built backend-as-a-service for the Durable Task Framework. It pushes work items to apps over gRPC (no polling) and uses in-memory plus persistent internal stores. — [Microsoft Learn: Durable Task Scheduler](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler)

**Cloudflare Workflows (step.do memoization on Workers)**
- The API is `step.do`, `waitForEvent` and step context. 2026 added saga-style rollback handlers per step (Jun 5), dynamic retry delay functions (Jul 9) and `.subscribe()` event streaming (Sep 15). — [Cloudflare Workflows changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md)

### Inferences
- The replay camp (Temporal, AWS, Vercel, Azure) is converging on "pin the run to the code version that started it" (Temporal Worker Versioning, Vercel Skew Protection). Patch APIs are no longer the main path. Vercel lists Skew Protection as a feature, per [Vercel docs](https://vercel.com/docs/workflow).
- The memoization camp is converging on replay-cost reducers such as checkpointing, journal-prefix restart and fork-from-step (DBOS Conductor "fork workflows from specific steps"). These blur the line between replay and resume.
- AWS and Vercel shipping first-party durable execution "inside the function" commoditizes the basic step/wait primitive for serverless users. That pressures Inngest, Trigger.dev and Restate's serverless pitch.

### Gaps
- No primary Cloudflare source was fetched confirming the internal implementation (Durable Objects + SQLite storage) of Workflows. This is widely stated but unverified here.
- Resonate's model (distributed async/await, "durable promises") was not verified from a primary source this session.
- Restate's Bifrost replicated log design was not re-verified from a 2026 primary source. Only 1.6 release-note references to log trimming and durability modes were checked.

---

## Q2. What storage/persistence backend does each use?

### Takeaway
Postgres is now the default substrate for the newer engines: DBOS, Hatchet, Absurd, pgflow, Vercel's Postgres "World" and Inngest self-host. The incumbents run custom or pluggable backends: Temporal/Cadence on Cassandra/MySQL/Postgres, Restate on a custom log plus RocksDB, Azure DTS as a managed purpose-built store. Embedded SQLite appears in Obelisk, and Cloudflare is believed to use DO SQLite. The visible R&D topic is Postgres retention and cleanup cost at scale.

### Cited Findings
- **DBOS**: all workflow state lives in the user's Postgres, and Conductor stores none of it. — [DBOS Conductor](https://www.dbos.dev/blog/what-is-dbos-conductor). Sep 2026 releases moved workflow inputs and outputs to separate tables "for improved performance at scale". Go v1.4 and Java v1.1 claim 10x queue throughput. Conductor data retention got 10x faster by batching deletes and improving locality, which addresses Postgres MVCC cleanup cost. — [What's New in DBOS, Sep 2026](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026). DBOS also published Postgres scaling posts on LISTEN/NOTIFY (Jul 2026), SELECT DISTINCT (Aug 2026) and DELETE (Sep 2026). — [DBOS blog](https://www.dbos.dev/blog)
- **Restate**: a single binary with RocksDB partition stores. 1.6 switched compression from LZ4 to ZSTD, replaced centralized log trimming with partition-driven trimming, added durability modes that control safe log trimming, and added an automatic RocksDB memory manager (2 GiB default, 85% memtables). — [Restate v1.6.0 notes](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md); [Restate 1.5](https://restate.dev/blog/announcing-restate-1-5)
- **Vercel Workflow**: on Vercel, "managed persistence stores all state and event logs in an optimized database", and Vercel Queues dispatch steps. — [Vercel docs](https://vercel.com/docs/workflow). Off Vercel, the open-source SDK supports "Worlds": Local, Postgres, Vercel or custom. — [Workflow SDK Worlds (linked from pricing page)](https://vercel.com/docs/workflows/pricing). Multi-region (SDK ≥5.0.0-beta.33) pins each run's state, queue and streams to one region. 4.x runs always live in iad1. — [Vercel docs](https://vercel.com/docs/workflow)
- **Azure DTS**: a managed store with in-memory short-lived state and a persistent store for recovery and queries. It replaces the BYO providers (Azure Storage, MSSQL, Netherite) and is now "the recommended storage provider". — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler)
- **Hatchet**: a task orchestration platform built on Postgres (v1, MIT). It claims more than 20K tasks/min (over 1B/month) and 2K+ events/s on simple infrastructure. — [Show HN: Hatchet v1](https://hn.svelte.dev/item/43572733)
- **Absurd**: everything lives inside Postgres. A SQLite port also exists (Nov/Dec 2025). — [letsdatascience](https://letsdatascience.com/news/absurd-delivers-durable-workflows-on-postgres-cb349d8d); [Simon Willison, Absurd in SQLite](https://simonwillison.net/2025/Nov/12/absurd-in-sqlite)
- **Obelisk**: SQLite. — [Introducing Obelisk](https://obeli.sk/blog/introducing-obelisk/)
- **Golem**: a durable oplog persisted by the runtime. — [Golem concepts](https://learn.golem.cloud/concepts)
- **Temporal**: External Payload Storage (Public Preview for Python and Go) offloads large payloads to Amazon S3 or custom drivers, aimed at agentic and AI data workloads. — [Temporal Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)
- **Cloudflare Workflows**: persisted state per instance is capped at 100 MB (Free) and 1 GB (Paid). Default retention for new Paid workflows dropped to 7 days (Sep 10, 2026). Storage charges began Aug 10, 2026. — [CF step-limit changelog](https://developers.cloudflare.com/changelog/post/2026-03-03-step-limits-to-25k/); [CF Workflows changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md)

### Inferences
- "Payload offload to object storage" is now a standard escape hatch for the history-size problem. Temporal ships S3 external payloads, Vercel streams do not count toward its 50 MB payload limit, and DTS documents large-payload workarounds. Its arrival in Temporal in 2026 suggests AI payloads (prompts, documents, tool outputs) are pushing past classic 2 MB limits.
- Retention and purge economics are becoming a priced and engineered surface. Signals: Cloudflare's storage billing and 7-day default, Vercel's plan-tiered retention of 1/7/30 days, DBOS's deletion work, and DTS autopurge.

### Gaps
- No 2026 primary source was fetched on Temporal's server persistence roadmap (Cassandra, SQL, Elasticsearch visibility) or on Cadence's storage changes.
- Inngest's backend (it historically used Redis plus Postgres/SQLite for self-host) was not verified in this session.

---

## Q3. What did each ship in 2025–2026?

### Takeaway
2025–2026 launches cluster around five themes: (a) AI agents (streaming, agent SDK integrations, durable tool calls), (b) serverless/FaaS-native workers, (c) versioning and safe deploys, (d) multi-tenant fairness and priority, and (e) operability (pause/resume, restart-from-step, dashboards, OTel/metrics, retention). Hyperscalers entered with AWS Lambda durable functions (Dec 2025), Azure DTS GA (Nov 2025) and Cloudflare Workflows GA (Apr 2025).

### Cited Findings
**Temporal (Replay 2026, announced ~May 2026)** — all items from [Temporal Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements) unless noted
- Serverless Workers on AWS Lambda: pre-release. Temporal Cloud handles invocation, scaling and graceful shutdown.
- Standalone Activities: Public Preview (Go, Python, .NET).
- Workflow Streams: Public Preview. Durable streaming on Signal and Update, for LLM tokens and tool-call updates.
- External Payload Storage: Public Preview (Python, Go; S3 plus custom drivers).
- GA items: Task Queue Priority & Fairness (proportional compute across tenants), Worker Versioning, Multi-region & multi-cloud replication (RTO 20 min, automatic failover/failback), OpenMetrics endpoint, SCIM, PrivateLink/PSC, Capacity Modes.
- Nexus: GA for Python, Public Preview for TypeScript and .NET. Nexus provides cross-namespace, service-to-service durable calls.
- Rust SDK: Public Preview. Worker Status UI: Public Preview.
- Principal Attribution: pre-release. It is a server-derived, non-spoofable identity field in workflow history.
- Billing API and billable-action metrics labelled by Workflow Type and Action Type: upcoming Public Preview.
- AI: Google ADK integration, OpenAI Agents SDK integration GA (including sandbox support), AWS AI Competency, and the Temporal AI Partner Ecosystem.

**AWS**
- Lambda durable functions GA on Dec 2, 2025 in us-east-2, for Python 3.13/3.14 and Node.js 22/24. Executions can run up to 1 year. — [AWS What's New](https://aws.amazon.com/about-aws/whats-new/2025/12/lambda-durable-multi-step-applications-ai-workflows/); [InfoQ](https://infoq.com/news/2025/12/aws-lambda-durable-functions/)
- Expansion: 14 more regions (Dec 2025) and 16 more (Apr 2026). — [AWS What's New Dec 2025](https://aws.amazon.com/about-aws/whats-new/2025/12/lambda-durable-functions-14-additional-regions/); [AWS What's New Apr 2026](https://aws.amazon.com/about-aws/whats-new/2026/04/lambda-durable-functions-16-new-regions/)
- The SDK is open source and now listed for JavaScript, TypeScript, Python and Java. The service has encryption docs. — [AWS Lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)

**Azure**
- Durable Task Scheduler Dedicated SKU GA; Consumption SKU Public Preview (Nov 2025). Offers up to 90 days of retention and HA in multi-CU deployments. — [Redmond Mag, Nov 2025](https://redmondmag.com/blogs/redmond-dispatch/2025/11/microsoft-makes-durable-task-scheduler-generally-available.aspx); [Microsoft Tech Community](https://techcommunity.microsoft.com/blog/appsonazureblog/-/4465328)
- DTS works with Durable Functions and with the Durable Task SDKs on any compute (ACA, AKS, App Service). It adds a dashboard, a Docker emulator, autopurge, private endpoints and multiple task hubs per scheduler. — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler)

**Cloudflare Workflows** — [CF Workflows changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md)
- 2025: GA plus `waitForEvent` (Apr 7), Python Workflows beta with DAG support (Aug 22), and 100/s creation with 10K concurrency (Oct 31).
- 2026: Agents SDK integration (Feb 3), auto-generated diagrams (Feb 4), step limit raised to 25K (Mar 3), local instance methods (Mar 23), 50K concurrency (Apr 15), Workflows in Dynamic Workers (May 1), cron schedules in config (Jun 2), saga rollback (Jun 5), per-step billing (Jul 7), dynamic retry delays (Jul 9), 7-day default retention (Sep 10), `.subscribe()` streaming (Sep 15), and delete APIs (Sep 17).

**Vercel Workflow**
- GA in April 2026, with TypeScript plus a beta Python SDK and AI SDK integration (durable agents, resumable streams). — [createwith.com GA note](https://www.createwith.com/tool/vercel/updates/vercel-workflows-reaches-general-availability-for-long-running-processes); [Vercel community weekly, Apr 2026](https://community.vercel.com/t/vercel-weekly-2026-04-20/38580)
- Since GA: multi-region pinning, configurable state location ("Configure where run state lives"), a redesigned trace viewer, experimental Dynamic Workflows, streams, and a Workflow Run Data Viewer permission for decrypted run data. Vercel also published a migration guide from Cloudflare Workflows. — [Vercel docs](https://vercel.com/docs/workflow)

**Inngest** — [Inngest blog index](https://www.inngest.com/blog-markdown)
- 2025: step.fetch (May), Realtime developer preview (May), metrics export to Datadog/Prometheus (Jun), Connect (persistent outbound worker connections, Jun 20), Vercel Marketplace (Jul), $21M Series A (Sep 16), launch week with Durable Endpoints ("step.run everywhere"), Insights (SQL over events and runs), useAgent and Replit (Sep 22–25), Python realtime (Sep 26), DigitalOcean Marketplace (Oct), Dev Server MCP (Nov 1) and Extended Traces (Nov 12).
- 2026: Checkpointing public beta (Jan 9) — [Inngest checkpointing](https://www.inngest.com/blog/introducing-checkpointing). Also a noisy-neighbor fairness engineering post (Sep 9), a Sep 18 incident report (DB connection pool exhaustion), and Bursty Concurrency, which lets workflows exceed concurrency limits during spikes (Oct 6).

**Restate**
- 1.5 (Oct 1, 2025): full journal history retention, "Restart as new" as a DLQ alternative, per-handler retry policies, pause instead of fail, cross-deployment invocation migration, 5–20x faster SQL introspection, Lambda payload compression and Rust SDK. Restate Cloud became publicly available. — [Restate 1.5](https://restate.dev/blog/announcing-restate-1-5)
- 1.6 (date not stated in the notes): pause/resume, restart from journal prefix, safer deployment registration, Update Deployment API, 32 MiB journal entry limit, and rejection of old SDKs. — [Restate v1.6.0 notes](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md)
- Integrations include durable agents with Pydantic AI. — [Pydantic article](https://pydantic.dev/articles/restate-durable-execution-pydanticai)

**DBOS** — [DBOS Sep 2026 update](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026)
- Python v3.0 and TypeScript v5.0: separate input/output tables. Not forward-compatible: older versions cannot process new-format workflows.
- Go v1.4 and Java v1.1: 10x queue throughput. A Rust SDK v0.5 targets embedded and robotics.
- Partitioned queues combine per-partition and queue-wide limits for fair queueing (TS, Python, Go).
- Conductor: autoscaling policies (KEDA on queue backlog), 10x faster retention, dashboard customization.
- Integrations: Vercel AI SDK (durable streams, durable subagents as child workflows) and Supabase Edge Functions plus pg_cron.

**Second group**
- **Cadence** joined the CNCF Sandbox on Oct 6, 2025, and its roadmap moved to GitHub. — [Cadence blog](https://cadenceworkflow.io/blog/2025/10/06/cadence-joins-cncf-cloud-native-computing-foundation); [Uber blog](https://www.uber.com/blog/cadence-workflow-joins-the-cloud-native-computing-foundation/)
- **Trigger.dev** v4 GA added warm starts and waitpoints. It raised a $16M Series A on Dec 17, 2025, led by Standard Capital. — [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga); [Seedtable](https://seedtable.com/companies/triggerdev/funding-rounds/series-a-2025-12)
- **Hatchet** v1 shipped new Python, TypeScript and Go SDKs on Postgres, plus a launch week. Only a $500K round (Feb 2024) is publicly recorded. — [Show HN Hatchet v1](https://hn.svelte.dev/item/43572733); [Hatchet Launch Week 01](https://hatchet.run/launch-week-01); [vcbacked](https://www.vcbacked.co/company/hatchet)
- **Absurd**: by April 2026 it had run 5 months in production. It added TypeScript, Python and experimental Go SDKs, a CLI and the "Habitat" dashboard. — [letsdatascience](https://letsdatascience.com/news/absurd-delivers-durable-workflows-on-postgres-cb349d8d)
- **Golem**: version 1.5.x is current (Scala index lists golem 1.5.1). It rebranded as "agent-native". — [Scala index golem 1.5.1](https://index.scala-lang.org/golemcloud/golem); [Golem docs](https://learn.golem.cloud/docs/intro)
- **Obelisk**: crate at 0.41.x. — [docs.rs obelisk](https://docs.rs/crate/obelisk)
- **Dapr Workflows**: Dapr.DurableTask packages at 1.16.x (Sep 2025). Dapr Workflow is built on a Durable Task Framework fork. — [NuGet Dapr.DurableTask.Abstractions](https://feed.nuget.org/packages/Dapr.DurableTask.Abstractions)

### Inferences
- Temporal's 2026 roadmap is mostly about absorbing adjacent categories. Standalone Activities target job queues, Serverless Workers target FaaS, Workflow Streams target AI streaming, and Priority/Fairness targets multi-tenant SaaS. Enterprise governance (SCIM, Principal Attribution, Billing API) widens its moat with large buyers.
- "Pause/resume/restart-from-step/fork" operability is now shipped by Restate (1.5/1.6), DBOS Conductor, Cloudflare (pause/resume/restart) and Azure DTS (pause/terminate/restart). It is a 2025–2026 convergence point.

### Gaps
- No exact Replay 2026 date was captured (it appears to be spring 2026). No Restate 1.6 release date was captured.
- No 2025–2026 primary sources were fetched for AWS Step Functions feature launches, LittleHorse, pgflow, Windmill, Resonate, or Prefect/Airflow 3 durable-execution adoption.
- No information was found on any Hatchet funding after 2024. The aggregator data may be stale.

---

## Q4. What limits and pricing do they publish?

### Takeaway
Published per-run ceilings sit in a narrow band of about 10K–50K events/steps per run. Payload limits diverge widely: 1 MB (Azure DTS), 2 MB (Temporal), 32 MiB (Restate journal entry) and 50 MB (Vercel). Pricing is moving to per-event/per-step plus storage GB-months: Vercel, Cloudflare from Jul/Aug 2026, and Temporal actions.

### Cited Findings
| Engine | Key published limits | Pricing signal | Source |
|---|---|---|---|
| Temporal Cloud | 51,200 events or 50 MB history per execution (warn at 10,240 / 10 MB); 2 MB payload; 4 MB transaction/gRPC; 10,000 signals per execution; 2,000 incomplete activities/children (≤500 optimal); 10 in-flight Updates; 30 in-flight Nexus ops; 500 APS default on-demand; retention 1–90 days (30 default); Nexus ScheduleToClose max 60 days | Billed per action. Capacity Modes GA; Billing API upcoming | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits); [Replay 2026](https://temporal.io/blog/replay-2026-product-announcements) |
| Vercel Workflow | 25,000 events/run; 10,000 steps/run; 50 MB payload; 2 GB entity storage/run; 240 s max replay duration; 1,000 run creations/s; 200 events/run/s; no max run or sleep duration; slower replay beyond 2,000 events or 1 GB | $0.02 per 1K events; $0.50/GB written; $0.50/GB-month retained; Hobby includes 50K events plus 1 GB. Retention 1/7/30 days by plan. Function and Queue compute billed separately | [Vercel Workflows pricing](https://vercel.com/docs/workflows/pricing) |
| Cloudflare Workflows | 10,000 steps default, configurable to 25,000 (was 1,024); 100 MB (Free) / 1 GB (Paid) state per instance; 50,000 concurrent instances; 300/s creation per account, 100/s per workflow; 2M queued per workflow | Per-step billing from Jul 7, 2026; storage charges from Aug 10, 2026 | [CF Mar 3 2026](https://developers.cloudflare.com/changelog/post/2026-03-03-step-limits-to-25k/); [CF Apr 15 2026](https://developers.cloudflare.com/changelog/post/2026-04-15-workflows-limits-raised/); [CF changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md) |
| AWS Lambda durable | Up to 1 year execution; no compute charge during waits | Specific durable-operation pricing not captured | [AWS docs](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html) |
| Azure DTS | 1 MB max for orchestrator/activity I/O, external events, custom status and entity state; instance ID ≤100 chars; Dedicated 25 schedulers/region/sub; Consumption 10 schedulers and 5 task hubs per scheduler; up to 90-day retention | Dedicated (capacity units) and Consumption (pay-per-use) SKUs | [Microsoft Learn](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler); [Redmond Mag](https://redmondmag.com/blogs/redmond-dispatch/2025/11/microsoft-makes-durable-task-scheduler-generally-available.aspx) |
| Restate | 32 MiB default journal entry/message limit (RT0003); metadata soft/hard limits at 80%/95% of gRPC size | Restate Cloud pauses invocations after 20 retries to avoid runaway FaaS billing | [Restate 1.6 notes](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md); [Restate 1.5](https://restate.dev/blog/announcing-restate-1-5) |
| Hatchet | Claims >20K tasks/min and 2K+ events/s | — | [Show HN Hatchet v1](https://hn.svelte.dev/item/43572733) |
| Trigger.dev | Warm start 100–300 ms | — | [Trigger.dev v4 GA](https://trigger.dev/changelog/trigger-v4-ga) |

- Temporal's scale signal: 1.9 trillion billable actions in August 2026, up 350% YoY. — [Temporal Series E post](https://temporal.io/blog/temporal-raises-usd550m-series-e-at-usd12-55b-valuation-ai)
- A secondary source claims Restate's Series A narrative centres on "cheap durable execution". This is unverified beyond the headline. — [FourWeekMBA](https://fourweekmba.com/ai-restate-series-a-durable-execution-price/)

### Inferences
- Event-count caps of about 25K–51K per run, and Vercel's 240 s replay ceiling, show that long-lived agent loops (thousands of LLM turns) must still be chunked with continue-as-new or child workflows. That is a live R&D gap for agent workloads.
- Hyperscaler pricing (AWS waits are free; Cloudflare charges per step) sets a low price anchor that independent vendors must answer.

### Gaps
- Per-action prices for Temporal Cloud, Inngest, Restate Cloud, DBOS Cloud/Conductor and AWS durable functions were not captured.
- Inngest's published limits (steps per function, payload sizes) were not fetched.

---

## Q5. Funding and market signals

### Takeaway
Capital is concentrating heavily in Temporal ($12.55B valuation, Sep 2026). Durable execution is now framed explicitly as "AI agent infrastructure", and challengers are raising Series A rounds of $16–21M.

### Cited Findings
- **Temporal**: $550M Series E at $12.55B on Sep 14, 2026. Led by Lightspeed and Wellington, co-led by Goldman Sachs Alternatives and Tiger Global. ARR is above $250M and growing more than 200% YoY. NDR has been above 200% since February. It has 4,300+ paying customers (up 139% YoY) and 43M OSS installs. Snap moves 414M Stories/day on Temporal, and OpenAI usage grew 60-fold in under a year. The prior round was a $300M Series D at $5B (Feb 2026, a16z-led). Funds go to headcount (to 570 people), core primitives, reliability and security. — [Temporal blog](https://temporal.io/blog/temporal-raises-usd550m-series-e-at-usd12-55b-valuation-ai); [Morningstar/BusinessWire](https://www.morningstar.com/news/business-wire/20260914222012/temporal-raises-550m-at-a-1255b-valuation-as-demand-surges-for-reliable-ai-infrastructure)
- **Restate**: $20M Series A (about Sep 30, 2026), led by Singular with Redpoint and Capital One Ventures; total $27M. The founders are ex-Apache Flink creators, and Ahmed Farghal (ex-Stripe/Meta) joined as fourth co-founder. Customers include Replit and Fortune 500 financial services firms. The company is opening an SF hub. — [tech.eu](https://tech.eu/2026/09/30/berlin-based-restate-raises-20m/); [The Next Web](https://thenextweb.com/news/restate-20m-series-a-singular-durable-execution-ai-agents)
- **Inngest**: $21M Series A (Sep 16, 2025). — [Inngest blog index](https://www.inngest.com/blog-markdown)
- **Trigger.dev**: $16M Series A (Dec 17, 2025). — [Seedtable](https://seedtable.com/companies/triggerdev/funding-rounds/series-a-2025-12)
- **Hatchet**: only $500K is recorded (Feb 2024). — [vcbacked](https://www.vcbacked.co/company/hatchet)
- **Cadence** moved to CNCF governance (Oct 2025). — [Cadence blog](https://cadenceworkflow.io/blog/2025/10/06/cadence-joins-cncf-cloud-native-computing-foundation)
- Distribution plays: Inngest is on the Vercel Marketplace (Jul 2025) and DigitalOcean Marketplace (Oct 2025). Temporal holds the AWS AI Competency. — [Inngest blog index](https://www.inngest.com/blog-markdown); [Temporal Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)

### Inferences
- Temporal's NDR above 200% and the 350% YoY action growth suggest that AI-agent workloads are driving consumption growth. That is consistent with every vendor's 2025–2026 launches leaning into agents and streaming.
- First-party hyperscaler offerings (AWS, Azure, Cloudflare, Vercel) bundle durable execution into compute. Independents therefore differentiate on portability, multi-cloud and self-host (Temporal OSS, Restate single binary, DBOS library, Hatchet, Absurd).

### Gaps
- No 2025–2026 funding data was found for DBOS, Golem, Resonate, LittleHorse or Windmill.

---

## Q6. Which capabilities are table stakes and which are differentiators?

### Takeaway
**Table stakes by late 2026**: durable steps with retries, durable sleep/timers, wait-for-external-event/HITL, cron schedules, a run dashboard with pause/resume/cancel/restart, metrics export or OTel, some form of versioning or skew protection, concurrency limits, and AI SDK integrations with streaming. **Still differentiating**: multi-region replication with stated RTO, cross-service durable RPC (Nexus, Restate), restart-from-arbitrary-step/fork, tenant fairness and priority, large-payload offload, auditable identity (Principal Attribution), encryption with key separation, replay-free low latency (checkpointing), and embedded/library deployment (DBOS, Absurd).

### Cited Findings
- **HITL / wait-for-event**: Cloudflare `waitForEvent` (Apr 2025); Trigger.dev waitpoints; Vercel hooks; AWS `context.wait()` and callbacks; Vercel Chat SDK durable approval steps. — [CF changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md); [Trigger.dev v4](https://trigger.dev/changelog/trigger-v4-ga); [Vercel docs](https://vercel.com/docs/workflow); [createwith Vercel Chat SDK note](https://www.createwith.com/tool/vercel/updates/vercel-chat-sdk-adds-durable-human-approval-steps-for-workflows)
- **Streaming to clients (AI)**: Temporal Workflow Streams; Inngest Realtime/useAgent; Vercel streams; Cloudflare `.subscribe()`; DBOS durable streams for the Vercel AI SDK. — [Temporal](https://temporal.io/blog/replay-2026-product-announcements); [Inngest](https://www.inngest.com/blog-markdown); [Vercel](https://vercel.com/docs/workflow); [CF](https://developers.cloudflare.com/changelog/product/workflows/index.md); [DBOS](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026)
- **Fairness / multi-tenancy**: Temporal Priority & Fairness GA; DBOS partitioned queues; Inngest noisy-neighbor post and bursty concurrency. — [Temporal](https://temporal.io/blog/replay-2026-product-announcements); [DBOS](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026); [Inngest](https://www.inngest.com/blog-markdown)
- **Operability (pause, resume, restart, fork)**: Restate 1.5/1.6; DBOS Conductor fork-from-step; Cloudflare local pause/resume/restart; Azure DTS dashboard. — [Restate 1.6](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md); [DBOS Conductor](https://www.dbos.dev/blog/what-is-dbos-conductor); [CF](https://developers.cloudflare.com/changelog/product/workflows/index.md); [MS Learn](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler)
- **Observability**: Temporal OpenMetrics GA; Inngest metrics export, Extended Traces and Insights SQL; Restate SQL introspection; Vercel trace viewer. — sources above
- **Compensation / saga**: Cloudflare per-step rollback handlers (Jun 2026). — [CF changelog](https://developers.cloudflare.com/changelog/product/workflows/index.md)
- **Versioning**: Temporal Worker Versioning GA; Vercel Skew Protection; Restate deployment registration and cross-deployment invocation migration. — [Temporal](https://temporal.io/blog/replay-2026-product-announcements); [Vercel](https://vercel.com/docs/workflow); [Restate 1.5](https://restate.dev/blog/announcing-restate-1-5)
- **Encryption / data access**: AWS durable execution data encryption docs; Vercel decrypted run data gated by a separate RBAC permission; DBOS Conductor "metadata-only mode". — [AWS](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html); [Vercel](https://vercel.com/docs/workflow); [DBOS](https://www.dbos.dev/blog/what-is-dbos-conductor)
- **Multi-region**: Temporal replication GA with 20-min RTO; Vercel region-pinned runs. — [Temporal](https://temporal.io/blog/replay-2026-product-announcements); [Vercel](https://vercel.com/docs/workflow)
- **Cross-service durable calls**: Temporal Nexus GA (Python). — [Temporal](https://temporal.io/blog/replay-2026-product-announcements)
- **Audit identity**: Temporal Principal Attribution (pre-release) adds a non-spoofable caller identity in history. — [Temporal](https://temporal.io/blog/replay-2026-product-announcements)
- **Autoscaling on backlog**: DBOS Conductor KEDA policies; Temporal Serverless Workers. — [DBOS](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026); [Temporal](https://temporal.io/blog/replay-2026-product-announcements)

### Inferences
- Fairness and priority went from a differentiator to near table stakes in 2026. Temporal GA'd it, and DBOS and Inngest shipped or explained theirs within months.
- Remaining differentiators are mostly in data governance: per-tenant encryption keys and rotation, erasure, audit identity, and data residency. Only Temporal (Principal Attribution), Vercel (RBAC on decrypted data) and DBOS (metadata-only Conductor) show visible work. No vendor in this sample publicly advertised in-place codec key rotation or PII erasure of history. That suggests open ground, though it is unverified (see Gaps).

### Gaps
- No primary source was found on whether Temporal, Restate or Inngest offer managed PII erasure or retroactive re-encryption of stored history. This was not searched specifically.
- No primary sources were checked for OTel-native tracing support per engine beyond the items listed.

---

## Q7. What gaps or pain points do users report?

### Takeaway
The persistent complaints are: (1) the determinism constraint and replay-vs-deploy versioning breakage (replay engines), (2) operational and conceptual complexity, where teams "don't understand why we need such a complex system", (3) the fact that durable execution does not remove the need for idempotency (at-least-once steps), (4) history and payload size ceilings, and (5) managed-service incidents and noisy neighbors. The "Just Use Postgres" simplicity pitch (DBOS, Absurd, Hatchet) is the most common positive counter-signal.

### Cited Findings
- Versioning: "a running workflow's history was written by code deployed in March, and replay runs today's code against March's history". Reordering or inserting a step can invalidate every in-flight execution. — [foojay.io](https://foojay.io/today/durable-execution-is-a-property-not-a-product/); [Kanopy Labs](https://kanopylabs.com/blog/restate-vs-temporal-vs-dbos-durable-execution)
- One writer calls replay-vs-deploy incompatibility the "nastiest production failure": non-determinism errors fire mid-flight after a deploy. — [iotdigitaltwinplm comparison](https://iotdigitaltwinplm.com/durable-execution-architecture-temporal-restate-dbos-2026/)
- From the HN thread on Hatchet's "How to think about durable execution" (about Dec 2025/Jan 2026):
  - teeray: each step does its work "at least once", so idempotency is still needed.
  - vouwfietsman: these systems "abstract concepts that are essential for developers to understand", which leads to confusing non-determinism errors.
  - nightpool: external API calls cannot be wrapped in a DB transaction.
  - dminor and nzoschke: DBOS is "much easier to integrate than Temporal" because it "Just Uses Postgres".
  - krisliu0611: on Temporal, "most of my teammates don't understand we need such a complex system".
  - coreylane: switched from Celery to Hatchet for dynamic cron, log isolation, worker affinity and cancellation.
  — [HN item 46245238](https://news.ycombinator.com/item?id=46245238)
- Restate's at-least-once window is documented in a GitHub issue: a `ctx.run` re-executes if the process dies before journaling. — [docs-restate #410](https://github.com/restatedev/docs-restate/issues/410)
- "You Don't Need Temporal Yet" argues that durable execution for agents fits in about 150 lines, reflecting backlash against heavyweight adoption. — [HackerNoon](https://hackernoon.com/you-dont-need-temporal-yet-durable-execution-for-ai-agents-in-150-lines)
- Managed-service reliability: Inngest published incident reports for Oct 2025 (function execution failures) and Sep 18, 2026 (DB connection pool exhaustion). — [Inngest blog index](https://www.inngest.com/blog-markdown)
- Upgrade friction: DBOS Python v3 and TS v5 cannot be read by older versions, a one-way format change. Restate 1.6 rejects deprecated SDK versions for new invocations and removed old retry configuration. — [DBOS Sep 2026](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026); [Restate 1.6](https://github.com/restatedev/restate/blob/main/release-notes/v1.6.0.md)
- History scaling: Vercel warns that runs with more than 2,000 events or 1 GB replay slowly and recommends child workflows or batching. Cloudflare raised its step limit from 1,024 because users resorted to recursive or child workflows. — [Vercel pricing/limits](https://vercel.com/docs/workflows/pricing); [CF changelog](https://developers.cloudflare.com/changelog/post/2026-03-03-step-limits-to-25k/)
- Postgres-backed engines hit Postgres-specific scaling walls: MVCC delete cost, LISTEN/NOTIFY and SELECT DISTINCT. DBOS published fixes for each. — [DBOS blog](https://www.dbos.dev/blog); [DBOS Sep 2026](https://www.dbos.dev/blog/whats-new-in-dbos-september-2026)
- Runaway retry cost on FaaS led Restate Cloud to pause after 20 retries by default. — [Restate 1.5](https://restate.dev/blog/announcing-restate-1-5)

### Inferences
- The two largest unsolved user pains are versioning safety for long-lived runs and history growth for agent loops. They match the most-funded R&D areas: Temporal Worker Versioning and External Payloads, Vercel replay limits and child-workflow guidance, and Restate journal-prefix restart.
- Postgres-native engines win on adoption simplicity but inherit Postgres operational limits: vacuum and retention, connection pools, notify fan-out. Retention/partitioning and connection efficiency are a visible R&D frontier for that camp.

### Gaps
- No systematic pass was made over GitHub issues (for example, temporalio/temporal top-voted issues) or Reddit threads in this session. Pain points come from one HN thread, blog commentary and vendor incident reports. Several comparison blogs (Kanopy Labs, iotdigitaltwinplm, PandaStack) are secondary and possibly SEO-driven, so treat them as opinion.
- No user post-mortems were found for AWS Lambda durable functions, Vercel Workflow or Cloudflare Workflows in production.
