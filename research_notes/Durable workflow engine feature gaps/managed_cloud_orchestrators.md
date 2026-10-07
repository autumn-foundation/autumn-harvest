# Managed-Cloud and Orchestration-Style Durable Workflow Services (as of October 2026)

Scope: AWS Step Functions, AWS Lambda durable functions, Azure Durable Functions / Durable Task Scheduler (DTS) / Microsoft Agent Framework durable extension, Google Cloud Workflows, Netflix Conductor OSS / Orkes Conductor, Camunda 8 / Zeebe. Focus is on features that a self-hosted, embedded durable workflow library may lack. Research date: 2026-10-07. Sources are primary vendor docs unless marked otherwise. Items not re-verified in this session are listed under Gaps, not Findings.

## Step Functions: state types, integrations, Distributed Map, redrive, versions/aliases, limits, Express vs Standard, TestState, Workflow Studio

### Takeaway
Step Functions is the reference managed orchestrator: a JSON DSL (Amazon States Language) with a visual designer, 220+ native AWS service integrations, Standard (exactly-once, 1-year, 25k-event history, redrivable) vs Express (5-minute, unlimited history, no redrive) modes, Distributed Map with 10,000 parallel child executions over S3-scale datasets, versions/aliases, and a 256 KiB payload cap. 2026 additions are agentic: an AgentCore-powered "agentic reasoning step" (June 2026) and 28 new SDK integrations including Bedrock AgentCore (March 2026).

### Cited Findings
**Limits (all from the service quotas page)**
- Max execution time: Standard 1 year; Express 5 minutes; timeout fails with `States.Timeout` and emits `ExecutionsTimedOut` metric — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Max execution history: Standard 25,000 events (execution fails if reached; AWS recommends continue-as-new style "start new executions"); Express unlimited — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Max input/output size for a task, state, or execution: 256 KiB UTF-8 (hard quota, both types) — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Max state machine definition size 1 MB (hard); max API request size 1 MB; 100,000 registered state machines (raisable to 150,000) — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Max open executions per account per Region: 1,000,000 Standard (raisable to "millions"; not applicable to Express) — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Execution history retention: 90 days after close (can be *reduced* to 30 days on request "to meet compliance, organizational, or regulatory requirements"); Express history only via CloudWatch Logs — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Redrivable period: 14 days after an execution completes; "Redrive is not supported for Express workflows" — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- StateTransition throttle (token bucket): Standard 5,000 bucket / 5,000 per sec in us-east-1, us-west-2, eu-west-1; 800/800 elsewhere; Express unlimited — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- StartExecution: Standard 1,300 bucket / 300 per sec refill (large regions), 800/150 elsewhere; Express 6,000/6,000. `StartSyncExecution` (synchronous Express) "doesn't contribute to existing account capacity limits" — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- `RedriveExecution` throttle 1,300/300; `TestState` throttle 50 bucket / 10 per sec; `SendTaskSuccess/Failure/Heartbeat` 3,000/500 — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- HTTP Task: token bucket 300/300 per sec; 60-second hard limit per HTTP request/response — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Versions and aliases: 1,000 published versions per state machine; 100 aliases per state machine — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Task max duration and max time in queue: 1 year Standard / 5 minutes Express (hard) — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)

**Distributed Map**
- Max 1,000 open Map Runs per account (backlogged runs wait at `MapRunStarted`); max 1,000 redrives of a Map Run; max 10,000 parallel child executions per Map Run; children dispatched at up to 1,000 TPS (Express) / 100 TPS (Standard) — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Each iteration runs as a child workflow execution with its own separate history; default (unset or 0) MaxConcurrency = 10,000 parallel children — [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)
- Use Distributed mode when dataset > 256 KiB, history would exceed 25,000 events, or concurrency > 40 iterations (implying Inline Map's practical concurrency ceiling is ~40) — [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)
- Fields: `ItemReader` (S3 CSV/JSON/objects, etc.), `ItemBatcher`, `ResultWriter` (exports child results to S3 grouped by status), `ToleratedFailurePercentage`/`ToleratedFailureCount` (raises `States.ExceedToleratedFailureThreshold`), `Label`, `Retry`/`Catch` (retry of a Map state creates a new Map Run); child `ExecutionType` STANDARD or EXPRESS; Distributed mode is not supported inside Express workflows — [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)
- Data source expansions: Feb 2025 (expanded data source and output options) and Sep 2025 (Athena manifests and Parquet files, improved Distributed Map observability) — [Recent launches](https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html)
- Map Run details page in console; `DescribeMapRun`; child executions emit CloudWatch metrics under a labelled ARN — [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)

**Feature timeline (dated launches)**
- 2022-12-01 Distributed Map; 2023-06-22 versions and aliases; 2023-08-31 Workflow Studio authoring enhancements; 2023-09-07 enhanced error handling; 2023-11-15 redrive from point of failure; 2023-11-26 HTTPS endpoint invocation + TestState API; 2024-06-25 KMS customer-managed keys for workflows/logs/activities; 2024-11-14 IaC export (SAM/CloudFormation/Infrastructure Composer); 2024-11-22 variables + JSONata; 2025-10-30 new metrics dashboard; 2026-03-26 28 new service integrations incl. Amazon Bedrock AgentCore; 2026-06-03 "AgentCore-powered agentic reasoning step" — [Recent launches](https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html)
- Variables let you assign data in one state and reference it later; JSONata enables date/time formatting, math, etc. (announced Nov 26, 2024) — [AWS What's New (China mirror)](https://www.amazonaws.cn/en/new/2024/amazon-step-functions-simplifies-developer-experience-with-support-for-variables-and-jsonata-transformations/)
- In JSONata mode, `MaxConcurrency`, `ToleratedFailurePercentage` etc. accept JSONata expressions; `Assign` stores variables; `Output` replaces JSONPath I/O filters — [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)

**Integrations and positioning**
- AWS itself positions Step Functions as having "native integrations to 220+ services" and "220+ AWS services, 16k APIs", visual builder in console and AWS Toolkit IDE extension, "zero maintenance (no patching, runtime updates)" — [Durable functions or Step Functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-step-functions.html)

**Pricing**
- Standard: $0.025 per 1,000 state transitions; free tier 4,000 transitions/month. Express: $1.00 per million requests + $0.00001667 per GB-second, 100 ms rounding, 64 MB memory chunks — [AWS Step Functions pricing](https://aws.amazon.com/step-functions/pricing) (figures as surfaced by search; page not fetched directly)

### Inferences
- The 25,000-event history cap and 256 KiB payload cap are the two most-cited Step Functions constraints; Distributed Map exists largely to escape them (separate per-child histories, S3 I/O). An embedded library storing history in its own database may not have these caps, which is a positioning advantage — but it also lacks a built-in "child execution with separate history + S3 result writer" primitive unless it has one.
- Express vs Standard is effectively a choice between at-least-once/short/cheap/unredrivable vs exactly-once/long/auditable — a two-tier execution-semantics model that a self-hosted engine could mimic (e.g., an "ephemeral" mode without persisted history).
- Redrive (resume a *failed* execution from the failed state, within 14 days, without re-running successful states) is a distinct feature from retry; library engines often only offer "reset/restart from start".
- Tolerated-failure thresholds on fan-out are an enterprise expectation for batch-style map workloads.

### Gaps
- Exact current count of state types (Task, Choice, Parallel, Map, Pass, Wait, Succeed, Fail) and optimized vs SDK integration counts for 2026 were not re-verified on a primary page this session; the "220+ services / 16k APIs" figure comes from the Lambda comparison page.
- Details (pricing, semantics, GA vs preview) of the June 2026 "agentic reasoning step" and the March 2026 AgentCore integration: only launch titles were retrieved.
- Whether the 256 KiB payload limit has changed in 2026: the quotas page fetched on 2026-10-07 still says 256 KiB.
- Compliance attestations (SOC, HIPAA eligibility, FedRAMP) for Step Functions were not fetched; AWS services are generally listed on AWS's "services in scope" pages.

## AWS Lambda durable functions: programming model (steps, waits, callbacks) and limits

### Takeaway
Lambda durable functions bring a Temporal/Durable-Functions-style checkpoint-and-replay model into Lambda itself: code-first workflows (JS/TS, Python, Java, C#) built from Step, Wait, Wait-for-Condition, Callback, Invoke, Parallel, Map and Child Context operations, running up to one year, with hard caps of 3,000 durable operations and 100 MB persisted data per execution, priced at $8.00 per million durable operations plus data written/retained.

### Cited Findings
- Durable functions run "for up to one year"; use checkpoints and replay — code re-runs from the start "while skipping completed work" — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- Waits "suspend execution without incurring compute charges", positioned for human-in-the-loop and polling; AWS explicitly targets "orchestrating agentic AI applications" — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- Handler is wrapped by the SDK, which supplies a `DurableContext`; SDK reference lives in a separate "AWS Durable Execution SDK Developer Guide" — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- SDK languages: the overview page lists JavaScript, TypeScript, Python, and Java — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html); the SDK page lists JavaScript, TypeScript, Python, Java, and C# (.NET) — [Durable execution SDK](https://docs.aws.amazon.com/lambda/latest/dg/durable-execution-sdk.html); and the comparison table lists only JS/TS and Python — [Durable functions or Step Functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-step-functions.html). The pages are inconsistent, which suggests languages were added over time; C# is the most recent addition.
- Operations: **Step** (checkpointed unit of work, "configurable retry strategies and execution semantics"), **Wait** (duration), **Wait for Condition** (polling with checkpoints between attempts), **Callback** (pause until an external system responds via Lambda API), **Invoke** (call another Lambda and await), **Parallel** ("configurable completion policies"), **Map** (concurrent per-item processing with concurrency control), **Child Context** (isolated grouping) — [Durable execution SDK](https://docs.aws.amazon.com/lambda/latest/dg/durable-execution-sdk.html)
- Checkpoint data is encrypted at rest; each execution has an isolated checkpoint log — [Durable execution SDK](https://docs.aws.amazon.com/lambda/latest/dg/durable-execution-sdk.html)
- Metering: Step = 1 + N retries operations; WaitForCondition = 1 + N polls; WaitForCallback = 3 + N retries (context + callback + step) and persists two copies of the callback payload; Parallel/Map = 1 + N branches; child-context results < 256 KB are checkpointed, larger ones are *recomputed on replay* rather than stored — [Durable execution SDK](https://docs.aws.amazon.com/lambda/latest/dg/durable-execution-sdk.html)
- Quotas: max running durable executions 5,000,000 per Region (10,000,000 in us-east-1/us-west-2/eu-west-1; soft); max 3,000 durable operations per execution (hard); 100 MB cumulative persisted data per execution (hard); 300 RPS function invocation rate to start durable executions; `CheckpointDurableExecution` 10,000 RPS (20,000 large regions); `GetDurableExecutionHistory` 15 RPS; `SendDurableExecutionCallbackSuccess/Failure/Heartbeat` 3,000 RPS (6,000); `StopDurableExecution` 300 RPS (600) — [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)
- Note: an earlier search snippet reported "maximum running durable executions is 1,000,000"; the current quotas page says 5,000,000 / 10,000,000, so the quota appears to have been raised — [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)
- Execution history retention is configurable in days; after that the history is unavailable via `GetDurableExecutionHistory` — [CheckpointDurableExecution API (search snippet)](https://docs.aws.amazon.com/lambda/latest/api/API_CheckpointDurableExecution.html)
- Pricing: durable operations $8.00 per million; data written $0.25/GB; data retained $0.15/GB-month; plus normal Lambda compute/request charges. AWS example: 1M insurance claims/month with ~7-day human review wait = $490.46 total ($421.34 compute, $32.00 durable ops, $26.00 data written, $10.92 retention) — [Lambda pricing](https://aws.amazon.com/lambda/pricing/)
- AWS positioning: durable functions for "application development in Lambda" and code-first teams; Step Functions for visual design, cross-service orchestration, and non-technical stakeholders. AWS suggests hybrid use — [Durable functions or Step Functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-step-functions.html)
- Docs pages exist for monitoring/debugging, security, encryption ("Encrypting AWS Lambda durable execution data") and best practices — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)

### Inferences
- The 3,000-operation cap is far below Temporal's or Step Functions' history limits; long-lived or chatty workflows need child contexts, invokes or restart patterns. A self-hosted engine without a hard cap may be advantageous here.
- The operation set (step/wait/waitForCondition/callback/invoke/parallel/map/childContext) is now the de-facto baseline API for code-first durable execution in 2026. An embedded library should map each one, especially callbacks with heartbeat and fail APIs, and parallel/map completion policies.
- Per-operation metering means AWS charges retries and polls individually. This is a cost-transparency model that a self-hosted engine can contrast against.

### Gaps
- Announcement date: this session did not re-verify that the launch was late 2025 (re:Invent, Dec 2025). The docs do not show a launch date.
- Max single wait duration, callback timeout ceilings, and the default and maximum retention days were not retrieved.
- Retry strategy specifics (backoff parameters, at-most-once vs at-least-once step semantics flags) live in the separate Durable Execution SDK guide, which this session did not fetch.
- Region availability and compliance scope for durable functions were not verified.

## Azure: Durable Functions patterns, orchestration versioning, Durable Task Scheduler GA features, durable agents

### Takeaway
Azure's stack is Durable Functions (on Azure Functions) and the portable Durable Task SDKs. Both run on the Durable Task Scheduler (DTS), a managed gRPC backend with a built-in RBAC-secured dashboard, multiple task hubs, autopurge, private endpoints, an emulator, and Dedicated vs Consumption billing. Orchestration versioning has first-class strategies: `defaultVersion` plus `None`/`Strict`/`CurrentOrOlder` matching and `Reject`/`Fail` handling. The Microsoft Agent Framework durable extension implements each agent session as a durable entity.

### Cited Findings
**Durable Task Scheduler (DTS)**
- DTS is "the recommended storage provider" for Durable Functions and the Durable Task SDKs. It is a "purpose-built backend-as-a-service", unlike the BYO storage providers (Azure Storage, MSSQL, etc.) — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) (page updated 2026-09-21)
- Apps connect over gRPC with TLS and the app's identity. The endpoint is `{scheduler}.{region}.durabletask.io`. Private endpoints are supported. Work items use a **push** model, so no polling — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- State uses an in-memory store for short-lived state and a persistent store for recovery and multi-instance queries. No separate storage account is needed — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- Built-in dashboard: filter instances; view status, duration, input and output; drill into sub-orchestrations and activities. It supports pause, terminate, and restart. Access is secured "by identity and role-based access controls" — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- Multiple task hubs per scheduler, each with its own dashboard and RBAC. Example uses: per environment or per team. Hubs share one scheduler's resources, so the noisy-neighbor risk is noted — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- The emulator runs as a local Docker container with the same dashboard. It keeps state in memory and is not for production — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- Autopurge retention policies clean up stale orchestration data automatically — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- Limits: 1 MB max each for orchestrator I/O, activity I/O, external event data, custom status, and entity state. Above that, use the "Large payload support" workaround. Instance IDs are ≤100 printable ASCII characters, and `@` prefix is reserved for entities. Dedicated SKU allows 25 schedulers and 25 task hubs per region per subscription. Consumption allows 10 schedulers per region and 5 task hubs per scheduler — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- Billing: Dedicated is a fixed monthly cost per Capacity Unit. One CU gives up to 2,000 actions/sec and 50 GB of orchestration data, with up to 3 CUs per deployment. HA needs 3 CUs, and retention is up to 90 days. Consumption is pay per action dispatched, with a 500 actions/sec cap, retention up to 30 days, and no HA — [DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing)
- An "action" is any message the scheduler dispatches: orchestration start, activity start, timer completion, external event, entity operation, pause/resume/terminate, or result processing. Each activity costs 2 actions (schedule + result). Prices are regional and listed on the Azure Functions pricing page, separate from compute — [DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing)
- DTS markets itself for "Distributed transactions, Multi-agent orchestration, Data processing, Infrastructure management". It works with any Functions SKU, and the Durable Task SDKs run on any compute (ACA, AKS, App Service) — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler); [DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing)

**Orchestration versioning**
- Each instance is permanently associated with a version string at creation, exposed read-only as `context.Version`. Pre-existing instances have a null version. Workers with newer code run older instances ("backward compatibility"). The runtime stops older workers from running newer-version instances ("forward protection") — [Orchestration versioning](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)
- Match strategies: `None`, `Strict`, and `CurrentOrOlder` (default). Mismatch strategies: `Reject` (default; back to the queue for another worker) and `Fail` (terminal). Configured via `host.json` `defaultVersion`, `versionMatchStrategy`, and `versionFailureStrategy`, or via the worker builder in the SDKs — [Orchestration versioning](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)
- Callers can start orchestrations and sub-orchestrations with an explicit version, for gradual migration, rollback, or testing — [Orchestration versioning](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)
- Minimum packages: .NET isolated `Microsoft.Azure.Functions.Worker.Extensions.DurableTask` 1.14.0. Non-.NET languages need Extension Bundle 4.30.0+. JS `durable-functions` 3.3.0, Python `azure-functions-durable` 1.5.0, PowerShell SDK 2.2.0, Java 1.6.3. Durable Task SDKs: .NET client v1.9.0+ and Java v1.6.0+. SDKs also offer `CompareVersionTo` helpers — [Orchestration versioning](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)
- Microsoft notes that old workers can interfere with routing on the Azure Storage and MSSQL providers, and recommends DTS for "an improved routing mechanism" — [Orchestration versioning](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)

**Durable agents (Microsoft Agent Framework durable extension)**
- Registering an agent with the extension makes it durable: "persistent sessions, built-in API endpoints, and distributed scaling — without changes to your agent logic". It is implemented as "entity-based agent loops, where each agent session is a durable entity" — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework) (doc dated 2026-05-04)
- Hosting is Azure Functions (Flex Consumption, "thousands of concurrent agent sessions (or to zero)") or bring-your-own-compute. Languages are C# and Python — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
- Multi-agent orchestrations checkpoint each agent call (`context.GetAgent()` returns `DurableAIAgent`). Graph-based Agent Framework workflows (`WorkflowBuilder`) get checkpoints too. Supported patterns: sequential, fan-out/fan-in, conditional (switch-case) routing, and HITL via `RequestPort`/`ctx.request_info()`. HITL auto-generates run, status, and respond HTTP endpoints — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
- The DTS dashboard shows agent conversation history, tool calls, structured outputs, and orchestration traces — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
- Session TTL defaults to 14 days, with a 5-minute minimum deletion delay. TTL is configurable in .NET only — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
- Known limitations: conversation state is bounded by DTS's 1 MB entity-state limit, so compaction is manual. Routing adds latency. Streaming goes through response callbacks (for example Redis Streams) because entities are request/response — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
- Advanced samples cover long-running tools that start orchestrations, an "Agent as MCP tool", and reliable resumable streaming — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)

### Inferences
- Azure's versioning design (an instance-pinned version, worker-side match and failure strategies, and an explicit version at start) is a concrete, documented pattern. An embedded library could adopt it nearly verbatim.
- The DTS dashboard is bundled at no extra product cost. Along with its RBAC, multi-hub isolation, and autopurge, it sets the observability baseline that a library-only engine would need to match with its own UI.
- Durable entities (actors with serialized operations and 1 MB state) are the foundation of Microsoft's agent story. An engine without an entity/actor primitive lacks a natural model for long-lived agent sessions.

### Gaps
- DTS GA dates were not confirmed in this session. Neither page fetched states GA vs preview explicitly. My unverified understanding is that the Dedicated SKU went GA in 2025 and the Consumption SKU arrived later. Treat this as unverified.
- Per-action and per-CU list prices: the docs defer to the Azure Functions pricing page, which this session did not fetch.
- The Durable Functions classic patterns overview page was not fetched: function chaining, fan-out/fan-in, async HTTP APIs, monitor, human interaction, aggregator (entities). These patterns are long-standing and well known, but they lack a primary citation here.
- The release status of the Durable Task extension for Agent Framework (preview vs GA) is not stated on the page fetched.
- DTS compliance certifications were not checked.

## Google Cloud Workflows: connectors, callbacks, limits

### Takeaway
Google Cloud Workflows is a low-cost, YAML/JSON step-based serverless orchestrator. Its limits are tight compared with the others: 512 KB of variable memory, 100,000 steps per execution, 10 parallel branches with 20 concurrent branches or iterations, and 10,000 concurrent executions per region. It offers 1-year executions, HTTP callbacks for HITL, and connectors to Google Cloud APIs. Billing is per step: $0.01 per 1,000 internal steps and $0.025 per 1,000 external steps.

### Cited Findings
- Limits: max 100,000 steps per execution and 1-year max duration. Executions are retained 90 days. Total variable memory is 512 KB, and execution arguments are 512 KB. HTTP responses can be up to 2 MB, but variable memory still applies. Strings are capped at 256 KB and workflow source at 128 KB — [Workflows quotas](https://docs.cloud.google.com/workflows/quotas)
- Concurrency: 10,000 active executions per region per project, plus up to 100,000 backlogged executions. Parallel steps allow 10 branches and 2 levels of nesting, with 20 branches or iterations running concurrently per execution before queuing. Other caps: 50 assignments per step, 50 switch conditions, and a call stack depth of 20 — [Workflows quotas](https://docs.cloud.google.com/workflows/quotas)
- Pricing: internal steps are free for the first 5,000 per month, then $0.01 per 1,000. External steps are free for the first 2,000, then $0.025 per 1,000. Above 100M steps, contact sales. Internal steps include googleapis.com calls, Cloud Run function calls, assignments, conditions, subworkflow calls, connector calls, and **connector polling attempts**. External steps include non-GCP HTTP calls and **waiting for callbacks via `events.await_callback`**. Failed and retried steps are billed — [Workflows pricing](https://cloud.google.com/workflows/pricing)
- Connectors provide blocking steps for Google Cloud services with long-running operations. HTTP callbacks create unique callback URLs and can wait up to one year, for external systems or human-in-the-loop use — [Google Cloud Workflows product page (search summary)](https://cloud.google.com/workflows?hl=en)

### Inferences
- The 512 KB variable memory and 20-way concurrent iteration limits make Workflows unsuitable for large fan-out. It is a "glue" orchestrator for GCP API calls, not a data-parallel engine. It has no equivalent of Distributed Map.
- Workflows has no visual designer of note and no human task inbox (callbacks only). It is the managed service closest to "embedded-library feature parity", which makes it a useful lower-bound comparator.

### Gaps
- The count of available connectors and their catalog (e.g., BigQuery, Cloud Run jobs, Pub/Sub) were not verified on a primary page.
- Retry/try-except semantics (default retry policies like `http.default_retry`) were not fetched.
- 2025–2026 feature additions (e.g., any Gemini/Vertex AI agent integrations, execution debugging/step history UI) were not verified.

## Conductor OSS / Orkes Conductor: JSON definitions, system tasks, human tasks, event handlers, AI orchestration, versioning, rate limits

### Takeaway
Conductor is a JSON-defined, server-side orchestrator with polling workers and a large catalog of built-in system tasks. In 2026 that catalog includes native LLM, embedding/vector, MCP and HUMAN tasks. Orkes, the commercial distribution, adds RBAC, SSO, secrets, task forms, AI Prompt Studio, a scheduler, webhooks, a 99.99% SLA with multi-region failover, and a claimed 10x throughput over OSS.

### Cited Findings
- Built-in system tasks include HTTP calls, inline JavaScript, JSON transforms, event publishing, wait timers, and human approval gates. LLM and AI-agent orchestration are native system tasks — [Conductor OSS docs](https://docs.conductor-oss.org/devguide/concepts/conductor.html) (via search summary)
- AI task types: `LLM_TEXT_COMPLETE`, `LLM_CHAT_COMPLETE`, `LLM_INDEX_TEXT`, Generate/Store/Get/Search Embeddings, Index Document, Search Index, Chunk Text, List Files, Parse Document, Generate Image/Audio/Video/PDF — [Orkes AI tasks](https://docs.orkes.io/content/category/reference-docs/ai-tasks); [Conductor LLM orchestration](https://docs.conductor-oss.org/devguide/ai/llm-orchestration) (via search summary)
- MCP is a first-class integration through the `LIST_MCP_TOOLS` and `CALL_MCP_TOOL` system tasks, and workflows can be exposed as MCP tools — [Conductor MCP guide](https://docs.conductor-oss.org/devguide/ai/mcp-guide) (via search summary)
- The `HUMAN` task is a durable pause. It survives restarts and deploys and resumes when a human responds via the Task Update API — [Conductor human-in-the-loop](https://docs.conductor-oss.org/devguide/ai/human-in-the-loop) (via search summary)
- Rate limits are declared in workflow or task definitions without custom code. Excess scheduled tasks are held PENDING until in-progress tasks complete — [Orkes rate limits](https://www.orkes.io/content/rate-limits) (via search summary)
- Orkes vs OSS, per Orkes's own comparison: RBAC to "securely share workflows, tasks, secrets, AI prompts"; directory sync and SSO with Okta, AD, and OAuth 2.0 providers; secrets management; up to 99.99% SLA with multi-region auto-failover; AI Prompt Studio; vector DB integration; human tasks with visual task forms and Human Task APIs; an enhanced visual workflow creator; a scheduler; webhooks to signal or start workflows; synchronous execution for API-gateway use; "1000+ tasks/second vs ~100 for OSS"; "60k parallel forks"; managed on AWS, Azure, and GCP, or on-prem — [Conductor OSS vs Orkes](https://docs.orkes.io/platform/conductor-oss-vs-orkes)

### Inferences
- Conductor is the main reference for a "system task catalog" approach, where LLM, vector, MCP, and human tasks are configured rather than coded. An embedded library would provide these as integrations or activities, not as engine primitives.
- The 1000 vs ~100 tasks/sec claim comes from the vendor and should be treated as marketing.

### Gaps
- This session did not fetch primary detail on several features: Conductor's JSON workflow definition schema, the full system/operator task list (FORK_JOIN, FORK_JOIN_DYNAMIC, DO_WHILE, SWITCH, SUB_WORKFLOW, WAIT, EVENT, TERMINATE, JSON_JQ_TRANSFORM, etc.), event handlers, and workflow versioning semantics (running instances stay pinned to their version). These are known from training data but are uncited here.
- Orkes compliance certifications (SOC 2 Type II, HIPAA, etc.) and pricing were not verified.
- Netflix's own Conductor status was not re-verified. My understanding is that Netflix archived its repo and Conductor OSS continues under Orkes stewardship (conductor-oss/conductor).

## Camunda 8 / Zeebe: BPMN execution, DMN, user tasks, Operate/Optimize/Tasklist, connectors, process versioning and instance migration

### Takeaway
Camunda 8 is the BPMN/DMN standard-based option: modeler, Zeebe engine, Operate (monitoring and repair), Tasklist (human tasks), Optimize (process analytics), and a connector marketplace. Version 8.8 merged these into a single "Orchestration Cluster" and added agentic orchestration: an AI Agent connector with ad-hoc sub-processes and an MCP Client connector. Process instance migration is documented, but only for instances in a wait state and with element-type restrictions.

### Cited Findings
- Process instance migration "fit[s] a running process instance to a different process definition". It supports inactive-flow changes, mapping active elements to same-type targets, preserved variables, user task assignments, and job properties, subprocess and call-activity elements, adding or removing catch-event subscriptions (message, timer, signal, error, escalation, compensation), gateways, and converting job-worker user tasks to Camunda user tasks — [Camunda process instance migration](https://docs.camunda.io/docs/components/concepts/process-instance-migration/)
- Migration limitations: only supported BPMN elements can be migrated, and throw, start, and end events are excluded. Multi-instance bodies cannot switch between parallel and sequential. Active elements cannot be nested or unnested, and element types cannot change. The instance "must be in a wait state". Every active element needs an explicit mapping, and the target definition must exist in Zeebe. Operate provides a UI. Docs are current as of 8.9 — [Camunda process instance migration](https://docs.camunda.io/docs/components/concepts/process-instance-migration/)
- Camunda 8.8 introduced the AI Agent connector (an LLM reasoning and tool-selection loop meant to run inside an ad-hoc sub-process), the MCP Client connector, and the Ad-hoc tools schema resolver connector. It also consolidated Zeebe, Operate, Tasklist, and Identity into a single "Orchestration Cluster", formerly called the automation cluster — [Camunda 8.8 release notes](https://docs.camunda.io/docs/reference/announcements-release-notes/880/880-release-notes/) (via search summary)
- SaaS runs on GCP and hosts orchestration cluster components (Zeebe, Tasklist, Operate, Optimize, Connectors) in GCP or AWS regions. Access to Modeler, Operate, Tasklist, and Optimize requires organization membership — [Camunda SaaS docs](https://docs.camunda.io/docs/components/saas/) (via search summary)
- The connectors marketplace includes REST, SOAP, Kafka, RabbitMQ, AWS and Google services, Slack, and email — [AutomationAtlas review (secondary)](https://automationatlas.io/answers/camunda-review-2026/)
- Pricing: the SaaS tiers are Free (€0, 5 user seats, unlimited BPMN/DMN modeling, 30-day trial of cluster and agentic features, US Central region) and Enterprise (contact sales) — [AutomationAtlas pricing explainer (secondary)](https://automationatlas.io/answers/camunda-pricing-explained-2026/)

### Inferences
- Camunda is the strongest comparator for enterprise features that a code-first library typically lacks: a standards-based visual model (BPMN 2.0), decision tables (DMN), a human task inbox with forms (Tasklist), process analytics (Optimize), operator repair and instance migration (Operate), and a connectors marketplace.
- Camunda's approach to agents keeps AI inside a governed BPMN process. The ad-hoc sub-process acts as the agent's tool-selection boundary, which contrasts with code-first agent loops.

### Gaps
- The following were not fetched directly: DMN feature details (FEEL, hit policies), user task features (assignment, candidate groups, due dates, forms, the Tasklist API), Optimize capabilities, Zeebe partitioning/throughput, and process versioning defaults (new instances get the latest version, running ones stay pinned).
- Camunda's compliance certifications (SOC 2, ISO 27001) were not verified. The search returned nothing on them.
- Camunda licensing changes, such as the self-managed production license requirements for 8.x components, were not verified.

## Cross-cutting: enterprise expectations (compliance, audit, RBAC, SSO, multi-region, observability, redrive) and pricing models

### Takeaway
Across these services, enterprises expect five things beyond the core durable-execution semantics. First, an operator console with drill-down plus pause, terminate, restart and redrive actions. Second, RBAC and SSO scoped per namespace, task hub or team. Third, configurable retention and purge, both for compliance and for cost. Fourth, customer-managed encryption keys. Fifth, HA or multi-region failover. Pricing models fall into five families: per state transition (Step Functions Standard), per request plus GB-second (Express), per operation plus data written/retained (Lambda durable), per dispatched action or provisioned capacity unit (Azure DTS), and per step (Google Workflows). Conductor/Orkes and Camunda price their enterprise tiers through sales.

### Cited Findings
- Retention is a configurable compliance control. Step Functions keeps 90 days by default and can be reduced to 30 "to meet compliance, organizational, or regulatory requirements" — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html). DTS retains data up to 90 days on Dedicated and 30 on Consumption, with autopurge policies — [DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing). Google Workflows retains 90 days — [Workflows quotas](https://docs.cloud.google.com/workflows/quotas). Lambda durable retention is configurable in days and billed at $0.15/GB-month — [Lambda pricing](https://aws.amazon.com/lambda/pricing/)
- Customer-managed KMS keys for workflows, logs and activities shipped in Step Functions on 2024-06-25 — [Step Functions recent launches](https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html). Lambda durable checkpoint data is encrypted at rest and has a dedicated encryption doc — [Durable execution SDK](https://docs.aws.amazon.com/lambda/latest/dg/durable-execution-sdk.html); [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- RBAC and SSO: the DTS dashboard and task hubs are secured by Azure identity and RBAC, with private endpoints available — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler). Orkes offers RBAC plus Okta, AD and OAuth SSO with directory sync — [Conductor OSS vs Orkes](https://docs.orkes.io/platform/conductor-oss-vs-orkes)
- HA and multi-region: DTS high availability requires 3 CUs, and Consumption has no HA — [DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing). Orkes offers a 99.99% SLA with multi-region auto-failover — [Conductor OSS vs Orkes](https://docs.orkes.io/platform/conductor-oss-vs-orkes)
- Observability consoles: Step Functions added a new metrics dashboard on 2025-10-30 and has a Map Run details page — [Recent launches](https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html); [Distributed Map](https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html). Azure ships the DTS dashboard, including agent conversation views — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework). Camunda ships Operate and Optimize — [Camunda SaaS docs](https://docs.camunda.io/docs/components/saas/)
- Restart and repair: Step Functions redrive runs from the point of failure within 14 days, Standard only — [Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html). The DTS dashboard can pause, terminate and restart instances — [DTS overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler). Camunda Operate offers instance migration — [Camunda migration](https://docs.camunda.io/docs/components/concepts/process-instance-migration/)
- AI agent orchestration is now table stakes across all vendors:
  - Step Functions: AgentCore agentic reasoning step (2026-06) — [Recent launches](https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html)
  - Lambda durable: positioned for "agentic AI applications" — [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
  - Azure: durable agents — [Durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework)
  - Conductor: LLM and MCP system tasks — [Conductor MCP guide](https://docs.conductor-oss.org/devguide/ai/mcp-guide)
  - Camunda 8.8: AI Agent and MCP connectors — [Camunda 8.8 release notes](https://docs.camunda.io/docs/reference/announcements-release-notes/880/880-release-notes/)
- Pricing summary:
  - Step Functions Standard: $0.025/1k transitions
  - Step Functions Express: $1/M requests plus $0.00001667/GB-s ([Step Functions pricing](https://aws.amazon.com/step-functions/pricing))
  - Lambda durable: $8/M ops, $0.25/GB written, $0.15/GB-month retained ([Lambda pricing](https://aws.amazon.com/lambda/pricing/))
  - Azure DTS: per-CU monthly (Dedicated) or per-million-actions (Consumption) ([DTS billing](https://learn.microsoft.com/en-us/azure/durable-task/scheduler/durable-task-scheduler-billing))
  - Google Workflows: $0.01/1k internal steps, $0.025/1k external steps ([Workflows pricing](https://cloud.google.com/workflows/pricing))
  - Camunda SaaS: Free or Enterprise via sales ([AutomationAtlas, secondary](https://automationatlas.io/answers/camunda-pricing-explained-2026/))

### Inferences
- These services share one feature-gap checklist for a self-hosted embedded library:
  1. An operator UI with search, drill-down and bulk actions.
  2. Redrive or resume-from-failure that is distinct from retry.
  3. Instance-pinned versioning with worker match strategies.
  4. Fan-out at 10k+ concurrency with failure-tolerance thresholds and external result sinks.
  5. Callbacks with tokens, heartbeat and fail APIs.
  6. Retention, autopurge and right-to-erasure controls.
  7. CMK encryption of payloads.
  8. RBAC and multi-tenant isolation (namespaces or task hubs).
  9. Human task inbox and forms (Camunda Tasklist, Orkes task forms).
  10. Declarative rate limits (Orkes).
  11. Visual design and visualization (Workflow Studio, Camunda Modeler, Orkes creator).
  12. Connector catalogs.
  13. First-class LLM, MCP and agent primitives.
- Payload caps differ widely: Step Functions 256 KiB, Google Workflows 512 KB of variables, DTS 1 MB, Lambda durable 100 MB total per execution. A large-payload or offload story (S3 or blob claim-check) is an expected feature.

### Gaps
- No vendor compliance attestations were retrieved in this session: SOC 2, HIPAA BAA eligibility, ISO 27001, FedRAMP or PCI for any service. This needs a separate pass over the AWS, Azure and GCP compliance scope pages, the Orkes trust center and the Camunda trust center.
- Audit-log specifics were not researched: CloudTrail for Step Functions and Lambda, Azure Activity Log for DTS, and Camunda or Orkes audit trails.
- Multi-region DR for Step Functions, Lambda durable and Google Workflows was not researched. These are regional services, and cross-region replication of in-flight executions is not documented in the pages fetched.
- Airflow 3 and Dagster were in optional scope but not researched, because the tool-call budget was used on the primary services.
