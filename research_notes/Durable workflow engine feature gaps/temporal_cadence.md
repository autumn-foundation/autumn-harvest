# Temporal and Cadence: Reference Feature Catalog (as of October 2026)

Release-stage vocabulary used throughout (Temporal's own definitions): **Pre-release** = experimental, limited functionality, may be disabled by default, invite-only on Cloud, no GA guarantee; **Public Preview** = core functionality complete, recommended for production, APIs may change while keeping backward compatibility, formal support; **GA** = stable APIs, committed SLA, all regions — [Temporal release stages](https://docs.temporal.io/evaluate/development-production-features/release-stages)

Key dated events: Replay 2025 announcements published **March 4, 2025** — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements). Replay 2026 held **May 5–7, 2026** at Moscone Center — [Temporal changelog banner](https://temporal.io/change-log/ruby-sdk-generally-available); announcements — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements).

---

## Programming model (workflows, activities, messages, Nexus, Standalone Activities, reset/pause, activity operations)

### Takeaway
Temporal's programming model is a superset reference: Workflows + Activities + three message types (Signals, Queries, Updates with validators and Update-with-Start), Signal-with-Start, Nexus for cross-namespace service calls (GA since March 2025), and — new in 2026 — Standalone Activities (GA Sept 2026, all six main SDKs), Standalone Nexus Operations (pre-release, server v1.32), Workflow Pause (pre-release, server v1.30+), and Activity pause/unpause/reset/update-options (Public Preview since March 2025).

### Cited Findings
**Messages**
- Three message types: Queries (read-only, no history entries, work on completed workflows), Signals (async fire-and-forget, sender cannot await result/error), Updates (synchronous write; caller gets completion or error; recorded in Event History) — [Temporal message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing)
- Update validators are optional, take the same args as the handler, and reject by returning an error/panicking; a rejected Update writes no `WorkflowExecutionUpdateAccepted` event and the caller gets "Update failed" — [Go message passing](https://docs.temporal.io/develop/go/message-passing)
- Update wait stages: `Accepted` (returns after validation) and `Completed` (returns after execution) — [Go message passing](https://docs.temporal.io/develop/go/message-passing)
- Update-with-Start: if the workflow exists the Update is processed; otherwise a new workflow is started and the Update runs before the main workflow method; `WorkflowIDConflictPolicy` is mandatory ("Use Existing" + idempotent handler recommended) — [Go message passing](https://docs.temporal.io/develop/go/message-passing)
- Signal-with-Start: signals the running workflow with that ID, or starts one and signals it immediately; takes no Run ID — [Go message passing](https://docs.temporal.io/develop/go/message-passing)
- Workers warn when a workflow completes with unfinished Update handlers; per-handler `HandlerUnfinishedPolicyAbandon` suppresses it. Continue-as-New is not allowed inside Update handlers; no direct workflow-to-workflow Updates (use an Activity) — [Go message passing](https://docs.temporal.io/develop/go/message-passing)
- Limits: 10 in-flight Updates and 2,000 total Updates per execution history; 10,000 Signals per execution — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)

**Nexus**
- Nexus GA announced at Replay 2025 (Mar 2025): connects Temporal applications across isolated Namespaces with service contracts and reverse-proxy endpoints — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- Per-SDK status at Replay 2026 (May 2026): GA in Python SDK; Public Preview in TypeScript and .NET — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements). (Go and Java were the earlier GA SDKs; the docs carry guides for Go, Java, Python, TypeScript, .NET — [Nexus docs](https://docs.temporal.io/nexus).)
- Model: Endpoints (reverse proxies routing to a target namespace + task queue), Services (groups of Operations), Operations; sync operations must finish within a 10-second handler deadline; async operations (start workflow, Standalone Activity, Update) return operation tokens with up to 60-day Schedule-to-Close; built-in retries with exponential backoff, rate/concurrency limiting, circuit breaker that trips after 5 consecutive retryable errors; at-least-once semantics; caller-namespace allowlists; multi-region endpoints on Cloud — [Nexus docs](https://docs.temporal.io/nexus)
- Cloud Nexus limits: 100 endpoints per account, 1,000 caller namespaces per endpoint, 30 in-flight Nexus operations per workflow, 2,000 callbacks per execution — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Server v1.31.0 (Apr 29, 2026): Nexus error-model redesign, Nexus always enabled, caller timeouts supported. Server v1.32.0 (Sept 11, 2026): **Standalone Nexus Operations** in Pre-release (top-level Nexus operations started directly from clients, durable retries and cancellation) — [temporal releases](https://github.com/temporalio/temporal/releases)

**Standalone Activities (activities without a workflow — durable job queue)**
- Replay 2026: Public Preview for Go, Python, .NET; Pre-release for Java, TypeScript — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- **GA Sept 15, 2026** "across all six SDKs" (Go, Python, Java, .NET, TypeScript, Ruby), for durable background jobs — [Temporal changelog](https://temporal.io/change-log)
- Server v1.31: public preview, off by default (`activity.enableStandalone`); v1.32: GA, on by default, with delayed starts, operator pause/resume, and batch cancel/terminate/delete — [temporal releases](https://github.com/temporalio/temporal/releases)
- Server v1.32 also enables **Activity Eager Execution** by default — [temporal releases](https://github.com/temporalio/temporal/releases)

**Operator controls on live executions**
- Activity operations commands — pause, unpause, reset, and update live Activities in production without code changes — Public Preview (Mar 2025) — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- Workflow Reset with Child Workflows — Pre-release (Mar 2025) — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- **Workflow Pause**: `temporal workflow pause` / `unpause` hold a workflow without terminating or losing state (incident, investigation, dependency outage); via CLI, UI or gRPC; Pre-release; self-hosted needs Server v1.30.0+ with `frontend.WorkflowPauseEnabled` and CLI v1.6.0+; Cloud invite-only — [Workflow Pause docs](https://docs.temporal.io/encyclopedia/workflow/workflow-pause)
- Principal Attribution (server-derived, non-spoofable "who invoked this" field on executions) — Pre-release Apr 30, 2026; self-hosted flag `system.enablePrincipalAttribution` in v1.31 — [Temporal changelog](https://temporal.io/change-log); [temporal releases](https://github.com/temporalio/temporal/releases)
- Timer max duration 100 years — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)

**Internal architecture note**
- Server v1.31 enables the **CHASM** framework by default (separate `businessID` spaces per archetype, with schema changes) — the substrate for Standalone Activities/Nexus as top-level executions — [temporal releases](https://github.com/temporalio/temporal/releases)

### Inferences
- The 2025–2026 direction is "every primitive becomes a top-level, independently addressable durable execution" (Standalone Activities, Standalone Nexus Operations) — Temporal is converging with job-queue products. An engine without a standalone durable job primitive now has a visible gap.
- Operator "live surgery" (pause/unpause/reset/update activity options, workflow pause) is now part of the reference standard, not just termination/reset.

### Gaps
- Not re-verified this session from primary docs (well-known core Temporal features, but no fetched citation): local activities, child workflows and Parent Close Policy (Terminate/Abandon/RequestCancel), continue-as-new semantics, cancellation scopes (TS/Java), side effects/mutable side effects, saga/compensation helpers, async activity completion via task token, dynamic handlers. The report writer should treat these as standard Temporal features but cite docs.temporal.io if exact semantics are needed.
- Exact GA dates for Nexus in TypeScript/.NET after May 2026 not found.

---

## Versioning (patching, Worker Versioning, replay testing)

### Takeaway
Worker Versioning (Worker Deployments with Pinned vs Auto-Upgrade workflows, ramping) went Pre-release Mar 2025 → GA at Replay 2026 (server Worker Deployment APIs GA in v1.31.0, Apr 29, 2026); the Kubernetes Temporal Worker Controller went GA May 4, 2026. Legacy V1/V2 versioning APIs are removed in server v1.33. Patching (`getVersion`/`patched`) remains the in-code mechanism for Auto-Upgrade workflows.

### Cited Findings
- Worker Versioning APIs + Deployments abstraction launched Pre-release at Replay 2025: pin workflows to deployment versions, ramp/cut over traffic, test versions before rollout, via APIs or Kubernetes controller — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- Worker Versioning GA at Replay 2026, "pins running Workflows to the Worker version that started them" — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- Server: Worker Deployment APIs GA in v1.31.0 (preview since v1.28); v1.32 adds one-time version overrides and enables version-reactivation signals by default; v1.32 is the final release with deprecated V1/V2 versioning APIs, removed in v1.33 — [temporal releases](https://github.com/temporalio/temporal/releases)
- Concepts: Worker Deployment (group of similar workers) and Deployment Version (deployment name + Build ID) reported by polling workers. **Pinned** workflows complete on one version (move only via manual CLI); **Auto-Upgrade** workflows move to the latest version but need patching for replay safety. Version states: Inactive → Active (Current or Ramping) → Draining → Drained (queries still served). Ramping version receives a configurable 0–100% of new workflows; target chosen by ramp % and Workflow ID. Pinned parents pass version to pinned children in the same deployment, but not across continue-as-new/retries/cron by default — [Worker Versioning docs](https://docs.temporal.io/worker-versioning)
- Temporal Worker Controller (Kubernetes) GA May 4, 2026, "manage the entire lifecycle of their Temporal workers" — [Temporal changelog](https://temporal.io/change-log)
- Cloud limits: 100 worker deployments per namespace, 100 versions per deployment, 100 task queues per deployment version — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Replay testing: `Replayer` validates a workflow definition against stored histories (JSON or fetched via client); succeeds only if deterministic-compatible — [Python testing suite](https://docs.temporal.io/develop/python/testing-suite)
- Cadence equivalent: workflow **shadowing** replays production histories against new code to catch non-determinism — [Cadence blog: replayers and shadowers](https://cadenceworkflow.io/blog/2023/08/27/nondeterministic-errors-replayers-shadowers); Uber's non-determinism detection "blocked 500+ diffs" and cut detection from days to minutes — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update); Cadence also published "Safe Deployments of Versioned Workflows" (July 1, 2025) — [Cadence blog index](https://cadenceworkflow.io/blog)

### Inferences
- The reference standard is now: deterministic replay + in-code patch markers + deployment-level pinning with ramp percentages + a Kubernetes operator. Pinning shifts the burden from code patches to running multiple worker versions concurrently (infra cost) — a design trade-off a comparison should call out.

### Gaps
- Details of the Patching API deprecation path (`deprecatePatch`) and Cadence's exact versioned-deployment mechanism not fetched.

---

## Task routing (task queues, priority/fairness, rate limits, worker tuning, serverless workers)

### Takeaway
Task Queue Priority & Fairness went GA May 5, 2026 (priority 1–5, weighted fairness keys, per-key RPS limits). Resource-based auto-tuning (slot suppliers) has been GA in all SDKs since Mar 2025. Serverless Workers: AWS Lambda Public Preview (Aug 3, 2026), GCP Cloud Run Pre-release (Aug 6, 2026).

### Cited Findings
- **Priority**: integer 1–5, default 3, lower = higher priority, strict tiering — [Priority & Fairness docs](https://docs.temporal.io/develop/task-queue-priority-fairness)
- **Fairness**: string fairness keys create virtual queues (e.g., per tenant); weight default 1.0 (2.0 dispatches twice as often); weights overridable per task queue for up to 1,000 keys; within priority tier dispatch is weighted-fair, FIFO within a key — [Priority & Fairness docs](https://docs.temporal.io/develop/task-queue-priority-fairness)
- Rate limits: queue-wide `queue-rps-limit` and per-key `fairness-key-rps-limit-default` scaled by weight; most restrictive wins — [Priority & Fairness docs](https://docs.temporal.io/develop/task-queue-priority-fairness)
- Fairness caveats: enforced only within a task-queue partition; weight applied at scheduling time; not guaranteed across worker versions; top 100 fairness keys survive server restarts — [Priority & Fairness docs](https://docs.temporal.io/develop/task-queue-priority-fairness)
- Availability: Cloud paid feature requiring namespace enablement; self-hosted `matching.enableFairness: true` — [Priority & Fairness docs](https://docs.temporal.io/develop/task-queue-priority-fairness); GA May 5, 2026 — [Temporal changelog](https://temporal.io/change-log)
- Resource-Based Auto-Tuning GA in all SDKs (Mar 2025) — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements); server v1.32 adds poller autoscaling backoff strategies — [temporal releases](https://github.com/temporalio/temporal/releases)
- Serverless Workers: "run Temporal Workers on serverless compute" with automatic invocation, scaling, and shutdown — Pre-release at Replay 2026 (AWS Lambda) — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements); Lambda Public Preview Aug 3, 2026; GCP Cloud Run Pre-release Aug 6, 2026 — [Temporal changelog](https://temporal.io/change-log)
- Worker Status UI (workers on a task queue with heartbeat data: CPU, task slots, config) — Public Preview (May 2026) — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- Cloud poller caps: 20,000 concurrent activity pollers and 20,000 workflow-task pollers per namespace — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Cadence: v1.4.1 adds "Domain Multi-Tenancy" — task-list-level isolation via hierarchical scheduling and rate limiting; v1.3.3 adds ephemeral task lists; v1.4.0 adds caller-type-based rate limiting and a Shard Distributor service — [Cadence releases](https://github.com/cadence-workflow/cadence/releases); "Adaptive Tasklist Scaler" (June 30, 2025) — [Cadence blog index](https://cadenceworkflow.io/blog); global rate limiters and host-level priority task processing — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update), [Uber multi-tenant task processing](https://eng.uber.com/blog/cadence-multi-tenant-task-processing/)

### Inferences
- Multi-tenant fairness (weighted per-tenant virtual queues) is now a GA, first-class feature in both Temporal (2026) and Cadence (2026) — a likely gap area for younger engines.

### Gaps
- Sticky execution (sticky task queue, schedule-to-start timeout default) details not re-fetched this session.

---

## Schedules

### Takeaway
Temporal Schedules support calendar/cron/interval specs, jitter, six overlap policies, a 1-year default catch-up window, pause-on-failure, backfill, pause with notes, action limits and manual trigger. Cadence shipped its own Schedules GA in v1.4.1 (June 2026).

### Cited Findings
- Specs: calendar (cron string or named fields year…second), interval with phase offset; jitter adds random 0..max offset to each action; time zone support (UTC recommended) — [Temporal Schedules](https://docs.temporal.io/schedule)
- Overlap policies: Skip (default), BufferOne, BufferAll, CancelOther, TerminateOther, AllowAll — [Temporal Schedules](https://docs.temporal.io/schedule)
- Catch-up window default one year; pause-on-failure (failure/timeout, not cancel/terminate); backfill of a past period; pause/resume with notes; remaining-action limits; last completion result and last failure available to next run; manual trigger — [Temporal Schedules](https://docs.temporal.io/schedule)
- Schedules are implemented as internal workflows (hidden from standard views on Elasticsearch) — [Temporal Schedules](https://docs.temporal.io/schedule)
- Cloud: 10 schedule requests/sec per namespace by default — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Cadence Schedules GA in v1.4.1 "with full API surface and overlap policy enforcement" — [Cadence releases](https://github.com/cadence-workflow/cadence/releases); "Introducing Cadence Schedules" post June 23, 2026 — [Cadence blog index](https://cadenceworkflow.io/blog); cron overlap policy integration in v1.3.3 — [Cadence releases](https://github.com/cadence-workflow/cadence/releases)

### Inferences
- Schedule parity in Cadence closed a long-standing gap vs Temporal in mid-2026.

### Gaps
- Temporal schedule count limits per namespace not found.

---

## Visibility (search attributes, memo, backends)

### Takeaway
Cloud allows 20 each of Bool/Datetime/Double/Int, 40 Keyword, 5 KeywordList, 5 Text custom search attributes; visibility API capped at 30 calls/sec. Self-hosted supports Elasticsearch (schema v14) and MySQL/PostgreSQL/SQLite visibility; server v1.32 unified the query converter (stricter type validation).

### Cited Findings
- Cloud custom search attribute caps: Bool 20, Datetime 20, Double 20, Int 20, Keyword 40, KeywordList 5, Text 5; names ≤ 64 chars; Visibility API 30 calls/sec (non-configurable) — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Server v1.32: unified visibility query converter default, enforces type validation, rejects empty-string Text searches; legacy converter removed in v1.33. Visibility schemas: Elasticsearch v14; MySQL/PostgreSQL v1.14 (payload metrics) — [temporal releases](https://github.com/temporalio/temporal/releases)
- Cadence: Apache Pinot visibility store (cost-efficient), alongside its other stores — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update); v1.3.6 supports multiple wildcard queries joined by OR; v1.4.0 adds cron schedule and execution status to visibility — [Cadence releases](https://github.com/cadence-workflow/cadence/releases)

### Gaps
- Memo size limit and Elasticsearch vs SQL "advanced visibility" feature differences were not re-fetched.

---

## Limits

### Takeaway
The canonical numbers: 2 MB per payload/blob (warn 256 KB), 4 MB gRPC message and 4 MB per history transaction, history hard limit 51,200 events or 50 MB (warn at 10,240 / 10 MB), 2,000 pending activities/signals/children/cancel requests (≤500 recommended), 10,000 signals, 10 in-flight/2,000 total Updates, 1,000-byte IDs. Cloud: 500 APS default (On-Demand), 1–90 day retention.

### Cited Findings
- Self-hosted defaults: blob warn 256 KB, error 2 MB; history size warn 10 MB / error 50 MB (`HistorySizeLimitWarn/Error`); history count warn 10,240 / error 51,200 (`HistoryCountLimitWarn/Error`); pending activities/signals/cancel requests/child executions 2,000 each (`limit.numPending*.error`), ≤500 recommended; IDs ≤1,000 chars (`limit.maxIDLength`); gRPC 4 MB; event batch 4 MB; Updates 10 in-flight / 2,000 total — [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- Cloud: 500 actions/sec default (On-Demand; varies with Provisioned Capacity); 1 new execution per second per ID with burst; 2 MB single-request payload; 4 MB gRPC/transaction; 51,200 events or 50 MB history; 2,000 callbacks; 30 in-flight Nexus ops; Nexus ScheduleToClose ≤60 days; 10-s Nexus handler timeout; 100-year timers; retention 1–90 days (default 30); batch jobs: 1 running at a time, 50 executions/sec; 10 namespaces default per account (auto-increase); 300 users; 25 custom roles — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Common community guidance: watch `historyLength` and use continue-as-new before the limit — [Keith Tenzer, Temporal Fundamentals](https://keithtenzer.com/temporal/Temporal_Fundamentals_Workflows/)

### Inferences
- External Storage (below) is Temporal's 2026 answer to the 2 MB payload ceiling; history-length limits still force continue-as-new.

### Gaps
- Cadence's exact default limits (blob size, history count) not fetched; Cadence blog "Bypass the 2 MB Limit Without Shrinking Your Workflow" (June 10, 2026) indicates Cadence also has a 2 MB limit — [Cadence blog index](https://cadenceworkflow.io/blog).

---

## Data (converters, codecs, encryption, large payloads, archival)

### Takeaway
External Storage (claim-check offload to S3/GCS/custom drivers, default 256 KiB threshold) moved Pre-release (Apr 8, 2026) → Public Preview (May 14, 2026); it runs after the payload codec so encrypted bytes are uploaded. Cadence added history payload encryption and large-payload guidance in June 2026.

### Cited Findings
- External Storage claim-check: large payloads offloaded to external store, reference tokens stored in Event History; default threshold 256 KiB; per-payload hard limit 2 MB (fixed on Cloud, configurable self-hosted); built-in S3 driver, GCS driver (Go, TypeScript), custom drivers; runs after the Payload Codec (encrypt then upload); UI shows reference tokens; requires object TTL > max run timeout + namespace retention; Public Preview — [External Storage docs](https://docs.temporal.io/external-storage)
- Changelog: External Storage Claim-Check Pre-release Apr 8, 2026 (Go, Python); External Storage Public Preview May 14, 2026 (Go, Python SDKs) — [Temporal changelog](https://temporal.io/change-log). Note: docs page lists Go, Java, Python, TypeScript support — [External Storage docs](https://docs.temporal.io/external-storage); Replay 2026 blog lists Python and Go only — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements). The SDK list appears to have widened after May 2026.
- Cadence (2026): "Your Workflow History Is Storing More Than You Think" (June 3), "Bypass the 2 MB Limit…" (June 10), "Encrypt Cadence History Payloads" (June 17) — [Cadence blog index](https://cadenceworkflow.io/blog)

### Gaps
- Codec Server details, archival (S3/GCS/filestore) status and Cloud "Export" (workflow history export) specifics not re-fetched; Export History is listed as available on Temporal Cloud on Google Cloud — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements).

---

## Operations (namespaces, replication/HA, UI, CLI, batch, Temporal Cloud features and pricing)

### Takeaway
Temporal Cloud offers multi-region (GA Mar 2025) and multi-cloud replication (GA, 20-min RTO, Replay 2026), same-region replication, 99.9% SLA (99.99% with HA), consumption pricing from $50 per million actions, API keys (GA), SCIM (GA 2026), Custom Roles and Projects (Pre-release 2026), OpenMetrics (GA Apr 2026), PrivateLink/PSC, and Azure (Pre-release Jun 2026).

### Cited Findings
**HA / replication**
- Multi-region Replication GA (Mar 2025), async, 99.99% SLA, automatic failover; Same-region Replication Public Preview (Mar 2025) — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- Multi-region replication with automatic failover (20-minute RTO) and Multi-cloud replication with automatic failover and failback — both GA (May 2026) — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements); "High Availability Features GA" Mar 31, 2026 — [Temporal changelog](https://temporal.io/change-log)
- Cadence: Active-Active domains (domain active in multiple clusters; each workflow active in exactly one) — [Cadence domains concept](https://www.mintlify.com/cadence-workflow/cadence/concepts/domains); added in v1.4.0, MySQL support in v1.4.1; replication cache cut DB calls 20% and replication latency 13 s → 2 s (v1.4.0) — [Cadence releases](https://github.com/cadence-workflow/cadence/releases); zonal isolation pins workflows to their starting zone — [Cadence zonal isolation](https://cadenceworkflow.io/blog/zonal-isolation-v1/zonal-isolation-v1)

**Cloud platform features (dates)**
- API Keys GA (Mar 2025; Service Accounts, mTLS migration); Temporal Cloud on Google Cloud GA (Mar 2025); SCIM Pre-release (Mar 2025, Enterprise/Mission Critical); Terraform provider Pre-release; automated zero-downtime migration tooling Pre-release — [Replay 2025 blog](https://temporal.io/blog/replay-2025-product-announcements)
- SCIM GA; Private Connectivity (AWS PrivateLink, GCP PSC) GA; Capacity Modes GA; OpenMetrics endpoint GA; Billing API Public Preview (forthcoming); Billable Action Metrics Public Preview — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- 2026 changelog: OpenMetrics GA Apr 2; Stable IPs GA May 29; Azure Pre-release (invite-only) Jun 1; Custom Roles Pre-release Jun 25; Projects Pre-release Aug 7; GCP Marketplace PAYG Aug 26; "Paygo with Developer Support" ($0/mo minimum) Sept 15; Cloud UI Strict Session Mode GA Sept 18 (15-min inactivity timeout, 12-h max session) — [Temporal changelog](https://temporal.io/change-log)
- Cloud access-control caps: 25 custom roles per account, 10 per principal, 20 permissions per role; 32 KB / 16 CA certificates per namespace — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)

**Pricing**
- Consumption: from $50 per million actions, volume discounts to $25 per million; active storage $0.042/GB-hr; retained storage $0.00105/GB-hr; billable action categories span workflows, activities, timers, signals, queries, schedules; SLA 99.9% (99.99% with HA); support tiers: PAYG developer support (10% of usage), Business ($500/mo minimum, 2-h P0 business hours), Enterprise/Mission Critical (30-min / 15-min P0 24/7); "No features locked behind plan upgrades" — [Temporal pricing](https://temporal.io/pricing) (summarized by fetch tool; verify exact tiers before quoting)

**Batch / UI / CLI**
- Cloud batch operations: 1 running batch job per namespace, 50 executions/sec — [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- Cadence Web v4.0.0 (React/Node rewrite) announced Apr 11, 2025; Workflow Diagnostics (Aug 6, 2025); Controlling Workflows From Web (Oct 12, 2025); Custom Workflow Controls turning queries into dashboards (Mar 23, 2026); Batch Actions UI for thousands of workflows (July 7, 2026) — [Cadence blog index](https://cadenceworkflow.io/blog); Instaclustr made Cadence Web 4.0 GA with Cadence 1.3.2 in Aug 2025 — [Instaclustr](https://www.instaclustr.com/blog/new-features-for-instaclustr-managed-cadence/.md)

**Self-hosted server**
- Latest Temporal server: v1.32.0 (Sept 11, 2026); patches v1.31.3 and v1.30.7 (Sept 18, 2026); v1.31 adds `passwordCommand` for AWS RDS / GCP Cloud SQL IAM auth; schemas MySQL/PostgreSQL v1.19, SQLite v1.11 — [temporal releases](https://github.com/temporalio/temporal/releases)

### Gaps
- SAML SSO, audit log streaming and Export specifics, and Web UI/CLI feature inventories were not re-fetched. The pricing page summary was fuzzy on plan names (Essentials/Business/Enterprise/Mission Critical) and exact tier breakpoints.

---

## SDK languages and maturity

### Takeaway
Eight official SDKs: Go, Java, Python, TypeScript, .NET, Ruby, PHP, Rust. Ruby went GA Oct 1, 2025; Rust went Public Preview May 7, 2026 and GA Sept 4, 2026. Community SDKs exist for Swift, Haskell, Clojure, Scala.

### Cited Findings
- Official SDKs: Go, Java, Python, TypeScript, .NET, Ruby, PHP, Rust; community (unsupported): Swift, Haskell (Mercury), Clojure (Manetu), Scala — [Temporal SDKs](https://docs.temporal.io/encyclopedia/temporal-sdks)
- Ruby: v0.1.0 alpha Mar 23, 2023; Pre-release at Replay 2025; Public Preview May 1, 2025; **GA Oct 1, 2025** — [Ruby SDK changelog](https://temporal.io/changelog/product-area/ruby-sdk), [Ruby GA](https://temporal.io/changelog/ruby-sdk-generally-available)
- Rust: Public Preview May 7, 2026; **GA Sept 4, 2026** — [Temporal changelog](https://temporal.io/change-log); [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- Standalone Activities GA spans Go, Python, Java, .NET, TypeScript, Ruby (PHP not listed) — [Temporal changelog](https://temporal.io/change-log)
- Cadence clients: Go and Java primary; a Python client is active (signal handling post Apr 28, 2026) — [Cadence blog index](https://cadenceworkflow.io/blog); "Client V2 modernization" on the 2024–25 roadmap — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update)

### Gaps
- Official SDK docs page does not list maturity per SDK or describe the shared Rust "Core" (sdk-core) underlying TS/Python/.NET/Ruby; not re-verified this session. PHP SDK feature parity status not found.

---

## Testing

### Takeaway
SDKs provide a time-skipping test environment, local dev-server environment, activity mocking by name, isolated activity test environments, and a Replayer for determinism regression tests; Cadence adds production shadowing.

### Cited Findings
- Python `WorkflowEnvironment.start_time_skipping()` fast-forwards timers except while activities run; `start_local()` runs a full local server; mock activities by registering same-name, same-signature implementations; `ActivityEnvironment` with `on_heartbeat()` tests activities without workers; `Replayer` replays histories from JSON or client fetch; time-skipping applies to the whole environment, so tests needing different time behavior run separately — [Python testing suite](https://docs.temporal.io/develop/python/testing-suite)
- Cadence shadowing for non-determinism regression — [Cadence blog](https://cadenceworkflow.io/blog/2023/08/27/nondeterministic-errors-replayers-shadowers)

### Gaps
- Per-SDK test framework differences (Go testsuite, Java TestWorkflowEnvironment) not re-fetched.

---

## AI / agent integrations

### Takeaway
Temporal has positioned itself as "durable AI" infrastructure: OpenAI Agents SDK integration (Public Preview Jul 30, 2025 → GA at Replay 2026, sandbox support Apr 2026), Vercel AI SDK plugin for TypeScript (Public Preview, Jan 20, 2026), Google ADK integration (GA, May 2026), LangGraph plugin (Python), MCP via tool patterns, and Workflow Streams for LLM token streaming (Python, Public Preview, May 2026).

### Cited Findings
- OpenAI Agents SDK + Temporal announced July 30, 2025, Public Preview, Python — [BusinessWire](https://www.businesswire.com/news/home/20250730783559/en/Temporal-and-OpenAI-Launch-Integration-for-Enterprises-Developing-Production-Agents), [OpenAI Agents docs](https://docs.temporal.io/develop/python/integrations/openai-agents); listed GA at Replay 2026 with sandbox support — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements); OpenAI Agents SDK Sandbox Integration Public Preview Apr 16, 2026 — [Temporal changelog](https://temporal.io/change-log)
- Vercel AI SDK plugin: AI SDK calls (`generateText`, `streamText`, `streamObject`) run in workflow code and are wrapped as Activities automatically; tools run in workflow context; Public Preview; changelog dated Jan 20, 2026 — [AI SDK integration docs](https://docs.temporal.io/develop/typescript/integrations/ai-sdk), [changelog](https://temporal.io/changelog/ai-sdk-vercel-integration), [blog](https://temporal.io/blog/building-durable-agents-with-temporal-and-ai-sdk-by-vercel)
- LangGraph plugin for Python SDK — [Temporal LangGraph docs tag](https://docs.temporal.io/tags/lang-graph); MCP durable agents example — [Code Exchange](https://temporal.io/code-exchange/mcp-temporal-durable-agents)
- Google ADK integration GA; Workflow Streams (Python, Public Preview) — durable streaming over Signal & Update primitives for real-time LLM responses; Temporal AI Partner Ecosystem launched; AWS AI Competency (Agentic AI) — [Replay 2026 blog](https://temporal.io/blog/replay-2026-product-announcements)
- Cadence positions itself for durable orchestration "in the era of AI" but no comparable first-party agent-framework integrations were found — [Instaclustr on CNCF donation](https://www.instaclustr.com/blog/cadence-workflow-uber-cncf-projects/.md)

### Inferences
- Streaming (Workflow Streams) and "wrap the LLM call as an activity automatically" plugins are now the reference pattern; an engine lacking a streaming primitive or framework plugins is behind on the AI axis.

### Gaps
- Exact GA date of the OpenAI Agents SDK integration (sometime between Jul 2025 and May 2026) not found; Pydantic AI / other framework integrations not verified.

---

## Cadence: shared core and 2024–2026 distinctive features

### Takeaway
Cadence (Uber, the Temporal ancestor) shares the event-sourced deterministic-replay model (domains ≈ namespaces, task lists ≈ task queues). It joined CNCF as a Sandbox project (accepted May 22, 2025) and is pursuing Incubation. Distinctive features: async (Kafka-queued) Start/Signal APIs, zonal isolation, active-active domains, Pinot visibility, shadowing, adaptive tasklist scaling, domain multi-tenancy, Batch Future with concurrency control, and a rewritten Web v4 with diagnostics and batch actions. Releases: v1.4.0 (Feb 27), v1.4.1 (Jun 30), v1.4.2 (Oct 2) — the last two are 2026; v1.4.0 year inferred as 2026.

### Cited Findings
- Scale at Uber: ~100K workflow updates/sec, 30 clusters, ~1,000 domains (tier 0–5) — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update)
- Shipped post-v1: zonal isolation, async Start/Signal APIs (control consumption rate), global rate limiters, Pinot visibility, Web v4 (Vue→React), non-determinism detection — [Cadence 2024 roadmap](https://cadenceworkflow.io/blog/2024/07/11/2024-07-11-yearly-roadmap-update/yearly-roadmap-update)
- CNCF Sandbox accepted May 22, 2025; Apache 2.0, community moved to CNCF Slack, roadmap on GitHub projects — [Instaclustr](https://www.instaclustr.com/blog/cadence-workflow-uber-cncf-projects/.md), [Uber blog](https://www.uber.com/blog/cadence-workflow-joins-the-cloud-native-computing-foundation/); "Help Cadence Reach CNCF Incubation" (Nov 12, 2025) — [Cadence blog index](https://cadenceworkflow.io/blog)
- Release highlights: v1.4.2 (Oct 2) cached queue reader (~90% fewer timer-task reads, ~13% fewer Cassandra requests), unified `cadence-server update-schema`; v1.4.1 (Jun 30) Schedules GA, active-active on MySQL, domain multi-tenancy; v1.4.0 (Feb 27) active-active domains, replication cache, Shard Distributor, caller-type rate limiting; v1.3.3 ephemeral task lists, cron overlap policy — [Cadence releases](https://github.com/cadence-workflow/cadence/releases). (GitHub displays v1.4.1/v1.4.2 dates without a year, i.e. current year 2026.)
- 2025–2026 blog features: Adaptive Tasklist Scaler (Jun 30, 2025), Safe Deployments of Versioned Workflows (Jul 1, 2025), Workflow Diagnostics (Aug 6, 2025), Batch Future with Concurrency Control (Sept 25, 2025), Custom Workflow Controls (Mar 23, 2026), history payload encryption (Jun 17, 2026), Schedules (Jun 23, 2026), Batch Actions UI (Jul 7, 2026) — [Cadence blog index](https://cadenceworkflow.io/blog)

### Inferences
- Cadence's differentiators are operator-scale features from Uber (zonal isolation, async ingest, multi-tenant task processing). It lacks Temporal's Updates, Nexus, Worker Deployments and AI integrations (none found in sources).

### Gaps
- "Cadence in 2025" (Jan 13, 2026) post could not be fetched (404 at guessed URL). Whether Cadence has an equivalent of Updates or Nexus was not confirmed.

---

## Known pain points / criticisms of Temporal

### Takeaway
Recurring complaints: steep learning curve and conceptual complexity, determinism constraints causing production non-determinism incidents, history-size limits forcing continue-as-new, heavy self-hosted infrastructure, and Cloud cost at scale. These have fueled lightweight Postgres/SQLite-based alternatives (DBOS, Absurd, etc.).

### Cited Findings
- HN users: onboarding "insanely difficult and complex"; teammates do not understand "we need such a complex system"; durable execution "too complex to run for a small company"; one production user called it "poorly designed, slow and ridiculously heavy infra wise," claiming 200+ events/workflow at a few hundred concurrent workflows means spending "millions" on infra — [HN: Durable execution should be lightweight](https://news.ycombinator.com/item?id=42877886), [HN: How to think about durable execution](https://news.ycombinator.com/item?id=46245238), [HN: Temporal Python](https://news.ycombinator.com/item?id=40287341) (quotes surfaced via search snippets across these threads; attribution to a specific thread not individually verified)
- Lightweight-alternative threads: "Absurd Workflows: Durable Execution with Just Postgres" — [HN](https://news.ycombinator.com/item?id=45797228); "Building a Durable Execution Engine with SQLite" — [HN](https://news.ycombinator.com/item?id=45992316); "Building durable workflows on Postgres" — [HN](https://news.ycombinator.com/item?id=48313530); DBOS vs Temporal — [HN](https://news.ycombinator.com/item?id=45458645)
- Determinism: `Math.random()`/`Date.now()` hazards; replay changes broke production ("One Line, One Outage") — [Keith Tenzer](https://keithtenzer.com/temporal/Temporal_Fundamentals_Workflows/), [Naman Gupta on Hashnode](https://hashnode.com/@naman-gupta)
- History limits push continue-as-new patterns — [Keith Tenzer](https://keithtenzer.com/temporal/Temporal_Fundamentals_Workflows/), [Temporal forum](https://community.temporal.io/t/workflow-flowload-history-size-issue/2001/2)

### Inferences
- Temporal's 2025–2026 roadmap directly targets these criticisms: Worker Versioning/Pinned (determinism-on-deploy), External Storage (payload limits), Serverless Workers and Worker Controller (ops burden), Standalone Activities (simple jobs without workflow overhead), PAYG with $0 support minimum (cost of entry).

### Gaps
- No quantitative survey of user pain points found; HN quotes are anecdotal and their exact thread attributions came from search snippets.
