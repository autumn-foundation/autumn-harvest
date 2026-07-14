# Comparing autumn-harvest to other durable-execution engines

This page positions autumn-harvest against the five durable-execution engines an
architect is most likely to shortlist alongside it — **Temporal, DBOS, Inngest,
Hatchet, and Restate** — across the eleven dimensions that usually decide the
choice. It is written for an engineer evaluating durable-execution engines who
wants to reach a defensible include-or-exclude decision on harvest in one
sitting, without cross-referencing five vendors' marketing pages.

It is deliberately not a sales page. Every harvest capability claimed as
_shipped_ links to the phase entry or GitHub issue that landed it, so any cell is
falsifiable against the repository. Planned work is labelled **planned** and
cites an open issue. And harvest's genuine gaps — no non-Rust SDK, single-region,
no managed cloud — get their own section named plainly, because a comparison that
hides its author's weaknesses is not worth reading.

> **Competitor facts accurate as of 2026-07-14.** Competitor rows are sourced
> from each vendor's public documentation (linked inline) and phrased neutrally.
> Claims that could not be confirmed against an official source are marked
> **(unverified)** rather than papered over.
>
> **Maintenance note:** this page is revisited on each harvest minor release, and
> the competitor facts are re-checked against each vendor's docs at that time. If
> you find a stale or wrong cell, open an issue — the whole point of the
> evidence-linked format is that it stays cheap to correct.

**Scope.** This compares _durable-execution / workflow-orchestration_ engines.
Plain job queues (Sidekiq, Celery, BullMQ) solve a different problem — at-least-once
task delivery without durable multi-step orchestration or deterministic replay —
and are out of scope beyond this sentence; [Oban](https://github.com/oban-bg/oban)
is the closest Postgres-native kin in that cohort. The DAG-scheduler cohort
(Airflow, Prefect, Dagster) overlaps harvest only through its `#[dag]` surface
([unified DAG execution, #256](https://github.com/madmax983/autumn-harvest/issues/256))
and is not compared here in full.

---

## At a glance

Engines as columns, the most decision-relevant dimensions as rows. Terse cells;
see the per-dimension tables below for the full facts, evidence links, and
sources.

| Dimension | autumn-harvest | Temporal | DBOS | Inngest | Hatchet | Restate |
|---|---|---|---|---|---|---|
| **Backing store** | Postgres only ([sharding](sharding.md)) | DB + Visibility store | Postgres only | Postgres + Redis | Postgres (+ opt. RabbitMQ) | Embedded (RocksDB) |
| **Self-host shape** | Embed in app / standalone runner | Server cluster + DB(s) | Library in app | Server binary + PG/Redis | Engine + Postgres | Single binary |
| **SDK languages** | Rust only ([planned](https://github.com/madmax983/autumn-harvest/issues/955): TS/Py) | 7 SDKs | Py/TS/Go/Java | TS/Py/Go | Py/TS/Go(/Ruby) | TS/Java/Kotlin/Py/Go/Rust |
| **Model** | Code-first + DAG ([#256](https://github.com/madmax983/autumn-harvest/issues/256)) | Imperative WF+Activity | Decorated WF+steps | Event/step functions | Queue/DAG/durable | Services/Objects/WF |
| **Determinism tooling** | Guardrails + replayer + [ND-block #603](https://github.com/madmax983/autumn-harvest/issues/603) | Replay + replay tests | Checkpoint/resume | Step memoization | Event-log replay | Journal replay |
| **Managed cloud** | None (by design) | Temporal Cloud | DBOS Cloud | Inngest Cloud | Hatchet Cloud | Restate Cloud |
| **License** | MIT OR Apache-2.0 | MIT | MIT | SSPL (server) | MIT | BSL 1.1 (server) |

---

## Dimension-by-dimension

Each table below covers one AC1 dimension across all six engines. In every
harvest cell, a **shipped** capability links to its phase entry or issue; a
**planned** capability says so and links an open issue. Competitor cells link the
sourcing doc and flag anything unverified.

### 1. Required infrastructure

| Engine | Requirement |
|---|---|
| **autumn-harvest** | **Postgres only.** No broker, no message queue, no separate server cluster — the task queue is `SELECT … FOR UPDATE SKIP LOCKED` and dispatch wakeups are Postgres LISTEN/NOTIFY (`notify.rs`). Runs as a companion to the [Autumn](https://github.com/madmax983/autumn) web framework or standalone via `HarvestRunner`. Optional [sharding](sharding.md) spreads state across N independent Postgres databases. |
| Temporal | A persistence DB (Cassandra / MySQL / PostgreSQL) **plus** a separate Visibility store (Elasticsearch recommended for production; Cassandra can't back Visibility). ([docs](https://docs.temporal.io/temporal-service/persistence), [Visibility](https://docs.temporal.io/self-hosted-guide/visibility)) |
| DBOS | PostgreSQL only; durable queues and step checkpoints live in Postgres, no separate broker. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Server needs external **Postgres + Redis** for production (SQLite + in-memory Redis in dev mode). ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | **PostgreSQL** as the durability layer; a single Postgres DB suffices, with **RabbitMQ** optional for high-throughput inter-service messaging. ([docs](https://docs.hatchet.run/home/architecture)) |
| Restate | **Single self-contained binary** with embedded RocksDB-based storage — no external database required. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 2. Self-host complexity

| Engine | Components to deploy / upgrade |
|---|---|
| **autumn-harvest** | Embed `HarvestPlugin` in your Autumn app (or run standalone `HarvestRunner`) + point at a Postgres URL + run `diesel migration run`. No separate orchestrator cluster. Separate web/worker connection pools with a shared ceiling (`pool.rs`) keep worker bursts from starving HTTP handling; optional [sharding](sharding.md) is additive. |
| Temporal | Separate server cluster of multiple services (Frontend, History, Matching, Worker) plus external DB(s); not a single production binary. Helm charts / docker-compose provided; your Workers are separate processes. ([docs](https://docs.temporal.io/self-hosted-guide/deployment)) |
| DBOS | Lightweight — a **library embedded in your app process** + Postgres, "no additional infrastructure required." Optional Conductor control plane for recovery/visualization/HA. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Single `inngest` server binary/container + external Postgres/Redis for production (Helm chart available); your functions are HTTP handlers the engine calls. Self-hosting is comparatively new (GA'd ~2025). ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | A Hatchet engine/API server + Postgres (+ optional RabbitMQ); workers connect over gRPC. Documented as "particularly easy to self-host." ([docs](https://docs.hatchet.run/home/architecture)) |
| Restate | Single binary via Homebrew/npm/Docker; "no extra databases, no separate worker processes." Your handlers are your own processes the server invokes. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 3. Language / SDK support

| Engine | SDKs |
|---|---|
| **autumn-harvest** | **Rust only.** Non-Rust systems participate as _callers_ via the HTTP [management API](management-api.md), the [workflow-result endpoint (#527)](https://github.com/madmax983/autumn-harvest/issues/527), published input/output [JSON Schema (#373)](https://github.com/madmax983/autumn-harvest/issues/373), and [MCP tools (#597)](https://github.com/madmax983/autumn-harvest/issues/597) ([docs](mcp-tools.md)) — but there is no non-Rust worker/author SDK. **Planned:** a TypeScript activity-worker SDK ([#959](https://github.com/madmax983/autumn-harvest/issues/959)) and TypeScript + Python management-API client SDKs ([#955](https://github.com/madmax983/autumn-harvest/issues/955)). |
| Temporal | 7 official SDKs: Go, Java, PHP, Python, TypeScript, .NET, Ruby. ([docs](https://docs.temporal.io/encyclopedia/temporal-sdks)) |
| DBOS | Python, TypeScript, Go, Java. ([docs](https://docs.dbos.dev/)) |
| Inngest | TypeScript, Python, Go (official); Elixir and Rust listed as in development. ([blog](https://www.inngest.com/blog/cross-language-support-with-new-sdks)) |
| Hatchet | Python, TypeScript, Go (official); Ruby also listed. ([repo](https://github.com/hatchet-dev/hatchet)) |
| Restate | TypeScript, Java, Kotlin, Python, Go, Rust. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 4. Programming model

| Engine | Model |
|---|---|
| **autumn-harvest** | Code-first async Rust with event-sourced deterministic replay. Also a first-class DAG surface (`#[dag]`, [unified DAG execution #256](https://github.com/madmax983/autumn-harvest/issues/256)); [signals/queries/updates](https://github.com/madmax983/autumn-harvest/issues/234) ([#140](https://github.com/madmax983/autumn-harvest/issues/140), [#346](https://github.com/madmax983/autumn-harvest/issues/346)); [Saga compensation](saga.md) ([#238](https://github.com/madmax983/autumn-harvest/issues/238)); inbound [webhook triggers (#344)](https://github.com/madmax983/autumn-harvest/issues/344). |
| Temporal | Code-first imperative durable Workflows + Activities (side effects isolated in Activities). ([docs](https://docs.temporal.io/)) |
| DBOS | Code-first — ordinary functions annotated as durable **workflows** and **steps** via decorators/annotations. ([docs](https://docs.dbos.dev/)) |
| Inngest | Event-driven step functions — functions triggered by events / cron / webhooks, logic split into memoized `step` calls. ([docs](https://www.inngest.com/docs/learn/how-functions-are-executed)) |
| Hatchet | Multi-paradigm — general-purpose task queue, DAG orchestrator, and durable-execution engine (DAGs can be built at runtime). ([docs](https://docs.hatchet.run/v1)) |
| Restate | Durable handlers across Services, **Virtual Objects** (per-key consistent state), and Workflows, with durable RPC/queuing built in. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 5. Determinism guarantees & tooling

| Engine | Approach |
|---|---|
| **autumn-harvest** | Event-sourced deterministic replay, layered with the deepest safety tooling in this set: **compile-time guardrails HVG001–HVG011** plus a `det_check` static analyzer (DET010/DET011) that flag non-deterministic patterns before they ship; the [`WorkflowReplayer` harness (Phase 3.5)](replay-verify.md) that replays current code against recorded histories in CI; **non-terminal [ND-blocking (#603)](https://github.com/madmax983/autumn-harvest/issues/603)** that _parks and alerts_ a divergent run rather than silently wedging or failing it; and [deterministic side-effect primitives (#384)](https://github.com/madmax983/autumn-harvest/issues/384) for time/UUID/random. See the [workflow determinism guide](workflow-determinism-guide.md). |
| Temporal | Deterministic replay is core (Event History replayed against code); ships **Replay testing** to detect non-determinism before deploy. ([docs](https://docs.temporal.io/develop/safe-deployments)) |
| DBOS | Checkpoint/resume from the last completed step (not command-comparison replay); docs state workflow functions must be deterministic and keep I/O in steps. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Step-based memoization; docs state **no determinism requirement** on the orchestration layer (each step runs once, result persisted, completed steps skipped on retry). ([docs](https://www.inngest.com/docs/learn/how-functions-are-executed)) |
| Hatchet | Durable event-log replay to the last checkpoint; the durable task should be deterministic to allow replay. A dedicated replay-safety test harness is **(unverified)** from official docs. ([docs](https://docs.hatchet.run/v1/durable-execution)) |
| Restate | Per-invocation journal + replay; non-deterministic/side-effecting operations wrapped in `ctx.run`. Dedicated replay-safety test tooling is **(unverified)**. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 6. Versioning / safe-deploy

| Engine | Mechanism |
|---|---|
| **autumn-harvest** | [`ctx.version()`](workflow-determinism-guide.md) plus the two-state [`ctx.patched` / `ctx.deprecate_patch` (#687)](https://github.com/madmax983/autumn-harvest/issues/687); [worker build-ID routing (#171)](https://github.com/madmax983/autumn-harvest/issues/171) with [percentage build ramp (#604)](https://github.com/madmax983/autumn-harvest/issues/604); and a read-only [workflow-type reachability check (#520)](https://github.com/madmax983/autumn-harvest/issues/520) to gate safe handler removal. Operator playbook: [safe-deploy runbook](runbooks/safe-deploy.md). |
| Temporal | Two methods — **Worker Versioning** (Build-ID pinning + progressive rollout; GA, the recommended default) and **Patching** (`GetVersion()` / `patched` markers). ([docs](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)) |
| DBOS | Workflows versioned by code version; recovery routes a workflow to a compatible live executor (Conductor coordinates). Patching-primitive parity with Temporal's `GetVersion` is **(unverified)**. ([docs](https://docs.dbos.dev/)) |
| Inngest | Set `appVersion` (e.g. commit SHA / image tag) so rolling deploys are managed. Automatic in-flight pinning depth is **(unverified)** vs Temporal Worker Versioning. ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | Tasks/workflows defined as code ("easy to version, deploy") + **Sticky Assignment** for worker affinity. A first-class in-flight version-pinning/patching primitive is **(unverified)**. ([docs](https://docs.hatchet.run/v1)) |
| Restate | Deploy updated code to a new endpoint/deployment; new invocations route to the latest, in-flight invocations continue on the deployment that started them. ([docs](https://docs.restate.dev/services/versioning)) |

### 7. Scheduling depth

| Engine | Capabilities |
|---|---|
| **autumn-harvest** | Cron + interval, with [jitter (#240)](https://github.com/madmax983/autumn-harvest/issues/240), [overlap policy (#241)](https://github.com/madmax983/autumn-harvest/issues/241), [calendars + backfill (#337)](https://github.com/madmax983/autumn-harvest/issues/337), [bounded catchup window (#484)](https://github.com/madmax983/autumn-harvest/issues/484), bounded/finite runs ([#478](https://github.com/madmax983/autumn-harvest/issues/478) / [#543](https://github.com/madmax983/autumn-harvest/issues/543)), [HA-safe multi-replica ticks (#350)](https://github.com/madmax983/autumn-harvest/issues/350), [schedule run history (#534)](https://github.com/madmax983/autumn-harvest/issues/534), [in-place schedule update (#771)](https://github.com/madmax983/autumn-harvest/issues/771), [last-completion carryover (#488)](https://github.com/madmax983/autumn-harvest/issues/488), plus start-shaping via [debounce (#499)](https://github.com/madmax983/autumn-harvest/issues/499) and [throttle (#607)](https://github.com/madmax983/autumn-harvest/issues/607). |
| Temporal | Schedules with interval and calendar/cron spec; Catchup Window (default 1 year, min 10s); overlap policies (Skip, BufferOne, BufferAll, AllowAll, CancelOther, TerminateOther); Backfill. ([docs](https://docs.temporal.io/schedule)) |
| DBOS | Cron-scheduled workflows (stored in DB, runtime create/pause/resume/delete), time zones, and automatic backfill of missed runs after downtime. Calendar-holiday / overlap-policy depth **(unverified)**. ([docs](https://docs.dbos.dev/python/tutorials/scheduled-workflows)) |
| Inngest | Cron/scheduled functions with per-step retries. Calendar, catchup/backfill, and overlap-policy depth **(unverified)** from official docs in this pass. ([docs](https://www.inngest.com/uses/scheduled-jobs)) |
| Hatchet | Cron scheduling for scheduled + DAG workflows. Calendar, catchup/backfill, and overlap-policy depth **(unverified)**. ([docs](https://docs.hatchet.run/v1)) |
| Restate | Delayed/scheduled invocations + durable timers (a scheduled invocation runs on the latest deployment at trigger time). A Temporal-style Schedule object with cron/calendar/backfill/overlap depth is **(unverified)** — the model centers on delayed calls + timers. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 8. Observability surface

| Engine | Surface |
|---|---|
| **autumn-harvest** | An [OpenTelemetry trace contract (ADR-0001)](adr/0001-otel-trace-contract.md) ([#136](https://github.com/madmax983/autumn-harvest/issues/136)) covering 8 named span kinds; a bounded-cardinality metric catalogue with a `metrics-rs` adapter; a [starter alert pack + runbooks](alerts/); a [Grafana dashboard pack (#754)](https://github.com/madmax983/autumn-harvest/issues/754); a per-execution [timeline API (#739)](https://github.com/madmax983/autumn-harvest/issues/739); a [DAG run graph view (#690)](https://github.com/madmax983/autumn-harvest/issues/690); and a rolled-up [health summary endpoint (#679)](https://github.com/madmax983/autumn-harvest/issues/679). The embedded **Vantage UI is partial** — the [Workers tab (#142)](https://github.com/madmax983/autumn-harvest/issues/142), a DLQ inspection page with a summary view ([#226](https://github.com/madmax983/autumn-harvest/issues/226) / [#385](https://github.com/madmax983/autumn-harvest/issues/385)), a [schedules management page (#333)](https://github.com/madmax983/autumn-harvest/issues/333), and [DAG list + detail pages (#426)](https://github.com/madmax983/autumn-harvest/issues/426) have shipped; a rendered DAG **graph** visualization is still Phase 4 (see [Where harvest is behind](#where-harvest-is-behind)). See [telemetry](telemetry.md). |
| Temporal | Open-source Web UI; SDK metrics (Prometheus); tracing/OTel via SDK interceptors. Temporal Cloud adds a Prometheus-compatible OpenMetrics endpoint. ([docs](https://docs.temporal.io/references/sdk-metrics)) |
| DBOS | OpenTelemetry traces per workflow/step; Prometheus-compatible metrics endpoint; Conductor dashboards of active/past workflows + queued tasks. ([docs](https://www.dbos.dev/dbos-conductor)) |
| Inngest | Built-in Dashboard UI with step-level observability (queue delay, step timing, flow control) + event history. Explicit OTel export is **(unverified)** in this pass. ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | Built-in dashboard; a built-in **OpenTelemetry** collector emitting traces/spans per task/workflow; Prometheus metrics (noted as Dedicated-tier+ on Hatchet Cloud). ([OTel](https://docs.hatchet.run/home/opentelemetry), [Prometheus](https://docs.hatchet.run/v1/prometheus-metrics)) |
| Restate | Built-in Restate UI with a live execution-step timeline (retries, nested RPC, awakeables, cancellation) and a distributed call-stack view; OpenTelemetry tracing supported. ([blog](https://www.restate.dev/blog/announcing-restate-ui)) |

### 9. HA / multi-region

| Engine | Story |
|---|---|
| **autumn-harvest** | HA-safe scheduler ticks under multi-replica deployments ([#350](https://github.com/madmax983/autumn-harvest/issues/350)); horizontal scale via Postgres [sharding](sharding.md). **Single-region**: each shard is one Postgres, and cross-shard workflows are explicitly out of scope per the sharding contract. There is no built-in multi-region replication or failover today. **Planned:** cross-region DR via logical replication with fenced failover ([#954](https://github.com/madmax983/autumn-harvest/issues/954)) and explicit shard pinning for data-residency placement ([#697](https://github.com/madmax983/autumn-harvest/issues/697)). |
| Temporal | Self-host multi-cluster replication (Global Namespaces). Temporal Cloud offers 2-region replication (active/passive, automatic failover, 99.99% target; same continent + same cloud, async replication). ([docs](https://docs.temporal.io/cloud/high-availability)) |
| DBOS | HA via Conductor — on worker crash/failure, workflows recover to a compatible live worker. Explicit self-host multi-region topology **(unverified)**; DBOS Cloud handles hosting. ([docs](https://www.dbos.dev/dbos-conductor)) |
| Inngest | Managed cloud is HA across multiple regions. Self-host HA requires you to run HA Postgres/Redis/queue backends; multi-region self-host is your responsibility. ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | Self-host HA documented (HA Postgres, 3-replica RabbitMQ, multiple engine/API instances behind a load balancer); Hatchet Cloud + Managed Compute advertise multi-region. ([docs](https://docs.hatchet.run/self-hosting/high-availability)) |
| Restate | Restate 1.x added distributed/clustered operation (Cloud features now also in OSS). Precise self-host multi-region topology + guarantees **(unverified)**; Restate Cloud is the managed HA path. ([blog](https://www.restate.dev/blog/announcing-restate-1-5)) |

### 10. Managed-cloud availability

| Engine | Offering |
|---|---|
| **autumn-harvest** | **None.** harvest is embed-in-your-app + self-host-Postgres **by design** — there is no first-party managed control plane or hosted SaaS, and none is currently planned (it is a deliberate positioning choice, not a backlog gap). |
| Temporal | **Temporal Cloud** (first-party managed). ([docs](https://docs.temporal.io/cloud/high-availability)) |
| DBOS | **DBOS Cloud** (serverless deploy + Conductor + time-travel debugging). ([docs](https://www.dbos.dev/dbos-conductor)) |
| Inngest | **Inngest Cloud** (the primary/first-party offering). ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | **Hatchet Cloud** (+ Managed Compute). ([docs](https://docs.hatchet.run/v1/cloud-vs-oss)) |
| Restate | **Restate Cloud** (publicly available). ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 11. License

| Engine | License |
|---|---|
| **autumn-harvest** | **MIT OR Apache-2.0** (dual, permissive) — the entire engine, plugin, macros, and CLI. No source-available/BSL/SSPL tier and no open-core split. |
| Temporal | Server, UI server, and SDKs all **MIT**; fully free to self-host. Temporal Cloud is the paid managed tier. ([server LICENSE](https://github.com/temporalio/temporal/blob/main/LICENSE)) |
| DBOS | **MIT** across all four SDK repos; the durable-execution engine is the OSS library. Conductor/Cloud are the paid managed control-plane layers. ([repo](https://github.com/dbos-inc/dbos-transact-py)) |
| Inngest | **Split:** server + CLI are **SSPL** (with delayed open-source publication to Apache-2.0); all SDKs are **Apache-2.0**. ([repo](https://github.com/inngest/inngest)) |
| Hatchet | **MIT** ("100% MIT licensed") for the OSS core; Hatchet Cloud is the paid managed tier (some features gated to paid). ([repo](https://github.com/hatchet-dev/hatchet)) |
| Restate | **Split:** server/runtime is **Business Source License 1.1** (source-available, not OSI-approved; converts to an open-source Change License after a set term — the exact change license/date is **(unverified)** against the LICENSE file in this pass); **SDKs are MIT**. ([repo](https://github.com/restatedev/restate)) |

---

## What sets harvest apart

Three narratives where harvest's design is genuinely distinctive, each claim
linked to shipped evidence.

### 1. Postgres-only operations — one dependency you already run

Harvest's task queue, durable timers, signals, dead-letter queue, schedules, and
event history all live in **one Postgres database**. Dispatch latency comes primarily from
LISTEN/NOTIFY wakeups (`notify.rs`), with a poll-loop fallback in `worker.rs`, and
work is claimed with `SELECT … FOR UPDATE SKIP LOCKED` — so there is no Redis, no Kafka/RabbitMQ,
no Elasticsearch, and no separate orchestrator cluster to stand up, secure,
back up, and upgrade. When you outgrow one database, [sharding](sharding.md)
spreads state across N independent Postgres instances, with each `ExecutionId`
carrying its `ShardId` so any caller routes to the owning shard in O(1) — no
directory service. Separate web/worker connection pools with a shared ceiling
(`pool.rs`) keep a worker burst from starving HTTP request handling in the same
process. For a team that already operates Postgres, the marginal operational
footprint of adding durable execution is close to zero. DBOS shares this
Postgres-only philosophy (and is the closest positioning to harvest here); the
difference is language and embedding, covered below.

### 2. Determinism safety in depth — caught before, and after, deploy

Deterministic replay is only as safe as your ability to _know_ your code is still
deterministic. Harvest layers four independent defenses instead of relying on
replay tests alone:

1. **Compile-time guardrails HVG001–HVG011** plus a `det_check` static analyzer
   (DET010/DET011) flag non-deterministic patterns — wall-clock reads, unseeded
   randomness, `HashMap` iteration order, `select!` races — at build time, before
   a bad workflow ever runs. See the
   [workflow determinism guide](workflow-determinism-guide.md).
2. **[Deterministic side-effect primitives (#384)](https://github.com/madmax983/autumn-harvest/issues/384)**
   give authors safe replacements (`ctx.system_now`, `ctx.new_uuid`,
   `ctx.random_*`) that record their value once and replay it verbatim.
3. **The [`WorkflowReplayer` harness (Phase 3.5)](replay-verify.md)** replays a
   code change against recorded production histories in CI, so a non-determinism
   regression is a failed test, not a 2 a.m. page.
4. **Non-terminal [ND-blocking (#603)](https://github.com/madmax983/autumn-harvest/issues/603)**
   is the runtime backstop: if a divergence does reach production, the affected
   execution is _parked and made alertable with a bounded backoff_ — not silently
   wedged, and not terminally failed — so a rolled-back deploy lets it resume
   exactly where it was. Most engines in this set treat replay determinism as an
   author responsibility validated by replay tests; harvest additionally makes a
   divergent run a recoverable, observable state.

### 3. Embedded in your web app — no separate orchestrator cluster

Harvest is designed to run **inside your application process**, wired in through
one `HarvestPlugin` on an [Autumn](https://github.com/madmax983/autumn) app (or a
standalone `HarvestRunner` when you're not on Autumn). There is no orchestrator
cluster to deploy alongside your service and no network hop between your request
handlers and your workflows — a workflow can be started from an HTTP route and
awaited in-process. That embedding also unlocks integrations that a separate
cluster makes awkward: [MCP tool exposure (#597)](https://github.com/madmax983/autumn-harvest/issues/597)
turns a `#[workflow(mcp)]` into a correlated agent tool set ([docs](mcp-tools.md)),
and an inbound [webhook receiver (#344)](https://github.com/madmax983/autumn-harvest/issues/344)
lets a verified provider delivery trigger a workflow directly. This is the same
lightweight-embedding shape as DBOS, in Rust, with a code-first replay model and
a first-class DAG surface.

---

## Where harvest is behind

Written plainly, before a competitor writes it. Each gap links its tracking issue
where one exists.

- **Rust-only — no polyglot SDK.** If your workflows or activity workers aren't in
  Rust, harvest can't author them today. Non-Rust systems can only be _callers_
  (HTTP API, MCP, JSON Schema). This is the single biggest adoption gate versus
  Temporal (7 SDKs) and Restate (6). Planned:
  [#959](https://github.com/madmax983/autumn-harvest/issues/959) (TypeScript
  activity-worker SDK), [#955](https://github.com/madmax983/autumn-harvest/issues/955)
  (TypeScript + Python management-API clients).
- **Single-region — no multi-region DR or replication.** Sharding scales harvest
  horizontally within a region, but there is no built-in cross-region replication
  or failover, and cross-shard workflows are out of scope by design. Temporal
  (Global Namespaces / Cloud 2-region) and the managed clouds are ahead here.
  Planned R&D: [#954](https://github.com/madmax983/autumn-harvest/issues/954)
  (cross-region DR with fenced failover),
  [#697](https://github.com/madmax983/autumn-harvest/issues/697) (data-residency
  shard pinning).
- **No managed cloud.** There is no hosted harvest — every competitor in this set
  offers a first-party managed tier. This is a deliberate positioning choice
  (embed + self-host Postgres), but if you want someone else to run the control
  plane, harvest is not that.
- **Postgres-only, no pluggable persistence.** The single-dependency story is a
  strength operationally, but it is also a ceiling: harvest cannot swap in
  Cassandra or another store the way Temporal can for very large-scale
  persistence. If your durability volume genuinely exceeds what a
  (sharded) Postgres fleet can carry, that is a real constraint.
- **UI parity is incomplete.** The embedded Vantage dashboard has a
  [Workers tab (#142)](https://github.com/madmax983/autumn-harvest/issues/142),
  a DLQ inspection page with a summary view
  ([#226](https://github.com/madmax983/autumn-harvest/issues/226) /
  [#385](https://github.com/madmax983/autumn-harvest/issues/385)), a
  [schedules management page (#333)](https://github.com/madmax983/autumn-harvest/issues/333),
  and [DAG list + detail pages (#426)](https://github.com/madmax983/autumn-harvest/issues/426),
  but a rendered DAG **graph** visualization (a node/edge diagram) is still Phase 4
  work in progress. Temporal, Inngest, Hatchet, DBOS (Conductor), and Restate all
  ship more mature UIs today.
- **Younger project, smaller ecosystem.** harvest is pre-1.0 (0.x, breaking
  changes in minor versions) with a smaller community, fewer third-party
  integrations, and a much smaller hiring pool than Temporal in particular. Some
  competitor cells above are marked **(unverified)** precisely because this is a
  young market moving fast.
- **No cross-shard workflows.** A single workflow's state (events, tasks, timers,
  signals, DLQ) is pinned to one shard; there is no cross-shard transaction or
  cross-shard workflow composition — an explicit scope boundary in the
  [sharding contract](sharding.md), not a bug, but a limit to know before you
  design around shards.
- **No first-party benchmarks yet.** harvest publishes no reproducible
  performance numbers today, so throughput/latency claims here are deliberately
  absent. Planned: a reproducible end-to-end benchmark suite
  ([#941](https://github.com/madmax983/autumn-harvest/issues/941)).

---

## Choose something else if…

The honest inverse of the sections above — one paragraph per competitor naming
exactly when it is the better call.

**Choose Temporal if** you need polyglot SDKs _today_ (Go, Java, Python,
TypeScript, .NET, PHP, Ruby), a mature managed cloud with multi-region high
availability, pluggable persistence (Cassandra/MySQL/PostgreSQL) for very
large-scale deployments, or the largest hiring pool and ecosystem in the durable-
execution market. Temporal is the safe institutional default; harvest trades that
breadth for a single-Postgres footprint and a Rust-native, embedded model.

**Choose DBOS if** you want harvest's lightweight Postgres-only, embed-in-your-app
shape but need it in **TypeScript, Python, Go, or Java** rather than Rust, or you
want a managed control plane (DBOS Cloud + Conductor) with a time-travel debugger.
DBOS is the closest positioning to harvest; language and the checkpoint/resume
recovery model (vs harvest's command-comparison replay + guardrail tooling) are
the main axes to weigh.

**Choose Inngest if** your problem is event-first — functions triggered by events,
webhooks, and crons, split into memoized steps — and you want TypeScript / Python /
Go serverless HTTP functions with a polished managed cloud as the primary product.
Inngest deliberately requires no orchestration-layer determinism, which is simpler
to author against but gives up the replay-safety guarantees harvest builds its
tooling around. (Note Inngest's server is SSPL; its SDKs are Apache-2.0.)

**Choose Hatchet if** you want a Postgres-backed engine that is equally a
general-purpose **task queue**, a DAG orchestrator, and a durable-execution engine,
with TypeScript / Python / Go SDKs, a built-in OpenTelemetry collector, and a
managed cloud (+ Managed Compute) — and you're comfortable adding RabbitMQ for
high-throughput paths. Hatchet occupies adjacent territory to harvest (Postgres,
DAG-capable); the split is language and harvest's deeper determinism-tooling story.

**Choose Restate if** you want the lightest possible deployment — a **single
self-contained binary with no external database** — polyglot SDKs (TypeScript,
Java, Kotlin, Python, Go, Rust), and its distinctive Virtual Objects / durable-RPC
model for per-key consistent state and lock-free coordination. Restate's embedded
storage removes even the Postgres dependency; weigh that against its server being
BSL 1.1 (source-available) rather than a permissive OSS license, and its
scheduling model centering on delayed calls + durable timers rather than a
Temporal-style Schedule object.

---

## Related

- **[README](../README.md)** — project overview and quick start.
- **[Getting started](getting-started/README.md)** — the chapter-by-chapter guide.
- **Temporal migration guide** — _planned_
  ([#947](https://github.com/madmax983/autumn-harvest/issues/947)): a
  move-from-Temporal / dual-run cutover playbook. That page does not exist yet;
  when it ships it will link back here, making the cross-reference bidirectional.
  This page answers _why / whether_ harvest; the migration guide will answer
  _how_ to move.
