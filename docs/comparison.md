# Comparing autumn-harvest to other durable-execution engines

This page positions autumn-harvest against the five durable-execution engines an
architect is most likely to shortlist alongside it — **Temporal, DBOS, Inngest,
Hatchet, and Restate** — across the eleven dimensions that usually decide the
choice. A twelfth dimension, [durable agents](#12-durable-agents), compares
the systems that ship a named, long-lived agent. It is written for an engineer
evaluating durable-execution engines who wants to reach a defensible
include-or-exclude decision on harvest in one sitting, without
cross-referencing five vendors' marketing pages.

It is deliberately not a sales page. Every harvest capability claimed as
_shipped_ links to the phase entry or GitHub issue that landed it, so any cell is
falsifiable against the repository. Planned work is labelled **planned** and
cites an open issue. Harvest's genuine gaps get their own section: no non-Rust
SDK, no automatic regional failover, no managed cloud. A comparison that hides
its author's weaknesses is not worth reading.

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
([unified DAG execution, #256](https://github.com/autumn-foundation/autumn-harvest/issues/256))
and is not compared here in full.

---

## At a glance

Engines as columns, the most decision-relevant dimensions as rows. Terse cells;
see the per-dimension tables below for the full facts, evidence links, and
sources.

| Dimension | autumn-harvest | Temporal | DBOS | Inngest | Hatchet | Restate |
|---|---|---|---|---|---|---|
| **Backing store** | Postgres ([sharding](sharding.md)); embedded single-writer [SQLite](sqlite-backend.md) for edge use | DB + Visibility store | Postgres only | Postgres + Redis | Postgres (+ opt. RabbitMQ) | Embedded (RocksDB) |
| **Self-host shape** | Embed in app / standalone runner | Server cluster + DB(s) | Library in app | Server binary + PG/Redis | Engine + Postgres | Single binary |
| **SDK languages** | Rust only (+ TypeScript management-API client) | 7 SDKs | Py/TS/Go/Java | TS/Py/Go | Py/TS/Go(/Ruby) | TS/Java/Kotlin/Py/Go/Rust |
| **Model** | Code-first + DAG ([#256](https://github.com/autumn-foundation/autumn-harvest/issues/256)) | Imperative WF+Activity | Decorated WF+steps | Event/step functions | Queue/DAG/durable | Services/Objects/WF |
| **Determinism tooling** | Guardrails + replayer + [ND-block #603](https://github.com/autumn-foundation/autumn-harvest/issues/603) | Replay + replay tests | Checkpoint/resume | Step memoization | Event-log replay | Journal replay |
| **Managed cloud** | None (by design) | Temporal Cloud | DBOS Cloud | Inngest Cloud | Hatchet Cloud | Restate Cloud |
| **License** | MIT OR Apache-2.0 | MIT | MIT | SSPL (server) | MIT | BSL 1.1 (server) |

---

## Dimension-by-dimension

Each table below covers one AC1 dimension across all six engines. In every
harvest cell, a **shipped** capability links to its phase entry or issue; a
**planned** capability says so and links an open issue. Competitor cells link the
sourcing doc and flag anything unverified. Section 12 compares another set of
peers: the systems that ship a durable agent.

### 1. Required infrastructure

| Engine | Requirement |
|---|---|
| **autumn-harvest** | **Postgres only.** No broker, no message queue, no separate server cluster — the task queue is `SELECT … FOR UPDATE SKIP LOCKED` and dispatch wakeups are Postgres LISTEN/NOTIFY (`notify.rs`). Runs as a companion to the [Autumn](https://github.com/autumn-foundation/autumn) web framework or standalone via `HarvestRunner`. Optional [sharding](sharding.md) spreads state across N independent Postgres databases. An optional Redis Streams [dispatch channel](operations/redis-dispatch.md) carries task references only; Postgres stays the source of truth. A separate crate, [`autumn-harvest-sqlite`](sqlite-backend.md), runs the same replay engine on one SQLite file for single-writer edge use. |
| Temporal | A persistence DB (Cassandra / MySQL / PostgreSQL) **plus** a separate Visibility store (Elasticsearch recommended for production; Cassandra can't back Visibility). ([docs](https://docs.temporal.io/temporal-service/persistence), [Visibility](https://docs.temporal.io/self-hosted-guide/visibility)) |
| DBOS | PostgreSQL only; durable queues and step checkpoints live in Postgres, no separate broker. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Server needs external **Postgres + Redis** for production (SQLite + in-memory Redis in dev mode). ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | **PostgreSQL** as the durability layer; a single Postgres DB suffices, with **RabbitMQ** optional for high-throughput inter-service messaging. ([docs](https://docs.hatchet.run/home/architecture)) |
| Restate | **Single self-contained binary** with embedded RocksDB-based storage — no external database required. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 2. Self-host complexity

| Engine | Components to deploy / upgrade |
|---|---|
| **autumn-harvest** | Embed `HarvestPlugin` in your Autumn app (or run standalone `HarvestRunner`) + point at a Postgres URL. Migrations are registered with Autumn and applied by the framework (`autumn migrate`, or automatically under the `dev` profile); a standalone `HarvestRunner` applies them itself. No separate orchestrator cluster. Separate web/worker connection pools with a shared ceiling (`pool.rs`) keep worker bursts from starving HTTP handling; optional [sharding](sharding.md) is additive. |
| Temporal | Separate server cluster of multiple services (Frontend, History, Matching, Worker) plus external DB(s); not a single production binary. Helm charts / docker-compose provided; your Workers are separate processes. ([docs](https://docs.temporal.io/self-hosted-guide/deployment)) |
| DBOS | Lightweight — a **library embedded in your app process** + Postgres, "no additional infrastructure required." Optional Conductor control plane for recovery/visualization/HA. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Single `inngest` server binary/container + external Postgres/Redis for production (Helm chart available); your functions are HTTP handlers the engine calls. Self-hosting is comparatively new (GA'd ~2025). ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | A Hatchet engine/API server + Postgres (+ optional RabbitMQ); workers connect over gRPC. Documented as "particularly easy to self-host." ([docs](https://docs.hatchet.run/home/architecture)) |
| Restate | Single-binary server via Homebrew/npm/Docker with embedded storage — **no external database or broker**. Your **service handlers still deploy and scale separately** (containers / serverless / VMs / k8s), which the server sits in front of and invokes over HTTP — as with any code-first engine. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 3. Language / SDK support

| Engine | SDKs |
|---|---|
| **autumn-harvest** | **Rust only.** Non-Rust systems participate as _callers_ via the HTTP [management API](management-api.md), the [workflow-result endpoint (#527)](https://github.com/autumn-foundation/autumn-harvest/issues/527), published input/output [JSON Schema (#373)](https://github.com/autumn-foundation/autumn-harvest/issues/373), and [MCP tools (#597)](https://github.com/autumn-foundation/autumn-harvest/issues/597) ([docs](mcp-tools.md)) — but there is no non-Rust worker/author SDK. A typed [TypeScript management-API client](../clients/typescript/README.md) generated from the OpenAPI document ships with each GitHub release after 0.6.0 (#1616). Non-Rust worker SDKs are out of scope by [ADR 0002](adr/0002-rust-native-execution-boundary.md); the TypeScript activity-worker SDK ([#959](https://github.com/autumn-foundation/autumn-harvest/issues/959)) and the TypeScript + Python client SDKs ([#955](https://github.com/autumn-foundation/autumn-harvest/issues/955)) were closed as not planned. |
| Temporal | 7 official SDKs: Go, Java, PHP, Python, TypeScript, .NET, Ruby. ([docs](https://docs.temporal.io/encyclopedia/temporal-sdks)) |
| DBOS | Python, TypeScript, Go, Java. ([docs](https://docs.dbos.dev/)) |
| Inngest | TypeScript, Python, Go (official); Elixir and Rust listed as in development. ([blog](https://www.inngest.com/blog/cross-language-support-with-new-sdks)) |
| Hatchet | Python, TypeScript, Go (official); Ruby also listed. ([repo](https://github.com/hatchet-dev/hatchet)) |
| Restate | TypeScript, Java, Kotlin, Python, Go, Rust. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 4. Programming model

| Engine | Model |
|---|---|
| **autumn-harvest** | Code-first async Rust with event-sourced deterministic replay. Also a first-class DAG surface (`#[dag]`, [unified DAG execution #256](https://github.com/autumn-foundation/autumn-harvest/issues/256)); [signals/queries/updates](https://github.com/autumn-foundation/autumn-harvest/issues/234) ([#140](https://github.com/autumn-foundation/autumn-harvest/issues/140), [#346](https://github.com/autumn-foundation/autumn-harvest/issues/346)); [Saga compensation](saga.md) ([#238](https://github.com/autumn-foundation/autumn-harvest/issues/238)); inbound [webhook triggers (#344)](https://github.com/autumn-foundation/autumn-harvest/issues/344); [keyed entities](adr/0006-keyed-entity.md) ([#1975](https://github.com/autumn-foundation/autumn-harvest/issues/1975)) with one handler at a time for each key; a payload-matching signal wait and [durable promises](durable-promises.md) ([#1985](https://github.com/autumn-foundation/autumn-harvest/issues/1985)). |
| Temporal | Code-first imperative durable Workflows + Activities (side effects isolated in Activities). ([docs](https://docs.temporal.io/)) |
| DBOS | Code-first — ordinary functions annotated as durable **workflows** and **steps** via decorators/annotations. ([docs](https://docs.dbos.dev/)) |
| Inngest | Event-driven step functions — functions triggered by events / cron / webhooks, logic split into memoized `step` calls. ([docs](https://www.inngest.com/docs/learn/how-functions-are-executed)) |
| Hatchet | Multi-paradigm — general-purpose task queue, DAG orchestrator, and durable-execution engine (DAGs can be built at runtime). ([docs](https://docs.hatchet.run/v1)) |
| Restate | Durable handlers across Services, **Virtual Objects** (per-key consistent state), and Workflows, with durable RPC/queuing built in. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

#### Small primitives (issue #1985)

Issue [#1985](https://github.com/autumn-foundation/autumn-harvest/issues/1985)
lists four small primitives that peer engines ship. Each has a decision.

| Primitive | Peer | Decision | Reason |
|---|---|---|---|
| Payload-matching event wait | Cloudflare `step.waitForEvent` | **Shipped.** `ctx.wait_for_signal_matching` and `ctx.receive_signal_matching` ([signals chapter](getting-started/04-signals.md)). | A signal that fails the predicate stays buffered. There is no timeout form yet. |
| Durable promise | Restate awakeables | **Shipped.** `ctx.new_promise`, `ctx.promise` and `durable_promise::resolve` / `reject` ([durable promises](durable-promises.md)). | Built on signals, not external task tokens, so a promise can race a timer and settles once. |
| Counting semaphore | No built-in peer primitive | **Declined.** | See below. |
| `AllowAll` overlap | Temporal Schedules | **Shipped.** `OverlapPolicy::AllowAll`. | It ignores `max_active_runs`, as Temporal does. Each tick phase starts at most 100 catch-up runs. |

**Why the counting semaphore is declined.** Two shipped features cover the
common needs. The per-key concurrency limit
([#247](https://github.com/autumn-foundation/autumn-harvest/issues/247)) caps
how many runs of one key are active across the fleet. The durable mutex
([#691](https://github.com/autumn-foundation/autumn-harvest/issues/691)) gives
one holder for a region of a workflow. An N-permit region lock needs its own
permit tables, a new event, lease renewal and reclaim, a terminal sweep, and
reset and rebalance hooks. That work is the size of the mutex itself. The
issue asks for small changes. Open a new issue with a use case that the two
features above cannot serve.

### 5. Determinism guarantees & tooling

| Engine | Approach |
|---|---|
| **autumn-harvest** | Event-sourced deterministic replay, layered with the deepest safety tooling in this set: **compile-time guardrails HVG001–HVG011** plus a `det_check` static analyzer (DET010/DET011) that flag non-deterministic patterns before they ship; the [`ReplayVerifier` CI gate (#251)](replay-verify.md), built on the `WorkflowReplayer` harness, that replays current code against recorded histories; **non-terminal [ND-blocking (#603)](https://github.com/autumn-foundation/autumn-harvest/issues/603)** that _parks and alerts_ a divergent run rather than silently wedging or failing it; and [deterministic side-effect primitives (#384)](https://github.com/autumn-foundation/autumn-harvest/issues/384) for time/UUID/random. See the [workflow determinism guide](workflow-determinism-guide.md), and [why Harvest keeps deterministic replay](why-deterministic-replay.md) for the trade-off against checkpoint-only steps. |
| Temporal | Deterministic replay is core (Event History replayed against code); ships **Replay testing** to detect non-determinism before deploy. ([docs](https://docs.temporal.io/develop/safe-deployments)) |
| DBOS | Checkpoint/resume from the last completed step (not command-comparison replay); docs state workflow functions must be deterministic and keep I/O in steps. ([docs](https://docs.dbos.dev/architecture)) |
| Inngest | Step-based memoization; docs state **no determinism requirement** on the orchestration layer (each step runs once, result persisted, completed steps skipped on retry). ([docs](https://www.inngest.com/docs/learn/how-functions-are-executed)) |
| Hatchet | Durable event-log replay to the last checkpoint; the durable task should be deterministic to allow replay. A dedicated replay-safety test harness is **(unverified)** from official docs. ([docs](https://docs.hatchet.run/v1/durable-execution)) |
| Restate | Per-invocation journal + replay; non-deterministic/side-effecting operations wrapped in `ctx.run`. Dedicated replay-safety test tooling is **(unverified)**. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 6. Versioning / safe-deploy

| Engine | Mechanism |
|---|---|
| **autumn-harvest** | [`ctx.version()`](workflow-determinism-guide.md) plus the two-state [`ctx.patched` / `ctx.deprecate_patch` (#687)](https://github.com/autumn-foundation/autumn-harvest/issues/687); [worker build-ID routing (#171)](https://github.com/autumn-foundation/autumn-harvest/issues/171) with [percentage build ramp (#604)](https://github.com/autumn-foundation/autumn-harvest/issues/604); and a read-only [workflow-type reachability check (#520)](https://github.com/autumn-foundation/autumn-harvest/issues/520) to gate safe handler removal. Operator playbook: [safe-deploy runbook](runbooks/safe-deploy.md). |
| Temporal | Two methods — **Worker Versioning** (Build-ID pinning + progressive rollout; GA, the recommended default) and **Patching** (`GetVersion()` / `patched` markers). ([docs](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)) |
| DBOS | Workflows versioned by code version; recovery routes a workflow to a compatible live executor (Conductor coordinates). Patching-primitive parity with Temporal's `GetVersion` is **(unverified)**. ([docs](https://docs.dbos.dev/)) |
| Inngest | Set `appVersion` (e.g. commit SHA / image tag) so rolling deploys are managed. Automatic in-flight pinning depth is **(unverified)** vs Temporal Worker Versioning. ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | Tasks/workflows defined as code ("easy to version, deploy") + **Sticky Assignment** for worker affinity. A first-class in-flight version-pinning/patching primitive is **(unverified)**. ([docs](https://docs.hatchet.run/v1)) |
| Restate | Deploy updated code to a new endpoint/deployment; new invocations route to the latest, in-flight invocations continue on the deployment that started them. ([docs](https://docs.restate.dev/services/versioning)) |

### 7. Scheduling depth

| Engine | Capabilities |
|---|---|
| **autumn-harvest** | Cron + interval, with [jitter (#240)](https://github.com/autumn-foundation/autumn-harvest/issues/240), [overlap policy (#241)](https://github.com/autumn-foundation/autumn-harvest/issues/241) with every Temporal mode, `AllowAll` included ([#1985](https://github.com/autumn-foundation/autumn-harvest/issues/1985)), [calendars + backfill (#337)](https://github.com/autumn-foundation/autumn-harvest/issues/337), [bounded catchup window (#484)](https://github.com/autumn-foundation/autumn-harvest/issues/484), bounded/finite runs ([#478](https://github.com/autumn-foundation/autumn-harvest/issues/478) / [#543](https://github.com/autumn-foundation/autumn-harvest/issues/543)), [HA-safe multi-replica ticks (#350)](https://github.com/autumn-foundation/autumn-harvest/issues/350), [schedule run history (#534)](https://github.com/autumn-foundation/autumn-harvest/issues/534), [in-place schedule update (#771)](https://github.com/autumn-foundation/autumn-harvest/issues/771), [last-completion carryover (#488)](https://github.com/autumn-foundation/autumn-harvest/issues/488), plus start-shaping via [debounce (#499)](https://github.com/autumn-foundation/autumn-harvest/issues/499) and [throttle (#607)](https://github.com/autumn-foundation/autumn-harvest/issues/607). |
| Temporal | Schedules with interval and calendar/cron spec; Catchup Window (default 1 year, min 10s); overlap policies (Skip, BufferOne, BufferAll, AllowAll, CancelOther, TerminateOther); Backfill. ([docs](https://docs.temporal.io/schedule)) |
| DBOS | Cron-scheduled workflows (stored in DB, runtime create/pause/resume/delete), time zones, and automatic backfill of missed runs after downtime. Calendar-holiday / overlap-policy depth **(unverified)**. ([docs](https://docs.dbos.dev/python/tutorials/scheduled-workflows)) |
| Inngest | Cron/scheduled functions with per-step retries. Calendar, catchup/backfill, and overlap-policy depth **(unverified)** from official docs in this pass. ([docs](https://www.inngest.com/uses/scheduled-jobs)) |
| Hatchet | Cron scheduling for scheduled + DAG workflows. Calendar, catchup/backfill, and overlap-policy depth **(unverified)**. ([docs](https://docs.hatchet.run/v1)) |
| Restate | Delayed/scheduled invocations + durable timers (a scheduled invocation runs on the latest deployment at trigger time). A Temporal-style Schedule object with cron/calendar/backfill/overlap depth is **(unverified)** — the model centers on delayed calls + timers. ([docs](https://docs.restate.dev/foundations/key-concepts)) |

### 8. Observability surface

| Engine | Surface |
|---|---|
| **autumn-harvest** | An [OpenTelemetry trace contract (ADR-0001)](adr/0001-otel-trace-contract.md) ([#136](https://github.com/autumn-foundation/autumn-harvest/issues/136)) covering 8 named span kinds; a bounded-cardinality metric catalogue with a `metrics-rs` adapter; a [starter alert pack + runbooks](alerts/); a [Grafana dashboard pack (#754)](https://github.com/autumn-foundation/autumn-harvest/issues/754); a per-execution [timeline API (#739)](https://github.com/autumn-foundation/autumn-harvest/issues/739); a [DAG run graph view (#690)](https://github.com/autumn-foundation/autumn-harvest/issues/690); and a rolled-up [health summary endpoint (#679)](https://github.com/autumn-foundation/autumn-harvest/issues/679). The embedded **Vantage UI is partial** — the [Workers tab (#142)](https://github.com/autumn-foundation/autumn-harvest/issues/142), a DLQ inspection page with a summary view ([#226](https://github.com/autumn-foundation/autumn-harvest/issues/226) / [#385](https://github.com/autumn-foundation/autumn-harvest/issues/385)), a [schedules management page (#333)](https://github.com/autumn-foundation/autumn-harvest/issues/333), and [DAG list + detail pages (#426)](https://github.com/autumn-foundation/autumn-harvest/issues/426) have shipped, and the DAG detail page renders each run as an inline SVG node/edge graph. The UI still trails the competitors' (see [Where harvest is behind](#where-harvest-is-behind)). See [telemetry](telemetry.md). |
| Temporal | Open-source Web UI; SDK metrics (Prometheus); tracing/OTel via SDK interceptors. Temporal Cloud adds a Prometheus-compatible OpenMetrics endpoint. ([docs](https://docs.temporal.io/references/sdk-metrics)) |
| DBOS | OpenTelemetry traces per workflow/step; Prometheus-compatible metrics endpoint; Conductor dashboards of active/past workflows + queued tasks. ([docs](https://www.dbos.dev/dbos-conductor)) |
| Inngest | Built-in Dashboard UI with step-level observability (queue delay, step timing, flow control) + event history. Explicit OTel export is **(unverified)** in this pass. ([docs](https://www.inngest.com/docs/self-hosting)) |
| Hatchet | Built-in dashboard; a built-in **OpenTelemetry** collector emitting traces/spans per task/workflow; Prometheus metrics (noted as Dedicated-tier+ on Hatchet Cloud). ([OTel](https://docs.hatchet.run/home/opentelemetry), [Prometheus](https://docs.hatchet.run/v1/prometheus-metrics)) |
| Restate | Built-in Restate UI with a live execution-step timeline (retries, nested RPC, awakeables, cancellation) and a distributed call-stack view; OpenTelemetry tracing supported. ([blog](https://www.restate.dev/blog/announcing-restate-ui)) |

### 9. HA / multi-region

| Engine | Story |
|---|---|
| **autumn-harvest** | HA-safe scheduler ticks under multi-replica deployments ([#350](https://github.com/autumn-foundation/autumn-harvest/issues/350)); horizontal scale via Postgres [sharding](sharding.md). Each shard is one Postgres, and a workflow's own state stays on its shard. **Cross-region DR** ships ([#954](https://github.com/autumn-foundation/autumn-harvest/issues/954)): each shard replicates to a standby region with stock Postgres replication, and an operator runs a fenced failover ([cross-region DR](cross-region-dr.md)). There is no automatic promotion, no active-active writing and no zero-RPO mode. Explicit shard pinning for data residency ships ([#697](https://github.com/autumn-foundation/autumn-harvest/issues/697)). |
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

### 12. Durable agents

Cloudflare's Agents SDK, Golem and Restate share one definition of a
durable agent. It is a named, long-lived actor. It owns its state, handles
one message at a time, wakes itself on timers and survives crashes. Issue
[#2276](https://github.com/autumn-foundation/autumn-harvest/issues/2276)
asks how Harvest compares on that definition, and on the mesh layer above it.

This section compares a different set of peers from the tables above.
Temporal stays, because it is in every table. DBOS, Inngest and Hatchet are out: the sources
for this section cover only their agent-framework integrations. The
competitor cells were checked on 2026-10-19. The Harvest cells ship in this
release, unless a cell names an earlier issue.

| Engine | Agent and inbox | Wakes | Crash recovery | Listing | Agent to agent | Authority across agents | Live tokens |
|---|---|---|---|---|---|---|---|
| **autumn-harvest** | An `Agent` is a keyed entity. Its id is `<kind>/<name>`, and it runs one op at a time ([#2263](https://github.com/autumn-foundation/autumn-harvest/issues/2263)). A paused agent holds its work in the inbox, in order. | Heartbeats with a precheck, and booked follow-ups ([#2266](https://github.com/autumn-foundation/autumn-harvest/issues/2266)). A wake races the op signal against a durable deadline, so a tick never overlaps a message. An unattended tick is read-only unless its heartbeat allows actions ([#1973](https://github.com/autumn-foundation/autumn-harvest/issues/1973)). | Replay of the recorded model turns and tool calls ([#1973](https://github.com/autumn-foundation/autumn-harvest/issues/1973)). A crash costs at most the step in flight. An idle agent holds no worker memory ([#2267](https://github.com/autumn-foundation/autumn-harvest/issues/2267)). | An indexed Postgres query over search attributes, such as `agent_status` and `agent_next_wake_at`. The CLI has `harvest agents list`, `describe`, `pause`, `resume` and `erase` ([#2267](https://github.com/autumn-foundation/autumn-harvest/issues/2267)). | A durable send by workflow id, with the key `<sender run id>:<seq>`. The engine dedupes the key across continue-as-new. The receiver also skips a sender seq that it applied ([#2269](https://github.com/autumn-foundation/autumn-harvest/issues/2269)). [`AgentMesh.tla`](../formal/tla/AgentMesh.tla) checks exactly-once delivery across a crash and an owner change. A send into a waiting cycle fails at once as a deadlock. | A [mandate](agent-mesh.md) only narrows, in a tree and across processes ([PR #2148](https://github.com/autumn-foundation/autumn-harvest/pull/2148), [#2269](https://github.com/autumn-foundation/autumn-harvest/issues/2269)). Flow rules gate a forbidden label set across the tree. Siblings split one token budget ([#2270](https://github.com/autumn-foundation/autumn-harvest/issues/2270)). `MeshBudget` caps a mesh for each period ([#2272](https://github.com/autumn-foundation/autumn-harvest/issues/2272)). | `publish_progress` sends each token delta over Postgres `pg_notify`. Nothing is stored ([#2268](https://github.com/autumn-foundation/autumn-harvest/issues/2268)). SQLite drops it. |
| Golem | A durable agent is "a stateful sandbox per identity" ([blog](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)). Its id is the agent type and its constructor parameters ([1.5](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)). One invocation at a time is **(unverified)**. | Scheduled agent calls are durable and can be cancelled ([post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)). A heartbeat with a precheck is **(unverified)**. | Oplog replay from the latest snapshot ([docs](https://learn.golem.cloud/v1.5/how-to-guides/ts/golem-configure-durability-ts)). Since 1.6, an epoch fence stops a stale executor from writing the oplog ([PR #3845](https://github.com/golemcloud/golem/pull/3845)). | `golem agent list` pages through agents with filters ([docs](https://learn.golem.cloud/how-to-guides/common/golem-list-and-filter-agents)). The docs say that enumeration is slow and not for application logic ([API](https://learn.golem.cloud/v1.5/develop/additional)). | Agent-to-agent calls run exactly once, with one key across retries and replays. `get` creates the target if it is absent ([post](https://golem.cloud/blog/whats-this-golem-cloud-thing-about/)). | Capability cards with upper bounds, and host-enforced tool mediation, for each agent ([blog](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)). Quotas cap rate, capacity and concurrency ([1.5](https://golem.cloud/blog/golem-1-5-the-agent-runtime/)). Narrowing across an agent call, flow rules and a token budget across agents are **(unverified)**. | 1.6 adds Durable Streams and streaming methods ([1.6](https://golem.cloud/blog/golem-1-6-agents-tools-and-artifacts/)). Their use for model tokens is **(unverified)**. |
| Cloudflare Agents SDK (Durable Objects) | An agent is a Durable Object ([Agent class](https://developers.cloudflare.com/agents/concepts/agent-class/)), addressed by name ([API](https://developers.cloudflare.com/durable-objects/api/namespace/)). An input gate holds events only during a storage call. A `fetch()` lets other requests interleave ([rules](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)). | One alarm for each object, run at least once ([alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)). The SDK keeps many schedules in SQL behind that alarm. `keepAlive()` uses alarm heartbeats ([Agent class](https://developers.cloudflare.com/agents/concepts/agent-class/)). | Each write to the SQLite storage of the object is replicated before it is confirmed ([blog](https://blog.cloudflare.com/sqlite-in-durable-objects/)). An object can restart at any time, and no shutdown hook runs ([lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)). Long multi-step work goes to Workflows ([changelog](https://developers.cloudflare.com/changelog/post/2026-02-03-agents-workflows-integration)). | No in-Worker API lists objects. Keep an index, for example in a parent object or in D1 ([rules](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)). | RPC between objects. The cited docs give no delivery guarantee. They advise a retry only for an idempotent call ([errors](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/)). | Not in the cited docs **(unverified)**. | An agent broadcasts its state to clients over WebSockets ([Agent class](https://developers.cloudflare.com/agents/concepts/agent-class/)). Streaming of model tokens is **(unverified)**. |
| Restate (Virtual Objects) | A Virtual Object has a key. One exclusive handler runs for each key. Later calls wait in a per-key inbox, in arrival order ([services](https://docs.restate.dev/concepts/services), [calls](https://docs.restate.dev/develop/ts/service-communication)). | Durable timers and delayed messages ([docs](https://docs.restate.dev/concepts/durable_building_blocks)). A heartbeat with a precheck is **(unverified)**. | Journal replay. State updates commit with the journal steps ([concepts](https://docs.restate.dev/foundations/key-concepts)). Leader epochs fence stale attempts ([architecture](https://docs.restate.dev/references/architecture)). | SQL over the `state`, `sys_inbox` and `sys_locks` tables ([introspection](https://docs.restate.dev/references/sql-introspection)). A query scans the live partition stores, and a heavy query can slow invocations ([#5170](https://github.com/restatedev/restate/issues/5170)). | Calls, one-way messages and delayed messages are exactly once ([docs](https://docs.restate.dev/concepts/durable_building_blocks)). Calls between exclusive handlers can deadlock. Restate does not detect it, and an operator cancels by hand ([calls](https://docs.restate.dev/develop/ts/service-communication)). | Concurrency limits for each scope, experimental in 1.7 ([notes](https://github.com/restatedev/restate/blob/main/release-notes/v1.7.0.md)). Restate frames them around inference calls ([blog](https://restate.dev/blog/announcing-restate-1-7)). Mandates and flow rules across objects are **(unverified)**. | Not in the cited docs **(unverified)**. The AI SDK middleware journals model calls ([AI docs](https://docs.restate.dev/ai/index)). |
| Temporal | A workflow, addressed by its Workflow Id ([server](https://docs.temporal.io/temporal-service/temporal-server)). Agent SDK integrations run each model call as an activity ([OpenAI Agents](https://docs.temporal.io/develop/python/integrations/openai-agents)). One message at a time is **(unverified)**. | Durable timers and Schedules ([docs](https://docs.temporal.io/schedule)). A heartbeat with a precheck is **(unverified)**. | Replay of the event history ([docs](https://docs.temporal.io/develop/safe-deployments)). | Visibility, a search index that Temporal updates asynchronously. It is eventually consistent ([docs](https://docs.temporal.io/visibility)). | A Signal is fire-and-forget. An Update returns a result. Signal-with-Start starts a missing target ([messages](https://docs.temporal.io/encyclopedia/workflow-message-passing), [Go](https://docs.temporal.io/develop/go/message-passing)). Nexus calls cross namespaces ([Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)). | Principal Attribution (pre-release) records the caller in history for audit ([Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)). Mandates, flow rules and budgets across agents are **(unverified)**. | Workflow Streams (Public Preview) streams token batches over Signals and Updates ([Replay 2026](https://temporal.io/blog/replay-2026-product-announcements)). |

**What differs.** Golem, Cloudflare and Restate each ship the core of the
definition: a named actor that owns its state and survives a crash. No
cited peer doc shows a mandate that narrows across agents. None shows flow
rules over a tree of agents, or a budget across a mesh. Golem comes
closest, with a capability card for each agent. Harvest ships these layers
with the agent, and replay keeps them across a crash. The same holds for
heartbeats with prechecks and for read-only unattended runs.

**Harvest limits for agents.**

- A peer send needs a running target. Start each agent of a mesh first.
  A Golem `get` and a Temporal Signal-with-Start create a missing target.
- Across shards, a send goes through the outbox. It lands up to one scanner
  poll interval later.
- SQLite drops live progress. A client of a SQLite agent sees only the
  final text.
- On SQLite, a running activity never sees a cancel. An interrupt ends the
  wait, but the model call runs to its end.
- Live tokens are best effort, and nothing stores them. A client that
  reconnects reads the final text once.
- A mandate carries no signature. Authenticate a remote caller as for any
  request ([agent mesh](agent-mesh.md)).
- The agent layer is Rust only, like the rest of Harvest.

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
2. **[Deterministic side-effect primitives (#384)](https://github.com/autumn-foundation/autumn-harvest/issues/384)**
   give authors safe replacements (`ctx.system_now`, `ctx.new_uuid`,
   `ctx.random_*`) that record their value once and replay it verbatim.
3. **The [`ReplayVerifier` gate (#251)](replay-verify.md)** replays a
   code change against recorded production histories in CI, so a non-determinism
   regression is a failed test, not a 2 a.m. page.
4. **Non-terminal [ND-blocking (#603)](https://github.com/autumn-foundation/autumn-harvest/issues/603)**
   is the runtime backstop: if a divergence does reach production, the affected
   execution is _parked and made alertable with a bounded backoff_ — not silently
   wedged, and not terminally failed — so a rolled-back deploy lets it resume
   exactly where it was. Most engines in this set treat replay determinism as an
   author responsibility validated by replay tests; harvest additionally makes a
   divergent run a recoverable, observable state.

Some engines skip replay and resume from step checkpoints instead.
[Why Harvest keeps deterministic replay](why-deterministic-replay.md) states
what replay buys, what it costs, and when a checkpoint-only engine is the
better choice.

### 3. Embedded in your web app — no separate orchestrator cluster

Harvest is designed to run **inside your application process**, wired in through
one `HarvestPlugin` on an [Autumn](https://github.com/autumn-foundation/autumn) app (or a
standalone `HarvestRunner` when you're not on Autumn). There is no orchestrator
cluster to deploy alongside your service and no network hop between your request
handlers and your workflows — a workflow can be started from an HTTP route and
awaited in-process. That embedding also unlocks integrations that a separate
cluster makes awkward: [MCP tool exposure (#597)](https://github.com/autumn-foundation/autumn-harvest/issues/597)
turns a `#[workflow(mcp)]` into a correlated agent tool set ([docs](mcp-tools.md)),
and an inbound [webhook receiver (#344)](https://github.com/autumn-foundation/autumn-harvest/issues/344)
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
  Temporal (7 SDKs) and Restate (6). A TypeScript management-API client ships
  (#1616), but it only calls the API. [ADR 0002](adr/0002-rust-native-execution-boundary.md)
  keeps workers Rust-only, so this gap stays open by design.
- **DR is operator-driven, not automatic.** Each shard replicates to a standby
  region, and an operator runs a fenced failover
  ([cross-region DR](cross-region-dr.md),
  [#954](https://github.com/autumn-foundation/autumn-harvest/issues/954)). There is
  no automatic promotion, no active-active writing and no zero-RPO mode. Temporal
  (Global Namespaces / Cloud 2-region) and the managed clouds are ahead here.
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
  [Workers tab (#142)](https://github.com/autumn-foundation/autumn-harvest/issues/142),
  a DLQ inspection page with a summary view
  ([#226](https://github.com/autumn-foundation/autumn-harvest/issues/226) /
  [#385](https://github.com/autumn-foundation/autumn-harvest/issues/385)), a
  [schedules management page (#333)](https://github.com/autumn-foundation/autumn-harvest/issues/333),
  [DAG list + detail pages (#426)](https://github.com/autumn-foundation/autumn-harvest/issues/426)
  and a rendered DAG run graph. The workflow list takes `AND`/`OR` filter
  expressions, and the list and detail pages update live
  ([#1982](https://github.com/autumn-foundation/autumn-harvest/issues/1982)).
  Temporal, Inngest, Hatchet, DBOS (Conductor), and Restate all
  ship more mature UIs today.
- **Younger project, smaller ecosystem.** harvest is pre-1.0 (0.x, breaking
  changes in minor versions) with a smaller community, fewer third-party
  integrations, and a much smaller hiring pool than Temporal in particular. Some
  competitor cells above are marked **(unverified)** precisely because this is a
  young market moving fast.
- **Cross-shard composition is limited.** A workflow's own state (events, tasks,
  timers, signals, DLQ) stays on one shard, and there is no cross-shard
  transaction. A child can run on another shard only when its spawn opts in
  ([cross-shard child placement](sharding.md#cross-shard-child-placement-issue-956),
  [#956](https://github.com/autumn-foundation/autumn-harvest/issues/956)). Its start
  and its terminal wake are each one scanner tick away, or longer after a failed
  attempt. Delivery is at least once with dedupe, so the parent sees one terminal
  event. An operator can move quiescent workflows between shards
  ([shard rebalancing](sharding.md#shard-rebalancing--migrating-quiescent-workflows-issue-964),
  [#964](https://github.com/autumn-foundation/autumn-harvest/issues/964)), but not
  running ones.
- **Keyed entities are a library, not a native primitive.** `autumn_harvest::entity`
  gives each key one handler at a time and durable state
  ([ADR 0006](adr/0006-keyed-entity.md),
  [#1975](https://github.com/autumn-foundation/autumn-harvest/issues/1975)). An
  operation has no reply, so a caller reads state with a query. A Restate
  virtual object call returns a result. An Azure durable entity call made from
  an orchestration also returns one.
- **Durable agents have backend limits.** A peer send needs a running target.
  A Golem agent call and a Temporal Signal-with-Start create a missing one.
  On SQLite, an agent streams no live tokens, and a running model call never
  sees an interrupt. See [durable agents](#12-durable-agents).
- **Behind Temporal on throughput, on one measured box.** harvest publishes
  its own reproducible end-to-end numbers and the harness that produces them
  ([`benchmarks.md`](benchmarks.md),
  [#941](https://github.com/autumn-foundation/autumn-harvest/issues/941)).
  [Assay #14](assays/0014-harvest-vs-temporal-0.7.0-depth-sweep.md)
  ([#1972](https://github.com/autumn-foundation/autumn-harvest/issues/1972))
  measures harvest 0.7.0 against Temporal 1.25.2 on one 4-core box. Both
  engines use one Postgres server and run a 3-activity workflow. Temporal
  completed more workflows/sec at every backlog depth from 250 to 2,000:

  | harvest mode | Temporal ahead by |
  |:--|--:|
  | default (Postgres claim path), 0.7.0 | 1.78x to 7.17x |
  | Redis dispatch, 0.7.0 | 1.74x to 2.10x |
  | default, with the #1971 claim fix (PR #2052, merged to `trunk-dev`, not yet released) | 1.16x to 1.36x |

  Without the claim fix, the default-mode gap grows sharply with backlog
  depth. With it, harvest's own rate stays flat, and the gap grows only from
  1.16x to 1.36x.

  The bounds matter more than the ratios. It is one workflow shape on one
  small box, with Temporal at its defaults and its four services sharing four
  cores. That venue favours harvest, so a better-tuned Temporal could widen
  the gap. A competitor's own published figure is not a substitute: its
  hardware, configuration and unit (often per action) differ. The assay
  states what the result may and may not be read to mean. Its predecessor,
  [assay #11](assays/0011-harvest-vs-temporal-single-box.md), measured the
  same shape on 0.6.x on another host.

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
- **[Temporal migration guide](migrating-from-temporal.md)**
  ([#947](https://github.com/autumn-foundation/autumn-harvest/issues/947)): a
  concept map, a workflow-porting checklist, and a dual-run cutover playbook.
  This page answers _why / whether_ harvest; the migration guide answers
  _how_ to move.
- **[Why Harvest keeps deterministic replay](why-deterministic-replay.md)**
  ([#1993](https://github.com/autumn-foundation/autumn-harvest/issues/1993)):
  what replay buys over checkpoint-only steps, and how Harvest lowers its cost.
- **[Durable agents](agent-adapter.md)** and the **[agent mesh](agent-mesh.md)**
  ([#2262](https://github.com/autumn-foundation/autumn-harvest/issues/2262)):
  the agent, its wakes, peers, mandates, flow rules and budgets.
- **[Safety report](safety-report.md)**
  ([#2004](https://github.com/autumn-foundation/autumn-harvest/issues/2004)):
  the claim, tests, results and known limits for leases, fencing,
  exactly-once completion and signal ordering.
