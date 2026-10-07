# Autumn Harvest: Platform, Operations and Ecosystem Feature Inventory

Scope: platform/ops/ecosystem capabilities of Autumn Harvest at git HEAD `301ea95` (branch `claude/awesome-wozniak-y24886`, committed 2026-10-06), for comparison against Temporal (self-hosted and Cloud), Restate, DBOS (+Conductor), Inngest, Hatchet, Trigger.dev, AWS Step Functions, Azure Durable Task Scheduler, Orkes Conductor and Camunda. This is a codebase inventory only. Every claim cites `path:line` in the repository at `/home/user/autumn-harvest` (sources are repository files, not URLs). Status vocabulary: **Present** (implemented in code), **Partial** (implemented with notable limits, or opt-in only), **Absent**, **Unclear**. "Opt-in" means off by default.

Important context: the prior notes in `research_notes/Autumn Harvest resilience gap analysis/operations_observability_security.md` were taken at commit `937b655` (2026-09-30). Many gaps listed there are **closed at HEAD**. The 50 most recent commits include: SLO burn-rate alerts (#1816), AES-256-GCM codec + KMS (#1825), per-client API rate limiting (#1827), N/N-1 rolling-deploy contract (#1828), signed releases + Dependabot + daily advisory scan (#1826), tenant cells ADR (#1837), audit hash chain + signed WASM modules + OTel Collector recipe (#1838), append-only enforcement trigger (#1817), Kubernetes probes (#1812), default history caps (#1804) (`git log --oneline`, HEAD `301ea95` back to `a9da7bd`). Treat the earlier notes' "absent" findings as stale unless re-verified below.

---

## 1. Deployment model (embedded vs server, single binary, Kubernetes, serverless, managed cloud)

### Takeaway
Harvest is an **embedded Rust library**, not a server: it runs inside the application process as an autumn-web plugin, as `HarvestEmbedding` on plain Axum, or as the bare core crate; Postgres is the only required dependency. There is no Helm chart, container image, Kubernetes operator, serverless/push-to-HTTP worker model, or managed cloud, and the project says the lack of a managed cloud is deliberate.

### Cited Findings
- **Embedded library — Present.** Four deployment paths: `HarvestPlugin` on autumn-web ("Every feature is available"), `HarvestEmbedding` on plain Axum, "HTTP from another language" (client only), and the core crate alone with no HTTP surface — [docs/embedding.md:30-37](../../docs/embedding.md). Workspace crates: core, plugin, macros, CLI, optional Redis dispatch, optional SQLite backend — [README.md:332-345](../../README.md).
- **Standalone runner — Present.** `pub struct HarvestRunner` — [autumn-harvest-plugin/src/runner.rs:900](../../autumn-harvest-plugin/src/runner.rs). Example with no `autumn-web` dependency: `examples/standalone-runner/` — [README.md:350-356](../../README.md).
- **Off-plugin limits — Partial.** On `HarvestEmbedding` (plain Axum), MCP tools, the workflow-start outbox relay, Kafka/SQS broker connectors, outbound webhook delivery, webhook replay protection and admin session login are **not available** — [docs/embedding.md:529-542](../../docs/embedding.md). The router is `axum::Router<()>`, "so this path is for Axum services, not an arbitrary framework" — [README.md:354-356](../../README.md).
- **Infrastructure — Postgres only by default.** "Postgres is the only required infrastructure dependency" — [README.md:1198](../../README.md). Postgres 12+ — [README.md:1094-1107](../../README.md). Optional Redis Streams is a *dispatch channel only* (Postgres stays source of truth); v1 is single Redis instance, single-shard; "Redis Cluster is not supported" — [README.md:1196-1231](../../README.md). Optional embedded SQLite backend is single-writer/single-server and rejects child workflows, external signals, local/external activities, updates, search attributes, continue-as-new, sessions and cancellable timers with a typed `UnsupportedFeature` failure — [docs/sqlite-backend.md:1-16](../../docs/sqlite-backend.md), [docs/sqlite-backend.md:359](../../docs/sqlite-backend.md) (single-writer contract section), and the non-goals list in section 11 of [docs/sqlite-backend.md](../../docs/sqlite-backend.md).
- **Single binary server — Absent.** No standalone Harvest server binary exists. The only shipped binaries are the `harvest` CLI (release targets x86_64-linux-gnu, aarch64-apple-darwin, x86_64-windows-msvc — [.github/workflows/release.yml:124-138](../../.github/workflows/release.yml)) and `harvest_replay` (`autumn-harvest-cli/src/bin/harvest_replay.rs`).
- **Kubernetes / Helm / operator — Partial (probes only).** A repo-wide `find` for `Chart.yaml`, `values.yaml`, `Dockerfile*` and `kustomization*` returned nothing, and no Helm, CRD or operator text appears in `docs/` (grep, HEAD). What does exist: `/health/live` (always 200, no I/O) and `/health/ready` probes with graceful-shutdown guidance (issue #1812) — [docs/operations/kubernetes-probes.md:1-21](../../docs/operations/kubernetes-probes.md), [README.md:1172-1175](../../README.md). There is also a KEDA/HPA scaling-signal endpoint (see §6).
- **HA multi-replica — Present.** "two or more replicas behind a load balancer ... is the default deployment topology and is fully supported". Schedule firing uses an atomic claim, so each `(schedule_id, logical_date)` produces exactly one execution — [docs/runbooks/ha-deployment.md:1-40](../../docs/runbooks/ha-deployment.md).
- **Serverless / push-based workers (invoke via HTTP, Lambda, Cloudflare) — Absent.** Grep for `lambda|cloudflare|serverless` in `docs/` and `README.md` finds only competitor descriptions and an SSE proxy note — [docs/comparison.md:83](../../docs/comparison.md), [docs/management-api.md:256](../../docs/management-api.md). ADR 0002 decides Harvest "will not maintain official polyglot worker runtimes, language SDKs, or a remote activity worker protocol unless this ADR is superseded". It routes non-Rust code through external activities with task tokens, signals, webhooks and the management API — [docs/adr/0002-rust-native-execution-boundary.md:44-60](../../docs/adr/0002-rust-native-execution-boundary.md). The closest analogues are external activity handoffs (`POST /activities/external/{token}/complete|fail|heartbeat`, `docs/openapi.json`; [docs/runbooks/external-activity-handoffs.md:1-10](../../docs/runbooks/external-activity-handoffs.md)) and outbound completion callbacks (§11).
- **WASM activities / hot code swap — Partial (experimental).** `wasm-activities` and `hot-code-swap` are off-by-default Cargo features described as an "R&D spike" and "Not a committed GA feature". They need Rust ≥1.95 — [autumn-harvest/Cargo.toml](../../autumn-harvest/Cargo.toml) `[features]`, [autumn-harvest/src/hot_swap.rs:1-4](../../autumn-harvest/src/hot_swap.rs), [autumn-harvest/src/wasm_activities.rs:1-4](../../autumn-harvest/src/wasm_activities.rs).
- **Managed cloud — Absent (by design).** "None. harvest is embed-in-your-app + self-host-Postgres **by design** ... none is currently planned" — [docs/comparison.md:166](../../docs/comparison.md), [docs/comparison.md:268-271](../../docs/comparison.md).

### Inferences
- Harvest's deployment shape is closest to DBOS Transact (library in the app process + Postgres). It is furthest from Temporal (a server cluster), Restate, Hatchet, Inngest and Orkes/Camunda (all server-based), and from Step Functions and Azure DTS (managed only). Harvest has no equivalent of DBOS Conductor, Temporal Cloud or Restate Cloud.
- Workers are long-lived in-process pollers. Inngest, Trigger.dev and Restate can invoke HTTP/serverless functions. Harvest has no way to do that.

### Gaps
- No container image or published deployment manifest was found. It is unclear whether crates are published to crates.io by automation: `release.yml` has no `cargo publish` step (grep for `publish` matched only release-asset steps), although the README carries a crates.io badge ([README.md:3](../../README.md)).

---

## 2. Multi-tenancy (namespaces, quotas, isolation, tenant-scoped retention)

### Takeaway
**Partial.** Harvest supports "cooperative multi-tenancy" through tenant-key quotas, throttles and concurrency caps, plus "cells" (a dedicated shard and worker pool per tenant). It has **no namespaces**, no per-tenant row scoping, and no tenant-scoped retention. A cell bounds load, not access.

### Cited Findings
- "**Harvest has no namespaces.** All tenants on one shard share its tables. Harvest has no per-tenant row scoping." — [docs/security-posture.md:679-704](../../docs/security-posture.md).
- "A cell is not a security boundary between tenants. It bounds load, not access." The caller-declared `x-harvest-tenant` header "is not an identity" — [docs/security-posture.md:684-697](../../docs/security-posture.md). Decision record: ADR 0004 tenant cells (issue #1837, 2026-10-06), built from residency pinning (#697), `WorkerConfig::with_shard_assignments` (#961) and queue scoping — [docs/adr/0004-tenant-isolation-cells.md:1-40](../../docs/adr/0004-tenant-isolation-cells.md); operating guide in the "Tenant cells (issue #1837)" section of [docs/sharding.md:415-517](../../docs/sharding.md).
- **Per-tenant quotas — Present (opt-in, per workflow type).** `QuotaPolicy` caps three things per resolved tenant key: active executions, history bytes and DLQ rows (issue #946). It shares the dot-path key resolver with throttle (#607, rate) and concurrency (#247, parallelism) — [autumn-harvest/src/quota.rs:1-30](../../autumn-harvest/src/quota.rs). These limits are shard-local, and a "Cross-shard global limits" section marks them out of scope — [docs/sharding.md:18-45](../../docs/sharding.md), [docs/sharding.md:401](../../docs/sharding.md).
- **Noisy-neighbour resources not covered by keys:** DB connections, per-shard scanner time, LISTEN/NOTIFY bandwidth and shared worker slots — [docs/adr/0004-tenant-isolation-cells.md:15-22](../../docs/adr/0004-tenant-isolation-cells.md).
- **Tenant-aware authorization — Partial (hook).** The authorizer hook (issue #1803, off by default) sees `principal`, `route_class`, `tenant_key` (from the `x-harvest-tenant` header) and `shard`. It "can only deny" — [docs/security-posture.md:392-450](../../docs/security-posture.md).
- **Usage / chargeback per tenant — Present.** `harvest usage` / `GET /admin/usage` reports per-tenant or per-workflow usage grouped by `workflow_name` or `search_attr:<key>` (issue #596) — [autumn-harvest-cli/src/lib.rs:1236-1257](../../autumn-harvest-cli/src/lib.rs), [autumn-harvest/src/usage.rs:1-4](../../autumn-harvest/src/usage.rs).
- **Tenant-scoped retention — Absent.** Retention has a global `max_age_secs` plus per-**workflow-type** overrides only (issue #737) — [autumn-harvest/src/retention.rs:333-345](../../autumn-harvest/src/retention.rs).

### Inferences
- Temporal namespaces, Orkes/Camunda tenants and Hatchet tenants give a named isolation and authorization unit. Harvest offers placement (cells) plus a deny-only hook that the embedder writes. This suits a single operator serving many internal tenants. It is weak for hostile multi-tenant SaaS.

### Gaps
- No per-tenant metrics labels were verified. The bounded-cardinality rule in ADR-0001 §7 suggests tenant labels are deliberately excluded ([docs/telemetry.md:282](../../docs/telemetry.md) section heading). This needs confirmation.

---

## 3. Security (authn/authz, RBAC, mTLS, API keys, OIDC, audit log, encryption/codec server, residency, PII erasure)

### Takeaway
Security is now fairly deep, but it is **mostly opt-in and delegated**. Authentication belongs to the host app. Mutating routes fail closed outside `dev`. Harvest ships opt-in scoped API tokens (read/mutate/admin), a read-only operator role, a deny-only authorizer hook, an audit trail with SIEM export and an optional hash chain, opt-in per-client rate limiting, an AES-256-GCM payload codec with Env/File/AWS-KMS key providers and key rotation, PII erasure, legal hold, and shard-pinned data residency. There is **no built-in IdP/OIDC, no RBAC engine, no mTLS for the API, and no remote codec server**. Server-side decode-on-read replaces the codec server.

### Cited Findings
- **AuthN model — Partial (delegated).** "Harvest does not ship its own identity provider, session system, or RBAC engine. Authentication and authorization are **delegated to the host Autumn application**" — [docs/security-posture.md:7-12](../../docs/security-posture.md).
- **Fail-closed mutations — Present (default since 0.7.0).** Outside the `dev` profile, a mount with no declared auth refuses every `Mutating` route, Vantage POST and mutating MCP tool with 401 (issue #1802) — [docs/security-posture.md:93-110](../../docs/security-posture.md). Listed as a breaking default in [CHANGELOG.md:18](../../CHANGELOG.md).
- **Route classification:** every route is `PublicSafe`, `ReadOnly` or `Mutating` via `audit::CLASSIFIED_ROUTES`. An unclassified route counts as `Mutating` — [docs/security-posture.md:16-42](../../docs/security-posture.md).
- **API keys — Present (opt-in).** `hvst_` tokens are stored as a SHA-256 hash. Scopes are `read` < `mutate` < `admin`, and the admin scope gates token minting. Tokens are revocable and can expire. Enable with `.enable_api_tokens()`; tokens compose with or replace embedder auth (issue #942) — [docs/security-posture.md:262-345](../../docs/security-posture.md). CLI: `harvest token create|list|revoke|rotate|bootstrap` — [autumn-harvest-cli/src/lib.rs:1894-1958](../../autumn-harvest-cli/src/lib.rs).
- **RBAC — Partial.** There are only three verb-level tiers: the read-only operator role (`api_with_role_auth`), token scopes, and the deny-only authorizer hook keyed by principal, route class, tenant and shard — [docs/security-posture.md:160-170](../../docs/security-posture.md), [docs/security-posture.md:392-450](../../docs/security-posture.md). No per-workflow-type or per-object permissions exist.
- **OIDC / JWT / SSO — Absent (embedder's job).** The docs only tell the embedder to "Replace the static token comparison with your actual validation logic (JWT ...)" — [docs/security-posture.md:227](../../docs/security-posture.md). The only OIDC mention is Sigstore signing of releases — [docs/security-posture.md:1082](../../docs/security-posture.md).
- **mTLS — Absent for the API.** The `tls` feature covers Harvest's own outbound Postgres connections (LISTEN/NOTIFY, backup verify, DR) — [autumn-harvest/Cargo.toml](../../autumn-harvest/Cargo.toml) `tls` feature comment. Redis supports `rediss://` (#1834) — [README.md:1224-1228](../../README.md). No server-side TLS or client-certificate configuration was found (grep `mtls|client cert` in security docs returned nothing).
- **API rate limiting — Present (opt-in).** In-process per-client token buckets: default 20 rps mutating (burst 40) and 100 rps read (burst 200). The limiter answers 429 with `Retry-After` and is "off by default. **Turn it on in production.**" (issue #1827) — [docs/security-posture.md:513-545](../../docs/security-posture.md).
- **Audit log — Present.** Records who did what for every high-impact mutation, without payloads (issue #158) — [autumn-harvest/src/audit.rs:1-4](../../autumn-harvest/src/audit.rs). Streaming export to an external sink with gap detection (issue #953) — [autumn-harvest/src/audit_export.rs:1](../../autumn-harvest/src/audit_export.rs). An **optional keyed hash chain** makes stored rows tamper-evident (issue #1838) — [autumn-harvest/src/audit_chain.rs:1-4](../../autumn-harvest/src/audit_chain.rs), [docs/security-posture.md:818-827](../../docs/security-posture.md). Authz denies are audited as `authz.deny` — [docs/security-posture.md:485](../../docs/security-posture.md). Audit retention defaults to 90 days — [autumn-harvest/src/retention.rs:353-355](../../autumn-harvest/src/retention.rs). CLI: `harvest audit list` — [autumn-harvest-cli/src/lib.rs:1826-1828](../../autumn-harvest-cli/src/lib.rs).
- **Payload encryption — Present (opt-in), with scope limits.** `AeadCodec` uses AES-256-GCM (RustCrypto) with a fresh 96-bit nonce and an authenticated key-id header (issue #1825) — [docs/security-posture.md:858-876](../../docs/security-posture.md). Key providers: `EnvKeyProvider`, `FileKeyProvider`, `KmsKeyProvider`; AWS KMS sits behind the plugin `aws-kms` feature — [docs/security-posture.md:930-990](../../docs/security-posture.md). **Not encrypted:** execution `input/output/memo/search_attrs` columns, task-queue input/output, signals, DLQ input, outbox/schedule copies, and all error/reason strings. No plaintext-to-ciphertext migration exists for already-stored data — [docs/security-posture.md:878-929](../../docs/security-posture.md), [docs/security-posture.md:1000-1013](../../docs/security-posture.md). No GCP or Azure KMS adapter exists (the table lists AWS only).
- **Codec server — Absent (replaced).** Read-path decoding (issue #608) decodes admin reads server-side with the in-process codec registry: "No sidecar codec server, no second key distribution". It is off by default and admin-gated — [docs/operations/read-path-decode.md:1-30](../../docs/operations/read-path-decode.md).
- **Codec key rotation — Present.** A lazy re-encryption sweep with CAS writes (issue #948). Fleet-gated `activate_codec_key` — [autumn-harvest/src/codec_rotation.rs:1](../../autumn-harvest/src/codec_rotation.rs), [docs/operations/codec-key-rotation.md](../../docs/operations/codec-key-rotation.md), `GET /admin/codec/rotation` (docs/openapi.json). TLA+ model: [formal/tla/CodecRotation.tla](../../formal/tla/CodecRotation.tla).
- **PII erasure — Present (terminal runs only).** `POST /workflows/{id}/erase-payloads` replaces payload fields with a tombstone (issue #495). It does not erase error text — [autumn-harvest/src/erase.rs:1](../../autumn-harvest/src/erase.rs), [docs/security-posture.md:911-916](../../docs/security-posture.md). The DB trigger `harvest_events_append_only_trg` (issue #1817) allows `event_data` rewrites only by `erase` or `codec_rotation` — [CLAUDE.md](../../CLAUDE.md) "Enforcement (issue #1817)".
- **Legal hold — Present.** `POST /workflows/{id}/legal-hold` and `/release` (issue #747), CLI `harvest legal-hold set|release` — [autumn-harvest-cli/src/lib.rs:1140-1151](../../autumn-harvest-cli/src/lib.rs), [autumn-harvest-cli/src/lib.rs:3035-3056](../../autumn-harvest-cli/src/lib.rs).
- **Data residency — Present (opt-in).** `shard_id` / `residency_key` pins a workflow and its descendant tree to a shard. A pin that cannot be honored fails closed with 503. `shard_id` "is not a capability", so an authorizer hook is needed to restrict callers (issue #697) — [docs/security-posture.md:649-677](../../docs/security-posture.md).
- **WASM module signing — Present (feature-gated).** Ed25519 publisher signature, checked before each run (issue #1838) — [docs/security-posture.md:829-856](../../docs/security-posture.md).
- **Supply chain — Present.** Daily advisory scan (cron `37 5 * * *`) — [.github/workflows/advisory-scan.yml:16-18](../../.github/workflows/advisory-scan.yml). SHA-pinned actions and Dependabot — [.github/dependabot.yml](../../.github/dependabot.yml). `cargo auditable`, a CycloneDX SBOM, Sigstore signing and GitHub attestations on releases — [.github/workflows/release.yml:3-22](../../.github/workflows/release.yml), [.github/workflows/release.yml:155-185](../../.github/workflows/release.yml).

### Inferences
- Harvest's encryption is comparable to Temporal's SDK codec model. It is weaker in one place: operator-facing columns stay in clear. Temporal's codec covers every payload because Temporal stores payloads only in history. Harvest's decode-on-read avoids the codec-server deployment that Temporal Web UI needs.
- Security is "safe if configured". Rate limiting, tokens, the hash chain, the codec and the authorizer are all opt-in. Fail-closed mutations is the main secure default.

### Gaps
- No SOC2, HIPAA or other compliance attestation is applicable or documented, as expected for a library. No SECURITY.md or vulnerability-disclosure policy file exists at the repo root (`ls -a` shows none).

---

## 4. Web UI (Vantage), management API and CLI

### Takeaway
**Present and broad, but server-rendered and less polished than competitors.** The embedded Vantage UI covers the workflow list (filters, including search attributes), the detail and history view, timeline, cancel, terminate, pause/resume, signal, reset, update, workers, DLQ, build routing, schedules (preview, runs, backfill, trigger-now, bulk pause), DAG list/detail with an SVG run graph, and admission gates. The ~158-path management API and a very large CLI (`harvest`, ~19k LOC) expose far more operator actions than the UI.

### Cited Findings
- **Vantage UI — Present.** Server-rendered, no external assets or CDN, mounted under the API prefix — [docs/vantage-ui.md:1-3](../../docs/vantage-ui.md). Routes: `/`, `/dags`, `/dags/{dag_name}`, DAG run retry, `/workflows`, `/workflows/{id}`, `/workflows/{id}/timeline`, cancel, **terminate**, pause, resume, signal, reset, trigger-update, `/workers`, `/dead-letters`, build-routing (set-policy, declare/revoke-compat, retire), schedules (bulk-pause/resume, pause, resume, delete, trigger-now, preview, runs, backfill), `/admin/gates` and lift — [autumn-harvest-plugin/src/ui.rs:718-795](../../autumn-harvest-plugin/src/ui.rs).
- **List search — Partial.** Filters: state, workflow name, started-after/before, exec-id prefix, plus `search_attr_key` / `search_attr_value` — [autumn-harvest-plugin/src/ui.rs:248-265](../../autumn-harvest-plugin/src/ui.rs), [docs/vantage-ui.md:7-21](../../docs/vantage-ui.md). The API supports richer search-attribute predicates (numeric range, set membership, AND, presence; issue #506) backed by a GIN index on JSONB — [docs/search-attributes.md:10-12](../../docs/search-attributes.md), [docs/search-attributes.md:165-190](../../docs/search-attributes.md). No SQL-like visibility query language (Temporal List Filter) was found.
- **History viewer — Present.** Paginated 100 events per page, jump-to-event, collapsible payloads, blocked-on panel, export link — [docs/vantage-ui.md:23-75](../../docs/vantage-ui.md).
- **DAG graph — Present.** Inline SVG `render_dag_run_graph_svg` — [autumn-harvest-plugin/src/ui.rs:7095](../../autumn-harvest-plugin/src/ui.rs), test at [autumn-harvest-plugin/src/ui.rs:16219-16232](../../autumn-harvest-plugin/src/ui.rs). Mermaid/DOT DAG export — [autumn-harvest/src/dag_export.rs:4-9](../../autumn-harvest/src/dag_export.rs). Chrome-trace/Perfetto DAG profile export — [autumn-harvest/src/trace_export.rs:1-4](../../autumn-harvest/src/trace_export.rs).
- **Doc drift:** [docs/vantage-ui.md:44](../../docs/vantage-ui.md) still says "Terminate — Disabled button (not yet available)", but the UI has a terminate route at [autumn-harvest-plugin/src/ui.rs:738](../../autumn-harvest-plugin/src/ui.rs) and the API has `POST /workflows/{id}/terminate` (docs/openapi.json). [docs/comparison.md:144](../../docs/comparison.md) and [docs/comparison.md:284-286](../../docs/comparison.md) still call the rendered DAG graph "Phase 4 work in progress", although the SVG renderer above exists.
- **Management API — Present.** OpenAPI 0.7.0, contract version 2, 158 paths — [docs/openapi.json](../../docs/openapi.json) `info`, [docs/openapi.md](../../docs/openapi.md). Manual actions in the API: start, signal, signal-with-start, update, update-with-start, query, cancel, terminate, pause/resume, reset, batch_reset, rerun, retry-now/fail-now for a single activity, erase-payloads, legal hold, triage annotation, batch start, batch operations, DLQ list/aggregate/replay/discard/redrive, completion-delivery redrive, worker drain/drain-preview, queue and activity pause/resume, circuit force-open/close, rate-limit and throttle overrides, admission gates, build-routing ramps, schedules CRUD/backfill/trigger, replay canary, live SSE event stream (`/executions/{exec_id}/events/stream`, `/workflows/{id}/stream`) (docs/openapi.json paths).
- **CLI — Present.** Top-level commands: health, preflight, shard, workflow, history, legal-hold, handoff, dag, schedule, dlq, completion-delivery, retention, queue, activity, concurrency, rate-limit, throttle, batch, audit, gate, token, usage, version-usage, version-gate-retirement, workflow-types, `tui`, worker, events, start-batch, canary, build, dr, partition, backup, det-check, debug, migrate, schema, `new` — [autumn-harvest-cli/src/lib.rs:1119-1517](../../autumn-harvest-cli/src/lib.rs). Workflow subcommands include list, summaries, get, stack, timeline, logs, awaitables, diagnose, tree, run-chain, replay-diagnosis, children, start, cancel, pause, resume, annotate, erase-payloads, retry-activity, fail-activity, reset, rerun, signal, query, update, handlers, batch-reset — [autumn-harvest-cli/src/lib.rs:2067-2590](../../autumn-harvest-cli/src/lib.rs). DB-direct commands that work with no app running: `migrate`, `backup verify`, `dr`, `partition` — [README.md:357-366](../../README.md), [autumn-harvest-cli/src/lib.rs:1379-1417](../../autumn-harvest-cli/src/lib.rs). Each request has a timeout of 30 s by default — [README.md:395-405](../../README.md).
- **TUI — Present.** `harvest tui`, plus a replay debugger TUI — [autumn-harvest-cli/src/lib.rs:1311](../../autumn-harvest-cli/src/lib.rs), `autumn-harvest-cli/src/tui.rs`, `autumn-harvest-cli/src/debug_tui.rs`.
- **Parity self-assessment:** "UI parity is incomplete ... Temporal, Inngest, Hatchet, DBOS (Conductor), and Restate all ship more mature UIs today" — [docs/comparison.md:277-286](../../docs/comparison.md).

### Inferences
- For operator actions, Harvest's API/CLI surface is at or above Temporal's `tctl`/`temporal` CLI breadth: pause, triage, legal hold, circuit control, admission gates and replay canary go beyond it. The gap is UI richness: no SPA, no visibility query language, no live-updating views in the UI (the UI has an "Auto-refresh" section, [docs/vantage-ui.md:281](../../docs/vantage-ui.md), while the API has SSE).

### Gaps
- Whether the Vantage DLQ page supports bulk replay/discard was not verified, only that the `/dead-letters` UI route exists ([autumn-harvest-plugin/src/ui.rs:748](../../autumn-harvest-plugin/src/ui.rs)).

---

## 5. Observability (metrics, tracing, logs, alerts, cost accounting)

### Takeaway
**Present and deep.** Harvest has about 80 bounded-cardinality `harvest.*` metrics via `metrics-rs`, a built-in Prometheus scrape endpoint, eight named OTel-compatible spans with W3C traceparent propagation through the task queue, replay-suppressed workflow logging with an opt-in durable per-execution log sink, a starter alert pack, an SLO multi-window burn-rate pack, a Grafana dashboard, a synthetic canary, scanner-liveness heartbeats, stall diagnosis, and usage reporting. There is **no native OTLP exporter**: a Collector recipe is the decided path.

### Cited Findings
- **Metrics — Present.** Built-in scrape endpoint behind the plugin `metrics` feature with `.with_metrics_scrape()` (issue #355) — [docs/telemetry.md:8-30](../../docs/telemetry.md). Metric catalogue (ADR-0001 §7) — [docs/telemetry.md:314](../../docs/telemetry.md). Custom per-workflow and per-activity metrics, replay-safe (issue #532) — [docs/telemetry.md:215-300](../../docs/telemetry.md). `GET /admin/metrics` returns Prometheus text — [README.md:418-421](../../README.md).
- **OTLP — Partial.** Uses a `metrics-exporter-otlp` bridge or the Collector recipe, which copies two metrics to OTel messaging semconv names (issue #1838) — [docs/telemetry.md:181-191](../../docs/telemetry.md), [docs/operations/otel-collector.md:1-30](../../docs/operations/otel-collector.md). Native OTLP was rejected because "The workspace has no `opentelemetry` crate" — [docs/adr/0004-security-extras.md:156-175](../../docs/adr/0004-security-extras.md).
- **Tracing — Present (opt-in bridge).** Eight spans (workflow execute/schedule, activity execute/schedule, signal send/deliver, timer fire, child start). Replay emits a new root span linked via `link.traceparent`. The user implements `TraceContextPropagator` to bridge to OTel. Without `telemetry(...)` the spans are no-ops — [README.md:1415-1488](../../README.md), [docs/adr/0001-otel-trace-contract.md](../../docs/adr/0001-otel-trace-contract.md).
- **Workflow logging with replay suppression — Present.** `ctx.log_info/warn/error` is a no-op while `ctx.is_replaying()`. An opt-in durable per-execution sink (issue #790) is readable via `GET /workflows/{id}/logs` and `harvest workflow logs` — [docs/workflow-logs.md:1-25](../../docs/workflow-logs.md), [docs/workflow-logs.md:140-146](../../docs/workflow-logs.md).
- **Alerts/runbooks — Present.** Starter pack `docs/alerts/starter-pack-v0.1.0.json` plus the SLO burn-rate pack, with three SLIs (workflow_task 99.9%, schedule_to_start 99% within 5 s, canary 99%) and `promtool` tests (issue #1816) — [docs/alerts/slo.md:1-30](../../docs/alerts/slo.md). 24 runbooks are in `docs/runbooks/` (e.g. `harvest-alerts.md`, `cross-region-failover.md`, `backup-restore.md`, `safe-deploy.md`). Grafana starter dashboard: `docs/dashboards/starter-pack-v0.1.0.json`.
- **Synthetic canary — Present.** Probes start → dispatch → activity → timer → complete (issue #796) — [autumn-harvest/src/canary.rs:1-4](../../autumn-harvest/src/canary.rs).
- **Control-loop liveness — Present** (issue #797) — [autumn-harvest/src/scanner_health.rs:1-4](../../autumn-harvest/src/scanner_health.rs). **Stall root-cause classifier — Present** (issue #809) — [autumn-harvest/src/stall_diagnosis.rs:1-4](../../autumn-harvest/src/stall_diagnosis.rs). Effective-config introspection (`GET /admin/config`, secret-free; issue #695) — [autumn-harvest/src/effective_config.rs:1-4](../../autumn-harvest/src/effective_config.rs).
- **Activity interceptors — Present** (issue #680) — [autumn-harvest/src/interceptor.rs:1-4](../../autumn-harvest/src/interceptor.rs).
- **Cost/usage accounting — Present (read-only).** `GET /admin/usage`, `harvest usage --group-by search_attr:tenant_id` (issue #596) — [autumn-harvest/src/usage.rs:1-4](../../autumn-harvest/src/usage.rs), [autumn-harvest-cli/src/lib.rs:1236-1257](../../autumn-harvest-cli/src/lib.rs). No billing or metering export exists. This is expected, since there is no cloud.

### Inferences
- Observability is a relative strength: it exceeds DBOS OSS and Restate on runbook and alert packaging, and is comparable to Temporal self-hosted. Harvest lacks a hosted metrics endpoint (Temporal Cloud OpenMetrics) and a native OTel exporter (Hatchet).

### Gaps
- I did not re-verify whether DB-pool saturation and poller-count metrics were added after `937b655`. The prior notes found them absent.

---

## 6. Scale (sharding, partitioning, benchmarks, history limits, autoscaling signals, backpressure)

### Takeaway
Harvest has a lot of scale-out machinery: multi-database sharding, cross-shard children, shard rebalancing, partitioned events, optional Redis dispatch, an adaptive slot tuner and concurrency limits, load shedding, admission gates, default history caps, and a KEDA/HPA scaling signal. **Measured throughput is low.** The project's own headline is 23.73 workflows/sec on one shard (4 cores). Its own pre-registered single-box assay against Temporal on the same Postgres measured **5.47 vs 43.29 workflows/sec (Temporal 7.91x faster)**.

### Cited Findings
- **Sharding — Present.** `ShardId` is encoded in the first two bytes of `ExecutionId`, so shard lookup is O(1) with no directory. Shards are added for new workflows only, with a readiness gate — [README.md:1139-1170](../../README.md). There is no cross-shard transaction. Cross-shard children are opt-in (issue #956). Rebalancing moves **quiescent** workflows only (issue #964); auto-resume after a stalled cutover is #1839 — [docs/comparison.md:292-302](../../docs/comparison.md), [docs/sharding.md:518-914](../../docs/sharding.md), [autumn-harvest/src/shard_rebalance.rs:1](../../autumn-harvest/src/shard_rebalance.rs), [autumn-harvest/src/rebalance_resume.rs:1-2](../../autumn-harvest/src/rebalance_resume.rs).
- **Partitioned `harvest_events` — Present (opt-in).** Retention drops partitions instead of deleting rows (issue #958). Managed with `harvest partition status|plan|enable|maintain|disable` — [docs/partitioned-events.md:1-25](../../docs/partitioned-events.md), [autumn-harvest-cli/src/lib.rs:278-420](../../autumn-harvest-cli/src/lib.rs).
- **Benchmarks — Present (self only, plus one head-to-head).** v0.6.0 on 4 CPUs: throughput 23.73 / 35.70 / 33.58 wf/s at 1 / 2 / 4 shards; dispatch p50 41-58 ms; replay 9.2M events/s — [docs/benchmarks.md:37-50](../../docs/benchmarks.md). Harness: `benchmarks/run.sh`, `benchmarks/docker-compose.yml`. Assay #11: `harvest_pg` 5.47 vs `temporal_go` 43.29 wf/s on the same 4-core box and the same PostgreSQL 16.13 server. Verdict: "KILL on L1, decisively and against harvest, by 7.91x" — [docs/assays/0011-harvest-vs-temporal-single-box.md:1](../../docs/assays/0011-harvest-vs-temporal-single-box.md), [docs/assays/0011-harvest-vs-temporal-single-box.md:53-56](../../docs/assays/0011-harvest-vs-temporal-single-box.md), [docs/assays/0011-harvest-vs-temporal-single-box.md:116-136](../../docs/assays/0011-harvest-vs-temporal-single-box.md). The assay attributes part of the gap to a known backlog-depth collapse found in assay #10 (post-hoc diagnostic, same file, after line 138).
- **Comparison-page conflict:** [docs/comparison.md:303-314](../../docs/comparison.md) says "No cell on this page claims a throughput or latency comparison against another engine". The repo now holds assay #11, which is such a comparison. The comparison page does not cite it.
- **History size limits — Present (default on since 0.7).** Hard caps of 50,000 events and 50 MiB (issue #1804); continue-as-new soft threshold of 10,000; bloat warning at 20.48% — [autumn-harvest/src/context.rs:49-85](../../autumn-harvest/src/context.rs), [docs/runbooks/history-ceiling.md:20](../../docs/runbooks/history-ceiling.md). Large-payload claim-check offload (issue #524) — [autumn-harvest/src/payload_store.rs:1-4](../../autumn-harvest/src/payload_store.rs).
- **Autoscaling signal — Present.** `GET /admin/queues/scaling` (JSON or `?format=prometheus`) "for KEDA/HPA autoscalers" — [autumn-harvest-plugin/src/api.rs:36861-36880](../../autumn-harvest-plugin/src/api.rs). No bundled KEDA ScaledObject manifest exists.
- **Backpressure — Present.** Admission gates (#377) — [autumn-harvest/src/admission_gate.rs:1-4](../../autumn-harvest/src/admission_gate.rs). Backlog-age load shedding (#1794) — [autumn-harvest/src/load_shed.rs:1-4](../../autumn-harvest/src/load_shed.rs). Adaptive concurrency per activity type (#1836) — [autumn-harvest/src/adaptive_limit.rs:1-4](../../autumn-harvest/src/adaptive_limit.rs). Adaptive slot tuner (#548) — [autumn-harvest/src/slot_tuner.rs:1-4](../../autumn-harvest/src/slot_tuner.rs). Start throttle and debounce; per-activity rate limits; circuit breakers (`/admin/circuits`, docs/openapi.json).
- **Persistence ceiling:** "Postgres-only, no pluggable persistence ... harvest cannot swap in Cassandra" — [docs/comparison.md:272-276](../../docs/comparison.md).

### Inferences
- Harvest scales by adding Postgres shards, but per-shard throughput is an order of magnitude below Temporal on equal hardware, per its own assay. For high-volume comparisons this is the key quantitative weakness. The honest reporting is a governance strength.

### Gaps
- No published benchmark exists for v0.7.0. The latest headline is v0.6.0 ([docs/benchmarks.md:37](../../docs/benchmarks.md)). No benchmark exists against Restate, DBOS, Hatchet or Inngest.

---

## 7. Data lifecycle (retention, archival, history export, erasure, codec rotation)

### Takeaway
**Present, but archival is a hook rather than a built-in integration.** Retention is opt-in, with per-workflow-type overrides, a dry run and run-now. Archival is a user-implemented `HistoryArchiver` trait with zero-loss semantics; there is no built-in S3/GCS archiver and no read-back of archived history. JSON and Mermaid history export, batch and sampled export, erasure, legal hold and codec rotation are all present.

### Cited Findings
- **Retention — Present (opt-in).** "Retention janitor (opt-in) ... Running workflows are unaffected" — [README.md:323-327](../../README.md). Config: `max_age_secs`, per-workflow `overrides` (#737), `tick_interval_secs`, `batch_size`, `dry_run`, `audit_retention_days` — [autumn-harvest/src/retention.rs:333-356](../../autumn-harvest/src/retention.rs). Since 0.7.0 the janitor also deletes finished task-queue rows after 7 days (#1811) — [CHANGELOG.md:18](../../CHANGELOG.md).
- **Archival — Partial.** A `HistoryArchiver` hook ships `HistoryExportDocument` before deletion. If the hook fails, the row is not deleted ("Zero-Loss Guarantee"). The S3 example is "mocked" user code with `aws-sdk-s3` — [docs/archival.md:1-50](../../docs/archival.md), [docs/archival.md:114-128](../../docs/archival.md). There is no first-party S3/GCS/Azure archiver, and no import or visibility of archived histories (grep for `rehydrat`/history import found nothing).
- **History export — Present.** `GET /workflows/{id}/history/export` (full and redacted JSON modes), `/admin/history/exports`, `/admin/history/export-sample`; CLI `harvest history export|export-batch|export-sample` — [autumn-harvest-cli/src/lib.rs:1723-1815](../../autumn-harvest-cli/src/lib.rs), [autumn-harvest/src/history_export.rs:1](../../autumn-harvest/src/history_export.rs), [docs/security-posture.md:901-904](../../docs/security-posture.md).
- **Erasure, legal hold, codec rotation:** see §3.

### Inferences
- Harvest matches Temporal's archival model (a provider hook) but without first-party providers or archived-history visibility. Temporal ships S3 and GCS archivers and can show archived histories in its UI.

### Gaps
- Not verified: whether retention honours legal hold in all paths. The legal-hold routes exist, but the retention code path was not read.

---

## 8. HA / DR (multi-region, replication, failover, backup/restore)

### Takeaway
**Partial.** Harvest supports multi-replica HA within a region. For cross-region DR, replication is stock Postgres, and Harvest adds a write-authority fence, an RPO metric, verification tooling and an operator-run failover. There is **no automatic failover, no active-active, and no zero-RPO**. Backup is the DBA's Postgres tooling, plus a read-only `harvest backup verify` drill.

### Cited Findings
- "Failover is **operator-initiated**. There is no automatic promotion, no active-active writing, and no zero-RPO mode." Harvest ships the fence, the measured RPO (`harvest.replication.lag_seconds{shard}`) and verification. It does not ship replication or unattended failover (issue #954) — [docs/cross-region-dr.md:1-35](../../docs/cross-region-dr.md). The runbook is "the only supported path" — [docs/runbooks/cross-region-failover.md:5](../../docs/runbooks/cross-region-failover.md).
- Fence implementation — [autumn-harvest/src/replication.rs:1-4](../../autumn-harvest/src/replication.rs). CLI `harvest dr status|fence|promote` talks to shard DBs directly — [autumn-harvest-cli/src/lib.rs:141-260](../../autumn-harvest-cli/src/lib.rs), [autumn-harvest-cli/src/lib.rs:1373-1384](../../autumn-harvest-cli/src/lib.rs).
- **Backup/restore — Partial (verify only).** `harvest backup verify` is read-only against scratch DBs, with exit codes 0, 1 (incoherent) and 2 (undetermined) (issue #943) — [autumn-harvest/src/backup_verify.rs:1-4](../../autumn-harvest/src/backup_verify.rs), [autumn-harvest-cli/src/lib.rs:1395-1409](../../autumn-harvest-cli/src/lib.rs). Runbook: [docs/runbooks/backup-restore.md](../../docs/runbooks/backup-restore.md).
- **In-region HA — Present.** Multi-replica schedule claim protocol — [docs/runbooks/ha-deployment.md:1-40](../../docs/runbooks/ha-deployment.md). Worker drain protocol — `POST /workers/{id}/drain`, `harvest worker drain` (docs/openapi.json).

### Inferences
- Harvest is behind Temporal Cloud (multi-region automatic failover, 99.99%), Temporal self-hosted (Global Namespaces) and the managed offerings. It is comparable to, or ahead of, DBOS OSS and Hatchet OSS, which rely entirely on database HA.

### Gaps
- None material.

---

## 9. Upgrades, migrations and compatibility policy

### Takeaway
**Present.** There is a written N/N-1 rolling-deploy contract with expand-then-contract migrations, a CI lint for migration lock safety, `harvest migrate status --check`, per-release upgrade guides and a mixed-version smoke job. The project is pre-1.0 with breaking changes in minor releases, and has **no LTS**.

### Cited Findings
- N and N-1 may run together on one DB; "Harvest supports no other pair. Do not run N-2 with N." Expand-only migrations in N; contract migrations at least one minor later; no `down.sql` rollbacks; event-format rule "N reads N-1 / N-1 reads N" (issue #1828) — [docs/upgrading/README.md:1-75](../../docs/upgrading/README.md).
- Lock-safe online migrations, gated by `migration_lock_safety` in CI (issue #1810) — [docs/upgrading/online-migrations.md](../../docs/upgrading/online-migrations.md), [CLAUDE.md](../../CLAUDE.md) "Bound every lock on a hot table".
- Per-release guides: 0.5.0, 0.6.0, 0.7.0 — [docs/upgrading/README.md:6-10](../../docs/upgrading/README.md). `harvest migrate` for split/external DBs (issue #1240) — [autumn-harvest-cli/src/lib.rs:1471-1478](../../autumn-harvest-cli/src/lib.rs).
- "API stability: pre-1.0. Breaking changes happen in minor versions" — [README.md:1122-1123](../../README.md). 0.7.0 shipped breaking defaults: fail-closed 401, a 10-minute default activity timeout, and build-id claim changes — [CHANGELOG.md:18](../../CHANGELOG.md). No LTS or support window was found (grep `LTS|long-term|support window`).
- Workflow-code versioning tooling (for comparison context): build-id routing and ramp, `harvest build ramp`, version-gate usage and retirement checks, workflow-type reachability, payload schema contract gate (`harvest schema check`, issue #794), replay canary — [autumn-harvest-cli/src/lib.rs:1258-1310](../../autumn-harvest-cli/src/lib.rs), [autumn-harvest-cli/src/lib.rs:1479-1492](../../autumn-harvest-cli/src/lib.rs).

### Inferences
- The compatibility contract is explicit and enforced in CI, more rigorous than most young engines. The 0.x status with breaking minor releases is the main enterprise blocker.

### Gaps
- None.

---

## 10. Developer experience (local dev, docs, examples, templates, IDE, skills, formal/verify/fuzz)

### Takeaway
**Strong.** DX includes a zero-setup `cargo dev` (an ephemeral Postgres, worker, API and UI), a `harvest new` project scaffold, a 13-chapter getting-started guide, 7 examples, an external docs site (which lags at 0.6.0), an agent skill (stale at 0.5.0), compile-time determinism guardrails, a `det-check` static analyzer, a MIR-level verifier, TLA+ models, Kani proofs, fuzzing, deterministic simulation, and a time-travel replay debugger. **Rust only**: there is no SDK in another language.

### Cited Findings
- **Local dev server — Present.** `cargo dev` starts an ephemeral PostgreSQL, applies migrations, runs a worker, and serves the API and Vantage. It is development-only (issue #525) — [README.md:30-50](../../README.md), [.cargo/config.toml:1-20](../../.cargo/config.toml).
- **Templates — Present.** `harvest new <name>` emits a runnable crate with compose Postgres and README (issue #692) — [autumn-harvest-cli/src/lib.rs:1485-1500](../../autumn-harvest-cli/src/lib.rs).
- **Docs — Present.** `docs/getting-started/` has 13 chapters plus activities and standalone-axum guides. There are a Temporal migration guide ([docs/migrating-from-temporal.md](../../docs/migrating-from-temporal.md)) and a comparison page. A hosted docs site exists via the Autumn docs site. Its `list_autumn_docs` reports `harvest_version: "0.6.0"` with a 14-guide "Harvest" group mirroring the getting-started chapters (Autumn_Docs MCP, queried 2026-10-07), so the hosted site **lags the 0.7.0 code**.
- **Examples — Present.** `billing-autumn-web`, `claude-agent-daemon`, `quickstart`, `saga-choreography`, `standalone-quickstart`, `standalone-runner`, `typescript-client` (`ls examples/`).
- **Skills folder — Present, stale.** `skills/SKILL.md` (458 lines) plus `references/architecture.md`. It declares "**Version**: 0.5.0" — [skills/SKILL.md:18](../../skills/SKILL.md), while the workspace is 0.7.0 ([Cargo.toml](../../Cargo.toml) `[workspace.package] version = "0.7.0"`).
- **Determinism tooling — Present.** Guardrails HVG001–HVG011 and `det_check` — [docs/comparison.md:111](../../docs/comparison.md), [autumn-harvest/src/guardrail.rs:1-4](../../autumn-harvest/src/guardrail.rs). `harvest det-check` (issue #778) — [autumn-harvest-cli/src/lib.rs:1410-1430](../../autumn-harvest-cli/src/lib.rs). `autumn-harvest-verify` is a MIR-level taint verifier ("R&D prototype", issue #962) with verdicts proven-deterministic / nondeterminism-found / unknown — [autumn-harvest-verify/README.md:1-20](../../autumn-harvest-verify/README.md).
- **Formal methods — Present.** TLA+ models ActivityClaim, WorkflowTaskClaim and CodecRotation (`formal/tla/`) plus Kani proofs (issue #1819, commit `4c1e5f9`). Fuzz targets: replay, event deserialization, det-check source, failure signature, URL validation (`fuzz/fuzz_targets/`), with a nightly fuzz workflow ([.github/workflows/fuzz-nightly.yml](../../.github/workflows/fuzz-nightly.yml)). DST and proptest nightly; chaos nightly with a watchdog ([.github/workflows/chaos.yml:23-25](../../.github/workflows/chaos.yml)).
- **Debugging — Present.** Time-travel replay debugger (`debugger` feature, issue #949) — [autumn-harvest/src/debugger.rs:1](../../autumn-harvest/src/debugger.rs). `harvest debug replay|diff` — [autumn-harvest-cli/src/lib.rs:1068-1118](../../autumn-harvest-cli/src/lib.rs). `WorkflowReplayer` CI harness — [README.md:1233-1260](../../README.md).
- **IDE support — Absent as a dedicated tool.** No LSP, VS Code or rust-analyzer integration was found (grep across `docs/` and `README.md`). Guardrails are proc-macro compile errors, so they surface in any rust-analyzer IDE. That is an inference, not a documented feature.
- **Polyglot SDKs — Absent.** "Rust only ... there is no non-Rust worker/author SDK" — [docs/comparison.md:89](../../docs/comparison.md), and ADR 0002 rules out polyglot workers — [docs/adr/0002-rust-native-execution-boundary.md:44-50](../../docs/adr/0002-rust-native-execution-boundary.md). A typed TypeScript **management-API client** exists, generated from OpenAPI (issue #1616). It is attached to GitHub releases after 0.6.0 and "not on the npm registry" — [clients/typescript/README.md:1-20](../../clients/typescript/README.md). No Python client was found.
- **Doc conflict:** [docs/comparison.md:48](../../docs/comparison.md), [docs/comparison.md:89](../../docs/comparison.md) and [docs/comparison.md:258-261](../../docs/comparison.md) list a TS activity-worker SDK (#959) and TS+Python clients (#955) as "planned". ADR 0002 (accepted 2026-05-03) rejects polyglot worker SDKs, and the TS client now ships via #1616.

### Inferences
- Harvest's determinism and verification tooling is the deepest in the comparison set. Its DX ceiling is the Rust-only authoring language. Every competitor except Restate's Rust SDK covers TS/Python/Go.

### Gaps
- None.

---

## 11. Integrations (brokers, HTTP/webhooks, OTel, frameworks, AI/LLM/MCP)

### Takeaway
**Partial.** Present: Kafka and SQS ingress connectors, inbound webhooks (signature verification delegated to autumn-web), outbound HMAC-signed completion callbacks, MCP tool exposure, autumn-web and Axum integration, and an OTel bridge. Absent: NATS, RabbitMQ, Pub/Sub, Kinesis and EventBridge connectors, and any first-party LLM/AI-agent SDK. There is one Claude-agent example.

### Cited Findings
- **Broker connectors — Present (plugin features, plugin path only).** `connectors` (broker-agnostic, with `MockSource`), `kafka` (rdkafka) and `sqs`, with idempotent redelivery, ack ordering, poison isolation and backpressure. A CI test keeps broker clients out of the core crate — [docs/getting-started/13-broker-connectors.md:1-40](../../docs/getting-started/13-broker-connectors.md). Plugin features: `webhooks`, `mcp`, `metrics`, `connectors`, `kafka`, `sqs`, `aws-kms`, `dev-runtime`, `dev-runtime-managed`, `redis` — [autumn-harvest-plugin/Cargo.toml](../../autumn-harvest-plugin/Cargo.toml) `[features]`. Connector modules exist for kafka and sqs only — `autumn-harvest-plugin/src/connector/` (no nats/rabbitmq/pubsub module).
- **Inbound webhooks — Present (opt-in `webhooks` feature).** `#[webhook]` maps an already-verified delivery to an idempotent workflow start. Signature verification (Stripe, GitHub, Slack, generic HMAC) is autumn-web's `SignedWebhook` — [docs/getting-started/12-webhooks.md:1-30](../../docs/getting-started/12-webhooks.md).
- **Outbound completion callbacks — Present.** Durable, HMAC-signed, SSRF-guarded POST of terminal results (issue #605) — [CHANGELOG.md:140](../../CHANGELOG.md), [docs/completion-callbacks.md](../../docs/completion-callbacks.md). Completion triggers (`/admin/completion-triggers`) — [docs/completion-triggers.md](../../docs/completion-triggers.md).
- **MCP / AI — Present (opt-in).** `#[workflow(mcp)]` exposes a workflow as correlated start/watch/steer MCP tools over autumn-web's Streamable-HTTP MCP layer (issue #597). It needs autumn-web — [docs/mcp-tools.md:1-30](../../docs/mcp-tools.md), [docs/embedding.md:533-534](../../docs/embedding.md). Example `examples/claude-agent-daemon` runs Claude agent sessions as durable workflows on SQLite — [examples/claude-agent-daemon/README.md:1-15](../../examples/claude-agent-daemon/README.md). No LLM-call step primitive, token-streaming, or AI SDK integration was found beyond this.
- **Frameworks — Present.** autumn-web (primary) and plain Axum (`HarvestEmbedding`) — [docs/embedding.md:30-37](../../docs/embedding.md). No Actix, Rocket or other framework adapters.
- **Transactional outbox start / transactional activities** — [docs/transactional-start.md](../../docs/transactional-start.md), [docs/transactional-activities.md](../../docs/transactional-activities.md) (exist; not read in depth).

### Inferences
- Integrations are thinner than Inngest's event platform, Trigger.dev's integration catalog, Step Functions' 200+ AWS service integrations, Orkes Conductor's system tasks (HTTP, Kafka, LLM tasks), and Camunda connectors. Harvest has no generic "HTTP task" or "LLM task" step type. Users write Rust activities.

### Gaps
- No catalogue of third-party integrations exists. None was found.

---

## 12. Governance (license, community, release cadence, changelog)

### Takeaway
The project is permissively licensed (MIT OR Apache-2.0) and releases roughly monthly (0.1.0 in April 2026 to 0.7.0 on 2026-10-05). The changelog and fragments are disciplined and releases are signed. Gaps: **no LICENSE file in the repo**, no CONTRIBUTING, CODE_OF_CONDUCT or SECURITY files, ADR numbering collisions, and several stale self-descriptions.

### Cited Findings
- License: `license = "MIT OR Apache-2.0"` — [Cargo.toml](../../Cargo.toml) `[workspace.package]`; [README.md:1489-1491](../../README.md). A repo-wide `find -iname 'LICENSE*'` returned **no files**, so the license texts are not in the tree.
- Release history: 0.1.0 and 0.1.1 on 2026-04-19, 0.3.0 on 2026-05-13, 0.4.0 on 2026-06-16, 0.5.0 on 2026-07-21, 0.6.0 on 2026-08-26, 0.7.0 on 2026-10-05 — [CHANGELOG.md:10-739](../../CHANGELOG.md). The changelog follows Keep a Changelog and SemVer — [CHANGELOG.md:6](../../CHANGELOG.md). `RELEASE_NOTES.md` is a pointer, and notes are generated by git-cliff at release time — [RELEASE_NOTES.md:1-9](../../RELEASE_NOTES.md). `docs/changelog.d/` holds 219 fragment files (`ls | wc -l`). `docs/shipped-work.md` (9,097 lines) records about 170 "Phase" entries (`grep -c Phase`).
- Community files: none of CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, GOVERNANCE or CODEOWNERS exist at the root or in `.github/` (`ls -a`; `.github` holds `ci`, `dependabot.yml`, `workflows`). AI-assisted review workflows exist: `claude-code-review.yml`, `claude.yml` (`.github/workflows/`).
- Project maturity self-assessment: "Younger project, smaller ecosystem ... pre-1.0" — [docs/comparison.md:287-291](../../docs/comparison.md).
- ADR numbering: four ADRs share number 0004 (`deterministic-simulation-testing`, `partitioned-duplicate-append-detection`, `security-extras`, `tenant-isolation-cells`) — `ls docs/adr/`.
- No explicit roadmap file was found. Planned work is referenced inline by issue numbers (e.g. [docs/comparison.md:255-261](../../docs/comparison.md)). No `TODO(#issue)` / `FIXME` comments exist in core, plugin or CLI sources (grep returned nothing), consistent with the comment-hygiene gate in [CLAUDE.md](../../CLAUDE.md).

### Inferences
- The cadence is fast, and release-supply-chain hygiene is good: signed, SBOM, attested. Community and governance scaffolding is minimal, and the comparison page, the skill and the hosted docs lag the code. This matters to evaluators who read docs, not source.

### Gaps
- Contributor count, issue volume and adoption metrics are not determinable from the repo. The commit history is shallow (50 commits visible). Issue numbers up to about #1961 suggest heavy activity, but this was not verified via GitHub.

---

## Summary matrix (for the report writer)

| Capability | Status | Key evidence |
|---|---|---|
| Embedded library deployment | Present | docs/embedding.md:30-37 |
| Standalone server / single binary | Absent | release.yml:124-138 (CLI only) |
| Helm / K8s operator / container image | Absent (probes only) | find returned none; docs/operations/kubernetes-probes.md |
| Serverless / HTTP-push workers | Absent (ADR 0002) | docs/adr/0002...:44-60 |
| Managed cloud | Absent by design | docs/comparison.md:166 |
| Non-Postgres backends | Partial (SQLite subset; Redis dispatch only) | docs/sqlite-backend.md §11; README.md:1196-1231 |
| Namespaces | Absent | docs/security-posture.md:703-704 |
| Per-tenant quotas / cells | Present (opt-in, shard-local) | quota.rs:1-30; ADR 0004 cells |
| Tenant-scoped retention | Absent | retention.rs:333-345 |
| Built-in authN / OIDC / mTLS | Absent (delegated) | security-posture.md:7-12 |
| API tokens + scopes | Present (opt-in) | security-posture.md:262-345 |
| RBAC | Partial (read/mutate/admin + deny hook) | security-posture.md:160-170, 392-450 |
| Audit log + export + hash chain | Present (chain opt-in) | audit.rs, audit_export.rs, audit_chain.rs |
| Payload encryption + KMS | Present (opt-in; AWS KMS only; columns and errors in clear) | security-posture.md:858-1013 |
| Codec server | Absent (server-side decode-on-read instead) | read-path-decode.md:1-30 |
| PII erasure / legal hold | Present | erase.rs; cli lib.rs:3035-3056 |
| Data residency pinning | Present | security-posture.md:649-677 |
| API rate limiting | Present (opt-in) | security-posture.md:513-545 |
| Web UI | Present (server-rendered; less mature) | ui.rs:718-795; comparison.md:277-286 |
| Visibility query language | Partial (search-attr predicates; no SQL-like language) | search-attributes.md:165-190 |
| CLI | Present (very broad) | cli lib.rs:1119-1517 |
| Prometheus metrics | Present | telemetry.md:8-30 |
| Native OTLP | Absent (Collector recipe) | adr/0004-security-extras.md:156-175 |
| Tracing propagation | Present (user-supplied bridge) | README.md:1415-1488 |
| Replay-safe workflow logs | Present (durable sink opt-in) | workflow-logs.md:140-146 |
| Alerts / SLO / dashboards / runbooks | Present | docs/alerts/slo.md; docs/runbooks/ |
| Usage / chargeback | Present (read-only) | usage.rs:1-4 |
| Sharding / rebalancing | Present (quiescent-only rebalance) | sharding.md; comparison.md:292-302 |
| Throughput vs Temporal | Weak (5.47 vs 43.29 wf/s, own assay) | assays/0011...:53-56 |
| History limits | Present (50k events / 50 MiB default) | context.rs:49-85 |
| Autoscaling signal | Present (KEDA/HPA endpoint) | api.rs:36861 |
| Backpressure / load shed | Present | load_shed.rs, admission_gate.rs, adaptive_limit.rs |
| Retention | Present (opt-in) | retention.rs:333-356 |
| Archival to S3/GCS | Partial (hook only; no providers, no read-back) | archival.md:1-50 |
| Cross-region DR | Partial (manual fenced failover; no auto, no active-active) | cross-region-dr.md:1-35 |
| Backup verify | Present | backup_verify.rs |
| Rolling-upgrade contract | Present (N/N-1) | upgrading/README.md |
| LTS / 1.0 stability | Absent (0.x) | README.md:1122-1123 |
| Local dev server | Present | README.md:30-50 |
| Polyglot SDKs | Absent (TS management client only) | comparison.md:89; clients/typescript |
| Formal / fuzz / DST / verifier | Present | formal/tla; fuzz/; autumn-harvest-verify |
| Kafka / SQS connectors | Present (plugin path) | getting-started/13 |
| NATS / RabbitMQ / Pub/Sub | Absent | connector/ has kafka, sqs only |
| Inbound webhooks / outbound callbacks | Present | getting-started/12; completion-callbacks.md |
| MCP tools | Present (autumn-web only) | mcp-tools.md |
| License | MIT OR Apache-2.0 (no LICENSE file in tree) | Cargo.toml; find |
