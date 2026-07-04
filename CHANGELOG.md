# Changelog

All notable changes to autumn-harvest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Expose `#[workflow]`s as durable MCP tools (issue #597). A single `#[workflow(mcp)]` opt-in (plus `HarvestPlugin::mcp_tools()` / `mcp_tools_at(prefix)` on the app side, under the new plugin `mcp` cargo feature) projects a workflow onto autumn-web 0.5's MCP layer as a correlated tool set: `start_{wf}` (returns the durable `execution_id` handle immediately — the work survives daemon restarts and does not require the agent to stay connected), `{wf}_status` (state / `current_details` progress / output / error by handle), `signal_{wf}` (async signal delivery, `Idempotency-Key` supported), `{wf}_watch` (streaming progress over MCP `notifications/progress`, driven by the shard's LISTEN/NOTIFY `harvest_events` channel — no polling), and one synchronous `{wf}_update_{name}` tool per `#[update(workflow = "…", mcp)]` handler. The `start_{wf}` tool's `inputSchema` is derived from the workflow's published `input_schema` (issue #373) via a process-global schema map drained by a static `ApiDoc::register_schemas` hook — no second, hand-maintained schema — and start input is validated against it at the tool edge before any storage access. Handlers delegate to the existing management-API primitives; every handle-taking tool verifies the execution belongs to its workflow (uniform 404, no cross-workflow existence oracle). Effectful-by-default safety: exposure is strictly opt-in, the mutating tools are never part of autumn-web's read-only `expose_all_as_mcp` hatch, tool calls replay the caller's credentials through the real handler pipeline (gate the endpoint with `secure_mcp`), and routes fail closed before the runtime starts. Known inherited gap: autumn-web 0.5 derives tool `annotations` from the HTTP verb, so mutating tools carry `readOnlyHint: false` but a literal `destructiveHint: true` is only emitted for DELETE routes — needs an autumn-web change to annotate POST tools. Gap-fills shipped along the way: `HarvestPlugin::updates()`/`queries()` now forward to the builder (previously `#[update]`/`#[query]` handlers could not be registered through the plugin at all), `HarvestBuilder` gains pre-build `workflow_infos()`/`update_handlers()`/`query_handlers()` accessors, and `RegisteredWorkflowRecord` (`GET /workflows/registered`) surfaces the new `mcp` flag (additive response field). New module `autumn-harvest-plugin/src/mcp_tools.rs` (pure descriptor layer + global schema map + route/handler layer), example `autumn-harvest-plugin/examples/mcp_tools_quickstart.rs`, doc page `docs/mcp-tools.md`. Determinism preserved by construction: exposure is an HTTP-edge concern — the `mcp` flag is never consulted by core execution. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Harden the MCP tool exposure (issue #597) against a code review pass. The most severe finding: the generated tool routes are registered via `AppBuilder::routes(...)`, not `nest()`, so a mutating tool's own HTTP path bypassed both `HarvestPlugin::api_with_auth`'s middleware (which only wrapped the `nest()`-mounted management router) and autumn-web's `secure_mcp` (which only gates the `/mcp` JSON-RPC envelope) — fixed by threading a new `McpToolMiddlewareFn` through the route builder so `api_with_auth`'s configured layer now wraps every generated tool route's `MethodRouter` directly, not just the management API. Also fixed: `{wf}_status`/`{wf}_watch` now transparently follow a `ContinuedAsNew` successor chain (reusing the `/result` endpoint's chain-walk, issue #527, extracted into `resolve_terminal_workflow_execution`) instead of reporting the sealed predecessor's dead-end sentinel with null output/error; `{wf}_watch`'s SSE loop now sends an `event: error` frame on a lost LISTEN/NOTIFY connection instead of silently ending the stream, and detects a client that disconnected during a quiet stretch instead of leaking the background polling task until the watched execution eventually terminates; the fallback `inputSchema` for a workflow/update with no published schema no longer asserts `"type": "object"` (would reject valid array/scalar input from a multi-param or non-object-param workflow); `start_tool`'s redundant, closure-captured-schema pre-validation (which also skipped the audit record the delegated `start_workflow` writes on rejection) was removed in favor of the single, already-existing check; the process-global MCP schema map now warns on a genuinely divergent same-name re-registration (an inherent limitation of autumn-web's bare-fn-pointer schema hook, not fully fixable without an upstream change, but no longer silent); and the triplicated `mcp`/`allow_nondeterministic_apis` bare-flag-or-`=bool` attribute parsing across `workflow.rs`/`update.rs` was consolidated into one `autumn-harvest-macros::attr_util::parse_bool_flag` helper. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Fix a broken CI lint job and two more MCP tool gaps found by automated PR review (issue #597). `StartWorkflowRequest::from_input`/`AdmitUpdateRequest::new` are only reachable from the `mcp`-gated `mcp_tools.rs` module, so the plain (no-`mcp`-feature) clippy lint job saw zero callers and failed on `-D warnings`; fixed with a feature-conditional `allow(dead_code)`. An `#[workflow(mcp)]` workflow that also carries a `debounce`/`batch` policy could return `202 Accepted` with no `execution_id` from `start_{wf}`, breaking every other generated tool's "durable handle immediately" contract — such workflows are now excluded from MCP exposure entirely (warned, not silently broken). `signal_{wf}`/`{wf}_update_{name}` now resolve a `ContinuedAsNew` chain the same way `{wf}_status`/`{wf}_watch` already did, so an agent holding the original durable handle can still signal or update a workflow after it continues-as-new instead of the delivery failing against the sealed predecessor. No new `WorkflowEvent` variant, no migration.

- Warn loudly when MCP tool routes have no route-level auth configured (issue #597). The prior auth-bypass fix only closes the gap for embedders who call `HarvestPlugin::api_with_auth`; an embedder who instead relies solely on autumn-web's `secure_mcp(...)` (which only gates the `/mcp` JSON-RPC envelope, not a route's own direct HTTP path) still has the generated `start_{wf}`/`signal_{wf}`/update routes reachable unauthenticated — and `HarvestPlugin` has no way to detect `secure_mcp`, since it's configured on the outer `AppBuilder` after `Plugin::build()` returns. `Plugin::build()` now logs a `tracing::warn!` naming the exact risk whenever `mcp_tools()` is enabled with no `api_with_auth` middleware configured. No new `WorkflowEvent` variant, no migration.

- Fix two more MCP tool gaps found by automated PR review (issue #597). `admit_update` (the standalone update-admit handler shared by the plain HTTP route and the new `{wf}_update_{name}` MCP tool) never consulted a registered update's validator before durably admitting it — an invalid payload became durable history instead of being rejected at the edge, unlike the sibling `update_with_start` path which already validates first. Fixed by running the same validator check (scoped by workflow name + update name, matching `update_with_start`'s `422` response shape) before admission. The MCP quickstart example previously told production users to add only `secure_mcp(...)`, but the generated tool routes are also reachable at their own direct HTTP paths under `/api/harvest/mcp`, which `secure_mcp` doesn't cover — the example now configures both `api_with_auth` and `secure_mcp`. No new `WorkflowEvent` variant, no migration.

- Exclude MCP workflows whose generated tool names collide with a sibling's (issue #597). Operation ids are derived purely from a workflow's (and its updates') name via fixed templates, so two differently-named workflows can still collide — e.g. workflow `invoice_status`'s start tool (`start_invoice_status`) is the same string as workflow `start_invoice`'s status tool. autumn-web keeps only the first registration on a collision and silently drops the rest, so an unchecked collision would make one exposed workflow quietly lose a tool from `tools/list` while its direct route still worked. `collect_descriptors` now tracks generated operation ids across the whole workflow list and excludes (with a warning) any workflow whose tool set would collide with an already-accepted one — first-seen wins, the whole colliding workflow is dropped rather than partially exposed. No new `WorkflowEvent` variant, no migration.

- Fix a no-DB test that asserted the wrong thing for the wrong reason (issue #597). `tests/mcp_tools_http_tests.rs::tools_call_dispatches_through_the_real_pipeline_and_fails_closed` sent a schema-violating `start_order_flow` body expecting to prove issue #373 schema rejection at the tool edge, but the earlier removal of `start_tool`'s redundant pre-delegation schema check (see the "Harden the MCP tool exposure" entry above) means this no-runtime harness now fails closed on `harvest runtime is not started` before ever reaching `start_workflow`'s own schema check — and that response's `"status":400` field kept the old assertion passing for an unrelated reason. Fixed by asserting the actual, reachable guarantee (fail-closed on the runtime-not-started error) and adding `mcp_start_tool_rejects_input_that_violates_the_published_schema` to `tests/mcp_tools_integration.rs` (testcontainers, with a real runtime) as the schema-rejection guarantee's actual coverage. No production code changed; no new `WorkflowEvent` variant, no migration.

- Fix MCP tool exposure for duplicate workflow-name registrations to match the runtime's own dedupe (issue #597). `HandlerRegistry` collapses the builder's workflow list into a `HashMap` via `.collect()`, so the runtime executes the *last* registration when the same workflow name appears twice; `collect_descriptors` instead filtered to `mcp`-flagged entries and kept the *first* same-named descriptor, so `.workflows([foo.with_mcp(), foo])` could still expose MCP tools for `foo` derived from a `WorkflowInfo` the runtime does not actually execute. Fixed by resolving the same last-wins collapse before filtering by `mcp` (with a warning naming the duplicate), so tool exposure and schema derivation always reflect the effective, runtime-executed `WorkflowInfo`. No new `WorkflowEvent` variant, no migration.

- Fix the MCP docs primary setup snippet to use `api_with_auth` instead of plain `api` (issue #597). The snippet paired unauthenticated `.api("/api/harvest")` with `secure_mcp(...)` commented as the production gate, but `secure_mcp` only protects the `/mcp` JSON-RPC envelope, not a generated tool route's own direct HTTP path — matching the same gap already fixed in the quickstart example. (A separate reported P2 finding, that `HarvestPlugin::api_with_auth`'s `M: Clone` bound was newly required by this feature, was investigated and found not to apply: that bound predates this PR, coming from `axum::Router::layer`'s own signature — no code change was needed.)

- Fix a post-merge compile break in `autumn-harvest/tests/nd_block_tests.rs` (issue #597). Merging `trunk-dev` (issue #603's `nd_block_tests.rs`, landed concurrently with this branch's `mcp: bool` field addition to `WorkflowInfo`) left a raw `WorkflowInfo { .. }` struct literal missing the new field, since a merge cannot auto-add a field to an existing literal. Added `mcp: false,`. Caught by CI's Lint job; verified with a full re-run of the CI lint/test command matrix.

- Fix duplicate update-handler resolution to match the runtime's own dedupe (issue #597). `admit_update`/`update_with_start` resolve a declarative update handler via `registry.update_handlers.iter().find(...)` (first-wins), but `collect_descriptors` filtered to `mcp`-flagged updates before dedup and kept the first *mcp-flagged* one — so a duplicate `(workflow, update_name)` registration where only the later one was `mcp`-flagged could expose a tool derived from a handler the runtime doesn't actually validate/admit against. Fixed by resolving first-wins across the whole unfiltered list before filtering by `mcp`, mirroring the workflow-name fix. No new `WorkflowEvent` variant, no migration.

- Fix a self-inflicted regression from the workflow-name-dedup fix above (issue #597): sorting workflow names alphabetically before the operation-id collision-detection loop silently flipped the collision tie-break from "first-registered wins" to "alphabetically-first wins" — registering `start_invoice` before `invoice_status` would incorrectly keep `invoice_status` instead. Fixed by processing collision candidates in first-registration order; the final returned list is still sorted by name.

- Harden the per-tenant usage report (issue #596) against a code review pass. `GET /admin/usage` now: (1) hard-caps the number of returned groups (`usage_max_groups`, default 10,000) and fails loudly with `413` naming the cap and actual group count rather than either a silent rollup or an unbounded response, mirroring the endpoint's existing "never silently drop a tenant" principle; (2) is properly configurable via `HarvestBuilder::usage_window_ceiling`/`usage_max_groups` (previously the underlying `HarvestApiState` knobs existed but were never wired through the builder); (3) derives the `"(unattributed)"` SQL literal from the `UNATTRIBUTED_GROUP` Rust constant instead of duplicating it, closing a drift risk, and documents the residual (accepted, AC-mandated) limitation that a real `search_attrs` value literally equal to `"(unattributed)"` merges with the missing-key bucket; (4) merges two previously-separate `harvest_events` scans for `ActivityFailed`/`ActivityTimedOut` events into one CTE, avoiding a duplicate table read; (5) adds three new indexes (`idx_harvest_we_shard_started`, `idx_harvest_we_shard_completed`, `idx_harvest_events_activity_type_ts`, migration `20260702000000_harvest_usage_report_indexes`) to keep the endpoint within its stated `<2s`/250k-execution SLA; (6) fixes an error message that mis-rounded non-whole-day window ceilings (e.g. a 30-hour ceiling previously reported "1-day"); (7) documents that `workflow_starts` deliberately counts every execution row including continue-as-new/auto-retry successors. Also extracted a shared `shard_fanout::summarize_shard_errors` helper (used by both `usage.rs` and `workflow_count.rs`, replacing near-identical duplicated accumulator logic) and removed a duplicate `cell_f64`/`format_f64` CLI helper. No new `WorkflowEvent` variant, no replay-determinism impact.

- Add per-tenant usage report for chargeback and capacity planning (issue #596). New read-only `GET /admin/usage` endpoint is the *historical* companion to the *point-in-time* `GET /admin/concurrency` endpoint (issue #247): it aggregates already-durable data (`harvest_workflow_executions` + `harvest_events`) over a required `from`/`to` window (RFC 3339 or relative duration), grouped by `workflow_name` (default) or a `search_attr:<key>` tenant key, fanned out and summed across every shard. Each record reports `workflow_starts`, terminal outcomes (`completed`/`failed`/`cancelled`/`timed_out`), `activity_executions`, `activity_executions_failed`, and `activity_compute_seconds` (summed final-attempt activity durations). Executions lacking the requested `search_attr` key are grouped under an explicit `"(unattributed)"` bucket rather than dropped. A window wider than a configurable ceiling (default 90 days, `HarvestApiState::set_usage_window_ceiling`) returns `400` naming the ceiling. An unreachable shard is named in `unavailable_shards` rather than failing the call wholesale. New `harvest usage --from … --to … [--group-by …]` CLI subcommand renders a tabular report by default (`--json` for piping). Documented alongside `/admin/concurrency` in `docs/sharding.md`. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Add grouped workflow-count visibility endpoint for fleet snapshots (issue #544). New read-only `GET /workflows/count` endpoint returns execution counts grouped by `state` and/or `workflow_name` (via `group_by`, default `state`) across all shards in one request, answering "how many RUNNING/FAILED per workflow type, right now?" without paginating `GET /workflows` or hand-querying every shard database. Filters (`workflow_name`, `state`, `started_after`, `started_before`) mirror `GET /workflows`. Counts are computed with a real per-shard SQL `GROUP BY … COUNT(*)` (`autumn_harvest::execution::count_workflow_executions_grouped`) and summed across shards in-process, so the response stays cheap at any execution volume. Bounded cardinality: the response caps returned groups at `limit_groups` (default 50, max 500), rolling the long tail into a single `other: true` group. An unreachable shard is named in `unavailable_shards` with a reason rather than failing the call wholesale; `status` reports `complete`/`partial`/`unavailable`. The response is documented as an eventually-consistent point-in-time snapshot with no replay or ordering guarantee under concurrent writes. No new `WorkflowEvent` variant, no JSON-tagging change, no migration.

- Make `WorkflowSimulator` honor `RetryPolicy` for failure-path tests (issue #541). Mocked activities that return `Err` are now retried by the simulator according to the activity's resolved `RetryPolicy` (from the registered `ActivityInfo` via `WorkflowSimulator::with_activity_info`, an explicit `WorkflowSimulator::with_retry_policy` override, or a call-site override), including typed non-retryable fast-fail classification (`ActivityFailure`, issue #227) and correct, incrementing `attempt` numbers on `ActivityFailed` events — never a hardcoded `1`. New `WorkflowSimulator::mock_activity_with_attempt` lets a mock observe the current attempt number. Backoff is advanced logically with no real-time sleeps. `Saga`-based compensation is reachable from a Postgres-free unit test. No new `WorkflowEvent` variant, no schema change, no migration.

- Add paginated, filterable single-execution workflow history API (issue #529). New read-only `GET /workflows/{id}/history` endpoint supports keyset pagination via opaque `next_cursor` (backed by `harvest_events.id`), `limit` (1–1000, default 100), `after` cursor, and repeatable `event_type` filter for server-side event-type narrowing. Response includes `total_events` (unfiltered) and `last_event_id` for UI progress tracking. `GET /workflows/{id}` now bounds `history` to the first 100 events and adds `history_truncated: bool` and `history_endpoint: string` fields so callers can discover the pagination endpoint. No new `WorkflowEvent` variant, no migration.

- Add workflow-type reachability check to gate safe handler removal (issue #520). New read-only `GET /admin/workflow-types/reachability` endpoint and `harvest workflow-types reachability [--type] [--json]` CLI subcommand report, per workflow type, how many non-terminal executions still depend on its handler and a `safe_to_remove` / `in_use` / `orphaned` verdict. The CLI exits `2` when any type is `orphaned` or the cross-shard answer is incomplete, so it is usable as a CI/deploy gate. No new `WorkflowEvent` variant, no migration.

## [0.4.0] - 2026-06-16

### Added

- Add external workflow cancellation primitive (issue #492) (#676)([33595ff](https://github.com/madmax983/autumn-harvest/commit/33595ff5890efeb75c95a71f0f6281ddb0fdc9a8))

- Add soft SLA breach detection for workflow executions (issue #487) (#671)([f85ba1b](https://github.com/madmax983/autumn-harvest/commit/f85ba1b28526f02d060fd5533cbf5aaf236ec00c))

- Add bounded catchup window policy for scheduled workflows (issue #484) (#669)([b11e5a9](https://github.com/madmax983/autumn-harvest/commit/b11e5a9197d17cbe616b990ecf01a448cccbbf99))

- Issue #482: Data-dependent DAG branching with condition predicates (#667)([774079b](https://github.com/madmax983/autumn-harvest/commit/774079bdc522375d29537926dd562945d2a6e2e1))

- Add per-execution context headers (issue #481) (#664)([44f2af3](https://github.com/madmax983/autumn-harvest/commit/44f2af383216cab8c6b52eb165cb6ac7c1d231ad))

- Add stalled-workflow discovery filter to GET /workflows (issue #486) (#663)([3a63370](https://github.com/madmax983/autumn-harvest/commit/3a63370d37635412a0c79f9fc9be6cf0792fb125))

- Implement non-determinism deploy detection (#480) (#652)([53fd0c9](https://github.com/madmax983/autumn-harvest/commit/53fd0c968455f7c10a9524e7494e435017d9c048))

- Add bounded schedule runs: end_at and max_runs (issue #478) (#651)([e4a3479](https://github.com/madmax983/autumn-harvest/commit/e4a3479268fa530b0500ceaaa44de2fdc3d4826d))

- Add update-with-start atomic primitive (issue #479) (#649)([5ac5716](https://github.com/madmax983/autumn-harvest/commit/5ac5716af41b35f34475bdad3c18bd7840622da7))

- **macro:** Implement compile-time determinism check guardrails for #[workflow] (#640)([9a7aafb](https://github.com/madmax983/autumn-harvest/commit/9a7aafb9d8f0489203ce487803bc5608fcc6399e))

- Add signal-or-deadline race primitive (issue #476) (#645)([0d58067](https://github.com/madmax983/autumn-harvest/commit/0d5806799b3cc28bfe7a4211cab2d3a981a7cd5c))

- Add set_current_details() for durable workflow status breadcrumbs (#643)([7254bb1](https://github.com/madmax983/autumn-harvest/commit/7254bb1b182fe30b187604e40a439e2c8f5f304f))

- Add operator pause/resume primitive for workflow executions (#383) (#630)([b56a916](https://github.com/madmax983/autumn-harvest/commit/b56a916d0258ab680b88ae2d9050ac3978485ae1))

- Implement worker capability labels and activity routing (issue #382) (#622)([1ba16d6](https://github.com/madmax983/autumn-harvest/commit/1ba16d68dd993d3470b2e1573fcddaed53efddbb))

- Add deterministic side-effect primitives to WorkflowContext (issue #384) (#628)([c702e68](https://github.com/madmax983/autumn-harvest/commit/c702e6877f98d843d7f850c27c4be587499e8bd2))

- Add DLQ root-cause aggregation API for fast incident triage (#385) (#629)([3783037](https://github.com/madmax983/autumn-harvest/commit/37830378e841ff091fdd19a4975306c51f8eb26a))

- Implement queue & task eligibility explainer for stuck-task triage (#595)([a41087e](https://github.com/madmax983/autumn-harvest/commit/a41087e6d4568d1769ae1d599ee01f7b82893a08))

- Add worker-level retry context: attempt(), previous_failure(), max_attempts() (#616)([6a0834c](https://github.com/madmax983/autumn-harvest/commit/6a0834c8844665e7c11e1314c0b8214f8d19a4a5))

- Add harvest.workflow.terminal counter for per-outcome success-rate SLO (#594)([c9ff68a](https://github.com/madmax983/autumn-harvest/commit/c9ff68a473796d12aa006286cb0de38f7e182ce2))

- Add replay-safe WorkflowLogger and HVG009 guardrail (issue #379) (#587)([0e24d26](https://github.com/madmax983/autumn-harvest/commit/0e24d266fb929eeb9c1320881d65faa58d012944))

- Display timezone next to cron expression in Vantage UI schedules page (#588)([e59293c](https://github.com/madmax983/autumn-harvest/commit/e59293c1a443262eb4c0f39de3f4827ea851579a))

- Add schedule-to-close timeout for cross-retry wall-clock deadlines (#586)([2b1e75a](https://github.com/madmax983/autumn-harvest/commit/2b1e75af87186a7543291ae72b04f99608d531af))

- **dag:** Implement dynamic task mapping (fan-out) for DAGs (#585)([3fbf957](https://github.com/madmax983/autumn-harvest/commit/3fbf957b708f7e42391a10cb624d3200d58a570c))

- Add admission gate primitive for incident-response workflow halts (#579)([7974582](https://github.com/madmax983/autumn-harvest/commit/797458248ed0108761ee1b5020b1256e0aa6cde2))

- Implement declarative completion triggers (issue #517) (#561)([96e82bf](https://github.com/madmax983/autumn-harvest/commit/96e82bfd10fe633c0d3887d0264bf9398053cf7c))

- **metadata:** Implement workflow metadata ownership (#372) (#550)([6b3fb18](https://github.com/madmax983/autumn-harvest/commit/6b3fb18c0ab288bf9a1a8bc774b0e54b20fb2d86))

- Add JSON Schema opt-in for workflow input validation (issue #373) (#556)([7f5323d](https://github.com/madmax983/autumn-harvest/commit/7f5323d9d7fba5004791462e1c1d7d8671416318))

- Add per-activity circuit breaker for downstream outage fast-fail (issue #369) (#549)([c03e1d8](https://github.com/madmax983/autumn-harvest/commit/c03e1d8d2b2908b80d9c834f0a8ecfe5d8fadb7a))

- Implement poison-pill task quarantine (issue #367) (#539)([31c8305](https://github.com/madmax983/autumn-harvest/commit/31c8305c1838875f131896ef9e33587827fec851))

- Add DAG retry-from-failed-node operator surface (issue #366) (#530)([f80154e](https://github.com/madmax983/autumn-harvest/commit/f80154e0e981b5e1147b52b27424239e4936e4f0))

- Auto-pause schedules after consecutive execution failures (issue #360) (#500)([24af11f](https://github.com/madmax983/autumn-harvest/commit/24af11f7819d3a26f49885932ef6dfa33b4a5221))

- Add fan-out / parallel activity execution (issue #359) (#497)([b3535fe](https://github.com/madmax983/autumn-harvest/commit/b3535fe140783f39d78a7ce548d3f0b5ef8ddb2c))

- Add POST /workflows/batch_start endpoint for bulk workflow execution (#496)([2ca0a6d](https://github.com/madmax983/autumn-harvest/commit/2ca0a6dcbea59bb1bb5c79876d865c3d89994959))

- Add HA scheduler claim protocol for multi-replica deployments (issue #350) (#490)([0a3c4f8](https://github.com/madmax983/autumn-harvest/commit/0a3c4f8b3e39de10154c382a509dc70134cfdb72))

- Add transactional activities (issue #352) (#491)([51fe1ab](https://github.com/madmax983/autumn-harvest/commit/51fe1ab36a46517f7b689c1a8094aa001958e630))

- Propagate workflow cancellation to in-flight activities via heartbeat (#483)([246378a](https://github.com/madmax983/autumn-harvest/commit/246378aa82884beaa3b90ccfb4c355fa7ce0429a))

- Add DAGs listing and detail pages to Harvest UI (#424)([8444dc9](https://github.com/madmax983/autumn-harvest/commit/8444dc9c2c168d8ba029248b5c54289a2866d712))

- Add schedule next-fires preview API (issue #348) (#474)([db0387a](https://github.com/madmax983/autumn-harvest/commit/db0387ae729bb59da82cb80d0a285e6193fae1d6))

- Implement detached child workflow spawns with parent-close policies (#453)([17a7036](https://github.com/madmax983/autumn-harvest/commit/17a703651f24f784c96300e7717ae658b894e11d))

- Implement webhooks feature gate with durable signed webhook delivery workflow and activity (#452)([e3f8676](https://github.com/madmax983/autumn-harvest/commit/e3f86762f675b181c1997a7a0ede67895e635311))

- Add Build Routing UI page and API endpoints (issue #362) (#448)([5f4975c](https://github.com/madmax983/autumn-harvest/commit/5f4975c3966be6ccada02269909bd0feebd25ec6))

- Add schedule trigger-now endpoint (issue #343) (#450)([472cc80](https://github.com/madmax983/autumn-harvest/commit/472cc80b41ff28e3184bd3ec8d471b4fa6a28ef5))

- Implement compile-time type-safe workflow client stubs and handles (#341) (#443)([29ec69f](https://github.com/madmax983/autumn-harvest/commit/29ec69f4892f19b22fa109da1077e993b6d52726))

- **await-condition:** Implement await_condition and await_condition_timeout (issue #340) (#439)([9a34b15](https://github.com/madmax983/autumn-harvest/commit/9a34b15b4c4ba08a5377221cde7f550365f52f14))

- **retention:** Implement pre-retention history archival hook (issue #345) (#432)([a4927f7](https://github.com/madmax983/autumn-harvest/commit/a4927f73073e1c5079e037f2e949d3e6b6228d4f))

- Deterministic retry jitter: add `JitterPolicy`, seeded delays, worker seeding, tests, docs and benchmark (#431)([3d24a53](https://github.com/madmax983/autumn-harvest/commit/3d24a53438de369b62135094d12bb36cc87edce4))

- **rate-limit:** Implement time-windowed per-activity downstream API protection (RPS rate limiting) (#332) (#428)([611de9b](https://github.com/madmax983/autumn-harvest/commit/611de9bed73a5ee4d6e083a16cf49da0d94d457c))

- Add DAGs pages to Vantage UI (list + detail) (#426)([a90ffe8](https://github.com/madmax983/autumn-harvest/commit/a90ffe8fc663eb50d183d84461aa24bfe0052a32))

- **signal:** Deterministic Cross-Workflow Signaling (#421)([1c6b996](https://github.com/madmax983/autumn-harvest/commit/1c6b996c80c97889a5184716f6e05d3ea33d2e47))

- Expose worker-pool scaling endpoints for KEDA (#325) (#420)([0aa07df](https://github.com/madmax983/autumn-harvest/commit/0aa07df0fe56e0245e02eac9a18d61c5a2adaa36))

- Implement schedule decision persistence (issue #325) (#419)([66c355d](https://github.com/madmax983/autumn-harvest/commit/66c355dd4f56be4b71d25cde16215d0d2cd36580))

- Persist Schedule Decisions (Issue #325) (#417)([0e5b441](https://github.com/madmax983/autumn-harvest/commit/0e5b44180c42e7a4d9b4916c2a6295bc4ea5aa68))

- Support delayed/future workflow execution start (Issue #322) (#403)([090226b](https://github.com/madmax983/autumn-harvest/commit/090226b99e300bd1f38acd9417938467ad7dadc8))

- Add SSE execution event stream endpoint (issue #324) (#402)([c08b9b0](https://github.com/madmax983/autumn-harvest/commit/c08b9b01551420b4b06a559447a4225a8760f217))

- Add calendar-aware schedule filtering (issue #337) (#399)([f5ca74a](https://github.com/madmax983/autumn-harvest/commit/f5ca74ac2fe0f701460b8de10f6185dd5d5d2a3b))

- Display history event count and continue-as-new threshold on workflow detail page (#398)([805c2f4](https://github.com/madmax983/autumn-harvest/commit/805c2f46a8fab3c5a48aaa03d0dfbcd4130efef0))

- Implement payload size caps for workflow payloads (issue #252) (#395)([2d903b0](https://github.com/madmax983/autumn-harvest/commit/2d903b05d4bbbab02f8d1fc3b8a2ff61f89057b4))

- Add ReplayVerifier batch CI gate for workflow determinism (issue #251) (#393)([a4c1299](https://github.com/madmax983/autumn-harvest/commit/a4c129976be1cb685eb75a7421026782a36e0abc))

- Add task priority support for within-queue ordering (issue #249) (#391)([e37b4f2](https://github.com/madmax983/autumn-harvest/commit/e37b4f2dc0104e5c82d661fa38710585e2469e4d))

- Add timezone-aware cron schedules (Schedule::CronInTimezone) (#389)([ee81805](https://github.com/madmax983/autumn-harvest/commit/ee818052126e7a6b50cb2f35848721bf0017a101))

- Add typed dispatch helpers for activities, workflows, and signals (#387)([68b4ceb](https://github.com/madmax983/autumn-harvest/commit/68b4ceb601f4aee5e43844d82ae031b9bc874afd))

- Add SignalWithStart primitive for atomic webhook handlers (issue #244) (#364)([d3729c4](https://github.com/madmax983/autumn-harvest/commit/d3729c485fe645f5884901b0edab51a5e702e7ec))

- Add per-key concurrency limits for tenant fair-share scheduling (#370)([14240b5](https://github.com/madmax983/autumn-harvest/commit/14240b52a93074755d90d06f267de6b29ec56aae))

- Add ctx.signal_external_workflow for saga choreography (issue #330) (#376)([cde9046](https://github.com/madmax983/autumn-harvest/commit/cde9046102c402be265187b70cbada57a815a4f4))

- Implement workflow execution timeout for SLA enforcement (issue #243) (#374)([5b74436](https://github.com/madmax983/autumn-harvest/commit/5b744368d67b4f3bf26dafd0e04e382f6b7dc03d))

- Add WorkflowTestEnv in-process unit-test harness (issue #250) (#375)([0e56b6e](https://github.com/madmax983/autumn-harvest/commit/0e56b6e1daf1c966bc66e0399a0ea7fe378260c0))

- Add schedule overlap policy for concurrent run collisions (issue #241) (#361)([995c1c4](https://github.com/madmax983/autumn-harvest/commit/995c1c40a37ba69815a79f0f324162b1e137d7de))

- Add TDD DB integration tests for sticky cross-worker routing (spec #35) (#363)([04f723a](https://github.com/madmax983/autumn-harvest/commit/04f723ab46f556192c6378788ab2867d0c2347a0))

- Add workflow detail page with operator actions and event timeline (#354)([1d73748](https://github.com/madmax983/autumn-harvest/commit/1d73748b440d2a728f620e57e15f50929ab13a12))

- Declarative query and update handlers (issue #346) (#353)([dfee5fc](https://github.com/madmax983/autumn-harvest/commit/dfee5fce5abb281ef5fc5ed60c57613745c9bfc4))

- Add deterministic jitter to schedule fire times (#351)([4bdb434](https://github.com/madmax983/autumn-harvest/commit/4bdb434ae49e2ff1f3bcc1c1e96967211cea7b96))

- Add saga cancellation semantics and idempotency contract (#339)([d415ab2](https://github.com/madmax983/autumn-harvest/commit/d415ab29292864baef8f24852025919303cfa9a7))

- Implement sticky cross-worker routing with in-process cache delta-load (issue #235) (#336)([59a10d9](https://github.com/madmax983/autumn-harvest/commit/59a10d92140dfbac3221720a249ff2ef918d6bfc))

- Issue #234: Read-only Query handlers for live workflow state inspection (#328)([ca0e166](https://github.com/madmax983/autumn-harvest/commit/ca0e16618952def10e18fee1f42da78add3ddc36))

- Add schedules management UI page with pause/resume/delete actions (#333)([bc8df91](https://github.com/madmax983/autumn-harvest/commit/bc8df91b604d09f00c6b37740a12fd4fe4df5bd6))

- Unify DAG execution onto workflow path (issue #256 Step 5) (#302)([4e914ca](https://github.com/madmax983/autumn-harvest/commit/4e914ca8484eee2a38f6442c26ef3f8992bc9202))

- Add Claude Code GitHub Workflow (#310)([cb0cb48](https://github.com/madmax983/autumn-harvest/commit/cb0cb48dd294fb75f26653190c5251544427167c))

- Add pause/resume metadata tracking for schedules (issue #229) (#301)([2da1c38](https://github.com/madmax983/autumn-harvest/commit/2da1c382ef5d6eee5b706694a7a9fabf76d72a22))

- Issue #227: Typed activity failure surface with error classification (#299)([511ea8e](https://github.com/madmax983/autumn-harvest/commit/511ea8e44c0e2261cdcd53a87a65cf08c0c7c1d2))


### Fixed

- **plugin:** Require auth for cancel and dead-letter endpoints (#316)([50e7c3e](https://github.com/madmax983/autumn-harvest/commit/50e7c3ef092a1f6b49c3c32ea9720fb5bb5389c3))


### Performance

- Pre-allocate markers vector to reduce heap allocations (#290)([23f0796](https://github.com/madmax983/autumn-harvest/commit/23f0796e6d1d441973cd43645b58257d8229fc06))


### Miscellaneous

- Fix changelog (#300)([512796c](https://github.com/madmax983/autumn-harvest/commit/512796c4ae1c1a365e24b2958d9869f8a787e174))

## [0.3.0] - 2026-05-13
## [0.1.1] - 2026-04-19

### Added

- **harvest:** Add history guardrails for long-running workflows (#280)([212abd4](https://github.com/madmax983/autumn-harvest/commit/212abd474b6aa450d281d37e18616ed418afc10c))

- Add workflow handle result waiting (#276)([af2b7ea](https://github.com/madmax983/autumn-harvest/commit/af2b7ea3fe639edf808f023ffa98cce64bb94f79))

- Add schedule backfill API for recovering missed scheduled runs (#277)([fc58a3f](https://github.com/madmax983/autumn-harvest/commit/fc58a3f7da4fedf0937b1b81158296ee85466546))

- Add versioned management API contract and regression tests (#274)([761ff09](https://github.com/madmax983/autumn-harvest/commit/761ff09841ea203571442e23e83fa65a61d3dd7d))

- **det_check:** Add deterministic workflow guardrails (issue #172) (#265)([8794a63](https://github.com/madmax983/autumn-harvest/commit/8794a63d2f646ff3001e23aff968d523820e2fd0))

- Add test coverage for update.rs and version_usage.rs (#259)([8dc00d9](https://github.com/madmax983/autumn-harvest/commit/8dc00d9aa0c4c39678109e45c7c00a3c8405f752))

- Management API auth posture — route classification + security coverage (issue #174) (#267)([165e188](https://github.com/madmax983/autumn-harvest/commit/165e1880d3b9c7e20f614d4da94f0b7a5301ef2e))

- Add deterministic guardrail rule catalog foundation (issue #173) (#266)([7301229](https://github.com/madmax983/autumn-harvest/commit/7301229532059b4c55597f8c924d9a84c876f3b5))

- Add build-id routing for safe rolling deploys (issue #171) (#261)([91cad67](https://github.com/madmax983/autumn-harvest/commit/91cad67c944c8acb1253ee6104a0b6656fc58948))

- Add comprehensive getting started guide for Autumn Harvest (#254)([2877dca](https://github.com/madmax983/autumn-harvest/commit/2877dcad11460214e0e14c5791341623c1230a7a))

- Add remote worker drain controls (issue #170) (#246)([f011825](https://github.com/madmax983/autumn-harvest/commit/f0118258e6779fe4f9dd271daddd5a5382d93a81))

- Export workflow histories as replay fixtures (#242)([bdb37b7](https://github.com/madmax983/autumn-harvest/commit/bdb37b76edff49d32d2382f1a7820cb4e4187464))

- Expose operator external activity handoffs (#239)([38ce949](https://github.com/madmax983/autumn-harvest/commit/38ce9498c794693939725ca48a0f50c43747683a))

- Version-gate retirement check (issue #164) (#228)([8f7ecb2](https://github.com/madmax983/autumn-harvest/commit/8f7ecb25a5c71cb5c95c56f7eddecd598feb4dff))

- Report workflow version-gate usage (#223)([4246798](https://github.com/madmax983/autumn-harvest/commit/4246798b0a9b1f3dc586d2ab5411ee9108e16ad1))

- Add compensation when payment capture signal missing capture_id (#221)([dd5ca1e](https://github.com/madmax983/autumn-harvest/commit/dd5ca1eb025f305d9734e5edaffac5aab6c36d5d))

- Implement todo 87([4bf6cb7](https://github.com/madmax983/autumn-harvest/commit/4bf6cb753d3fd6f68c0bf0a562d8a2dee9c69a6e))

- Add documentation for WorkflowResetError, RetentionConfig, and Worker types (#218)([7707294](https://github.com/madmax983/autumn-harvest/commit/77072944451699e22554043e17d113817357330f))

- Add shard readiness health gate (#219)([10655a3](https://github.com/madmax983/autumn-harvest/commit/10655a3403049bbd45c6ac617977f868500c9912))

-  feat(harvest): add deployment preflight checks (#212)([9cf96ee](https://github.com/madmax983/autumn-harvest/commit/9cf96eeca0e99aa67deaacb5b234c81a1fa53075))

- Add mutable search attributes API for workflow executions (#210)([ddf6aa3](https://github.com/madmax983/autumn-harvest/commit/ddf6aa3626a0e827b4d58213ed1196e82a948ffa))

- Add audit trail for management API mutations (issue #158) (#208)([fa9f183](https://github.com/madmax983/autumn-harvest/commit/fa9f183a6a946f2d6fdb063d04dd461c5977f4a3))

- Add activity idempotency key support for at-least-once semantics (#203)([feb23b5](https://github.com/madmax983/autumn-harvest/commit/feb23b599839b4e4dc825a209f19af66e60d3c55))

- Heartbeats (#202)([68b6e59](https://github.com/madmax983/autumn-harvest/commit/68b6e599abcd6d29d54b46dbcbf043920d357988))

- Add test to verify api filters ignore unknown query keys (#190)([4b8ba87](https://github.com/madmax983/autumn-harvest/commit/4b8ba87f82e1e72490b6d5b7fe5703bb442bbf5d))

- Add workflow `stack` inspection endpoint and CLI command (#188)([bdb5216](https://github.com/madmax983/autumn-harvest/commit/bdb5216306d655362a68e204b1bef24b01da1e26))

- Add pluggable PayloadCodec system for event payload encoding/decoding (#187)([ca859d6](https://github.com/madmax983/autumn-harvest/commit/ca859d6b7758c69f1c040f71cfb1bbae6961cebf))

- Add Workers tab to Vantage dashboard UI (#186)([d5679b6](https://github.com/madmax983/autumn-harvest/commit/d5679b6a546fe869ef91190715e19fc08d00af42))

- Add Codecov badge to README([2143608](https://github.com/madmax983/autumn-harvest/commit/21436080ccd7ef78ae64610db2c86a2458031b64))

- Add badge for Ask DeepWiki to README([a1dbdde](https://github.com/madmax983/autumn-harvest/commit/a1dbdde4eb3254794fbde22398a47f6ea526c5be))

- Implement Update primitive for synchronous workflow requests (issue #140) (#181)([0af6d35](https://github.com/madmax983/autumn-harvest/commit/0af6d35bff9332cb34dae51124971eec468a13ef))

- Add metrics recording infrastructure and telemetry adapters (#178)([50dc23d](https://github.com/madmax983/autumn-harvest/commit/50dc23dbe96a31b999d032852c51991bbefbcf3a))

- Implement ADR-0001 OpenTelemetry span emission for workflow execution (#167)([5dbe09e](https://github.com/madmax983/autumn-harvest/commit/5dbe09e21bc80effb46603eb61eb9d1718a782e5))

- Add advanced workflow examples (#156)([f0ccf8d](https://github.com/madmax983/autumn-harvest/commit/f0ccf8d016f1d26b4ddfce51776cf6035013c04f))

- Add unit tests for ID string parsing and formatting in types.rs (#147)([bf04595](https://github.com/madmax983/autumn-harvest/commit/bf045950a357c7b7e8acebaf911bab8e1aeb5e46))

- Add version gate support with mismatch detection (#146)([8942960](https://github.com/madmax983/autumn-harvest/commit/89429604b2416afcc2777492ce6edd0f933e5193))

- Add WorkflowReplayer harness for pre-deploy replay-safety testing (#141)([8a331f9](https://github.com/madmax983/autumn-harvest/commit/8a331f92a91035dc31d3180c1c1d4d8c338d43a2))

- Add quickstart example with workflow, activity, and durable timer (#137)([5e3f4c4](https://github.com/madmax983/autumn-harvest/commit/5e3f4c4c65df912cc6f04628fb2ff39081ea9b51))

- Add batch operations for fleet-wide workflow management (issue #102) (#134)([3418c2f](https://github.com/madmax983/autumn-harvest/commit/3418c2fd5e091a98d626e75339c84d337cabfb82))

- Add Vantage spec for Child Workflows (#128)([265ae74](https://github.com/madmax983/autumn-harvest/commit/265ae74f38c19f1569f302719219581a2e7c19a8))

- Add worker fleet observability with liveness tracking and heartbeats (#133)([2f455ce](https://github.com/madmax983/autumn-harvest/commit/2f455ceab88c5fd2332b6b299e7f9984be9c7044))

- Implement local activities (issue #98) (#132)([69a44a6](https://github.com/madmax983/autumn-harvest/commit/69a44a6106ad902ca529edb92ba9ccfb05e908f1))

- Add bulk dead-letter queue replay and discard operations (#131)([c7f181a](https://github.com/madmax983/autumn-harvest/commit/c7f181a41f4c97be2bf8fbd85d4bb73f0ed8bbf7))

- Add external activity completion via task tokens (issue #92) (#123)([d5a7828](https://github.com/madmax983/autumn-harvest/commit/d5a7828055c4220d4576ee48d641eeb039e8c4bc))

- Add per-workflow cron schedules (issue #91) (#122)([a56bb42](https://github.com/madmax983/autumn-harvest/commit/a56bb421524433313e56d9e08460c3194e9e830f))

- Add tests covering unreachable code paths in history export (#118)([24a7af0](https://github.com/madmax983/autumn-harvest/commit/24a7af091f755ac559eeccf61172cf3a668351df))

- Add cluster-wide concurrency caps for rate-limited activities (#117)([59c3c62](https://github.com/madmax983/autumn-harvest/commit/59c3c6249494e741b7cbd01c388a97f225af0cc4))

- Add WorkflowIdReusePolicy to control duplicate workflow starts (#115)([fc5ae22](https://github.com/madmax983/autumn-harvest/commit/fc5ae22a7264ec3e5159170887cce143d9bfd5b6))

- Add opt-in retention janitor with management API and CLI controls (#111)([cbf39b3](https://github.com/madmax983/autumn-harvest/commit/cbf39b36abd0c5d24f1e9fd30e071ba3e89d88e9))

- Filter GET /workflows by state, workflow name, and search_attrs (#85)([53bef7f](https://github.com/madmax983/autumn-harvest/commit/53bef7f9264f41f5145775ef12851f7cb481aff6))

- Add Redis Streams task queue adapter (autumn-harvest-redis) (#79)([69172b2](https://github.com/madmax983/autumn-harvest/commit/69172b272ad8cfbdaf18f6b64307c979cc5c4042))

- Implement continue-as-new for long-running workflows (#77)([ca89e9c](https://github.com/madmax983/autumn-harvest/commit/ca89e9c49a49f01d52b12393e60e757746483f35))

- Add tests for wait_for_signal and child workflow diverged history paths (#71)([411e5e0](https://github.com/madmax983/autumn-harvest/commit/411e5e0ff4cffe7a1825efd1f40fa0a7eec13ae8))

- Add Vantage Spec for Redis-backed Task Queue Adapter (#68)([4f1d18a](https://github.com/madmax983/autumn-harvest/commit/4f1d18a68c7edfb7d781a21bdd38ed9c678c0d6a))

- Add DagSimulator for in-memory DAG testing (#59)([18ea876](https://github.com/madmax983/autumn-harvest/commit/18ea87604329c8f165c756af3fba6e891e8eb53f))

- Add Vantage embedded dashboard UI for workflow observability (#55)([5d28376](https://github.com/madmax983/autumn-harvest/commit/5d283765b55e35dc5d69fd18f42d98e6879b19e4))

- Add OpenTelemetry integration for trace context propagation and metrics (#53)([ad4a35b](https://github.com/madmax983/autumn-harvest/commit/ad4a35b901dcdee234e8dacf042d74362b41a11d))

- Implement cancellation grace period for uncooperative activities (#52)([6577e36](https://github.com/madmax983/autumn-harvest/commit/6577e3624347c861d93dcc9b00ed19c0c5624d4e))

- Add workflow sharding support across multiple Postgres databases (#54)([94dfc99](https://github.com/madmax983/autumn-harvest/commit/94dfc99d2780716ecf39c6a50748ee4d275dbe68))

- Add sticky cross-worker routing for workflow task affinity (#51)([329053d](https://github.com/madmax983/autumn-harvest/commit/329053d0296e3aad37a639c0c51d64c71cfd8ce8))

- Add Vantage spec for Database Sharding (#48)([3d58b5c](https://github.com/madmax983/autumn-harvest/commit/3d58b5ccd04679af198ca2ea50c70e688270b14f))

- Add Vantage specs for OpenTelemetry integration and Cancellation Semantics. (#43)([d093c75](https://github.com/madmax983/autumn-harvest/commit/d093c75f7d14f00926bd5088fbdbad8159167bea))

- Add `side_effect` and `random_uuid` to workflow context (#42)([100fcf0](https://github.com/madmax983/autumn-harvest/commit/100fcf0a625ea2fc399b3d67dd6331e9287ebb8a))

- Add Vantage spec for Sticky Cross-Worker Routing (#35)([a98b3e7](https://github.com/madmax983/autumn-harvest/commit/a98b3e789e08725baea2e73cf895daf972e04da7))

- Add harvest management cli (#29)([597dcfc](https://github.com/madmax983/autumn-harvest/commit/597dcfca7406dd5554223a843589a31d6cd2647c))

- Add Mermaid.js workflow history exporter (#25)([7226560](https://github.com/madmax983/autumn-harvest/commit/72265609750ef9b02b18f6b9cb49b2d31ddcee91))

- Add DAG visualization exporters for Mermaid and DOT (#17)([3cfdec9](https://github.com/madmax983/autumn-harvest/commit/3cfdec96e21f52059ccd09af11f8ed7041aba398))


### Fixed

- Route DLQ UI actions through audited bulk API (#286)([30e01dd](https://github.com/madmax983/autumn-harvest/commit/30e01dd44335e858417f0312f375f626635423a7))

- Compensate saga when approve_high_value_subscription fails (#222)([3784578](https://github.com/madmax983/autumn-harvest/commit/3784578bfd90754f2e1cdb1f0e80267e5b5e74db))

- Gate shared rollouts on readiness attempt 2 (#220)([190e5be](https://github.com/madmax983/autumn-harvest/commit/190e5be5a647cdf0769fda6ee405b1297b7ad6e9))

- Gate shard rollouts on readiness([b960a15](https://github.com/madmax983/autumn-harvest/commit/b960a15efd39e84ef924283ca24ee44fb67f3d7b))

- **worker:** Prevent panic in chrono_duration_from_secs (#126)([8511545](https://github.com/madmax983/autumn-harvest/commit/851154528d8d14afd3f264fc74706c3038498f70))

- Redis depth handling([76335e6](https://github.com/madmax983/autumn-harvest/commit/76335e65f96643faf3e08c635dc3a7ad5fe2277f))

- Replay determinism([d0a14d7](https://github.com/madmax983/autumn-harvest/commit/d0a14d7eede408bcf4449f1ad68ce04013c47b91))

- Redit and continue-as-new([fd1c024](https://github.com/madmax983/autumn-harvest/commit/fd1c0242a42985e6a8cfc4a1d19b40daf091e88e))

- Fix deadlock in WorkflowContext::execute_query (#66)([de47dd5](https://github.com/madmax983/autumn-harvest/commit/de47dd52a892e9a44c4bbd7fd473cb2077ac8f5b))

- Prevent silent integer truncation in concurrency limits and event indexing (#45)([68d5225](https://github.com/madmax983/autumn-harvest/commit/68d5225d6d5e97850ba975188d971e86a4bd5ce3))

- Fix integer overflow in task_duration string parsing (#32)([36add46](https://github.com/madmax983/autumn-harvest/commit/36add4631e3c16741acd7dfa4f5301017c3fbdea))

- Resolve broken intra-doc links for HarvestError (#19)([cd77f63](https://github.com/madmax983/autumn-harvest/commit/cd77f6365848bcd7c9861564ffc53b2c0bf69f08))

- Fix retry backoff calculation panic on negative floats and NaN (#16)([6eb93b6](https://github.com/madmax983/autumn-harvest/commit/6eb93b68cd1fa8776595b23b73af479b29c7d733))


### Changed

- **worker:** Extract helper methods from `run_with_listener` (#258)([ba5da61](https://github.com/madmax983/autumn-harvest/commit/ba5da61e6622148a7f4ccec323669cb54f9fc7b7))

- Simplify context history matching and worker execution error handling (#116)([f163e67](https://github.com/madmax983/autumn-harvest/commit/f163e6757515e4b78582abf85e649826fb56c131))

- **plugin:** Render Vantage dashboard with maud + autumn-web extractors (#101)([875830e](https://github.com/madmax983/autumn-harvest/commit/875830e51a60407a16dd68d3af488183c1786be4))


### Documentation

- **alerts:** Add starter Harvest alert pack and runbooks (#275)([49c41f2](https://github.com/madmax983/autumn-harvest/commit/49c41f209a44bde2dd7eccb8e2054e9af4719f7d))

- Fix broken intra-doc links across workspace (#236)([0726486](https://github.com/madmax983/autumn-harvest/commit/0726486382140a672a4e289876677a5c9eded8cd))

- Polyglot ADR([c43e8e4](https://github.com/madmax983/autumn-harvest/commit/c43e8e4b6770e140fc387670a671c8f295e4dc9f))

- Skills([be33d2e](https://github.com/madmax983/autumn-harvest/commit/be33d2ebcde3c6eeb29540baf2e728ae175bd565))

- Add module-level documentation and executable doc tests for query, dag_export, schema, and signal. (#69)([859bb21](https://github.com/madmax983/autumn-harvest/commit/859bb2101fcbc7f6efbc0b486adb05b6b171e1c4))

- Add Vantage spec for Continue-As-New (#60)([d884143](https://github.com/madmax983/autumn-harvest/commit/d884143fe78da00cf627b11a72ea07445d8b077e))

- Add Vantage specification for Saga Primitives (#18)([0cc01fe](https://github.com/madmax983/autumn-harvest/commit/0cc01fed2dea12ed2ce076ae230a1fd5d0ee3cf4))


### Testing

- **error:** Add unit tests for HarvestError Display formatting (#215)([d326df5](https://github.com/madmax983/autumn-harvest/commit/d326df50a4e7d6be7142c4bf492567fa3d0d5f6d))

- Add test coverage for WorkflowContext::check_cancellation (#90)([b4ed42e](https://github.com/madmax983/autumn-harvest/commit/b4ed42ed9ba7c5a0fb0d8da6ef1003b54261c7bc))

- **saga:** Add comprehensive unit tests for Saga compensation logic (#44)([72d5760](https://github.com/madmax983/autumn-harvest/commit/72d5760b1217909d79342ac9a5242bc3a672b57d))

- Improve test coverage for Error, Info, and Saga modules (#40)([48f5a37](https://github.com/madmax983/autumn-harvest/commit/48f5a377dd024b65c2782447fca4407ad3577e2a))


### Miscellaneous

- Delete claude files([3c73e9e](https://github.com/madmax983/autumn-harvest/commit/3c73e9e6271497864a414987799f6cbed4caa85d))

- Relase work([7e61ec0](https://github.com/madmax983/autumn-harvest/commit/7e61ec038ee985e93186a72c0cad28d53d8b7ddc))

- Clippy([51b9b6e](https://github.com/madmax983/autumn-harvest/commit/51b9b6ec3626e5804eca98f138ac907f58554fdb))

- PR feedback([6f40114](https://github.com/madmax983/autumn-harvest/commit/6f40114d7ac6109a0a277d98cce469ca8dfefc8f))

- Cleanup([2c59de4](https://github.com/madmax983/autumn-harvest/commit/2c59de42684a19c036e1360584bfc53522eaa4e7))

## [0.1.0] - 2026-04-19

### Added

- **worker:** Add worker runtime with semaphore-bounded poll loop([47f8291](https://github.com/madmax983/autumn-harvest/commit/47f8291b8f6e7716f6950ff8505105825f67ca3c))

- **pool:** Add separate worker pool with shared ceiling enforcement([c602cbd](https://github.com/madmax983/autumn-harvest/commit/c602cbdb5e086aaecf10a44137b9fc50389696b9))

- **dlq:** Add dead letter queue for permanently failed tasks([07b0ba0](https://github.com/madmax983/autumn-harvest/commit/07b0ba03a5bae5379b8b5d497feb77f1e395a5e7))

- **cache:** Add LRU workflow state cache for replay optimization([6f5eb96](https://github.com/madmax983/autumn-harvest/commit/6f5eb96c1c2db92493863f1648b06d1d5bca212a))

- **timeout:** Add timeout enforcement for heartbeat, start-to-close, and schedule-to-start([f860113](https://github.com/madmax983/autumn-harvest/commit/f860113d6647e8fca33f5286c6dc38dc9b32e960))

- **heartbeat:** Add batched heartbeat flusher with 1s debounce([b563e83](https://github.com/madmax983/autumn-harvest/commit/b563e834ef99e491385af6db591e9afec729f784))

- **executor:** Add workflow executor with replay and suspension detection([b00d056](https://github.com/madmax983/autumn-harvest/commit/b00d056b1cefa6023839d42f8a2fa04ca5d22ca0))

- **notify:** Add LISTEN/NOTIFY wrapper with channel naming and payload types([387dd3a](https://github.com/madmax983/autumn-harvest/commit/387dd3a102d823dccf0ae6f7d8d8103c1d2d4e22))

- **queue:** Add Postgres-backed task queue with SKIP LOCKED claiming([574503f](https://github.com/madmax983/autumn-harvest/commit/574503f2a97de05ef5f8bcb83c67eb45bffc67b9))

- **context:** Implement ActivityContext with heartbeat channel and cancellation([233189a](https://github.com/madmax983/autumn-harvest/commit/233189a1bf368412c6757ec050e0199697f1d4d2))

- **context:** Implement WorkflowContext with replay-aware execute_activity and versioning([3e908bb](https://github.com/madmax983/autumn-harvest/commit/3e908bb22c173de66d784b0d7eae93d9f473b182))

- **replay:** Add HistoryMatcher with activity, timer, and version matching([f79b6ef](https://github.com/madmax983/autumn-harvest/commit/f79b6ef340d2d18ad457203af123b880c517fc4d))

- **store:** Add load_history reader for event replay([921a57f](https://github.com/madmax983/autumn-harvest/commit/921a57f7c81ef856ef03929adff88b1e40e41658))

- **store:** Add event store writer with sequential event IDs([3f7337f](https://github.com/madmax983/autumn-harvest/commit/3f7337f45b250d3978c27cb762a3399238802b0f))

- **macros:** Add workflows![] and activities![] collection macros([d09fd56](https://github.com/madmax983/autumn-harvest/commit/d09fd56e518a949a95af5d379535efc46d856bc1))

- **macros:** Implement #[activity] with retry/timeout/queue attributes([a6ea41a](https://github.com/madmax983/autumn-harvest/commit/a6ea41ae142b7164b61ae34a779b195339387c18))

- **macros:** Implement #[workflow] companion function generator([bd9de34](https://github.com/madmax983/autumn-harvest/commit/bd9de34c630260902e0bf2d4a1132d7b60be100c))

- **builder:** Add HarvestBuilder, WorkerConfig, and prelude([923edbe](https://github.com/madmax983/autumn-harvest/commit/923edbe84551000dbefedfaca4cd24384dce5e67))

- **db:** Add Diesel schema and Queryable/Insertable models for harvest_* tables([cbac2e7](https://github.com/madmax983/autumn-harvest/commit/cbac2e7d4d4486bbc72881177f18e15cdb85161d))

- **migrations:** Add harvest_* Postgres schema (initial migration)([6881762](https://github.com/madmax983/autumn-harvest/commit/6881762d8c64ec409f0a43712514f47fd9df9da2))

- **context:** Add WorkflowContext and ActivityContext skeletons with state access([747a1c4](https://github.com/madmax983/autumn-harvest/commit/747a1c43fdcb7729b6df50ff5db491f206b18fb9))

- **info:** Add WorkflowInfo, ActivityInfo registration types([713f20a](https://github.com/madmax983/autumn-harvest/commit/713f20a62f5ab275b1cebb82c6856ab95eaa4ed9))

- **event:** Add WorkflowEvent enum with serde round-trip([bb2d637](https://github.com/madmax983/autumn-harvest/commit/bb2d6371756cb823334a059e41e6edb261bd9943))

- **policy:** Add RetryPolicy, TriggerRule, Schedule([e22c9ea](https://github.com/madmax983/autumn-harvest/commit/e22c9ea35bb3865d64fb7b1cb346c8795176aecf))

- **error:** Add HarvestError, HarvestResult, TimeoutType, compute_retry_delay([7f6a27e](https://github.com/madmax983/autumn-harvest/commit/7f6a27e335577aabff50b753d5257ed552adc282))

- **types:** Add WorkflowId, ExecutionId, ActivityExecId, TimerId, WorkerId newtypes([e1b26a7](https://github.com/madmax983/autumn-harvest/commit/e1b26a7ba371fcf07c1d1d5143f0bb2b593f277c))


### Fixed

- **plugin:** Explicitly enable autumn-harvest db feature([2603761](https://github.com/madmax983/autumn-harvest/commit/2603761b1a7008077f51ce5023deff0f023c4ef9))

- **migrations:** Remove duplicate harvest initial migration([a3deb05](https://github.com/madmax983/autumn-harvest/commit/a3deb0518745ae99376346ecfcb22aa2115bc944))

- **plugin:** Explicitly enable autumn-harvest db feature([bbf93f3](https://github.com/madmax983/autumn-harvest/commit/bbf93f3bf0c82610071c5b32684be3417954f725))

- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([cd98e36](https://github.com/madmax983/autumn-harvest/commit/cd98e3656868cb67ee7e1dc26cf8eb0697fefefe))

- **manifests:** Add version to internal workspace path deps([8ed6456](https://github.com/madmax983/autumn-harvest/commit/8ed6456fd812ee06fa4dfecfb6883bf877324fc5))

- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([43ed6f7](https://github.com/madmax983/autumn-harvest/commit/43ed6f70e94163052e2d3debb2e1ea46aa6c5fd1))

- **manifests:** Add version to internal workspace path deps([3d2254f](https://github.com/madmax983/autumn-harvest/commit/3d2254f1c33889ab3f4466be8397015269ea55ab))

- Resolve clippy warnings in pool/worker and run cargo fmt([28d83ae](https://github.com/madmax983/autumn-harvest/commit/28d83aea5d11a201e09ca5ff43234a4f92d872e7))

- **macros:** Propagate serialization errors and add Debug impls to builder types([c460035](https://github.com/madmax983/autumn-harvest/commit/c460035d8d6d6c2c763f728a337962819932d9a8))

- **models:** Add serde derives and doc comments to all model structs([309f156](https://github.com/madmax983/autumn-harvest/commit/309f1563ace0803b15fb71414a565267deedb6e4))

- **context:** Add production constructors and fix unused_async lint([83645ad](https://github.com/madmax983/autumn-harvest/commit/83645ad69707997e0c5d9b1dca71afe5f58a047b))

- Move compute_retry_delay to policy, fix underflow, AllDone vacuous truth, const type_name, expanded tests([e9d7c46](https://github.com/madmax983/autumn-harvest/commit/e9d7c46715e68a4ffb867783077010e586935820))

- **types:** Const fn on as_uuid, add as_str to TimerId and WorkerId([5fe02cb](https://github.com/madmax983/autumn-harvest/commit/5fe02cb46f3b8114aa35eabf311731513ddbccd6))


### Documentation

- Add README and crates.io metadata for first publish([74dbbb9](https://github.com/madmax983/autumn-harvest/commit/74dbbb912eb4f3d430c209c1ef98fb36e830785e))

- Add README and crates.io metadata for first publish([df0d628](https://github.com/madmax983/autumn-harvest/commit/df0d628153bb66641d4affbef34cb61ae45ba453))

- Update CLAUDE.md with Phase 2 completion status([38f506e](https://github.com/madmax983/autumn-harvest/commit/38f506e52a995e85342d5f5aa3ddfebc854b10a8))

- Add CLAUDE.md with architecture overview and development guide([48675e7](https://github.com/madmax983/autumn-harvest/commit/48675e777c46cbfa0701477aaccf18a1044dad7b))


### Testing

- Fix clippy doc_markdown warnings in replay tests([2ee4667](https://github.com/madmax983/autumn-harvest/commit/2ee466788daae3300818a5fda85ee3856ceda493))

- Add E2E integration tests with testcontainers Postgres([c670f8b](https://github.com/madmax983/autumn-harvest/commit/c670f8bc577ba3bd09277b877835bc3095ac0937))

- Add replay engine correctness tests including non-determinism detection([c624524](https://github.com/madmax983/autumn-harvest/commit/c6245246344cc3beea5e95da8910ffa065d9b0f9))

- Add end-to-end integration tests for workflow lifecycle([c81bcc7](https://github.com/madmax983/autumn-harvest/commit/c81bcc75effb9db41f6aee875d3786c1ff2109ba))


### Miscellaneous

- Trigger on trunk-dev push and pull_request([0601dd0](https://github.com/madmax983/autumn-harvest/commit/0601dd092f66b074ccced99c18862e075d93f150))

- Trigger on trunk-dev push and pull_request([7bf8cc0](https://github.com/madmax983/autumn-harvest/commit/7bf8cc0908ed0de336389c506fdc4cca530622be))

- **plugin:** Use published autumn-web 0.2 from crates.io([20aa1cd](https://github.com/madmax983/autumn-harvest/commit/20aa1cd9cfc63e087fb6b732a04e5d1cfec74bf1))

- **plugin:** Use published autumn-web 0.2 from crates.io([15d9eba](https://github.com/madmax983/autumn-harvest/commit/15d9ebaaf32dcd125d3e34a205b07c7f095c7450))

- Sync Phase 3 from autumn monorepo([0ee395e](https://github.com/madmax983/autumn-harvest/commit/0ee395e142d34271b5eaa4c391254f0517235a20))

- Clippy + fmt clean across autumn-harvest Phase 2([9d44e2d](https://github.com/madmax983/autumn-harvest/commit/9d44e2d4f11826338399beffe235de290bd5e80c))

- **lib:** Remove stale task comment from context module declaration([11ce797](https://github.com/madmax983/autumn-harvest/commit/11ce79793270986a5bbbf65c0449a58e2a1bacb4))

- Fix workspace review issues (deadpool version, clippy msrv, gitignore, testcontainers deps)([1d99fc3](https://github.com/madmax983/autumn-harvest/commit/1d99fc3da51bbafb231d6743a51caa501176dae0))

- Initialize autumn-harvest workspace([55f4ceb](https://github.com/madmax983/autumn-harvest/commit/55f4ceb915fd88dc59249db9a98adf1f044c09a7))
