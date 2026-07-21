# RED→GREEN findings — Area 1+2 (getting-started + core concept docs)

Sweep of autumn-harvest docs against released 0.5.0 API. Ground truth = crate source
(`autumn-harvest/src`, `autumn-harvest-plugin/src`, `autumn-harvest-cli/src`,
`autumn-harvest-macros/src`) + `docs/api-contract.json`. Legend: **FIX** = applied,
source-verified; **REVIEW** = left for a later phase (0.6-dep-sensitive / ownership /
judgment); **GAP** = missing coverage (added a mention/pointer where feasible).

Environment: Postgres NOT running here — DB examples compile-checked only; autumn-web 0.6
API not verifiable from this sandbox (release PR #1124 open draft), so 0.6 code-signature
questions are REVIEW, never guessed.

Deferred pins in my area (NEVER edited): `01-project-skeleton.md` L18-20
(`autumn-harvest="0.4"`/`autumn-harvest-plugin="0.4"`/`autumn-web="0.5"`);
`telemetry.md` L18 (`autumn-harvest-plugin` `version="0.4"`); `replay-verify.md` L29
(`autumn-harvest` `version="0.3"`). Verified untouched.

---

## Confirmed source facts used throughout

New-in-0.5.0 `WorkflowContext` APIs confirmed present in `context.rs`: `start_timer`
(returns `TimerHandle<'a>`; `TimerOutcome` enum), `cancel_timer`, `reset_timer`,
`sleep_until`, `try_receive_signal`, `drain_signals`, `drain_signals_raw`,
`drain_signals_collect`, `try_wait_for_signal`, `execute_child_workflow_timeout`,
`spawn_child_workflow_timeout`, `patched`, `deprecate_patch`, `info`, `mutex`,
`publish_progress`, `await_external_workflow`. DAG signal gates confirmed in `dag.rs`
(`GateTimeoutAction`, `DagSignalGate`, `DagBuilder::signal_gate`,
`signal_gate_with_timeout`). `WorkerConfig::with_workflow_panic_max_attempts` +
`workflow_panic_max_attempts` field confirmed in `builder.rs`.

Metric catalogue extracted from `telemetry.rs` (used for telemetry.md cross-check) — see
the telemetry.md section below.

---

## docs/mcp-tools.md

- **FIX** L13: "Built on autumn-web **0.5**'s MCP layer" -> "autumn-web **0.6**'s MCP
  layer". Pure version-narrative prose, no adjacent Cargo pin in this file (verified).
  The 0.5.0 release ports to autumn-web 0.6 (#1124), and this docs PR merges after it.
  The `AppBuilder::mount_mcp("/mcp")` API name in the same paragraph is left unchanged
  (REVIEW — verify against autumn-web 0.6; not a version string).

## docs/getting-started/01-project-skeleton.md

- **GAP -> FIX**: the chapter never mentioned `harvest new` (issue #692), the shipped
  scaffolding CLI (`autumn-harvest-cli/src/lib.rs` `New` variant ~L631; template dir
  `autumn-harvest-cli/templates/minimal/`). Added a "Shortcut" callout pointing at
  `harvest new <name>`, above the manual `Cargo.toml` block. Did NOT touch the deferred
  pin lines (L18-20).
- Verified the manual `main.rs` wiring (`autumn_web::app().plugin(HarvestPlugin::new()
  .worker(...).api("/api/harvest")).run()`) matches the shipped template
  `main.rs.tmpl` pattern (`.plugin()`, `.worker()`, `.api()`). **REVIEW**: the doc's
  zero-route hello-world may need a `.routes(...)` under autumn-web (the template adds
  `.routes(routes![index])` with a comment "autumn-web requires at least one HTTP
  route") — left as-is (pre-existing, 0.6-sensitive).

## docs/api-contract-guide.md

- **REVIEW** L14: example `"version": "0.4.0"`. The guide says the contract version
  "matches the `autumn-harvest-plugin` crate version". That crate bumps to 0.5.0 via
  release PR #1125, which also owns `docs/api-contract.json`'s `version` field (L2, also
  `"0.4.0"`). Editing either here risks conflicting with #1125 (R6); the version field is
  release-process-owned, not docs-owned. Left both as-is; flagged for the release/gate
  phase to reconcile once #1125 lands.

## docs/typed-workflow-failures.md

- **OK** (no fix needed): L72-76 correctly documents the #767 change — a failed child now
  surfaces as `HarvestError::WorkflowFailed` (the `ActivityFailed{name:"child-workflow:.."}`
  mention is explicitly the *pre-#767* behavior, in the "Behavior change (upgrading)"
  section). Accessors `workflow_error_type()`/`workflow_details()`/
  `is_workflow_non_retryable()` all verified in source.

<!-- Remaining per-file findings appended below as agent evidence is integrated. -->

---

## Batch 2 (this worker, continuing from 6e8b6262)

### Mechanical stale-pattern sweep across ALL area 1+2 docs — CLEAN
- `rate_limit_saturated` (#611 rename → `_exhausted`): **0 hits** in my docs.
- Failed-child `HarvestError::ActivityFailed{child-workflow:..}` (#767): only 1 hit,
  `typed-workflow-failures.md:74`, and it is the deliberate *pre-#767* behavior note in
  the "Behavior change (upgrading)" section — **correct, no fix**.
- pause/resume `409` (#609 → 200): **0 stale hits** in operations/management-api.
- `autumn-web 0.5` prose (non-pin): **0 hits** (mcp-tools already fixed to 0.6 in the
  prior commit).
- `0.4.0` narrative: **0 hits** in these docs (the only 0.4 refs are the DEFERRED Cargo
  pins, left untouched).

### docs/getting-started/03-durable-timers.md — VERIFIED
- `start_timer` → `TimerHandle`, `cancel`/`reset`/`await_fire`, `TimerOutcome::{Fired,Cancelled}`,
  `sleep_until` all confirmed in `context.rs`. No fix.

### docs/getting-started/04-signals.md — VERIFIED
- `await_condition`/`await_condition_timeout` (context.rs:7007/7015),
  `signal_external_workflow_with_idempotency` (7437), `idempotency_key()` (11563),
  plugin `.signals`/`.queries`/`.updates` (plugin.rs:201/208/215),
  `SignalHandlerInfo::with_arg_schema_fn`/`with_schemas`/`validate_arg` (info.rs) all
  confirmed. `/workflows/registered/{name}/interface` (#610). No fix.

### docs/getting-started/07-reliability-knobs.md — VERIFIED
- `should_continue_as_new` (3503), `deadline` (3082), `time_until_deadline` (3135);
  builder `with_default_activity_retry_policy`/`with_default_activity_start_to_close`,
  `history_continue_as_new_deadline_fraction` (1688), `max_workflow_chain_timeout` (1754);
  `patched`/`deprecate_patch`/`version`; `METRIC_WORKFLOW_CHAIN_TIMEOUT`
  (`harvest.workflow.chain_timeout`, telemetry.rs:351); `harvest concurrency status` CLI.
  All confirmed. No fix.

### docs/getting-started/08-dags-and-schedules.md — VERIFIED
- DAG signal gates (`signal_gate`/`signal_gate_with_timeout`/`GateTimeoutAction`, #746),
  node input binding (`input_from`/`_all`/`_aliased`, #702), dynamic mapping, MCP
  exposure, `#[dag(mcp)]`, carryover (`last_completion_result`/`last_error`, #488) all
  present and source-consistent. No fix.
  - **REVIEW (out of area/scope, noted for Area 3):** in-place schedule update
    `PATCH /admin/schedules/{id}` (#771) is not mentioned; the chapter says "Resume by
    patching it back to active" generically. That's a management-API/Area-3 concern.

### docs/getting-started/10-operations.md — VERIFIED
- `dlq bulk-discard --activity-name` / `bulk-replay` / `dlq_reason` facets (#613),
  `worker drain-preview`/`drain`, `concurrency status` CLI all confirmed. #685 conflict
  policies section accurate (admin-auth-iff-can-cancel, deferred-start 400). No fix.

### docs/getting-started/11-testing.md — VERIFIED
- `WorkflowReplayer`, `WorkflowTestEnv`, `TestRunOutcome::{final_now,elapsed}`,
  `queue_signal`, virtual-clock contract table incl. `receive_signal_timeout`. No fix.
  - **GAP (minor, not fixed):** #541 "WorkflowSimulator honors RetryPolicy" — the
    simulator retry-honoring detail isn't surfaced here; judged not a "reader expects it"
    gap in the WorkflowTestEnv/Replayer chapter. Left.

### docs/getting-started/12-webhooks.md — VERIFIED
- `#[webhook(path=, starts=/signals=+signal_name=)]`, `webhooks![]`, `WebhookCtx`,
  `.webhooks(...)` plugin method, response-shape table, `harvest.webhook.received/rejected`,
  audit op `webhook.trigger`, #808 upstream-`Idempotency-Key`-not-a-start-key note. Accurate.
  - **REVIEW:** `autumn_web::webhook::SignedWebhook` + `[security.webhooks]` are
    autumn-web-0.6-surface *type/config path* references (not version strings). Left as-is;
    a later phase verifies against the autumn-web-0.6 branch.

### docs/getting-started/activities.md — FIX (coverage gaps closed)
- **GAP → FIX:** added an "Automatic liveness heartbeats" callout to the Heartbeating
  section pointing at `ctx.start_auto_heartbeat(interval)` / `start_auto_heartbeat_default()`
  → `AutoHeartbeatGuard` (issue #682, confirmed context.rs:11838/11926). Requires
  `heartbeat_timeout`; `#[must_use]` RAII guard.
- **GAP → FIX:** added a "Cross-cutting behavior — activity interceptors" section pointing
  at `HarvestBuilder::activity_interceptor(impl ActivityInterceptor)` (issue #680, confirmed
  builder.rs:1520; example `examples/activity_interceptor.rs`). Neither had any prior
  mention in activities.md, the natural home for both.

### docs/getting-started/01,02,06,09 — VERIFIED
- 01: `harvest new` callout (prior commit) correct; deferred pins L26-28
  (`autumn-harvest="0.4"`/`plugin="0.4"`/`autumn-web="0.5"`) VERIFIED UNTOUCHED. REVIEW
  (0.6-sensitive, pre-existing): zero-route hello-world `main.rs` may need `.routes(...)`
  under autumn-web (the scaffold template adds one) — left.
- 02: `#[activity]` attr table, `execute_activity_raw`, plugin registration — accurate.
- 06: #808 `idempotency_key` vs `workflow_id`, #521/#753 signal dedup, `ctx.idempotency_key()`
  (11563) + `subkey` (types.rs:1056), `start_idempotency_window` (builder.rs:1777) — all confirmed.
- 09: `with_queues`/`with_queue_weights`/`with_label`/`with_labels` (builder.rs 3046/3072/3346/3353),
  `requires` attr, `capable_of`, eligibility endpoint, `harvest.queue.dispatched` — accurate.

### docs/getting-started/05-child-workflows.md — FIX (coverage)
- **GAP → FIX:** added a "Bounding and fanning out children" section pointing at
  `execute_child_workflow_timeout` (#779, context.rs:6780; example `child_with_timeout.rs`)
  and `spawn_child_workflow_fan_out` (#601, context.rs:8954; example `fanout_child_workflows.rs`).
  Both are matrix-targeted at Ch.5 and had no prior mention.

### docs/getting-started/07-reliability-knobs.md — FIX (API drift)
- **DRIFT → FIX:** L65 `WorkerConfig::default().queues(vec![...])` → `.with_queues([...])`.
  There is **no bare `queues()` method** on `WorkerConfig` (only `with_queues`,
  builder.rs:3046). Repo-wide grep confirms this was the only `.queues(` occurrence in my area.

### docs/telemetry.md — VERIFIED
- Metric constants spot-checked against telemetry.rs (`harvest.retention.summary_deleted`,
  `harvest.mutex.wait_duration`, `harvest.canary.roundtrip`, `harvest.schedule.overdue`,
  `harvest.workflow.panic`, `harvest.update.duration`) — all OK. Catalogue is CI-cross-checked
  by `dashboard_pack_docs.rs`. Includes all matrix "new metrics" (#782/#770/#752/#781/#801/#691/
  #344/#607/#611-adjacent). No fix.
- DEFERRED PINS **untouched**: L18 `autumn-harvest-plugin = { version = "0.4" ...}`, and
  L94 `autumn-harvest = { version = "0.2" ...}` (a second resolvable pin NOT enumerated in
  plan §6 but treated as deferred per R1/R2 — left as-is).

### docs/streaming-progress.md — VERIFIED
- `ctx.publish_progress`, `GET /workflows/{id}/stream` SSE, seq/dedupe contract, auth posture,
  scope-vs-#473/#527/#324/#790 table (#791). Accurate. No fix.

### docs/workflow-determinism-guide.md — FIX
- **BROKEN LINK → FIX (AC6):** L62 `[WorkflowReplayer](file:///c:/Users/markm/autumn-harvest/docs/replay-verify.md)`
  — an editor-inserted absolute local Windows filesystem path — replaced with the relative
  `replay-verify.md`. Unambiguous bug regardless of org/version.
- **ORG RENAME → FIX (partition-authorized):** 6 `github.com/madmax983/autumn-harvest/issues/N`
  links (all harvest's OWN issue tracker, uniform mapping) → `github.com/autumn-foundation/...`.
  Partition L59: "correct harvest's OWN repo/badge/DeepWiki links". These were the ONLY
  `madmax983` occurrences in the entire area (concentrated in this one file); repo-wide there
  are 41 (rest owned by Area 5's holistic sweep — mapping is idempotent so no conflict).
- Content VERIFIED current: HVG010 (#600) + combinator functions (#799), HVG011/DET010 (#785),
  DET011 (#799), `harvest det-check` CLI (#778), `ctx.metrics()` replay-safe metrics under
  HVG007 (#758/#532), `ctx.patched()`/`deprecate_patch()` in the release playbook. No content fix.

### docs/management-api.md — FIX (coverage) + VERIFIED
- Content VERIFIED: SSE stream (#174), by-id addressing (#805), list filters + typed
  search-attr predicates (#506/#159), stack heartbeat checkpoint (#503), history
  pagination (#529), updates result API, signal delivery idempotency (#521/#753). Accurate.
  No stale pause/resume 409 (the only pause/resume mention is the by-id delegation table).
- **GAP → FIX:** the page had **no pointer to the authoritative route registry** and no
  mention of the new-in-0.5.0 route families. Added a "Full route registry" section at the
  top pointing at `docs/api-contract.json` + `api-contract-guide.md`, and listing 13 new
  0.5.0 route families by method/path (all confirmed present in api-contract.json):
  `/workflows/summaries` (#752), `/workflows/count` (#544), `/workflows/{id}/run-chain`
  (#701), `/workflows/{id}/timeline` (#739), legal-hold (#747), fail-now (#765),
  completion-deliveries (#605), `PATCH /admin/schedules/{id}` (#771), `/admin/status` (#679),
  `/admin/config` (#695), `/admin/usage` (#596), workflow-types/reachability (#520),
  `/dags/{name}/runs/{run_exec_id}` graph (#690). Deep per-route sections deferred (contract
  is authoritative); this meets the "at least a mention" floor.

### docs/saga.md — VERIFIED
- #801 observability (both counters, exactly-once marker contract, per-unwind coherence,
  cancel limitation), cancel-does-not-auto-compensate, idempotency + replay-determinism
  contracts. Test paths correctly say `tests/integration/saga_tests.rs`. No fix.

### docs/retry-jitter.md — FIX (stale test path)
- **STALE PATH → FIX:** L45 `autumn-harvest/tests/replayer_tests.rs` →
  `.../tests/integration/replayer_tests.rs` (the test suite was reorganized into
  `tests/integration/`; the root file no longer exists — verified). API (`JitterPolicy`,
  `with_jitter`, `next_delay_with_seed`, bench, quickstart bin) all confirmed.
- Broad sweep of every `tests/NAME.rs` ref across all area docs: only this one was stale;
  the two mcp-tools.md refs are correctly `autumn-harvest-plugin/tests/...` (exist).

### docs/sharding.md — VERIFIED
- Per-key concurrency shard-local scope (#247), `GET /admin/concurrency`, `GET /admin/usage`
  (#596) with metric semantics, debounce shard-local (#499), per-key activity rate limits
  (#699), durable mutex shard-local (#691, `harvest_mutex_locks`/`harvest_mutex_waiters`),
  cross-shard keyset pagination + `search_attr_filter` pushdown (#506), add-a-shard runbook
  (#522). Narrative concept doc, matches CLAUDE.md. No fix.

### docs/completion-callbacks.md — VERIFIED
- Matches Phase 3.46 (#605) exactly: SSRF validator (HTTPS-only + allowlist), HMAC envelope
  + headers, `harvest_completion_deliveries`, at-least-once/`delivery_id`, retry/backoff/DLQ,
  `CompletionCallbackDeliverer` trait, list/redrive routes, CLI, builder-config table. No fix.

### docs/completion-triggers.md — VERIFIED
- Matches Phase 3.48: output guards (#810, operator set, caps, fail-closed, exactly-once
  skip, rollout ordering), `Outcome` input mapping (#748), `Terminated` terminal state (#504),
  `harvest.completion_trigger.fires`/`.skipped` metrics, admin API. Very thorough. No fix.

### docs/transactional-activities.md — VERIFIED
- `ctx.run_transactional` (context.rs:12123), idempotency guard, restrictions (final-expr,
  no local-activity, no test-ctx, heartbeat, retry). `ctx.idempotency_key()` exists
  (context.rs:11563). Interceptor self-commit interaction (#680) consistent. No fix.

### docs/sticky-routing.md — VERIFIED
- Phase 3.11 (#235): `StickyRoutingConfig`, `WorkerConfig::with_sticky_routing`, `lease_ttl`,
  hard-exclusion-during-lease semantics, cache_hit/cache_miss metrics, `workflow_cache_size`
  default 1000 (builder.rs:3009), build-id/shard/continue-as-new interactions. No fix.

### docs/search-attributes.md — VERIFIED
- `ctx.upsert_search_attrs` merge semantics, reserved keys (`exec_id`/`workflow_name`/
  `shard_id`/`status`/`run_id` + `_harvest` prefix), replay-safety, `BatchFilter`,
  `search_attr_filter` op grammar (#506), GIN durability. No fix.
  (Note: #603 also reserves six ND diagnostic keys under `_harvest`-adjacent names via
  `RESERVED_SEARCH_ATTR_KEYS` — internal, not user-facing; not worth adding to the doc.)

### docs/replay-verify.md — VERIFIED (contains DEFERRED PIN L29 — untouched)
- `ReplayVerifier` (testing.rs:2271), `verify_all`/`fixtures_dir`/`into_ci_report`/
  `into_ci_report_with_threshold`/`allow_unregistered`, `ReportFormat` (Text/JUnit/Json/
  GitHub), exit codes 0/1/2, `--fail-on rate=`, GH Actions snippet, #251 perf budget. All
  confirmed. **L29 `autumn-harvest = { version = "0.3" }` is a DEFERRED PIN — NOT edited.**

### docs/api-contract-guide.md — FIX (contract_version drift) + REVIEW
- **DRIFT → FIX:** L37 said `contract_version` is "currently `"1"`" but the live
  `docs/api-contract.json` has `contract_version: "2"`. Corrected to `"2"`.
- **REVIEW:** L14 `"version": "0.4.0"` is an illustrative example that currently MATCHES the
  live contract file (`version: 0.4.0`); PR #1125's crate bump will move both to 0.5.0. Left
  as-is (editing to 0.5.0 now would be wrong until #1125 lands). Flag for the version-bump PR.

### docs/calendars.md — FIX (non-compiling snippet)
- **DRIFT → FIX:** the `WorkflowSchedule` builder snippet used `.with_timezone("...")`, but
  no such method exists on `WorkflowSchedule` (verified full method list, policy.rs:975-1241).
  Timezone is expressed via the `Schedule::CronInTimezone { expr, tz }` variant. Rewrote the
  snippet to construct `CronInTimezone` in `WorkflowSchedule::new`. (The HTTP-API `timezone`
  JSON field is a plugin-side field and is correct.)
- VERIFIED: migration `20260519000000_harvest_calendar_awareness`, `SkipPolicy`
  Skip/RunNextBusinessDay/RunPrevBusinessDay, `is_excluded_date`/`apply_skip_policy` pure fns,
  all calendar CRUD + preview routes present in api-contract.json, `with_calendar`/
  `with_skip_policy` builders. observability `reason="calendar"`.

### docs/archival.md — FIX (imprecise error/method claim)
- **DRIFT → FIX:** the retention-override comment claimed "`.build()` fails with
  `HarvestBuilderError::UnknownRetentionOverrideWorkflow`" — but `build()` PANICS
  (`try_build().expect(...)`, builder.rs:1934); only `try_build()` returns the error. Both
  variants exist: `UnknownRetentionOverrideWorkflow` (unknown type, builder.rs:2243) and
  `InvalidRetention` (bad value, builder.rs:1951). Rewrote the comment to state `build()`
  panics / `try_build()` returns the typed error, and named both variants.
- VERIFIED: `HistoryArchiver` trait shape, zero-loss guarantee, `RetentionConfig::with_max_age`/
  `with_workflow_override` (#737)/`with_audit_retention_days`/`with_schedule_decision_retention_days`,
  `harvest.retention.deleted{workflow}` label (#737), `history_archiver` builder method.
