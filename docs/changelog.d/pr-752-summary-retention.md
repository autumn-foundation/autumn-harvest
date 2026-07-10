## Phase 3.51 — Tiered / summary retention (issue #752)

When the history-retention janitor (issue #737) hard-deletes a terminal execution, it can now optionally **demote** it into a compact, queryable `harvest_execution_summaries` row instead of losing all trace of the run — a "tiered" retention policy where full histories expire on one horizon and lightweight summaries live (far longer, or forever) on a second, decoupled horizon. The summary answers "did this run exist, when, and what did it produce?" in one management-API call at ~<1 KB/execution, long after the event log is gone. **No new `WorkflowEvent` variant, no replay impact** — a summary is a retention-time projection of terminal state, never part of the append-only event contract (AC8).

### Configuration (AC1)

New `RetentionConfig.summary: Option<SummaryPolicy>` (`policy.rs`/`retention.rs`); `None` (default) is **byte-for-byte identical to pre-#752 behavior** — hard delete, no summary. `SummaryPolicy { retention: SummaryRetention, capture_payload: bool, max_payload_bytes: usize }` with `SummaryRetention::{For(Duration), Unbounded}`. Builders: `SummaryPolicy::for_days(u64)` / `::for_duration(Duration)` / `::unbounded()`, chained `.with_payload_capture()` / `.with_max_payload_bytes(usize)`; on `RetentionConfig`: `with_summary_retention(SummaryPolicy)`, `with_summary_retention_days(u64)`, `with_summary_retention_unbounded()`. Default payload cap `DEFAULT_SUMMARY_PAYLOAD_CAP` = 4 KiB. `validate()` bounds the summary horizon by the same `[MIN_MAX_AGE, MAX_MAX_AGE]` range as `max_age`; `enabled()` returns `true` for an overrides-only or summary-only config so the janitor still spawns.

### Atomic demotion in the delete transaction (AC2/AC7)

Summarization happens **inside `delete_candidate_execution`'s existing `FOR UPDATE` transaction**, *after* the #747 legal-hold re-check and *before* the execution-row delete (the `.for_update()` select is widened to also fetch `output`/`error`/`search_attrs`/timing/shard/identity/state). The `INSERT` (with `ON CONFLICT DO NOTHING`) is atomic with the delete: there is **no window where both the execution and its summary are absent**, and a rolled-back delete leaves no orphan summary. A legally-held execution is exempt from deletion **and** is never summarized until released. Summary disabled ⇒ no `INSERT` at all.

### Opt-in capped payload with typed markers (AC3)

Captured `result`/`error` payloads are **opt-in** (`capture_payload`, default off ⇒ identity + timing + search-attrs only) and byte-bounded. Pure helpers (`retention.rs`): `cap_result_payload(output, cap)` stores the value **verbatim** (codec-encoded form preserved, matching the history-export *Full* policy — the summary is not decoded/redacted here; read-path decode #608 is a follow-up), except:
- an **offload reference envelope** (#524) becomes `{"_harvest_omitted": true, "reason": "offloaded"}` — **never** store a blob ref, whose backing blob may be GC'd out from under the summary;
- an oversized value becomes `{"_harvest_omitted": true, "reason": "too_large", "bytes": N}` (valid JSON, never a silent truncation);
- `cap_error_text` maps oversized error text to `[omitted: too_large, N bytes]`.

Migration `20260710000000_harvest_execution_summaries` adds the table (`execution_id` PK, `workflow_name`/`workflow_id`/`state`, `started_at`/`completed_at`/`duration_ms`, `shard_id`, `search_attrs` JSONB, `result` JSONB, `error` TEXT, `summarized_at`) with a `(completed_at, execution_id)` GC/keyset index, a `(workflow_name, completed_at DESC)` filter index, and a GIN index on `search_attrs`. `ExecutionSummary`/`NewExecutionSummary` models in `models.rs`, schema in `schema.rs`.

### Read-only list route (AC4)

New admin-guarded route **`GET /api/harvest/workflows/summaries`** (handler `list_workflow_summaries` in the plugin). Registered **before** `/workflows/{id}` (per the #544 `/workflows/count` precedent) so axum does not capture `summaries` as a path param. Filters mirror `GET /workflows`: `workflow_name`, `workflow_id`, `state` (repeatable/CSV), `completed_after`/`completed_before` (RFC 3339), `search_attr` (repeatable `key:value` JSONB containment). Same keyset-cursor pagination contract as `GET /workflows` (#498), here keyed on `(completed_at DESC, execution_id DESC)`; `limit` default 50 / max 500. **Cross-shard fan-out + merge** (summaries are shard-local) via the core helper `retention::list_execution_summaries(conn, &SummaryQuery, limit)` per shard, then a k-way merge + truncation + `next_cursor` derivation in `load_summaries_from_shards`. Malformed params return `400` (never `500`, never a silent empty match); an unreachable shard fails the call (matching `GET /workflows`). Each response item is the summary columns **plus `summarized: true` and `history_available: false`** so a summary is unambiguously distinguishable from a live execution. Admin-guarded because a summary may carry retained/redacted payloads. Registered in all four plugin registries (router, `management_api_routes()`, `management_api_response_fields()` as free-form `None`), `docs/api-contract.json` (read-only, `admin` category), and `autumn_harvest::audit` (`CLASSIFIED_ROUTES` ReadOnly / `ALL_MUTATION_ROUTES` None / `EXCLUDED_ROUTES`), with a pinned `workflow_summaries_route_is_classified` test in `contract_regression.rs`.

### Summary GC pass + metric (AC5)

Second GC pass `purge_expired_summaries(conn, shard, summary_age, batch, dry_run, now)` folded into `RetentionRuntime::spawn` next to the audit/schedule purges. Selection is by **`completed_at < now - summary_age`** (deterministic, anchored on the original run window — *not* `summarized_at`); `Unbounded` never GCs; batched `DELETE ... RETURNING workflow_name` to bound the transaction. Emits the new **`harvest.retention.summary_deleted{workflow}`** counter (`METRIC_SUMMARY_DELETED`, `record_summary_deleted(&str, u64)` no-op-default trait method on `MetricsRecorder`, bridged in `metrics_rs_adapter`) — a **distinct metric in the retention family** so summary GC is observable separately from history deletion. `summarized_count` added to `RetentionTickResult`/`ShardTickOutcome` (real deletes only; a `dry_run` tick creates no summaries) and surfaced on `GET /admin/retention` for creation observability.

### PII erasure scrubs the summary too (AC6)

`erase_workflow_payloads` (#495) extended: it now **also scrubs a matching `harvest_execution_summaries` row** in the same transaction via `erase_execution_summary`, tombstoning `result` and `search_attrs` with the shared `{"_harvest_erased": true}` marker (the summary's `error` is kept, consistent with #495's stance on operational error text). Critically, erase **succeeds when only the summary remains** (the full execution row was already retention-deleted → the terminal gate returns `NotFound`, but the summary scrub reports success), so a summarized execution's PII is never un-erasable once its history is collected. The HTTP handler is unchanged: it returns `200` on the core's `Ok` (which now covers the summary-only case) and `404` only when neither an execution nor a summary exists. `EraseOutcome.summary_scrubbed` reports the outcome.

### CLI

`harvest workflow summaries [--workflow-name --workflow-id --state --completed-after --completed-before --search-attr --limit --cursor --json]` maps 1:1 to `GET /workflows/summaries` (query-encoded via the shared helper). Table output by default, `--json` for raw. Mapping tests in `request_mapping.rs`, coverage in `contract_coverage.rs`.

### Tests

Core: pure `SummaryPolicy` builder/validate + cap-helper (verbatim/too_large/offloaded/error-cap) unit tests and `enabled()`/`summary_gc_active()` truth tables in `retention.rs`; metric no-op + `metrics_rs_adapter` bridge tests; erase pure-tombstone tests in `erase.rs`; DB integration tests in `tests/integration/retention_summary_tests.rs` (atomic demotion, legal-hold exemption, payload cap/marker, GC-by-`completed_at`, erase-scrubs-summary). Plugin: HTTP integration tests in `tests/workflow_summaries_integration.rs` (list-with-flags, filter-by-name/state/completed-range/search-attr, keyset pagination round-trip with no dupes/gaps, admin-guard 401, malformed-param 400, and summary-only erase → 200 + tombstoned `result`/`search_attrs`). New test files wired into `.github/workflows/ci.yml` and `tests/integration/mod.rs`.

### Success metric

After a GC tick that hard-deletes a run's history, a single management-API call (`GET /workflows/summaries?workflow_name=…`) returns that run's identity, terminal state, timing, and (opt-in, capped) result in well under a second, at ~<1 KB/execution — no full-history scan, no per-shard hand query.
