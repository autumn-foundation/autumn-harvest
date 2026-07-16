## Phase 3.52 — Cause-targeted bulk DLQ replay/discard + `dlq_reason`/`error_class` facets (issue #613)

**Before:** during a DLQ flood an operator could aggregate dead-letter counts by
workflow/activity/queue/failure-signature (`GET /dead-letters/aggregate`, shipped
in #385) but could **not** group by *why* entries failed, and could **not** target
a bulk replay/discard at a specific failure cause — `BulkDlqFilter` matched only
workflow/activity/time. "Replay every poison-pilled entry" or "discard everything
that failed with `CircuitOpen`" required scripting over the raw row list.

**After:** a two-call incident runbook. (1) `GET /dead-letters/aggregate?group_by=dlq_reason`
(or `error_class`) shows the flood's cause shape; (2) feed a facet's cause tuple
back into `POST /dead-letters/replay|discard` (or `harvest dlq replay/discard
--dlq-reason poison_pill`) to act on exactly that cohort. A facet's cause round-trips
into the bulk filter and re-selects exactly the rows it counted.

**Reconciliation with prior art (#385).** Per the #543/#593 reconciliation
precedent, #613's ACs were ground-truthed against the code first: the
faceted-aggregation half of #613 **already shipped in #385** (the `/aggregate`
route *is* the AC1 "summary" route — reconciled name; per-group
`first_seen`/`last_seen` == the AC's `earliest`/`latest`; `dlq::failure_signature`,
`merge_dlq_aggregates`, cross-shard fan-out, top-N rollup, time-window narrowing,
descending sort, and the CLI `dlq aggregate` command all reused verbatim). The
genuine residual shipped here is the **cause dimension** #385 did not ship: the
`dlq_reason`/`error_class` grouping facets (AC1 named them explicitly; #385 shipped
only `failure_signature`) and the entire cause-targeted **bulk filter** (AC4/AC5/AC6),
tied together by AC5's round-trip tuple `(workflow_name, activity_name, dlq_reason,
error_class)`.

**What shipped.**
- **Two pure classifiers** in `dlq.rs`, compute-on-read from the freeform `error`
  TEXT column (all production DLQ rows are `DeadLetterReason` tagged JSON):
  `dlq_reason(error)` → snake_case mechanism class
  (`poison_pill`/`history_cap_exceeded`/`workflow_task_timeout`/`callback_delivery_exhausted`,
  else `retry_exhaustion`); `error_class(error)` → typed activity `error_type`
  (`CircuitOpen`, …) if present, else the `DeadLetterReason` PascalCase tag, else a
  normalized leading token. Both bounded and deterministic. `failure_signature`
  reused unchanged.
- **Aggregate**: two new `DlqGroupDimension` variants (`DlqReason`, `ErrorClass`)
  plug into the existing load-and-classify pass — no new SQL.
- **Bulk filter**: `BulkDlqFilter` gains `error_class`/`dlq_reason`/`failure_signature`
  (compute-on-read Rust post-filter, applied before the limit) plus
  `queue_name`/`min_attempts` (SQL) so any aggregate scope is reproducible.
  `is_empty`/`MAX_BULK_LIMIT`/`dry_run` preserved. An empty/whitespace cause value
  is **rejected `400`** in both the JSON and form paths (an empty cause must never
  silently widen a bulk op).
- **CLI**: `dlq summary` (alias of `aggregate`);
  `--dlq-reason`/`--error-class`/`--failure-signature`/`--queue-name`/`--min-attempts`
  on `dlq replay`/`discard`.
- **Contract + CI**: `management_api_request_fields()` and `docs/api-contract.json`
  updated (contract-regression green); `dlq_aggregate_integration`/`dlq_bulk_integration`
  graduated from the CI ALLOWLIST into `.github/ci/integration-suites.txt` so the new
  tests run under Docker in CI.

**Invariants.** **No new `WorkflowEvent` variant, no migration, no `schema.rs`
column, no event-log write** (AC7) — read-mostly, plus the additive bulk-filter
dimension.

**Review + tests.** Multi-angle adversarial review, no P1. **P2-1** (aggregate
`queue_name`/`min_attempts` scope not reproducible in the bulk filter → cross-queue
over-action) fixed by adding those two SQL filters — `bulk_cause_plus_queue_round_trips_exactly`
proves a queue-scoped facet no longer over-acts. **P2-2** (cause matching is a
compute-on-read full-shard scan) documented — symmetric with the already-shipped
aggregate's accepted cost; `MAX_BULK_LIMIT` caps rows acted-on, narrowable by
time/queue/activity/workflow/min_attempts. Executed on real Postgres 16
(testcontainers): `dlq_bulk_integration` 17/17, `dlq_aggregate_integration` 14/14,
`contract_regression` 22/22, core `dlq` (`--features db`) 71/71; plus the pure-unit
tests `aggregate_group_by_dlq_reason_orders_by_count`, `aggregate_group_by_error_class`,
`aggregate_group_by_dlq_reason_merges_across_shards`, `bulk_cause_dry_run_count_equals_aggregate_facet`,
and CLI `request_mapping`/`contract_coverage` coverage.
