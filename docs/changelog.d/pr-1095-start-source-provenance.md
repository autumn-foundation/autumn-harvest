## Phase 3.51 — Workflow start provenance (`StartSource`) across every start path (issue #740)

**Implemented.** Every workflow execution now durably records *what created it, and from where* — the answer that previously required reverse-engineering `workflow_id` prefixes (`sched:…`) and hand-walking `parent_id`. A new bounded `StartSource` enum (`types.rs`, non-`db`-gated; snake_case serialization; `from_str` falls back to `unknown` and **never errors**) is threaded through `StartWorkflowParams` and `NewWorkflowExecution` and stamped at construction on every start path:

- schedule tick / backfill / manual trigger-now / Vantage UI trigger → `schedule` / `backfill` (both non-throttled workflow **and** DAG branches, plus the throttled/deferred branch) / manual-trigger with `start_source_ref = schedule_id` and `started_by = operator`;
- `signal_with_start` / `update_with_start`;
- completion-trigger (inline + deferred + cross-shard outbox), `start_source_ref = triggering source exec id`;
- child (fan-out / single / detached), `start_source_ref = parent exec id`;
- `continue_as_new` — records its **own** `continue_as_new` source (with `start_source_ref = predecessor`), never misattributed as a fresh `api` start (falsified in test by stamping the predecessor a *distinct* `schedule` source and asserting the successor does not inherit it);
- `reset` — own `reset` source + `start_source_ref = source exec id`;
- batch (immediate + throttle-carrier branches);
- generic outbox;
- inbound webhook (override via `StartWorkflowParams::from_webhook`);
- the deferred throttle / debounce / batch paths — source is captured at **admission** and restored at **fire** time from the JSONB start-options carrier;
- workflow-level retry — **inherits** the predecessor's source, so a retried scheduled run stays `schedule`.

Pre-upgrade rows report `unknown`.

**Storage.** Three additive nullable columns (`start_source`, `start_source_ref`, `started_by`) plus a partial index on `harvest_workflow_executions` (migration `20260712000000_harvest_execution_start_source`), with **no backfill**. The read struct serializes NULL `start_source` as `"unknown"` and omits `start_source_ref` / `started_by` when absent. **No new `WorkflowEvent` variant** and no change to the adjacently-tagged event JSON: provenance lives on a row column that is never read on replay, so the value is **replay-safe by construction** and immutable once frozen at start.

**Read / query surface.** `GET /workflows?start_source=` filter applied at **both** the standard (`load_workflows`) and stalled (`load_stalled_workflows`) loaders (mirroring the `sla_breached` / `nd_blocked` / `legal_hold` predicates), fanned across shards: an invalid value returns a `400` JSON error (never `500`, never a silent empty match); `?start_source=unknown` matches NULL/pre-upgrade rows via `IS NULL OR = 'unknown'`; a blank `?start_source=` is treated as absent (no filter), matching the `state` / `workflow_name` / `owner` siblings; omitting the param preserves today's behavior. AC4 read-path surfacing on the describe/details API and list rows is automatic (the `WorkflowExecution` read struct is serialized directly). CLI: `harvest workflow list --start-source <value>` + describe passthrough. `docs/api-contract.json` gains the `start_source` param entry. Operator triage stanza added to `docs/runbooks/triage-pending-tasks-idle-workers.md` (find the culprit mechanism during a spawn-storm with one call).

**Tests.** AC3 enumeration directly asserts the recorded `start_source` (and predecessor ref for `continue_as_new` / `reset`) for every listed start path, driven via public entrypoints where reachable and the module-local worker harness for the engine-internal insert paths (`continue_as_new`, `child`) — RED→GREEN against Postgres; backfill is asserted through `POST /admin/schedules/{id}/backfill` in the plugin suite (this direct assertion uncovered and fixed a real gap where the two non-throttled backfill branches still carried the neutral `api` placeholder). Filter/describe HTTP integration tests: filter narrows results (proves exclusion), `?start_source=unknown` matches NULL rows, invalid → `400`, absent ref/`started_by` omitted; deferred-carrier restore, webhook override, and workflow-retry inheritance each asserted end-to-end. Full local CI mirror green (`fmt`, per-crate clippy `-D warnings`, MSRV `check --workspace`, DB suites).
