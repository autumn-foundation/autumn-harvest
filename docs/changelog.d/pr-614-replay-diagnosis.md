## Phase 3.52 — Single-execution replay diagnosis endpoint (issue #614)

New read-only, admin-gated management route `POST /api/harvest/workflows/{id}/replay-diagnosis`
that loads ONE execution's recorded history from its owning shard and replays it against the
**currently-registered** `#[workflow]` handler via the already-reachable
`WorkflowReplayer` (`autumn_harvest::testing`, already enabled since the plugin builds core with
`features = ["db","testing"]`), returning a structured determinism verdict. It answers, on demand
for one specific run, the same question the fleet-wide #480/#603 non-determinism-block incident
signal raises: "will — or did — this run diverge under the code I have deployed right now?" — the
headline use case being a PAUSED or non-determinism-blocked RUNNING run, or a terminal FAILED run
for retroactive forensics (AC4).

**Verdict shape.** `200` for every reachable diagnosis (the diagnosis, not the HTTP status,
carries the answer): `clean` (message `"no divergence under current code"`, AC6), `diverged`
(with a `divergence { kind, event_index, expected, actual }` object mirroring the #603 block
diagnostic vocabulary so an operator can cross-check it against a blocked run's `search_attrs`,
AC2), `workflow_failed` (with `failure { error, event_index }`), `not_registered` (the workflow
type is not registered on this node), or `not_replayable_dag` (a classic non-unified DAG is not on
the replay path). The AC5 `not_registered`/`not_replayable_dag` verdicts are resolved by a
pre-check (`runtime.registry.workflows.contains_key` then `runtime.is_registered_dag`) BEFORE any
history is loaded. Non-`200` statuses are reserved for the resource itself: `400` (malformed id),
`404` (unknown execution), `408` (replay exceeded the `WorkerConfig::query_timeout` budget), `410`
(history unavailable — pruned by retention, released on reset, or PII-erased per issue #495, gated
by the terminal-only O(1) `erase::execution_input_is_erased` row check, mirroring #612).

**Read-only / zero-writes (AC3).** New pure plugin module `autumn-harvest-plugin/src/replay_diagnosis.rs`
owns the serializable DTOs (`ReplayDiagnosisResponse`, `DiagnosisVerdict`, `DivergenceDetail`,
`FailureDetail`) and the total `report_to_response` / `not_registered_response` /
`not_replayable_dag_response` mapping (every `ReplayStatus` variant → a distinct verdict),
unit-tested with no DB or async. The `api.rs` handler mirrors `hydrate_ctx_for_query`'s (#612)
discipline: load the execution (shard-routed via `db_conn_for_execution`), run the erased/410 and
AC5 pre-checks, load the history, **drop the DB connection**, then build a `HistorySnapshot` from
the execution row + history (threading its own `execution_timeout`/`deadline_at`/`parent_id`/
`workflow_id`/`context_headers` per #772/#698/#481 so a deadline-/parent-/header-aware run replays
cleanly instead of false-reporting non-determinism) and replay via
`WorkflowReplayer::replay_from_snapshot` (drop-first, so no pool slot is held during the replay —
chosen over `replay_from_db`, which holds its connection across the whole replay). The replay is
bounded by `query_timeout` via `tokio::time::timeout`; on the timer winning, `408` — the deadline
bounds async-yielding replays, and a synchronous busy-loop workflow is out of scope here (it blocks
a worker thread rather than yielding) and is caught instead by the #494 workflow-task timeout. The
DTO layer is pure in-memory replay: no events appended, no state recomputed, no audit row written.

**No feature-gate change, no core-signature change, no migration, no new `WorkflowEvent` variant.**

Route registered admin-gated (`.route_layer(require_admin)`) in the axum router, in all four
parallel plugin registries (`management_api_routes()`, `management_api_request_fields()` = `Some(&[])`,
`management_api_response_fields()`), `docs/api-contract.json` (`read_only: true` +
`post_for_body_only: true`, the same POST-but-read-only precedent as the query route), and the three
`autumn_harvest::audit` route lists (`CLASSIFIED_ROUTES` `ReadOnly` / `ALL_MUTATION_ROUTES` `None` /
`EXCLUDED_ROUTES`) with dedicated pinned classification tests in both `audit.rs`
(`replay_diagnosis_route_is_classified_read_only`) and `contract_regression.rs`
(`replay_diagnosis_route_is_classified`) — the pins catch the route being dropped from BOTH lists at
once, which the mutual cross-check guards miss. CLI: `harvest workflow replay-diagnosis <id>` (bodyless
POST, mirroring `resume`/`retry-now`), with mapping + coverage tests.

**AC9 runbook / alert wiring.** `docs/runbooks/nondeterminism-block.md` gains a "Diagnose the
divergence" triage section (the curl, the `diverged { kind, event_index, expected, actual }` body,
the confirm-a-fix-is-clean and pinpoint-the-reset-`event_index` workflows), and the #480/#603
`harvest_workflow_non_determinism` starter-pack alert (`docs/alerts/starter-pack-v0.1.0.json`) lists
the endpoint as a recommended `management_checks` next step (plus a `#614` dependency entry).

Tests, TDD red→green: 6 pure unit tests in `replay_diagnosis.rs` (every `ReplayStatus` → verdict
mapping, the two not-replayable builders, snake_case serialization) — RED was captured on the
`audit.rs` pin (route absent from the class lists → `test result: FAILED`) and confirmed GREEN after
wiring; DB/HTTP integration tests in `autumn-harvest-plugin/tests/replay_diagnosis_integration.rs`
(clean, diverged, FAILED-retroactive, 404, 400, not_registered, not_replayable_dag, erased→410,
zero-writes, and the headline nd-blocked-RUNNING → diverged). Registered as one sorted manifest line
in `.github/ci/integration-suites.txt`; compile-checked in the authoring sandbox (no Docker) and run
Docker-backed on Linux in CI, per the #543/#544/#601 precedent.
