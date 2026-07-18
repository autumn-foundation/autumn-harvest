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
AC5 pre-checks, load the history (via `store::load_history_inflated` with the runtime's own
`PayloadCodecs` + registry offloader, so an encrypted (#608) or offloaded (#524) history replays as
the live worker sees it — the default no-codec/no-offloader path is byte-identical to `load_history`),
**drop the DB connection**, then build a `HistorySnapshot` from the execution row + history
(threading its own `execution_timeout`/`deadline_at`/`parent_id`/`workflow_id`/`context_headers` per
#772/#698/#481 so a deadline-/parent-/header-aware run replays cleanly instead of false-reporting
non-determinism) and replay via `WorkflowReplayer::replay_canary_snapshot` (drop-first, so no pool
slot is held during the replay — chosen over `replay_from_db`, which holds its connection across the
whole replay). The replay is bounded by `query_timeout` via `tokio::time::timeout`; on the timer
winning, `408` — the `query_timeout` bound applies to async-yielding replays, and a workflow that
busy-loops synchronously without ever `.await`-ing is out of scope, exactly as for the live executor.
A large but healthy history is not at risk from the executor's 100 ms `SUSPENSION_TIMEOUT`: that
per-cycle heuristic fires only when the handler future is genuinely *pending* on an unresolved oneshot
at the replay frontier — it never cuts off a CPU-bound replay consuming recorded events, so a
completed history replays to its verdict regardless of wall-clock duration (bounded only by the outer
`query_timeout`, ample headroom for the ~<200 ms/10k-event replay budget, issue #135). The
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

Tests, TDD red→green: 11 pure unit tests in `replay_diagnosis.rs` (every `ReplayStatus` → verdict
mapping, the two not-replayable builders, snake_case serialization, and the five
`reclassify_sealed_mid_await` cases below) — RED was captured on the `audit.rs` pin (route absent
from the class lists → `test result: FAILED`) and confirmed GREEN after wiring; DB/HTTP integration
tests in `autumn-harvest-plugin/tests/replay_diagnosis_integration.rs` (clean, diverged,
FAILED-retroactive, 404, 400, not_registered, not_replayable_dag, erased→410, zero-writes incl. no
task-queue enqueue, the headline nd-blocked-RUNNING → diverged, and the three post-review scenarios
below). Registered as one sorted manifest line in `.github/ci/integration-suites.txt`; run
Docker-backed on Linux in CI (and, unlike prior slices, executed against a real local Postgres 16 in
the authoring sandbox via `HARVEST_TEST_DATABASE_URL`), per the #543/#544/#601 precedent.

**Post-review hardening.**
- **Canary, not strict, replay mode (P1 correctness).** The endpoint replays via
  `replay_canary_snapshot`, not `replay_from_snapshot`. Strict replay classifies a *frontier
  suspension* — every healthy in-flight `RUNNING`/`SUSPENDED`/`PAUSED` run parked mid-flight, the
  endpoint's HEADLINE use case — as `NonDeterminismDetected`, a FALSE `diverged`. Canary excepts
  exactly that frontier suspension → `clean`, while a genuine mid-history divergence still surfaces
  (it resolves synchronously during the drive, never as a frontier suspension). So a healthy PAUSED /
  in-flight run now correctly returns `clean`. Proven by `running_healthy_run_parked_mid_activity_is_clean`
  (fails under strict, passes under canary).
- **Sealed-mid-await terminal reclassification (P2 correctness).** A terminal run externally sealed
  *mid-await* (`TIMED_OUT`, or operator cancel/terminate) records its terminal-lifecycle seal where
  the next command would go. In the single-activity shape (`WorkflowStarted + ActivityScheduled(a) +
  WorkflowExecutionTimedOut`, the activity never completed) the workflow *suspends* awaiting the
  in-progress result → canary reports `clean` directly. In the multi-activity shape (item 1 completed,
  sealed awaiting item 2), the workflow consumes item 1 and issues the NEXT command, which lands on
  the seal → a raw `diverged{actual: "WorkflowExecutionTimedOut"}`. The pure
  `reclassify_sealed_mid_await` (unit-tested) rewrites *that* false positive to `clean` — but ONLY
  when the divergence's `actual` names an EXTERNAL-seal event (`WorkflowExecutionTimedOut` /
  `WorkflowCancelled` / `WorkflowResetTerminated`) AND the run's DB state is terminal AND the history
  reached a terminal seal. The load-bearing condition is the external-seal `actual`: a genuine
  mid-history divergence (an activity rename) names a real command event, and new code that does MORE
  work than a `COMPLETED`/`FAILED` run names the workflow's OWN outcome seal (`WorkflowCompleted`/
  `WorkflowFailed`) — neither is an external seal, so both correctly stay `diverged` (retroactive
  forensics, AC4, is never masked). Proven by `timed_out_run_sealed_mid_activity` (clean via
  suspension) and `timed_out_run_sealed_awaiting_second_activity_is_clean` (clean via reclassification).
- **Codec/offload read fidelity (P2).** History is loaded with the runtime's `PayloadCodecs` +
  registry offloader (`load_history_inflated`) rather than the identity `load_history`, so an
  encrypted deployment (#608) no longer hard-500s on ciphertext and an offload deployment (#524) no
  longer false-diverges on a reference envelope. Identity deployments (the default) are byte-identical.
- **Tighter assertions + task-queue zero-write.** The diverged tests now assert the exact
  `event_index`/`expected`/`actual` (AC2), the nd-blocked test uses a clean-prefix history so the
  divergence lands at a real mid-history position (event index 3), and the zero-writes test also
  asserts `harvest_task_queue` is unchanged (no spurious enqueue, AC3). Negative admin-gate coverage
  in `security.rs` (`eris_unauthenticated_replay_diagnosis_is_blocked` → 401).
