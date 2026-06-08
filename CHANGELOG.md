# Changelog

All notable changes to autumn-harvest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **DLQ root-cause aggregation** (issue #385). New read-only management route
  `GET /api/harvest/dead-letters/aggregate` (admin auth, parity with the DLQ
  list endpoint; placed under the existing `/dead-letters` family) answers an operator's first incident question — *"what is the shape
  of this fire?"* — by grouping dead-letter entries and returning per-group
  counts plus a few representative `dead_letter_id`s. Repeatable `group_by=`
  supports `workflow_name`, `activity_name`, `queue_name`, `task_type`,
  `time_bucket` (with companion `time_bucket=hour|day`), and a derived
  `failure_signature`; repeats build a hierarchical key. Filters mirror the list
  endpoint (`workflow_name`, `activity_name`, `queue_name`, `since`, `until`,
  `min_attempts`; `since`/`until` accept RFC 3339 or relative durations like
  `24h`). `limit_groups` (default 50, max 500) rolls the long tail into a single
  `{"_other": true}` group so counts reconcile to `filtered_total`;
  `samples_per_group` (default 3, max 10) caps the sample IDs. Counts sum across
  shards via `iter_shards()`. **Failure-signature derivation is the
  compute-on-read normalized-substring option (zero schema change):** the first
  line of `error`, with UUIDs/hex/decimal runs normalized to placeholders and
  truncated to 200 chars — deterministic and shard-stable. Invalid parameter
  values return `400` with a JSON error body (never `500`, never a silent empty
  match). New CLI subcommand `harvest dlq aggregate --group-by … [--json]`
  (table by default). Runbook: a "DLQ flood — first 60 seconds" section in
  `docs/runbooks/harvest-alerts.md`. **No new `WorkflowEvent` variant, no
  migration.** New core types in `autumn_harvest::dlq`: `failure_signature`,
  `DlqGroupDimension`, `TimeBucketGranularity`, `DlqAggregateParams`,
  `DlqAggregatePartial`, `DlqRawGroup`, `DlqGroup`, `DlqAggregateResponse`,
  `aggregate_dead_letters`, `merge_dlq_aggregates`. The DLQ inspection page
  (Vantage UI) gains a **Summary toggle** that renders the top-N groups with a
  `group_by` selector and click-through into the filtered list view.
- **DAG retry-from-failed-node** (issue #366). New management route
  `POST /api/harvest/dags/{dag_name}/runs/{run_exec_id}/retry` with body
  `{ from_nodes, reason, operator_id, dry_run }` lets an operator re-run a failed
  unified-DAG run from a specific failed node (and its declared downstream)
  without re-executing successful upstream nodes. It is a thin orchestrator that
  resolves `(dag_name, run_exec_id, from_nodes)` to a single `reset_to_event_id`
  by walking the DAG topology and the recorded history, then delegates to the
  existing workflow-reset internals (#148). No new `WorkflowEvent` variant and no
  migration. The reset reason is augmented with `dag_retry: nodes=[...]` so the
  audit trail (#158) reads cleanly. `dry_run: true` returns the resolved reset
  point and the explicit re-execute / carry-over node sets without writing.
  Source-state gating: `COMPLETED` → `409` (use a fresh run), `RUNNING` → `409`
  (cancel first), `FAILED`/`CANCELLED`/`TIMED_OUT` accepted; classic
  (non-unified) DAGs are rejected with `400` (see #256 step 5); a node name that
  maps to more than one task (DAG reuses the activity) is rejected `400`. A reset
  point that lands inside an unresolved *upstream* side effect returns `409` with
  a remediation hint. Semantics are **level-granular**: retrying any node
  auto-widens to its full execution level plus downstream closure (upstream
  always carried over), so the failed node's same-level siblings re-run with it
  and there is no "name the succeeded sibling to widen" dead-end.
  `WorkflowResetRequest.allow_terminal_source` is `#[serde(skip)]`, so it cannot
  be enabled from the public reset endpoint body. New CLI subcommand
  `autumn-harvest dag retry <dag> <run-id>
  --from-node <node> --reason <text> [--dry-run]`. Runbook at
  `docs/runbooks/dag-retry-from-failed-node.md`. The core
  `WorkflowResetRequest` gains an opt-in `allow_terminal_source` flag (default
  `false`) so the DAG-retry path can fork a terminal *failed* run; the standalone
  `/workflows/{id}/reset` endpoint keeps its strict `RUNNING`-only contract.

- **Worker-pool scaling signal and metrics endpoints** (issue #325).
  Expose KEDA/HPA compatible worker-pool scaling signal endpoint (`GET /admin/queues/scaling`)
  and Prometheus metrics scraping endpoint (`GET /admin/metrics`). Per-queue metrics include
  `backlog` (pending, ready now), `in_flight` (currently running), `scheduled` (pending in future),
  and `active_workers` (healthy, non-draining). Automatically aggregates across all database shards.

- **Timezone-aware cron schedules** (`Schedule::CronInTimezone`, issue #245).
  A new `Schedule` variant lets schedule authors anchor a cron expression to an
  IANA timezone so that jobs like `"0 9 * * 1-5"` fire at 9 AM local time
  year-round, regardless of DST transitions:

  ```rust
  Schedule::CronInTimezone {
      expr: "0 9 * * 1-5".into(),
      tz: "America/Los_Angeles".into(),
  }
  ```

  DST disambiguation rules: spring-forward gaps do not produce spurious firings
  (the skipped local time is not back-fired); fall-back repetitions fire exactly
  once on the first occurrence of the repeated hour.

  Unknown IANA timezone names are rejected at builder/registration time with
  `HarvestBuilderError::UnknownTimezone { name }`, not at first scheduler tick.

  The `harvest_schedules.timezone` column is now written with the schedule's
  declared timezone (was always hard-coded `"UTC"`). The management API
  `GET /admin/schedules` and `POST /admin/schedules/workflow` accept and emit a
  `"timezone"` field; schedules created without the field default to `"UTC"`.

  **Backward compatibility:** `Schedule::Cron(expr)` schedules retain UTC
  semantics on upgrade. The new variant is strictly opt-in. No migration is
  required — the `timezone` column already exists.

## [0.3.0] - 2026-05-13

### Documentation

- Update CHANGELOG.md for v0.2.0([2eb8e1d](https://github.com/madmax983/autumn-harvest/commit/2eb8e1d6754cf0e3e2a5f1e755425a7541623891))
## [0.2.0] - 2026-04-27

### Documentation

- Update CHANGELOG.md for v0.1.1([0dc1ff1](https://github.com/madmax983/autumn-harvest/commit/0dc1ff1e8973c2076bc95c591a0a3883cbc1966c))
## [0.1.1] - 2026-04-19

### Added

- **harvest:** Add history guardrails for long-running workflows (#280)([212abd4](https://github.com/madmax983/autumn-harvest/commit/212abd474b6aa450d281d37e18616ed418afc10c))
- Add workflow handle result waiting (#276)([af2b7ea](https://github.com/madmax983/autumn-harvest/commit/af2b7ea3fe639edf808f023ffa98cce64bb94f79))
- **det_check:** Add deterministic workflow guardrails (issue #172) (#265)([8794a63](https://github.com/madmax983/autumn-harvest/commit/8794a63d2f646ff3001e23aff968d523820e2fd0))
- Management API auth posture — route classification + security coverage (issue #174) (#267)([165e188](https://github.com/madmax983/autumn-harvest/commit/165e1880d3b9c7e20f614d4da94f0b7a5301ef2e))
- Add deterministic guardrail rule catalog foundation (issue #173) (#266)([7301229](https://github.com/madmax983/autumn-harvest/commit/7301229532059b4c55597f8c924d9a84c876f3b5))
- Add remote worker drain controls (issue #170) (#246)([f011825](https://github.com/madmax983/autumn-harvest/commit/f0118258e6779fe4f9dd271daddd5a5382d93a81))
- Export workflow histories as replay fixtures (#242)([bdb37b7](https://github.com/madmax983/autumn-harvest/commit/bdb37b76edff49d32d2382f1a7820cb4e4187464))
- Expose operator external activity handoffs (#239)([38ce949](https://github.com/madmax983/autumn-harvest/commit/38ce9498c794693939725ca48a0f50c43747683a))
- Version-gate retirement check (issue #164) (#228)([8f7ecb2](https://github.com/madmax983/autumn-harvest/commit/8f7ecb25a5c71cb5c95c56f7eddecd598feb4dff))
- Report workflow version-gate usage (#223)([4246798](https://github.com/madmax983/autumn-harvest/commit/4246798b0a9b1f3dc586d2ab5411ee9108e16ad1))
- Add shard readiness health gate (#219)([10655a3](https://github.com/madmax983/autumn-harvest/commit/10655a3403049bbd45c6ac617977f868500c9912))
- Heartbeats (#202)([68b6e59](https://github.com/madmax983/autumn-harvest/commit/68b6e599abcd6d29d54b46dbcbf043920d357988))
- Filter GET /workflows by state, workflow name, and search_attrs (#85)([53bef7f](https://github.com/madmax983/autumn-harvest/commit/53bef7f9264f41f5145775ef12851f7cb481aff6))
- Add harvest management cli (#29)([597dcfc](https://github.com/madmax983/autumn-harvest/commit/597dcfca7406dd5554223a843589a31d6cd2647c))
- Add Mermaid.js workflow history exporter (#25)([7226560](https://github.com/madmax983/autumn-harvest/commit/72265609750ef9b02b18f6b9cb49b2d31ddcee91))

### Fixed

- Route DLQ UI actions through audited bulk API (#286)([30e01dd](https://github.com/madmax983/autumn-harvest/commit/30e01dd44335e858417f0312f375f626635423a7))
- Compensate saga when approve_high_value_subscription fails (#222)([3784578](https://github.com/madmax983/autumn-harvest/commit/3784578bfd90754f2e1cdb1f0e80267e5b5e74db))
- Gate shared rollouts on readiness attempt 2 (#220)([190e5be](https://github.com/madmax983/autumn-harvest/commit/190e5be5a647cdf0769fda6ee405b1297b7ad6e9))
- Gate shard rollouts on readiness([b960a15](https://github.com/madmax983/autumn-harvest/commit/b960a15efd39e84ef924283ca24ee44fb67f3d7b))
- **worker:** Prevent panic in chrono_duration_from_secs (#126)([8511545](https://github.com/madmax983/autumn-harvest/commit/851154528d8d14afd3f264fc74706c3038498f70))
- Redis depth handling([76335e6](https://github.com/madmax983/autumn-harvest/commit/76335e65f96643faf3e08c635dc3a7ad5fe2277f))
- Replay determinism([d0a14d7](https://github.com/madmax983/autumn-harvest/commit/d0a14d7eede408bcf4449f1ad68ce04013c47b91))
- Redit and continue-as-new([fd1c024](https://github.com/madmax983/autumn-harvest/commit/fd1c0242a42985e6a8cfc4a1d19b40daf091e88e))
- Prevent silent integer truncation in concurrency limits and event indexing (#45)([68d5225](https://github.com/madmax983/autumn-harvest/commit/68d5225d6d5e97850ba975188d971e86a4bd5ce3))
- Resolve broken intra-doc links for HarvestError (#19)([cd77f63](https://github.com/madmax983/autumn-harvest/commit/cd77f6365848bcd7c9861564ffc53b2c0bf69f08))

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
- Update CHANGELOG.md for v0.1.0([b12cfc1](https://github.com/madmax983/autumn-harvest/commit/b12cfc193de46590f7e05eecc8b2063e4b18e675))

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

- Update CHANGELOG.md for v0.1.0([e117e4d](https://github.com/madmax983/autumn-harvest/commit/e117e4dc56667350af54e271160f63bdc1067081))
- Update CHANGELOG.md for v0.1.0([9aafd65](https://github.com/madmax983/autumn-harvest/commit/9aafd6508b9c42eb82586dce095b5c5e8f447ebb))
- Add README and crates.io metadata for first publish([74dbbb9](https://github.com/madmax983/autumn-harvest/commit/74dbbb912eb4f3d430c209c1ef98fb36e830785e))
- Update CHANGELOG.md for v0.1.0([7ed8a5f](https://github.com/madmax983/autumn-harvest/commit/7ed8a5fdbe5a816bde8ad65567e549f4d5fb34bd))
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

