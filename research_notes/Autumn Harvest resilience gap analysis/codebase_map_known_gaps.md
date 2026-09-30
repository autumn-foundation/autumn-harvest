# Autumn Harvest: codebase map, self-documented gaps, and code-maturity signals

State as of repo HEAD `937b655` (2026-09-30), plus GitHub issues on autumn-foundation/autumn-harvest read on 2026-09-30. Citations are `path:line` in the repo at `/home/user/autumn-harvest`. Issues are cited as `#N` (autumn-foundation/autumn-harvest#N). Counts come from grep and wc. Nothing was built or run.

## A. Architecture and runtime topology: processes, replicas, singletons, ownership, SPOFs

### Takeaway
Harvest is a library embedded in the application process (`HarvestPlugin` for Autumn, or a standalone `HarvestRunner`). Postgres is the only required dependency and the only source of truth. There is no leader election. By default every replica runs a worker, a scheduler, and all background scanners, and they coordinate only through Postgres row claims (`FOR UPDATE SKIP LOCKED`, claim tokens with TTLs, advisory transaction locks, lease columns). The effective single points of failure are therefore each shard's Postgres primary (one per shard, single region, failover is manual) and, when enabled, the single Redis instance. Redis only affects startup: once running, the engine falls back to Postgres when Redis is lost.

### Cited Findings
**Workspace and size**
- The workspace has 7 product crates plus corpus and example members. The version is 0.6.0, the edition is 2024, and the MSRV is 1.88 — `Cargo.toml:1-30`.
- Crate sizes (`src/` .rs lines, then `tests/` .rs lines), measured with wc:
  - autumn-harvest: 116 files, 317,604 src lines, 276,490 test lines.
  - autumn-harvest-plugin: 58 files, 138,171 src lines, 110,314 test lines.
  - autumn-harvest-cli: 20,526 src, 8,977 test.
  - autumn-harvest-verify: 14,361 src, 8,294 test.
  - autumn-harvest-sqlite: 7,748 src, 8,767 test.
  - autumn-harvest-macros: 7,084 src, no tests dir.
  - autumn-harvest-redis: 3,731 src, 3,471 test.
- `autumn-harvest/src/lib.rs` has 115 `mod` declarations.
- Several source files are very large: `autumn-harvest-plugin/src/api.rs` 58,641 lines, `autumn-harvest/src/worker.rs` 43,735, `context.rs` 29,450, `plugin/src/ui.rs` 19,830, `cli/src/lib.rs` 18,980, `replay.rs` 13,659, `queue.rs` 11,120, `execution.rs` 10,224, `scheduler.rs` 9,266 (wc -l).
- Test attribute counts (`#[test]` / `#[tokio::test]`): autumn-harvest 8,359 (plus 11 `proptest!` blocks, 63 `#[ignore]`); plugin 3,369 (23 ignored); cli 772; verify 356 (3 ignored); sqlite 184; redis 95 (4 ignored); macros 68. `autumn-harvest/tests/integration/` holds 208 files.
- There are 113 Postgres migrations in `autumn-harvest/migrations/`. 56 use the legacy day-only `YYYYMMDD000000` prefix; `CLAUDE.md` says migrations up to `20260728000000` keep those names. There are no duplicate version prefixes. The newest is `20260928231722_harvest_audit_purge_watermark`. The SQLite crate has no migrations directory; its schema lives in `autumn-harvest-sqlite/src/schema.rs`.
- There are 130 unreleased changelog fragments in `docs/changelog.d/` against the last release, 0.6.0 of 2026-08-26 (`CHANGELOG.md:10`).

**Execution model (summary; covered in depth by another researcher)**
- Workflows are deterministic async Rust functions. An activity call enqueues a Postgres task and suspends the workflow. A worker claims the task with `SELECT … FOR UPDATE SKIP LOCKED` and writes the result as an event. The workflow then resumes on the same worker if its state is cached, or on any worker by replay. "You only need Postgres, not a separate service." — `README.md:1081-1093`
- The suspension model keeps the coroutine in memory, with durability from event history, and the executor replays from the top on each cycle (DD-1). There are separate worker and web DB pools under a shared ceiling (DD-2). The in-process LRU cache and sticky routing are hints only (DD-4). — `docs/architecture.md:1573-1600`

**Deployment modes and roles**
- `HarvestMode` is `Embedded` (default), `Split` or `External` — `autumn-harvest-plugin/src/config.rs:11-16`. Split and External require `harvest.database.url` — `config.rs:351-355`.
- Per-process role flags `worker_enabled` and `scheduler_enabled` both default to `true` — `autumn-harvest-plugin/src/config.rs:119-120`, `:542-543`.
- The HA runbook calls "two or more replicas behind a load balancer" the default deployment topology and says it is fully supported — `docs/runbooks/ha-deployment.md:7`.

**Coordination without a leader**
- Every replica runs its own scheduler tick loop, every 1 s by default — `docs/runbooks/ha-deployment.md:13-15`.
- Slot exclusivity comes from an atomic `UPDATE … SET fire_claim_token, fire_claimed_until = NOW()+30s WHERE fire_claim_token IS NULL OR fire_claimed_until < NOW()` (#350) — `docs/runbooks/ha-deployment.md:17-32`.
- Crash recovery for a claimed slot is bounded by the 30 s claim TTL, and deterministic IDs `sched:{workflow}:{logical_date}` prevent double fires — `docs/runbooks/ha-deployment.md:51-65`.
- The shipped-work record says it outright: "Every process with `scheduler_enabled` (the default) spawns a `SchedulerRuntime`". Registration writes are de-duplicated with `pg_try_advisory_xact_lock` on `"harvest:schedule_registration:v1"` instead of leader election — `docs/shipped-work.md:6138-6140`.
- Workers coordinate through `FOR UPDATE SKIP LOCKED` — `docs/runbooks/ha-deployment.md:159`.
- Concurrency caps use `pg_try_advisory_xact_lock(hashtext(concurrency_key))` — `autumn-harvest/src/queue.rs:1120`, `:1313`.
- Quota uses a blocking `pg_advisory_xact_lock` — `autumn-harvest/src/quota.rs:774-791`.
- Queue pause uses 2×32-bit advisory keys — `autumn-harvest/src/queue_pause.rs:101-196`.
- DR replication uses `pg_try_advisory_xact_lock` — `autumn-harvest/src/replication.rs:1347-1356`.
- The retention janitor leases rows by writing `retention-lease-<uuid>` into `harvest_workflow_executions.sticky_worker_id`. The column is overloaded, and a `Drop` guard releases the lease — `autumn-harvest/src/retention.rs:1930-1960`, `:2126-2262`.
- Background control loops run per assigned shard. Each loop runs on every assigned shard in every process:
  - timeout/SLA enforcement
  - poison-pill reclaim
  - pause auto-resume
  - session-slot reconciliation
  - audit export
  - external signal/cancel outboxes
  - scheduler ticks

  Source: `docs/architecture.md:181-188`.
- Scanner cadences: timeout/sla/external_outbox 500 ms, schedule 1 s, poison_pill/pause_auto_resume 5 s, retention janitor hourly — `docs/shipped-work.md:6211`.
- Spawn sites in core (`tokio::spawn` count): `context.rs` 22, `worker.rs` 18, `dispatch.rs` 4, `slot_tuner.rs` 3, `notify.rs` 3, `retention.rs` 2, `guardrail.rs` 2, and one each in `scheduler`, `heartbeat`, `audit_export`, `completion_trigger`, `workers`, `det_check`.

**Sharding (horizontal scale inside one region)**
- The shard id sits in the first two bytes of `ExecutionId`. `ShardRouter` uses seahash rendezvous hashing, and `ShardedDbPool` is a `BTreeMap<ShardId, DbPool>`. All of a workflow's state lives on one shard — `docs/architecture.md:160-172`.
- Adding a shard applies to new workflows only. Cross-shard rebalancing of existing workflows is described as limited — `docs/architecture.md:160`, `:174-180`.
- `/api/harvest/health` is liveness-only by default. It becomes a shard-readiness probe only with `require_shard_readiness = true` — `docs/architecture.md:179`, `README.md:1133-1145`.

**Optional components**
- Redis Streams is an optional dispatch channel, behind the plugin `redis` feature (#1312). Postgres keeps every row, claim gate and history write — `README.md:1147-1162`.
- If Redis is lost while running, the engine falls back to the Postgres claim path. A configured URL that cannot connect fails startup — `README.md:1164-1171`.
- SQLite is a separate, embedded, single-writer companion crate, not a storage trait in core — `docs/sqlite-backend.md:1-16`.

**Cross-region DR**
- Cross-region DR is "shipped (issue #954)". It provides per-shard async replication to a standby region, epoch fencing, and a measured RPO. Failover is operator-initiated, with "no automatic promotion, no active-active writing, and no zero-RPO mode" — `docs/cross-region-dr.md:3-9`.
- Fencing is opt-in per process. A partitioned old-region worker can still write to its own old primary, so isolating that primary is a mandatory manual step — `docs/cross-region-dr.md:241-275`.

### Inferences
- There is no single process-level SPOF: any replica can do any job, and every singleton-like job uses a row claim or advisory lock. The actual SPOFs are:
  - each shard's Postgres primary, which holds all state, queue, timers and history for its workflows;
  - the manual, operator-run DR promotion;
  - boot-time dependency on Redis when it is configured.
- Because every replica runs every scanner, the scanner load and polling query volume against each shard grow linearly with replica count. Correctness under contention depends on claim TTLs: 30 s for schedule claims, and heartbeat or lease timeouts elsewhere.
- Overloading `sticky_worker_id` with a retention lease prefix is a coupling risk. Any code that reads `sticky_worker_id` as a real worker id has to filter `retention-lease-%`, as `retention.rs:2198`/`:2217` already do.

### Gaps
- No single document lists every background loop, its claim mechanism and its failure bound. The inventory above is pieced together from `architecture.md`, `shipped-work.md` and the HA runbook.
- I did not trace plugin startup (`runner.rs`/`plugin.rs`) far enough to list every task spawned in Split versus Embedded mode.

## B. Self-documented limitations, non-goals, deferred work and open issues

### Takeaway
The project documents its limits openly and in detail. Across the docs, "limitation" appears 269 times, "deferred" 354, "follow-up" 395 and "out of scope" 212. The main resilience-relevant limits are:
- single region with manual DR only;
- no cross-shard workflows and no cross-shard global limits;
- no migration of running workflows between shards;
- Redis dispatch v1 supports one shard per process, no Cluster and no TLS;
- a buffered-schedule drain path with a known double-dispatch risk;
- SQLite is a reduced-feature, single-writer backend.

GitHub has only 30 open issues. More than 120 issues were closed or updated since 2026-09-10, and many of them are concurrency or correctness fixes (zombie-owner writes, stuck RUNNING runs, clock skew, ABBA deadlocks, codec-rotation races). This is a high defect-discovery rate in exactly the areas that matter for resilience.

### Cited Findings
**Keyword density.** Across docs, README, DESIGN-*, RELEASE_NOTES and CHANGELOG: "limitation" 70 files / 269 hits, "not yet" 58/89, "deferred" 70/354, "follow-up" 125/395, "out of scope" 84/212, "future work" 9/12, "known issue" 0 (grep -c). There are dedicated "Known limitations" sections in 13 or more docs, including `performance-sticky-routing.md:309`, `performance-worker-sessions.md:344`, `performance-mutex-lease-reclaim.md:297`, `performance-poison-pill-orphan-recheck.md:354`, `performance-external-outbox-scan.md:357`, `workflow-determinism-guide.md:560`, `replay-verify.md:245`, `benchmarks.md:310` and `performance.md:1082`/`:1506`.

**Stated gaps (`docs/comparison.md:250-313`)**
- Rust-only SDK (#959, #955).
- Single region.
- No managed cloud.
- Postgres-only, with no pluggable persistence ("a ceiling").
- UI parity incomplete: no DAG graph visualisation.
- Pre-1.0, with breaking changes in minor versions.
- No cross-shard workflows.
- No cross-engine benchmark on equal hardware.

**Doc drift about DR.** `docs/comparison.md:155` and `:262-268` still say there is "no built-in cross-region replication or failover today" and list #954 as "Planned R&D". `docs/cross-region-dr.md:3` says "Status: shipped (issue #954)".

**HA runbook exclusions (`docs/runbooks/ha-deployment.md:157-161`)**
- `drain_buffered_schedule_runs` (the `BufferOne`/`BufferAll` overlap policies) "has a lower-severity double-dispatch risk". Protection relies on `RejectDuplicate` reuse policy, and "a dedicated claim guard for drain is tracked separately".
- "Cross-region active-active" is out of scope; the runbook says to pin the scheduler to one region.

**Sharding limits**
- Uniqueness of `(workflow_name, workflow_id)` is per shard, so pinning a run can create duplicates.
- signal-with-start, update-with-start and the SDK start APIs cannot pin.
- Deferred starts (debounce/batch) cannot be pinned.
- During rolling deploys, pre-#697 nodes silently ignore pins.
- Explicitly out of scope: migrating a running workflow between shards, geo-replication and cross-region failover (`docs/sharding.md:245-255`).
- Cross-shard global concurrency limits are "explicitly out of scope". The per-shard guarantee is the contract — `docs/sharding.md:398-402`.
- By default, children are pinned to the parent's shard, and that pinning is permanent. As a result, one orchestrator puts all of its fan-out write load on a single database — `docs/sharding.md:414-416`.

**Redis dispatch v1 limits (`docs/operations/redis-dispatch.md:263-293`)**
- One shard per process: a multi-shard API process is rejected, and #1429 tracks true multi-shard support.
- No Redis Cluster: the crate uses a single-node client and does not follow `MOVED`/`ASK`.
- Priority and sticky affinity are best effort only.
- No TLS: `rediss://` is rejected at startup and passwords are sent in cleartext — `README.md:1173-1178`, `redis-dispatch.md:120-130`.
- #1429 was closed in the last three weeks, but the TLS and multi-shard text above still stands in the docs.

**SQLite non-goals (`docs/sqlite-backend.md:382-414`, `autumn-harvest-sqlite/README.md:105-116`)**
- Rejected with `Unsupported`: child workflows, external signals/cancels, local activities, external/task-token activities, updates, search attributes, continue-as-new, worker sessions, and cancellable timers (only fire-once `ctx.timer` works).
- A rejected execution "stays `RUNNING` and keeps erroring on every later drive". It no longer blocks unrelated executions since #1530/#1555.
- Backend-level non-goals: multi-writer workers, LISTEN/NOTIFY, multi-server crash recovery, schedules, the management API, retention, worker sessions, sharding and DAGs.

**Other documented residuals**
- Scanner-liveness metrics cannot separate two runtimes on the same shard in one process, so a healthy peer can mask a wedged one — `docs/shipped-work.md:6225`.
- A child-timeout hard-cap deadline cleanup can leave an over-deadline child un-cancelled; this is documented as a known limitation (PR #1041) — `CHANGELOG.md:286`.

**Open issues (all 30 listed on 2026-09-30).** The resilience-relevant ones:
- #1693: a source workflow's task is "genuinely never dequeued, not just slow" after a quota-exceeded defer-to-outbox. In a local repro it stayed RUNNING for 90 s with an unclaimed `harvest_task_queue` row. Fix PR #1782 is open and titled "build the shared test database from the full migration bundle", which suggests a test-DB schema cause rather than an engine bug.
- #1552: `Worker::run_with_listener`'s heartbeat `JoinHandle` detaches instead of aborting when the worker future is aborted, and "keeps running indefinitely, holding its database pool connection". Fix PR #1778 is open.
- #1676: backup verify's completion-trigger adjudication degrades once a fire outlives its target's summary.
- #1772: `run_next/prev_business_day` lands on Saturday or Sunday for the us-federal-holidays and nyse calendars (repro 10/10). This is a durable-timer correctness bug.
- #1631, #1511: the quota_key reconcile scan is O(backlog) and opens one transaction per row.
- #1585, #1548: dev-runtime robustness.
- #1723: a Vantage UI form loses data.
- #1568: schedule health.
- #1605, #1613–#1616: epic to make the non-autumn-web path first class, including a standalone harvest-server binary.
- The rest are refactor, "Echo" duplication and "Bolt" performance negative results: #1774, #1752, #1751, #1695, #1632, #1546, #1448, #1440, #1770, #1757, #1745, #1733, #1721.
- #1751 records "real missed-fix history" across three `requeue_workflow_task_*` clones.

**Recently closed correctness and resilience issues (sample, closed or updated since 2026-09-10; 121 total in that window)**
- #1184: terminal workflow writes had no worker-ownership check, so a zombie dispatcher could corrupt a reclaimed run.
- #1347: ownership guard missing on the pause fast-path park.
- #1459: a parent task could wedge RUNNING indefinitely when the workflow-task-timeout reset raced DB-pool contention.
- #1558: timeout test still failed after a "no race left to lose" hardening.
- #1392, #1389: outbox, quota and backoff deadlines used the host clock and were vulnerable to cross-replica skew.
- #1391: a stale sentinel defeated the quota retry backoff.
- #1228: quota ABBA deadlock on detached-child fan-out.
- #1360: potential pool-size-1 self-deadlock in `batch.rs`.
- #1426: per-shard monitor loops could hang worker shutdown on an exhausted shard pool.
- #1323: unbounded peer-pool acquisitions.
- #1251, #1257, #1258: codec-rotation races (commit under a key retired mid-batch, stale sweepers, starvation).
- #1253, #1758: business data shaped like a codec or offload envelope corrupted replay (data integrity, 10/10 repro).
- #1636: `start_workflow_with_id` silently dropped the new input (data loss).
- #1348: terminal metrics lost to cancellation.
- #1267, #1273, #1508: audit retention and redrive races.
- #1717: LISTEN/NOTIFY hardcoded NoTls.
- #1430: legacy debounce rows could swallow valid requests.
- #1409, #1262: redrive/continue-as-new history gaps.
- #1278: no CSRF protection on Vantage POSTs.
- #1215: claim sort spills to disk at about 10k rows.
- #1290: flaky wall-clock tests on Windows.

### Inferences
- The defect stream is dominated by races between concurrent background actors: dispatchers, sweepers, scanners and redrives. This fits the leaderless every-replica-runs-everything topology. Many invariants are enforced by compare-and-swap, ownership checks and TTLs added one issue at a time, not by a single ownership/fencing abstraction.
- Most open-issue risk is low severity. #1693 and #1552 are the two open items with stuck-work or resource-leak character, and both have open fix PRs.
- The drain double-dispatch residual and the DR text in `comparison.md` are the clearest doc-versus-reality gaps to verify.

### Gaps
- GitHub's semantic search returned few hits for lease/fencing/parity queries, and there are no issue labels. The classification above comes from titles, plus full bodies for #1693 and #1552.
- I could not find an issue number for the "dedicated claim guard for drain" mentioned in `ha-deployment.md:160`.
- I did not confirm whether the 121 "since 2026-09-10" issues were all closed inside that window: the filter is by update time.

## C. Code-level maturity signals

### Takeaway
Panic discipline in core non-test code is good:
- 7 `.unwrap()`, 125 `.expect()`, 6 `panic!`, 42 `unreachable!`, and no `todo!`/`unimplemented!`.
- Most `expect`s are mutex-poison checks or invariants stated in the message.

The weak spots are:
- `expect("database UUIDs must round-trip into ExecutionId")` on worker, timeout and poison-pill hot paths;
- lock-poison `expect`s in the retention lease `Drop` guard;
- 18 `unwrap()`s in plugin `api.rs`;
- builder panics on misconfiguration.

Hygiene signals:
- five stray scratch files are committed, including two 4 MB binaries;
- an unwired `TODO(#606 step 9)` sits on a feature marked shipped;
- several analysis modules are public API that no runtime path references;
- webhooks and mcp feature tests are not run in CI;
- docs have drifted from the code in places.

### Cited Findings
**Panic sites.** These come from a scratch script that strips `#[cfg(test)]` items and comments.

*Core crate `autumn-harvest/src`* — `expect` 125, `unreachable!` 42, `unwrap` 7, `panic!` 6, `todo!`/`unimplemented!` 0. Including tests, raw grep finds 707 `unwrap` and 1,070 `expect`. Per file:
- `context.rs`: 63 `expect` (almost all `"matcher lock poisoned"` / `"signal_registry lock poisoned"`, e.g. `context.rs:3104`, `:3147`, `:3197`), 19 `unreachable!`, 2 `unwrap`.
- `worker.rs`: 14 `expect`, 2 `unreachable!`.
- `history_export.rs`: 11 `_ => unreachable!()` (`:728`–`:1140`).
- `retention.rs`: 10 `expect`.
- `scheduler.rs`: 6 `expect`, 1 `panic!`.
- `execution.rs`: 6 `expect`.

*Plugin crate* — 85 `expect` and 18 `unwrap`, mostly in `api.rs` (71 `expect`, mainly `"harvest api state lock poisoned"`). Examples:
- `api.rs:11360`, `:11434`, `:11457`: `last_continued_as_new.unwrap()` on a continue-as-new chain walk.
- `api.rs:40010-40044`: `writeln!(…).unwrap()` in metrics rendering.
- Five `panic!`s at startup: `plugin.rs:126` (migration registration), `:1191`, `:1201`, `:1223` (connector config), `webhook_receiver.rs:236`.

*Other crates* — Redis `src/`: none. SQLite: 2 `panic!` in `runtime.rs`, the documented setup-time rejection.

**Worst hot-path examples**
- `execution_id_from_uuid` does `.expect("database UUIDs must round-trip into ExecutionId")`: `autumn-harvest/src/worker.rs:1506-1510`, and the same pattern at `timeout.rs:528`, `poison_pill.rs:246`, `sessions.rs:809`. It runs on every row these scanners and workers touch. It is sound only if `ExecutionId` parsing accepts every UUID in the table.
- The retention lease `Drop` guard calls `self.active_ids.lock().expect("lease guard lock poisoned")` (`retention.rs:1944`, also `:2280`, `:2726`, `:3435`). A poisoned mutex would panic inside `Drop`, and during unwinding that aborts the process.
- `retention.rs:1056-1120` and `scheduler.rs:365-377`: monitor locks use `expect("… lock poisoned")`.
- `worker.rs:27086` `.expect("assigned shard pool presence verified at run() entry")` and `shard.rs:1651` `.expect("default shard pool is always present")`: invariant `expect`s in the shard routing path.
- `scheduler.rs:432`: `SchedulerRuntime::spawn_sharded` calls `panic!("{error}")` when classic DAGs are registered without `unified-dag-execution`. That is non-default code, since the feature is in `default`.
- `builder.rs:1731`/`:1747` panic on an invalid or unregistered payload-codec key id, deliberately at boot; `builder.rs:2386` has `.expect("HarvestBuilder::build failed validation")`.
- `context.rs:12807`/`:12881`: `Arc::get_mut(&mut ctx).unwrap()`, justified by the comment "Arc was just created".

**TODO/FIXME.** There are only 4 `TODO|FIXME|XXX|HACK` hits across all crate `src/` trees, which the comment-hygiene CI gate enforces (`CLAUDE.md`, `docs/audits/comment-hygiene.py`). The one real one is `autumn-harvest/src/worker.rs:1676-1690`: `TODO(#606 step 9)` says the session `session_id`/`session_worker_id` fields are "Not yet wired -- `#[allow(dead_code)]` is temporary". It is supposed to write `harvest_task_queue.session_id` and hard-pin `sticky_worker_id` for sessions. Worker sessions (#606) are nonetheless recorded as "Phase 3.46 (implemented)" in `docs/shipped-work.md:79`.

**Modules not referenced from any runtime path.** These are declared `pub mod` and re-exported from `lib.rs`, but no worker, scheduler, plugin, CLI or other crate `src` references them:
- `dag_linter` (`lib.rs:264`, re-export `:543`)
- `dag_simulator` (`:268`, `:550`)
- `trace_export` (`:433`, `:708`)
- `schema_contract` (`:405`, `:661`)
- `dag_export` (`:263`, `:541-542`)
- `test_generator` (`:424`, `:688`), referenced only by `simulator.rs`
- `simulator` (`:414`), referenced only by offline analysis modules and `testing.rs`

These are offline tooling or library APIs, not dead engine code. R&D-spike modules `wasm_activities`, `wasm_store`, `hot_swap` and `hot_swap_store` (`lib.rs:328-488`) are compiled only behind non-default features.

**Feature flags**
- Core defaults are `["db", "unified-dag-execution", "tls"]` — `autumn-harvest/Cargo.toml:15`.
- Non-default features:
  - `testing` and `debugger`
  - `metrics-rs`
  - `schema`
  - `wasm-activities` (an "R&D spike" needing Rust ≥ 1.94)
  - `hot-code-swap` ("R&D spike. … Off by default")
  - `chaos` (fault injection, "never in `default`")

  Source: `autumn-harvest/Cargo.toml:33-73`.
- Gate counts in core src: `feature="db"` 786, `wasm-activities` 67, `testing` 32, `hot-code-swap` 14, `chaos` 9, `tls` 5, `schema` 5, `unified-dag-execution` 4 (plus 5 `not(...)`).
- The plugin has no `default` feature list. `redis`, `webhooks`, `mcp`, `metrics`, `connectors`, `kafka` and `sqs` are all opt-in — `autumn-harvest-plugin/Cargo.toml:14-66`. Redis and SQLite crates have `default = []`.
- CI runs clippy for each plugin feature (`.github/workflows/ci.yml:352-426`). A source constant, however, says webhooks and mcp tests are "not run in CI … AND all tests are #[ignore]d" (ALLOWLIST_*_IGNORED_REASON, found by grep).
- The chaos suite runs only nightly or on demand and is "Not part of the required CI matrix" (`.github/workflows/chaos.yml:8-25`). Loom is `workflow_dispatch`-only (`.github/workflows/loom.yml:4-16`).

**Backend parity**
- Redis is not a storage backend. Its dispatch role carries only task references, and Postgres keeps "every row, every claim gate and the whole history write path" (`README.md:1151-1156`). Leases, timers, retention, codec rotation, erasure, fencing and sharding therefore stay on Postgres.
- The `autumn-harvest-redis` crate doc still opens as a full "Redis Streams task queue adapter" with its own enqueue, claim and visibility-timeout recovery (`autumn-harvest-redis/src/lib.rs:1-20`). The integrated dispatch-channel role is described separately at `:58-59`. So the crate holds two roles, and only the dispatch channel is wired into the plugin.
- SQLite supports:
  - activities
  - fire-once durable timers
  - pull-only signals
  - crash recovery by replay, with at-least-once activities
  - idempotent starts
- SQLite does not support:
  - retention, schedules, sharding, worker sessions, DAGs, the management API
  - child workflows, continue-as-new, updates, local activities
  - LISTEN/NOTIFY
  - multi-writer or multi-server operation

  Source: `docs/sqlite-backend.md:382-414`.
- There is no codec-rotation, DLQ or fencing code in `autumn-harvest-sqlite/src`: grep for `codec`, `dlq` and `fenc` finds no files.
- Neither alternative backend claims production-grade parity. SQLite calls itself "embedded, single-writer … edge / local-first" (`docs/sqlite-backend.md:1-7`). Redis v1 is single-instance, single-shard-per-process, with no TLS (`redis-dispatch.md:263-293`).

**Hygiene and doc drift**
- `test_debug`, `test_keys` (two ELF binaries of about 4 MB each), `test_debug.rs`, `test_keys.rs` (HashMap-print scratch programs) and `autumn-harvest/src/analyzer.rs.orig` (353 lines against 419 in `analyzer.rs`) are all tracked by git. They were added in commit 90f5c71 (#1722, 2026-09-24), a docs-link PR ("📖 Folio: re-link orphaned quota-reconcile ledger").
- `docs/architecture.md:198-199` has two contradictory `event.rs` rows: "41 variants" and "35 variants". The enum at `autumn-harvest/src/event.rs:86` has about 50 variant lines by grep.
- `docs/architecture.md:1603` is still titled "Phase 4 Scope (next)" but lists items marked implemented.
- `RELEASE_NOTES.md` stops at 0.4.0 (`RELEASE_NOTES.md:1-8`) while the crate is at 0.6.0.
- `.gitignore` lists `Cargo.lock`, yet `Cargo.lock` is present at the root.

### Inferences
- Panic discipline in core is deliberate, since pedantic and nursery clippy lints are warnings workspace-wide (`Cargo.toml` `[workspace.lints.clippy]`). The residual risk is concentrated in two places: mutex-poison `expect`s, where one panic under a lock cascades, and the UUID round-trip `expect` in scanners, where one bad row could crash-loop a scanner on every replica.
- Committed binaries in a docs PR, together with doc drift, point to a high-velocity, agent-driven change process. Review gates catch Rust comment defects but not stray artifacts.
- "Shipped" status in `shipped-work.md` does not always mean fully wired, as the #606 step-9 TODO shows. Resilience claims tied to worker sessions (hard pinning) should be verified in code.

### Gaps
- The panic counts come from a heuristic script: it strips brace-matched `#[cfg(test)]` items, so test helpers outside such blocks may be counted. They are approximate.
- I did not verify whether `ExecutionId::from_str` can reject any valid UUID, which would decide whether the round-trip `expect` is reachable.
- The event-variant count (about 50) comes from a regex over `pub enum WorkflowEvent` and is not an exact count.
- I did not check which integration tests run in CI's DB matrix beyond the job names seen in `ci.yml`.
