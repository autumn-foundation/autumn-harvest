# SQLite edge/local-first harvest — storage-trait feasibility report (issue #966)

> **Status: retrospective.** This report was written *after* the spike ran and
> after its recommendation was executed. That is unusual and worth stating in
> the first paragraph rather than burying: the spike (PR #1066) and its
> productization (`autumn-harvest-sqlite`, issue #1068, Phase 5.0) both landed
> before the written deliverable this issue asked for. The report is therefore
> a **decision record backed by shipped evidence** rather than an estimate
> made in advance of one. Where a forward-looking spike report would have said
> "we estimate X", this one says "X, measured" — which makes it more useful,
> not less, but the reader should know the arrow of time.
>
> Nothing here re-opens the decision. It exists so that (i) the coupling
> inventory is written down and **kept honest by CI**, (ii) the road not taken
> — a `StorageBackend` trait inside core — is costed so nobody re-litigates it
> from scratch, and (iii) the hub-sync question is scoped before someone builds
> it by accident.

**Audit date:** 2026-08-12. **Audited revision:** `195a663`.

The inventory in this document is not hand-maintained trivia. It is
re-derived from `autumn-harvest/src/*.rs` on every CI run by
`autumn-harvest/tests/integration/sqlite_feasibility_docs.rs`, which fails the
build if a module becomes Postgres-coupled without being added here, if a row
here names a module that no longer exists, if a row is left unclassified, or
if the counts quoted below drift from a live grep. Treat the numbers as
current for the audited revision, not as prose.

---

## Decision summary

**Go — as a separate companion crate, not as a `StorageBackend` trait in core.**

| Question | Answer |
|---|---|
| Is harvest's determinism core backend-portable? | **Yes, already.** It consumes plain values (`ExecutionId`, `Vec<WorkflowEvent>`, a handler `fn`, JSON) — no connection, no trait object. |
| Is harvest's *coordination* layer backend-portable? | **No.** Multi-worker claim, push notification, and cross-connection locking are the three load-bearing Postgres features, and SQLite substitutes them only by dropping capability. |
| Did the prototype work? | **Yes — 4/4 durability scenarios**, plus cross-backend replay. Now productized. |
| Should core grow a `StorageBackend` trait? | **No.** Costed below at a scale the benefit does not justify, and it would tax the Postgres hot path for a use case that does not share its concurrency model. |
| What shipped instead? | `autumn-harvest-sqlite` — reuses the determinism core wholesale, reimplements persistence only. |

The one-sentence version: **the valuable half of harvest is already portable
and the coupled half should not be abstracted, so the correct seam is a
separate crate that reuses the core and rewrites persistence — which is what
was built.**

---

## Coupling mechanisms

Seven distinct Postgres-coupled mechanisms appear in core. The counts are
module counts at the audited revision, recomputed by CI:

| Mechanism | Reach | Portable? |
|---|---|---|
| `diesel` query layer | 42 modules | Query construction is mechanical; the *type* layer is not. |
| `skip-locked` claim (`FOR UPDATE SKIP LOCKED`) | 13 modules | Only by dropping multi-worker concurrency. |
| `interval-sql` (`INTERVAL '…'`, `make_interval()`) | 8 modules | Yes — integer epoch milliseconds. |
| `advisory-lock` (`pg_advisory_xact_lock`) | 7 modules | Subsumed by the single write lock. |
| `to_regclass` table-existence probes | 6 modules | Yes — `sqlite_master` lookup. |
| `listen/notify` push wakeups | 4 modules | No — polling is a degradation, not a translation. |
| `gen_random_uuid` server-side ids | 1 module | Yes — mint application-side. |

Plus **83 migrations** written in Postgres DDL (`JSONB`, `TIMESTAMPTZ`,
`INTERVAL`, `UUID`, partial indexes, `gen_random_uuid()` defaults), none of
which apply to SQLite. The SQLite crate does not translate them; it declares
its own schema.

**43 of the 93 core modules** exhibit at least one mechanism — a shade under
half. That ratio is the headline finding, and it cuts *both* ways: the
determinism core really is clean, and the persistence layer really is
saturated.

### The finding a SQL-only grep would have missed

Two modules — `store` and `concurrency` — reference `SKIP LOCKED` **only in
comments**. They issue no such SQL; they *rely on the invariant that someone
else does*. `concurrency`'s per-key limit is enforced "across the whole fleet"
precisely because the claim query serialises it, and `store` skips a TOCTOU
guard because "tasks are serialised per-execution by SKIP LOCKED".

This is the single most important structural result in the audit. A
`StorageBackend` trait derived mechanically from SQL call sites would have
abstracted the *writers* of that invariant and silently orphaned its
*consumers*, which would compile, pass unit tests, and be wrong under
concurrency. Coupling in harvest is not only syntactic, and that is why the
classification below is a human judgement rather than a generated table.

---

## Postgres-coupling inventory

Classification rule:

- **(a) trivially trait-able** — plain query/CRUD through Diesel with no
  Postgres-specific semantics. A trait method signature falls straight out.
- **(b) needs a semantic substitute** — uses a Postgres-specific mechanism for
  which SQLite has a *semantically equivalent* replacement.
- **(c) fundamentally Postgres-shaped** — "porting" it means either **dropping
  a capability** or **reimplementing the module wholesale**. Not a translation.

| Module | Coupling | Class | Substitute on SQLite / note |
|---|---|---|---|
| `admission_gate` | diesel, advisory-lock | (b) | Advisory lock subsumed by the single write lock. |
| `audit` | diesel | (a) | Append-only row writes. |
| `batch` | diesel | (a) | Plain CRUD. |
| `build_routing` | diesel, interval-sql | (b) | Integer epoch ms for the interval arithmetic. |
| `calendar` | diesel | (a) | Plain CRUD. |
| `completion_callback` | diesel, skip-locked, to_regclass | (c) | Two-transaction claim scanner; multi-worker delivery dropped. |
| `completion_trigger` | diesel, skip-locked, advisory-lock | (c) | Terminal-commit fan-out; claim semantics dropped. |
| `concurrency` | skip-locked | (c) | **Consumer of the claim invariant, issues no SQL.** Per-key fleet limits are meaningless single-writer. |
| `context` | diesel, listen/notify | (c) | Wakeup path; no push primitive exists. |
| `debounce` | diesel, skip-locked, to_regclass | (c) | Scanner claim; `sqlite_master` probe for the table check. |
| `dlq` | diesel | (a) | Plain CRUD. |
| `erase` | diesel | (a) | In-place field scrub. |
| `error` | diesel | (a) | `From<diesel::result::Error>` only — swap the error type. |
| `event_batch` | diesel, skip-locked, to_regclass | (c) | Scanner claim. |
| `execution` | diesel, skip-locked, interval-sql | (c) | Start/reuse matrix under `FOR UPDATE`; row-lock ordering is load-bearing. |
| `external_task` | diesel | (a) | Token-addressed CRUD. |
| `handle` | diesel | (a) | Read paths. |
| `heartbeat` | diesel | (a) | Batched last-write-wins update. |
| `lib` | diesel | (a) | `embed_migrations!()` only. |
| `models` | diesel | (c) | Postgres type layer (`Jsonb`/`Timestamptz`/`Interval`/`Uuid`); reimplemented wholesale. |
| `mutex` | diesel, advisory-lock, to_regclass, interval-sql | (b) | Advisory lock subsumed by the write lock; lease TTL as epoch ms. |
| `notify` | diesel, listen/notify | (c) | **The one mechanism with no SQLite equivalent at all.** Polling replaces it. |
| `poison_pill` | diesel, skip-locked, interval-sql | (c) | Crash reclaim keyed on *peer* worker liveness — no peers single-writer. |
| `queue` | diesel, skip-locked, listen/notify, interval-sql | (c) | The claim path itself. Reimplemented on `BEGIN IMMEDIATE`. |
| `queue_pause` | diesel, skip-locked, advisory-lock | (c) | Claim-time gate. |
| `reset` | diesel | (a) | Fork bookkeeping. |
| `retention` | diesel, skip-locked | (c) | Batched delete scanner with claim. |
| `schedule_decision` | diesel | (a) | Append-only decision log. |
| `scheduler` | diesel, interval-sql | (b) | Cron/interval arithmetic as epoch ms. |
| `schema` | diesel | (c) | Diesel `table!` definitions; reimplemented wholesale. |
| `sessions` | diesel, interval-sql | (b) | Lease expiry as epoch ms. |
| `signal` | diesel | (a) | Insert + dedupe index. |
| `start_idempotency` | diesel, to_regclass, interval-sql | (b) | `ON CONFLICT` upsert has a direct SQLite form. |
| `store` | diesel, skip-locked | (c) | **Consumer of the claim invariant, issues no SQL.** Event append itself is (a); its TOCTOU assumption is not. |
| `testing` | diesel | (a) | Test-only helpers. |
| `throttle` | diesel, skip-locked, to_regclass, gen_random_uuid | (c) | Token-bucket scanner claim. |
| `timeout` | diesel, skip-locked, advisory-lock | (c) | The scanner family; lock ordering vs the claim path is load-bearing. |
| `usage` | diesel | (a) | Aggregate reads. |
| `version_gate_retirement` | diesel | (a) | Marker scan. |
| `version_usage` | diesel | (a) | Marker scan. |
| `wasm_store` | diesel, advisory-lock | (b) | Content-hash upsert; advisory lock subsumed. |
| `worker` | diesel, listen/notify, advisory-lock | (c) | The dispatch loop; wakeups and persistence are interleaved. |
| `workers` | diesel | (a) | Fleet registry rows. |

**Totals: (a) 18 · (b) 7 · (c) 18.**

The shape matters more than the totals. The (a) column is broad but shallow —
mechanical CRUD. The (c) column is narrow but load-bearing: it contains the
claim path, the wakeup path, the scanner family, the start/reuse matrix, and
the type layer. **Those are precisely the modules a `StorageBackend` trait
would have to abstract, and precisely the ones whose semantics do not survive
abstraction.**

---

## Sizing a hypothetical `StorageBackend` seam

This seam was **not built**. This section costs it so the option is closed
with a reason rather than left as folklore.

### What it would have to cover

A trait that genuinely allowed a second backend would need, at minimum:

1. **Event store** — append (with the sequential-id contract), load, delta-load.
   *Trait-able cleanly.* ~6 methods.
2. **Task queue** — enqueue, claim, complete, fail, requeue, park, wake, plus
   the timer and signal tables. *This is where it breaks.* The claim method's
   contract is not "give me a task"; it is "give me a task **such that no
   concurrent claimer can also get it**, without blocking on rows other
   claimers hold". SKIP LOCKED is that contract. Single-writer SQLite satisfies
   it vacuously. There is no shared contract that is meaningfully testable
   against both.
3. **The scanner family** — timeout, retention, poison-pill, debounce,
   throttle, completion-callback, event-batch. Each is a claim-batch-mutate
   loop; each currently relies on `FOR UPDATE SKIP LOCKED` for its concurrency
   safety. ~13 modules.
4. **Coordination primitives** — advisory locks with a *defined lock ordering*
   (documented in `timeout.rs` and the mutex work) and the notification
   channel.
5. **The type layer** — `models.rs`/`schema.rs`, ~185 combined KB of Diesel
   definitions bound to Postgres column types.
6. **Migrations** — 83, in Postgres DDL.

### Why the seam is the wrong shape

The trait's hard part is not the method list. It is that **the two backends do
not share a concurrency model.** Postgres harvest is a multi-worker, multi-
replica fleet coordinating through the database. SQLite harvest is one writer
with a global write lock. A trait spanning both either:

- **encodes the Postgres contract** (SKIP LOCKED semantics, advisory-lock
  ordering, push notification) — in which case SQLite implements it by
  pretending, and every guarantee the trait documents is unverifiable on one
  side; or
- **encodes the weaker intersection** — in which case the Postgres path loses
  the ability to express the very coordination that makes it correct, and the
  `store`/`concurrency` consumers of the claim invariant have no way to state
  their requirement at all.

The `store`/`concurrency` finding above is the empirical proof: those modules
depend on the claim invariant *without a call site*. No trait boundary drawn
around SQL call sites would have caught them.

### Estimated build cost of the seam

| Component | Scope |
|---|---|
| Trait definition + Postgres impl | ~43 modules touched |
| Rewriting scanners against the trait | ~13 modules, each with a concurrency contract to re-specify |
| Type-layer abstraction | `models.rs` + `schema.rs` wholesale |
| Test matrix | Every DB-gated suite runs twice, with per-backend expectations where semantics diverge |
| Ongoing | Every future feature pays the trait tax — and this codebase ships features weekly |

Against a companion crate, which touched **zero** core modules.

---

## Cost to the Postgres path

The seam's cost to the shipped product was the decisive factor.

**Performance.** The claim path is the hot path and is already benchmarked with
a CI-gated contract (`docs/performance.md`). Routing it through a trait object
introduces dynamic dispatch on the per-task path and, more importantly,
forecloses Postgres-specific query shapes — the current claim query fuses the
rate-limit gate, the concurrency gate, the pause gate, and the debit into a
single statement with CTEs. A generic interface cannot express that fusion; it
would decompose into multiple round trips.

**Code complexity.** Harvest's correctness work is overwhelmingly about
concurrency edges — lock ordering, claim-vs-scanner races, the wake-request
mechanism, ABBA-deadlock avoidance. Every one of those fixes reasons about
concrete Postgres semantics. An abstraction layer between that reasoning and
the SQL would make the hardest class of bug in this codebase harder to see.

**Test matrix.** Doubling every DB-gated suite is not the real cost. The real
cost is that most of the interesting ones (multi-worker contention, lock
ordering, HA claim exclusivity) are **meaningless** on the single-writer side,
so the matrix would be mostly skips — carrying the maintenance weight of a
second axis while testing nothing new.

**Blast radius.** The companion crate's cost to the Postgres path is
**zero, structurally**: the dependency edge points one way
(`autumn-harvest-sqlite` → `autumn-harvest`, `default-features = false`), core
has no `rusqlite` dependency, and `cargo build -p autumn-harvest` output is
byte-unaffected. This is asserted by
`zero_regression_to_the_postgres_path_is_structurally_true`, not by prose.

---

## Test-suite portability

**No-DB suite: 100% passes, unchanged.** At the audited revision the core no-DB
set is 1849 lib tests plus 1176 integration tests, green with
`--no-default-features`. The SQLite work required no change to any of them —
which is the strongest single piece of evidence that the determinism core was
already backend-neutral.

**SQLite crate: 163 tests green** (43 unit + 119 integration + 1 doc), no
Docker required (`rusqlite` `bundled`).

Of the Postgres-gated suites, portability splits three ways:

| Category | Portable? | Examples |
|---|---|---|
| Replay determinism, event-schema fidelity | **Yes — and ported.** | Cross-backend replay, golden encoding, typed-failure metadata |
| Single-execution lifecycle | **Yes — ported with substitutes.** | Retry policy/backoff, timers across restart, signals, payload caps, reuse-policy matrix |
| Multi-worker contention | **No — semantically void.** | SKIP LOCKED claim exclusivity, HA scheduler claim (#350), sticky routing (#235), poison-pill peer-liveness reclaim (#367) |
| Fleet/operational surfaces | **No — out of model.** | Sharding, management API, Vantage UI, build routing, worker fleet registry |

The third row is the honest one: those tests are not "hard to port", they are
**not meaningful**. A claim-exclusivity test on a single-writer backend asserts
a tautology. Porting them would manufacture false confidence.

### Cross-backend portability evidence

The invariant that the event log is backend-portable is proven, not asserted,
by `sqlite_history_replays_on_core_replayer` in
`autumn-harvest-sqlite/tests/integration/cross_backend.rs`: a history written
by the SQLite backend replays clean on the core `WorkflowReplayer`. Alongside
it, `stored_event_matches_golden_encoding` pins the adjacently-tagged JSON
byte-for-byte and `pg_shaped_history_with_activity_started_is_replay_equivalent`
proves a Postgres-shaped history (which carries an `ActivityStarted` the SQLite
backend never writes) is replay-equivalent.

**No new `WorkflowEvent` variant was introduced.** That was the point: the
event log is portable *because* the schema did not change.

---

## Recommendation

**Go — as a separate companion crate (option ii). Do not add a
`StorageBackend` trait to core.**

Executed as `autumn-harvest-sqlite` (issue #1068, Phase 5.0; hardened in
Phase 5.1). Rationale, in priority order:

1. **The expensive half is already free.** The determinism core needed no
   abstraction — it was reused verbatim. A trait would have been built to
   share code that was already shareable.
2. **The coupled half should not be shared.** The (c) modules encode a
   concurrency model the two backends do not have in common.
3. **Zero cost to the shipped product.** A separate crate cannot regress the
   Postgres path; a core trait provably would touch it.
4. **Saying "no" stays free.** If the edge use case dies, the crate is deleted
   and core is untouched.

**Load-bearing constraint, restated:** single writer, single process, one
SQLite file. This is not an implementation gap to close later; it is the
assumption that makes `BEGIN IMMEDIATE` a valid substitute for SKIP LOCKED. If
multi-writer edge deployment ever becomes a requirement, this decision must be
re-opened, not extended.

### Known capability losses

Multi-worker/multi-process; push wakeups (polling latency floor); per-key fleet
concurrency (#247); sticky routing (#235); poison-pill peer reclaim (#367);
sharding; management API and Vantage UI; child workflows, external
signal/cancel, updates, local and external activities, cancellable timers,
worker sessions, and continue-as-new — each **rejected loudly by name** at the
command layer rather than silently ignored. Activity execution is
**at-least-once** (a crash mid-body re-runs it on resume), so bodies must be
idempotent.

---

## Scoping the hub handoff

**Not built. Not designed. Scoped only**, so it is not built by accident.

Handing an edge-executed workflow to a Postgres hub raises three questions the
current design does not answer:

1. **`ExecutionId` shard semantics on an edge node.** `ExecutionId` encodes a
   shard in its first two bytes; the SQLite backend has no shard concept and
   mints ids the router resolves via the `UNENCODED` sentinel. An ingested
   history would need a shard assigned at ingest — which changes the id, which
   is the primary key the whole event log hangs off. Either ingest rewrites the
   id (and every reference to it) or the hub accepts foreign-shard ids. Neither
   is free.
2. **Conflict rules.** The uniqueness contract is `(workflow_name,
   workflow_id)` per shard. Two disconnected edge nodes can both start
   `order-42`. There is no merge semantics for two divergent event logs of the
   same logical workflow, and there should not be one invented casually —
   append-only histories do not merge.
3. **Replay fidelity on ingest.** A history is only safe to resume on the hub
   if the hub has a registered handler for that workflow type at a compatible
   version, and if every side effect the edge recorded is honoured verbatim.
   The cross-backend replay test proves the *format* is portable; it does not
   prove an arbitrary edge history is *resumable* on a given hub build. The
   ingest path would need the replay-canary treatment (#512) as an admission
   gate, plus a rule for the derived state (task rows, timers) that the event
   log does not carry.

A credible first cut would be **export-only**: ship terminal histories to the
hub for archival and analytics, with no resumption. That sidesteps all three
questions above and is a genuinely useful product on its own. Live handoff of
in-flight executions should be treated as a separate, larger design.

---

## Other embedded backends

**libSQL / Turso** is the strongest adjacent option: a SQLite fork that keeps
the file format and adds server-side replication, which is exactly the axis
this backend is weakest on. Because it is wire- and file-compatible, the
persistence layer would largely carry over; its embedded-replica model could
plausibly relax the single-writer constraint at the edge, and it is the first
thing to evaluate if multi-writer edge becomes a requirement. **DuckDB** is a
poor fit and should be declined: it is an OLAP engine optimised for columnar
scans, and harvest's workload is high-frequency small point writes and
row-level claims — the opposite. It would be interesting as an *analytics*
sink over exported histories, not as an execution store. **`sled`** and other
embedded KV stores would require reimplementing indexing and transactional
multi-table updates that SQLite provides for free, for no clear gain.

---

## Timebox and method

The spike was scoped as evidence-gathering, not productization: the
prototype was to be abandoned rather than polished once the four durability
scenarios passed. In practice it passed that bar and the recommendation was
acted on immediately, so the prototype was carried into
`autumn-harvest-sqlite` instead of being discarded — the outcome the timebox
was protecting against (indefinite polishing of throwaway code) did not
occur, but neither did the discipline of stopping at the evidence.

**The process lesson, recorded honestly:** the report should have preceded the
productization. The decision was sound and the evidence supports it, but
issue #966's own success metric — *"leadership can green-light or kill the
direction from the report alone"* — was not available to leadership at the
moment the direction was green-lit. For the next R&D spike, the written
deliverable should gate the productization issue, not trail it.

**Method.** The inventory was produced by grepping `autumn-harvest/src/*.rs`
for seven precise mechanism tokens, then classified by hand. The detector is
deliberately narrow: a bare `INTERVAL` matches Rust constants such as
`DEFAULT_WORKER_POLL_INTERVAL`, and a bare `diesel` matches the guardrail
lint's own prose listing forbidden crates. Four modules
(`effective_config`, `guardrail`, `history_export`, `wasm_activities`) matched
the loose detector and are correctly excluded by the precise one. The exact
tokens live in `MECHANISMS` in the guard test and are the executable
definition of "Postgres-coupled" for this audit.
