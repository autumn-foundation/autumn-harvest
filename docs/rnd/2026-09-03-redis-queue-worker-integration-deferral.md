# 🏛️ Keystone [deferred]: Redis task-queue worker-integration (trigger: signed throughput commitment or tier-1 saturation evidence)

**Status:** deferral record — no RFC, no code change, no decision required from
architecture review.
**Scope examined:** `autumn-harvest-redis/`, `autumn-harvest/src/worker.rs`,
`docs/autumn-workflow-architecture.md` (Phase 4 roadmap), `docs/performance.md`
(issue #786 and its follow-up fixes), `docs/plans/vantage-spec-redis-adapter.md`,
`docs/assays/0001-redis-adapter-throughput-ceiling.md`. Full history through
`origin/trunk-dev` HEAD `29162e22` (2026-09-03).

## The decision

Roadmap doc `docs/autumn-workflow-architecture.md` lists "Optional Redis-backed
task queue adapter (`autumn-harvest-redis`)" under **Phase 4: Advanced Features
(ongoing)** — no date, no owner. The crate exists (shipped, tested, 1,243
lines) but its own module doc (`autumn-harvest-redis/src/lib.rs:55-65`) states
it is not wired into `worker.rs`: doing so requires **splitting the worker's
transactional boundary** — today "append events + update queue row" happens in
one Diesel transaction; wiring Redis changes that to "append events, commit,
then ack the Redis stream," with idempotency-on-replay to cover the gap. That
is a change to the execution kernel's atomicity story, not a mechanical
integration.

**The question this record answers:** should finishing that transactional-
boundary refactor be scheduled now? **No — deferred, pending a trigger below.**

## Why this isn't forced

- **No tier-4 fact.** The founding spec (`vantage-spec-redis-adapter.md`)
  frames the need as "enterprise users or hyper-growth startups scaling beyond
  [10k ops/sec]" — aspirational persona language, not a signed deal, dated
  requirement, or measured-growth projection. Inadmissible per the evidence
  ladder.
- **The cited ceiling is a documented, partially-fixed defect, not a wall.**
  `docs/performance.md` (issue #786) traces the Postgres claim path's
  shortfall to a non-indexable `CASE` expression leading the claim query's
  `ORDER BY`, forcing a full sequential scan + sort per claim — "the single
  biggest lever on this page." Two of the ten accreted predicates have
  already been fixed this way (concurrency-key gate: -99.23% buffers;
  queue-pause anti-join: -98.05% buffers) — proof that this class of fix
  works on this query, not evidence about the sort-key defect specifically.
  The sort-key defect itself remains unfixed, and no rewrite has been
  proposed or evaluated for it in the repo; what exists is an `EXPLAIN`
  plan (`docs/performance.md:434-462`) that names the mechanism precisely
  (`Sort Key` leads with a non-indexable `CASE` on `sticky_worker_id`/
  `sticky_until`, defeating `idx_harvest_tq_poll`'s ordering). Diagnosed,
  not designed — but still a single-file, storage-agnostic query problem
  with a known root cause, which is a smaller unknown than standing up and
  maintaining a second datastore's worker integration.
- **The crate's own throughput claim doesn't settle the case either way.**
  The `Prospect` spike (`docs/assays/0001-...md`) measured the standalone
  adapter honestly: it misses 10k ops/sec at the registered 8-worker
  steady-state shape (~8,760 mean) but clears it at higher concurrency
  (11,985-15,635) and under a backlog-drain shape (~12,004). Its own verdict
  explicitly declines to make the roadmap call: *"Decider: whoever owns that
  architecture doc and the spec — this is a docs-accuracy and
  roadmap-prioritization call... not a request to build anything."* This
  record is that call.

## Door class and reversal cost

**Two-way door**, and asymmetric. Not building it now: reversal cost is
**zero** — nothing merges into the worker's transaction boundary, nothing to
undo. Building it now and reverting later: the standalone crate is already an
isolated, optional workspace member, so ripping out an unshipped integration
attempt is cheap in itself, but the refactor's natural landing zone is
`worker.rs` — already the repo's single largest file (35,926 lines) and its
second-most-coupled by commit-touch frequency (170 touches, 31.0% of all
commits with `*.rs` changes over full history; `worker.rs` × `context.rs` is
the single strongest co-change edge in the repo at 87 commits). Landing an
unforced, atomicity-changing refactor in that file today adds coordination
risk to the hottest part of the codebase for a throughput ceiling nothing
currently hits.

## Default path

Postgres remains the only usable task-queue backend. `autumn-harvest-redis`
stays shipped-but-unintegrated, already correctly caveated in
`docs/autumn-workflow-architecture.md:1017` ("the adapter is not yet wired
into the worker... cannot be turned on by an operator today"). No operator
loses anything that works today; no roadmap commitment currently depends on
this line closing.

## Seam kept open

The task-queue adapter boundary is already drawn: `autumn-harvest-redis`
implements the same conceptual queue operations (`enqueue`/`claim`/`complete`/
`fail`/`recover_pending`) as the Postgres path, as an independent, optional
crate. Nothing needs to be un-done to resume this later — the seam is the
crate boundary itself.

## Trigger to revisit

Either:
1. A signed customer/design-partner commitment naming a throughput number
   and a date (tier 4), or
2. Tier-1 production telemetry, once the system has real deployments, showing
   the claim path actually saturating Postgres under real traffic — not a
   synthetic fixed-depth backlog benchmark.

Independent of this record and not gated by it: the non-indexable `ORDER BY`
sort key in `queue::claim_task` (`docs/performance.md:454-462`) is a
diagnosed, unscoped defect — its root cause is known, no rewrite has been
proposed or evaluated yet. Whatever fix eventually emerges is a two-way
door (a query/index change, reversible in hours) and storage-agnostic —
it is the implementing team's call to pick up and design whenever, per
Keystone's own charter on sub-2-week reversible decisions, not something
this record resolves.
