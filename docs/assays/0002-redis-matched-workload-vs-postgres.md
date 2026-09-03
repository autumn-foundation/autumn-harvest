# ⛏️ Prospect: does Redis clear a decisive margin over Postgres under docs/performance.md's own matched scenario shape? (ledger #2)

> Status: **pre-registered, not yet measured.** This section is committed before
> the apparatus is built or run. The Apparatus, Assay, Verdict and Reproduce
> sections are appended afterward, in a follow-up commit, with the actual
> numbers — nothing above this line is edited after that commit lands except
> this status line.

## 🎯 Question

Ledger #1 (`docs/assays/0001-redis-adapter-throughput-ceiling.md`) measured
`autumn-harvest-redis`'s standalone throughput and killed its own registered
8-worker steady-state sub-question (8,760 mean vs 10,000 ops/sec), while
leaving the founding, unconstrained ">10,000 ops/sec" claim looking
achievable, not refuted. It explicitly could **not** produce a verified
multiplier against the Postgres control published in `docs/performance.md`:
two attempts to approximate that harness's workload shape both failed to
match it, and the report withdrew both multiplier claims rather than publish
an unmatched comparison. It named the fix as a re-charter: "reproducing
`claim_bench_support.rs`'s exact scenario shape (bounded-fraction draws,
multi-queue spread, wall-clock-bounded sampling) is a real engineering lift
... the honest scope for that follow-up is porting or directly reusing that
harness's scenario definitions against a Redis backend."

This assay is that re-charter. Reading `claim_bench_support.rs` directly
(rather than approximating it) gives the exact scenario shape `docs/performance.md`'s
"Claim latency vs backlog depth" table measures:

- **Baseline gate**, no build-id/concurrency/rate-limit/pause predicates.
- **8 concurrent claimers, backlog spread round-robin across 4 queues**
  (`queue_name = '{prefix}-q-' || (i % 4)`), every claimer polling across all
  4 queue names.
- **Bounded-fraction draw**: total measured claim operations =
  `measured_claims_for(backlog) = min(800, backlog / 5)` — never more than a
  fifth of the seeded backlog, so the backlog stays at 80-100% of its seeded
  depth for the whole measured window. Split exactly across claimers via
  `claims_for_claimer` (first `total % claimers` claimers take one extra op).
- **claim-only**, no enqueue and no complete/ack during the measured phase —
  `docs/performance.md`'s table measures `claim_task` alone, so this assay
  calls `RedisTaskQueue::claim` alone and does not call `complete`.
- **Window-folding**: each claimer times its own `(resume, finish)` span; the
  reported window is `measured_window` = earliest resume to latest finish
  across all claimers (ported verbatim from `claim_bench_support.rs`, not
  reimplemented from a description), so the throughput denominator does not
  understate contention the way a per-claimer `deadline`-based clock would.

Falsifiable question: **at this exact scenario shape — 8 claimers, 4 queues,
bounded-fraction claim-only draw, at the published 10,000-row headline
backlog — does `RedisTaskQueue::claim` sustain claims/sec at least 10x
`docs/performance.md`'s published Postgres number (29 claims/s), i.e.
≥290 claims/s?**

**Decision this feeds:** whether the Redis-adapter worker-integration
follow-up (the transactional-boundary refactor `autumn-harvest-redis/src/lib.rs`
names as a prerequisite, and which ledger #1 found is entirely unbuilt) is
worth prioritizing on the near-term roadmap, versus staying deferred behind
other backlog items. A decisive, matched-workload multiplier is evidence
worth weighing in that prioritization call; a marginal one is evidence the
other roadmap items should keep priority. **This assay cannot and does not
decide whether to build the integration** — only whether the throughput case
for doing so, under fair conditions, is strong. **Decider:** whoever owns
`docs/autumn-workflow-architecture.md`'s Phase-4 roadmap and prioritizes the
adapter backlog — same decider as ledger #1.

## ⚖️ Pre-registration

- **Registered cell (success/kill line):** the **10,000-row backlog** cell —
  `docs/performance.md`'s headline, CI-defended scenario (`headline_scenario()`
  in `claim_bench_support.rs`: `backlog: 10_000, claimers: 8, queues: 4`).
- **Success line (pursue):** Redis claims/s at the registered cell ≥ **290
  claims/s** (10x the published Postgres 29 claims/s at the same backlog
  depth). Ten times, not "any margin," because the decision this feeds is
  whether to prioritize real engineering work (the integration refactor); a
  threshold is chosen to separate "decisively worth it" from "faster, but not
  obviously worth reprioritizing the backlog for."
- **Kill line:** < 290 claims/s at the registered cell. A miss here means the
  matched-workload advantage, while it may still exist, is not decisive
  enough on its own to move the integration refactor up the priority list.
- **Supplementary cells (measured, not gated):** the same shape at **1,000**
  and **100,000**-row backlogs, matching the other two rows of
  `docs/performance.md`'s table (published: 640 claims/s and 3 claims/s
  respectively), reported for context but not compared against a pre-set
  line.
- **Conditions:** loopback `redis-server` 7.0.15, `save ""` / `appendonly no`
  (matches the crate's own ephemeral-queue design intent, logged as a stub
  regardless), 4 logical CPUs, single Redis node, tokio multi-thread runtime,
  ~`{}`-sized JSON payload (matching `claim_bench_support.rs`'s own seeded
  rows, which carry an empty `'{}'::jsonb` input).
- **Control:** the already-published Postgres numbers in `docs/performance.md`'s
  "Claim latency vs backlog depth" table (640 / 29 / 3 claims/s at 1,000 /
  10,000 / 100,000 rows respectively), taken on the same reference machine
  shape (4 logical CPUs). Not re-measured here — admissible as published,
  reproducible-harness prior art, per the precedent ledger #1 set for the
  same control.
- **Time box:** this session, single pass.
- **Riskiest assumption attacked first:** whether a *genuinely matched*
  workload shape still shows a decisive Redis advantage, or whether the
  large multipliers implied by ledger #1's unmatched attempts were an
  artifact of the mismatch (near-empty queue vs. seeded backlog; single
  queue vs. four; full drain vs. bounded fraction) rather than a real
  property of the adapter. This is measured first and is the entire
  registered cell.
- **Containment:** a standalone Rust binary at
  `docs/assays/apparatus/0002-redis-matched-workload/`, path-dependency on
  `autumn-harvest-redis`'s existing public API only (zero modifications to
  `autumn-harvest-redis` or `autumn-harvest` source), never added to the
  workspace `Cargo.toml` members list. Runs against a local, ephemeral,
  disposable Redis inside this sandboxed session only. No production data,
  no spend, no network egress. The scenario constants and functions
  (`measured_claims_for`, `claims_for_claimer`, `measured_window`) are ported
  by value from `claim_bench_support.rs` (read, not guessed at) rather than
  re-derived from the prose description in `docs/performance.md`.
- **Anticipated stubs (finalized in the Apparatus section below):** single
  Redis node, no replication/failover, no auth/TLS, claim-only (no
  complete/ack — matches what `docs/performance.md`'s table itself measures,
  not a Redis-specific shortcut), one fixed empty-payload shape, loopback
  only, no worker/Postgres integration exercised, no warmup trimming applied
  to the Redis side unless the Assay section states otherwise (the Postgres
  control's published claims/s already includes its own warmup-trimmed
  latency percentiles but its claims/s column is `total_claimed / wall_secs`
  over the **untrimmed** window — mirrored here for a fair comparison), the
  Postgres control is not re-measured on this machine.

## 🔍 Prior art

- `docs/assays/0001-redis-adapter-throughput-ceiling.md` — the founding assay;
  measured the adapter's standalone ceiling, could not produce a matched
  comparison, and named this exact re-charter.
- `autumn-harvest/tests/integration/claim_bench_support.rs` — read directly
  for this assay: `headline_scenario()` (backlog 10,000 / claimers 8 / queues
  4 / `ClaimGate::Baseline`), `measured_claims_for` (bounded-fraction draw,
  capped at 800), `claims_for_claimer` (exact per-claimer split),
  `measured_window` (min-start/max-end folding), `seed_backlog` (round-robin
  queue assignment via `i % queues`).
- `docs/performance.md`'s "Claim latency vs backlog depth" table — the
  control, and the exact table this assay's registered cell is matched
  against.
- `autumn-harvest-redis/src/lib.rs`, `redis_queue.rs`, `envelope.rs` — the
  adapter's public API (`RedisTaskQueue::connect`, `claim(&[String], &str)`,
  `EnqueueParams`); confirms `claim` already accepts a queue-name slice, so
  polling across 4 queues from one claimer needs no new capability.
- `docs/assays/apparatus/0001-redis-throughput-bench/src/main.rs` — reused
  patterns from the already-reviewed apparatus (per-run unique key prefix,
  barrier-gated start, `Instant`-based per-worker windows) rather than
  reinventing them.

## 🧪 Apparatus

_Appended after the pre-registration commit, with the code as built._

## 📊 Assay

_Appended after the apparatus runs, with every measured cell._

## 🏁 Verdict

_Appended after the assay runs, compared against the lines above._

## 🔬 Reproduce

_Appended with exact commands once the apparatus exists._
