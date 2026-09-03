# ⛏️ Prospect: does Redis clear a decisive matched-workload margin over Postgres at the 10k-backlog headline cell? (pursue: 18,933 vs 290 claims/sec line, ledger #2)

> Status: **measured.** The Pre-registration section above was committed
> (`883fd7a`) before the apparatus was built or run; nothing in it has been
> edited since except this status line. The Apparatus, Assay, Verdict and
> Reproduce sections below were appended afterward, in a follow-up commit,
> with the actual numbers.

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

A standalone Rust binary
(`docs/assays/apparatus/0002-redis-matched-workload/`, archived, **not a
workspace member, never build against it**) with a path dependency on the
real `autumn-harvest-redis` crate. Zero lines of `autumn-harvest-redis` or
`autumn-harvest` were touched.

Four functions are ported **by value** from
`autumn-harvest/tests/integration/claim_bench_support.rs` (copied and
credited in a code comment, not re-derived from `docs/performance.md`'s
prose): `measured_claims_for` (`min(800, backlog / 5)`, floored at 1),
`claims_for_claimer` (exact split, remainder to the first N claimers),
`warmup_claims_for` (`collected / 10`, applied per claimer to *observed*
count so a truncated run still reports its real samples), and
`measured_window` (min-start/max-end fold across each claimer's own clock).

Per cell: seed `backlog` rows round-robin across 4 queue names
(`{prefix}-q-0..3`, mirroring `seed_backlog`'s `i % queues`), each an empty
`{}` JSON payload via `EnqueueParams::new`. 8 claimers connect, each with its
own `RedisTaskQueue` handle sharing the run's key prefix, and wait at a
`tokio::sync::Barrier`. On release, each claimer performs exactly
`claims_for_claimer(measured_claims_for(backlog), 8, index)`
`RedisTaskQueue::claim(&all_4_queue_names, worker_id)` calls — **claim only,
no `complete`/ack**, matching what `docs/performance.md`'s table itself
measures (`claim_task` alone) — timing each call and recording
`(latency_ms, got_a_task)`. A claimer stops early if the scenario's
wall-clock budget (120s, generous headroom over the sub-100ms cells actually
observed) elapses first.

After every claimer finishes: each claimer's own observations are split at
its warmup boundary exactly as `ClaimerOutcome::from_observed` does — the
post-warmup tail feeds the latency samples (`n`) and the `claimed`/`empty`
counts, while `total_claimed` (the throughput numerator) counts every
successful claim across the *whole* observed sequence, warmup included,
because the wall-clock denominator (`measured_window`) starts at the first
warmup call too. `claims_per_sec = total_claimed / wall_secs`.

> **Post-review correction (Codex, two findings on the first pushed
> commit).** **P1 — the four queues were not actually being sampled.**
> `RedisTaskQueue::claim_inner` checks the queue list it's given *in order*
> and returns as soon as the first queue yields an entry
> (`autumn-harvest-redis/src/redis_queue.rs:416-464`). The apparatus's first
> version passed the same fixed `[q0, q1, q2, q3]` order to every claimer on
> every call; since `q0` alone holds 2,500 entries at the registered cell —
> far more than the 800-op measured budget — **every measured claim came
> from `q0`**, and `q1..q3` were never read at all. That silently reduced
> the "4 queues" scenario back to the single-queue shape ledger #1's own
> apparatus already tried and found unmatched, while still *reporting* a
> matching `n` (an `n` match is a function of `measured_claims_for` and
> warmup trimming, not of which queue a claim came from, so the earlier
> `n`-matches-published-`n` check did not — and could not — catch this).
> Fixed by rotating the queue order per call from a shared, call-ordered
> counter across every claimer (`run_claimer`'s `rotation: Arc<AtomicUsize>`
> in the apparatus source), so claims distribute across all four queues
> instead of draining one. Verified directly, not just by argument: after a
> post-fix run of the registered cell, `XPENDING <stream> harvest_workers`
> on each of the four queue streams reported **exactly 200 pending entries
> per queue** (800 total, matching `total_claimed`), each roughly evenly
> spread across the 8 consumer identities (individual worker counts ranged
> 12-35 per queue — real, not perfectly uniform, contention, not a
> single-queue skew).
>
> **P2 — each claim call was not individually bounded by the remaining
> scenario deadline.** The pre-registration and this section both describe
> "wall-clock-bounded sampling" ported from `claim_bench_support.rs`, but
> the first version only checked the deadline at the *top* of each
> claimer's loop, leaving the `claim` call itself an unbounded await — a
> stalled Redis would have sat past the advertised ceiling with the
> loop-top check never running again, exactly the failure mode
> `claim_bench_support.rs`'s own `tokio::time::timeout(deadline - now,
> queue::claim_task(...))` (`claim_bench_support.rs:4107-4119`) exists to
> prevent. Fixed by wrapping each call in
> `tokio::time::timeout(deadline - now, queue.claim(...))`, mirroring that
> pattern; a timeout now behaves the same as a hard error (break, and the
> claimer's `truncated` state is implied by having collected fewer than its
> planned `per_claimer` observations).
>
> **Post-review correction, round 2 (Codex, on the P1/P2 fix commit).** **P2
> follow-up — connection establishment itself was still unbounded.** Each
> claimer's `RedisTaskQueue::connect` was called sequentially, after
> `deadline` was already computed, but with no timeout of its own; an
> unreachable or stalled Redis could hang `run_cell` past
> `BENCH_SCENARIO_SECS` before a single claim was ever attempted, with no
> `truncated` report to show for it. Fixed by wrapping each connect in
> `tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),
> ...)`, mirroring the same pattern `claim_bench_support.rs` uses around its
> own pool checkout. This is a defensive fix for a failure mode this
> sandbox's local, always-reachable Redis never actually hit — re-running
> the registered cell after the fix reproduced the same range (18,974.27
> claims/s on one spot-check), so it is not folded into a new set of four
> repeats.
>
> **Post-review correction, round 3 (Codex, on the round-2 fix).** The
> round-2 fix bounded the connect call by a timeout but still `.expect()`-ed
> the result, so a timed-out or failed connect would panic the whole
> process rather than produce the promised truncated report —
> `claim_bench_support.rs`'s own checkout-timeout path returns an empty,
> explicitly-truncated `ClaimerOutcome` instead of unwrapping. Fixed the
> same way: connection establishment moved inside each claimer's spawned
> task; a timed-out or failed connect now clears the start barrier (via the
> newly-ported `arrive_at_start_gate`, also now used for every claimer's own
> barrier wait, not only the failure path — matching
> `claim_bench_support.rs` exactly) and returns an empty, sample-free
> `ClaimerOutcome` rather than panicking. Another spot-check of the
> registered cell after this fix (18,803.91 claims/s) again lands inside the
> already-reported range.
>
> **Post-review correction, round 4 (Codex, on the round-3 fix).** With
> connect failures now surviving instead of panicking, an all-connections-
> fail run would report zero latency samples — and the CSV printed that as
> `p50_ms=0.000,p99_ms=0.000`, indistinguishable from a genuinely
> instantaneous claim. `claim_bench_support.rs` renders a zero-sample
> consumer as `n/a` rather than `0` for exactly this reason. Fixed by making
> the percentile fields `Option<f64>` (`None` when `n == 0`), rendered as
> `n/a` in the printed row. Cosmetic only for every run this assay actually
> reports (`n = 720`/`184` throughout, never `0`); re-verified with another
> spot-check of the registered cell (19,083.43 claims/s), again inside the
> reported range.
>
> Both fixes changed the *mechanism*, not the *conclusion*: the corrected
> registered-cell mean (18,933.43 claims/s across four runs, see Assay) is
> within the pre-fix runs' range (15,319.64-19,140.83) and clears the same
> 290 claims/s line by the same roughly-two-orders-of-magnitude margin. The
> numbers in Assay and Verdict below are all from the corrected apparatus;
> the pre-fix numbers are not reported as a separate result, since the fix
> is what makes this apparatus actually answer the registered question
> (both P1's queue-4 fidelity and P2's timeout-bounding property that the
> Apparatus section above already claimed as ported).

**Stubs list (what was faked/skipped, and why it matters for reading the
number):**

- Single Redis node. No replication, no cluster mode, no failover path.
- Redis persistence disabled (`save ""` / `appendonly no`) — same rationale
  as ledger #1: matches the crate's own ephemeral-queue design intent, but a
  reader running with AOF/RDB enabled should discount for write-persistence
  overhead this number never pays.
- No auth/TLS, loopback only. A real deployment's Redis is typically a
  separate host; a network hop adds latency this number never pays.
- **Claim-only, no `complete`/ack.** This is not a Redis-specific shortcut —
  it is fidelity to the actual thing being matched: `docs/performance.md`'s
  table measures `queue::claim_task` in isolation, not a full task
  lifecycle. A deployment-shaped number (claim + complete, or claim + real
  activity work) is a different, not-yet-answered question.
- One fixed empty-payload (`{}`) shape, matching the Postgres seed's
  `'{}'::jsonb`. No large-payload cell.
- No worker/Postgres integration exercised — the crate remains unwired into
  `worker.rs` (see ledger #1); nothing here changes that.
- The Postgres control is **not** re-measured on this machine — reused as
  published, per the precedent ledger #1 set for the same control (the
  headline scenario alone costs Postgres up to 240s per cell by its own
  documented wall-clock ceiling, and the 100k-row cell doesn't even finish
  within it — reproducing it here for a number that is already published,
  reproducible, and dated would buy little for the time it costs).
- `n` (the post-warmup sample count) is a function of how many operations
  were *planned* (`measured_claims_for`) minus warmup, not of anything the
  Redis side struggled with — every cell below finished with `truncated =
  false` and `empty = 0`, i.e. the apparatus never needed its wall-clock
  escape hatch and every attempted claim found a task. That itself is a
  result, not a stub, but it means this assay does not exercise Redis under
  backlog exhaustion or contention-driven `None` returns — a genuinely
  adversarial condition (a queue drained faster than it's fed, or a Redis
  instance under memory pressure) is untested here.

## 📊 Assay

All numbers below are from the **corrected** apparatus (post-review fixes
P1/P2, see Apparatus). Registered cell (10,000-row backlog) run four times
total (the sweep run below, plus three independent repeats), fresh
`FLUSHALL` and a fresh process-derived key prefix each time:

| run | n | claimed | empty | total_claimed | wall_secs | claims/s | p50 ms | p99 ms |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 (sweep) | 720 | 720 | 0 | 800 | 0.044 | 18,334.91 | 0.412 | 0.744 |
| 2 | 720 | 720 | 0 | 800 | 0.043 | 18,631.97 | 0.392 | 0.794 |
| 3 | 720 | 720 | 0 | 800 | 0.042 | 18,921.21 | 0.381 | 0.846 |
| 4 | 720 | 720 | 0 | 800 | 0.040 | 19,845.62 | 0.382 | 0.685 |

Mean **18,933.43 claims/s** across the four runs (range 18,334.91-19,845.62,
spread 7.98% of the mean — tighter than the pre-fix runs, consistent with
claims no longer piling contention onto a single queue). Every run: `n =
720` (exactly matching `docs/performance.md`'s published `n` for the same
10,000-row cell), `empty = 0`, `truncated = false`, and (directly verified
via `XPENDING` on the registered-cell run, not merely inferred) **200
claimed entries per queue across all four queues** — the fidelity check P1
required.

Control (`docs/performance.md`, "Claim latency vs backlog depth", published,
same reference machine shape, not re-measured here): **29 claims/s** at the
10,000-row / 8-claimer / 4-queue baseline cell (`n = 720` — identical to
this apparatus's `n`, confirming both sides drew the same
`measured_claims_for`-bounded sample).

Supplementary cells, same sweep run, not gated by a pre-set line:

| backlog | n (this run) | claims/s (this run) | Postgres control | ratio |
|--:|--:|--:|--:|--:|
| 1,000 | 184 (Postgres: 184) | 17,895.95 | 640 | ~28.0x |
| 10,000 (registered) | 720 (Postgres: 720) | 18,334.91 | 29 | ~632.2x |
| 100,000 | 720 (Postgres: 583, ⚠ truncated) | 18,587.22 | 3 | ~6,196x (not apples-to-apples — see below) |

The 1,000-row `n` (184) also matches the Postgres control's published `n`
exactly. The 100,000-row row does **not**: Postgres's own published cell hit
its wall-clock ceiling at `n = 583` (⚠, "cannot finish its planned 800
claims in three minutes" — see `docs/performance.md`), while this apparatus
finished the full planned 800 (`n = 720` post-warmup) in 41ms. The ratio
column for that row is arithmetic, not a claim that Redis is "6,542x
faster" at 100k rows in any meaningful sense — the Postgres side never got a
clean measurement to compare against at that depth in the first place
(that's the finding docs/performance.md itself reports: Postgres's claim
cost is dominated by backlog depth via a structural non-indexable `ORDER BY`
plan, and 100k rows blows through its own benchmark's time budget). This
row is reported for completeness, not compared by ratio, consistent with how
ledger #1 treated an unmatched comparison.

## 🏁 Verdict

**PURSUE, decisively, on the registered cell.** At the 10,000-row backlog,
8-claimer, 4-queue, bounded-fraction, claim-only scenario —
`docs/performance.md`'s own headline shape, reproduced with the harness's
own scenario-defining functions ported by value rather than approximated,
and (after the P1 fix) verified via `XPENDING` to actually be reading all
four queues rather than draining one — `RedisTaskQueue::claim` sustained a
mean **18,933.43 claims/s** across four independent runs (range
18,334.91-19,845.62), against a **≥290 claims/s** (10x the published 29
claims/s) pre-registered success line. Every run clears the line by roughly
two orders of magnitude (~632x-684x the Postgres control on the individual
runs, ~653x on the mean). This is not a borderline call decided by which run
you pick — the *lowest* of the four runs (18,334.91) still clears the line
by ~63x.

This closes the gap ledger #1 flagged and could not close: the earlier
assay's two unmatched attempts (near-empty steady-state loop, then a
single-queue full drain) both failed to reproduce
`claim_bench_support.rs`'s actual shape, and both multiplier claims were
withdrawn as a result. This assay's `n` column matching the published
Postgres `n` exactly at both the 1,000-row and 10,000-row cells (184 and 720
respectively) is direct, checkable evidence that this apparatus is now
measuring the same thing docs/performance.md measures, not a plausible
approximation of it — the failure mode that sank ledger #1's comparison
twice.

**What this verdict is, precisely, and what it is not.** It answers exactly
the registered question: under a fair, matched workload shape, is Redis's
claim throughput decisively ahead of Postgres's? Yes, overwhelmingly. It
does **not** claim a deployment would see anything close to this multiplier
— see the stubs list: no worker integration, no `complete`/ack, no network
hop, no concurrent Postgres load, no contention against an emptying queue,
loopback-only, single Redis node. The multiplier is largest at exactly the
condition that hurts Postgres the most (a deep, non-emptying backlog forcing
a sequential scan-and-sort on every claim) and Redis structurally never pays
that cost (a consumer-group read is not a function of backlog depth the way
a non-indexable `ORDER BY` is) — so the size of the number is explained by
*why* Postgres is slow here, not by anything specific this assay discovered
about Redis. That structural asymmetry was already documented in
`docs/performance.md` and ledger #1; this assay's contribution is
converting "looks achievable" and "no verified multiplier" into a measured,
matched-condition number with a citable methodology.

**For the named decision:** the throughput case for prioritizing the
worker-integration refactor is strong and, on this evidence, not
proportionate to a marginal call — a two-orders-of-magnitude matched-workload
advantage at the exact depth `docs/performance.md` flags as the point where
"the fleet's throughput collapses to a handful of claims per second
regardless of how many workers you add" is a real, decisive signal for
whoever prioritizes that backlog item. It is still, deliberately, **not**
this assay's place to decide whether the refactor ships: that call also
weighs the refactor's own cost and the deployments that would actually reach
a 10k-row backlog, neither of which this assay measured. What has changed is
that "the number was never verified under matched conditions" is no longer a
reason to discount the case.

**Re-charter, not close:** the natural next step is now the second item
ledger #1 named — a deployment-shaped assay once the worker-integration
refactor exists (real worker pool, real transactional-boundary cost, real
`complete`/ack in the loop) — which remains unbuildable until that refactor
lands, so it stays a shelf entry rather than a chartered assay today.

## 🔬 Reproduce

```bash
# Redis: local, ephemeral, no persistence, version-matched to the
# pre-registration (7.0.15).
redis-server --daemonize yes --port 6379 --save "" --appendonly no

# Build and run the archived apparatus (path-dependency on the real crate;
# never add this to the workspace Cargo.toml).
cd docs/assays/apparatus/0002-redis-matched-workload
BENCH_SCENARIO_SECS=120 cargo run --release
# prints one CSV line per swept backlog cell:
# backlog,n,claimed,empty,total_claimed,wall_secs,claims_per_sec,p50_ms,p99_ms,truncated
# defaults to backlog in {1000, 10000, 100000}; override with
# BENCH_BACKLOGS=1000,10000 (comma-separated).

# Repeat the registered cell alone, with a fresh FLUSHALL between runs:
redis-cli flushall
BENCH_BACKLOGS=10000 BENCH_SCENARIO_SECS=60 cargo run --release
```

The Postgres control is reproduced via the existing, already-documented
harness in `docs/performance.md` (`cargo bench -p autumn-harvest --features db
--bench claim_bench`), not re-run for this report — see Apparatus stubs
list.
