# ⛏️ Prospect: does the Redis adapter clear its own >10k ops/sec bar? (kill: 8,828 vs 10,000 ops/sec — but only for this assay's own 8-worker steady-state sub-question, ledger #1)

> Status: **measured.** The Pre-registration section below was committed
> (`3a0e3b9`) before the apparatus was built or run; nothing in it has been
> edited since except this status line. The Apparatus, Assay, Verdict and
> Reproduce sections were appended afterward, in a follow-up commit, with the
> actual numbers.

## 🎯 Question

`autumn-harvest-redis` already exists in the workspace (1,243 lines, shipped in
#1234 / issue-referenced by `docs/plans/vantage-spec-redis-adapter.md`), built
to satisfy that spec's stated success line:

> Success = Engine can reliably sustain > 10,000 tasks/second queue dispatch
> and claim operations.

No benchmark for this crate exists anywhere in the repository (`grep -rn
"throughput\|ops/sec\|tasks/sec\|bench" autumn-harvest-redis/` matches nothing
but a doc comment). Meanwhile `docs/performance.md` — which *does* measure the
Postgres claim path this crate is meant to escape — publishes real numbers far
below the "~10k ops/sec ceiling" the crate's own module doc and
`docs/autumn-workflow-architecture.md` cite as Postgres's breaking point: **640
claims/sec** at a 1,000-row backlog (8 concurrent claimers, 4 logical CPUs),
collapsing to **29/sec** at a 10,000-row backlog, due to a documented
structural query-plan defect (a non-indexable `ORDER BY` forcing a sequential
scan + sort on every claim), not an intrinsic Postgres/`SKIP LOCKED` limit.

Falsifiable question: **on a machine built to give the adapter every possible
advantage — loopback Redis, no Postgres in the loop, no worker integration, no
network hop — does `RedisTaskQueue` sustain ≥10,000 enqueue→claim→complete
round trips/sec through its own public API?**

A second, structural fact surfaced during prior-art reading and is reported
regardless of the throughput number: `autumn-harvest-redis/src/lib.rs`'s own
module doc states the crate is **not wired into `worker.rs`** — "this crate
ships the adapter and its tests so the integration work can build on a stable
foundation" — meaning no operator can use it as an escape hatch today, at any
throughput.

**Decision this feeds:** whether `docs/autumn-workflow-architecture.md`'s
Phase-4 claim —

> If a deployment needs >10,000 tasks/second sustained, Harvest will support
> an optional `autumn-harvest-redis` adapter crate (Phase 4) that uses Redis
> Streams for the task queue... But this is an escape hatch, not the default
> path.

— is safe to keep documenting as a working, verified escape hatch, or needs a
correction (the adapter is unintegrated, and/or its headline number is
unverified). **Decider:** whoever owns that architecture doc and the spec —
this is a docs-accuracy and roadmap-prioritization call (does the
worker-integration follow-up get scheduled), not a request to build anything.

## ⚖️ Pre-registration

- **Success line (pursue on the throughput sub-question):** `RedisTaskQueue`
  sustains **≥10,000 ops/sec** (one op = one full `enqueue` → `claim` →
  `complete` round trip through the adapter's public API, matching the spec's
  own "queue dispatch and claim operations" framing) at 8 concurrent worker
  tasks, over a ≥10s measured window, on the machine described below.
- **Kill line:** <10,000 ops/sec sustained under the same conditions. This is
  independent of the Postgres comparison — it is the crate's own founding
  number, tested under the best-case conditions it will ever see.
- **Conditions:** loopback `redis-server` 7.0.15, `save ""` / `appendonly no`
  (an ephemeral task queue backing a system where Postgres remains the durable
  source of truth is the crate's own stated design; disabling Redis
  persistence matches that intent rather than cheating the number — logged as
  a stub below regardless), 4 logical CPUs, single Redis node, no replication,
  ~40-byte JSON payload (activity-dispatch-shaped), tokio multi-thread
  runtime, single queue name.
- **Control:** the already-published Postgres claim-path numbers in
  `docs/performance.md` — 640 claims/s at a 1,000-row backlog / 8 concurrent
  claimers, 29/s at a 10,000-row backlog — taken on the same reference machine
  shape (4 logical CPUs, Postgres 16.13). Those numbers are prior art, not
  re-measured here; if time inside the box permits, a spot reproduction of the
  shallow-backlog cell is attempted as a same-run sanity check, but the
  published number is admissible as the control on its own (published,
  reproducible harness, already in the repo).
- **Time box:** this session, single pass.
- **Riskiest assumption attacked first:** raw adapter throughput with nothing
  else in the way. If the standalone adapter cannot clear 10k ops/sec with no
  Postgres, no worker, no network latency, and no other tenant on the box, the
  "escape hatch" framing is dead regardless of whether the worker-integration
  follow-up ever ships. That is measured before anything else.
- **Containment:** apparatus is a throwaway Rust example calling
  `autumn-harvest-redis`'s existing public API with zero modifications to
  `autumn-harvest-redis` or `autumn-harvest` source. It runs against a local,
  ephemeral, disposable Redis and Postgres inside this sandboxed session only.
  No production data, no spend, no network egress. Archived under
  `docs/assays/apparatus/0001-redis-throughput-bench/`, marked non-production,
  and never wired into the workspace `Cargo.toml` members list.
- **Anticipated stubs (finalized in the Apparatus section below):** single
  Redis node, no cluster/replica failover, Redis persistence disabled, one
  fixed small payload shape only, no worker/Postgres integration exercised
  (the adapter is used standalone, matching its own documented scope), loopback
  only (no real network hop, which a production deployment would have), no
  auth/TLS, `recover_pending` visibility-timeout recovery path not exercised
  under load.

## 🔍 Prior art

- `docs/plans/vantage-spec-redis-adapter.md` — the founding spec; sets the
  >10,000/sec success line and asserts (uncited) that Postgres "eventually
  hits a ceiling around 10k ops/sec."
- `docs/autumn-workflow-architecture.md:1012-1015` — repeats the >10,000/sec
  framing as the trigger for reaching for this crate.
- `docs/performance.md` — the only *measured* claim-path numbers in the repo.
  They do not support the ~10k ops/sec Postgres ceiling framing: the measured
  ceiling is two to three orders of magnitude lower and is dominated by
  backlog depth via a named, partially-fixed structural defect, not by a flat
  ops/sec wall.
- `docs/benchmarks.md` — end-to-end workflow throughput (23.73-35.70
  workflows/sec at 1-2 shards); a different metric (completed workflows, not
  raw queue ops) and not directly comparable, noted for completeness only.
- `autumn-harvest-redis/src/lib.rs` — the crate's own module doc, which is the
  source of the "not wired into worker.rs" structural finding above.
- No prior benchmark of `autumn-harvest-redis` exists (`git log --oneline
  --all -- autumn-harvest-redis` shows two commits: initial delivery and an
  unrelated release bump; no `*bench*` file anywhere under the crate).

## 🧪 Apparatus

A standalone Rust binary (`docs/assays/apparatus/0001-redis-throughput-bench/`,
archived, **not a workspace member, never build against it**) with a path
dependency on the real `autumn-harvest-redis` crate. It calls only that
crate's existing public API (`RedisTaskQueue::connect`, `enqueue`, `claim`,
`complete`) — zero lines of `autumn-harvest-redis` or `autumn-harvest` were
touched.

Per worker task: enqueue one ~40-byte JSON payload to the **one shared queue**
named in the pre-registration, then loop `claim` (200µs backoff between empty
polls, claiming as its own distinct consumer identity within the queue's
consumer group) until it gets a task back — not necessarily the one it just
enqueued, since any of the N workers may claim any pending entry, which is
exactly the shared-queue contention a single queue name is meant to exercise
— then `complete` it, then repeat until the deadline. A round trip counts
**only if `complete` returns `Ok`**, so a transient ack failure cannot inflate
the number. All workers connect and reach a `tokio::sync::Barrier` before any
of them starts the enqueue/claim/complete loop, and the measured window
starts only once every worker has cleared that barrier, so no worker runs
solo while others are still opening a connection. Concurrency swept 1 / 4 / 8
(the registered cell, matching the Postgres control's "8 concurrent
claimers") / 16 / 32 / 64 (the last three are supplementary, added after
seeing the 8-worker result — flagged as such, not folded into the registered
verdict). 10s measured window per cell. Machine: this session's 4-logical-CPU
container, local `redis-server` 7.0.15 on loopback, `save ""` / `appendonly
no`.

> **Post-review correction #1** (commit `505a176`). The first version of this
> apparatus (commit `368abec`) gave each worker its own dedicated stream
> rather than the single shared queue the pre-registration committed to,
> counted a round trip before checking whether `complete` succeeded, and
> started its measured window before every worker had a live connection. All
> three were flagged by automated review on the PR and are fixed in the
> version described above and archived here; the numbers in this report are
> from the corrected apparatus. The corrected, contention-bearing
> shared-queue numbers landed within noise of the original dedicated-stream
> numbers (registered cell: 8,586-9,092 ops/sec across three shared-queue
> runs vs. 8,578-8,643 ops/sec across the original dedicated-stream runs) —
> the fix changed what was actually being tested, not the verdict.
>
> **Post-review correction #2 — a second, more consequential methodology gap.**
> Automated review also pointed out that the steady-state
> enqueue→claim→complete loop above keeps the queue near-empty by
> construction (each worker enqueues one item and immediately tries to claim
> it back), which is *not* the workload `docs/performance.md`'s Postgres
> number measures: that number is "N real `claim_task()` calls draining the
> full backlog" — claimers only, against a **static, pre-seeded** 1,000-row
> backlog, no concurrent producer. Comparing the two as a controlled,
> matched-workload result (as the original version of this report did, under
> a "13.8x" headline) was not sound. A `backlog_drain_scenario` was added
> to the apparatus afterward — seed 1,000 entries, then 8 claim-only workers
> drain them with no further enqueues, mirroring the Postgres methodology
> exactly — and is reported below as a **separate, explicitly post-hoc**
> measurement: it was run after the registered result was already known, so
> it is evidence for a follow-up charter, not a replacement for the
> registered verdict. See Verdict for how the two are kept apart.

**Stubs list (what was faked/skipped, and why it matters for reading the number):**

- Single Redis node. No replication, no cluster mode, no failover path
  exercised.
- Redis persistence disabled. Matches the crate's own "ephemeral queue,
  Postgres is the durable source of truth" design intent, but is still a
  condition a reader should discount for if their deployment enables AOF/RDB.
- One fixed small payload shape (~40 bytes) only. No large-payload or
  worst-case-payload cell.
- `claim` polls (no Redis `BLOCK`); at saturation this is a non-issue since a
  worker's own enqueue always precedes its claim, but it is a stub relative to
  a production polling/backoff strategy.
- **No worker integration, no Postgres, nothing else running on the box.**
  This is the single largest stub and the reason the number below is a
  best-case ceiling, not a deployment-representative one: `autumn-harvest`'s
  worker does not call this adapter at all today (see Prior art). The
  transactional-boundary refactor `autumn-harvest-redis/src/lib.rs` describes
  as a required follow-up is entirely unbuilt, so nothing measured here says
  anything about what throughput would survive that refactor.
- Loopback only. A real deployment's Redis is typically a separate host; a
  real network hop adds latency this number never pays.
- No auth/TLS, no `record_heartbeat`/`recover_pending`/`fail`/
  `requeue_for_retry`/`queue_depths` exercised, no concurrent Postgres load on
  the same box.
- One canonical timed run per cell for the full sweep, with the registered
  cell (8 workers, shared queue) independently repeated three times at the
  full 10s window: 8,586.70 / 9,091.80 / 8,805.20 ops/sec (mean 8,827.90,
  range 5.9% of the mean) — a shared, contended stream shows more run-to-run
  variance than the original dedicated-stream design did (0.8% apart across
  two runs), which is itself a small piece of evidence that the fix changed
  a real contention path rather than being a no-op.
- The Postgres control is **not** re-measured on this machine (see
  Pre-registration — admissible as-is); reproducing it would require draining
  a 10,000-row backlog under the existing `claim_bench` harness, which the
  harness's own docs put at minutes, not seconds, and that did not fit this
  session's box for marginal value over the already-published, already
  fully-documented number.

## 📊 Assay

Full sweep, one canonical run, 10s measured window each, shared queue:

| concurrency (workers) | ops/sec (enqueue→claim→complete) | vs 10,000/sec line |
|--:|--:|:--|
| 1 | 2,540.30 | below |
| 4 | 5,616.20 | below |
| **8 (registered cell)** | **8,586.70** (3-run mean 8,827.90, range 8,586.70-9,091.80) | **below — 86-91% of line** |
| 16 (supplementary) | 11,840.00 | above |
| 32 (supplementary) | 14,457.00 | above |
| 64 (supplementary) | 14,706.00 | above (plateau) |

Control (`docs/performance.md`, published, same reference machine shape — 4
logical CPUs, not re-measured here): Postgres claim path sustains **640
claims/sec** at an 8-concurrent-claimer / 1,000-row-backlog scenario, falling
to **29/sec** at a 10,000-row backlog.

The steady-state number above (8,827.90 mean at 8 workers) and the Postgres
640 claims/sec figure are **not a matched-workload comparison** — see
post-review correction #2 — because the Redis loop keeps the queue near-empty
while the Postgres number specifically measures draining a static 1,000-row
backlog. The earlier version of this report divided one by the other and
called it "13.8x"; that comparison is withdrawn.

**Matched-workload comparison (post-hoc, not pre-registered):** seed a static
1,000-row backlog, drain it with 8 claim-only workers, no further enqueues —
the same shape as the Postgres cell. Three runs, each racing to drain the
full 1,000 rows (wall-clock ceiling 60s, never approached):

| run | claims/sec | drained before ceiling |
|--:|--:|:--|
| 1 | 12,358.76 | yes |
| 2 | 11,738.81 | yes |
| 3 | 12,716.71 | yes |

Mean **12,271.43 claims/sec** — **above the 10,000/sec line**, and **19.2x**
the Postgres control (640 claims/sec) at the same backlog depth and
concurrency. This is the more faithful reproduction of what
`docs/performance.md` and the founding spec both actually describe
("dispatch and claim operations" against a real backlog), and it does not
agree with the registered steady-state result: at the same 8-worker
concurrency, claim-only draining is ~40% faster than enqueue-claim-complete
under steady load, which makes sense (one fewer Redis round trip per op with
no enqueue in the timed path, and no empty-poll backoff since the backlog
never runs dry before the ceiling).

Separately, the steady-state sweep does show the adapter crossing 10,000/sec
once concurrency roughly doubles past the registered 8-worker shape,
plateauing near 14.5-14.7k ops/sec by 32 workers — consistent with a single
Redis instance's serialized command throughput becoming the limit, not this
box's 4 cores (workers 8→64 is an 8x concurrency increase for only a 1.7x
throughput increase).

## 🏁 Verdict

**KILL — but scoped narrowly, and automated review correctly caught this
report initially overstating that scope.** The pre-registered claim was:
"`RedisTaskQueue` sustains ≥10,000 ops/sec, one op = one enqueue→claim→complete
round trip, at **8 concurrent worker tasks**." That 8-worker figure was *this
assay's own choice*, made for comparability with the Postgres control's
shape — it is not a constraint the founding spec itself imposes.
`docs/plans/vantage-spec-redis-adapter.md:10` sets no worker-count condition
at all: "Success = Engine can reliably sustain > 10,000 tasks/second queue
dispatch and claim operations."

On the **registered, 8-worker, steady-state sub-question**: the adapter
measured 8,586.70-9,091.80 ops/sec (mean 8,827.90) across three runs, against
the committed **≥10,000 ops/sec** line. That is a miss on every one of the
three runs, under the single most favorable conditions this adapter will
ever run in (no Postgres, no worker, no network hop, nothing else on the
box). Per the pre-registration, this is a no for that specific sub-question —
not a rounding call.

**This does not establish that the founding spec's own, unconstrained claim
is false, and the first version of this report was wrong to imply it did.**
Two pieces of evidence, both gathered honestly but neither pre-registered
(so neither overrides the registered kill above — they inform the next
charter instead):

1. The steady-state sweep itself crosses 10,000/sec once concurrency doubles
   past 8 workers: 11,840-12,058 ops/sec at 16 workers, plateauing near
   14.5-14.7k by 32.
2. The post-hoc, matched-workload backlog-drain measurement (see Assay) —
   the one that actually mirrors how `docs/performance.md`'s Postgres number
   was taken — clears 10,000/sec even at 8 workers: mean 12,271.43
   claims/sec, 19.2x the Postgres control at the same backlog depth and
   concurrency.

Read together: **the founding, unconstrained ">10,000 ops/sec, reliably
sustained" claim looks achievable, not refuted**, once the workload shape
matches what either the spec or the Postgres control actually describes. What
*is* genuinely established is narrower and still worth having: an
enqueue-heavy, always-near-empty-queue workload at exactly 8 concurrent
workers does not clear 10,000/sec on this hardware — a real, if
narrower, finding, not a verdict on the crate as a whole.

Separately, and unaffected by any of the above either way:
`autumn-harvest-redis/src/lib.rs`'s own module doc states the crate has never
been wired into `worker.rs` — the transactional-boundary split it describes
as a prerequisite is unbuilt. Whatever this adapter can sustain in isolation,
no operator can turn it on as the "escape hatch"
`docs/autumn-workflow-architecture.md` describes. This was true before this
assay and no measurement here changes it.

**For the named decision** (is `docs/autumn-workflow-architecture.md`'s
Phase-4 claim safe to keep documenting as-is): **no, but not because the
number is false** — because it was never measured at all, and "never
measured" is not the same finding as "measured and wrong." The doc should
say: the founding >10,000/sec claim has now been measured and looks
achievable (12,271 claims/sec mean, matched-workload, exploratory), an
artificially narrow 8-worker/steady-state sub-question was separately killed
(8,828 mean), and — the part that actually gates usability — the adapter is
unintegrated with the worker regardless of any of these numbers.

**Re-charter, not close:** two follow-ups are now worth pre-registering
properly, since the exploratory data above was gathered after this assay's
own registered result and should not be laundered into a second verdict
without its own commitment: (1) a backlog-drain-methodology assay, matching
the Postgres control's actual shape from the start rather than discovering
the mismatch mid-assay, across a range of concurrency and backlog depths; (2)
once the worker-integration refactor lands, a deployment-shaped assay (real
worker pool size, real transactional-boundary cost included) that is the one
that should actually gate the "ship this as the documented escape hatch"
decision.

## 🔬 Reproduce

```bash
# Redis: local, ephemeral, no persistence.
redis-server --daemonize yes --port 6379 --save "" --appendonly no

# Build and run the archived apparatus (path-dependency on the real crate;
# never add this to the workspace Cargo.toml).
cd docs/assays/apparatus/0001-redis-throughput-bench
BENCH_SECS=10 cargo run --release
# prints one "concurrency,ops_completed_per_sec" line per swept steady-state
# cell, then one "backlog_drain,backlog=1000,claimers=8,claims_per_sec=...,
# drained_before_ceiling=..." line for the matched-workload comparison
```

The Postgres control is reproduced via the existing, already-documented
harness in `docs/performance.md` (`cargo bench -p autumn-harvest --features db
--bench claim_bench`), not re-run for this report — see Apparatus stubs list.
