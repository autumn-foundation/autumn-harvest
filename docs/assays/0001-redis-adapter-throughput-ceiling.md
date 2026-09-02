# ⛏️ Prospect: does the Redis adapter clear its own >10k ops/sec bar? (kill: 8,828 vs 10,000 ops/sec @ matched concurrency, ledger #1)

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

> **Post-review correction.** The first version of this apparatus (commit
> `368abec`) gave each worker its own dedicated stream rather than the single
> shared queue the pre-registration committed to, counted a round trip before
> checking whether `complete` succeeded, and started its measured window
> before every worker had a live connection. All three were flagged by
> automated review on the PR and are fixed in the version described above and
> archived here; the numbers in this report are from the corrected apparatus.
> The corrected, contention-bearing shared-queue numbers landed within noise
> of the original dedicated-stream numbers (registered cell: 8,586-9,092
> ops/sec across three shared-queue runs vs. 8,578-8,643 ops/sec across the
> original dedicated-stream runs) — the fix changed what was actually being
> tested, not the verdict.

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

At the concurrency shape the control was measured under (8 concurrent
workers/claimers), the adapter beats the Postgres number by **13.8x**
(8,827.90 mean vs 640 claims/sec) — a large, real margin, even though it
misses the crate's own flat 10,000/sec bar at that same concurrency. The
adapter does cross 10,000/sec, but only once concurrency roughly doubles past
the registered/control-matched shape, then plateaus near 14.5-14.7k ops/sec by
32 workers — consistent with a single Redis instance's serialized command
throughput becoming the limit, not this box's 4 cores (workers 8→64 is an
8x concurrency increase for only a 1.7x throughput increase).

## 🏁 Verdict

**KILL** — on the pre-registered claim, at the pre-registered condition. The
adapter measured 8,586.70-9,091.80 ops/sec (mean 8,827.90) across three runs
at 8 concurrent workers on the pre-registered shared queue, against the
committed **≥10,000 ops/sec** line. That is a miss on every one of the three
runs, under the single most favorable conditions this adapter will ever run
in (no Postgres, no worker, no network hop, nothing else on the box). Per the
pre-registration, this is a no, not a rounding call — 400 vs a 200ms line was
never the test here, and neither is 8,800-ish vs 10,000.

Two things sit alongside that kill and change what it means for the decision,
rather than reversing it:

1. **The adapter is not dead technology.** Against the actual measured control
   (not the vendor-style "~10k ceiling" narrative the spec and architecture
   doc assert but never measured), it wins by 13.8x at matched concurrency,
   and does clear 10,000/sec at roughly double the worker count. The founding
   claim is wrong as a flat, concurrency-independent number, but the
   underlying approach clearly outperforms Postgres by a wide margin on this
   hardware.
2. **None of this is reachable today.** `autumn-harvest-redis/src/lib.rs`'s
   own module doc states the crate has never been wired into `worker.rs` —
   the transactional-boundary split it describes as a prerequisite is
   unbuilt. Whatever this adapter can sustain in isolation, no operator can
   turn it on as the "escape hatch" `docs/autumn-workflow-architecture.md`
   describes. This was true before this assay and is unaffected by the
   throughput number either way.

**For the named decision** (is `docs/autumn-workflow-architecture.md`'s
Phase-4 claim safe to keep documenting as-is): **no.** It currently describes
a working, verified >10,000/sec escape hatch. It should instead say the
adapter exists, is unintegrated, and its standalone throughput has now been
measured (crosses 10k/sec only above ~16 concurrent workers on a 4-core
reference box; beats the measured Postgres path by an order of magnitude at
matched concurrency). That correction does not require deciding whether to
prioritize the integration follow-up — it is owed regardless, immediately,
because the current docs assert a measurement that did not exist until this
report.

**Re-charter, not close:** the underlying "is this worth finishing"
question is genuinely open and worth a properly scoped follow-up assay once
someone is ready to spend on it — not this one, whose registered line was
answered. A sharper next charter: pick the concurrency shape a real
`autumn-harvest` deployment would actually run (worker pool size, not an
arbitrary sweep), and gate the worker-integration decision on that number,
post-refactor, against a control that includes the transactional-boundary
cost this assay explicitly stubbed out.

## 🔬 Reproduce

```bash
# Redis: local, ephemeral, no persistence.
redis-server --daemonize yes --port 6379 --save "" --appendonly no

# Build and run the archived apparatus (path-dependency on the real crate;
# never add this to the workspace Cargo.toml).
cd docs/assays/apparatus/0001-redis-throughput-bench
BENCH_SECS=10 cargo run --release
# prints one "concurrency,ops_completed_per_sec" line per swept cell
```

The Postgres control is reproduced via the existing, already-documented
harness in `docs/performance.md` (`cargo bench -p autumn-harvest --features db
--bench claim_bench`), not re-run for this report — see Apparatus stubs list.
