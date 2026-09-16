# Pre-registration: cross-mode throughput (SQLite / Postgres / Redis+Postgres)

> Committed **before** the apparatus was built or run. Assay ledger #10.
> Nothing in this file is edited after the first measurement; the report at
> `docs/assays/0010-cross-mode-throughput.md` appends the apparatus, the
> numbers and the verdict.

Source tree: `3d6ad88`. Registered 2026-09-16.

## 🎯 Question

Harvest ships three persistence/dispatch modes. No single run has ever put
them side by side:

| mode | what it is |
|:--|:--|
| SQLite | `autumn-harvest-sqlite` — embedded, single-writer, caller-driven `run_until_idle` |
| Postgres | `autumn-harvest` core — a `Worker` pool on the Postgres claim path |
| Redis + Postgres | the same pool with `RedisDispatch` installed; Postgres stays the source of truth |

What exists instead is four measurements taken at four different shapes, on
at least three different hosts, against at least three different source
trees: `docs/benchmarks.md` (23.73 workflows/sec, 3-activity, Postgres,
bounded closed loop), ledger #2 (18,933 claims/sec, Redis `claim` in
isolation, no Postgres write behind it), ledger #8 (173.04 tasks/sec,
1-activity, integrated Redis dispatch, against a control crippled by the
since-fixed #1428 sampler defect) and ledger #9 (a tail-latency re-charter
that names its own un-isolated confounds). SQLite appears in none of them.

Ledger #9 closes by naming the missing thing outright: *"a same-host,
same-lockfile, same-source-tree ablation to isolate the fix's own
contribution remains an open, cheap, un-run pit."* This assay is that pit,
widened to three arms and moved to the shape `docs/benchmarks.md` actually
publishes.

Falsifiable question: **at the canonical 3-activity workflow shape, on one
box, from one tree, in one sitting, what is each mode's sustained
completed-workflows/sec — and does the Redis dispatch channel still buy a
decisive margin over plain Postgres once the control arm is healthy?**

**Decision this feeds:** whether `docs/benchmarks.md` and
`docs/sqlite-backend.md` can carry a mode-selection table backed by measured
numbers rather than by the qualitative "Throughput: modest, single-writer /
high, horizontally scaled" row `docs/sqlite-backend.md` carries today.
**Decider:** whoever owns `docs/benchmarks.md`.

## ⚖️ Pre-registration

### Shape

One workload, ported **by value** from
`autumn-harvest/tests/integration/e2e_bench_support.rs` rather than
approximated — ledger #2's failure mode was an apparatus that plausibly
resembled the control's shape and did not match it:

* the canonical 3-activity sequential workflow (`BENCH_ACTIVITIES`, 3 entries)
* `DISPATCHES_PER_WORKFLOW` = 7 claims per completed run
* worker pool shape from the published constants: `MAX_CONCURRENT_WORKFLOWS`
  = 8, `MAX_CONCURRENT_ACTIVITIES` = 16, `POLL_INTERVAL_MS` = 25,
  `POOL_SIZE_PER_SHARD` = 32
* one shard, one worker — `WORKERS_PER_SHARD` = 1
* **drain shape**: seed a backlog of 2,000 workflows, start the pool, measure
  wall-clock to terminal completion of all of them; report completed
  workflows/sec over that window
* 3 repetitions per arm; report mean and range

### Lines

**L1 — apparatus validity (the Postgres arm must reproduce the published
shape).** The `postgres` arm's mean completed workflows/sec lands **within 3x
either side of 23.73**, the `docs/benchmarks.md` v0.6.0 1-shard `throughput`
cell — i.e. in `[7.91, 71.19]`.

*Why a 3x band and not a tolerance:* this apparatus drains a seeded backlog;
`docs/benchmarks.md` runs a bounded closed loop at 32 in flight with paced
starts. Same workflow, same pool constants, same class of box, different load
discipline. A band wide enough to survive that difference but narrow enough to
catch "this apparatus is measuring something else entirely" is the most this
check can honestly claim. **Kill = outside the band**, and on a kill no
cross-mode number from this apparatus is reported as comparable to the
published one.

**L2 — does Redis dispatch still pay at this shape?** `redis_pg` mean
completed workflows/sec ≥ **2.0x** `postgres` mean.

*Why 2.0x:* ledger #8's only finite arm-pair measured 10.72x, but that was a
1-activity shape and a sampler-quieted diagnostic, and #1428 has since been
fixed — so the control arm this assay runs is healthy in a way #8's never
was, and the margin should compress by an unknown amount. 2.0x is set as the
threshold below which the channel stops being worth its operational cost (a
second stateful dependency) at this shape. **Kill = < 2.0x.** A kill here is
a real finding about the 3-activity shape, not a retraction of ledger #2 or
#8, which measured different things.

**L3 — is the embedded mode competitive on one box?** `sqlite` mean
completed workflows/sec ≥ `postgres` mean.

*Why this direction:* the Postgres arm pays loopback round trips, pool
checkout, `LISTEN`/`NOTIFY` and multi-worker coordination that the embedded
arm does not pay at all; the embedded arm is single-writer and drives
activities as synchronous bodies on one thread, which the Postgres arm
parallelises across 16 activity slots. Which effect dominates at this shape
is genuinely unknown to the registrant. **Kill = `sqlite` < `postgres`.**

### Correctness precondition (all arms, every repetition)

A throughput number from a run that did not do the work is void. Every rep
must satisfy all three, or that rep is discarded and reported as discarded:

1. every seeded execution reaches `COMPLETED`;
2. the side-effect row count equals `workflows × 3` exactly — one row per
   activity, so a duplicate activity execution or a dropped one both fail;
3. on the `redis_pg` arm only: an empty stream, empty PEL and empty marker
   set after the run, per ledger #8's own drain probe.

### Conditions

* 4 logical CPUs (Intel Xeon @ 2.10GHz, 1 thread/core), 15 GiB RAM.
* Native loopback PostgreSQL 16.13, `fsync=off`, `synchronous_commit=off`,
  `max_connections=300` — matching `benchmarks/docker-compose.yml`, which
  documents `fsync=off` as deliberate and says so next to the published
  numbers. Not Docker: prior assays (#8) used native loopback, and four cores
  is a poor place to add container overhead to a control arm.
* Native loopback `redis-server` 7.0.15, `save ""`, `appendonly no` — the
  ephemeral-queue configuration ledger #1 registered and justified.
* SQLite arm on a **file-backed** database (not `open_in_memory`), so all
  three arms pay real storage.
* Box otherwise idle. `docs/benchmarks.md` makes idleness a precondition
  rather than a formality: a concurrent build there moved a published latency
  by more than 10x on this same class of box. No build, no container pull and
  no other assay runs during a measured window.

### Declared stubs (named now, not after the numbers)

* **The SQLite arm is not the same kind of thing as the other two, and no
  verdict here should be read as if it were.** It is single-writer by
  contract, drives activities as synchronous caller-supplied bodies rather
  than async handlers, has no `LISTEN`/`NOTIFY`, and is pumped by a
  caller-owned `run_until_idle` loop whose cadence this apparatus chooses.
  L3 compares the two numbers because an evaluator choosing a mode has to
  compare them; it does not claim they measure the same architecture.
  `docs/sqlite-backend.md` lists schedules, DAGs, the management API,
  sharding and multi-server recovery as v0.1 non-goals, and none of that
  absence is priced into a workflows/sec cell.
* One box, one shard, one worker. Nothing here measures scale-out, and the
  Postgres and Redis arms are exactly the deployment shape that flatters the
  embedded arm most.
* No network hop on any arm: Postgres and Redis are both loopback. A real
  deployment pays a hop the Redis arm pays twice per dispatch.
* Single Redis node, no replication, no persistence.
* The drain shape measures saturated throughput. It says nothing about
  latency at low utilisation, which is the regime `docs/benchmarks.md`'s
  `dispatch_latency` and `signal_roundtrip` scenarios cover and this assay
  does not.
* Dependency artifacts for the workspace were compiled before this file was
  committed. The apparatus itself is built after, from a clean target dir,
  per the remedy ledger #9 adopted. No measurement of any kind had been taken
  when these lines were set.

## Reproduce

Apparatus at `docs/assays/apparatus/0010-cross-mode-throughput/`, added in the
follow-up commit. Run instructions in its README.
