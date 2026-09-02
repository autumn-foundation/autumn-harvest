# ⛏️ Prospect: does the Redis adapter clear its own >10k ops/sec bar? (pre-registration)

> Status: **pre-registered, not yet measured.** This section is committed before
> the apparatus is built or run. The Assay section below is appended in a
> follow-up commit with results; nothing in this section is edited after that
> point except to fix a typo that doesn't change a number or a line.

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
