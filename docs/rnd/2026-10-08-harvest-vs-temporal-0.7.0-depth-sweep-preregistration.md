# Pre-registration: assay #11 rerun on 0.7.0, at every depth (assay ledger #14)

> Committed **before** any run of this assay. Nothing in this file changes
> after the first measurement. The report
> `docs/assays/0014-harvest-vs-temporal-0.7.0-depth-sweep.md` appends the
> apparatus, the numbers and the verdict.

Registered 2026-10-08, for issue #1972.

## 🎯 Question

Assay #11 measured harvest 7.91x behind Temporal in the default mode at a
2,000-row backlog. It measured harvest 1.5x to 2x behind in the best mode.
That assay predates #1796, #1797, #1798 and #1815. The claim-path fix (#1971,
PR #2052) was not written.

Questions:

1. On 0.7.0 as it ships, what is the gap at each depth, in each mode?
2. Does the claim-path fix flatten the default mode across depth?
3. After the fix, how far behind Temporal is the default mode at depth 2,000?

**Decision this feeds:** what `docs/comparison.md` and `docs/benchmarks.md`
say about cross-engine throughput. **Decider:** the owner of
`docs/comparison.md`.

## ⚖️ What this result does not mean

Assay #11's bounding section applies without change. Read it in
`docs/rnd/2026-09-16-harvest-vs-temporal-single-box-preregistration.md`.

- Four cores is near the bottom of Temporal's envelope. It is near the top of
  harvest's.
- A harvest win here is not evidence that harvest is faster than Temporal.
- A Temporal win here is strong for Temporal, because it comes despite the
  venue.
- The Temporal arm is not tuned by someone who operates Temporal.

No claim of the form "engine A is faster than engine B" follows from any
outcome.

## ⚖️ Pre-registration

### Trees

| label | commit | what it is |
|:--|:--|:--|
| base | `0aeb887` | 0.7.0, the base of PR #2052 |
| fix | `513b7aa` | the head of PR #2052, with the claim-path fix (#1971) |
| trunk | `9f444b7` | the `trunk-dev` head, 0.7.0 as this change ships it |

The fix is graded only against `0aeb887`. The two trees differ only by
PR #2052. `9f444b7` adds about 11,000 lines on top of `0aeb887`, mostly DR
fencing (#1823). A comparison of `513b7aa` with `9f444b7` mixes the two, so no
line uses it.

### Arms

| arm | mode | engine |
|:--|:--|:--|
| `postgres` | default: the Postgres claim path | harvest |
| `redis_pg` | best: Redis dispatch, Postgres the source of truth | harvest |
| `temporal_go` | Go SDK `v1.36.0`, Go 1.24.7, server `temporalio/auto-setup:1.25.2` | Temporal |

`temporal_go` is assay #11's binary, unchanged. The harvest arms are assay
#10's harness, with two changes. The harness sweeps depth in one process. It
also captures the #1815 signals (below). The workload, the slots, the pool,
the poll interval and the payload do not change.

### Shape

As assay #11 registered it:

- Canonical 3-activity sequential workflow. Inert activity bodies.
- 8 workflow slots, 16 activity slots, 1 worker, 32 connections, 25 ms poll,
  LISTEN/NOTIFY wired.
- Workflow input `{"p":"0123456789abcdef0123456789abcdef"}`.
- Drain shape: seed the backlog, start the worker, time to the last
  completion. Report completed workflows/sec.

### Matrix

- Depths: 250, 500, 1,000 and 2,000 seeded workflows.
- Three repetitions per cell: 3 trees × 2 harvest arms × 4 depths, plus
  `temporal_go` × 4 depths. That is 28 cells and 84 runs.
- Each repetition round runs every cell once. The order in a round:
  `temporal_go`, then `0aeb887`, `513b7aa` and `9f444b7`. Inside a tree, the
  order is `postgres` then `redis_pg`, each at 250, 500, 1,000 and 2,000.
- Every run starts on a freshly built database. The Temporal arm drops and
  rebuilds `temporal` and `temporal_visibility` before every run.

### Lines

Every line uses the mean of the valid repetitions. Ratios use unrounded
means. `docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0/grade.py` grades
the lines from the raw output. It is committed with this file.

**L1. The registered question, on 0.7.0.** `postgres` on `9f444b7` at depth
2,000 is at least `temporal_go` at depth 2,000. Pass or kill.

**L2. Best mode at every depth, on 0.7.0.** At each depth, the faster of
`postgres` and `redis_pg` on `9f444b7` is at least `temporal_go`. Pass only
when every depth passes.

**L3. The fix flattens the default mode.** `postgres` on `513b7aa` at depth
2,000 is at least 0.80 × `postgres` on `513b7aa` at depth 250.

**L4. The fix is the cause.** `postgres` on `513b7aa` at depth 2,000 is at
least 2.0 × `postgres` on `0aeb887` at depth 2,000.

**L5. Assay #11's re-charter prediction.** Assay #11 predicted that the fix
takes the default-mode gap at depth 2,000 to "roughly 2x". Line:
`temporal_go` ÷ `postgres` on `513b7aa`, at depth 2,000, is at most 2.5.

**Separation.** A line that compares two cells also reports whether the two
ranges overlap. An overlap does not change the grade. The report states it
beside the grade.

### Validity and correctness (every run)

1. The run drains inside a 900 s cap. A truncated run is invalid.
2. Every started workflow completes.
3. Activity runs equal `3 × depth` exactly.
4. The Redis arm leaves no stream entry, no pending entry and no marker.
5. The Temporal arm records zero workflow task failures and zero unread
   histories.

An invalid run is discarded and reported. A cell with no valid run makes each
line that reads it **indeterminate**.

### Attribution (#1815), descriptive only

The harness installs a recorder that returns `is_enabled() = false`, so no
sampler SQL runs. Per-event signals still reach it:

- `harvest.db.query.duration` for `claim`, `persist`, `scan` and `heartbeat`:
  count, mean and p99;
- `harvest.db.pool.wait_duration`: p99 and maximum;
- pool occupancy, read from the pool's own counters every 100 ms, with no
  SQL: mean and maximum in use.

These are not lines. Where `postgres` falls by more than 25% from depth 250
to depth 2,000 on a tree, the report names the signal whose mean rises most
over the same range.

### Conditions

- One box: 4 logical CPUs (Intel Xeon @ 2.80 GHz), 15 GiB RAM. Assay #11 ran
  on a 2.10 GHz Xeon, so its numbers are not compared with these.
- PostgreSQL 16.15, native, loopback, `fsync=off`, `synchronous_commit=off`,
  `max_connections=300`. Both engines use this one server, in separate
  databases.
- Redis 7.0.15, loopback, no persistence.
- The Temporal container runs only during the Temporal arm. The harvest pool
  runs only during a harvest arm.
- Every binary is built before the first run. No build, no git command and no
  other agent runs during the sweep.
- The Temporal image comes from `mirror.gcr.io`, because Docker Hub refused
  the pull with HTTP 429. It is the same tag. The report records the digest.

### Declared stubs

- One box, one shape, one Temporal version, one payload.
- Tuning asymmetry, as in assay #11.
- The `513b7aa` tree is a PR head, not a release.
- Worker startup is inside the measured window on both arms, as in assay #11.
  That flatters harvest at shallow depths.
- No crash-recovery time, no long run and no history bloat. Those are the
  stretch goals of issue #1972, and stay out of this assay.
- No measurement of any kind had been taken when these lines were set.

## Reproduce

Apparatus in `docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0/`. Run
instructions are in its README.
