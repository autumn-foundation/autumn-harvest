# ⛏️ Prospect: assay #11 rerun on 0.7.0, at every depth (0.7.0 kills at 7.17x; the #1971 fix closes it to 1.36x, ledger #14)

> Status: **measured.** The pre-registration is
> [`docs/rnd/2026-10-08-harvest-vs-temporal-0.7.0-depth-sweep-preregistration.md`](../rnd/2026-10-08-harvest-vs-temporal-0.7.0-depth-sweep-preregistration.md).
> It was committed and pushed (`535c943`) before any run. It is unchanged
> since. The sections below add the apparatus, the numbers and the verdict.

## 🎯 Question

Assay #11 measured harvest 7.91x behind Temporal in the default mode at a
2,000-row backlog, on one 4-core box. It measured 1.5x to 2x behind in the
best mode. Since then, #1796, #1797, #1798 and #1815 have landed. The
claim-path fix (#1971) is in review as PR #2052.

Registered questions:

1. On 0.7.0 as it ships, what is the gap at each depth, in each mode?
2. Does the claim-path fix flatten the default mode across depth?
3. After the fix, how far behind Temporal is the default mode at depth 2,000?

## 🧪 Apparatus

[`apparatus/0014-harvest-vs-temporal-0.7.0/`](apparatus/0014-harvest-vs-temporal-0.7.0/).

| label | commit | what it is |
|:--|:--|:--|
| base | `0aeb887` | 0.7.0, the base of PR #2052 |
| fix | `513b7aa` | the head of PR #2052, with the #1971 claim fix |
| trunk | `9f444b7` | the `trunk-dev` head that this change ships on |
| Temporal | `1.25.2` | `temporalio/auto-setup:1.25.2`, digest `b1edc1e20002`, Go SDK `v1.36.0` |

- **Harvest arms.** Assay #10's workload, ported by value. One binary per
  tree, built from byte-identical source and one shared `Cargo.lock`.
- **Temporal arm.** Assay #11's binary and runner, unchanged.
- **Signals.** A capture recorder keeps the per-event #1815 signals. It
  returns `is_enabled() = false`, so the worker runs no sampler SQL.
- **Host.** 4 logical CPUs (Intel Xeon @ 2.80 GHz), 15 GiB RAM. PostgreSQL
  16.15, `fsync=off`, `synchronous_commit=off`, `max_connections=300`.
  Redis 7.0.15. Both engines use one Postgres server.
- **Sweep.** 2026-10-08, 14:49 to 16:57 UTC. Three rounds. Each round ran
  every cell once. Nothing else ran on the box.

### Changes after the pre-registration, before any run

Review (Codex, PR #2055) found five apparatus defects after `535c943`. Each
fix landed before the first run. None changes a line or the workload.

| commit | change |
|:--|:--|
| `e6def71` | the seeded input converts to `SharedJson`; the harness did not compile |
| `dcd9e5e` | `AtomicU64::load` by path; Diesel's `load` shadowed it |
| `4ff5fa3` | the grader prints every #1815 field, not only the means |
| `92390b3` | the Redis probe reads the hash-tagged keys `{prefix:dispatch:queue}` |
| `3fe7cf0`, `9dbd9f4` | a cell needs exactly one run from each of rounds 0, 1 and 2 |
| `50824a6` | the apparatus lockfile is committed |
| `980bddc` | a run whose elapsed time reaches the cap is truncated |

**The Redis probe defect is inherited.** Assay #10's harness searches
`prefix*` only. `RedisDispatch` names its keys `{prefix:dispatch:queue}`, so
that probe never saw a dispatch key. The residue checks of assays #10 and #11
therefore verified nothing. This assay's probe reads both key families. A
smoke run saw 167 tagged keys mid-run, then a residue of `0/0/0`.

Two smoke runs checked the apparatus after the fixes: a harvest run at depth
300 and a Temporal run at depth 50. They are not cells and no table uses them.

The Temporal image came from `mirror.gcr.io`, because Docker Hub refused the
pull with HTTP 429. The tag and the digest are as above.

## 📐 Assay

Verbatim output is in
[`apparatus/0014-harvest-vs-temporal-0.7.0/results/raw/`](apparatus/0014-harvest-vs-temporal-0.7.0/results/raw/).
`grade.py` produced every table and line below. Its full output is
[`results/graded.md`](apparatus/0014-harvest-vs-temporal-0.7.0/results/graded.md).
All 84 runs are valid: every workflow completed, activity runs equal
`3 × depth`, and every Redis residue is `0/0/0`.

### Cells (completed workflows/sec)

| tree | arm | depth | mean workflows/sec | per rep | valid reps |
|:--|:--|--:|--:|:--|--:|
| `0aeb887` | `postgres` | 250 | 13.86 | 13.20 / 14.10 / 14.28 | 3 |
| `0aeb887` | `redis_pg` | 250 | 15.06 | 15.67 / 14.98 / 14.53 | 3 |
| `513b7aa` | `postgres` | 250 | 21.30 | 21.35 / 22.74 / 19.81 | 3 |
| `513b7aa` | `redis_pg` | 250 | 13.73 | 13.67 / 14.13 / 13.40 | 3 |
| `9f444b7` | `postgres` | 250 | 13.90 | 13.48 / 14.54 / 13.69 | 3 |
| `9f444b7` | `redis_pg` | 250 | 14.27 | 14.98 / 14.36 / 13.46 | 3 |
| `1.25.2` | `temporal_go` | 250 | 24.77 | 22.83 / 25.77 / 25.70 | 3 |
| `0aeb887` | `postgres` | 500 | 13.42 | 12.83 / 14.39 / 13.05 | 3 |
| `0aeb887` | `redis_pg` | 500 | 14.32 | 14.76 / 14.65 / 13.54 | 3 |
| `513b7aa` | `postgres` | 500 | 22.10 | 21.75 / 22.40 / 22.14 | 3 |
| `513b7aa` | `redis_pg` | 500 | 14.05 | 13.78 / 14.57 / 13.81 | 3 |
| `9f444b7` | `postgres` | 500 | 11.40 | 13.83 / 13.78 / 6.59 | 3 |
| `9f444b7` | `redis_pg` | 500 | 14.61 | 14.28 / 14.35 / 15.21 | 3 |
| `1.25.2` | `temporal_go` | 500 | 26.43 | 25.43 / 28.19 / 25.66 | 3 |
| `0aeb887` | `postgres` | 1000 | 9.19 | 8.67 / 10.93 / 7.99 | 3 |
| `0aeb887` | `redis_pg` | 1000 | 13.87 | 13.85 / 14.37 / 13.38 | 3 |
| `513b7aa` | `postgres` | 1000 | 22.14 | 22.36 / 22.51 / 21.54 | 3 |
| `513b7aa` | `redis_pg` | 1000 | 13.64 | 13.63 / 14.28 / 13.01 | 3 |
| `9f444b7` | `postgres` | 1000 | 8.54 | 8.15 / 8.43 / 9.04 | 3 |
| `9f444b7` | `redis_pg` | 1000 | 13.89 | 13.98 / 13.80 / 13.90 | 3 |
| `1.25.2` | `temporal_go` | 1000 | 28.14 | 30.51 / 23.91 / 30.00 | 3 |
| `0aeb887` | `postgres` | 2000 | 4.05 | 4.13 / 4.01 / 3.99 | 3 |
| `0aeb887` | `redis_pg` | 2000 | 14.04 | 14.17 / 14.45 / 13.51 | 3 |
| `513b7aa` | `postgres` | 2000 | 21.14 | 20.34 / 22.85 / 20.25 | 3 |
| `513b7aa` | `redis_pg` | 2000 | 13.84 | 13.63 / 14.37 / 13.53 | 3 |
| `9f444b7` | `postgres` | 2000 | 4.00 | 4.03 / 3.98 / 4.00 | 3 |
| `9f444b7` | `redis_pg` | 2000 | 13.67 | 13.06 / 14.08 / 13.86 | 3 |
| `1.25.2` | `temporal_go` | 2000 | 28.69 | 27.36 / 29.67 / 29.04 | 3 |

### Temporal over harvest, from unrounded means

| depth | default mode, 0.7.0 (`9f444b7`) | best mode, 0.7.0 (`9f444b7`) | default mode, fixed (`513b7aa`) |
|--:|--:|--:|--:|
| 250 | 1.78x | 1.74x | 1.16x |
| 500 | 2.32x | 1.81x | 1.20x |
| 1,000 | 3.29x | 2.03x | 1.27x |
| 2,000 | 7.17x | 2.10x | 1.36x |

### One outlier, kept

`9f444b7` / `postgres` / depth 500, round 2, reads **6.59** against 13.83 and
13.78 in rounds 0 and 1. The run is valid, so the registered rules keep it.
Its load average at start was 3.76, as in its siblings. Its claim p99 was
241 ms, against 17 to 19 ms in its siblings. That is the claim signature of
depth 1,000 and 2,000 on the unfixed trees, here at depth 500. Assay #12 saw
the same bimodal pattern on this arm at depth 1,000. The cause is not tested
here. Without the outlier the cell would read 13.81, and no line reads this
cell.

## 🏁 Verdict

The lines, as `grade.py` printed them:

* **L1** `postgres` on `9f444b7` at 2000: 4.00 against `temporal_go` 28.69: **KILL**. Ranges do not overlap.
* **L2** best mode on `9f444b7` at every depth (250: 14.27 against 24.77; 500: 14.61 against 26.43; 1000: 13.89 against 28.14; 2000: 13.67 against 28.69): **KILL**.
* **L3** `postgres` on `513b7aa`, depth 2000 over depth 250: 21.14 / 21.30 = 0.99 against a 0.80 line: **PASS**. Ranges overlap.
* **L4** `postgres` at 2000, `513b7aa` over `0aeb887`: 21.14 / 4.05 = 5.23x against a 2.0x line: **PASS**. Ranges do not overlap.
* **L5** `temporal_go` over `postgres` on `513b7aa` at 2000: 28.69 / 21.14 = 1.36x against a 2.5x line: **PASS**.

The L3 ranges overlap, as a flat curve predicts. Overlap does not change a
grade.

What the lines say:

1. **0.7.0 as it ships still loses badly in the default mode.** At depth
   2,000 the gap is 7.17x. Assay #11 read 7.91x on another host, so this
   assay cannot say whether #1796, #1797 or #1798 moved it. The best mode is
   1.74x to 2.10x behind.
2. **The #1971 claim fix removes the depth collapse.** On `513b7aa` the
   default mode holds 21.1 to 22.1 workflows/sec at every depth. Its base
   falls from 13.86 to 4.05. At depth 2,000 the fix is 5.23x faster.
3. **After the fix, Temporal still wins at every depth, by 1.16x to 1.36x.**
   Assay #11 predicted "roughly 2x". The measured gap is smaller.
4. **The fix also helps the shallow backlog.** At depth 250, `513b7aa`
   reads 21.30 against 13.86 on its base. Its claim mean is 6.2 ms against
   9.8 ms.
5. **After the fix, the default mode beats the Redis mode.** On `513b7aa`,
   `postgres` reads 21 to 22 and `redis_pg` reads 13.6 to 14.1. The Redis
   channel still pays a Postgres claim by id, at about 9.6 ms.

### Attribution (#1815)

Registered rule: where `postgres` falls by more than 25% from depth 250 to
2,000, name the signal whose mean rises most.

* `0aeb887`: `postgres` falls 13.86 to 4.05. `claim_mean_ms` rises most: 9.76 to 34.92 (3.6x).
* `513b7aa`: `postgres` does not fall by more than 25%. No attribution.
* `9f444b7`: `postgres` falls 13.90 to 4.00. `claim_mean_ms` rises most: 9.79 to 35.33 (3.6x).

The pool is not the bottleneck. Mean connections in use stay at 2 to 4 of
32, and the pool-wait p99 stays near 2 ms. Persist means stay at 4 to 6 ms
at every depth.

**Post hoc, not registered: the claim loop is saturated in every harvest
cell.** Claims per second times the mean claim time gives the share of wall
time spent in a claim. It reads 0.91 to 0.99 over all 72 harvest runs, mean
0.94. That fits one claim in flight at a time. Throughput then follows
`1 / (claims per workflow × claim latency)`, with 7 claims per workflow. The
fix cut the claim latency at depth 2,000 from 35 ms to 6.3 ms. It did not
add claim concurrency, so the claim loop is still the ceiling.

## ⚖️ What this licenses

It licenses this: on one 4-core box, at a 3-activity workflow, Temporal
1.25.2 sustained more throughput than harvest 0.7.0 at every depth tested.
The gap is 1.78x to 7.17x in the default mode and 1.74x to 2.10x in the best
mode. With the #1971 claim fix, the default-mode gap is 1.16x to 1.36x.

It does not license a general speed claim. Assay #11's bounding section
applies in full: one box, one shape, one Temporal version, a Temporal arm at
defaults, and a venue that favours harvest.

**Do not compare these numbers with assay #11's.** This host is a different
CPU. Temporal reads 24.8 to 28.7 here, against 34.6 to 45.8 in assay #11.
Harvest reads lower here too. Only ratios measured on one box compare.

`513b7aa` is a PR head, not a release. Its numbers describe what #1971 buys
when it merges, not what 0.7.0 ships.

## 🔁 Re-charter

1. **Claim concurrency.** The claim loop runs at 0.94 occupancy. A second
   concurrent claimer, or a batched claim, is the next lever. Re-run this
   matrix after it lands.
2. **The depth-500 outlier.** The claim-p99 signature points to a plan flip
   on the unfixed trees. Fresh statistics after the seed are the untested
   candidate, as in assay #13.
3. **The open-benchmark stretch goals of #1972.** Crash-recovery time, a run
   long enough to expose bloat, and payload and step-count sweeps.
4. **A tuned Temporal arm**, configured by someone who operates Temporal.
5. **Fix assay #10's Redis probe** at its source, and note the defect in
   assays #10 and #11.

## Reproduce

See the [apparatus README](apparatus/0014-harvest-vs-temporal-0.7.0/README.md).
In short: build one harness binary per tree, build assay #11's Temporal arm,
start Redis, then run `run.sh` with `ASSAY14_BINS` set. It writes
`results/raw/` and prints the graded Markdown.
