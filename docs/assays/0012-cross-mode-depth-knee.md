# ⛏️ Prospect: does the postgres/redis_pg throughput crossover replicate inside (500, 1000]? (kill: postgres range [14.94, 24.04] overlaps redis_pg range [21.24, 22.03] at depth 1000)

> Status: **measured.** The pre-registration lives in
> [`docs/rnd/2026-09-21-cross-mode-depth-knee-preregistration.md`](../rnd/2026-09-21-cross-mode-depth-knee-preregistration.md)
> and was committed (`3b56e91`) before either depth in this report was run
> at n=3. Nothing in it has been edited since. The Apparatus, Assay, Verdict
> and Reproduce sections below were appended afterward, with the actual
> numbers.

## 🎯 Question

Ledger #10 closed by naming three open re-charters. This assay is the
third: *"the position of the depth curve's knee."* Its own post-hoc,
single-repetition depth diagnostic found `postgres` ahead of `redis_pg` at
depths 250 and 500, and behind it at depths 1000 and 2000 — suggesting a
crossover somewhere in `(500, 1000]`.

`docs/operations/redis-dispatch.md`'s "When to use it" section has no
number: "Turn it on when the queue is deep... Leave it off when the
backlog is shallow." **Falsifiable question:** under proper repetition
(n=3, not n=1), does the `postgres` arm's mean completed-workflows/sec
first fall below the `redis_pg` arm's mean at a depth inside `(500, 1000]`,
with non-overlapping rep ranges — confirming the single-rep diagnostic's
suggested location — or does replication fail to reproduce that ordering?

**Decision this feeds:** whether `docs/operations/redis-dispatch.md`'s
"When to use it" section can cite a concrete backlog-depth figure instead
of "deep"/"shallow". **Decider:** whoever owns that operator guide (issue
#1312's owner).

## 🔍 Prior art

`docs/assays/0010-cross-mode-throughput.md` and its
`results/depth-diagnostic.md`: one repetition per depth, explicitly
post-hoc and not pre-registered. Re-running the same two bracketing depths
with repetition is not re-digging a closed pit — ledger #10 named this
exact gap as open, and n=1 cannot by itself support the operator guide
citing a number.

## 🧪 Apparatus

Reused, unmodified: `docs/assays/apparatus/0010-cross-mode-throughput/`.
Same workload (`wf_three_activities`, ported by value and already verified
field-for-field against `e2e_bench_support.rs`), same canonical `{}` input,
same worker-pool constants. Only `ASSAY10_ARMS` (narrowed to
`postgres,redis_pg`) and `ASSAY10_WORKFLOWS` (depth) vary between runs.

**Stubs / conditions carried over from #10, unchanged:** single worker
process, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll —
not a multi-worker production shape. `redis_pg` still measures dispatch
only; Postgres remains the source of truth on both arms.

**One correction made before running:** the freshly `service postgresql
start`ed server in this session defaulted to `fsync = on`, `synchronous_commit
= on` — not ledger #10's registered `off`/`off`. This was caught from the
apparatus's own printed durability line before treating any number as
comparable, and `postgresql.conf` was edited to match (`fsync = off`,
`synchronous_commit = off`) and the server restarted, before the reported
runs below. The mismatched first attempt at depth 500 was discarded
(not reported) rather than reused.

## 📐 Assay

Raw output:
[`depth-500.md`](apparatus/0010-cross-mode-throughput/results/depth-knee-0012/depth-500.md),
[`depth-1000.md`](apparatus/0010-cross-mode-throughput/results/depth-knee-0012/depth-1000.md).

Per the pre-registration, depths 500 and 1000 run first as the
riskiest-assumption check; depth 750 runs only if both replicate the
single-rep ordering with non-overlapping ranges.

| depth | arm | rep 0 | rep 1 | rep 2 | mean | range |
|--:|:--|--:|--:|--:|--:|:--|
| 500 | `postgres` | 23.86 | 23.85 | 23.55 | **23.76** | [23.55, 23.86] |
| 500 | `redis_pg` | 22.10 | 22.29 | 20.06 | **21.48** | [20.06, 22.29] |
| 1000 | `postgres` | 14.94 | 24.04 | 23.80 | **20.93** | [14.94, 24.04] |
| 1000 | `redis_pg` | 21.24 | 21.87 | 22.03 | **21.71** | [21.24, 22.03] |

All twelve repetitions across both depths passed correctness (every
seeded workflow `COMPLETED`, activity-run counts exact, and every
`redis_pg` repetition left an empty stream, PEL and marker set).

**Depth 500 replicates the diagnostic's ordering with no overlap**:
postgres's range floor (23.55) sits above redis_pg's range ceiling (22.29).

**Depth 1000 does not.** The single-rep diagnostic's own rep 0 pattern
reproduces almost exactly here (`postgres` rep 0: 14.94 vs. the
diagnostic's 14.54; `redis_pg` rep 0: 21.24 vs. 22.53) — but `postgres`
reps 1 and 2 (24.04, 23.80) land back at depth-500 levels, not a degraded
one. The three `postgres` reps at depth 1000 span 14.94-24.04, a 61%
swing between the slowest and fastest repetition of the *same arm, same
depth, same registered conditions, run back to back* — far wider than
this same arm's own spread at depth 500 (23.55-23.86, 1.3%) or ledger
#10's registered depth-2000 sweep (21.70-22.35 for `redis_pg`; that
report doesn't carry multiple `postgres` reps at 2000 to compare against).
That spread swallows the ~1 workflows/sec gap the crossover claim depends
on: `postgres`'s range (`[14.94, 24.04]`) fully contains `redis_pg`'s
(`[21.24, 22.03]`) at this depth.

**No isolated diagnosis of the variance was run** — that would be a
separate, un-chartered pit. The apparatus drops and recreates the Postgres
database before every repetition; one candidate explanation is a cold
buffer-cache/page-cache effect specific to whichever repetition runs first
against a freshly recreated database at this depth, since neither depth
500 nor `redis_pg` at either depth shows anything like this spread — but
that is a plausible reading of the numbers, not a measured claim, and is
recorded as such rather than asserted.

## 🏁 Verdict

**KILL, on the pre-registered crossover-replication line — and the kill is
about the diagnostic's reliability, not about where a knee sits.**

Per the pre-registration: *"if reps overlap at either depth... the
crossover claim itself is unsettled and the assay stops there rather than
spending time narrowing a band that may not exist."* Depth 1000's ranges
overlap. Depth 750 was not run, per that committed stop rule — narrowing a
band whose existence this same data just put in doubt would not be
cheaper evidence, it would be more of the same apparatus applied to a
question this result no longer supports asking that way.

This does **not** show `redis_pg` failing to help at depth, and does not
reopen ledger #10's own separate, tightly-clustered finding (`redis_pg`
flat within 1.4% from 250 to 2,000 in that sweep). It shows that
`postgres`'s own repetition-to-repetition variance at depth 1000, under
this apparatus's single-worker shape, is large enough that a lone
repetition — exactly what the depth-diagnostic that suggested this
crossover was — cannot be trusted to say which arm is ahead at that depth
on any given run. `docs/operations/redis-dispatch.md` should **not** cite
a specific backlog-depth figure on the strength of that diagnostic; "deep"
stays undefined, honestly, rather than being given a false-precision
number this assay just failed to replicate.

**Open, un-chartered pits named here:** (1) what causes the ~60% spread in
`postgres`'s own repeated measurements at depth 1000 specifically — cold
cache after a fresh database recreation is one candidate, untested; (2) a
higher-repetition sweep (n≥6) at depth 1000 to characterize whether that
spread is bimodal (as these three reps suggest) or continuous; (3) the
knee question itself remains open and would need a variance-characterized
apparatus before a depth number is decision-grade.

## 🔬 Reproduce

```bash
# Requires local Postgres (fsync=off, synchronous_commit=off) and Redis.
cd docs/assays/apparatus/0010-cross-mode-throughput
cargo build --release

redis-server --daemonize yes --port 6379 --save "" --appendonly no

ASSAY10_DATABASE_URL="postgres://postgres:<pw>@127.0.0.1:5432/assay10" \
ASSAY10_ADMIN_URL="postgres://postgres:<pw>@127.0.0.1:5432/postgres" \
ASSAY10_ARMS="postgres,redis_pg" \
ASSAY10_WORKFLOWS=500 \
ASSAY10_REPS=3 \
./target/release/cross_mode_throughput_assay

ASSAY10_WORKFLOWS=1000 \
ASSAY10_REPS=3 \
./target/release/cross_mode_throughput_assay
```
