# ⛏️ Prospect: does per-rep database recreation drive the depth-1000 postgres variance? (pursue: 4.3% warm CV vs 5% line)

> Status: **measured, corrected.** The pre-registration lives in
> [`docs/rnd/2026-09-28-depth1000-cache-warmth-preregistration.md`](../rnd/2026-09-28-depth1000-cache-warmth-preregistration.md)
> and was committed (`8766957`) before any warm-condition measurement was
> taken. Nothing in it has been edited since. **This report's own first
> published version reported a KILL, on a flawed apparatus** — see
> [erratum](#erratum-the-first-warm-run-did-not-test-the-hypothesis) below.
> The corrected Apparatus, Assay, Verdict, Cost and Reproduce sections
> reflect the fixed apparatus and its numbers.

## 🎯 Question

Ledger #12 killed the depth-knee crossover claim because the `postgres`
arm's three reps at depth 1000 spanned 14.94-24.04 workflows/sec (a 61%
swing), fully containing `redis_pg`'s tight range at the same depth. It
named an untested candidate cause and left it as an open pit: cold
buffer-cache/page-cache effects from the apparatus dropping and recreating
the database before every repetition.

**Falsifiable question:** does running the `postgres` arm at depth 1000
**without** dropping/recreating the database between repetitions (create
once, clear rows and reseed between reps — "warm") tighten the
repetition-to-repetition coefficient of variation (CV) to ≤5%, while the
unmodified drop/recreate-every-rep condition ("cold"), run on the same
host for a same-host control, reproduces a CV ≥15%?

**Decision this feeds:** whether `docs/operations/redis-dispatch.md`'s
"When to use it" section can eventually cite a concrete backlog-depth
figure, and whether the apparatus's per-rep drop/recreate methodology
(reused across ledger #2, #8-#12) needs to change for `postgres`-arm
measurements at this depth to be trustworthy. **Decider:** issue #1312's
owner (operator guide) and whoever reviews future assay reports against
this apparatus (methodology).

## ⚖️ Pre-registration

Committed before any measurement: n=6 per condition, `postgres` arm only,
depth 1000 fixed, warm run first (riskiest assumption), cold run second as
the same-host control. Success (pursue the cold-cache hypothesis): warm CV
≤5% **and** cold CV ≥15%. Kill: warm CV ≥15% (variance persists with no
drop/recreate). Undetermined: warm CV in (5%, 15%), or cold CV on this host
fails to reproduce ledger #12's spread at all. Full text, including why
`n=6` and why `redis_pg` is out of scope here, in the linked
pre-registration.

## 🔍 Prior art

`docs/assays/0012-cross-mode-depth-knee.md` is the entirety of the prior
art. It measured the spread (n=3, cold only) and named the cold-cache
candidate explicitly as untested — this assay is that test, not a re-dig.

## 🧪 Apparatus

Forked from `docs/assays/apparatus/0010-cross-mode-throughput/` (which
stays unmodified — it remains the artifact of record for ledger
#2/#8-#12) into
[`docs/assays/apparatus/0013-depth1000-cache-warmth/`](apparatus/0013-depth1000-cache-warmth/).
Same workload (`wf_three_activities`), same canonical `{}` input, same
worker-pool constants. The only functional change: an
`ASSAY10_RECREATE_DB` env knob controlling `reset_database` (drop +
recreate, 0010's only behavior) versus a new `truncate_database`.

**`truncate_database`'s mechanism, as fixed:** `DELETE` every row from
every `public`-schema table, then `VACUUM` (a separate round trip —
`VACUUM` cannot run inside a transaction block, and `batch_execute` sends
its whole argument as one simple-query message that Postgres wraps in an
implicit transaction when it holds more than one statement). `DELETE` +
`VACUUM` keeps each table and index on its existing relfilenode, so its
Postgres-buffer and OS-page-cache residency survives into the next
repetition — see [erratum](#erratum-the-first-warm-run-did-not-test-the-hypothesis)
for why this replaced an earlier, broken `TRUNCATE`-based version.

**Stubs / conditions carried over from #10, unchanged:** single worker
process, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll —
not a multi-worker production shape. Every repetition, in both conditions,
still opens fresh seed connections and a fresh worker pool — no
connection-level cache (prepared-statement plans, per-backend relcache)
survives between reps either way. Flagged by the same Codex review that
found the `TRUNCATE` bug; not fixed here, because it's a *common* factor
between the warm and cold conditions, so it cannot be the mechanism that
separates their CVs the way this pre-registration is graded — but a
production deployment holding connections open across requests could run
even tighter than this warm figure suggests, which this assay does not
measure. This is a fresh container with untuned Postgres defaults
(`shared_buffers = 128MB`, default 5-minute checkpoint timer) rather than
a benchmark-tuned host; that is named as a condition of this run, not
corrected for.

## ⚠️ Erratum: the first warm run did not test the hypothesis

The version of this report first pushed to PR #1761 used `TRUNCATE TABLE
... CASCADE` to clear rows between warm repetitions, reasoning that
avoiding `DROP DATABASE`/`CREATE DATABASE` would be enough to preserve
cache residency. **Codex review on PR #1761 caught the flaw:** Postgres
`TRUNCATE` allocates a fresh relfilenode per table (that's what makes it
fast — it unlinks the old file rather than scanning and deleting rows), so
the "warm" condition was still faulting in a brand-new, empty, uncached
file every repetition — mechanically not much different from
`reset_database`'s drop/recreate for the specific effect this assay exists
to isolate. That version reported **warm CV = 19.7%**, a KILL against the
pre-registered line. It is superseded, not merely revised: the underlying
number never measured what the pre-registration asked for, so it is
withdrawn rather than reported as a valid data point.

The apparatus was corrected to `DELETE` + `VACUUM` (same relfilenode,
mechanism above), rebuilt, and rerun in full (all six warm repetitions,
fresh database). The cold condition needed no change — it never called
the broken function — so its six repetitions stand as originally measured
and are reused as this assay's control.

## 📐 Assay

Raw output:
[`warm-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/warm-depth1000.md)
(corrected `DELETE`+`VACUUM` mechanism),
[`cold-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/cold-depth1000.md)
(unaffected by the fix).

| condition | rep 0 | rep 1 | rep 2 | rep 3 | rep 4 | rep 5 | mean | stdev | **CV** |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| WARM (`DELETE`+`VACUUM`) | 21.16 | 20.42 | 19.79 | 19.21 | 19.10 | 19.04 | 19.79 | 0.86 | **4.3%** |
| COLD (recreate/rep) | 19.44 | 13.44 | 20.09 | 10.44 | 12.32 | 11.14 | 14.48 | 4.23 | **29.2%** |

All twelve reported repetitions passed correctness: every seeded workflow
`COMPLETED`, activity-run counts exact (3000 of 3000) in every rep.

**Cold reproduces ledger #12's spread on this host** (29.2% CV) — the
same-host control behaves as the pre-registration expected.

**Warm clears the success line decisively.** 4.3% sits under the 5% line,
and the six warm reps form a smooth, monotonically-settling sequence
(21.16 → 20.42 → 19.79 → 19.21 → 19.10 → 19.04) rather than cold's jumpy,
bimodal-looking spread (19.44, 13.44, 20.09, 10.44, 12.32, 11.14, no
positional pattern). The warm mean (19.79) is also **36.7% higher** than
the cold mean (14.48) at the identical depth, same host, same tree, same
durability settings — cold isn't just noisier, it is measuring a slower
steady state on average, not only a wider one.

## 🏁 Verdict

**PURSUE, on the pre-registered line.** Warm CV (4.3%) clears the ≤5%
success line; cold CV (29.2%) clears the ≥15% same-host-control line. The
cold-cache-from-database-recreation hypothesis ledger #12 named is
**confirmed**: dropping and recreating the database before every
repetition is what drives the depth-1000 `postgres`-arm variance, and it
also depresses the mean relative to a warmed steady state.

This has a real implication beyond this one depth: ledger #2, #8, #9,
#10, #11 and #12 all measured the `postgres` arm using the same
drop/recreate-per-repetition pattern this assay just showed costs ~37% of
throughput and inflates variance ~7x at depth 1000. **This assay does not
re-grade any of those reports** — each used a different depth, workload,
or shape, and re-litigating a closed verdict on an untested inference
would be exactly the goalpost-moving this repo's own assay discipline
rules out. What follows is narrower: their `postgres`-arm absolute numbers
at depth ≥1000 should be read as cold-start figures, not steady-state
ones, until someone re-runs the specific comparison each of those reports
made under the warm pattern.

**Open, un-chartered pits named here:** (1) whether the ~37%
mean-throughput gap and the CV gap hold at other depths (500, 2000, and
beyond) and other workload shapes — this assay tested depth 1000 only;
(2) whether the same warm pattern, applied to `redis_pg`'s own depth-1000
reps (already tight under the cold pattern per ledger #12), changes
anything there; (3) whether a production deployment's persistent
connections (unlike this apparatus's fresh pool per repetition, named
above as a common, unfixed factor) would tighten the warm CV further or
raise its mean; (4) re-running any of ledger #2/#8-#12's specific
comparisons under the warm pattern, per report, is each its own
un-chartered re-charter, not implied or performed here.

## 💰 Cost to productionize

This is a benchmarking-apparatus methodology fix, not an engine change —
**zero engine impact**, no migration, no public API change.

- Add the `DELETE`+`VACUUM`-based warm-reset option demonstrated in
  `0013-depth1000-cache-warmth/src/main.rs`'s `truncate_database` to
  `0010-cross-mode-throughput` (and any sibling apparatus reusing its
  `reset_database` pattern) as an available mode for `postgres`-arm
  measurements at depth ≥~1000, alongside the existing drop/recreate mode
  — a small, mechanical, single-file change, already prototyped here.
- Flag, in `docs/benchmarks.md` and any report citing a `postgres`-arm
  absolute number at depth ≥1000 from the drop/recreate pattern (ledger
  #2, #8, #9, #10, #11, #12), that the figure is a cold-start
  measurement, not a steady-state one, pending a warm-pattern rerun of
  that specific comparison.
- No build-agent gate applies: this doesn't touch `autumn-harvest`,
  `autumn-harvest-redis`, or `autumn-harvest-sqlite` — only throwaway,
  non-workspace apparatus code under `docs/assays/apparatus/`.

## 🔬 Reproduce

```bash
# Requires local Postgres, fsync=off, synchronous_commit=off.
cd docs/assays/apparatus/0013-depth1000-cache-warmth
cargo build --release

export ASSAY10_DATABASE_URL="postgres://postgres:<pw>@127.0.0.1:5432/assay13"
export ASSAY10_ADMIN_URL="postgres://postgres:<pw>@127.0.0.1:5432/postgres"
export ASSAY10_DB_NAME="assay13"
export ASSAY10_ARMS="postgres"
export ASSAY10_WORKFLOWS=1000
export ASSAY10_REPS=6

ASSAY10_RECREATE_DB=0 ./target/release/depth1000_cache_warmth_assay   # warm
ASSAY10_RECREATE_DB=1 ./target/release/depth1000_cache_warmth_assay   # cold
```
