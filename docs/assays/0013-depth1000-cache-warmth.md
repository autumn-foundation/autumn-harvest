# ⛏️ Prospect: does per-rep database recreation drive the depth-1000 postgres variance? (kill: 19.7% warm CV vs 15% line)

> Status: **measured.** The pre-registration lives in
> [`docs/rnd/2026-09-28-depth1000-cache-warmth-preregistration.md`](../rnd/2026-09-28-depth1000-cache-warmth-preregistration.md)
> and was committed (`8766957`) before either condition in this report was
> run. Nothing in it has been edited since. The Apparatus, Assay, Verdict
> and Reproduce sections below were appended afterward, with the actual
> numbers.

## 🎯 Question

Ledger #12 killed the depth-knee crossover claim because the `postgres`
arm's three reps at depth 1000 spanned 14.94-24.04 workflows/sec (a 61%
swing), fully containing `redis_pg`'s tight range at the same depth. It
named an untested candidate cause and left it as an open pit: cold
buffer-cache/page-cache effects from the apparatus dropping and recreating
the database before every repetition.

**Falsifiable question:** does running the `postgres` arm at depth 1000
**without** dropping/recreating the database between repetitions (create
once, truncate + reseed between reps — "warm") tighten the
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
recreate, 0010's only behavior) versus a new `truncate_database` (create
once, then `TRUNCATE ... CASCADE` every public-schema table between reps —
see the function doc comments in `src/main.rs` for the exact mechanism and
why it's the natural read of "warm").

**One correction made before the reported runs.** The first warm attempt
ran against this container's Postgres defaults (`fsync = on`,
`synchronous_commit = on`) — not the registered `off`/`off` durability used
throughout the ledger. Caught from the apparatus's own printed durability
line, exactly as ledger #12 caught the same class of mismatch; that run
(mean 16.15, CV 23.5%) is discarded below, not reported as data, and
`postgresql.conf` was corrected (`fsync = off`, `synchronous_commit =
off`, server restarted) before either reported run.

**Stubs / conditions carried over from #10, unchanged:** single worker
process, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll —
not a multi-worker production shape. This is a fresh container with
untuned Postgres defaults (`shared_buffers = 128MB`, default 5-minute
checkpoint timer) rather than a benchmark-tuned host; that is named as a
condition of this run, not corrected for, since ledger #12's own run
predates this session and its host configuration is not on record to
match against.

## 📐 Assay

Raw output:
[`warm-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/warm-depth1000.md),
[`cold-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/cold-depth1000.md).

| condition | rep 0 | rep 1 | rep 2 | rep 3 | rep 4 | rep 5 | mean | stdev | **CV** |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| WARM (no recreate) | 13.15 | 21.08 | 14.63 | 20.61 | 20.91 | 21.08 | 18.58 | 3.66 | **19.7%** |
| COLD (recreate/rep) | 19.44 | 13.44 | 20.09 | 10.44 | 12.32 | 11.14 | 14.48 | 4.23 | **29.2%** |

All twelve reported repetitions (plus the discarded durability-mismatched
one) passed correctness: every seeded workflow `COMPLETED`, activity-run
counts exact (3000 of 3000) in every rep.

**Cold reproduces ledger #12's spread on this host**, at a higher CV
(29.2% here vs. #12's own 3-point spread, which computes to roughly a
similar order of magnitude) — the same-host control behaves as the
pre-registration expected.

**Warm does not tighten it.** 19.7% sits far above the 5% success line and
above the 15% kill line — closer to the cold condition's 29.2% than to
depth 500's 1.3% CV (ledger #12) or `redis_pg`'s own tight spread at depth
1000 (21.24-22.03, ledger #12). Both conditions show the same qualitative
pattern: a mix of ~11-14/sec reps and ~19-21/sec reps, not a smooth
distribution — in the warm run, reps 0 and 2 are slow and reps 1, 3, 4, 5
are fast; in the cold run, reps 1, 3, 4, 5 are slow and reps 0, 2 are fast.
Neither run shows the slow reps clustering at the start (which a pure
"first-touch cache fill" story would predict) or at any other fixed
position.

**No isolated diagnosis of what actually causes the bimodal-looking split
was run** — that is a separate, un-chartered pit. Two candidates are named,
neither measured here: (1) Postgres checkpoint I/O stalls under this
container's untuned defaults (`shared_buffers = 128MB`, default 5-minute
`checkpoint_timeout`) landing mid-rep regardless of database recreation —
plausible given reps run 47-96s and six reps span several minutes, but
`pg_stat_bgwriter`'s checkpoint counters were not reset before this
session and are cumulative since March, so they cannot isolate activity
during these specific runs and are not cited as evidence; (2) contention
from something else on this shared host, equally unmeasured.

## 🏁 Verdict

**KILL, on the pre-registered warm-CV line.** Removing per-repetition
`DROP DATABASE`/`CREATE DATABASE` does not tighten the `postgres` arm's
depth-1000 variance below the 15% kill line (19.7% observed) — the
cold-cache-from-database-recreation hypothesis ledger #12 named is
falsified on this host. The spread ledger #12 found is real (the cold
condition here reproduces it, 29.2% CV) but is not explained by the
apparatus dropping and recreating the database between reps.

This does not reopen ledger #12's own separate finding that `redis_pg`
stays flat at this depth, and it does not identify what *does* cause the
spread — per pre-registration, a kill on the riskiest-assumption check
stops the assay rather than spending the box chasing a second candidate
that was never pre-registered.

**Open, un-chartered pits named here:** (1) whether Postgres checkpoint
activity under untuned defaults explains the bimodal-looking split, in
either condition — would need `pg_stat_bgwriter` reset immediately before
the run and cross-referenced against each repetition's wall-clock window;
(2) whether the same bimodal pattern reproduces on a host with
benchmark-appropriate `shared_buffers`/checkpoint tuning; (3) the
depth-1000 knee question itself (ledger #12's own third pit) remains open
and, per this result, needs a variance-characterized apparatus that
controls for whatever actually causes this spread, not database
recreation, before a depth number is decision-grade for
`docs/operations/redis-dispatch.md`.

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
