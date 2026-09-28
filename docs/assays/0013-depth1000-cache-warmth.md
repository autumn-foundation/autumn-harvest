# ⛏️ Prospect: does per-rep database recreation drive the depth-1000 postgres variance? (undetermined: 8.0% warm CV, cold no longer reproduces ledger #12 at 7.4%)

> Status: **measured, corrected three times — final verdict undetermined.**
> The pre-registration lives in
> [`docs/rnd/2026-09-28-depth1000-cache-warmth-preregistration.md`](../rnd/2026-09-28-depth1000-cache-warmth-preregistration.md)
> and was committed (`8766957`) before any warm-condition measurement was
> taken. Nothing in it has been edited since. This report went through
> three rounds of Codex review, each catching a real defect in the
> apparatus, and each one **changed the actual verdict**: KILL →
> [erratum 1](#erratum-1-the-first-warm-run-did-not-test-the-hypothesis) →
> PURSUE →
> [erratum 2](#erratum-2-plain-vacuum-also-failed-to-preserve-what-it-claimed-to)
> → **UNDETERMINED**. The numbers, Apparatus, Assay, and Verdict sections
> below reflect that final, fully-corrected state. Reading only this
> report's current sections without the errata would miss the most
> important thing it has to say: **the apparatus's own history is evidence
> that this kind of assay is easy to get wrong in a way that looks clean.**
> A fourth review round found two further reproducibility gaps (a default
> arm set that would panic on an unstarted Redis, a missing guard against
> grading a noncanonical seeded input) — both fixed, neither changing a
> reported number, since every run in this report always set the relevant
> environment variables explicitly.

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
drop/recreate). **Undetermined: warm CV in (5%, 15%), or cold CV on this
host fails to reproduce ledger #12's spread at all.** Full text, including
why `n=6` and why `redis_pg` is out of scope here, in the linked
pre-registration. The undetermined branch is what this assay's final,
corrected numbers land in — both clauses of it, in fact.

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
recreate, 0010's only behavior) versus a new `truncate_database`, in its
final form:

- `DELETE` every row from every `public`-schema table (not `TRUNCATE` —
  see [erratum 1](#erratum-1-the-first-warm-run-did-not-test-the-hypothesis)),
  then `VACUUM (TRUNCATE FALSE)` — not plain `VACUUM` — as a separate round
  trip (`VACUUM` cannot run inside a transaction block, and `batch_execute`
  wraps a multi-statement call in one implicitly). See
  [erratum 2](#erratum-2-plain-vacuum-also-failed-to-preserve-what-it-claimed-to)
  for why plain `VACUUM` was also wrong.
- `ANALYZE`, run once after seeding and before timing starts, **in both
  conditions** — see
  [erratum 2](#erratum-2-plain-vacuum-also-failed-to-preserve-what-it-claimed-to).

**Stubs / conditions, unchanged:** single worker process, 8 workflow
slots, 16 activity slots, 32 connections, 25 ms poll — not a multi-worker
production shape. Every repetition, in both conditions, still opens fresh
seed connections and a fresh worker pool — no connection-level cache
(prepared-statement plans, per-backend relcache) survives between reps
either way; a production deployment holding connections open across
requests is not measured here. This is a fresh container with untuned
Postgres defaults (`shared_buffers = 128MB`, default 5-minute checkpoint
timer) rather than a benchmark-tuned host — named as a condition of this
run, not corrected for.

## ⚠️ Erratum 1: the first warm run did not test the hypothesis

The version of this report first pushed to PR #1761 used `TRUNCATE TABLE
... CASCADE` to clear rows between warm repetitions, reasoning that
avoiding `DROP DATABASE`/`CREATE DATABASE` would be enough to preserve
cache residency. **Codex review caught the flaw:** Postgres `TRUNCATE`
allocates a fresh relfilenode per table (that's what makes it fast — it
unlinks the old file rather than scanning and deleting rows), so the
"warm" condition was still faulting in a brand-new, empty, uncached file
every repetition. That version reported **warm CV = 19.7%**, a KILL. It is
withdrawn, not revised — the number never measured what the
pre-registration asked for.

Fixed to `DELETE` + plain `VACUUM`, rebuilt, rerun. That version reported
**warm CV = 4.3%, then 2.8%** after an unrelated reproducibility hardening
pass (see the PR thread for that round — three separate gaps, a wrong
default database name and two missing validity guards, none of which
altered the measured mechanism). Both numbers were reported as a clean
**PURSUE**. Both are also withdrawn — see the next erratum.

## ⚠️ Erratum 2: plain `VACUUM` also failed to preserve what it claimed to

A second round of Codex review, on the same commit, raised two more
findings — both confirmed directly against this apparatus's own database
before accepting or fixing them, not taken on faith:

**Finding A: plain `VACUUM` still truncates the relation.** Postgres's
default `vacuum_truncate` behavior shrinks a table's file when it finds a
sufficiently large all-empty tail — and after a full-table `DELETE`, the
*entire* table is empty. Verified directly: `harvest_task_queue` sat at
159 pages before a `DELETE` + plain `VACUUM`, and at **0 pages** after.
Same for `harvest_events` (323 → 0) and `harvest_workflow_executions`
(48 → 0). The relfilenode number is unchanged, exactly as designed, but
the file behind it was truncated to nothing — the same "brand-new, empty
file" outcome erratum 1's `TRUNCATE` fix was supposed to rule out, reached
by a different mechanism. Confirmed the fix, too, on a scratch table:
`VACUUM (TRUNCATE FALSE)` after the same `DELETE` left the file at its
pre-delete page count, and a reseed reused that same page count rather
than extending the file.

**Finding B: the two conditions handed the planner different statistics
for the same content.** A `VACUUM` (with or without `TRUNCATE FALSE`)
records what it saw — zero live rows, immediately after the `DELETE` — as
the table's `pg_class` row-count estimate. A cold repetition's table, by
contrast, is freshly created and has never been vacuumed or analyzed at
all. Both look "empty" to the planner at the moment of reset, but they
reach that state through different statistics paths, and neither state
matches the 1,000 rows about to be seeded into either one. Without an
explicit `ANALYZE` after seeding, the claim query's planner in each
condition would cost its plan against stale or absent statistics — a
possible source of systematic difference between warm and cold that has
nothing to do with cache warmth. Fixed by adding `ANALYZE` after seeding,
symmetrically, in both conditions, before the timed portion of each
repetition begins.

**Both fixes applied, rebuilt, and both conditions rerun in full — and the
result changed a third time, this time reversing the mean's own
direction.** Warm CV moved from the withdrawn 2.8% to **8.0%**. Cold CV
moved from the withdrawn 34.2% to **7.4%** — an order-of-magnitude drop,
and it means the same-host control **no longer reproduces ledger #12's
spread at all**. The cold mean (20.29) is now *higher* than the warm mean
(18.81), the opposite of every previous version of this report. See
[verdict](#-verdict) for what this means and what it doesn't.

## 📐 Assay

Raw output (final, twice-corrected apparatus):
[`warm-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/warm-depth1000.md),
[`cold-depth1000.md`](apparatus/0013-depth1000-cache-warmth/results/cold-depth1000.md).

| condition | rep 0 | rep 1 | rep 2 | rep 3 | rep 4 | rep 5 | mean | stdev | **CV** |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| WARM (`DELETE`+`VACUUM (TRUNCATE FALSE)`+`ANALYZE`) | 17.17 | 17.13 | 18.68 | 20.64 | 20.36 | 18.88 | 18.81 | 1.50 | **8.0%** |
| COLD (recreate/rep + `ANALYZE`) | 20.91 | 19.92 | 21.19 | 20.39 | 21.83 | 17.53 | 20.29 | 1.50 | **7.4%** |

All twelve reported repetitions passed correctness: every seeded workflow
`COMPLETED`, activity-run counts exact (3000 of 3000) in every rep.

**Neither condition looks anything like the previous (uncorrected) runs, or
like ledger #12.** Both now sit in a similar, moderate CV band (7-8%), and
both show the same rough shape: five repetitions within a tight cluster
and one outlier (warm's rep 0/1 pair reads low; cold's rep 5 reads low).
The two means are close (18.81 vs. 20.29, cold 7.9% *higher*) and their
sample stdevs are identical to two decimal places (1.50), which is either
a coincidence at n=6 or a sign that whatever now drives the residual
spread is similar in both conditions.

## 🏁 Verdict

**UNDETERMINED, on the pre-registered line — both of its clauses fire.**
Warm CV (8.0%) lands inside the pre-registered (5%, 15%) undetermined
band: too tight to call cache-recreation the dominant driver at the kill
threshold, too loose to call it fixed at the pursue threshold. Separately,
and more importantly, cold CV (7.4%) **fails to reproduce ledger #12's
spread on this host at all** — the same-host control this pre-registration
depended on no longer shows the effect it was supposed to control for. Per
the pre-registration's own rule, either clause alone is enough to stop
here rather than force a pursue/kill call the data doesn't support.

**This is not a null result — it is a more interesting and more important
finding than either the withdrawn KILL or the withdrawn PURSUE, and it
points somewhere this assay was never chartered to look.** The variance
ledger #12 found, and that this report's own first two (withdrawn)
versions reproduced, collapsed by roughly 4-5x in *both* conditions the
moment `ANALYZE` was added symmetrically after seeding — a change that has
nothing to do with dropping or recreating the database. That strongly
suggests the real driver of the original variance was stale or absent
planner statistics immediately after a fresh seed, not database
recreation. Every ledger entry that measures a `postgres` arm by seeding a
backlog and immediately timing claims against it — #2, #8, #9, #10, #11,
#12, and this assay's own two earlier (withdrawn) versions — shares that
same gap. **This assay does not re-grade any of those reports, and does
not claim to have confirmed the `ANALYZE` hypothesis** — it was never
pre-registered here, this run cannot separate it from ordinary
run-to-run noise at n=6, and treating a discovery made mid-assay as
already-confirmed is exactly the goalpost-moving this repo's own
discipline rules out.

**Open, un-chartered pits named here**, in priority order: (1) **the
`ANALYZE`-after-seed hypothesis itself** — does adding it to the existing
drop/recreate apparatus, alone, with no warm/cold change at all, collapse
ledger #12's original variance the same way it did here? This is now the
highest-value, cheapest next pit: it needs only a one-line change to
`0010-cross-mode-throughput` and no new apparatus. (2) Whether the
cold-now-faster-than-warm reversal here is real or an n=6 artifact — a
higher-repetition rerun of both conditions, together, would settle it.
(3) Whatever now produces each condition's one-outlier-of-six pattern is
unexplained by anything measured in this report. (4) The original
depth-1000 knee question (ledger #12's own third pit) remains open, and
now looks like it needs an `ANALYZE`-corrected apparatus before any of
this assay's numbers, or #12's, can be trusted for it.

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
