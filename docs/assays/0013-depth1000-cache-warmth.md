# ⛏️ Prospect: does per-rep database recreation drive the depth-1000 postgres variance? (undetermined — the apparatus cannot test it at this depth)

> Status: **measured, corrected across five review rounds — final verdict
> undetermined, and not fixable by a sixth.** The pre-registration lives in
> [`docs/rnd/2026-09-28-depth1000-cache-warmth-preregistration.md`](../rnd/2026-09-28-depth1000-cache-warmth-preregistration.md)
> and was committed (`8766957`) before any warm-condition measurement was
> taken. Nothing in it has been edited since. Every round below is Codex
> review on PR #1761. Two rounds caught defects that changed the verdict:
> round 1 found the original "warm" condition never tested the hypothesis
> ([erratum 1](#erratum-1-the-first-warm-run-did-not-test-the-hypothesis),
> withdrew a KILL); round 3 found the replacement fix was also broken
> ([erratum 2](#erratum-2-plain-vacuum-also-failed-to-preserve-what-it-claimed-to),
> withdrew a PURSUE). Rounds 2 and 4 caught reproducibility gaps (wrong
> defaults, missing validity guards) that never changed a reported number,
> because every run in this report always set the relevant environment
> variables explicitly. **Round 5 found something no further patch can
> fix**: this apparatus's warm/cold toggle, at this backlog depth, was
> never capable of testing genuine cache coldness at all — see
> [erratum 3](#erratum-3-the-fix-for-erratum-2-cannot-cleanly-test-the-original-hypothesis-either).
> Reading only this report's current sections without the errata would
> miss the most important thing it has to say: **the apparatus's own
> history is evidence that this kind of assay is easy to get wrong in a
> way that looks clean, all the way down to its own foundations.**

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
[erratum 3](#erratum-3-the-fix-for-erratum-2-cannot-cleanly-test-the-original-hypothesis-either)
and [verdict](#-verdict) for what this means and what it doesn't.

## ⚠️ Erratum 3: the fix for erratum 2 cannot cleanly test the original hypothesis either

A fourth round of Codex review, on the commit containing both fixes above,
raised a deeper problem with the fix itself, not a new bug in its code.

`ANALYZE`'s block-sampling algorithm targets `300 * default_statistics_target`
sample rows — 30,000 at this server's default. The seeded backlog holds
1,000 rows. Since the target sample size is far larger than the table,
`ANALYZE` does not sample: it reads every page. At this depth, that means
the `ANALYZE` erratum 2 added — right after seeding, immediately before
the timed portion of each repetition — necessarily pulls the entire
just-seeded table into Postgres's shared buffers, **in both conditions**,
a few milliseconds before the clock starts.

That is a real problem for this assay's own question, but on reflection it
is not a new one introduced by `ANALYZE` — it was already true. Seeding
writes the 1,000 rows through the buffer manager regardless of whether
`ANALYZE` runs afterward: a freshly inserted row's page is resident in
shared buffers the moment the `INSERT` commits, in *every* version of this
apparatus, including ledger #12's own original one and this report's two
earlier (withdrawn) versions. **The claim query's own working set — the
backlog rows a claim actually reads — was never capable of being "cold" at
the moment timing starts, in any version of this assay, because seeding it
is what makes it warm.** `ANALYZE`'s full scan only makes the same
already-true fact more obviously true, and only for the exact rows the
seed step just wrote.

This reframes, rather than reopens, ledger #12's own hypothesis. "Cold
buffer-cache/page-cache effect ... specific to whichever repetition runs
first against a freshly recreated database" cannot mean the backlog rows
themselves, in an apparatus shaped like this one, at a depth this small —
those are warmed by construction. What a fresh `CREATE DATABASE` can
still make genuinely cold, and what `DELETE`+`VACUUM (TRUNCATE FALSE)`
still avoids, is the *surrounding* state: `CREATE DATABASE`'s template-copy
I/O, and catalog pages and cache entries for OIDs no prior repetition has
ever touched. Erratum 2's own finding — that fixing planner statistics
alone collapsed most of the variance — says that surrounding-state cost
was never the dominant term at this depth either, at least not on this
host. Building an apparatus that can isolate genuine backlog-row cache
coldness would need to either evict Postgres's shared buffers for those
specific pages between seeding and timing (no such primitive is available
from a normal client connection; the closest is a full server restart,
which is not attempted here) or run at a depth deep enough that the
backlog's own working set exceeds `shared_buffers` (128 MB here, roughly
16,000 8 KB pages — comfortably larger than this assay's ~150-page tables,
so depth 1000 cannot exceed it; a depth on the order of 100,000+ rows
might). Neither is a small change to this apparatus, and neither is
attempted here — per this repo's own re-charter discipline, that is a
separately-chartered next assay, not another patch to this one.

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

**A fourth review round, per
[erratum 3](#erratum-3-the-fix-for-erratum-2-cannot-cleanly-test-the-original-hypothesis-either),
found something more fundamental than a threshold miss: this apparatus,
at this depth, was never capable of testing genuine backlog-row cache
coldness at all**, in any of its four versions. Seeding necessarily warms
the exact rows a claim query reads, through Postgres's ordinary buffer
manager, independent of `DROP DATABASE`/`CREATE DATABASE` versus
`DELETE`+`VACUUM`. `ANALYZE`'s own full-table scan (unavoidable once the
table is smaller than the sampling target) only makes that pre-existing
fact impossible to miss. So this assay's undetermined verdict is not "not
enough evidence yet" in the usual sense — it is "this specific
manipulation cannot move the variable ledger #12's hypothesis names,
because the seed step moves it first, the same way, regardless of which
condition runs." A same-host rerun with a larger n, or a rerun at a
different depth using this same drop/recreate-vs-reuse toggle, would not
fix that; the toggle itself is not wired to backlog-row cache state at
this table size.

**Open, un-chartered pits named here**, in priority order: (1) **whether
genuine backlog-row cache coldness affects claim throughput at all** —
untestable by this apparatus's toggle; would need either a buffer-eviction
primitive between seeding and timing, or a depth large enough (roughly
100,000+ rows, estimated from `shared_buffers = 128 MB` against this
table's own per-1,000-row page count) that the backlog's working set
exceeds the buffer pool regardless of eviction. This is now the
highest-priority pit, and a materially different, larger undertaking than
this assay. (2) **The `ANALYZE`-after-seed hypothesis** — does adding it
alone to the existing (unmodified) drop/recreate apparatus, with no
warm/cold change, collapse ledger #12's original variance the same way it
did here? Cheaper than (1): a one-line change to
`0010-cross-mode-throughput`, no new apparatus, and it would settle
whether *surrounding* database-level state (catalog, template-copy cost —
see erratum 3) rather than backlog-row caching is what ledger #12 actually
measured. (3) Whether the cold-now-faster-than-warm reversal in this run
is real or an n=6 artifact. (4) Whatever produces each condition's
one-outlier-of-six pattern is unexplained by anything measured here. (5)
The original depth-1000 knee question (ledger #12's own third pit)
remains open, and now looks like it needs pit (1) resolved, not just an
`ANALYZE`-corrected apparatus, before any number here or in #12 can be
trusted for it.

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
