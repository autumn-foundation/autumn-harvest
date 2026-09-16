# ⛏️ Prospect: does harvest match Temporal's throughput on one box, at one shape? (kill: 5.47 against 43.29 workflows/sec, ledger #11)

> Status: **measured.** The Pre-registration lives in
> [`docs/rnd/2026-09-16-harvest-vs-temporal-single-box-preregistration.md`](../rnd/2026-09-16-harvest-vs-temporal-single-box-preregistration.md)
> and was committed (`5624f13`) before the apparatus was built or run. Nothing
> in it has been edited since, including the section fixing what the result may
> not be read to mean. The Apparatus, Assay, Verdict and Reproduce sections
> below were appended afterward, with the actual numbers.

## 🎯 Question

`docs/benchmarks.md` states the gap this assay attacks in its own words: the
suite has never run another engine's benchmark, and a comparison worth
trusting re-runs both engines on the same hardware.

Falsifiable question, as registered: on one 4-core box, against the same
PostgreSQL server, at the same 3-activity shape and the same closed-loop
drain, does harvest's Postgres mode sustain at least Temporal's completed
workflows/sec?

## 🔬 Apparatus

[`apparatus/0011-harvest-vs-temporal/`](apparatus/0011-harvest-vs-temporal/).
The harvest arm is assay #10's `postgres` arm, run from the same binary but
**at this assay's own registered payload**, which #10's L1 cannot use. An
earlier revision reused #10's run unchanged and so measured the wrong
workload; see the note in the Assay section. The Temporal arm is Go, on
`go.temporal.io/sdk` v1.36.0 and Go 1.24.7 as registered, against
`temporalio/auto-setup:1.25.2`.

Both engines used **the same PostgreSQL 16.13 server**, in separate databases,
so neither arm got a storage engine the other did not. The arms never ran
concurrently. Every Temporal repetition began on freshly dropped and rebuilt
`temporal` and `temporal_visibility` databases, matching the harvest arm's
per-repetition `reset_database`.

Review (Codex, PR #1617) found four parity defects in the Temporal arm, all
of which **disadvantaged Temporal**, and all of which were fixed before the
reported run: the SDK's default logger writing a line per activity dispatch;
activities returning their input, which Temporal persists into every
completion event; the workflow input forwarded into all three activity
commands; and no per-repetition database reset, which left later repetitions
running against earlier ones' histories.

## 📐 Assay

Verbatim output in
[`apparatus/0011-harvest-vs-temporal/results/`](apparatus/0011-harvest-vs-temporal/results/).

Both arms at the **registered ~40-byte workflow payload**. See the note below
on why these numbers replace an earlier pair.

| arm | mean workflows/sec | per rep | valid reps | correctness |
|:--|--:|:--|--:|:--|
| `temporal_go` | **43.29** | 48.95 / 39.28 / 41.66 | 3 | PASS |
| `harvest_pg` | **5.47** | 5.64 / 5.38 / 5.40 | 3 | PASS |

Every Temporal repetition completed all 2,000 executions with exactly 6,000
activity runs, **zero workflow task failures** and **zero unread histories**,
so the registered correctness precondition held in all three.

**Temporal's spread is far wider than harvest's**: 39.28 to 48.95, about 25%,
against harvest's 5.38 to 5.64, about 5%. Three repetitions cannot
characterise that, and this assay does not try to. It is reported rather than
smoothed into the mean, because a reader deciding on these numbers should see
it.

### These numbers replace an earlier pair, for two reasons

A seventh review round (Codex, PR #1617) found that **this assay had never run
its own registered workload**. Its Shape table registers a ~40-byte payload for
both arms. Both had been changed to the canonical empty object so that assay
#10's L1 could compare against a published figure taken that way, the deviation
was disclosed, and this assay then reused #10's harvest arm unchanged.
Disclosing a deviation is not the same as grading registered lines on the
registered shape. The apparatus now takes `ASSAY10_INPUT_JSON`, so #10 keeps
the empty object its L1 needs and #11 runs both arms at its own registered
payload. The same round found the Temporal arm persisting **no** activity input
payload where harvest persists an explicit JSON null — 6,000 smaller history
records per repetition, in Temporal's favour.

The first reported pair was 44.31 against 5.58. The corrected pair is 43.29
against 5.47, a ratio of **7.91x** computed from the unrounded means
(43.2943 / 5.47184 = 7.9122). An earlier revision printed 7.92x by dividing
the rounded display values, which is double-rounding; the error was caught in
review and is noted here rather than silently fixed. The corrections moved the
headline by nothing measurable. Both facts belong in the record: the first
numbers came from an apparatus with known defects, *and* they happened to be
right. The first is why the re-run was necessary; the second is not a
justification for having skipped it.

**A fourth harvest run was discarded before the one above.** Its repetitions
read 5.39, 5.37 and 15.02 workflows/sec, and that single outlier dragged the
mean to 8.60, which *passes* assay #10's L1 validity band that the same arm
otherwise kills. The cause was this session running `git merge` and `git push`
on the box mid-measurement, breaking the idleness precondition
`docs/benchmarks.md` insists on. It is recorded because it is the most
dangerous failure mode encountered in this work: a contaminated run that
flipped a validity verdict from kill to pass, visible only in the per-repetition
numbers and invisible in the mean.

## 🏁 Verdict

**KILL on L1, decisively and against harvest, by 7.91x.** `harvest_pg`
sustained 5.47 workflows/sec against `temporal_go`'s 43.29 at the registered
shape and the registered payload. The pre-registration called this outcome a genuine and publishable
negative result, and it is reported as one.

**L2 passes.** Both arms drained inside the 900 s cap in every repetition, so
L1 is resolved on rates rather than on a truncation.

**The venue makes this worse for harvest, not better.** The pre-registration
fixed in advance that four cores is near the bottom of Temporal's envelope,
with its frontend, history, matching and worker services co-resident on cores
they share with their own Postgres and the load generator, and near the top of
harvest's, which is architected for exactly this footprint. It also fixed what
a Temporal win would then mean:

> A Temporal win here, by contrast, *would* be a strong result for Temporal,
> because it would come despite the venue.

That is the result. Harvest lost on its own home ground, to an engine running
in the configuration that suits it least.

### 🔭 Post-hoc diagnostic: how much of this is one known defect?

Not pre-registered. One repetition per cell. It exists because the registered
cell sits at a backlog depth where assay #10 independently found the harvest
Postgres arm collapsing, and reporting a single ratio from that depth alone
would attribute a specific, documented defect to the engine as a whole.

Every cell below was **re-measured after the payload corrections**, at this
assay's registered payload on both arms. An earlier revision kept the
pre-correction cells and argued from the one re-run cell that the curve was
unaffected. Review caught that as an overreach, and it was: the corrections
are not symmetric, since one of them adds persisted activity payloads to
Temporal only, and the shallowest cell had the narrowest margin of all of
them. The cell least able to detect a problem was the one being used to rule
one out.

| backlog depth | harvest `postgres` | harvest `redis_pg` | `temporal_go` | Temporal / best harvest |
|--:|--:|--:|--:|--:|
| 250 | 23.64 | 22.60 | 36.17 | 1.53x |
| 500 | 23.90 | 21.82 | 34.56 | 1.45x |
| 1,000 | 13.93 | 22.58 | 45.80 | 2.03x |
| 2,000 | 5.60 | 21.84 | 43.29 † | 1.98x |

† **One cell aggregates differently from the rest.** Every other Temporal cell
is a single repetition; the 2,000 cell is the registered sweep's three-repetition
mean (48.95 / 39.28 / 41.66), because that sweep already measured this depth
properly and discarding it for consistency would be discarding the better
number. Against a ~25% spread that matters, so the 1.98x cell is not directly
comparable to the rows above it. Its single-repetition span would be 2.24x to
1.80x taken from the same three repetitions.

The re-measured 250-row Temporal cell reads **36.17**, against **28.85** in
the pre-correction diagnostic, so the shallowest margin in the table above is
1.53x where the old one was 1.22x.

**That difference cannot be attributed to the correction.** Both are single
repetitions, and the gap between them is about 25%, which is the same size as
this arm's own measured repetition spread over the registered sweep. An
earlier revision of this report read the increase as evidence that the
pre-correction cells had been understating Temporal, and that inference does
not hold: run-to-run variation alone is sufficient to produce it. Separating
the two would need the before and after configurations repeated enough times
to tell them apart, which this diagnostic does not do. What can be said is
narrower and is all that is said here: **the re-measured cell is 36.17, and
Temporal still wins at every depth in the table.**

**These are single repetitions against a Temporal arm whose spread over three
repetitions was about 25%.** The ratios are therefore coarse, and the 1.45x
and 1.53x cells are not meaningfully different from each other. What the
column supports is a range, not a trend: roughly **1.5x to 2x** against
harvest's best-configured mode across this depth range.

Three things this separates, none of which the registered line could:

1. **Temporal is faster at every depth measured, including the shallowest.**
   There is no depth in this range where harvest wins. The registered kill is
   not an artifact of the depth chosen.
2. **Against harvest's *best* mode the margin runs roughly 1.5x to 2x**, not
   7.91x. The 7.91x figure is the margin against harvest's *default* mode at
   the depth where its claim path is worst.
3. **The widening margin against plain Postgres is one known, fixable defect**,
   not a general architectural gap. Harvest's Postgres arm falls 4.2x from 500
   to 2,000 rows of depth while Temporal shows no comparable collapse, which
   is the
   `#786`/`#1177` claim-path behaviour `docs/performance.md` already documents:
   a non-indexable `ORDER BY` forcing a scan-and-sort on every claim. The Redis
   dispatch channel routes around it, which is why that arm is flat.

**A caveat that cuts against harvest, recorded because it would be convenient
to omit.** The measured window opens before the worker starts, on both arms,
so worker startup is charged inside every repetition. Temporal's startup is a
gRPC client, a sticky cache and a poller fleet; harvest's is in-process. At
depth 250 the whole Temporal drain is 6.91 s, so startup is a large fraction
of it, and the 36.17 cell is therefore an **understatement**. The shallow-depth
cells flatter harvest, and the true 1.53x is wider still.

### What this does and does not license

It licenses: *on one 4-core box, at a 3-activity workflow, Temporal sustained
higher throughput than harvest at every backlog depth tested, by roughly 1.5x
to 2x against harvest's best-configured mode and up to 7.91x against its
default.*

It does not license a general claim that Temporal is faster than harvest. One
shape, one box, one version, one workload, no fan-out, no child workflows, no
timers, no signals, no large histories, and a Temporal configuration at
documented defaults rather than tuned by someone who operates it. A Temporal
expert would likely produce a better Temporal number, which would widen the
gap rather than close it.

It also says nothing about the reasons `docs/comparison.md` gives for choosing
harvest, none of which are throughput: one Postgres dependency instead of an
orchestrator cluster, embedding in a Rust web application, and determinism
tooling. A throughput cell is not an evaluation. What it does do is remove
"harvest is at least as fast on a single box" from the set of things anyone
may assume without evidence.

### 🔁 Re-charter

1. **The same comparison at a valid closed-loop shape**, alongside assay #10's
   first re-charter. The drain shape is the one #941 rejected.
2. **The comparison after the `#1177` claim-path defect is fixed.** This assay
   bounds what fixing it would buy: harvest's default mode would move from
   5.60 to something near its Redis arm's flat 22, which would take the margin
   at depth 2,000 from 7.7x to roughly 2x. That is the single highest-value
   performance fix this assay found.
3. **A tuned Temporal arm**, configured by someone who operates Temporal, so
   the competitor number stops being a floor.

## Reproduce

The Temporal arm:

```bash
cd docs/assays/apparatus/0011-harvest-vs-temporal
go build -o assay11 . && ./run.sh
```

`run.sh` starts and removes the Temporal container itself and resets its
persistence before every repetition.

The harvest arm, **separately and never concurrently**. It needs
`ASSAY10_INPUT_JSON` set to this assay's registered payload, because the
apparatus defaults to the canonical empty object that assay #10's L1 requires.
Without it the run reproduces #10's cell, not the 5.47 above:

```bash
ASSAY10_ARMS=postgres \
ASSAY10_WORKFLOWS=2000 \
ASSAY10_REPS=3 \
ASSAY10_INPUT_JSON='{"p":"0123456789abcdef0123456789abcdef"}' \
  cargo run --release --manifest-path \
  docs/assays/apparatus/0010-cross-mode-throughput/Cargo.toml
```
