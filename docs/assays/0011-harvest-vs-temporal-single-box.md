# ⛏️ Prospect: does harvest match Temporal's throughput on one box, at one shape? (kill: 5.47 against 43.30 workflows/sec, ledger #11)

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
| `temporal_go` | **43.30** | 48.95 / 39.28 / 41.66 | 3 | PASS |
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

The first reported pair was 44.31 against 5.58, a ratio of 7.9x. The corrected
pair is 43.30 against 5.47, a ratio of **7.92x**. The corrections moved the
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

**KILL on L1, decisively and against harvest, by 7.92x.** `harvest_pg`
sustained 5.47 workflows/sec against `temporal_go`'s 43.30 at the registered
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

**These cells were taken before the payload corrections above**, so harvest ran
at the canonical empty object and Temporal persisted no activity input. Both
corrections slow both arms slightly, and at the registered cell they moved the
ratio from 7.9x to 7.92x, so the shape of the curve is unaffected. The cells
are left as measured rather than re-run, and are read for their shape rather
than their exact values.

| backlog depth | harvest `postgres` | harvest `redis_pg` | `temporal_go` | Temporal / best harvest |
|--:|--:|--:|--:|--:|
| 250 | 23.65 | 22.41 | 28.85 | 1.22x |
| 500 | 23.90 | 22.21 | 41.10 | 1.72x |
| 1,000 | 14.54 | 22.53 | 38.54 | 1.71x |
| 2,000 | 5.63 | 22.18 | 39.02 | 1.76x |

Three things this separates, none of which the registered line could:

1. **Temporal is faster at every depth measured, including the shallowest.**
   There is no depth in this range where harvest wins. The registered kill is
   not an artifact of the depth chosen.
2. **Against harvest's *best* mode the margin is stable and roughly 1.7x**, not
   7.9x. The 7.9x figure is the margin against harvest's *default* mode at the
   depth where its claim path is worst.
3. **The widening margin against plain Postgres is one known, fixable defect**,
   not a general architectural gap. Harvest's Postgres arm falls 4.2x from 500
   to 2,000 rows of depth while Temporal stays flat, which is the
   `#786`/`#1177` claim-path behaviour `docs/performance.md` already documents:
   a non-indexable `ORDER BY` forcing a scan-and-sort on every claim. The Redis
   dispatch channel routes around it, which is why that arm is flat.

**A caveat that cuts against harvest, recorded because it would be convenient
to omit.** The measured window opens before the worker starts, on both arms,
so worker startup is charged inside every repetition. Temporal's startup is a
gRPC client, a sticky cache and a poller fleet; harvest's is in-process. At
depth 250 the whole Temporal drain is 8.67 s, so startup is a large fraction
of it, and the 28.85 cell is therefore an **understatement**. The shallow-depth
cells flatter harvest, and the true 1.22x is narrower still.

### What this does and does not license

It licenses: *on one 4-core box, at a 3-activity workflow, Temporal sustained
higher throughput than harvest at every backlog depth tested, by roughly 1.7x
against harvest's best-configured mode and up to 7.9x against its default.*

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
   5.63 to something near its Redis arm's flat 22, which would take the margin
   from 6.9x to roughly 1.8x at depth 2,000. That is the single highest-value
   performance fix this assay found.
3. **A tuned Temporal arm**, configured by someone who operates Temporal, so
   the competitor number stops being a floor.

## Reproduce

```bash
cd docs/assays/apparatus/0011-harvest-vs-temporal
go build -o assay11 . && ./run.sh
```

`run.sh` starts and removes the Temporal container itself and resets its
persistence before every repetition. Run the harvest arm separately, never
concurrently, per
[`apparatus/0010-cross-mode-throughput/README.md`](apparatus/0010-cross-mode-throughput/README.md).
