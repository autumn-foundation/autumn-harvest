# ⛏️ Prospect: does harvest match Temporal's throughput on one box, at one shape? (kill: 5.58 against 44.31 workflows/sec, ledger #11)

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
The harvest arm is assay #10's `postgres` arm, run unchanged, so the harvest
number here and there is one measurement rather than two that might drift.
The Temporal arm is Go, on `go.temporal.io/sdk` v1.36.0 and Go 1.24.7 as
registered, against `temporalio/auto-setup:1.25.2`.

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

| arm | mean workflows/sec | per rep | valid reps | correctness |
|:--|--:|:--|--:|:--|
| `temporal_go` | **44.31** | 42.01 / 45.99 / 44.92 | 3 | PASS |
| `harvest_pg` | **5.58** | 5.60 / 5.57 / 5.58 | 3 | PASS |

Every Temporal repetition completed all 2,000 executions with exactly 6,000
activity runs, **zero workflow task failures** and **zero unread histories**,
so the registered correctness precondition held in all three.

## 🏁 Verdict

**KILL on L1, decisively and against harvest, by 7.9x.** `harvest_pg`
sustained 5.58 workflows/sec against `temporal_go`'s 44.31 at the registered
shape. The pre-registration called this outcome a genuine and publishable
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
