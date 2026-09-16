# Pre-registration: harvest vs Temporal, one box, one workflow shape

> Committed **before** the apparatus was built or run. Assay ledger #11.
> Nothing in this file is edited after the first measurement; the report at
> `docs/assays/0011-harvest-vs-temporal-single-box.md` appends the apparatus,
> the numbers and the verdict.

Source tree: `3d6ad88`. Registered 2026-09-16.

## 🎯 Question

`docs/benchmarks.md` states the gap this assay attacks, in its own words:

> This suite has not run another engine's benchmark, and a competitor's own
> published figure would carry accuracy and staleness this project cannot
> vouch for. A comparison worth trusting re-runs both engines on the same
> hardware, which is why this suite ships in the repo.

`docs/comparison.md` is entirely qualitative by design and says so ("No
first-party benchmarks yet… throughput/latency claims here are deliberately
absent"). Temporal is the engine harvest's README, its
`docs/migrating-from-temporal.md` guide and its comparison page all treat as
the reference point.

Falsifiable question: **on one 4-core box, against the same PostgreSQL
server, at the same 3-activity workflow shape and the same closed-loop drain,
does harvest's Postgres mode sustain at least Temporal's completed
workflows/sec?**

**Decision this feeds:** whether `docs/comparison.md` can replace "no
first-party benchmarks yet" with a measured, hard-bounded single-box number,
or must keep declining to make any performance claim. **Decider:** whoever
owns `docs/comparison.md`.

## ⚖️ What this question is *not*, fixed before any number exists

This is the part most likely to be misread later, so it is registered rather
than added as a caveat afterwards.

**The box is near the bottom of Temporal's envelope and near the top of
harvest's.** Temporal is architected as four scale-out services (frontend,
history, matching, worker) that expect dedicated hosts; here all four run
co-resident, in one container, on four cores they share with their own
Postgres, harvest's Postgres, and the load generator. Harvest is architected
for exactly the footprint this box provides — that is its stated design
premise, the thing `docs/comparison.md` calls "one fewer service to operate."

So a harvest win here is **not** evidence that harvest is faster than
Temporal, and this report will not say that it is. It is evidence about one
deployment size: the small single-box deployment where an evaluator is
choosing between "add an orchestrator cluster" and "use the Postgres I
already run." A Temporal win here, by contrast, *would* be a strong result
for Temporal, because it would come despite the venue.

**The competitor arm is written by someone who does not operate Temporal.**
The worker is configured to match harvest's registered concurrency rather
than tuned for Temporal. That is the fairest thing available here and it is
not the same as fair: a Temporal expert would likely produce a better number,
and the report will say so in the verdict, not only in a stub list.

No claim of the form "engine A is faster than engine B" is licensed by this
apparatus at any outcome. The registered claim is bounded to the venue.

## ⚖️ Pre-registration

### Shape

Matched as tightly as two SDKs permit:

| | harvest arm | Temporal arm |
|:--|:--|:--|
| workflow | canonical 3-activity sequential, ported by value from `e2e_bench_support.rs` | same three steps, same order, same ~40-byte payload |
| activity body | inert (no sleep, no I/O beyond the side-effect row) | identical |
| workflow slots | 8 (`MAX_CONCURRENT_WORKFLOWS`) | `MaxConcurrentWorkflowTaskExecutionSize: 8` |
| activity slots | 16 (`MAX_CONCURRENT_ACTIVITIES`) | `MaxConcurrentActivityExecutionSize: 16` |
| workers | 1 | 1 |
| persistence | PostgreSQL 16.13, loopback | the **same** PostgreSQL 16.13 server, loopback |
| retries | default | `MaximumAttempts: 1`, matching an inert body that cannot fail |

* **drain shape**: seed/start 2,000 workflows, measure wall-clock to terminal
  completion of all of them; report completed workflows/sec.
* 3 repetitions per arm; report mean and range.
* Arms run **sequentially, never concurrently** — four cores cannot host both
  engines at once without each becoming the other's noise.

### Lines

**L1 — the registered question.** `harvest_pg` mean completed workflows/sec
**≥** `temporal_go` mean completed workflows/sec.

**Pursue = harvest ≥ Temporal**, reported strictly as "at this venue and this
shape," per the bounding section above. **Kill = harvest < Temporal**, which
would be a genuine and publishable negative result: harvest losing on the
single-box footprint it is designed for, on a box that disadvantages its
competitor.

**L2 — is the comparison in a reportable regime at all?** Both arms complete
their 2,000-workflow drain within a **900 s** cap, in every repetition.

*Why:* a truncated or timed-out arm gives a bound, not a rate, and ledger #8
spent its L2 on a zero denominator for exactly this reason. **Kill = either
arm fails to drain inside the cap in any rep**, in which case L1 is reported
as indeterminate rather than resolved on a truncated number.

### Correctness precondition (both arms, every repetition)

1. every started workflow reaches a successful terminal state
   (`COMPLETED` / `WORKFLOW_EXECUTION_STATUS_COMPLETED`);
2. side-effect rows equal `workflows × 3` exactly — one per activity, so a
   Temporal activity retry that double-writes fails this, as does a dropped
   one;
3. no workflow task failures recorded on the Temporal arm.

A rep failing any of these is discarded and reported as discarded.

### Conditions

* 4 logical CPUs (Intel Xeon @ 2.10GHz), 15 GiB RAM.
* PostgreSQL 16.13 native loopback, `fsync=off`, `synchronous_commit=off`,
  `max_connections=300`. **Both engines use this one server**, in separate
  databases (`temporal`, `temporal_visibility`, and the assay's own) — so
  neither arm gets a storage engine the other does not.
* Temporal server `temporalio/auto-setup:1.25.2`, `--network host`, default
  namespace, default dynamic config.
* Temporal worker: Go SDK, `go.temporal.io/sdk`, Go 1.24.7.
* The Temporal container is **stopped** while the harvest arm runs, and the
  harvest pool is stopped while the Temporal arm runs. Recorded per rep.
* Box otherwise idle, per `docs/benchmarks.md`'s idleness precondition.

### Declared stubs (named now, not after the numbers)

* **Venue.** Stated at length above. Four cores; Temporal's services
  co-resident; no scale-out dimension measured on either side.
* **Tuning asymmetry.** Harvest runs at constants this repo tuned and
  published. Temporal runs at documented defaults plus matched concurrency.
  Defaults are not a tuned configuration, and the dynamic config is untouched.
* **Version staleness.** One Temporal server version, pinned. A later
  version may differ.
* **Unit.** Completed workflows/sec, both arms. Temporal's own published
  figures are usually per-action or per-state-transition; nothing here is
  comparable to those without the conversion `docs/benchmarks.md` documents.
* **Language.** The Temporal arm's worker is Go; harvest's is Rust. Activity
  bodies are inert on both, so this should be small, but it is not zero and
  this apparatus does not separate it.
* **One shape.** A 3-activity sequential workflow. Nothing here covers
  fan-out, child workflows, long timers, signals, or large histories, and
  Temporal's relative position may differ on any of them.
* **Feature parity is not measured and not claimed.** Temporal ships
  multi-region, versioning, and a managed cloud that harvest does not;
  `docs/comparison.md`'s "Where harvest is behind" section stands untouched
  by any number here. A throughput cell is not an evaluation.
* No measurement of any kind had been taken when these lines were set.

## Reproduce

Apparatus at `docs/assays/apparatus/0011-harvest-vs-temporal/`, added in the
follow-up commit. Run instructions in its README.
