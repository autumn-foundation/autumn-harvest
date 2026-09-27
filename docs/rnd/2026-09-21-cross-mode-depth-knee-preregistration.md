# Pre-registration: cross-mode throughput depth knee

> Committed **before** any repeated-rep measurement at these depths was
> taken. Assay ledger #12. Nothing in this file is edited after the first
> measurement; the report at `docs/assays/0012-cross-mode-depth-knee.md`
> appends the apparatus run, the numbers and the verdict.

Source tree: `b673b8b`. Registered 2026-09-21.

## 🎯 Question

Ledger #10 named three open re-charters when it closed. This is the third:
*"the position of the depth curve's knee."* Its own post-hoc depth
diagnostic (`docs/assays/apparatus/0010-cross-mode-throughput/results/
depth-diagnostic.md`, single repetition per depth, not pre-registered)
found `postgres` ahead of `redis_pg` at depths 250 and 500, and behind it at
depths 1000 and 2000 — a crossover somewhere in `(500, 1000]`, on n=1.

`docs/operations/redis-dispatch.md`'s own "When to use it" section gives no
number: "Turn it on when the queue is deep... Leave it off when the backlog
is shallow." Both claims are true and neither is actionable without a
depth.

**Falsifiable question:** at the canonical 3-activity drain shape (the same
apparatus, same host, same tree lineage as ledger #10), under proper
repetition (n=3, not n=1), does the `postgres` arm's mean
completed-workflows/sec first fall below the `redis_pg` arm's mean at a
backlog depth inside `(500, 1000]` — confirming the single-rep diagnostic's
suggested location — or does replication move the crossover outside that
band, or erase it?

**Decision this feeds:** whether `docs/operations/redis-dispatch.md`'s "When
to use it" section can cite a concrete backlog-depth figure instead of
"deep" / "shallow". **Decider:** whoever owns that operator guide (issue
#1312's owner).

## ⚖️ Pre-registration

### Shape

Reuse of `docs/assays/apparatus/0010-cross-mode-throughput/` unmodified —
same workload (`wf_three_activities`, ported by value and already verified
field-for-field against `e2e_bench_support.rs` per ledger #10), same
canonical `{}` input, same Postgres durability (`fsync=off`,
`synchronous_commit=off`), same worker-pool constants. Only `ASSAY10_ARMS`
(narrowed to `postgres,redis_pg` — the `sqlite` arm is not part of this
question) and `ASSAY10_WORKFLOWS` (depth) vary between runs.

### Depths

`500` and `1000` first, in that order — these two bracket the suggested
knee and are the riskiest-assumption check (below). If both replicate
ledger #10's single-rep ordering (postgres > redis_pg at 500, postgres <
redis_pg at 1000) with no overlap across reps, `750` is run to narrow the
band. `250` is not re-run: ledger #10 already shows postgres decisively
ahead there and this assay has no line that depends on it.

### Reps

`ASSAY10_REPS=3` per depth per arm (the apparatus's own default), same as
ledger #10's registered sweep. A depth's ordering counts as replicated only
if the two arms' rep ranges do not overlap.

### Riskiest assumption, attacked first

That the single-rep crossover is a stable feature of the two arms rather
than run-to-run noise inside a single measurement each. Depths 500 and 1000
are run before anything else; if reps overlap at either depth (the arms'
ranges intersect), the crossover claim itself is unsettled and the assay
stops there rather than spending time narrowing a band that may not exist.

### Success line

The crossover depth `D*` — the smallest tested depth at which `postgres`'s
mean is below `redis_pg`'s mean, with non-overlapping rep ranges at that
depth and at the depth immediately below it — falls inside `(500, 1000]`.

### Kill line

Either arm's rep ranges overlap at 500 or at 1000 (ordering not
replicated), or the non-overlapping crossover falls outside `(500, 1000]`
among the depths actually tested.

### Control

The two arms are each other's control: same apparatus invocation, same
fixture, same host, same tree, run back to back per depth. No third arm is
needed for this question.

### Containment

`docs/assays/apparatus/0010-cross-mode-throughput/` unmodified, non-workspace
member, throwaway `assay10` Postgres database dropped and recreated per
repetition, Redis keys under the apparatus's own prefix only, no
`FLUSHALL`, no production data or spend.

### Time box

Same session, same day (2026-09-21). Depths 500 and 1000 first; 750 only if
the riskiest-assumption check clears; stop regardless once the box closes
today.

## 🔍 Prior art

`docs/assays/0010-cross-mode-throughput.md` and its `depth-diagnostic.md`
are the entirety of the prior art: one repetition per depth, run post-hoc
and explicitly not pre-registered, explicitly named as a "durable finding"
about flatness (`redis_pg` within 1.4% across depth) but never
pre-registered or repeated as a claim about *where* the two arms cross.
Re-running the same two depths with repetition is not re-digging a closed
pit: ledger #10 named this exact gap as open, and n=1 cannot by itself
support the operator guide citing a number.
