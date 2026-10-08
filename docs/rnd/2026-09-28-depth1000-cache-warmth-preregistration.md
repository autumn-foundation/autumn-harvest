# Pre-registration: does per-rep database recreation drive the depth-1000 `postgres`-arm variance?

> Committed **before** any repeated-rep measurement under the warm condition
> was taken. Assay ledger #13. Nothing in this file is edited after the
> first measurement; the report at
> `docs/assays/0013-depth1000-cache-warmth.md` appends the apparatus run,
> the numbers and the verdict.

Source tree: `7906f22`. Registered 2026-09-28.

## 🎯 Question

Ledger #12 killed the depth-knee crossover claim because the `postgres`
arm's three reps at depth 1000 spanned 14.94-24.04 workflows/sec (61%
swing), fully containing `redis_pg`'s tight range at the same depth. It
named an untested candidate cause and left it as an open pit: *"cold
buffer-cache/page-cache effect specific to whichever repetition runs first
against a freshly recreated database at this depth."* Every arm in this
apparatus (`postgres` and `redis_pg` alike) drops and recreates the assay
database before each repetition (`src/main.rs:365-379`, called once per
rep regardless of arm), so if cold cache were driving `postgres`'s spread,
removing the drop/recreate between reps should tighten it.

**Falsifiable question:** at backlog depth 1000, holding everything else
about the apparatus fixed, does running six repetitions of the `postgres`
arm **without** dropping/recreating the database between reps (single
`CREATE DATABASE` up front, `TRUNCATE` + reseed between reps — a "warm"
condition) produce a coefficient of variation (CV = sample stdev / mean)
at or below 5% — matching depth 500's own 1.3% CV and `redis_pg`'s tight
spread at depth 1000 — while six repetitions run the unmodified "cold"
condition (drop/recreate every rep, ledger #12's own method, run on this
same host/tree for a same-host comparison per this repo's own established
practice in ledger #9) reproduce a CV at or above 15%?

**Decision this feeds:** (a) whether `docs/operations/redis-dispatch.md`'s
"When to use it" section can eventually cite a concrete backlog-depth
figure once a non-noisy `postgres`-arm measurement exists at depth 1000
(decider: whoever owns that operator guide, issue #1312's owner), and (b)
whether the assay apparatus's per-rep drop/recreate methodology — reused
as-is across ledger #2, #8, #9, #10, #11 and #12 — needs to change to a
warm-reuse pattern for `postgres`-arm measurements to be trustworthy at
depth ≥1000 (decider: whoever reviews future assay reports against this
apparatus).

## ⚖️ Pre-registration

### Shape

A patched copy of `docs/assays/apparatus/0010-cross-mode-throughput/`,
forked into `docs/assays/apparatus/0013-depth1000-cache-warmth/` (0010
stays unmodified — it is the artifact of record for ledger #2/#8-#12).
The only functional change: an `ASSAY10_RECREATE_DB` env knob.

* `ASSAY10_RECREATE_DB=1` (default, matches 0010's only behavior exactly):
  `DROP DATABASE IF EXISTS ... WITH (FORCE)` + `CREATE DATABASE` + schema
  apply, before every repetition. This is the **cold** condition and is
  byte-for-byte the existing 0010 code path.
* `ASSAY10_RECREATE_DB=0`: `CREATE DATABASE` + schema apply exactly once,
  before the first repetition; each subsequent repetition truncates the
  workflow/event/task-queue tables the workload writes to and reseeds,
  instead of dropping the database. This is the **warm** condition.

Same workload (`wf_three_activities`), same canonical `{}` input, same
Postgres durability (`fsync=off`, `synchronous_commit=off`), same
worker-pool constants, `postgres` arm only — `redis_pg` is not needed
here: ledger #12 already shows it stays tight at depth 1000 (21.24-22.03)
under the identical drop/recreate-every-rep condition, so it is not an
open question this assay needs to re-settle. `ASSAY10_WORKFLOWS=1000`
fixed for every run.

### Reps

`n=6` per condition (cold, warm) — double ledger #12's `n=3`, because the
question here is the *shape* of the spread (CV against a numeric line),
which needs more points than the crossover-replication question did.

### Riskiest assumption, attacked first

That the drop/recreate step, not something else about the `postgres` arm
at this depth, is the source of the variance. The **warm** condition is
run first: if it does *not* tighten the spread (CV stays ≥15%), the
cold-cache hypothesis is dead immediately and the cold condition is run
anyway only to confirm this host reproduces ledger #12's spread at all
(needed either way, as the same-host control), not to narrow anything
further.

### Success line (cold-cache hypothesis: pursue)

Warm CV ≤ 5% **and** cold CV ≥ 15% on this host. Read as: the drop/recreate
step is the driver: recommend the apparatus switch depth-≥1000 `postgres`
measurements to the warm pattern, and flag every prior ledger entry that
used the cold pattern at these depths as carrying unquantified variance
from this cause.

### Kill line (cold-cache hypothesis: kill)

Warm CV ≥ 15% (the spread persists with no drop/recreate between reps).
Read as: something else — not per-rep database recreation — drives the
depth-1000 `postgres` spread; the apparatus's drop/recreate pattern is
exonerated for this specific effect.

### Undetermined

Warm CV lands in (5%, 15%), or cold CV on this host lands outside [15%,
∞) and so does not reproduce ledger #12's spread at all (a same-host
replication failure, reported as such rather than forced into either
line).

### Control

The cold condition, run on this same host/tree back to back with the warm
condition, is the control — this repo's own established practice (ledger
#9) for isolating one variable is same-host/same-tree, not a cross-host
comparison against ledger #12's own numbers.

### Containment

Forked apparatus directory, non-workspace member, throwaway `assay13`
Postgres database (distinct name from `assay10`/`assay12` to avoid any
collision with a stale prior run), no production data, no spend. `redis`
is not started for this assay — the `postgres` arm doesn't touch it.

### Time box

Same session, same day (2026-09-28). Warm six reps first, then cold six
reps for the same-host control. Stop regardless once both are collected;
no depth other than 1000 is in scope.

## 🔍 Prior art

`docs/assays/0012-cross-mode-depth-knee.md` is the entirety of the prior
art: it measured the spread (n=3, cold only) and named the cold-cache
candidate explicitly as untested. Re-running depth 1000 is not re-digging
that closed pit — #12 named this exact gap as open and its own n=3 cold
data cannot by itself isolate cache warmth as a cause versus any other
explanation.
