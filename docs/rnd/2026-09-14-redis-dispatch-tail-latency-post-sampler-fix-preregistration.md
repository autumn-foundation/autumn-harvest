# ⛏️ Prospect pre-registration: does Redis dispatch clear the L3 tail-latency line now that the sampler confound is fixed? (re-charter of assay ledger #8, line L3)

**Committed:** 2026-09-14T09:09:34Z (commit `704c8c5`'s actual author and
committer timestamp — an earlier version of this line gave a fabricated
round `00:00:00Z`, caught in PR review and corrected here), before any
measurement was taken. This document is the contract; the report that
follows it is graded against these lines, not against whatever the
numbers turn out to be.

**Sequencing note, disclosed rather than smoothed over:** a background
`cargo build --release` of the archived, source-unmodified apparatus
binary (no code changes from ledger #8's) was already running by
approximately 09:06 UTC, a few minutes before this commit. That build
produces no output bearing on the criteria below — no measurement existed
at commit time, and this document was written and committed before the
build finished and before the apparatus was ever run — but the hard
gate's letter ("you may not start building until [pre-registration is
committed]") was not honored in sequence. Recorded as a process lapse,
not excused as harmless by assertion.

**Remediated, per PR review, not left as a disclosed defect:** a reviewer
correctly rejected disclosure alone as insufficient to restore the
required sequencing. The run built under that lapse was discarded outright
— its numbers are not reported anywhere as evidence. The apparatus was
`cargo clean`'d (996.6 MiB removed) and rebuilt from scratch, and the
registered paced sweep in
`docs/assays/0009-redis-dispatch-tail-latency-post-sampler-fix.md` was
rerun in full, both actions taken well after this commit. The verdict in
that report rests on the clean rerun only.

## 🎯 Question

Assay ledger #8 killed the integrated Redis dispatch path on two of three
pre-set lines, including L3: paced at the drain arm's own sustained rate
(86.52 workflows/s), the Redis arm's dispatch-latency p99 was 426.96 ms
against a 250 ms line (1.71x over), while the no-channel control arm held
146.09 ms at the same pace. Issue #1428, filed from that same assay run,
found that four of ten `Worker` metrics samplers issued SQL on every poll
tick with no `metrics.is_enabled()` guard — `pg_stat_activity` was dominated
by one of them during the run — and #1429's own follow-up list (item 1,
"tail latency") says explicitly: *"Re-charter once the sampler defect
(#1428) is fixed, because it distorts both arms."* #1428 is now closed,
fixed by PR #1468 ("guard 4 metrics samplers on `is_enabled()`"), merged
2026-09-11, present on `trunk-dev` as of this commit.

**Falsifiable question:** with the sampler defect fixed and every other
condition held identical to assay #8's own paced sweep, does the Redis
dispatch arm's p99 dispatch latency now clear the original 250 ms line, and
does the control arm's p99 move at all?

This is a **measurement re-run**, not a new mechanism test — the code under
test is the same integration, on a codebase that has changed in exactly one
relevant way since ledger #8's numbers were taken.

> **Correction, added after the assay ran, caught in PR review — left
> attached to the original claim rather than silently rewritten:** "exactly
> one relevant way" was wrong even at the time this was written, and the
> report this document gated found two more: #1478 added a per-shard
> quota-key reconciler running on every worker's heartbeat cadence (both
> arms), and #1447 replaced `persist_scheduled_activities`'s per-row
> enqueue loop with a batched call — both land between ledger #8's `df4bd0d`
> baseline and this run's tree, alongside #1468. Neither this document's
> "one relevant way" framing below nor the Conditions section's implicit
> assumption that only #1468 changed should be read as established; see
> `docs/assays/0009-redis-dispatch-tail-latency-post-sampler-fix.md`'s
> Verdict section for the full, honestly-widened confound list. The
> pre-registered line and criteria are unaffected — they do not depend on
> this claim being true — but the framing sentence was inaccurate as
> written and is preserved above, uncorrected in place, so the record of
> what was believed at commit time stays honest.

## 👤 Decision this feeds

Whether issue #1429 item 1 ("tail latency under dispatch") still needs the
mechanism work it proposes — investigating the per-iteration `maintain`
promote round trips, the permit-sized poll read, and the reconcile sweep
holding a connection — or whether the sampler fix alone closed the gap.
**Decider:** whoever triages #1429 (filed by `madmax983`, the #1312
integration author); a clean pass here lets that item be closed or
downgraded, a miss confirms the mechanism work is still required and gives
it a fresh baseline number to design against.

## ⚖️ Success / kill criteria (numeric, set now)

Identical to ledger #8's own L3, unchanged:

- **Redis arm p99 ≤ 250 ms** at the paced shape, mean over 3 reps. Kill if
  above.

Recorded but not gating (context only, since #8 never gated on it):
control-arm p99 at the same pace, and the delta between this run's numbers
and #8's original 426.96 ms / 146.09 ms means, to size whatever the sampler
fix actually bought.

**Correctness precondition, unchanged from #8:** every paced run's
correctness checks (COMPLETED count, side-effect count, empty stream/PEL/
marker set after the run) must pass or the run is voided.

## 🧪 Conditions

Reused verbatim from ledger #8's own committed paced-sweep invocation
(`docs/assays/0008-redis-dispatch-integrated-throughput.md`, reproduce
section) — same apparatus, same env vars, same machine class:

- Same reference machine class: 4 logical CPUs, loopback Postgres 16
  (`fsync=off`, `synchronous_commit=off`), loopback Redis 7 (`save ""`,
  `appendonly no`). This session's container, not #8's — a different
  physical machine is an acknowledged condition change, not something this
  re-charter can hold constant; both are 4-core boxes, but absolute numbers
  are not expected to reproduce to the millisecond.
- Same pool shape: 4 in-process `Worker` instances, 8 workflow slots / 16
  activity slots each, one shared 64-connection pool,
  `dispatch.poll_interval` 20 ms on the Redis arm, `poll_interval` 25 ms on
  the control arm (`ASSAY6_WORKER_POLL_MS`, unchanged from #8 — the sampler
  fix removes the *unguarded query*, not the poll cadence itself).
- Same paced shape: `ASSAY6_SHAPES=paced ASSAY6_REPS=3 ASSAY6_PACED_SECS=30
  ASSAY6_PACED_RATE_MILLI=86520 ASSAY6_CONTROL_CAP_SECS=180`, i.e. the
  Redis drain arm's own measured rate from #8, not re-derived here.
- Same apparatus binary source
  (`docs/assays/apparatus/0008-redis-dispatch-integrated/`), rebuilt from
  the current `trunk-dev`-based branch so it picks up PR #1468's fix via
  its `autumn-harvest` path dependency. No apparatus code changes.
- Repetitions: 3 per arm, alternating, matching #8 exactly.

## 🚫 Anticipated stubs

Same as #8: loopback only, one Postgres, one Redis, no replication, no TLS,
no auth, a single queue, a no-op activity, four workers in one process, and
relaxed Postgres durability settings. A different physical/container host
than #8's own run is an added stub this time — absolute latency numbers are
compared against #8's own line and mean, not claimed reproducible to the
millisecond across machines.

## 🔍 Prior art

- `docs/assays/0008-redis-dispatch-integrated-throughput.md` — the original
  L3 kill this re-charter re-measures, its exact per-run p99 numbers, and
  the "found on the way" sampler defect.
- Issue #1428 (closed, fixed by PR #1468, merged 2026-09-11) — the
  confound this re-charter removes.
- Issue #1429 — the follow-up list naming this exact re-charter condition
  ("re-charter once the sampler defect (#1428) is fixed").
- No other ledger entry or open issue covers this question; nothing here
  duplicates a closed pit.

## ⏱️ Time box

One session, single pass, paced shape only (the drain-shape lines L1/L2 are
not part of this re-charter — #1429 doesn't ask for them and the mechanism
candidates it names are latency-specific, not throughput-specific). The
apparatus's own reproduce doc estimates the paced sweep at ~3 minutes of
run time; budget one hour total including build.
