## Phase R&D — assay ledger #14, latency-driven limiter vs `DefaultSlotTuner` (issue #1836)

Assay ledger #14 answers #1836's build-or-not question in a closed-loop
simulation. Pre-registration: `docs/rnd/2026-10-05-adaptive-concurrency-limiter-preregistration.md`.
Report: `docs/assays/0014-adaptive-concurrency-limiter.md`. No engine source
changed.

Verdict: kill, as registered. On the retrograde-collapse plant the best AIMD
candidate reached 18.8% of achievable goodput against an 85% line, and
Gradient2 reached 0.0%. The real `DefaultSlotTuner` also fails: it grows to the
cap in every cell when permit waits are reported. The AIMD failure is mostly a
pre-registration defect, and the Gradient2 failure depends on per-tick
batching, so the report names a re-charter and does not close the idea.
The apparatus is archived under `docs/assays/apparatus/0014-adaptive-concurrency-limiter/`
and is never a workspace member.
