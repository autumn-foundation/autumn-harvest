## Testing — The fenced owner lease, modelled before code (issue #2250)

Epic #2247 adds a fenced decision owner per partition. This change writes
the spec first. It adds no engine code, no migration and no new
`WorkflowEvent` variant.

**TLA+.** `formal/tla/OwnerLease.tla` models one partition, its owner row,
decisions, renewals, releases and signals. The four nemeses are always on.
A stale owner commits after a takeover. The lease flaps during an append.
A renewal lands during a commit. A signal is acked just before an epoch
bump. TLC passes the design (5,629,559 distinct states with worker
symmetry). Seven mutant configs each break one rule and fail on the named
invariant. The mutant without the commit-time fence (golem#3766) fails
`OneOwnerPerEpoch`. `OwnerLeaseReach.cfg` shows that one behaviour of the
design reaches all four nemeses.

**Simulator.** `autumn_harvest::dst::owner` runs the same defects,
nemeses and invariants from a seed. `OwnerStore` is the seam for the
stage 1a store (issue #2251), with `run_with` and `sweep_with`. The `dst`
target runs the sweep on every PR. The nightly sweep runs it on 4,000,000
seeds. A guard test fails when `harvest_decision_owner` lands with no
simulator run against a real store.

**ADR.** `docs/adr/0010-fenced-owner-lease.md` records the rules R1 to R7
and the spec changes that the model forces. One change: the fence takes
`FOR KEY SHARE` and the takeover takes `FOR UPDATE`, so a renewal does not
wait on a commit. Another: the fence compares the epoch that the decision
pinned, not the holder.

**Refactor.** The `named!` enum macro moves from `dst::speculate` to
`dst/named.rs`, so both harnesses share it.
