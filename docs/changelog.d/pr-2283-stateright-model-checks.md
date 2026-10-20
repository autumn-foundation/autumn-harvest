## Testing — Stateright model checks of the DST oracles (PR #2283)

**What shipped.** A new test target, `stateright`
(`autumn-harvest/tests/stateright/`), with the
[Stateright](https://www.stateright.rs) crate as a dev-dependency. Two models
visit every reachable state of the DST oracles within a bound:

- `activity_claim.rs` drives `OracleStore` through the operations of
  `dst::run`: beat, claim, start, heartbeat, release, complete, crash,
  restart, orphan scan and orphan requeue. It judges each run with
  `dst::Ghost`, the checker of the seeded sweep.
- `owner_lease.rs` drives `OracleOwnerStore`, the store half of the fenced
  owner lease (issue #2250). The owner half copies the scheduler of
  `dst::owner`, with every choice enumerated.

**Why.** TLA+ is exhaustive, but its specs restate the protocol by hand. DST
runs the Rust oracle, but a seed samples one schedule. Stateright checks
every schedule of the oracle that the differential test compares with
Postgres. ADR 0010 records the decision.

**Results.**

- The claim-epoch design keeps all six invariants of
  `formal/tla/ActivityClaim.tla`. With `Fencing::StateOnly`, the checker
  finds the issue #1789 trace in 7 steps and breaks `TerminalByCurrentClaim`,
  `OwnerWritesByCurrentClaim`, `HeartbeatByCurrentClaim` and
  `ClaimIdsAreUnique`.
- The owner-lease design keeps all seven invariants, and one behavior
  reaches all five nemesis tags of `OwnerLeaseReach.cfg`. Each of the seven
  defects breaks the invariant that its TLA+ row names.
- The tests read `formal/tla/models.txt`. A new red TLA+ row for either spec
  fails here until a Stateright test breaks the same invariant.
- Mutation check: dropping the `attempt` term of the claim fence, or the
  fence-lock check of an acquisition, turns the design checks red.

**CI.** The per-PR bounds run in about 10 seconds in a debug build, in the
existing no-db test job on Linux, macOS and Windows. The new `stateright`
job of `dst-nightly.yml` sets `HARVEST_STATERIGHT_DEEP=1` and checks the
larger bounds in release mode, in about 1 minute.

**Library changes.** `OracleStore`, `OracleOwnerStore`, their row and
decision types and the `named!` enums derive `Hash`. `dst::Ghost`,
`dst::Live` and `dst::WriteKind` are re-exported. No new `WorkflowEvent`
variant, no migration, no production code path changed.

**Docs.** New `docs/testing/stateright.md` and
`docs/adr/0010-stateright-model-checks.md`. `formal-methods.md`,
`concurrency-model-checking.md`, `simulation.md` and `architecture.md` link
them.
