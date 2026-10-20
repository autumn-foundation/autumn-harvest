# ADR 0011: Stateright model checks of the DST oracles

## Status

Accepted.

## Context

Three tools check the Postgres-coordinated protocols today:

- **TLA+ and TLC** (issue #1819) visit every state of a bounded spec. The
  spec restates the protocol by hand, so it can differ from the code.
- **DST** (issue #1830, [ADR 0004](0004-deterministic-simulation-testing.md))
  runs the Rust oracle of the SQL. A seed picks one schedule, so a sweep
  samples the schedules. A rare schedule can stay unseen for a long time.
- **The differential test** replays DST runs on Postgres. It keeps the oracle
  honest for the schedules that DST picked.

No tool visits every schedule of the Rust oracle. A guard change in
`OracleStore` or `OracleOwnerStore` is thus checked exhaustively only after
someone ports it to a TLA+ spec by hand.

## Decision

Add the `stateright` test target, with the
[Stateright](https://www.stateright.rs) crate as a dev-dependency.

- A Stateright model is plain Rust. Its `next_state` calls the oracle, so
  the checker explores the code that the differential test compares with
  Postgres. No second spec is needed.
- The checker visits every reachable state within a bound, breadth first.
  A counter-example is therefore short and replays as a list of actions.
- The models reuse the DST ghost checker and invariant names. They read
  `formal/tla/models.txt`, so each TLA+ counter-example must fail here on the
  same invariant.
- The per-PR bound runs in about 10 seconds in a debug build, inside the
  existing no-db test job. The nightly DST workflow checks larger bounds.

The first models cover the activity claim protocol (issue #1789) and the
fenced owner lease (issue #2250). Both have a Rust oracle already.

Two other options were rejected:

- **More DST seeds.** A sweep cannot show that no bad schedule exists. It
  only makes one less likely.
- **Kani on the oracle.** CBMC unrolls loops and heap data. A protocol with
  several actors and a growing history is outside what it finishes.

## Consequences

- The owner half of the owner-lease model copies the scheduler of
  `dst::owner`. That scheduler mixes the action choice with a seeded random
  draw, so the model cannot call it. A change there must change the model.
- `OracleStore`, `OracleOwnerStore` and the DST ghost types derive `Hash`.
  Stateright fingerprints each state with it.
- `stateright` adds ten crates to the lockfile, all MIT or Apache-2.0. None
  of them ships. The loom and Shuttle builds do not compile it.
- A pass holds within the bound only. Liveness is out of scope, because a
  bound ends every path.
- Next scope: a Rust oracle for `WorkflowTaskClaim` and `CodecRotation`,
  then a model of each.

See [`docs/testing/stateright.md`](../testing/stateright.md).
