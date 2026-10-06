//! Deterministic simulation testing (issue #1830).
//!
//! A seed drives three worker processes and the orphan reclaimer through the
//! activity claim protocol of issue #1789: claim, start, heartbeat, orphan
//! reclaim and complete. One thread applies one store operation per step.
//! The seed fixes the order of every operation, every clock advance and
//! every fault, so a failing seed replays exactly.
//!
//! The default store is [`OracleStore`], an in-memory model of the SQL. The
//! differential test replays each run's operation log on Postgres and
//! requires equal outcomes and rows. [`Fencing::StateOnly`] restores the
//! guard from before issue #1789, and a sweep then finds the stale-owner bug.
//!
//! See `docs/testing/simulation.md` and
//! `docs/adr/0004-deterministic-simulation-testing.md`.

mod invariant;
mod rng;
mod sim;
mod store;
mod sweep;

pub use invariant::{Invariant, Violation};
pub use rng::SplitMix64;
pub use sim::{SimConfig, SimReport, SimStats, StepRecord, run, run_with};
pub use store::{
    Claim, ClaimStore, Fencing, Op, OracleStore, Orphan, Outcome, Row, TaskState, WriteOutcome,
};
pub use sweep::{
    FENCING_VAR, Nondeterminism, SEED_BASE_VAR, SEED_VAR, SEEDS_VAR, SeedPlan, SweepFailure,
    SweepSummary, TAIL_LINES, fencing_from_env, first_divergence, repro_command, run_twice, sweep,
};
