//! Deterministic simulation testing (issue #1830).
//!
//! A seed drives three worker processes and the orphan reclaimer through the
//! activity claim protocol of issue #1789. The operations are claim, start,
//! heartbeat, release, orphan reclaim and complete. One thread applies one store operation per step.
//! The seed fixes the order of every operation, every clock advance and
//! every fault, so a failing seed replays exactly.
//!
//! The default store is [`OracleStore`], an in-memory model of the SQL. The
//! differential test replays each run's operation log on Postgres and
//! requires equal outcomes and rows. [`Fencing::StateOnly`] restores the
//! guard from before issue #1789, and a sweep then finds the stale-owner bug.
//!
//! This module is test infrastructure, not a stable API. It can change in
//! any release. [`Op`] and [`Outcome`] are exhaustive on purpose: a new
//! operation then breaks the build of the Postgres replay until it gets a
//! replay step.
//!
//! The [`world`] module drives whole actors instead (issue #2002). Its
//! Postgres world runs the real `worker.rs` poll loop.
//!
//! The [`speculate`] module models speculative decisions over an in-flight
//! commit (issue #2011). It is an R&D spike with no database.
//!
//! See `docs/testing/simulation.md` and
//! `docs/adr/0004-deterministic-simulation-testing.md`.

mod invariant;
mod rng;
mod sim;
pub mod speculate;
mod store;
mod sweep;
pub mod world;

pub use invariant::{Invariant, Violation};
pub use rng::SplitMix64;
pub use sim::{SimConfig, SimReport, SimStats, StepRecord, run, run_with};
pub use store::{
    Claim, ClaimStore, Fencing, Op, OracleStore, Orphan, Outcome, Row, TaskState, WriteOutcome,
};
pub use sweep::{
    CHECKS_VAR, FENCING_VAR, Nondeterminism, SEED_BASE_VAR, SEED_VAR, SEEDS_VAR, SeedPlan,
    SweepFailure, SweepSummary, TAIL_LINES, config_from_env, config_from_vars, first_divergence,
    repro_command, run_twice, run_twice_with, sweep,
};
