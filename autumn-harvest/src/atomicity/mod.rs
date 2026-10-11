//! Contention-adaptive atomicity: an R&D spike (issue #2012).
//!
//! A workflow whose effects live in the Harvest Postgres can undo a failed
//! run in two ways. Physical backout runs every step in one transaction and
//! rolls it back. A saga commits each step and runs compensations. This
//! module measures both and sketches a rule that picks one per workflow.
//!
//! - [`backout`] runs the steps of one run in one transaction, with one
//!   savepoint per step.
//! - [`rule`] picks an [`rule::Atomicity`] from a [`rule::WorkloadProfile`].
//! - [`verdict`] applies the go or no-go criteria to measured cells.
//! - [`harness`] runs one order workflow under each arm and measures it.
//!
//! The module has no stability guarantee. No production path calls it.
//! See `docs/rnd/contention-adaptive-atomicity.md` for the results.

pub mod backout;
pub mod harness;
pub mod rule;
pub mod verdict;
