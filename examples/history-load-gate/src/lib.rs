//! Measures the cold history load for the stage 2 gate (issue #2257).
//!
//! Stage 2 of epic #2247 is an owner-local disk cache (#2258). It ships
//! only when cold decisions are common or slow. This crate times the work
//! that such a cache saves:
//!
//! - one full history load from Postgres, through the worker load path;
//! - the same history from a local `SQLite` file, through the read path of
//!   `autumn-harvest-sqlite`;
//! - the cold load of every live run after an owner restart;
//! - the same warm-up through the owner disk cache (#2258), in
//!   [`disk_side`];
//! - the cold share that the cache cap alone causes, in [`cache_model`].
//!
//! [`gate`] applies the thresholds of the typed-state-snapshot spike.

pub mod cache_model;
pub mod disk_side;
pub mod gate;
pub mod pg_side;
pub mod seed;
pub mod sqlite_side;
pub mod stats;

/// The error type of this crate.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
