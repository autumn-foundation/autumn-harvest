//! Karpathy's autoresearch loop as durable Harvest workflows.
//!
//! An agent edits `train.py`, a trainer runs it, and a pure rule keeps or
//! discards the change. The loop runs all night. Each step is a workflow, a
//! child workflow or an activity, so a crash costs at most the step in flight.
//!
//! - [`workflows`] holds the session and the experiment workflows.
//! - [`activities`] holds the git, trainer and results activities.
//! - [`agent`] builds the agent harness: tools, policy, peers and stores.
//! - [`rules`] holds the keep rule. [`results`] holds the `results.tsv` format.
//! - [`runtime`] registers everything on the SQLite backend and drives it.
//!
//! See `README.md` for the guide and `docs/design/DESIGN-autoresearch.md` for the plan.

pub mod activities;
pub mod agent;
pub mod claude;
pub mod inbox;
pub mod lab;
pub mod results;
pub mod rules;
pub mod runtime;
pub mod scripted;
pub mod stores;
pub mod summary;
pub mod trainer;
pub mod workflows;
