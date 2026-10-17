//! The `SQLite` driver: one in-memory `SqliteRuntime` on the wall clock.

use crate::compare::Run;
use crate::scenario::Scenario;

/// Run `scenario` on a fresh in-memory `SQLite` runtime.
pub async fn run(scenario: &Scenario) -> Run {
    unimplemented!("issue #2030: the SQLite driver for `{}`", scenario.name)
}
