//! The Postgres driver: a testcontainers Postgres 16 and one real `Worker`.

use crate::compare::Run;
use crate::scenario::Scenario;

/// Run `scenario` on a fresh Postgres and return what the backend did.
pub async fn run(scenario: &Scenario) -> Run {
    unimplemented!("issue #2030: the Postgres driver for `{}`", scenario.name)
}
