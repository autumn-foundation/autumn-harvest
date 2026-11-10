//! The backend-neutral registration of the agent on a `HarvestBuilder`.
//!
//! It needs no backend feature. The SQLite plugin test runs the result on
//! a real driver.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, BoxFuture, ChatRequest, ChatResponse, activities,
    register, workflows,
};

/// A model that the test never calls.
#[derive(Debug)]
struct Idle;

impl AgentModel for Idle {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        panic!("the registration calls no model")
    }
}

fn names<T>(items: &[T], name: impl Fn(&T) -> &str) -> Vec<String> {
    let mut names: Vec<String> = items.iter().map(|item| name(item).to_owned()).collect();
    names.sort();
    names
}

#[test]
fn register_adds_every_workflow_activity_and_the_harness() {
    let harness = AgentHarness::new(Arc::new(Idle));
    let mut builder = register(HarvestBuilder::new(), harness);
    assert_eq!(
        names(builder.workflow_infos(), |info| info.name),
        names(&workflows(), |info| info.name),
    );
    assert_eq!(
        names(&builder.take_activities(), |info| info.name),
        names(&activities(), |info| info.name),
    );
    let built = builder.build();
    assert!(
        built.state::<AgentHarness>().is_some(),
        "the harness is state"
    );
}
