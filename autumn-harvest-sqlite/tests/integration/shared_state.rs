//! `SqliteRuntime::set_shared_state` (issue #2467).
//!
//! `ctx.state::<T>()` reads one map in every context: a workflow, an
//! activity, a local activity and a query replay. The Postgres worker gives
//! them the `HarvestBuilder::state` values in the same way.

// The `#[workflow]` and `#[activity]` macros need an `async fn`.
#![allow(clippy::unused_async)]
// The macros read the unused `_n` input through their expansion.
#![allow(clippy::used_underscore_binding)]

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::{Value, json};

#[derive(Debug)]
struct Tag(&'static str);

fn tag_of_activity(ctx: &ActivityContext) -> String {
    ctx.state::<Tag>().map_or("none", |tag| tag.0).to_owned()
}

#[activity]
async fn remote_tag(ctx: &ActivityContext, _n: i64) -> Result<String, String> {
    Ok(tag_of_activity(ctx))
}

#[activity(local = true)]
async fn local_tag(ctx: &ActivityContext, _n: i64) -> Result<String, String> {
    Ok(tag_of_activity(ctx))
}

#[workflow]
async fn tagged(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.register_query("tag", || json!("query"));
    let workflow = ctx.state::<Tag>().map_or("none", |tag| tag.0).to_owned();
    let remote: String = ctx
        .execute_activity(&remote_tag_info(), n)
        .await
        .map_err(|error| error.to_string())?;
    let local: String = ctx
        .execute_local_activity(&local_tag_info(), n)
        .await
        .map_err(|error| error.to_string())?;
    ctx.wait_for_signal("go")
        .await
        .map_err(|error| error.to_string())?;
    Ok(json!({ "workflow": workflow, "remote": remote, "local": local }))
}

#[workflow]
async fn query_reads_state(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let tag = ctx.state::<Tag>().map_or("none", |tag| tag.0);
    ctx.register_query("tag", move || json!(tag));
    ctx.wait_for_signal("go")
        .await
        .map_err(|error| error.to_string())
}

fn runtime(tag: Option<&'static str>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&tagged_info());
    rt.register_workflow(&query_reads_state_info());
    rt.register_activity(&remote_tag_info());
    rt.register_activity(&local_tag_info());
    if let Some(tag) = tag {
        let mut map: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
        map.insert(TypeId::of::<Tag>(), Box::new(Tag(tag)));
        rt.set_shared_state(Arc::new(map));
    }
    rt
}

#[tokio::test]
async fn every_context_reads_the_shared_state() {
    let mut rt = runtime(Some("hive"));
    let exec = rt.start_workflow("tagged", json!(1)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingSignal(_)
    ));
    rt.send_signal(exec, "go", json!(null)).unwrap();
    let RunState::Completed(output) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run completes after the signal");
    };
    assert_eq!(
        output,
        json!({ "workflow": "hive", "remote": "hive", "local": "hive" })
    );
}

#[tokio::test]
async fn a_query_replay_reads_the_shared_state() {
    let mut rt = runtime(Some("hive"));
    let exec = rt.start_workflow("query_reads_state", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(
        rt.query(exec, "tag", json!(null)).await.unwrap(),
        json!("hive")
    );
}

#[tokio::test]
async fn the_default_state_is_empty() {
    let mut rt = runtime(None);
    let exec = rt.start_workflow("tagged", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.send_signal(exec, "go", json!(null)).unwrap();
    let RunState::Completed(output) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run completes after the signal");
    };
    assert_eq!(
        output,
        json!({ "workflow": "none", "remote": "none", "local": "none" })
    );
}
