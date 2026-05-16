//! Procedural macros for the `autumn-harvest` workflow engine.
//!
//! This crate provides attribute macros to define workflows, activities, and DAGs,
//! as well as collection macros to bundle them for registration.

use proc_macro::TokenStream;

mod activity;
mod collect;
mod dag;
mod query;
mod workflow;

/// Marks an async function as a Harvest workflow.
///
/// This macro generates a companion function that returns a `WorkflowInfo`
/// struct describing the workflow, which is used for registration.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    workflow::workflow_macro(attr.into(), item.into()).into()
}

/// Marks an async function as a Harvest activity.
///
/// This macro generates a companion function that returns an `ActivityInfo`
/// struct describing the activity, which is used for registration.
///
/// Supports attributes like `start_to_close`, `heartbeat_timeout`, and `retry`.
#[proc_macro_attribute]
pub fn activity(attr: TokenStream, item: TokenStream) -> TokenStream {
    activity::activity_macro(attr.into(), item.into()).into()
}

/// Marks a function as a Harvest DAG builder.
///
/// This macro generates a companion function that returns a `DagInfo`
/// struct describing the DAG, which is used for registration.
#[proc_macro_attribute]
pub fn dag(attr: TokenStream, item: TokenStream) -> TokenStream {
    dag::dag_macro(attr.into(), item.into()).into()
}

/// Collects multiple workflow functions into a `Vec<WorkflowInfo>`.
///
/// # Examples
///
/// ```ignore
/// let w = workflows![my_workflow, another_workflow];
/// ```
#[proc_macro]
pub fn workflows(input: TokenStream) -> TokenStream {
    collect::workflows_macro(input.into()).into()
}

/// Collects multiple activity functions into a `Vec<ActivityInfo>`.
///
/// # Examples
///
/// ```ignore
/// let a = activities![my_activity, another_activity];
/// ```
#[proc_macro]
pub fn activities(input: TokenStream) -> TokenStream {
    collect::activities_macro(input.into()).into()
}

/// Collects multiple DAG builder functions into a `Vec<DagInfo>`.
///
/// # Examples
///
/// ```ignore
/// let d = dags![my_dag, another_dag];
/// ```
#[proc_macro]
pub fn dags(input: TokenStream) -> TokenStream {
    collect::dags_macro(input.into()).into()
}

/// Marks a function as a read-only Harvest query handler (issue #234).
///
/// This attribute is a **documentation and tooling marker** — it validates that
/// the annotated item is a function but otherwise passes it through unchanged.
/// Register the function with [`WorkflowContext::register_query_handler`] inside
/// the workflow body.
///
/// # Example
///
/// ```ignore
/// use autumn_harvest::prelude::*;
///
/// #[derive(serde::Deserialize)]
/// struct ProgressQuery { include_details: bool }
///
/// #[derive(serde::Serialize)]
/// struct ProgressResponse { processed: u32 }
///
/// #[query]
/// fn progress_query(req: &ProgressQuery, count: u32) -> Result<ProgressResponse, String> {
///     Ok(ProgressResponse { processed: count })
/// }
/// ```
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    query::query_macro(attr.into(), item.into()).into()
}
