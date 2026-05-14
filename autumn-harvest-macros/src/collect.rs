//! The registry collection macros.
//!
//! This module provides the `workflows![]`, `activities![]`, and `dags![]`
//! bang macros. These are essential for taking the disparate companion
//! functions generated across a user's codebase and bundling them into
//! a unified registry that the engine can actually execute.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Token, parse::Parser, punctuated::Punctuated};

/// Bundles multiple `#[workflow]` companion functions into a unified registry.
///
/// When configuring a worker, the developer needs a way to hand the engine a list of
/// all the workflows it is allowed to execute. Instead of forcing them to manually
/// call `__autumn_workflow_info_foo()` for every function, this macro takes a list of
/// function names and expands them into a `Vec<autumn_harvest::WorkflowInfo>`.
///
/// # Examples
///
/// ```ignore
/// let registry = workflows![send_email, process_payment];
/// ```
pub fn workflows_macro(input: TokenStream) -> TokenStream {
    let names = match Punctuated::<Ident, Token![,]>::parse_terminated.parse2(input) {
        Ok(n) => n,
        Err(e) => return e.to_compile_error(),
    };

    let calls: Vec<_> = names
        .iter()
        .map(|name| {
            let companion = quote::format_ident!("__autumn_workflow_info_{name}");
            quote! { #companion() }
        })
        .collect();

    quote! {
        vec![ #(#calls),* ]
    }
}

/// Bundles multiple `#[activity]` companion functions into a unified registry.
///
/// Just like workflows, activities must be registered with the worker. This macro
/// provides a clean, ergonomic syntax to collect them all into a `Vec<autumn_harvest::ActivityInfo>`.
///
/// # Examples
///
/// ```ignore
/// let registry = activities![charge_credit_card, generate_pdf];
/// ```
pub fn activities_macro(input: TokenStream) -> TokenStream {
    let names = match Punctuated::<Ident, Token![,]>::parse_terminated.parse2(input) {
        Ok(n) => n,
        Err(e) => return e.to_compile_error(),
    };

    let calls: Vec<_> = names
        .iter()
        .map(|name| {
            let companion = quote::format_ident!("__autumn_activity_info_{name}");
            quote! { #companion() }
        })
        .collect();

    quote! {
        vec![ #(#calls),* ]
    }
}

/// Bundles multiple `#[dag]` companion functions into a unified registry.
///
/// DAGs (Directed Acyclic Graphs) define scheduled, dependency-driven pipelines.
/// This macro collects the generated metadata for each DAG so the engine's
/// scheduler knows when to trigger them.
///
/// # Examples
///
/// ```ignore
/// let registry = dags![daily_etl_job, weekly_report];
/// ```
pub fn dags_macro(input: TokenStream) -> TokenStream {
    let names = match Punctuated::<Ident, Token![,]>::parse_terminated.parse2(input) {
        Ok(n) => n,
        Err(e) => return e.to_compile_error(),
    };

    let calls: Vec<_> = names
        .iter()
        .map(|name| {
            let companion = quote::format_ident!("__autumn_dag_info_{name}");
            quote! { #companion() }
        })
        .collect();

    quote! {
        vec![ #(#calls),* ]
    }
}
