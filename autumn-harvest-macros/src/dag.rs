//! The `#[dag]` attribute macro implementation.
//!
//! This module is responsible for parsing declarative pipeline definitions
//! and translating scheduling rules (like cron strings) into metadata that
//! the engine uses to wake up and dispatch work.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitBool, LitInt, LitStr, parse::Parser as _};

/// The parsed configuration payload extracted from the `#[dag(...)]` attribute.
///
/// DAGs represent recurring, scheduled pipelines of execution. This struct captures
/// the scheduling rules defined by the developer, translating them from macro tokens
/// into the runtime [`autumn_harvest::DagInfo`] that the scheduler uses to wake up and dispatch work.
#[derive(Debug, Default)]
struct DagAttrs {
    /// The cron expression defining when this DAG should run.
    /// If omitted, the DAG can only be triggered manually.
    schedule: Option<String>,
    /// Determines whether the scheduler should historically "backfill" runs
    /// if the system was offline during scheduled intervals.
    catchup: bool,
    /// Limits how many instances of this specific DAG can be running at the exact same time.
    /// Crucial for preventing heavy data pipelines from trampling each other.
    max_active_runs: u32,
    /// The default task queue to route child activities to, unless they specify their own.
    default_queue: Option<String>,
}

/// Parses the raw token stream from the `#[dag(...)]` macro into a structured `DagAttrs`.
///
/// It acts as the frontline validation, ensuring that developers get immediate,
/// red squiggly lines in their editor if they misconfigure the DAG's schedule or limits,
/// rather than discovering the error at runtime.
fn parse_attrs(attr: TokenStream) -> syn::Result<DagAttrs> {
    let mut result = DagAttrs {
        max_active_runs: 1,
        ..DagAttrs::default()
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("schedule") {
            let value: LitStr = meta.value()?.parse()?;
            result.schedule = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("catchup") {
            let value: LitBool = meta.value()?.parse()?;
            result.catchup = value.value;
            Ok(())
        } else if meta.path.is_ident("max_active_runs") {
            let value: LitInt = meta.value()?.parse()?;
            result.max_active_runs = value.base10_parse()?;
            Ok(())
        } else if meta.path.is_ident("default_queue") {
            let value: LitStr = meta.value()?.parse()?;
            result.default_queue = Some(value.value());
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected schedule, catchup, max_active_runs, or default_queue",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

/// The core engine of the `#[dag]` macro.
///
/// This macro transforms a standard Rust function into a declarative pipeline builder.
/// It leaves the original function intact, but creates a hidden companion function
/// that returns a [`autumn_harvest::DagInfo`]. The engine uses this info to register the schedule
/// and execute the builder to construct the dependency graph.
///
/// # Examples
///
/// ```ignore
/// #[dag(schedule = "0 0 * * *", catchup = true)]
/// fn daily_report(dag: &mut DagBuilder) {
///     // Define the graph structure here
/// }
/// ```
pub fn dag_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(attrs) => attrs,
        Err(error) => return error.to_compile_error(),
    };

    let input_fn: ItemFn = match syn::parse2(item) {
        Ok(function) => function,
        Err(error) => return error.to_compile_error(),
    };

    if input_fn.sig.asyncness.is_some() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "#[dag] functions must not be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_dag_info_{fn_name}");

    let schedule_expr = attrs.schedule.as_deref().map_or_else(
        || quote! { None },
        |expr| quote! { Some(::autumn_harvest::Schedule::Cron(#expr.to_string())) },
    );
    let catchup = attrs.catchup;
    let max_active_runs = attrs.max_active_runs;
    let default_queue = attrs
        .default_queue
        .as_deref()
        .map_or_else(|| quote! { None }, |queue| quote! { Some(#queue) });

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::DagInfo {
            ::autumn_harvest::DagInfo {
                name: #fn_name_str,
                module: module_path!(),
                schedule: #schedule_expr,
                catchup: #catchup,
                max_active_runs: #max_active_runs,
                default_queue: #default_queue,
                builder: |dag| {
                    #fn_name(dag);
                },
            }
        }
    }
}
