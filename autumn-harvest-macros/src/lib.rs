//! Procedural macros for the `autumn-harvest` workflow engine.
//!
//! This crate provides attribute macros to define workflows, activities, DAGs,
//! query handlers, and update handlers, as well as collection macros to bundle
//! them for registration.

use proc_macro::TokenStream;

mod activity;
mod attr_util;
mod collect;
mod dag;
pub(crate) mod determinism_lint;
mod query;
mod signal;
mod update;
mod webhook;
mod workflow;

/// Parse a human-readable byte-size string into a `u64` byte count (issue #252).
///
/// Accepted suffixes: `KiB`, `KB`, `MiB`, `MB`, `GiB`, `GB`, or no suffix
/// (plain bytes). Used at macro expansion time to validate and embed the byte cap
/// as a `u64` literal in the generated `WorkflowInfo`/`ActivityInfo`.
///
/// Returns `None` for empty strings or strings with unrecognised suffixes.
#[allow(clippy::option_if_let_else)]
pub(crate) fn parse_byte_size_macro(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Try suffixes in longest-first order to avoid prefix collisions.
    let (digits, multiplier): (&str, u64) = if let Some(n) = s.strip_suffix("GiB") {
        (n.trim(), 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GB") {
        (n.trim(), 1_000_000_000)
    } else if let Some(n) = s.strip_suffix("MiB") {
        (n.trim(), 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n.trim(), 1_000_000)
    } else if let Some(n) = s.strip_suffix("KiB") {
        (n.trim(), 1024)
    } else if let Some(n) = s.strip_suffix("KB") {
        (n.trim(), 1_000)
    } else {
        (s, 1)
    };
    let n: u64 = digits.parse().ok()?;
    n.checked_mul(multiplier)
}

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

/// Marks a synchronous function as a read-only Harvest query handler (issue #346).
///
/// **Without attributes** (`#[query]`): documentation marker only — validates
/// the annotated item is a function and passes it through unchanged. Register
/// imperatively via `WorkflowContext::register_query_handler`.
///
/// **With `workflow = "name"`**: generates a companion function
/// `__autumn_query_handler_info_{fn_name}() -> QueryHandlerInfo` for use with
/// `queries![…]` and `HarvestBuilder::queries(…)`. The function must be
/// synchronous and take `ctx: &WorkflowContext` as its first argument.
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
/// #[query(workflow = "batch_processor")]
/// fn progress_query(_ctx: &WorkflowContext, req: ProgressQuery) -> Result<ProgressResponse, String> {
///     Ok(ProgressResponse { processed: 0 })
/// }
/// ```
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    query::query_macro(attr.into(), item.into()).into()
}

/// Marks an async function as a Harvest update handler (issue #346).
///
/// Generates a companion function
/// `__autumn_update_handler_info_{fn_name}() -> UpdateHandlerInfo` for use
/// with `updates![…]` and `HarvestBuilder::updates(…)`.
///
/// # Attributes
///
/// - `workflow = "name"` (**required**) — the workflow this handler belongs to.
/// - `validator = path::to::fn` (optional) — synchronous validator with
///   signature `fn(&serde_json::Value) -> Result<(), String>`.
///
/// # Example
///
/// ```ignore
/// use autumn_harvest::prelude::*;
///
/// #[derive(serde::Deserialize, serde::Serialize)]
/// struct ApproveRequest { approved: bool }
///
/// fn validate_approve(input: &serde_json::Value) -> Result<(), String> {
///     if input.get("approved").and_then(|v| v.as_bool()).unwrap_or(false) {
///         Ok(())
///     } else {
///         Err("not approved".to_string())
///     }
/// }
///
/// #[update(workflow = "subscription", validator = validate_approve)]
/// async fn approve(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
///     Ok(req.approved)
/// }
/// ```
#[proc_macro_attribute]
pub fn update(attr: TokenStream, item: TokenStream) -> TokenStream {
    update::update_macro(attr.into(), item.into()).into()
}

/// Marks a synchronous function as an inbound webhook trigger mapping
/// function (issue #344).
///
/// Signature verification, timestamp tolerance, replay protection, and secret
/// rotation are handled entirely by autumn-web 0.5's
/// `[security.webhooks.endpoints]` / `SignedWebhook` extractor, upstream of
/// this macro -- `#[webhook]` only maps an already-verified delivery to a
/// deterministic workflow trigger.
///
/// Generates a companion function
/// `__autumn_webhook_info_{fn_name}() -> WebhookTriggerInfo` and a public
/// alias `{fn_name}_info()`, for use with `webhooks![…]` and
/// `HarvestPlugin::webhooks(…)`.
///
/// # Attributes
///
/// - `path = "/hooks/..."` (**required**) — the exact HTTP path this webhook
///   binds to.
/// - Exactly one of `starts = "workflow_name"` or `signals = "workflow_name"`
///   (the latter requires `signal_name = "..."`).
/// - `queue = "..."` (optional) — task-queue override.
///
/// # Example
///
/// ```ignore
/// use autumn_harvest::prelude::*;
///
/// #[derive(serde::Deserialize)]
/// struct StripeEvent { id: String }
///
/// #[webhook(path = "/hooks/stripe", starts = "subscription_flow")]
/// fn map_stripe(_ctx: &WebhookCtx, evt: StripeEvent) -> Result<WorkflowId, String> {
///     Ok(WorkflowId::new(format!("stripe-{}", evt.id)))
/// }
/// ```
#[proc_macro_attribute]
pub fn webhook(attr: TokenStream, item: TokenStream) -> TokenStream {
    webhook::webhook_macro(attr.into(), item.into()).into()
}

/// Marks a function as a Harvest workflow signal handler.
///
/// Generates a typed signal sender method `signal_[signal_name]` on the sibling `[WorkflowName]Stub` struct.
#[proc_macro_attribute]
pub fn signal(attr: TokenStream, item: TokenStream) -> TokenStream {
    signal::signal_macro(attr.into(), item.into()).into()
}

/// Collects multiple workflow functions into a `Vec<WorkflowInfo>`.
#[proc_macro]
pub fn workflows(input: TokenStream) -> TokenStream {
    collect::workflows_macro(input.into()).into()
}

/// Collects multiple activity functions into a `Vec<ActivityInfo>`.
#[proc_macro]
pub fn activities(input: TokenStream) -> TokenStream {
    collect::activities_macro(input.into()).into()
}

/// Collects multiple DAG builder functions into a `Vec<DagInfo>`.
#[proc_macro]
pub fn dags(input: TokenStream) -> TokenStream {
    collect::dags_macro(input.into()).into()
}

/// Collects multiple query handler functions into a `Vec<QueryHandlerInfo>`.
///
/// Each name must have been annotated with `#[query(workflow = "…")]`.
///
/// # Example
///
/// ```ignore
/// let qs: Vec<QueryHandlerInfo> = queries![get_status, get_count];
/// ```
#[proc_macro]
pub fn queries(input: TokenStream) -> TokenStream {
    collect::queries_macro(input.into()).into()
}

/// Collects multiple update handler functions into a `Vec<UpdateHandlerInfo>`.
///
/// Each name must have been annotated with `#[update(workflow = "…")]`.
///
/// # Example
///
/// ```ignore
/// let us: Vec<UpdateHandlerInfo> = updates![approve, cancel];
/// ```
#[proc_macro]
pub fn updates(input: TokenStream) -> TokenStream {
    collect::updates_macro(input.into()).into()
}

/// Collects multiple `#[webhook]` mapping functions into a
/// `Vec<WebhookTriggerInfo>` (issue #344).
///
/// # Example
///
/// ```ignore
/// let hooks: Vec<WebhookTriggerInfo> = webhooks![map_stripe, map_github];
/// ```
#[proc_macro]
pub fn webhooks(input: TokenStream) -> TokenStream {
    collect::webhooks_macro(input.into()).into()
}

pub(crate) struct WorkflowPath {
    pub(crate) is_absolute: bool,
    pub(crate) workflow_simple_name: String,
    pub(crate) path_tokens: Vec<proc_macro2::TokenStream>,
    pub(crate) original_module_parts: Vec<String>,
}

pub(crate) fn parse_and_validate_workflow_path(
    workflow_name: &str,
    span: proc_macro2::Span,
) -> Result<WorkflowPath, syn::Error> {
    let is_absolute = workflow_name.starts_with("::");
    let parts: Vec<&str> = workflow_name.split("::").collect();

    // Validate segments
    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            // An empty segment is only allowed at the very start for absolute paths
            if idx > 0 || !is_absolute {
                return Err(syn::Error::new(
                    span,
                    format!("invalid workflow path: contains empty segment in '{workflow_name}'"),
                ));
            }
        }
    }

    let non_empty_parts: Vec<&str> = parts.into_iter().filter(|s| !s.is_empty()).collect();
    if non_empty_parts.is_empty() {
        return Err(syn::Error::new(
            span,
            format!("invalid workflow path: '{workflow_name}' has no segments"),
        ));
    }

    let workflow_simple_name = (*non_empty_parts.last().unwrap()).to_string();
    let module_parts = &non_empty_parts[..non_empty_parts.len() - 1];

    let mut path_tokens = Vec::new();
    let mut original_module_parts = Vec::new();
    for p in module_parts {
        // Validate that each part is a valid identifier (must start with letter/underscore and contain letters/numbers/underscores)
        if p.is_empty() {
            return Err(syn::Error::new(
                span,
                format!("invalid workflow path: contains empty segment in '{workflow_name}'"),
            ));
        }
        let first_char = p.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '_' {
            return Err(syn::Error::new(
                span,
                format!("invalid path segment '{p}' in workflow path '{workflow_name}'"),
            ));
        }
        for c in p.chars().skip(1) {
            if !c.is_alphanumeric() && c != '_' {
                return Err(syn::Error::new(
                    span,
                    format!("invalid path segment '{p}' in workflow path '{workflow_name}'"),
                ));
            }
        }
        let id = quote::format_ident!("{}", p);
        path_tokens.push(quote::quote! { #id });
        original_module_parts.push((*p).to_string());
    }

    Ok(WorkflowPath {
        is_absolute,
        workflow_simple_name,
        path_tokens,
        original_module_parts,
    })
}

#[allow(clippy::option_if_let_else)]
pub(crate) fn to_pascal_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out = first.to_uppercase().collect::<String>();
            let mut capitalize_next = false;
            for c in chars {
                if c == '_' {
                    capitalize_next = true;
                } else if capitalize_next {
                    out.push_str(&c.to_uppercase().collect::<String>());
                    capitalize_next = false;
                } else {
                    out.push(c);
                }
            }
            out
        }
    }
}

pub(crate) fn extract_ok_type(output: &syn::ReturnType) -> syn::Type {
    let syn::ReturnType::Type(_, ty) = output else {
        return syn::parse_quote! { () };
    };
    let syn::Type::Path(type_path) = &**ty else {
        return *ty.clone();
    };
    let Some(last) = type_path.path.segments.last() else {
        return *ty.clone();
    };
    if last.ident != "Result" && last.ident != "HarvestResult" {
        return *ty.clone();
    }
    if let syn::PathArguments::AngleBracketed(ref args) = last.arguments
        && let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first()
    {
        return ok_ty.clone();
    }
    syn::parse_quote! { () }
}

pub(crate) fn extract_ok_type_hint(output: &syn::ReturnType) -> String {
    let syn::ReturnType::Type(_, ty) = output else {
        return "()".to_string();
    };
    let syn::Type::Path(type_path) = &**ty else {
        return type_name_hint(ty);
    };
    let Some(last) = type_path.path.segments.last() else {
        return "()".to_string();
    };
    if last.ident != "Result" {
        return last.ident.to_string();
    }
    if let syn::PathArguments::AngleBracketed(ref args) = last.arguments
        && let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first()
    {
        return type_name_hint(ok_ty);
    }
    "()".to_string()
}

pub(crate) fn type_name_hint(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(tp) => tp.path.segments.last().map_or_else(
            || "?".to_string(),
            |s| {
                let ident = s.ident.to_string();
                if let syn::PathArguments::AngleBracketed(ref args) = s.arguments {
                    let inner: Vec<_> = args
                        .args
                        .iter()
                        .filter_map(|a| {
                            if let syn::GenericArgument::Type(t) = a {
                                Some(type_name_hint(t))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if inner.is_empty() {
                        ident
                    } else {
                        format!("{ident}<{}>", inner.join(", "))
                    }
                } else {
                    ident
                }
            },
        ),
        syn::Type::Reference(r) => type_name_hint(&r.elem),
        syn::Type::Tuple(t) if t.elems.is_empty() => "()".to_string(),
        syn::Type::Tuple(t) => {
            let parts: Vec<_> = t.elems.iter().map(type_name_hint).collect();
            format!("({})", parts.join(", "))
        }
        _ => "?".to_string(),
    }
}
