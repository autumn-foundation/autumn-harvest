//! `#[webhook]` attribute macro for declarative inbound webhook triggers
//! (issue #344).
//!
//! Emits the original function unchanged plus a companion
//! `__autumn_webhook_info_{fn_name}() -> ::autumn_harvest::WebhookTriggerInfo`
//! and a public alias `{fn_name}_info()`, collected by `webhooks![]`.
//!
//! Signature verification, timestamp tolerance, replay protection, and secret
//! rotation are **not** configured here — autumn-web 0.5's
//! `[security.webhooks.endpoints]` / `SignedWebhook` extractor owns all of
//! that, upstream of this macro. `verifier`/`idempotency_header` attributes
//! (from the original issue #344 spec, predating that autumn-web capability)
//! are rejected at compile time with a pointer to the real configuration
//! surface.
//!
//! Attributes:
//! - `path = "/hooks/..."` (required) — the exact HTTP path this webhook
//!   binds to; must match a `security.webhooks.endpoints[].path` exactly.
//! - Exactly one of:
//!   - `starts = "workflow_name"` — start a fresh workflow execution.
//!   - `signals = "workflow_name"` (with required `signal_name = "..."`) —
//!     atomically start-or-attach and signal an existing execution.
//! - `queue = "..."` (optional) — task-queue override for the dispatched start.
//!
//! The annotated function must be synchronous, take `ctx: &WebhookCtx` as its
//! first argument and exactly one typed payload parameter, and return
//! `Result<WorkflowId, E>`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

struct WebhookAttrs {
    path: Option<String>,
    starts: Option<String>,
    signals: Option<String>,
    signal_name: Option<String>,
    queue: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<WebhookAttrs> {
    let mut result = WebhookAttrs {
        path: None,
        starts: None,
        signals: None,
        signal_name: None,
        queue: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("path") {
            let value: LitStr = meta.value()?.parse()?;
            result.path = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("starts") {
            let value: LitStr = meta.value()?.parse()?;
            result.starts = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("signals") {
            let value: LitStr = meta.value()?.parse()?;
            result.signals = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("signal_name") {
            let value: LitStr = meta.value()?.parse()?;
            result.signal_name = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("queue") {
            let value: LitStr = meta.value()?.parse()?;
            result.queue = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("verifier") || meta.path.is_ident("idempotency_header") {
            let attr_name = meta
                .path
                .get_ident()
                .map_or_else(|| "?".to_string(), std::string::ToString::to_string);
            Err(meta.error(format!(
                "unsupported attribute `{attr_name}`: signature verification, timestamp \
                 tolerance, and replay protection are configured on the embedding app under \
                 `[security.webhooks]` (autumn-web 0.5 signed-webhook endpoints), not on \
                 #[webhook]. See docs/getting-started/12-webhooks.md."
            )))
        } else {
            Err(meta.error(
                "unsupported attribute: expected `path = \"/...\"`, exactly one of \
                 `starts = \"workflow_name\"` or `signals = \"workflow_name\"` (with \
                 `signal_name = \"...\"`), and an optional `queue = \"...\"`",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
pub fn webhook_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let func: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    let Some(path) = attrs.path else {
        return syn::Error::new_spanned(func.sig.fn_token, "#[webhook] requires `path = \"/...\"`")
            .to_compile_error();
    };

    let target_expr = match (&attrs.starts, &attrs.signals, &attrs.signal_name) {
        (Some(_), Some(_), _) => {
            return syn::Error::new_spanned(
                func.sig.fn_token,
                "#[webhook] accepts exactly one of `starts = \"workflow_name\"` or \
                 `signals = \"workflow_name\"`, not both",
            )
            .to_compile_error();
        }
        (None, None, _) => {
            return syn::Error::new_spanned(
                func.sig.fn_token,
                "#[webhook] requires exactly one of `starts = \"workflow_name\"` or \
                 `signals = \"workflow_name\"`",
            )
            .to_compile_error();
        }
        (Some(_), None, Some(_)) => {
            return syn::Error::new_spanned(
                func.sig.fn_token,
                "#[webhook] `signal_name` is only valid alongside `signals`, not `starts`",
            )
            .to_compile_error();
        }
        (Some(workflow), None, None) => {
            quote! { ::autumn_harvest::WebhookTarget::Starts { workflow: #workflow } }
        }
        (None, Some(_), None) => {
            return syn::Error::new_spanned(
                func.sig.fn_token,
                "#[webhook] `signals = \"workflow_name\"` requires `signal_name = \"...\"`",
            )
            .to_compile_error();
        }
        (None, Some(workflow), Some(signal_name)) => {
            quote! {
                ::autumn_harvest::WebhookTarget::SignalsWithStart {
                    workflow: #workflow,
                    signal_name: #signal_name,
                }
            }
        }
    };

    // ── Validation ────────────────────────────────────────────────────────────

    if func.sig.asyncness.is_some() {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[webhook] mapping functions are synchronous -- do I/O inside the target workflow, \
             not in the mapping function",
        )
        .to_compile_error();
    }

    if !first_param_is_webhook_ctx(&func.sig.inputs) {
        return syn::Error::new_spanned(
            &func.sig,
            "#[webhook] mapping functions must take `ctx: &WebhookCtx` as the first argument",
        )
        .to_compile_error();
    }

    if func.sig.inputs.iter().skip(1).count() != 1 {
        return syn::Error::new_spanned(
            &func.sig.inputs,
            "#[webhook] mapping functions must take exactly one typed payload parameter \
             after `ctx: &WebhookCtx`",
        )
        .to_compile_error();
    }

    if !returns_result(&func.sig.output) {
        return syn::Error::new_spanned(
            &func.sig.output,
            "#[webhook] mapping functions must return `Result<WorkflowId, E>`",
        )
        .to_compile_error();
    }

    // ── Companion generation ──────────────────────────────────────────────────

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_webhook_info_{fn_name}");
    let public_info_name = format_ident!("{fn_name}_info");

    let queue_expr = attrs.queue.as_ref().map_or_else(
        || quote! { ::std::option::Option::None },
        |q| quote! { ::std::option::Option::Some(#q) },
    );

    quote! {
        #func

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::WebhookTriggerInfo {
            fn __dispatch(
                ctx: &::autumn_harvest::WebhookCtx,
                payload: ::autumn_harvest::serde_json::Value,
            ) -> ::std::result::Result<
                ::autumn_harvest::WorkflowId,
                ::autumn_harvest::WebhookHandlerError,
            > {
                let typed = ::autumn_harvest::serde_json::from_value(payload).map_err(|e| {
                    ::autumn_harvest::WebhookHandlerError::Deserialize(e.to_string())
                })?;
                #fn_name(ctx, typed).map_err(|e| {
                    ::autumn_harvest::WebhookHandlerError::Rejected(e.to_string())
                })
            }

            ::autumn_harvest::WebhookTriggerInfo {
                name: #fn_name_str,
                module: module_path!(),
                path: #path,
                target: #target_expr,
                handler: __dispatch,
                queue: #queue_expr,
            }
        }

        /// Returns the [`::autumn_harvest::WebhookTriggerInfo`] for this webhook binding.
        pub fn #public_info_name() -> ::autumn_harvest::WebhookTriggerInfo {
            #companion_name()
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` when the first parameter matches `ctx: &WebhookCtx`.
fn first_param_is_webhook_ctx(
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
) -> bool {
    let Some(first) = inputs.first() else {
        return false;
    };
    let syn::FnArg::Typed(pt) = first else {
        return false;
    };
    let syn::Type::Reference(r) = &*pt.ty else {
        return false;
    };
    let syn::Type::Path(tp) = &*r.elem else {
        return false;
    };
    tp.path
        .segments
        .last()
        .is_some_and(|s| s.ident == "WebhookCtx")
}

fn returns_result(output: &syn::ReturnType) -> bool {
    let syn::ReturnType::Type(_, ty) = output else {
        return false;
    };
    let syn::Type::Path(type_path) = &**ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "Result")
}
