//! `#[update]` attribute macro for declarative workflow update handlers (issue #346).
//!
//! Generates a companion function
//! `__autumn_update_handler_info_{fn_name}() -> UpdateHandlerInfo`
//! alongside the user's async function.
//!
//! Attributes:
//! - `workflow = "name"` (required) — the workflow this handler belongs to.
//! - `validator = path::to::fn` (optional) — synchronous validator called before
//!   the update is admitted to history.
//!
//! The annotated function must:
//! - Be `async`
//! - Take `ctx: &WorkflowContext` as its first argument
//! - Return `Result<T, E>`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Expr, ItemFn, LitStr, parse::Parser as _};

struct UpdateAttrs {
    workflow: Option<String>,
    validator: Option<Expr>,
    /// Opt-in MCP tool exposure for this update (issue #597). Parsed from
    /// `#[update(workflow = "…", mcp)]` or `mcp = true`.
    mcp: bool,
    /// Optional human-readable description for interface discovery (issue #610).
    description: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<UpdateAttrs> {
    let mut result = UpdateAttrs {
        workflow: None,
        validator: None,
        mcp: false,
        description: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("workflow") {
            let value: LitStr = meta.value()?.parse()?;
            result.workflow = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("validator") {
            let value: Expr = meta.value()?.parse()?;
            result.validator = Some(value);
            Ok(())
        } else if meta.path.is_ident("mcp") {
            result.mcp = crate::attr_util::parse_bool_flag(&meta)?;
            Ok(())
        } else if meta.path.is_ident("description") {
            let value: LitStr = meta.value()?.parse()?;
            result.description = Some(value.value());
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected `workflow = \"name\"`, `validator = path::to::fn`, `mcp`, or `description = \"…\"`",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
pub fn update_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let func: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    let Some(workflow_name) = attrs.workflow else {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[update] requires `workflow = \"workflow_name\"`",
        )
        .to_compile_error();
    };

    // ── Validation ────────────────────────────────────────────────────────────

    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(func.sig.fn_token, "#[update] handlers must be async")
            .to_compile_error();
    }

    // First parameter must be ctx: &WorkflowContext.
    if !first_param_is_ctx(&func.sig.inputs) {
        return syn::Error::new_spanned(
            &func.sig,
            "#[update] handlers must take `ctx: &WorkflowContext` as the first argument",
        )
        .to_compile_error();
    }

    // Return type must be Result<T, E>.
    if !returns_result(&func.sig.output) {
        return syn::Error::new_spanned(
            &func.sig.output,
            "#[update] return type must be `Result<T, E>`",
        )
        .to_compile_error();
    }

    // ── Companion generation ──────────────────────────────────────────────────

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_update_handler_info_{fn_name}");
    let public_info_name = format_ident!("{fn_name}_info");

    let description_expr = attrs.description.as_deref().map_or_else(
        || quote! { ::std::option::Option::None },
        |s| quote! { ::std::option::Option::Some(#s) },
    );

    // Skip the leading ctx param when building type hints and dispatch args.
    let params: Vec<_> = func.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg
                && let syn::Pat::Ident(ident) = &*pt.pat
            {
                return Some(&ident.ident);
            }
            None
        })
        .collect();

    let input_type_hint = build_input_type_hint(&params);
    let output_type_hint = extract_ok_type_hint(&func.sig.output);

    let dispatch = build_update_dispatch(fn_name, &param_names);

    let (has_validator, validator_expr) = attrs.validator.as_ref().map_or_else(
        || (quote! { false }, quote! { None }),
        |validator_path| {
            (
                quote! { true },
                quote! { Some(#validator_path as ::autumn_harvest::UpdateValidatorFn) },
            )
        },
    );

    let mcp = attrs.mcp;
    let mcp_expr = quote! { #mcp };

    let parsed_path = match crate::parse_and_validate_workflow_path(
        &workflow_name,
        proc_macro2::Span::call_site(),
    ) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error(),
    };
    let workflow_simple_name = parsed_path.workflow_simple_name;
    let camel_wf = to_pascal_case(&workflow_simple_name);
    let stub_ident = format_ident!("{camel_wf}Stub");
    let method_name = format_ident!("update_{fn_name}");
    let method_name_uws = format_ident!("update_with_start_{fn_name}");
    let ok_type = extract_ok_type(&func.sig.output);

    let serialize_payload = if param_names.is_empty() {
        quote! { ::autumn_harvest::serde_json::Value::Null }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! { ::autumn_harvest::serde_json::to_value(&#name).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    } else {
        quote! { ::autumn_harvest::serde_json::to_value((#(&#param_names),*)).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    };

    let method_name_with_timeout = format_ident!("{}_with_timeout", method_name);

    let mod_name = format_ident!("__autumn_update_impl_{fn_name}");
    let path_tokens = parsed_path.path_tokens;
    let is_absolute = parsed_path.is_absolute;
    let leading_colon = if is_absolute {
        quote! { :: }
    } else {
        quote! {}
    };
    let nested_path_tokens = if is_absolute
        || parsed_path
            .original_module_parts
            .first()
            .is_some_and(|s| s == "crate")
    {
        path_tokens.clone()
    } else if parsed_path.original_module_parts.is_empty() {
        Vec::new()
    } else {
        let mut tokens = Vec::new();
        tokens.push(quote! { super });
        let first = parsed_path.original_module_parts.first().unwrap();
        if first == "self" {
            for p in parsed_path.original_module_parts.iter().skip(1) {
                let id = format_ident!("{}", p);
                tokens.push(quote! { #id });
            }
        } else {
            for p in &parsed_path.original_module_parts {
                let id = format_ident!("{}", p);
                tokens.push(quote! { #id });
            }
        }
        tokens
    };
    // The three typed-stub methods are identical whether `#stub_ident` is
    // implemented directly (same-module case) or inside a private mod that
    // re-imports it (nested-module case, needed so the `impl` sees the type
    // through a `use` rather than an unresolvable relative path). Splicing
    // one `method_defs` block into both `impl_block` arms keeps them in
    // lockstep by construction; hand-duplicating this block previously let
    // the chain-timeout-cap fields land in only one copy (commit 896978eb,
    // issue #617), caught by a compile error rather than a test. Mirrors the
    // same hoist already done for `#[signal]` in signal.rs.
    let method_defs = quote! {
        /// Execute this typed update handler in-process with a default 30-second timeout.
        pub async fn #method_name(
            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
            handle: &::autumn_harvest::WorkflowHandle,
            #(#params),*
        ) -> ::autumn_harvest::HarvestResult<#ok_type> {
            Self::#method_name_with_timeout(
                conn,
                handle,
                #(#param_names,)*
                ::std::time::Duration::from_secs(30)
            ).await
        }

        /// Execute this typed update handler in-process with a custom timeout.
        pub async fn #method_name_with_timeout(
            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
            handle: &::autumn_harvest::WorkflowHandle,
            #(#params,)*
            timeout: ::std::time::Duration,
        ) -> ::autumn_harvest::HarvestResult<#ok_type> {
            let args = #serialize_payload;
            let raw = handle.execute_update_in_process(
                conn,
                #workflow_simple_name,
                #fn_name_str,
                args,
                timeout
            ).await?;
            ::autumn_harvest::serde_json::from_value(raw)
                .map_err(::autumn_harvest::error::HarvestError::Serialization)
        }

        /// Atomically start-or-attach the workflow and admit this update.
        ///
        /// Returns [`UpdateWithStartOutcome`] describing whether a fresh
        /// execution was started and whether the update was admitted.
        /// Use the `update_id` in the outcome to poll for the result via the
        /// management API (`GET /workflows/{exec_id}/updates/{update_id}`) or
        /// the `poll_update_result` helper.
        ///
        /// `start_input` is the JSON-serialised workflow input. The update
        /// arguments are typed from the `#[update]` function signature.
        /// Use [`TypedUpdateWithStartOptions`] to control the reuse policy,
        /// idempotency key, queue, and other per-call settings.
        ///
        /// [`UpdateWithStartOutcome`]: ::autumn_harvest::UpdateWithStartOutcome
        /// [`TypedUpdateWithStartOptions`]: ::autumn_harvest::TypedUpdateWithStartOptions
        pub async fn #method_name_uws(
            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
            client: &::autumn_harvest::WorkflowHandleClient,
            workflow_id: impl Into<::std::string::String>,
            start_input: ::autumn_harvest::serde_json::Value,
            #(#params,)*
            opts: ::autumn_harvest::TypedUpdateWithStartOptions,
        ) -> ::autumn_harvest::HarvestResult<::autumn_harvest::UpdateWithStartOutcome>
        {
            let workflow_id = workflow_id.into();
            let update_args = #serialize_payload;
            // Issue #499: a debounced workflow cannot be started through the
            // typed client (it can't route to the debounce-key shard or admit
            // through the gate); debounce admission is HTTP-only. Reject early.
            if let ::std::option::Option::Some(debounce_policy) = Self::info().debounce {
                if ::autumn_harvest::debounce::resolve_debounce_key(
                    debounce_policy.key_expr,
                    &start_input,
                )
                .is_some()
                {
                    return ::std::result::Result::Err(
                        ::autumn_harvest::error::HarvestError::Config(::std::format!(
                            "workflow '{0}' has a debounce policy; debounced starts \
                             must use the HTTP start route POST /workflows/{0}/start \
                             (the typed client cannot express a deferred debounced start)",
                            #workflow_simple_name,
                        )),
                    );
                }
            }
            if let ::std::option::Option::Some(batch_policy) = Self::info().batch.as_ref() {
                if ::autumn_harvest::concurrency::resolve_concurrency_key(
                    &batch_policy.key_expr,
                    &start_input,
                )
                .is_some()
                {
                    return ::std::result::Result::Err(
                        ::autumn_harvest::error::HarvestError::Config(::std::format!(
                            "workflow '{0}' has an event batching policy; batched starts \
                             must use the HTTP start route POST /workflows/{0}/start \
                             (the typed client cannot express a deferred batched start)",
                            #workflow_simple_name,
                        )),
                    );
                }
            }
            let update_id = opts.idempotency_key.as_ref().map_or_else(
                ::autumn_harvest::types::UpdateId::new,
                |key| {
                    let ns = ::autumn_harvest::uuid::Uuid::parse_str(
                        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                    ).expect("static namespace UUID is valid");
                    ::autumn_harvest::types::UpdateId::from_uuid(
                        ::autumn_harvest::uuid::Uuid::new_v5(&ns, key.as_bytes())
                    )
                }
            );
            let exec_id = opts.exec_id.unwrap_or_else(|| {
                let shard = client.pick_shard_for_new_workflow(#workflow_simple_name, &workflow_id);
                ::autumn_harvest::types::ExecutionId::new_for_shard(shard)
            });
            let execution_timeout = match opts.execution_timeout {
                ::std::option::Option::Some(d) => ::std::option::Option::Some(
                    ::autumn_harvest::chrono::Duration::from_std(d)
                        .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                            "execution_timeout exceeds chrono duration range".to_string()
                        ))?
                ),
                ::std::option::Option::None => ::std::option::Option::None,
            };
            let max_execution_timeout_ceiling = match client.max_workflow_execution_timeout() {
                ::std::option::Option::Some(d) => ::std::option::Option::Some(
                    ::autumn_harvest::chrono::Duration::from_std(d)
                        .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                            "max_execution_timeout_ceiling exceeds chrono duration range".to_string()
                        ))?
                ),
                ::std::option::Option::None => ::std::option::Option::None,
            };
            let concurrency_key = Self::info().concurrency.and_then(|p|
                ::autumn_harvest::concurrency::resolve_concurrency_key(p.key_expr, &start_input)
            );
            let concurrency_limit = Self::info().concurrency.map(|p| p.limit);
            let params = ::autumn_harvest::UpdateWithStartParams {
                workflow_name: #workflow_simple_name,
                workflow_id: &workflow_id,
                exec_id,
                input: start_input,
                parent_id: opts.parent_id,
                queue_name: opts.queue_name.as_deref().unwrap_or("default"),
                execution_timeout,
                memo: opts.memo,
                search_attrs: opts.search_attrs,
                reuse_policy: opts.reuse_policy.unwrap_or(
                    ::autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate
                ),
                trace_context: opts.trace_context,
                max_execution_timeout_ceiling,
                // Chain-scoped lifetime cap (issue #617): the typed-stub
                // update-with-start does NOT thread the chain cap; it is
                // resolved on the HTTP update-with-start route and the
                // typed stub's own `start`/`start_with_options` path.
                chain_execution_timeout: ::std::option::Option::None,
                max_workflow_chain_timeout_ceiling: ::std::option::Option::None,
                concurrency_key,
                concurrency_limit,
                concurrency_on_conflict: Self::info()
                    .concurrency
                    .map_or(
                        ::autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                        |p| p.on_conflict,
                    ),
                update_id,
                update_name: #fn_name_str.to_string(),
                update_args,
                idempotency_key: opts.idempotency_key,
                max_workflow_input_bytes: client.max_workflow_input_bytes(::std::option::Option::None),
                owner: ::std::option::Option::None,
                runbook_url: ::std::option::Option::None,
                severity: ::std::option::Option::None,
                context_headers: opts.context_headers,
                sla: opts.sla.or_else(|| Self::info().sla).and_then(|d|
                    ::autumn_harvest::chrono::Duration::from_std(d).ok()
                ),
                workflow_retry_policy: Self::info().retry_policy
                    .and_then(|p| ::autumn_harvest::serde_json::to_value(&p).ok()),
                max_workflow_attempts_ceiling: client.max_workflow_attempts(),
                // Typed stubs already reject debounced workflows up front.
                reject_fresh_if_debounced: false,
            };
            let _ = client;
            ::autumn_harvest::update_with_start_workflow_execution(conn, params).await
        }
    };

    let impl_block = if path_tokens.is_empty() {
        quote! {
            ::autumn_harvest::cfg_db! {
                impl #stub_ident {
                    #method_defs
                }
            }
        }
    } else {
        quote! {
            ::autumn_harvest::cfg_db! {
                mod #mod_name {
                    use super::*;
                    use #leading_colon #(#nested_path_tokens::)*#stub_ident;
                    impl #stub_ident {
                        #method_defs
                    }
                }
            }
        }
    };

    quote! {
        #func

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::UpdateHandlerInfo {
            fn __dispatch(
                ctx: ::std::sync::Arc<::autumn_harvest::WorkflowContext>,
                args: ::autumn_harvest::serde_json::Value,
            ) -> ::std::pin::Pin<::std::boxed::Box<
                dyn ::std::future::Future<
                    Output = Result<::autumn_harvest::serde_json::Value, String>,
                > + Send,
            >> {
                ::std::boxed::Box::pin(async move {
                    #dispatch
                })
            }

            ::autumn_harvest::UpdateHandlerInfo {
                name: #fn_name_str,
                workflow: #workflow_simple_name,
                module: module_path!(),
                input_type_hint: #input_type_hint,
                output_type_hint: #output_type_hint,
                has_validator: #has_validator,
                handler: __dispatch,
                validator: #validator_expr,
                mcp: #mcp_expr,
                description: #description_expr,
                arg_schema: ::std::option::Option::None,
                response_schema: ::std::option::Option::None,
            }
        }

        /// Returns the [`::autumn_harvest::UpdateHandlerInfo`] for this update.
        ///
        /// Chain schema builders at registration, e.g.
        /// `.updates(vec![#public_info_name().with_schemas::<Arg, Resp>()])`.
        pub fn #public_info_name() -> ::autumn_harvest::UpdateHandlerInfo {
            #companion_name()
        }

        #impl_block
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` when the first parameter matches `ctx: &WorkflowContext`.
fn first_param_is_ctx(inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>) -> bool {
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
        .is_some_and(|s| s.ident == "WorkflowContext")
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

fn build_update_dispatch(fn_name: &syn::Ident, param_names: &[&syn::Ident]) -> TokenStream {
    if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx.as_ref()).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(args)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(ctx.as_ref(), #name).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else {
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = param_names.to_owned();
        quote! {
            let __args: ::autumn_harvest::serde_json::Value = args;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(__args[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(ctx.as_ref(), #(#names),*).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    }
}

fn build_input_type_hint(params: &[&syn::FnArg]) -> String {
    if params.is_empty() {
        return "()".to_string();
    }
    if params.len() == 1
        && let syn::FnArg::Typed(pt) = params[0]
    {
        return type_name_hint(&pt.ty);
    }
    let parts: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg {
                Some(type_name_hint(&pt.ty))
            } else {
                None
            }
        })
        .collect();
    format!("({})", parts.join(", "))
}

fn extract_ok_type_hint(output: &syn::ReturnType) -> String {
    crate::extract_ok_type_hint(output)
}

fn type_name_hint(ty: &syn::Type) -> String {
    crate::type_name_hint(ty)
}

fn to_pascal_case(s: &str) -> String {
    crate::to_pascal_case(s)
}

fn extract_ok_type(output: &syn::ReturnType) -> syn::Type {
    crate::extract_ok_type(output)
}

// ── Characterization tests ──────────────────────────────────────────────────
//
// `update_macro` generates the same three typed-stub methods twice: once for
// the same-module case (`path_tokens.is_empty()`) and once wrapped in a
// private `mod` for the nested-module case. The two copies are hand-kept in
// sync; commit 896978eb (issue #617) shipped a diff that updated the first
// copy's `UpdateWithStartParams` literal and initially missed the second,
// caught only by `cargo clippy --all-features --tests` failing to compile
// (a struct-literal missing-field error), not by any test. These tests pin
// the two branches to token-identical method bodies (module wrapper and doc
// comments aside) so a future one-sided edit fails a fast `cargo test
// -p autumn-harvest-macros` instead of waiting for a full clippy run.
#[cfg(test)]
mod same_module_vs_nested_module_parity_tests {
    use super::update_macro;
    use quote::quote;

    /// Removes `# [doc = r"..."]` attribute tokens (the `///` doc comments on
    /// `#method_name_uws` are deliberately fuller in the same-module branch;
    /// everything else must match). `quote!`'s fallback (non-bridged, i.e.
    /// `cargo test`) `Display` renders doc comments as *raw* string literals
    /// (`r"..."`), and the doc text can itself contain `]` (e.g. markdown
    /// links like `[UpdateWithStartOutcome]`), so the closing quote is found
    /// by scanning the string content rather than by a naive search for the
    /// next `]`.
    fn strip_docs(s: &str) -> String {
        const PREFIX: &str = "# [doc = r\"";
        let mut out = String::new();
        let mut rest = s;
        loop {
            match rest.find(PREFIX) {
                None => {
                    out.push_str(rest);
                    break;
                }
                Some(start) => {
                    out.push_str(&rest[..start]);
                    let after_prefix = &rest[start + PREFIX.len()..];
                    // Raw strings have no escapes: the next `"` always closes it.
                    let quote_end = after_prefix
                        .find('"')
                        .expect("unterminated raw string in doc attribute");
                    let after_quote = &after_prefix[quote_end + 1..];
                    let close = after_quote
                        .find(']')
                        .expect("expected ']' closing the doc attribute");
                    rest = &after_quote[close + 1..];
                }
            }
        }
        out
    }

    /// Collapses every run of whitespace to a single space. `quote!`'s
    /// fallback `Display` reproduces string-literal *source spelling*
    /// verbatim rather than the resolved value (verified empirically: a
    /// `"a \<newline>   b"` continuation literal round-trips with the
    /// original newline and indentation still embedded), so a debounce/batch
    /// rejection message written at two different nesting depths in the two
    /// branches otherwise looks like a content difference when it is really
    /// only a difference in how many leading spaces preceded it in
    /// `update.rs`'s own source -- irrelevant once real `rustc` resolves the
    /// `\`-continuation the same way for both. Real content differences
    /// (an added/removed field, a different call) still show up as
    /// different words, which whitespace collapsing does not hide.
    fn normalize_whitespace(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// Extracts the contents of the (single, outermost) `impl #stub_ident {
    /// ... }` block via balanced-brace scanning over the `Display`ed
    /// `TokenStream`, which normalises whitespace deterministically.
    fn extract_impl_body(full: &str, stub_ident: &str) -> String {
        let marker = format!("impl {stub_ident} {{");
        let start = full
            .find(&marker)
            .unwrap_or_else(|| panic!("no `{marker}` in generated output:\n{full}"))
            + marker.len();
        let mut depth = 1i32;
        let bytes = full.as_bytes();
        let mut i = start;
        while i < bytes.len() && depth > 0 {
            match bytes[i] as char {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        normalize_whitespace(&strip_docs(full[start..i - 1].trim()))
    }

    fn generate(workflow_path: &str) -> String {
        let attr = quote! { workflow = #workflow_path };
        let item = quote! {
            async fn my_update(ctx: &WorkflowContext, n: u32) -> Result<u32, String> {
                Ok(n)
            }
        };
        update_macro(attr, item).to_string()
    }

    #[test]
    fn same_module_and_nested_module_branches_generate_identical_impl_bodies() {
        let same_module = generate("MyWorkflow");
        let nested = generate("some_mod::MyWorkflow");

        let same_module_body = extract_impl_body(&same_module, "MyWorkflowStub");
        let nested_body = extract_impl_body(&nested, "MyWorkflowStub");

        assert_eq!(
            same_module_body, nested_body,
            "the same-module and nested-module branches of `update_macro` must \
             generate identical method bodies (module wrapper and doc comments \
             aside) -- a divergence here is exactly the missed-fix class fixed \
             for `chain_execution_timeout`/`max_workflow_chain_timeout_ceiling` \
             in commit 896978eb (issue #617)"
        );
        // Sanity: make sure the extraction actually found real content, not
        // two empty strings that would trivially "match".
        assert!(same_module_body.contains("update_with_start_workflow_execution"));
    }
}
