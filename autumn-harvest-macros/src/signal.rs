//! `#[signal]` attribute macro for declarative workflow signal handlers.
//!
//! Generates a typed `signal_[signal_name]` method on the sibling `[WorkflowName]Stub` struct,
//! enabling type-safe external signal emission.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

struct SignalAttrs {
    workflow: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<SignalAttrs> {
    let mut result = SignalAttrs { workflow: None };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("workflow") {
            let value: LitStr = meta.value()?.parse()?;
            result.workflow = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("unsupported attribute: expected `workflow = \"workflow_name\"`"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

#[allow(clippy::too_many_lines)]
pub fn signal_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
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
            "#[signal] requires `workflow = \"workflow_name\"`",
        )
        .to_compile_error();
    };

    // First parameter must be ctx: &WorkflowContext.
    if !first_param_is_ctx(&func.sig.inputs) {
        return syn::Error::new_spanned(
            &func.sig,
            "#[signal] handlers must take `ctx: &WorkflowContext` as the first argument",
        )
        .to_compile_error();
    }

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let method_name = format_ident!("signal_{fn_name}");

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

    // Skip the leading ctx param when building signal args.
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

    let serialize_payload = if param_names.is_empty() {
        quote! { ::autumn_harvest::serde_json::Value::Null }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! { ::autumn_harvest::serde_json::to_value(&#name).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    } else {
        quote! { ::autumn_harvest::serde_json::to_value((#(&#param_names),*)).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    };

    let mod_name = format_ident!("__autumn_signal_impl_{fn_name}");
    let path_tokens = parsed_path.path_tokens;
    let is_absolute = parsed_path.is_absolute;
    let leading_colon = if is_absolute {
        quote! { :: }
    } else {
        quote! {}
    };
    let impl_block = if path_tokens.is_empty() {
        quote! {
            ::autumn_harvest::cfg_db! {
                impl #stub_ident {
                    /// Send a type-safe signal to this workflow execution.
                    pub async fn #method_name(
                        conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                        handle: &::autumn_harvest::WorkflowHandle,
                        #(#params),*
                    ) -> ::autumn_harvest::HarvestResult<()> {
                        let payload = #serialize_payload;
                        ::autumn_harvest::signal::send_signal(
                            conn,
                            handle.exec_id(),
                            #fn_name_str,
                            payload,
                        )
                        .await
                    }
                }
            }
        }
    } else {
        quote! {
            ::autumn_harvest::cfg_db! {
                mod #mod_name {
                    use super::*;
                    use #leading_colon #(#path_tokens::)*#stub_ident;
                    impl #stub_ident {
                        /// Send a type-safe signal to this workflow execution.
                        pub async fn #method_name(
                            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                            handle: &::autumn_harvest::WorkflowHandle,
                            #(#params),*
                        ) -> ::autumn_harvest::HarvestResult<()> {
                            let payload = #serialize_payload;
                            ::autumn_harvest::signal::send_signal(
                                conn,
                                handle.exec_id(),
                                #fn_name_str,
                                payload,
                            )
                            .await
                        }
                    }
                }
            }
        }
    };

    quote! {
        #func

        #impl_block
    }
}

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

#[allow(clippy::option_if_let_else)]
fn to_pascal_case(s: &str) -> String {
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
