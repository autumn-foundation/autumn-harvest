use proc_macro::TokenStream;

mod activity;
mod collect;
mod workflow;

#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    workflow::workflow_macro(attr.into(), item.into()).into()
}

#[proc_macro_attribute]
pub fn activity(attr: TokenStream, item: TokenStream) -> TokenStream {
    activity::activity_macro(attr.into(), item.into()).into()
}

#[proc_macro]
pub fn workflows(input: TokenStream) -> TokenStream {
    collect::workflows_macro(input.into()).into()
}

#[proc_macro]
pub fn activities(input: TokenStream) -> TokenStream {
    collect::activities_macro(input.into()).into()
}
