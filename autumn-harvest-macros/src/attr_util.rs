//! Small parsing helpers shared across the `#[workflow(...)]`, `#[update(...)]`,
//! and sibling attribute-macro argument parsers.

/// Parse a bare-flag-or-explicit-bool attribute value: `name` (bare, implies
/// `true`) or `name = true`/`name = false`.
///
/// Used by every `#[workflow(...)]`/`#[update(...)]` boolean attribute
/// (`mcp`, `allow_nondeterministic_apis`, ...) so the bare-vs-explicit
/// parsing rule lives in exactly one place.
pub fn parse_bool_flag(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<bool> {
    if meta.input.peek(syn::Token![=]) {
        let value: syn::LitBool = meta.value()?.parse()?;
        Ok(value.value)
    } else {
        Ok(true)
    }
}
