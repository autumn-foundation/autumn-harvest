//! Struct layouts: the serde attributes and fields that checks 2, 3, 6
//! and 7 read.

use std::collections::{BTreeSet, HashSet};

use super::aliases::{canonical_paths, canonical_type};
use super::mask::code_only;
use super::py::{self, paren};
use super::state::{self, StructBlock};
use super::syntax::split_expression;
use super::tables::{FIELD_KEEPING, KNOWN_DESERIALIZERS, LAYOUT_KEEPING, QUOTED, wire_types};

/// One serde item: its key, its form (`=`, `(` or none) and its value.
///
/// The value is the literal after `=` without its quotes, or `None`.
pub type SerdeItem = (String, Option<String>, Option<String>);

/// One field of a struct layout: its name, type, items and unread alias.
pub type LayoutField = (String, String, Vec<SerdeItem>, Option<String>);

/// One field serde reads: wire name, type, whether it is mandatory, and
/// spellings.
pub type Field = (String, String, bool, Vec<String>);

/// The spellings of each field serde will accept from the wire.
pub fn accepted_fields(block: &StructBlock) -> Vec<Vec<String>> {
    struct_fields(block)
        .into_iter()
        .map(|(_, _, _, spellings)| spellings)
        .collect()
}

/// The spellings of each field a caller must send, given serde's rules.
pub fn mandatory_fields(block: &StructBlock) -> Vec<Vec<String>> {
    struct_fields(block)
        .into_iter()
        .filter(|(_, _, mandatory, _)| *mandatory)
        .map(|(_, _, _, spellings)| spellings)
        .collect()
}

/// The first spelling of a field that the contract documents, if any.
pub fn documented_as<'a>(
    spellings: &'a [String],
    documented: &dyn Fn(&str) -> bool,
) -> Option<&'a str> {
    spellings
        .iter()
        .map(String::as_str)
        .find(|name| documented(name))
}

/// The text without its comment lines, so a doc comment is not code.
pub fn without_comment_lines(text: &str) -> String {
    text.split('\n')
        .filter(|line| !line.trim().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every item of every `#[serde(..)]` attribute in `attributes`.
///
/// Each attribute is read bracket-balanced, so one that rustfmt splits over
/// several lines is read whole. Its items are split at top-level commas by
/// `split_expression`. Quoted values are set aside first. So a key word
/// inside a value, as in `alias = "skip"`, is never read as a key. Only keys
/// are compared. Values are literals.
pub fn serde_items(attributes: &str) -> Vec<SerdeItem> {
    let mut items = Vec::new();
    for found in py::finditer(r"#\s*\[\s*serde\s*\(", attributes) {
        let group = paren(&attributes[found.end() - 1..]);
        let inner = py::sl(group, 1, group.len().saturating_sub(1));
        let mut values: Vec<String> = Vec::new();
        let parked = py::sub_with(QUOTED, inner, |literal| {
            let text = literal.whole();
            values.push(py::sl(text, 1, text.len().saturating_sub(1)).to_string());
            format!("\"{}\"", values.len() - 1)
        });
        for item in split_expression(&parked, &[","], false, None) {
            let Some(key) = py::rmatch(r#"\s*([a-z_]+)\s*(\(|=)?\s*(?:"(\d+)")?"#, &item) else {
                continue;
            };
            let value = key
                .get(3)
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| values.get(index).cloned());
            items.push((key.g(1).to_string(), key.get(2).map(str::to_string), value));
        }
    }
    items
}

/// The container serde items, and each field's name, type, items and alias.
///
/// Comments are removed first. An attribute or a type that rustfmt splits
/// over several lines stays open until its brackets balance. Each field type
/// is read through `resolve_field_type`. The last value is the alias it
/// could not read, or `None`. Every serde reader in the audit reads a struct
/// through this one parse.
pub fn struct_layout(block: &StructBlock) -> (Vec<SerdeItem>, Vec<LayoutField>) {
    let text = code_only(&block.text, false);
    let opener = text.find('{').unwrap_or(0);
    let container = serde_items(&canonical_paths(&without_comment_lines(&text[..opener])));
    let mut fields = Vec::new();
    let mut attributes: Vec<String> = Vec::new();
    let mut open_attribute = String::new();
    let mut open_field = String::new();
    for line in text[opener..].split('\n') {
        let mut line_text = py::sub(r"/\*.*?\*/", "", line).trim().to_string();
        if !open_attribute.is_empty() || line_text.starts_with("#[") {
            open_attribute.push(' ');
            open_attribute.push_str(&line_text);
            if py::count_byte(&open_attribute, b'[') <= py::count_byte(&open_attribute, b']') {
                attributes.push(open_attribute.trim().to_string());
                open_attribute.clear();
            }
            continue;
        }
        line_text = py::sub(r"//.*$", "", &line_text).trim().to_string();
        if !open_field.is_empty() {
            line_text = format!("{open_field} {line_text}");
            open_field.clear();
        }
        if line_text.is_empty() || line_text == "{" || line_text == "}" {
            continue;
        }
        let opens = py::count_byte(&line_text, b'<') + py::count_byte(&line_text, b'(');
        let closes = py::count_byte(&line_text, b'>') + py::count_byte(&line_text, b')');
        if opens > closes {
            open_field = line_text;
            continue;
        }
        let tidied = py::sub(
            r"\s*,\s*([>)])",
            r"\1",
            &py::sub(r"([<(])\s+", r"\1", &line_text),
        );
        if let Some(field) = py::rmatch(
            r"(?:pub(?:\([^)]*\))?\s+)?(?:r#)?([a-z_0-9]+)\s*:\s*(.+?),?$",
            &tidied,
        ) {
            let (resolved, unread) = canonical_type(field.g(2), Some(&block.site));
            let items = serde_items(&canonical_paths(&attributes.join(" ")));
            fields.push((field.g(1).to_string(), resolved, items, unread));
        }
        attributes.clear();
    }
    (container, fields)
}

/// Serde attributes in a struct that change its layout in ways not read.
///
/// A container attribute passes only when `LAYOUT_KEEPING` names it, and a
/// container `rename` passes only in its `rename = ".."` form. Any other,
/// such as `transparent`, `untagged`, `tag`, `from` or `rename_all`, is
/// reported, so the audit fails closed. Field attributes are read by
/// `unreadable_field_serde`.
pub fn unreadable_serde(block: &StructBlock) -> Vec<String> {
    let (container, fields) = struct_layout(block);
    let mut found: BTreeSet<String> = BTreeSet::new();
    for (key, form, _) in &container {
        let form = form.as_deref();
        if !LAYOUT_KEEPING.contains(&key.as_str()) || (key == "rename" && form != Some("=")) {
            found.insert(if form == Some("(") {
                format!("{key}(..)")
            } else {
                key.clone()
            });
        }
    }
    for (_, declared_type, items, unread) in &fields {
        found.extend(unreadable_field_serde(items, declared_type));
        if let Some(unread) = unread {
            found.insert(unread.clone());
        }
    }
    found.into_iter().collect()
}

/// The serde attributes of one field that the audit cannot read.
///
/// A key passes when `FIELD_KEEPING` names it, and `rename` only in its
/// `rename = ".."` form. `deserialize_with` passes only for a deserializer
/// in `KNOWN_DESERIALIZERS`, when the field has the type that entry accepts
/// and the audited file defines the function with an `Option<Option<T>>`
/// result.
pub fn unreadable_field_serde(items: &[SerdeItem], declared_type: &str) -> HashSet<String> {
    let mut found = HashSet::new();
    for (name, form, value) in items {
        let form = form.as_deref();
        if name == "deserialize_with" && modeled_deserializer(value.as_deref(), declared_type) {
            continue;
        }
        if FIELD_KEEPING.contains(&name.as_str()) && !(name == "rename" && form != Some("=")) {
            continue;
        }
        found.insert(if form == Some("(") {
            format!("{name}(..)")
        } else {
            name.clone()
        });
    }
    found
}

/// Whether `KNOWN_DESERIALIZERS` models `function` for a field of this type.
pub fn modeled_deserializer(function: Option<&str>, declared_type: &str) -> bool {
    let function = function.unwrap_or("");
    let Some((_, shape)) = KNOWN_DESERIALIZERS
        .iter()
        .find(|(name, _)| *name == function)
    else {
        return false;
    };
    if !py::full(shape, declared_type.trim()) {
        return false;
    }
    let defined = format!(
        r"\bfn\s+{}\s*<[^(]*>\s*\([^)]*\)\s*->\s*Result<\s*Option<\s*Option<",
        py::esc(function)
    );
    py::found(&defined, &state::source())
}

/// `(name, type, mandatory, spellings)` for each field serde reads.
///
/// `name` is the wire name, after a serde `rename = ".."`. `spellings`
/// holds that name, then each serde `alias`. A container
/// `#[serde(default)]` makes every field optional. The serde items come
/// from `struct_layout`, so only keys are compared.
pub fn struct_fields(block: &StructBlock) -> Vec<Field> {
    let (container, layout) = struct_layout(block);
    let all_default = container.iter().any(|(key, _, _)| key == "default");
    let mut fields = Vec::new();
    for (name, declared_type, items, _) in layout {
        let keys: HashSet<&str> = items.iter().map(|(key, _, _)| key.as_str()).collect();
        if keys.contains("skip") || keys.contains("skip_deserializing") {
            continue;
        }
        let optional =
            all_default || keys.contains("default") || declared_type.starts_with("Option<");
        let renamed: Vec<&Option<String>> = items
            .iter()
            .filter(|(key, form, _)| key == "rename" && form.as_deref() == Some("="))
            .map(|(_, _, value)| value)
            .collect();
        let wire = match renamed.first() {
            Some(Some(value)) => value.clone(),
            _ => name,
        };
        let mut spellings = vec![wire.clone()];
        spellings.extend(
            items
                .iter()
                .filter(|(key, _, _)| key == "alias")
                .filter_map(|(_, _, value)| value.clone()),
        );
        fields.push((wire, declared_type, !optional, spellings));
    }
    fields
}

/// The `OpenAPI` type of a Rust field type, unwrapping one `Option`.
pub fn wire_type(declared_type: &str) -> Option<&'static str> {
    let inner = py::fullmatch(r"Option<\s*(.+?)\s*>", declared_type);
    wire_types(inner.as_ref().map_or(declared_type, |inner| inner.g(1)))
}
