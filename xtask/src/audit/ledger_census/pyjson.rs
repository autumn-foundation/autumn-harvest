//! JSON text in the exact form that Python's `json.dumps` writes.
//!
//! `statements.json` is committed. The census wrote it with
//! `json.dumps(merged, indent=1, sort_keys=True)`, so the port writes the same
//! bytes. A rerun on an unchanged census then leaves no diff. The literal sets
//! of a statement are also sorted by their compact JSON text, so that form
//! must match too.

use std::fmt::Write as _;

use serde_json::Value;

/// Writes `value` as `json.dumps(value, sort_keys=True)` does.
///
/// The separators are `", "` and `": "`, and every non-ASCII character is escaped.
pub fn dumps_compact(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value, None, 0);
    out
}

/// Writes `value` as `json.dumps(value, indent=indent, sort_keys=True)` does.
pub fn dumps_indent(value: &Value, indent: usize) -> String {
    let mut out = String::new();
    write_value(&mut out, value, Some(indent), 0);
    out
}

fn newline(out: &mut String, indent: Option<usize>, level: usize) {
    if let Some(width) = indent {
        out.push('\n');
        out.push_str(&" ".repeat(width * level));
    }
}

fn write_value(out: &mut String, value: &Value, indent: Option<usize>, level: usize) {
    let item_sep = if indent.is_some() { "," } else { ", " };
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => write_str(out, s),
        Value::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, indent, level + 1);
                write_value(out, item, indent, level + 1);
            }
            newline(out, indent, level);
            out.push(']');
        }
        Value::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, indent, level + 1);
                write_str(out, key);
                out.push_str(": ");
                if let Some(item) = map.get(key) {
                    write_value(out, item, indent, level + 1);
                }
            }
            newline(out, indent, level);
            out.push('}');
        }
    }
}

/// Writes a string with Python's `ensure_ascii` escapes.
fn write_str(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(ch),
            _ => {
                let mut units = [0_u16; 2];
                for unit in ch.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_form_matches_python() {
        let value = json!({"2": "'A'", "1": "true"});
        assert_eq!(dumps_compact(&value), r#"{"1": "true", "2": "'A'"}"#);
        assert_eq!(dumps_compact(&json!({})), "{}");
    }

    #[test]
    fn indent_form_matches_python() {
        let value = json!({"c": {"k": {"types": {}, "literals": [{}]}}});
        let want = "{\n \"c\": {\n  \"k\": {\n   \"literals\": [\n    {}\n   ],\n   \"types\": {}\n  }\n }\n}";
        assert_eq!(dumps_indent(&value, 1), want);
    }

    #[test]
    fn non_ascii_text_is_escaped() {
        let want = "\"caf\\u00e9 \\ud83d\\ude00\\u007f\"";
        assert_eq!(dumps_compact(&json!("caf\u{e9} \u{1f600}\u{7f}")), want);
    }
}
