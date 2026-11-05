//! The `--json` report, in the exact format of Python `json.dumps`.
//!
//! The format is Python `json.dumps(..., indent=2)`, with its default
//! `ensure_ascii`. Tools that read the old output keep working, and a
//! byte comparison of old and new output holds.

use std::fmt::Write as _;

use super::ratchet::Tally;
use super::rules::Finding;

/// A JSON value with ordered keys.
///
/// The Python report can hold `null` for the scope. The scope is always a
/// list on each path that reaches the report, so this type has no null.
enum Value {
    Number(usize),
    Text(String),
    List(Vec<Self>),
    Object(Vec<(String, Self)>),
}

/// A string literal with Python `ensure_ascii` escapes.
fn quote(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

/// Writes `value` with an indent of two spaces per level.
fn write_value(value: &Value, level: usize, out: &mut String) {
    let pad = "  ".repeat(level + 1);
    let close = "  ".repeat(level);
    match value {
        Value::Number(number) => {
            let _ = write!(out, "{number}");
        }
        Value::Text(text) => quote(text, out),
        Value::List(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push_str("[\n");
            for (index, item) in items.iter().enumerate() {
                out.push_str(&pad);
                write_value(item, level + 1, out);
                if index + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&close);
            out.push(']');
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push_str("{\n");
            for (index, (key, item)) in entries.iter().enumerate() {
                out.push_str(&pad);
                quote(key, out);
                out.push_str(": ");
                write_value(item, level + 1, out);
                if index + 1 < entries.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&close);
            out.push('}');
        }
    }
}

fn texts<S: AsRef<str>>(items: &[S]) -> Value {
    Value::List(
        items
            .iter()
            .map(|item| Value::Text(item.as_ref().to_string()))
            .collect(),
    )
}

/// The whole `--json` report.
pub fn report(
    findings: &[Finding],
    counts: &Tally,
    scope: &[&String],
    merge_base: &str,
    regressions: &[String],
) -> String {
    let findings = Value::List(
        findings
            .iter()
            .map(|finding| {
                Value::Object(vec![
                    ("rule".to_string(), Value::Text(finding.rule.to_string())),
                    ("path".to_string(), Value::Text(finding.path.clone())),
                    ("line".to_string(), Value::Number(finding.line)),
                    ("text".to_string(), Value::Text(finding.text.clone())),
                ])
            })
            .collect(),
    );
    let counts = Value::Object(
        counts
            .iter()
            .map(|(rule, files)| {
                let files = files
                    .iter()
                    .map(|(path, prints)| (path.clone(), texts(prints)))
                    .collect();
                (rule.clone(), Value::Object(files))
            })
            .collect(),
    );
    let root = Value::Object(vec![
        ("findings".to_string(), findings),
        ("counts".to_string(), counts),
        ("scope".to_string(), texts(scope)),
        (
            "merge_base".to_string(),
            Value::Text(merge_base.to_string()),
        ),
        ("tier_b_regressions".to_string(), texts(regressions)),
    ]);
    let mut out = String::new();
    write_value(&root, 0, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_match_python() {
        let mut out = String::new();
        quote("a\"\\\n\u{e9}\u{1f600}", &mut out);
        assert_eq!(out, "\"a\\\"\\\\\\n\\u00e9\\ud83d\\ude00\"");
    }

    #[test]
    fn empty_containers_stay_on_one_line() {
        let text = report(&[], &Tally::new(), &[], "", &[]);
        assert_eq!(
            text,
            "{\n  \"findings\": [],\n  \"counts\": {},\n  \"scope\": [],\n  \"merge_base\": \"\",\n  \"tier_b_regressions\": []\n}"
        );
    }
}
