//! Python `repr` and `str` forms for gate messages.
//!
//! The finding text of the gate came from Python f-strings. A list renders as
//! `['a', 'b']`, and a bad driver renders with `!r`. These helpers keep that
//! text byte-identical, so a CI log reads the same after the port.

use std::fmt::Write as _;

/// Returns the Python `repr` of a string.
///
/// Python picks single quotes, unless the text holds a single quote and no
/// double quote.
pub fn str_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c if c.is_control() => {
                let code = c as u32;
                if code <= 0xff {
                    let _ = write!(out, "\\x{code:02x}");
                } else {
                    let _ = write!(out, "\\u{code:04x}");
                }
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Returns the Python `repr` of a list of strings.
pub fn list_repr<S: AsRef<str>>(items: &[S]) -> String {
    let parts: Vec<String> = items.iter().map(|s| str_repr(s.as_ref())).collect();
    format!("[{}]", parts.join(", "))
}

/// Returns the Python `repr` of a value that `tomllib` parsed.
///
/// A TOML date or time renders as its TOML text. Python renders a
/// `datetime` object there, and no profile holds one.
pub fn toml_repr(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => str_repr(s),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => float_repr(*f),
        toml::Value::Boolean(b) => if *b { "True" } else { "False" }.to_string(),
        toml::Value::Datetime(d) => d.to_string(),
        toml::Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(toml_repr).collect();
            format!("[{}]", parts.join(", "))
        }
        toml::Value::Table(table) => {
            let parts: Vec<String> = table
                .iter()
                .map(|(k, v)| format!("{}: {}", str_repr(k), toml_repr(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

/// Returns the Python `repr` of an optional value, with `None` for a gap.
pub fn opt_toml_repr(value: Option<&toml::Value>) -> String {
    value.map_or_else(|| "None".to_string(), toml_repr)
}

/// Returns the Python `str` of a value: a string is bare, and the rest is `repr`.
pub fn toml_str(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => s.clone(),
        other => toml_repr(other),
    }
}

/// Returns the Python `repr` of a float, for the common shapes.
fn float_repr(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    let text = format!("{value:?}");
    if text.contains('e') {
        let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
        let (sign, digits) = exponent
            .strip_prefix('-')
            .map_or(("+", exponent), |rest| ("-", rest));
        let mantissa = mantissa.strip_suffix(".0").unwrap_or(mantissa);
        return format!("{mantissa}e{sign}{digits:0>2}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_takes_python_quotes() {
        assert_eq!(str_repr("often"), "'often'");
        assert_eq!(str_repr("it's"), "\"it's\"");
        assert_eq!(str_repr("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(str_repr("tab\there"), "'tab\\there'");
    }

    #[test]
    fn a_string_escapes_newline_cr_and_a_bare_backslash() {
        assert_eq!(str_repr("a\nb"), "'a\\nb'");
        assert_eq!(str_repr("a\rb"), "'a\\rb'");
        assert_eq!(str_repr("a\\b"), "'a\\\\b'");
    }

    #[test]
    fn a_list_renders_like_python() {
        assert_eq!(list_repr(&["extra"]), "['extra']");
        assert_eq!(list_repr::<&str>(&[]), "[]");
        assert_eq!(list_repr(&["a", "b"]), "['a', 'b']");
    }

    #[test]
    fn a_toml_value_renders_like_python() {
        assert_eq!(toml_repr(&toml::Value::Integer(3)), "3");
        assert_eq!(toml_repr(&toml::Value::Boolean(true)), "True");
        assert_eq!(toml_repr(&toml::Value::Float(1.5)), "1.5");
        assert_eq!(toml_repr(&toml::Value::Float(1e20)), "1e+20");
        assert_eq!(opt_toml_repr(None), "None");
        let list = toml::Value::Array(vec![toml::Value::String("x".into())]);
        assert_eq!(toml_repr(&list), "['x']");
        assert_eq!(toml_str(&toml::Value::String("p.md".into())), "p.md");
    }
}
