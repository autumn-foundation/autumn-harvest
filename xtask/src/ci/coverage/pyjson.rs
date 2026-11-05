//! JSON and number text that matches the Python standard library.
//!
//! The summary and the baseline must keep the bytes that `coverage.py`
//! wrote. So this module keeps the key order of an object, keeps an integer
//! apart from a float, and prints numbers as Python prints them.
//! `serde_json` sorts keys unless its `preserve_order` feature is on. That
//! feature would change the maps of every crate in the workspace build.

use std::fmt::Write as _;

use smop::prelude::*;

/// A JSON value, as `json.loads` returns it.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `null`, Python `None`.
    Null,
    /// `true` or `false`.
    Bool(bool),
    /// A number with no fraction and no exponent.
    Int(i128),
    /// Any other number, and `NaN` or `Infinity`.
    Float(f64),
    /// A string.
    Str(String),
    /// An array.
    Array(Vec<Self>),
    /// An object, in key order.
    Object(Vec<(String, Self)>),
}

impl Value {
    /// The value at `key` of an object, like `dict.get`.
    pub fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Object(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// The value at `key`, or an error like a Python `KeyError`.
    pub fn index(&self, key: &str) -> Result<&Self> {
        match self {
            Self::Object(_) => self.get(key).ok_or_else(|| anyhow!("KeyError: {key:?}")),
            _ => bail!("TypeError: cannot read {key:?} of a non-object"),
        }
    }

    /// Sets `key` of an object, in place or at the end, like a `dict`.
    pub fn set(&mut self, key: &str, value: Self) -> Result<()> {
        let Self::Object(items) = self else {
            bail!("TypeError: cannot set {key:?} of a non-object");
        };
        if let Some(slot) = items.iter_mut().find(|(k, _)| k == key) {
            slot.1 = value;
        } else {
            items.push((key.to_string(), value));
        }
        Ok(())
    }

    /// The items of an object, like `dict.items`.
    pub fn items(&self) -> Result<&[(String, Self)]> {
        match self {
            Self::Object(items) => Ok(items),
            _ => bail!("AttributeError: the value has no items"),
        }
    }

    /// The items of an object, for change in place.
    pub fn items_mut(&mut self) -> Result<&mut Vec<(String, Self)>> {
        match self {
            Self::Object(items) => Ok(items),
            _ => bail!("AttributeError: the value has no items"),
        }
    }

    /// True for `null`, like `is None`.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The truth value that Python gives this value.
    pub fn truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(b) => *b,
            Self::Int(i) => *i != 0,
            Self::Float(f) => *f != 0.0,
            Self::Str(s) => !s.is_empty(),
            Self::Array(a) => !a.is_empty(),
            Self::Object(o) => !o.is_empty(),
        }
    }

    /// The value as a number, for arithmetic and comparison.
    #[allow(clippy::cast_precision_loss)]
    pub fn number(&self) -> Result<f64> {
        match self {
            Self::Bool(b) => Ok(f64::from(u8::from(*b))),
            Self::Int(i) => Ok(*i as f64),
            Self::Float(f) => Ok(*f),
            _ => bail!("TypeError: {} is not a number", self.py_str()),
        }
    }

    /// The text of `str(value)` in Python.
    ///
    /// A container prints as compact JSON, which is close to the Python text.
    pub fn py_str(&self) -> String {
        match self {
            Self::Null => "None".to_string(),
            Self::Bool(true) => "True".to_string(),
            Self::Bool(false) => "False".to_string(),
            Self::Int(i) => i.to_string(),
            Self::Float(f) => float_repr(*f),
            Self::Str(s) => s.clone(),
            Self::Array(_) | Self::Object(_) => dumps_compact(self),
        }
    }
}

/// The text of `repr(x)` for a Python float.
///
/// Python prints the shortest digits that read back as `x`. It uses the
/// fixed form when the decimal point position is in -3..=16. Otherwise it
/// uses the exponent form, with a sign and at least two exponent digits.
pub fn float_repr(x: f64) -> String {
    if x.is_nan() {
        return "nan".to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // `{:e}` prints the shortest digits, for example `-1.2345e-5`.
    let text = format!("{x:e}");
    let (sign, text) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |rest| ("-", rest));
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let exponent: i64 = exponent.parse().unwrap_or(0);
    let point = exponent + 1;
    let len = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    let body = if point > -4 && point <= 16 {
        if point <= 0 {
            let zeros = "0".repeat(usize::try_from(-point).unwrap_or(0));
            format!("0.{zeros}{digits}")
        } else if point >= len {
            let zeros = "0".repeat(usize::try_from(point - len).unwrap_or(0));
            format!("{digits}{zeros}.0")
        } else {
            let split = usize::try_from(point).unwrap_or(0);
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let exp_sign = if exponent < 0 { '-' } else { '+' };
        format!("{first}{rest}e{exp_sign}{:02}", exponent.abs())
    };
    format!("{sign}{body}")
}

/// Rounds `x` to `digits` decimals, as Python `round(x, digits)` does.
///
/// Rust formats the exact binary value and rounds half to even, as Python
/// does. So printing and reading back gives the Python result.
pub fn py_round(x: f64, digits: usize) -> f64 {
    if !x.is_finite() {
        return x;
    }
    format!("{x:.digits$}").parse().unwrap_or(x)
}

/// Parses JSON text as `json.loads` does.
///
/// A repeated key keeps its first place and takes its last value. `NaN`,
/// `Infinity` and `-Infinity` are numbers.
pub fn loads(text: &str) -> Result<Value> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        pos: 0,
    };
    parser.skip_ws();
    let value = parser.value()?;
    parser.skip_ws();
    ensure!(
        parser.pos == parser.bytes.len(),
        "JSONDecodeError: Extra data at char {}",
        parser.pos
    );
    Ok(value)
}

/// A recursive descent JSON parser over the bytes of a string.
struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    pos: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
    }

    fn fail<T>(&self, what: &str) -> Result<T> {
        bail!("JSONDecodeError: {what} at char {}", self.pos)
    }

    fn eat(&mut self, word: &str) -> bool {
        if self.text[self.pos..].starts_with(word) {
            self.pos += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Value> {
        match self.bytes.get(self.pos) {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(Value::Str(self.string()?)),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.literal(),
        }
    }

    fn literal(&mut self) -> Result<Value> {
        for (word, value) in [
            ("null", Value::Null),
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
            ("NaN", Value::Float(f64::NAN)),
            ("Infinity", Value::Float(f64::INFINITY)),
            ("-Infinity", Value::Float(f64::NEG_INFINITY)),
        ] {
            if self.eat(word) {
                return Ok(value);
            }
        }
        self.fail("Expecting value")
    }

    fn object(&mut self) -> Result<Value> {
        self.pos += 1;
        let mut items: Vec<(String, Value)> = Vec::new();
        self.skip_ws();
        if self.eat("}") {
            return Ok(Value::Object(items));
        }
        loop {
            self.skip_ws();
            if self.bytes.get(self.pos) != Some(&b'"') {
                return self.fail("Expecting property name enclosed in double quotes");
            }
            let key = self.string()?;
            self.skip_ws();
            if !self.eat(":") {
                return self.fail("Expecting ':' delimiter");
            }
            self.skip_ws();
            let value = self.value()?;
            if let Some(slot) = items.iter_mut().find(|(k, _)| *k == key) {
                slot.1 = value;
            } else {
                items.push((key, value));
            }
            self.skip_ws();
            if self.eat("}") {
                return Ok(Value::Object(items));
            }
            if !self.eat(",") {
                return self.fail("Expecting ',' delimiter");
            }
        }
    }

    fn array(&mut self) -> Result<Value> {
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.eat("]") {
            return Ok(Value::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            if self.eat("]") {
                return Ok(Value::Array(items));
            }
            if !self.eat(",") {
                return self.fail("Expecting ',' delimiter");
            }
        }
    }

    fn number(&mut self) -> Result<Value> {
        if self.text[self.pos..].starts_with("-Infinity") {
            return self.literal();
        }
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        match self.bytes.get(self.pos) {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => self.digits(),
            _ => return self.fail("Expecting value"),
        }
        let mut is_float = false;
        if self.bytes.get(self.pos) == Some(&b'.')
            && self.bytes.get(self.pos + 1).is_some_and(u8::is_ascii_digit)
        {
            is_float = true;
            self.pos += 1;
            self.digits();
        }
        if let Some(b'e' | b'E') = self.bytes.get(self.pos) {
            let mut end = self.pos + 1;
            if let Some(b'+' | b'-') = self.bytes.get(end) {
                end += 1;
            }
            if self.bytes.get(end).is_some_and(u8::is_ascii_digit) {
                is_float = true;
                self.pos = end;
                self.digits();
            }
        }
        let token = &self.text[start..self.pos];
        if is_float {
            Ok(Value::Float(token.parse().with_context(|| {
                format!("JSONDecodeError: bad number {token:?}")
            })?))
        } else {
            Ok(Value::Int(token.parse().with_context(|| {
                format!("JSONDecodeError: integer out of range {token:?}")
            })?))
        }
    }

    fn digits(&mut self) {
        while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
            self.pos += 1;
        }
    }

    fn hex4(&mut self) -> Result<u32> {
        let Some(hex) = self.text.get(self.pos..self.pos + 4) else {
            return self.fail("Invalid \\uXXXX escape");
        };
        let Ok(code) = u32::from_str_radix(hex, 16) else {
            return self.fail("Invalid \\uXXXX escape");
        };
        self.pos += 4;
        Ok(code)
    }

    fn string(&mut self) -> Result<String> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            let Some(c) = self.text[self.pos..].chars().next() else {
                return self.fail("Unterminated string");
            };
            self.pos += c.len_utf8();
            match c {
                '"' => return Ok(out),
                '\\' => self.escape(&mut out)?,
                c if (c as u32) < 0x20 => return self.fail("Invalid control character"),
                c => out.push(c),
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<()> {
        let Some(&b) = self.bytes.get(self.pos) else {
            return self.fail("Unterminated string");
        };
        self.pos += 1;
        let c = match b {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\x08',
            b'f' => '\x0c',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let mut code = self.hex4()?;
                if (0xd800..0xdc00).contains(&code) && self.eat("\\u") {
                    let low = self.hex4()?;
                    if (0xdc00..0xe000).contains(&low) {
                        code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
                    } else {
                        return self.fail("a lone surrogate is not supported");
                    }
                }
                match char::from_u32(code) {
                    Some(c) => c,
                    None => return self.fail("a lone surrogate is not supported"),
                }
            }
            _ => return self.fail("Invalid \\escape"),
        };
        out.push(c);
        Ok(())
    }
}

/// Writes a string as `json.dumps` does, with `ensure_ascii=True`.
fn dump_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0_u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The JSON text of a float, as `json.dumps` writes it.
fn float_json(x: f64) -> String {
    if x.is_nan() {
        "NaN".to_string()
    } else if x.is_infinite() {
        if x > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        float_repr(x)
    }
}

/// Writes `value` at nesting `level`, with `indent` spaces per level.
fn dump(value: &Value, indent: Option<usize>, level: usize, out: &mut String) {
    let (open_sep, item_sep, close_sep) = indent.map_or_else(
        || (String::new(), ", ".to_string(), String::new()),
        |width| {
            let inner = " ".repeat(width * (level + 1));
            let outer = " ".repeat(width * level);
            (
                format!("\n{inner}"),
                format!(",\n{inner}"),
                format!("\n{outer}"),
            )
        },
    );
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => out.push_str(&float_json(*f)),
        Value::Str(s) => dump_str(s, out),
        Value::Array(items) if items.is_empty() => out.push_str("[]"),
        Value::Object(items) if items.is_empty() => out.push_str("{}"),
        Value::Array(items) => {
            out.push('[');
            out.push_str(&open_sep);
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(&item_sep);
                }
                dump(item, indent, level + 1, out);
            }
            out.push_str(&close_sep);
            out.push(']');
        }
        Value::Object(items) => {
            out.push('{');
            out.push_str(&open_sep);
            for (i, (key, item)) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(&item_sep);
                }
                dump_str(key, out);
                out.push_str(": ");
                dump(item, indent, level + 1, out);
            }
            out.push_str(&close_sep);
            out.push('}');
        }
    }
}

/// The text of `json.dumps(value, indent=2)`.
pub fn dumps_indent2(value: &Value) -> String {
    let mut out = String::new();
    dump(value, Some(2), 0, &mut out);
    out
}

/// The text of `json.dumps(value)`, with no indent.
pub fn dumps_compact(value: &Value) -> String {
    let mut out = String::new();
    dump(value, None, 0, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr_matches_python() {
        for (x, want) in [
            (66.67, "66.67"),
            (100.0, "100.0"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (0.1, "0.1"),
            (1e-5, "1e-05"),
            (0.0001, "0.0001"),
            (1e16, "1e+16"),
            (1e15, "1000000000000000.0"),
            (1.5e-7, "1.5e-07"),
            (123_456_789_012_345_680.0, "1.2345678901234568e+17"),
            (1.0 / 3.0, "0.3333333333333333"),
            (5e-324, "5e-324"),
            (1e100, "1e+100"),
        ] {
            assert_eq!(float_repr(x), want, "{x}");
        }
    }

    #[test]
    fn py_round_rounds_half_to_even_on_the_exact_value() {
        assert!((py_round(0.125, 2) - 0.12).abs() < f64::EPSILON);
        assert!((py_round(0.375, 2) - 0.38).abs() < f64::EPSILON);
        assert!((py_round(2.675, 2) - 2.67).abs() < f64::EPSILON);
        assert!((py_round(66.666_666, 2) - 66.67).abs() < f64::EPSILON);
    }

    #[test]
    fn loads_keeps_order_types_and_the_last_duplicate() {
        let value = loads(r#"{"b": 1, "a": 2.0, "b": [true, null, "é😀"]}"#).unwrap_or(Value::Null);
        assert_eq!(
            value,
            Value::Object(vec![
                (
                    "b".to_string(),
                    Value::Array(vec![
                        Value::Bool(true),
                        Value::Null,
                        Value::Str("\u{e9}\u{1f600}".to_string()),
                    ])
                ),
                ("a".to_string(), Value::Float(2.0)),
            ])
        );
        assert_eq!(loads("-0").ok(), Some(Value::Int(0)));
        assert_eq!(loads("1E2").ok(), Some(Value::Float(100.0)));
    }

    #[test]
    fn loads_refuses_malformed_text() {
        for bad in ["", "{", "[1,]", "01", "{\"a\" 1}", "\"\t\"", "nul", "1 2"] {
            assert!(loads(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn dumps_matches_python_indent_two() {
        let value = loads(
            r#"{"files": {"aé": {"x": 1, "y": 66.67, "z": null}}, "e": [], "o": {}, "l": [1, 2]}"#,
        )
        .unwrap_or(Value::Null);
        assert_eq!(
            dumps_indent2(&value),
            "{\n  \"files\": {\n    \"a\\u00e9\": {\n      \"x\": 1,\n      \"y\": 66.67,\n      \
             \"z\": null\n    }\n  },\n  \"e\": [],\n  \"o\": {},\n  \"l\": [\n    1,\n    2\n  ]\n}"
        );
        assert_eq!(
            dumps_compact(&value),
            r#"{"files": {"a\u00e9": {"x": 1, "y": 66.67, "z": null}}, "e": [], "o": {}, "l": [1, 2]}"#
        );
    }

    #[test]
    fn truthiness_and_str_follow_python() {
        assert!(!Value::Int(0).truthy());
        assert!(!Value::Object(Vec::new()).truthy());
        assert!(Value::Float(0.5).truthy());
        assert_eq!(Value::Int(50).py_str(), "50");
        assert_eq!(Value::Float(50.0).py_str(), "50.0");
        assert_eq!(Value::Null.py_str(), "None");
    }
}
