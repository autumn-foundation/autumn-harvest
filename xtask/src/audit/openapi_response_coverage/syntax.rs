//! Bracket-aware readers of Rust text that every section shares.

use super::py::{self, brace};

/// `text` split at each top-level separator, such as `,`, `&&` or `||`.
///
/// A separator inside `( )`, `[ ]` or `{ }` is not at the top level. A `<`
/// opens a generic only in a type (`types`) or right after `::`, as in a
/// turbofish. Anywhere else it is a comparison. Every argument list,
/// parameter list and boolean guard is split here. Each separator met is
/// added to `found`, when it is given.
pub fn split_expression(
    text: &str,
    separators: &[&str],
    types: bool,
    mut found: Option<&mut Vec<String>>,
) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut items = Vec::new();
    let mut stack: Vec<u8> = Vec::new();
    let mut current = 0;
    let mut index = 0;
    while index < bytes.len() {
        let char = bytes[index];
        if b"([{".contains(&char) {
            stack.push(char);
        } else if char == b'<' && (types || text[..index].trim_end().ends_with("::")) {
            stack.push(char);
        } else if char == b'>'
            && stack.last() == Some(&b'<')
            && !(index > 0 && bytes[index - 1] == b'=')
        {
            stack.pop();
        } else if b")]}".contains(&char) && !stack.is_empty() {
            while let Some(top) = stack.pop() {
                if top != b'<' {
                    break;
                }
            }
        } else if stack.is_empty() {
            if let Some(separator) = separators
                .iter()
                .find(|separator| bytes[index..].starts_with(separator.as_bytes()))
            {
                if let Some(found) = found.as_deref_mut() {
                    found.push((*separator).to_string());
                }
                items.push(text[current..index].trim().to_string());
                index += separator.len();
                current = index;
                continue;
            }
        }
        index += 1;
    }
    let rest = text[current..].trim();
    if !rest.is_empty() {
        items.push(rest.to_string());
    }
    items
}

/// The comma-separated items of an argument list, as `split_expression`
/// reads it.
pub fn split_top_level(text: &str) -> Vec<String> {
    split_expression(text, &[","], false, None)
}

/// Whether `TEST <joined>` has the test's `value` whenever the test has it.
///
/// `joined` is the rest of an `if` condition after its test, such as
/// `&& ready()` or `|| (a() && b())`. Only top-level `&&` and `||` count, as
/// `split_expression` reads them, and `&&` binds tighter than `||`. A true
/// test keeps the condition true when no operator follows or the first one
/// is `||`. A false test keeps it false when every operator is `&&`. Any
/// other text after the test, such as `== false`, keeps nothing. Every guard
/// reader in the audit asks this.
pub fn condition_keeps(joined: &str, value: bool) -> bool {
    let joined = joined.trim();
    if joined.is_empty() {
        return true;
    }
    if !(joined.starts_with("&&") || joined.starts_with("||")) {
        return false;
    }
    let mut operators = Vec::new();
    split_expression(joined, &["&&", "||"], false, Some(&mut operators));
    if value {
        return operators.first().map(String::as_str) == Some("||");
    }
    operators.iter().all(|operator| operator == "&&")
}

/// Where the statement that runs from `start` ends: its top-level `;` or
/// the block end.
pub fn statement_end(text: &str, start: usize) -> usize {
    let bytes = text.as_bytes();
    let mut depth = 0i64;
    for (index, &char) in bytes.iter().enumerate().skip(start) {
        if b"([{".contains(&char) {
            depth += 1;
        } else if b")]}".contains(&char) {
            depth -= 1;
            if depth < 0 {
                return index;
            }
        } else if char == b';' && depth == 0 {
            return index;
        }
    }
    text.len()
}

/// `text` up to the end of the block it starts in.
pub fn scope_rest(text: &str) -> &str {
    let mut depth = 0i64;
    for (index, char) in text.bytes().enumerate() {
        depth += i64::from(b"([{".contains(&char));
        depth -= i64::from(b")]}".contains(&char));
        if depth < 0 {
            return &text[..index];
        }
    }
    text
}

/// Where the `(` or `[` that the closer at `closer` matches starts, or `None`.
pub fn matching_opener(text: &str, closer: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let close = *bytes.get(closer)?;
    let opener = if close == b')' { b'(' } else { b'[' };
    let mut depth = 0i64;
    for index in (0..=closer).rev() {
        if bytes[index] == close {
            depth += 1;
        } else if bytes[index] == opener {
            depth -= 1;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

/// `(start, end)` of the innermost `{ .. }` in `block` that holds `position`.
pub fn enclosing_block(block: &str, position: usize) -> (usize, usize) {
    let bytes = block.as_bytes();
    let mut depth = 0i64;
    for index in (0..position.min(bytes.len())).rev() {
        depth += i64::from(bytes[index] == b'}');
        depth -= i64::from(bytes[index] == b'{');
        if depth < 0 {
            return (index, index + brace(&block[index..]).len());
        }
    }
    (0, block.len())
}

/// The expression a closure body yields: the body, or the tail of its block.
pub fn closure_value(body: &str) -> &str {
    let body = body.trim();
    if !body.starts_with('{') {
        return body;
    }
    let group = brace(body);
    let inner = if group.len() >= 2 {
        &group[1..group.len() - 1]
    } else {
        ""
    };
    let mut depth = 0i64;
    let mut tail = 0;
    for (index, char) in inner.bytes().enumerate() {
        depth += i64::from(b"([{".contains(&char));
        depth -= i64::from(b")]}".contains(&char));
        if char == b';' && depth == 0 {
            tail = index + 1;
        }
    }
    inner[tail..].trim()
}

/// The top-level statements of a block, without any nested block.
///
/// A `return` inside a nested `if`, `match` or closure does not run on every
/// path. So only a top-level one exits the block for certain.
pub fn unconditional(block: &str) -> String {
    let mut inner = if block.starts_with('{') {
        py::sl(block, 1, block.len().saturating_sub(1)).to_string()
    } else {
        block.to_string()
    };
    while let Some(opener) = inner.find('{') {
        let nested = brace(&inner[opener..]).len();
        inner.replace_range(opener..opener + nested, "");
    }
    inner
}

/// Whether the block that holds `start` is still open at `position`.
///
/// An `if` nested inside another conditional closes with it. So a parse
/// after that conditional runs on paths the `if` never saw.
pub fn same_scope(block: &str, start: usize, position: usize) -> bool {
    let mut depth = 0i64;
    for char in py::sl(block, start, position).bytes() {
        depth += i64::from(char == b'{');
        depth -= i64::from(char == b'}');
        if depth < 0 {
            return false;
        }
    }
    true
}

/// The text of the `match` arm whose pattern starts at `start`.
///
/// The arm runs from its `=>` to the end of its block, or to the next comma
/// at the top level of the arm list.
pub fn match_arm(arms: &str, start: usize) -> &str {
    let arrow = py::find(arms, "=>", start);
    if arrow < 0 {
        return "";
    }
    let rest = py::from(arms, py::to_usize(arrow) + 2);
    if rest.trim_start().starts_with('{') {
        let opener = rest.find('{').unwrap_or(0);
        return brace(&rest[opener..]);
    }
    let mut depth = 0i64;
    for (index, char) in rest.bytes().enumerate() {
        depth += i64::from(b"([{".contains(&char));
        depth -= i64::from(b")]}".contains(&char));
        if depth < 0 || (char == b',' && depth == 0) {
            return &rest[..index];
        }
    }
    rest
}

/// Where the body of a closure whose parameters end at `after` starts and
/// ends.
pub fn closure_body_end(text: &str, after: usize) -> (usize, usize) {
    let tail = py::from(text, after);
    let body = tail.trim_start();
    let opener = after + tail.len() - body.len();
    if body.starts_with('{') {
        return (opener, opener + brace(body).len());
    }
    let rest = scope_rest(body);
    let cut = rest.bytes().position(|char| char == b',' || char == b';');
    (opener, opener + cut.unwrap_or(rest.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_separator_inside_brackets_does_not_split() {
        assert_eq!(
            split_expression("a, f(b, c), Vec<u8, X>", &[","], true, None),
            vec!["a", "f(b, c)", "Vec<u8, X>"]
        );
        assert_eq!(
            split_expression("a < b, c", &[","], false, None),
            vec!["a < b", "c"]
        );
    }

    #[test]
    fn condition_keeps_reads_the_operators() {
        assert!(condition_keeps("", true));
        assert!(condition_keeps("|| ready()", true));
        assert!(!condition_keeps("&& ready()", true));
        assert!(condition_keeps("&& ready()", false));
        assert!(!condition_keeps("== false", false));
    }

    #[test]
    fn unconditional_drops_nested_blocks() {
        assert_eq!(unconditional("{ a; if b { return c; } d }"), " a; if b  d ");
    }

    #[test]
    fn the_closure_value_is_the_block_tail() {
        assert_eq!(closure_value("{ a; b }"), "b");
        assert_eq!(closure_value(" x + 1 "), "x + 1");
    }
}
