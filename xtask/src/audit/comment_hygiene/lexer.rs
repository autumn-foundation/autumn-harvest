//! The Rust comment lexer.

use std::collections::{HashMap, HashSet};

use super::patterns::{CHAR_LIT_RE, DOC_PAYLOAD_RE, DOC_VALUE_RE, RAW_OPEN_RE};
use super::pyre::{py_isspace, splitlines, strip};

/// The markers that Rustdoc renders as Markdown.
///
/// `//` and `////` are ordinary code comments that no renderer ever sees,
/// and `/*` is their block form.
pub const DOC_MARKERS: [&str; 4] = ["///", "//!", "/**", "/*!"];

/// Is `marker` one of `DOC_MARKERS`?
pub fn is_doc(marker: &str) -> bool {
    DOC_MARKERS.contains(&marker)
}

/// One line of comment text, located and classified.
///
/// A block comment over N lines gives N pieces, so each finding keeps a
/// true line number.
// The flags match the slots of the Python `Piece` one to one.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct Piece {
    pub line: usize,
    pub marker: String,
    /// The comment follows code on its line, as in `foo(); // note`.
    pub trailing: bool,
    /// The comment is `/* ... */`, not `//`.
    pub block: bool,
    /// The outermost `/* */` that this piece came from.
    ///
    /// One block comment is one comment, however many lines it covers and
    /// however many comments nest inside it. So its pieces stay in one run.
    /// It is -1 for line comments, which group by adjacency instead.
    pub group: i64,
    /// How deep inside that comment the piece sits.
    ///
    /// A nested comment is inside the fenced example of the enclosing
    /// comment, if there is one. So it inherits the fence. What it opens
    /// itself does not survive the close.
    pub nest: usize,
    /// Is this piece the end of its line?
    ///
    /// The delimiters of a nested comment are literal text in the rendered
    /// document. So anything after this piece on the line means that the
    /// line does not end here, even an empty "/**/" that gives no piece. A
    /// closing fence can have only spaces after it, so this decides one.
    pub line_end: bool,
    /// Does this piece begin its own line?
    ///
    /// A nested comment splits one line into several pieces, and only the
    /// first starts at column zero. Each Markdown block marker must start a
    /// line: a fence, a list item, a heading, a quote and a table row. So a
    /// piece that begins after literal "/*" or "*/" text has none of them.
    /// Only the line rules apply to it, and its text joins the prose around
    /// it.
    pub line_start: bool,
    /// Is the gap above this piece only attributes?
    ///
    /// A doc comment is a `#[doc]` attribute, and Rustdoc joins each one
    /// that an item has. So a `#[cfg(...)]` between two doc lines sits
    /// inside the document. It does not end it. `extract_comments` sets
    /// this, because only it still has the source to read.
    pub bridged: bool,
    /// Does this piece sit inside an attribute?
    ///
    /// A comment between the brackets of an attribute belongs to the
    /// attribute, not to the document around it. So it neither joins the
    /// doc run nor breaks it.
    pub in_attribute: bool,
    /// Does the document continue past this piece in an explicit
    /// `#[doc = ...]`?
    ///
    /// Rustdoc joins each `#[doc]` that an item has, and the value form
    /// holds content. So a doc run that ends beside one has not ended. The
    /// value is "outer" for `\#[doc = ...]`, "inner" for `\#![doc = ...]`,
    /// and "" for neither. The two forms document different things, so the
    /// marker must agree. `//!` continues into `\#![doc]` and `///` into
    /// `\#[doc]`, never across.
    pub doc_above: &'static str,
    pub doc_below: &'static str,
    /// The comment body, with the leading `*` gutter and one space removed.
    pub text: String,
}

impl Piece {
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn new(
        line: usize,
        marker: &str,
        body: &str,
        trailing: bool,
        block: bool,
        group: i64,
        nest: usize,
        line_start: bool,
    ) -> Self {
        let mut text = if block { strip_gutter(body) } else { body };
        text = text.strip_prefix(' ').unwrap_or(text);
        Self {
            line,
            marker: marker.to_string(),
            trailing,
            block,
            group,
            nest,
            line_end: true,
            line_start,
            bridged: false,
            in_attribute: false,
            doc_above: "",
            doc_below: "",
            text: text.to_string(),
        }
    }
}

/// `body` with a leading `*` gutter removed.
///
/// This is the Python `re.sub(r"^\s*\*(?!\*)", "", body)`. A second `*`
/// means that the line is not a gutter, so nothing is removed.
fn strip_gutter(body: &str) -> &str {
    let rest = body.trim_start_matches(py_isspace);
    match rest.as_bytes() {
        [b'*', b'*', ..] => body,
        [b'*', ..] => &rest[1..],
        _ => body,
    }
}

/// Is `body` only a `*` gutter and white space?
///
/// This asks the same question as the normalization of `Piece::text`. A
/// segment that reduces to nothing is not a blank line. Rustdoc renders
/// "* /* note */" as paragraph text, delimiters and all. So to emit it ends
/// a paragraph that the document still holds open.
fn gutter_only(body: &str) -> bool {
    strip(strip_gutter(body)).is_empty()
}

/// The marker of a block comment that starts at `at`, and its length.
///
/// `/**/` is an empty comment, not a doc marker. To treat it as one eats
/// the closing `*/` and swallows the rest of the file as comment body.
/// `/***` is not a doc marker either. Rustdoc documents nothing for it, as
/// a compiled test with an empty item page shows. To call it a doc comment
/// exempts its indented content as rendered code, and that takes Tier A off
/// a whole comment.
fn block_marker(bytes: &[u8], at: usize) -> &'static str {
    let rest = &bytes[at..];
    if rest.starts_with(b"/*!") {
        "/*!"
    } else if rest.starts_with(b"/**") && !(rest.starts_with(b"/**/") || rest.starts_with(b"/***"))
    {
        "/**"
    } else {
        "/*"
    }
}

/// The marker of a line comment, from its leading slash run.
fn line_marker(raw: &str) -> usize {
    let mut marker_len = raw.len() - raw.trim_start_matches('/').len();
    if marker_len == 2 && raw[marker_len..].starts_with('!') {
        marker_len += 1;
    }
    marker_len
}

/// Each Rust comment in `source`, and nothing that only looks like one.
///
/// This is a lexer, not a line regex, because both halves matter:
///
/// - Text inside a string is not a comment. This corpus embeds Rust and
///   SQL snippets in raw strings constantly: fixtures, `include_str!`
///   analogues and generated-source tests. A `// ...` line inside one is
///   string data. To flag it fails CI on a legitimate fixture. Tier A gates
///   at zero, so that is a hard block on an innocent PR.
/// - A comment is still a comment after code. `let n = 1; // TODO: fix`
///   and `/* TODO: fix */` are exactly the defects that this audit gates,
///   and a regex anchored at the line start never sees either.
///
/// It handles nested block comments and raw strings with any hash count.
/// It also handles byte and C-string prefixes, escapes, and the choice
/// between a lifetime and a char literal.
#[allow(clippy::too_many_lines)]
pub fn extract_comments(source: &str) -> Vec<Piece> {
    let mut pieces: Vec<Piece> = Vec::new();
    // Where each part that is not code sits: each string, char literal and
    // comment. `mark_bridges` counts the brackets of an attribute. A `]`
    // inside a string or a comment is text, not a bracket. Only the lexer
    // knows the difference, so it records the spans while it has them.
    let mut not_code: Vec<(usize, usize)> = Vec::new();
    let bytes = source.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    let mut line = 1;
    let mut code_on_line = false;
    let mut block_group: i64 = 0;

    while i < n {
        let ch = bytes[i];

        if ch == b'\n' {
            line += 1;
            i += 1;
            code_on_line = false;
            continue;
        }

        // A line comment runs to the end of the line.
        if bytes[i..].starts_with(b"//") {
            let end = source[i..].find('\n').map_or(n, |offset| i + offset);
            let raw = &source[i..end];
            // The marker is the whole leading slash run, plus `!` for `//!`.
            // `////` is an ordinary comment, not a doc comment. A marker of
            // only three slashes left a stray `/` on the body, and then the
            // anchored rules missed `//// let stale = compute();`.
            let marker_len = line_marker(raw);
            pieces.push(Piece::new(
                line,
                &raw[..marker_len],
                &raw[marker_len..],
                code_on_line,
                false,
                -1,
                0,
                true,
            ));
            not_code.push((i, end));
            i = end;
            continue;
        }

        // A block comment nests in Rust, so the lexer tracks its depth.
        if bytes[i..].starts_with(b"/*") {
            let marker = block_marker(bytes, i);
            let mut depth = 1;
            let comment_start = i;
            block_group += 1;
            let root_group = block_group;
            // Past the whole marker. A `!` of `/*!` left on the body made
            // the anchored rules miss `/*! TODO */` and `/*! let x = 1; */`.
            i += marker.len();
            let mut seg_start = i;
            let mut seg_line = line;
            let mut seg_nest = depth;
            // The outer marker is not part of the rendered document. So the
            // text after it is still the start of the first line. A nested
            // `/*` and a nested `*/` are rendered, literally. So text after
            // either is in the middle of a line and can open no block.
            let mut seg_start_of_line = true;
            let mut first = true;
            let mut line_started = false;
            while i < n && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    // A nested comment. The segment ends here, so the inner
                    // body starts a piece of its own. Otherwise
                    // `/* outer /* let x = 1; */ */` gives the rules one
                    // string that begins "outer", and the nested code is
                    // never anchored.
                    //
                    // A segment that is only the gutter before the opener is
                    // not emitted, on a line whose text continues inside the
                    // nested comment. The empty segment that a nested close
                    // leaves has the same defect on the other side. To emit
                    // either ended the paragraph one line early.
                    if i > seg_start && !gutter_only(&source[seg_start..i]) {
                        pieces.push(Piece::new(
                            seg_line,
                            marker,
                            &source[seg_start..i],
                            code_on_line && first,
                            true,
                            root_group,
                            seg_nest,
                            seg_start_of_line,
                        ));
                        first = false;
                    }
                    // The opener itself is text on this line, whether or not
                    // text came before it. So the line is no longer blank,
                    // and the text before it no longer ends the line.
                    line_started = true;
                    if let Some(last) = pieces.last_mut()
                        && last.line == line
                    {
                        last.line_end = false;
                    }
                    // A nested comment stays in the run of the enclosing
                    // comment, and `nest` records that it is inside. The
                    // fence state is kept per nesting level, not reset
                    // here. The inner text is inside the outer fenced
                    // example if there is one. A fence that the inner text
                    // opens is discarded when it closes.
                    depth += 1;
                    // Past the whole nested marker, for the same reason as
                    // the outer marker. A `!` of `/*!` that stays leaves the
                    // anchored rules at "! TODO".
                    i += block_marker(bytes, i).len();
                    seg_start = i;
                    seg_line = line;
                    seg_nest = depth;
                    seg_start_of_line = false;
                } else if bytes[i..].starts_with(b"*/") {
                    if depth > 1 {
                        if i > seg_start && !gutter_only(&source[seg_start..i]) {
                            pieces.push(Piece::new(
                                seg_line,
                                marker,
                                &source[seg_start..i],
                                code_on_line && first,
                                true,
                                root_group,
                                seg_nest,
                                seg_start_of_line,
                            ));
                            first = false;
                        }
                        line_started = true;
                        // The nested "*/" is literal text too.
                        if let Some(last) = pieces.last_mut()
                            && last.line == line
                        {
                            last.line_end = false;
                        }
                    }
                    depth -= 1;
                    i += 2;
                    if depth > 0 {
                        // The enclosing comment resumes, in the same run and
                        // at the nesting level it had before.
                        seg_start = i;
                        seg_line = line;
                        seg_nest = depth;
                        seg_start_of_line = false;
                    }
                } else if bytes[i] == b'\n' {
                    // A nested comment that closes at the end of a line
                    // leaves a segment of zero length. That is a blank
                    // segment, not a blank line, and to emit it ends the
                    // paragraph that the line is still part of. A real blank
                    // comment line emits nothing before the newline, so it
                    // still counts.
                    if i > seg_start || !line_started {
                        pieces.push(Piece::new(
                            seg_line,
                            marker,
                            &source[seg_start..i],
                            code_on_line && first,
                            true,
                            root_group,
                            seg_nest,
                            seg_start_of_line,
                        ));
                        first = false;
                    }
                    line_started = false;
                    line += 1;
                    i += 1;
                    seg_start = i;
                    seg_line = line;
                    seg_nest = depth;
                    seg_start_of_line = true;
                } else {
                    i += 1;
                }
            }
            not_code.push((comment_start, i));
            let tail_end = if depth == 0 { i - 2 } else { i };
            if tail_end > seg_start && !gutter_only(&source[seg_start..tail_end]) {
                pieces.push(Piece::new(
                    seg_line,
                    marker,
                    &source[seg_start..tail_end],
                    code_on_line && first,
                    true,
                    root_group,
                    seg_nest,
                    seg_start_of_line,
                ));
            }
            code_on_line = true;
            continue;
        }

        // A raw string has no escapes. A `"` and the opening hash count
        // close it.
        if matches!(ch, b'b' | b'c' | b'r')
            && !(i > 0 && is_ident_byte(bytes[i - 1]))
            && let Some(raw) = RAW_OPEN_RE.match_start(&source[i..])
        {
            let closer = format!("\"{}", raw.group(&source[i..], 1));
            let open_end = i + raw.end();
            let end = source[open_end..]
                .find(&closer)
                .map_or(n, |offset| open_end + offset + closer.len());
            line += source[i..end].matches('\n').count();
            not_code.push((i, end));
            i = end;
            code_on_line = true;
            continue;
        }

        // An ordinary string, and its b"" and c"" forms, with escapes.
        if ch == b'"' {
            let literal_start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'\\' {
                    // A backslash-newline continues the line. This corpus
                    // uses it in its long SQL and `#[error(...)]` strings.
                    // The escaped character is a real newline, and it must
                    // still be counted, or each later line number drifts.
                    if i + 1 < n && bytes[i + 1] == b'\n' {
                        line += 1;
                    }
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                if bytes[i] == b'\n' {
                    line += 1;
                }
                i += 1;
            }
            i = i.min(n);
            not_code.push((literal_start, i));
            code_on_line = true;
            continue;
        }

        // A `'` opens a char literal only when the literal closes.
        // Otherwise it is a lifetime tick, and the rest of the line is
        // ordinary code.
        if ch == b'\'' {
            if let Some(lit) = CHAR_LIT_RE.match_start(&source[i..]) {
                // `']'` is a char literal that holds a bracket, which is
                // text.
                not_code.push((i, i + lit.end()));
                i += lit.end();
            } else {
                i += 1;
            }
            code_on_line = true;
            continue;
        }

        if let Some(c) = source.get(i..).and_then(|rest| rest.chars().next()) {
            if !py_isspace(c) {
                code_on_line = true;
            }
            i += c.len_utf8();
        } else {
            code_on_line = true;
            i += 1;
        }
    }

    mark_bridges(&blank_non_code(source, &not_code), &mut pieces);
    pieces
}

/// Is `byte` an ASCII identifier character?
const fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// `source` with each part that is not code replaced by spaces.
///
/// Newlines survive, so each line keeps its number. Only the bracket
/// counter reads this copy. The comment text always comes from the real
/// source.
pub fn blank_non_code(source: &str, spans: &[(usize, usize)]) -> String {
    if spans.is_empty() {
        return source.to_string();
    }
    let mut out = String::with_capacity(source.len());
    let mut last = 0;
    for &(start, end) in spans {
        let start = start.max(last);
        out.push_str(&source[last..start]);
        for c in source[start..end].chars() {
            out.push(if c == '\n' { '\n' } else { ' ' });
        }
        last = end;
    }
    out.push_str(&source[last..]);
    out
}

/// Records which pieces have only attributes between them and the piece
/// above.
///
/// Rustdoc joins each `#[doc]` that an item has, and a doc comment is a
/// `#[doc]`. So an attribute between two doc lines does not end the
/// document. It sits inside it. rustdoc 1.94.1 confirms this for `//!`
/// across `#![cfg(...)]` and for `///` across `#[allow(...)]`. Both render
/// two paragraphs, and to delete the blank doc line beside the attribute
/// merges them. CH004 read that blank line as a block edge and failed the
/// build on it.
///
/// An attribute can wrap, so the scan follows its brackets, not its first
/// line. It reads them from the output of `blank_non_code`. A `]` inside a
/// string or a comment is text. A count of it closed a wrapped attribute
/// early, and that failed the build on the doc line after it.
///
/// The blanked copy answers the other question too. A line that holds only
/// a comment blanks to nothing, so it cannot open an attribute. That is why
/// `// #[derive(Debug)]` is still prose about an attribute, not a bridge.
/// But a comment written inside an open attribute is part of it, and it is
/// recorded so. `comment_runs` reads that, and it keeps such a comment from
/// cutting the document on either side of the attribute.
fn mark_bridges(source: &str, pieces: &mut [Piece]) {
    let occupied: HashSet<usize> = pieces.iter().map(|piece| piece.line).collect();
    let mut attributes: HashSet<usize> = HashSet::new();
    let mut inside: HashSet<usize> = HashSet::new();
    // The attribute lines that hold document content, and of which kind.
    // Only the value form does. `\#[doc(hidden)]` and `\#[doc(alias =
    // "zz")]` render nothing, as rustdoc 1.94.1 confirms. So a blank doc
    // line in front of either is a real block edge.
    let mut doc_value: HashMap<usize, &'static str> = HashMap::new();
    let mut depth: i64 = 0;
    let mut kind: &'static str = "";
    let mut opened_at = 0;
    let mut carried = String::new();
    let lines = splitlines(source);
    for (offset, raw) in lines.iter().enumerate() {
        let index = offset + 1;
        let text = strip(raw);
        let opened = depth > 0;
        if !opened {
            // An empty source line renders nothing either. So it does not
            // end the document, as an attribute does not. A blank line
            // between two doc runs leaves rustdoc 1.94.1 with one document
            // of two paragraphs. The blank doc line beside it is the
            // separator that makes them two. The real source decides this:
            // a line with only a comment blanks to nothing in this copy, and
            // it is not empty.
            if text.is_empty() && !occupied.contains(&index) {
                attributes.insert(index);
                continue;
            }
            if !text.starts_with("#[") && !text.starts_with("#![") {
                continue;
            }
            let doc = DOC_VALUE_RE.match_start(text);
            opened_at = index;
            carried.clear();
            if let Some(doc) = doc {
                kind = if doc.group(text, 1).is_empty() {
                    "outer"
                } else {
                    "inner"
                };
            }
        }
        attributes.insert(index);
        carried.push(' ');
        carried.push_str(text);
        let open = i64::try_from(text.matches('[').count()).unwrap_or(i64::MAX);
        let close = i64::try_from(text.matches(']').count()).unwrap_or(i64::MAX);
        depth = (depth + open - close).max(0);
        // The attribute has closed, so its whole text is known. Now the
        // test can ask if it holds document content.
        if depth == 0 && !kind.is_empty() && DOC_PAYLOAD_RE.is_match(&carried) {
            doc_value.insert(opened_at, kind);
        }
        if depth == 0 {
            kind = "";
            carried.clear();
        }
        // A comment on this line is inside the attribute, unless the line
        // neither began inside one nor leaves one open. Both ends are read,
        // because either alone gets a real case wrong.
        // `#[allow(dead_code)] // why` is open at neither end, so the
        // comment is beside the attribute, an ordinary trailing comment.
        // `#[cfg(all(/* note */` is open only at the end. `/* why */ )]` is
        // open only at the start.
        //
        // Line granularity stands in for a question about position here.
        // So one case is decided the safe way, not the correct way.
        // `)] // done` reads as inside, although the comment follows the
        // attribute. The cost is a CH004 that goes unreported. The
        // alternative costs a CH004 reported on correct documentation, and
        // a Tier A false positive fails the build.
        if occupied.contains(&index) && (opened || depth > 0) {
            inside.insert(index);
        }
    }

    let last = lines.len();
    // The kind of doc-value attribute that the document continues into.
    // The walk covers attribute lines only. So it stops at the first line
    // that renders something of its own: an item, or another comment.
    let reach = |start: i64, step: i64| -> &'static str {
        let mut line = start;
        while line >= 1 {
            let Ok(index) = usize::try_from(line) else {
                break;
            };
            if index > last || !attributes.contains(&index) {
                break;
            }
            if let Some(kind) = doc_value.get(&index) {
                return kind;
            }
            line += step;
        }
        ""
    };

    let mut previous: Option<usize> = None;
    for piece in pieces.iter_mut() {
        piece.in_attribute = inside.contains(&piece.line);
        if piece.in_attribute {
            // Its own line is part of the attribute, and it is not part of
            // the document. So it neither carries a gap nor closes one.
            piece.bridged = false;
            continue;
        }
        piece.bridged = previous.is_some_and(|previous| {
            piece.line > previous + 1
                && (previous + 1..piece.line).all(|gap| attributes.contains(&gap))
        });
        let line = i64::try_from(piece.line).unwrap_or(i64::MAX);
        piece.doc_above = reach(line - 1, -1);
        piece.doc_below = reach(line + 1, 1);
        previous = Some(piece.line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(source: &str) -> Vec<String> {
        extract_comments(source)
            .iter()
            .map(|piece| strip(&piece.text).to_string())
            .filter(|text| !text.is_empty())
            .collect()
    }

    #[test]
    fn strings_hold_no_comments() {
        assert_eq!(texts("let s = \"// not\";\n"), Vec::<String>::new());
        assert_eq!(texts("let s = r#\"\n// not\n\"#;\n"), Vec::<String>::new());
    }

    #[test]
    fn a_lifetime_does_not_open_a_char_literal() {
        assert_eq!(texts("fn f<'a>(x: &'a str) {} // yes\n"), vec!["yes"]);
    }

    #[test]
    fn nested_blocks_give_one_piece_each() {
        assert_eq!(
            texts("/* a /* b */ c */\n"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn the_gutter_is_stripped_from_block_text() {
        assert_eq!(strip_gutter("  * text"), " text");
        assert_eq!(strip_gutter("  ** text"), "  ** text");
    }

    #[test]
    fn an_attribute_bridges_two_doc_lines() {
        let pieces = extract_comments("/// a\n#[cfg(test)]\n/// b\n");
        assert!(pieces[1].bridged);
    }
}
