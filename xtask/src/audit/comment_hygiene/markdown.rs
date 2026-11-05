//! The Markdown block layer: lists, quotes, fences, tables and HTML blocks.
//!
//! This layer is written by hand and is partial on purpose. Each marker
//! is measured against its container, as CommonMark measures it. A marker
//! pattern here is wrong by default until it takes a container.

use super::lexer::Piece;
use super::patterns::{
    DELIM_CELL_RE, FENCE_RE, HEADING_RE, HTML_BLOCK_RE, HTML_VERBATIM_RE, LIST_MARKER_RE, SETEXT_RE,
};
use super::pyre::{Caps, expanded_len, lstrip, strip, tail};
use super::shapes;

/// A list container: its content column and its quote depth.
pub type Container = (usize, usize);

/// A fence: its delimiter character and its length.
pub type Fence = (char, usize);

/// The positions of a cell boundary in `bytes[start..end]`.
///
/// This is `(?<!\\)\|`. An escaped pipe is content. Rustdoc renders
/// "| a \\| b | c |" as two columns, and the first holds a pipe. The
/// look-behind reads the byte before `start`, as Python does.
fn cell_splits(bytes: &[u8], start: usize, end: usize) -> Vec<usize> {
    (start..end)
        .filter(|&i| bytes[i] == b'|' && (i == 0 || bytes[i - 1] != b'\\'))
        .collect()
}

/// The cells of a GFM table row.
///
/// A row can open a list item, so one list marker is removed first.
/// Rustdoc renders "- | a | b |" as an item that holds a table with two
/// columns, so the marker is not a cell. The leading and trailing pipes
/// are optional.
pub fn row_cells(text: &str, container: usize) -> Vec<String> {
    let mut text = text;
    if let Some(content) = list_content(text, container) {
        text = tail(text, content.1);
    }
    let mut text = strip(text);
    if let Some(rest) = text.strip_prefix('|') {
        text = rest;
    }
    if text.ends_with('|') && !text.ends_with("\\|") {
        text = &text[..text.len() - 1];
    }
    let mut cells = Vec::new();
    let mut cursor = 0;
    for split in cell_splits(text.as_bytes(), 0, text.len()) {
        cells.push(text[cursor..split].to_string());
        cursor = split + 1;
    }
    cells.push(text[cursor..].to_string());
    cells
}

/// Is `text` a valid fence opener that its container accepts?
///
/// Two rules apply. `fence_delimiter` and its caller already apply both,
/// and the lazy-continuation test applied neither. A delimiter sits within
/// three columns of its container. `FENCE_RE` captures the indent and does
/// not bound it, so each caller can measure it. A caller that forgets asks
/// a question with no bound. And the info string of a backtick fence can
/// hold no backtick. So a backtick fence line with a second backtick later
/// on the line is not an opener at all.
///
/// Both matter to a lazy continuation. A line that opens no fence opens no
/// block, and Rustdoc keeps it inside the quoted paragraph.
pub fn opens_fence(text: &str, container: usize) -> bool {
    let Some(found) = FENCE_RE.match_start(text) else {
        return false;
    };
    if expanded_len(found.group(text, 1)) > container + 3 {
        return false;
    }
    !(found.group(text, 2).starts_with('`') && tail(text, found.end()).contains('`'))
}

/// Where the cells of a GFM table row are, as offsets into `text`.
///
/// This removes the same markers as `row_cells`, and it keeps offsets,
/// because the span pass must blank in place. Each column after a blanked
/// span must still line up with the source line that the finding reports.
///
/// Rustdoc parses each cell as its own inline context. So a backtick in one
/// cell cannot pair with a backtick in the next. This tells the span pass
/// where one context ends. That is a boundary inside a line, the one kind
/// that the block layer cannot express.
pub fn row_cell_spans(text: &str, container: usize) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut start = list_content(text, container).map_or(0, |content| content.1);
    while start < bytes.len() && matches!(bytes[start], b' ' | b'\t') {
        start += 1;
    }
    let mut end = bytes.len();
    while end > start && matches!(bytes[end - 1], b' ' | b'\t') {
        end -= 1;
    }
    if start < end && bytes[start] == b'|' {
        start += 1;
    }
    if end > start + 1 && bytes[end - 1] == b'|' && bytes[end - 2] != b'\\' {
        end -= 1;
    }
    let mut spans = Vec::new();
    let mut cursor = start;
    if start < end {
        for split in cell_splits(bytes, start, end) {
            spans.push((cursor, split));
            cursor = split + 1;
        }
    }
    spans.push((cursor, end));
    spans
}

/// Is `text` a delimiter row for the row `header` above it?
///
/// It must sit within three columns of the container, like each marker
/// here. A strip of the line before the test loses that. Then a delimiter
/// with too much indent turns the paragraph above it into a table, and the
/// sentence is dropped.
///
/// A delimiter row is a delimiter row only for a header of the same width.
/// Rustdoc renders "Intro | header |" over "| - |" as one paragraph,
/// because two cells do not match one. To read it as a table there closes a
/// paragraph that the document still holds open. One hyphen in a cell is
/// valid. A one-cell "| - |" under a one-cell "| a |" is a table, as
/// rustdoc 1.94 renders it.
fn table_delimiter(text: &str, container: usize, header: &str) -> bool {
    if leading_columns(text) > container + 3 {
        return false;
    }
    let stripped = strip(text);
    if !stripped.contains('|') || !stripped.trim_matches([' ', '\t', '|', '-', ':']).is_empty() {
        return false;
    }
    // Each cell needs its own hyphen run. "| | --- |" has an empty first
    // cell. So it is not a delimiter row, and the pipe line above it is not
    // a header. Both are prose, and their sentence must be counted.
    let cells = row_cells(text, container);
    if cells.is_empty()
        || !cells
            .iter()
            .all(|cell| DELIM_CELL_RE.match_start(strip(cell)).is_some())
    {
        return false;
    }
    cells.len() == row_cells(header, container).len()
}

/// The rest of the line of a piece, past any nested comment on it.
///
/// The whole line decides a fence delimiter, and a nested comment splits
/// one line into several pieces. Rustdoc keeps a fence open across "```
/// /* note */", because only spaces can follow a closing fence. To close
/// it there reports the TODO of a fenced example. That is a false positive
/// on a gate that sits at zero.
pub fn line_tail(pieces: &[&Piece], index: usize) -> String {
    let mut out = String::new();
    for piece in &pieces[index + 1..] {
        if piece.line != pieces[index].line || piece.line_start {
            break;
        }
        out.push_str(&piece.text);
    }
    out
}

/// Is `text` a line of a CommonMark indented code block?
///
/// The line is four columns past the container, and only where no
/// paragraph is open. Indented code cannot interrupt a paragraph, so a
/// wrapped line indented under its own paragraph stays prose. Rustdoc
/// renders the block form as a Rust example. So its content is an example,
/// not commentary. Without this rule, "///     let x = compute();" after a
/// blank line fails CH001. Then no rustdoc example without a fence can be
/// added to this tree.
///
/// A blank line stays inside an open block. Only a line indented less than
/// four columns ends one.
///
/// This applies to doc comments only. That limit is the point of the rule,
/// not a shortcut. Indented code is a fact about what Rustdoc renders, and
/// Rustdoc renders no `//` comment. There, indentation is how this tree
/// lays out a long argument. To exempt it would stop the measure of
/// nineteen real sentences. A ``` fence is different. An author writes one
/// to say "this is an example", whatever the marker. So fences stay exempt
/// everywhere.
#[allow(clippy::fn_params_excessive_bools)]
pub fn indented_code(
    text: &str,
    container: usize,
    paragraph: bool,
    inside: bool,
    doc: bool,
) -> bool {
    if !doc {
        return false;
    }
    if inside {
        return strip(text).is_empty() || leading_columns(text) >= container + 4;
    }
    !paragraph && !strip(text).is_empty() && leading_columns(text) >= container + 4
}

/// Does `text` begin a CommonMark block other than a paragraph?
///
/// `paragraph` says that a paragraph is already open. Only a list marker
/// that can interrupt it ends it. "2." cannot. Rustdoc renders a paragraph
/// line that opens a code span, and a "2." line that closes it, as one
/// paragraph. A marker between the two is inside the code span. A cut of
/// the span there failed the build on it.
///
/// This is what ends a GFM table. A body row needs no pipe. Rustdoc renders
/// "ordinary row" under a table as a cell and fills the rest. So the table
/// runs to a blank line or the next block. A rule that needed a pipe in
/// each row ended the table one line early.
pub fn starts_block(text: &str, container: usize, paragraph: bool, doc: bool) -> bool {
    if strip(text).is_empty() {
        return true;
    }
    opens_fence(text, container)
        || heading(text, container)
        // This applies to doc comments only, with the scope that
        // `lazy_continuation` gives it. Nothing renders a `//` comment, so
        // "<pre>" there opens no block. The table callers pass `true`, so
        // they keep the question that they always asked.
        || html_block(text, container, doc)
        || thematic_break(text, container)
        || (list_content(text, container).is_some()
            && (!paragraph || LIST_MARKER_RE.match_start(text).is_some_and(|m| interrupts_paragraph(&m, text))))
        || quote_marker(text, container).is_some()
}

/// The piece after `index`, if it is in the same comment.
///
/// A delimiter row belongs to the header above it only when both are the
/// same comment. Rustdoc renders the delimiters of a nested comment
/// literally. So "/* | --- | */" under "| h |" is paragraph text. To read
/// it as a delimiter turns a paragraph into a table and drops the sentence.
fn next_row<'p>(pieces: &[&'p Piece], index: usize, nest: usize) -> Option<&'p Piece> {
    pieces
        .get(index + 1)
        .copied()
        .filter(|piece| piece.nest == nest)
}

/// Is `text` a table header: a pipe row with a delimiter row under it?
///
/// This is the one place that decides it, for both scanners and for the
/// lazy-continuation test. A pipe alone is not a table. "| foo" wraps a
/// sentence like any other word, and only the delimiter row below makes a
/// block.
pub fn table_header(
    pieces: &[&Piece],
    index: usize,
    nest: usize,
    text: &str,
    container: usize,
) -> bool {
    if !text.contains('|') {
        return false;
    }
    next_row(pieces, index, nest)
        .is_some_and(|row| table_delimiter(strip_quote(&row.text, container), container, text))
}

/// Is `text` the opening line of an HTML block?
///
/// This applies to doc comments only, and `doc` has no default, so no
/// caller can forget it. Nothing renders a `//` comment. So "<pre>" there
/// is five literal characters and no block at all. Callers once had to
/// remember that rule one at a time, and three of five forgot it. So it is
/// now an argument that each caller must supply.
///
/// It must sit within three columns of the container, like each marker
/// here. An HTML block is a block, so it ends the paragraph above it. Then
/// the next "22." can open the list that the rendered document gives it.
pub fn html_block(text: &str, container: usize, doc: bool) -> bool {
    if !doc {
        return false;
    }
    HTML_BLOCK_RE
        .match_start(text)
        .is_some_and(|found| expanded_len(found.group(text, 1)) <= container + 3)
}

/// The kind of HTML block that `text` opens, which decides what closes it.
///
/// A verbatim block takes its name from its own tag. Rustdoc keeps a
/// "<pre>" block open across a literal "</style>" line. So a rule that
/// accepted any of the four close tags ended the block early and reported
/// its content.
pub fn html_kind(text: &str) -> &'static str {
    let stripped = lstrip(text);
    if let Some(verbatim) = HTML_VERBATIM_RE.match_start(text) {
        return match verbatim.group(text, 1).to_lowercase().as_str() {
            "script" => "script",
            "pre" => "pre",
            "style" => "style",
            _ => "textarea",
        };
    }
    if stripped.starts_with("<!--") {
        return "comment";
    }
    if stripped.starts_with("<?") {
        return "instruction";
    }
    if stripped.starts_with("<![CDATA[") {
        return "cdata";
    }
    if stripped.starts_with("<!") {
        return "declaration";
    }
    "tag"
}

/// Does `text` end an open HTML block of `kind`?
///
/// Its content is raw HTML, not Markdown. Rustdoc renders a 26-word line
/// between "<pre>" and "</pre>" preformatted. To count it as a sentence
/// reports a CH007 on something that is not prose.
///
/// Types 1 to 5 end on the line that has their closer. Type 6, a "tag"
/// block, ends at a blank line.
pub fn html_closes(text: &str, kind: &str) -> bool {
    let closers: &[&str] = match kind {
        "tag" => return strip(text).is_empty(),
        "script" => &["</script>"],
        "pre" => &["</pre>"],
        "style" => &["</style>"],
        "textarea" => &["</textarea>"],
        "comment" => &["-->"],
        "instruction" => &["?>"],
        "declaration" => &[">"],
        _ => &["]]>"],
    };
    let lower = text.to_lowercase();
    closers.iter().any(|closer| lower.contains(closer))
}

/// Is this "N)" the tail of a wrapped parenthesis, not a list marker?
///
/// The "1)" ordered form makes each wrapped "(202)" and "(index 1)" look
/// like a list item on the line where its digits land. The paragraph so
/// far says which it is. An unmatched "(" before it means that the ")"
/// closes that parenthesis. A sentence split there reports prose that the
/// author never wrote as two sentences.
pub fn closes_an_open_paren(marker: &Caps, body: &str, run: &[String]) -> bool {
    if !strip(marker.group(body, 2)).ends_with(')') {
        return false;
    }
    let text = run.join(" ");
    text.matches('(').count() > text.matches(')').count()
}

/// Can this list marker start an item in the middle of a paragraph?
///
/// This is the rule of CommonMark. A wrapped "202)" or "503)" matches the
/// ordered form, and a split of a paragraph there hides the long sentence
/// that it belongs to. Only a bullet, or the number one, can interrupt a
/// paragraph. Inside a list any number continues it.
///
/// An item that is only its marker cannot interrupt a paragraph at all.
/// CommonMark requires content on the first line of an interrupting item.
/// The item opens a list at the start of a block. But in the middle of a
/// paragraph a lone "-" stays paragraph text. To treat it as a container
/// invents a fence allowance that nothing in the document has.
pub fn interrupts_paragraph(marker: &Caps, text: &str) -> bool {
    if strip(tail(text, marker.end())).is_empty() {
        return false;
    }
    let marker_text = strip(marker.group(text, 2));
    if !marker_text.starts_with(|c: char| c.is_ascii_digit()) {
        return true;
    }
    // The number, not its spelling. CommonMark reads the value of the
    // marker, so "01." starts at one and can interrupt as "1." does.
    marker_text[..marker_text.len() - 1].parse::<u64>().ok() == Some(1)
}

/// Where the content of a list item starts on `text`: its column, and its
/// byte offset.
///
/// Three CommonMark rules apply. Each was a way to open a fence that is
/// not there, and so to exempt each later comment in silence:
///
/// * The marker itself can have at most three columns of indent past its
///   container. Past that "    - ~~~rust" is an indented code line.
/// * One to four columns after the marker are padding. Five or more means
///   that the content starts one column after the marker, and the rest is
///   indented code. So "-     ```rust" does not open a fence.
/// * Padding is counted in columns, not characters. A tab is up to four
///   columns wide. So a space and two tabs is three characters of padding
///   and seven columns of it.
pub fn list_content(text: &str, container: usize) -> Option<(usize, usize)> {
    // A thematic break wins over a list item in CommonMark, and "* * *" or
    // "- - -" matches both. To read one as an item opens a container, and a
    // fence allowance, where the rendered document has a horizontal rule.
    if thematic_break(text, container) {
        return None;
    }
    let marker = LIST_MARKER_RE.match_start(text)?;
    let indent = expanded_len(marker.group(text, 1));
    if indent > container + 3 {
        return None;
    }
    // The expansion starts at the start of the line. The width of a tab
    // depends on its column, not on how many characters come before it.
    let marker_column = indent + marker.group(text, 2).len();
    let padding = expanded_len(marker.group(text, 0)).saturating_sub(marker_column);
    // An item that is only its marker has no padding to measure.
    // CommonMark puts its content one column past the marker, as for too
    // much padding. Otherwise "-" alone opens no container, and an indented
    // fence under it is recorded at the top level.
    if padding > 4 || padding == 0 {
        let group3 = marker.span(3).map_or_else(|| marker.end(), |span| span.0);
        return Some((marker_column + 1, group3 + 1));
    }
    Some((marker_column + padding, marker.end()))
}

/// The block-quote marker of `text`, if it is allowed to have one.
///
/// It can have three columns of indent, like each other CommonMark marker,
/// and like each other one, three columns past the container. A quote
/// inside a list item whose content starts at column four has four columns
/// of indent and is still a quote. An absolute limit misses it, and then
/// misses the fence inside it.
///
/// It returns the end of the indent and the end of the markers.
fn quote_marker(text: &str, container: usize) -> Option<(usize, usize)> {
    shapes::blockquote(text).filter(|&(indent, _)| expanded_len(&text[..indent]) <= container + 3)
}

/// `text` past any block-quote marker.
///
/// Each container decision below measures from here. So a quoted fence,
/// list or indent reads as the unquoted form does.
pub fn strip_quote(text: &str, container: usize) -> &str {
    quote_marker(text, container).map_or(text, |(_, end)| tail(text, end))
}

/// Is `text` a Setext underline that its container accepts?
///
/// It must sit within three columns of the container, like each marker
/// here.
pub fn setext_underline(text: &str, container: usize) -> bool {
    SETEXT_RE
        .match_start(text)
        .is_some_and(|found| expanded_len(found.group(text, 1)) <= container + 3)
}

/// Is `text` an ATX heading that its container accepts?
///
/// It must sit within three columns of the container, like each marker
/// here. An indent with no bound in a marker pattern is a defect. Four
/// columns into a paragraph, "# text" is indented content. To read it as a
/// heading clears a paragraph that the rendered document still holds open.
pub fn heading(text: &str, container: usize) -> bool {
    HEADING_RE
        .match_start(text)
        .is_some_and(|found| expanded_len(found.group(text, 1)) <= container + 3)
}

/// Is `text` a thematic break that its container accepts?
///
/// It must sit within three columns of the container, like each other
/// CommonMark marker. Past that it is indented content. To treat it as a
/// break clears a paragraph that the rendered document still has open.
pub fn thematic_break(text: &str, container: usize) -> bool {
    shapes::thematic_break(text)
        .is_some_and(|indent| expanded_len(&text[..indent]) <= container + 3)
}

/// `text` past exactly `levels` block-quote markers, and no more.
///
/// Inside a fence, only the markers of the container of the fence are
/// continuation syntax. A deeper "> " is literal sample text. To strip it
/// turns the quoted delimiter of an example into a closer.
fn strip_quote_levels(text: &str, levels: usize, container: usize) -> &str {
    let mut text = text;
    let mut container = container;
    for _ in 0..levels {
        let Some((indent, end)) = shapes::one_quote(text) else {
            break;
        };
        if expanded_len(&text[..indent]) > container + 3 {
            break;
        }
        text = tail(text, end);
        // Inside the quote the content starts again at column zero.
        container = 0;
    }
    text
}

/// How many block-quote levels `text` opens with.
pub fn quote_depth(text: &str, container: usize) -> usize {
    quote_marker(text, container).map_or(0, |(_, end)| text[..end].matches('>').count())
}

/// The visual column where the content of `text` starts.
///
/// Columns, not characters. A tab is up to four columns, so each indent
/// comparison here must expand before it compares.
pub fn leading_columns(text: &str) -> usize {
    expanded_len(&text[..text.len() - lstrip(text).len()])
}

/// The innermost container column recorded at `depth`, else zero.
///
/// Columns have meaning only inside one quote depth. An entry pushed while
/// unquoted is a raw column. An entry pushed inside a quote is measured
/// after the marker. A fence must remember the container of its own frame,
/// or its body reads as dedented out of a list that it was never in.
fn container_at_depth(stack: &[Container], depth: usize) -> usize {
    stack
        .iter()
        .rev()
        .find(|entry| entry.1 == depth)
        .map_or(0, |entry| entry.0)
}

/// Has `text` dedented out of the container where an open fence started?
///
/// CommonMark ends a fenced block with its container, with or without a
/// closing delimiter. Without this, a fence with no close in one list item
/// stays open over each later comment in the run. That is a gate that
/// stops gating in silence, the failure that this audit exists to prevent.
pub fn leaves_container(text: &str, scope: (usize, usize, usize)) -> bool {
    let (outer, container, depth) = scope;
    // There are two frames, because a quote marker and the content behind
    // it are not measured the same way. `outer` is the container that the
    // marker sits in. A quote inside a list item has the indent of the item.
    // `container` is the container of the fence itself, after that marker
    // is stripped. To read either in the frame of the other ends the fence
    // on its own body.
    //
    // Columns compare only inside one quote depth. A shallower line has
    // left the quote outright. A deeper one is nested inside the container,
    // so its column after the strip says nothing about an exit.
    let line_depth = quote_depth(text, outer);
    if line_depth != depth {
        return line_depth < depth;
    }
    let body = strip_quote(text, outer);
    if strip(body).is_empty() {
        return false;
    }
    leading_columns(body) < container
}

/// The open list containers after `text`, innermost last, and whether a
/// paragraph is still open.
///
/// Each entry is (content column, quote depth). It is a stack, not one
/// column. A line that dedents out of a nested item lands back in its
/// parent. To collapse to zero there records any fence opened on it at the
/// top level. Then the fence outlives the list and swallows each later
/// comment.
///
/// The depth is what ends a quoted list. A list opened inside "> " does not
/// survive the quote. A stale container makes a top-level line with too
/// much indent look like a fence relative to a list that is no longer open.
///
/// `paragraph` applies the interruption rule of CommonMark to the marker
/// itself, not only to the sentence split. "2." partway through the
/// paragraph of an item continues that paragraph and does not open a
/// nested list. Any pop clears it. A dedent out of an item ends the
/// paragraph inside it. That is why "2." on the line after "1." still
/// starts an item. This is simpler than CommonMark in one way. Any line
/// that is not blank opens a paragraph, so a table or a heading does not
/// close one here.
#[allow(clippy::fn_params_excessive_bools)]
pub fn update_containers(
    text: &str,
    stack: &[Container],
    paragraph: bool,
    quoted: usize,
    table: bool,
    doc: bool,
) -> (Vec<Container>, bool) {
    let mut paragraph = paragraph;
    // Read against the container in force before this line. The indent
    // allowance of a quote is relative to the list item that still holds it.
    let enclosing = stack.last().map_or(0, |entry| entry.0);
    let depth = quote_depth(text, enclosing);
    let body = strip_quote(text, enclosing);
    // A block quote is its own block. So to cross into or out of one ends
    // the paragraph. The prose path did this before the container path did.
    // Until then, a "22." after a quote was refused the container that the
    // rendered document gives it.
    if depth != quoted {
        paragraph = false;
    }
    let popped = stack.len();
    let mut stack: Vec<Container> = stack
        .iter()
        .copied()
        .filter(|entry| entry.1 <= depth)
        .collect();
    if !strip(body).is_empty() {
        let indent = leading_columns(body);
        // Only at the same depth, for the same reason. A block quote nested
        // in a list item sits inside it, and its stripped body starts at
        // column zero. A pop on that closes the item that holds the quote.
        while stack
            .last()
            .is_some_and(|entry| entry.1 == depth && entry.0 > indent)
        {
            stack.pop();
        }
    }
    if stack.len() < popped {
        paragraph = false;
    }

    let marker = LIST_MARKER_RE.match_start(body);
    let content = list_content(body, stack.last().map_or(0, |entry| entry.0));
    if let Some(content) = content {
        let interrupts = marker.is_some_and(|m| interrupts_paragraph(&m, body));
        if !paragraph || interrupts {
            stack.push((content.0, depth));
        }
    }
    let top = stack.last().map_or(0, |entry| entry.0);
    // Only paragraph content leaves a paragraph open. A heading, a thematic
    // break or a table row is its own block. To treat one as a paragraph
    // refuses the next "22." a container that it has a right to. The text
    // of a list marker is a paragraph, so those still count.
    //
    // Only a CommonMark block ends a paragraph. A line with a leading pipe
    // is not one. Tables are a GFM extension, and "| not a table" is prose.
    // So a clear here gives the next "22." a container that the rendered
    // document does not have. A Setext underline is a block, but only with
    // a paragraph above it to underline.
    let block = table
        || heading(body, top)
            // This applies to doc comments only, the third site of this
            // question after `starts_block` and `lazy_continuation`. A clear
            // of the paragraph on the "<pre>" of a `//` comment let the next
            // "2." open a list that it has no right to. The span cut there
            // failed the build.
            || html_block(body, top, doc)
            || thematic_break(body, top)
        || (paragraph && setext_underline(body, top));
    let prose = !strip(body).is_empty() && !block;
    (stack, prose)
}

/// The arguments of `lazy_continuation`.
pub struct LazyLine<'a> {
    pub text: &'a str,
    pub peeled: &'a str,
    pub container: usize,
    pub depth: usize,
    pub quoted: usize,
    pub paragraph: bool,
    pub pieces: &'a [&'a Piece],
    pub index: usize,
    pub nest: usize,
    pub doc: bool,
}

/// Does this line continue a quoted paragraph without a marker of its own?
///
/// CommonMark lets a paragraph inside a block quote continue on a line
/// with no ">" at all. So to leave the quote by depth is not always to
/// leave the block. Rustdoc renders "> Explain the `literal" and an
/// unmarked "TODO: issue required` suffix." as one quoted paragraph, with
/// the marker inside a code span.
///
/// Anything that begins a block ends the continuation instead: a list
/// marker, a confirmed table, a heading, an HTML block, a rule or a fence.
/// That is the whole difference between this and "no marker here".
pub fn lazy_continuation(line: &LazyLine<'_>) -> bool {
    line.depth < line.quoted
        && line.paragraph
        && !strip(line.text).is_empty()
        // On the raw line, not the peel. `strip_containers` has already
        // taken the marker off `peeled`. So this test could never fire, and
        // a list that left a quote read as a continuation of it.
        //
        // This is not gated on the paragraph, unlike `starts_block`. The
        // paragraph of the quote is not open at the level where this line
        // lands. Rustdoc closes the quote and opens "<ol start=\"2\">". So
        // even a marker that cannot interrupt a paragraph starts a block
        // here.
        && list_content(line.text, line.container).is_none()
        && !table_header(line.pieces, line.index, line.nest, line.peeled, line.container)
        && !heading(line.peeled, line.container)
        // This applies to doc comments only. Nothing renders a `//`
        // comment, so "<pre>" there is five literal characters and no block
        // at all. Each other test here holds in both kinds. A heading, a rule
        // and a list marker are block syntax that this tree writes in plain
        // comments too. So HTML is the one test that takes the marker.
        && !html_block(line.peeled, line.container, line.doc)
        // The CommonMark break, not the broad decorative rule.
        // `SEPARATOR_RE` answers "is this a section rule?". It has no bound
        // on purpose, because a rule is not a word of a sentence at any
        // indent. That is the question of `prose_units`. This asks if a
        // block begins, and there the answers differ twice. "===" is
        // ordinary text to CommonMark, and a "---" with too much indent is
        // indented content. Rustdoc keeps both inside the quoted paragraph.
        && !thematic_break(line.peeled, line.container)
        && !opens_fence(line.peeled, line.container)
}

/// Removes container markers from `text` until a fence delimiter can show.
///
/// One line can open several containers at once. "- > ```rust" is a list
/// item that holds a block quote that holds a fence, and "- 1. ```rust" is
/// two lists. Each marker starts the content column again. So they are
/// removed one at a time, and the next is measured in the frame that the
/// last one left.
///
/// The marker decides which frame that is. A list marker on this line opens
/// an item with nothing in it yet, so the next frame starts at zero. A
/// quote enters a depth that can already have a container in the stack,
/// such as a quoted list item. Its column is the one recorded there.
///
/// `paragraph` says that the first list marker that this loop removes
/// cannot interrupt an open paragraph. A "2." right after open prose cannot
/// interrupt it. So `update_containers` opens no container for it, and to
/// strip it here anyway invents a fence that `update_containers` never
/// agreed to. A quote marker leaves the gate in force. A paragraph open at
/// the same depth that the quote reaches is the same paragraph. So "2."
/// still cannot interrupt it past one. To enter or leave a quote is a block
/// boundary, though. So the caller passes `false` once a quote marker here
/// reaches a depth where the paragraph was not open. A marker past a list
/// marker opens fresh content in any case, so the gate applies once and
/// stops there.
///
/// It returns the remaining text, the container to measure the delimiter
/// against, the quote depth reached, and the column of the content. The
/// last two are the scope of the fence, and neither can come from the raw
/// line. A quote behind a list marker is invisible there. A fence inside
/// "- 1. " belongs to the inner item, not the outer one.
pub fn strip_containers<'t>(
    text: &'t str,
    stack: &[Container],
    container: usize,
    paragraph: bool,
) -> (&'t str, usize, usize, usize) {
    let mut text = text;
    let mut container = container;
    let mut depth = 0;
    let mut column = container;
    let mut origin = 0;
    let mut first = true;
    loop {
        if let Some((indent, end)) = quote_marker(text, container) {
            // One match can hold several markers (">>", "> >").
            depth += text[indent..end].matches('>').count();
            text = tail(text, end);
            container = container_at_depth(stack, depth);
            column = container;
            origin = 0;
            continue;
        }
        if let Some(content) = list_content(text, container) {
            if first
                && paragraph
                && !LIST_MARKER_RE
                    .match_start(text)
                    .is_some_and(|m| interrupts_paragraph(&m, text))
            {
                return (text, container, depth, column);
            }
            text = tail(text, content.1);
            origin += content.0;
            column = origin;
            container = 0;
            first = false;
            continue;
        }
        return (text, container, depth, column);
    }
}

/// A fence delimiter on a line.
pub struct Delimiter<'t> {
    /// The `FENCE_RE` match, in `tail`.
    pub found: Caps,
    /// The text that the delimiter was matched in, with its info string.
    pub tail: &'t str,
    /// The quote depth that the line reached.
    pub reached: usize,
    /// The column of the content.
    pub column: usize,
}

/// The fence delimiter on `text`, or `None` if the line is not one.
///
/// Two things separate a delimiter from ordinary text. A fence can open on
/// the same line as the container markers that hold it: a list item, a
/// block quote, or several nested. So those are removed before the match.
/// And CommonMark allows at most three columns of indent relative to the
/// container, for closers as well as openers. Past that the line is
/// indented content. That is fenced content while a fence is open, and an
/// indented code line while one is not.
///
/// `paragraph` goes straight to `strip_containers`. A marker that cannot
/// interrupt the paragraph open on entry to this line opens no container.
/// So no fence behind it is real either.
pub fn fence_delimiter<'t>(
    text: &'t str,
    container: usize,
    in_fence: bool,
    depth: usize,
    stack: &[Container],
    paragraph: bool,
) -> Option<Delimiter<'t>> {
    // Markers are removed only as far as they are syntax. Inside a fence
    // the sample text is literal. "- ```" is a hyphen and three backticks
    // of example content, not an item whose body closes the fence. A "> "
    // deeper than the container of the fence is sample text too.
    let (tail, container, reached, column) = if in_fence {
        (
            strip_quote_levels(text, depth, container),
            container,
            depth,
            container,
        )
    } else {
        strip_containers(text, stack, container, paragraph)
    };
    let found = FENCE_RE.match_start(tail)?;
    if expanded_len(found.group(tail, 1)) > container + 3 {
        return None;
    }
    Some(Delimiter {
        found,
        tail,
        reached,
        column,
    })
}

/// Applies one fence delimiter line to the fence state.
///
/// This is CommonMark, not "any three backticks toggle it". A closer must
/// use the same character as its opener and be at least as long, with
/// nothing after it. Otherwise a ``` line inside an example fenced with
/// ```` closes the fence early. Then the sample text of the example reads
/// as real comments.
///
/// `tail` is what `fence_delimiter` matched: the line past any list marker.
/// So the end of the match indexes into it, not into the raw line.
pub fn fence_transition(fence: Option<Fence>, found: &Caps, tail: &str) -> Option<Fence> {
    let delimiter = found.group(tail, 2);
    let char = delimiter.chars().next().unwrap_or('`');
    let length = delimiter.len();
    let info = super::pyre::tail(tail, found.end());
    let Some((open_char, open_length)) = fence else {
        // An opener. An info string ("```rust") is allowed. But CommonMark
        // forbids a backtick inside the info string of a backtick fence. So
        // "```foo`bar" is not a fence at all. To treat it as one opens a
        // fence that never closes and suppresses the rest of the run.
        if char == '`' && info.contains('`') {
            return None;
        }
        return Some((char, length));
    };
    let closes = char == open_char && length >= open_length && strip(info).is_empty();
    if closes { None } else { fence }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_content_counts_columns() {
        assert_eq!(list_content("- a", 0), Some((2, 2)));
        assert_eq!(list_content("-     a", 0), Some((2, 2)));
        assert_eq!(list_content("-", 0), Some((2, 2)));
        assert_eq!(list_content("    - a", 0), None);
        assert_eq!(list_content("* * *", 0), None);
    }

    #[test]
    fn only_one_may_interrupt_a_paragraph() {
        let text = "2. b";
        let marker = LIST_MARKER_RE
            .match_start(text)
            .map(|m| interrupts_paragraph(&m, text));
        assert_eq!(marker, Some(false));
        let text = "01. b";
        let marker = LIST_MARKER_RE
            .match_start(text)
            .map(|m| interrupts_paragraph(&m, text));
        assert_eq!(marker, Some(true));
    }

    #[test]
    fn a_delimiter_row_needs_the_width_of_its_header() {
        assert!(table_delimiter("| - |", 0, "| a |"));
        assert!(!table_delimiter("| - |", 0, "Intro | header |"));
        assert!(!table_delimiter("| | --- |", 0, "| a | b |"));
    }

    #[test]
    fn cell_splits_match_their_pattern() {
        use super::super::patterns::CELL_SPLIT_RE;
        for text in ["", "|", "a|b", "a\\|b|c", "\\||", "||"] {
            let mut want = Vec::new();
            let mut pos = 0;
            while let Some((start, end)) = CELL_SPLIT_RE.find_from(text, pos) {
                want.push(start);
                pos = end;
            }
            assert_eq!(
                cell_splits(text.as_bytes(), 0, text.len()),
                want,
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_closer_must_match_its_opener() {
        let open = FENCE_RE
            .match_start("````rust")
            .map(|m| fence_transition(None, &m, "````rust"));
        assert_eq!(open, Some(Some(('`', 4))));
        let short = FENCE_RE
            .match_start("```")
            .map(|m| fence_transition(Some(('`', 4)), &m, "```"));
        assert_eq!(short, Some(Some(('`', 4))));
    }

    #[test]
    fn quote_markers_stop_before_an_operator() {
        assert_eq!(strip_quote("> a", 0), "a");
        assert_eq!(strip_quote(">= a", 0), ">= a");
        assert_eq!(quote_depth("> > a", 0), 2);
    }
}
