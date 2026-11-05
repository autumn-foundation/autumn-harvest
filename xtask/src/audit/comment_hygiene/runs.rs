//! Comment runs, and the two scanners that walk them.
//!
//! `comment_lines` gives each comment line with its fence state, for the
//! line rules. `prose_units` joins comment prose into units, for the
//! sentence rules. Both carry the same block state, so a fix to one of them
//! usually belongs in the other too.

use super::lexer::{Piece, is_doc};
use super::markdown::closes_an_open_paren;
use super::markdown::{
    Container, Fence, LazyLine, fence_delimiter, fence_transition, heading, html_block,
    html_closes, html_kind, indented_code, interrupts_paragraph, lazy_continuation,
    leaves_container, line_tail, quote_depth, row_cell_spans, setext_underline, starts_block,
    strip_containers, strip_quote, table_header, thematic_break, update_containers,
};
use super::patterns::LIST_MARKER_RE;
use super::pyre::{strip, tail};
use super::shapes::{one_quote, separator, task_marker};

/// A scope: the outer container, the content column and the quote depth.
type Scope = (usize, usize, usize);

/// Groups pieces into blocks of contiguous comment.
///
/// A run is what a reader sees as one comment: consecutive lines of the
/// same kind. A trailing comment is always its own run. It is a note on its
/// own line, not a continuation of the note on the line above, even when
/// the two are adjacent.
///
/// "Consecutive" is what a reader sees, not what the file holds. A doc run
/// survives an attribute between its lines, because Rustdoc joins the
/// `#[doc]` attributes on either side into one document. See
/// `mark_bridges`. It survives a comment written inside that attribute for
/// the same reason. Such a comment is part of the attribute. So it is set
/// aside as its own run, and it does not cut the document in two. A plain
/// `//` run survives neither. Nothing renders it, so nothing joins it.
pub fn comment_runs<'p>(pieces: &[&'p Piece]) -> Vec<Vec<&'p Piece>> {
    let mut runs = Vec::new();
    let mut run: Vec<&Piece> = Vec::new();
    let mut held: Vec<&Piece> = Vec::new();
    let mut prev_line: i64 = -2;
    for &piece in pieces {
        // A comment inside an attribute belongs to the attribute. It is
        // still judged, as a run of its own. But the document on either side
        // of the attribute is one document, so it must not break the run.
        if piece.in_attribute {
            // Held pieces group by the same rules as any other run. So two
            // comments in two different attributes stay two comments.
            if let Some(last) = held.last()
                && (piece.trailing
                    || last.trailing
                    || piece.marker != last.marker
                    || piece.line != last.line + 1)
            {
                runs.push(std::mem::take(&mut held));
            }
            held.push(piece);
            continue;
        }
        if !held.is_empty() {
            runs.push(std::mem::take(&mut held));
        }
        if let Some(last) = run.last() {
            let same_block = piece.block && last.block && piece.group == last.group;
            let line = i64::try_from(piece.line).unwrap_or(i64::MAX);
            if !same_block
                && (piece.trailing
                    || last.trailing
                    || piece.block != last.block
                    || piece.group != last.group
                    || piece.marker != last.marker
                    || (line != prev_line + 1 && !(piece.bridged && is_doc(&piece.marker))))
            {
                runs.push(std::mem::take(&mut run));
            }
        }
        run.push(piece);
        prev_line = i64::try_from(piece.line).unwrap_or(i64::MAX);
    }
    if !held.is_empty() {
        runs.push(held);
    }
    if !run.is_empty() {
        runs.push(run);
    }
    runs
}

/// Carries block state across the boundary of a nested comment.
///
/// To go deeper inherits the enclosing state. A nested comment inside a
/// fenced example is part of that example, so its text is sample text. To
/// come back out restores what was saved. So nothing that the nested
/// comment opened leaks into the text that resumes after it closes.
///
/// `state` is each part of the block state that the caller carries, not
/// only the fence. A list marker inside a nested comment is ordinary
/// paragraph text to Rustdoc. If it pushes a container onto the stack of
/// the enclosing run, a later delimiter gets a fence allowance that nothing
/// opened.
///
/// Each caller must check "each part" by hand. A variable added to a loop
/// but not to its state leaks in silence. `in_table` once did that. A table
/// inside a nested comment left the enclosing run with pipe lines read as
/// table rows, so their paragraph closed early.
fn nesting_shift<S: Clone>(saved: &mut Vec<S>, nest: usize, state: S) -> S {
    let mut state = state;
    while saved.len() < nest {
        saved.push(state.clone());
    }
    while saved.len() > nest {
        if let Some(popped) = saved.pop() {
            state = popped;
        }
    }
    state
}

/// The block state of `comment_lines`.
#[derive(Clone, Default)]
struct LineState {
    fence: Option<Fence>,
    scope: Scope,
    /// The container where an open HTML block started, read by
    /// `leaves_container` as the `scope` of a fence is. Without it, a
    /// line could leave the quote or list item that opened an HTML block.
    /// Then the block stayed open over each later line in the run (#1383).
    html_scope: Scope,
    /// The container where an open table started. See `html_scope`.
    table_scope: Scope,
    stack: Vec<Container>,
    paragraph: bool,
    quoted: usize,
    in_table: bool,
    indented: bool,
    html: Option<&'static str>,
}

/// One comment line, as `comment_lines` gives it.
pub struct LineOut {
    pub line: usize,
    pub text: String,
    /// The line is fenced, raw HTML or indented code, so the rules skip it.
    pub in_fence: bool,
    /// The line begins a block, so no code span reaches across it.
    pub opens: bool,
    /// The cell spans of a confirmed table row.
    pub cells: Option<Vec<(usize, usize)>>,
}

/// The container column at the top of `stack`, or zero.
fn top(stack: &[Container]) -> usize {
    stack.last().map_or(0, |entry| entry.0)
}

/// Sets the HTML state to closed in each snapshot that waits for restore.
///
/// A close that the enclosing line drives is a fact about the outer text,
/// not about the nested comment being read. That is a closer found in a
/// nested comment, or a line that leaves the container where an HTML block
/// or a table opened. `nesting_shift` restores an older snapshot when the
/// nesting returns to a shallower level. So a close made only to the live
/// value is undone right there, unless the same close reaches each
/// snapshot that still waits. To open new state while nested is different.
/// The restore of `nesting_shift` already keeps that isolated.
fn close_html<S>(saved: &mut [S], field: impl Fn(&mut S)) {
    saved.iter_mut().for_each(field);
}

/// Each comment piece as a `LineOut`, with its ``` fence state.
///
/// The fence state is per run, never for the whole file. Otherwise an
/// unclosed ``` in one doc block keeps the fence open for each later
/// comment in the file. Then the scan skips all of them in silence. That is a gate
/// that stops gating without failure. A fence delimiter comes out with
/// `in_fence` set, so callers skip it with the fenced body.
#[allow(clippy::too_many_lines)]
pub fn comment_lines(pieces: &[&Piece]) -> Vec<LineOut> {
    let mut out = Vec::new();
    // The Python original kept these as locals of the whole generator. A
    // later line reads the value of an earlier one, so they stay outside
    // the loops.
    let mut marker_paragraph = false;
    let mut enclosing = 0;
    for run in comment_runs(pieces) {
        let mut st = LineState::default();
        // A one-line block ended on the line before, so this line begins a
        // new one. `starts_block` answers "does a block begin here". Each
        // block of several lines also answers that on its own first line.
        // But a heading is one line, and the paragraph under it says
        // nothing.
        let mut after_block = false;
        let mut saved: Vec<LineState> = Vec::new();
        let mut nest = run[0].nest;
        // The physical line where a closer was last found. CommonMark ends
        // an HTML block at its closing line, not at the column of the
        // closer. So a piece that follows the closer on the same line is raw
        // HTML too, even though `html` is already clear when it is reached.
        // That happens after a nested comment resumes the enclosing one.
        let mut html_closing_line: Option<usize> = None;
        for (index, &piece) in run.iter().enumerate() {
            if piece.nest != nest {
                st = nesting_shift(&mut saved, piece.nest, st);
                nest = piece.nest;
            }
            let text = piece.text.as_str();
            let doc = is_doc(&piece.marker);
            // A piece that does not begin its own line can open no block.
            // "Outer /* ```rust" puts the backticks after literal text, and
            // Rustdoc renders the whole line as a paragraph. To open a fence
            // there exempts each line up to the next delimiter.
            if !piece.line_start {
                // The characters of the closer are literal HTML text. So they
                // close the block wherever they sit, even in the middle of a
                // line inside a nested comment (#1383). A blank physical line
                // ends a "tag" block. A piece in the middle of a line is never
                // that, so it is not asked here.
                let was_html = st.html.is_some();
                if let Some(kind) = st.html
                    && kind != "tag"
                    && html_closes(text, kind)
                {
                    html_closing_line = Some(piece.line);
                    st.html = None;
                    // See `close_html`: the closer is final at each nesting
                    // depth that still waits for it.
                    close_html(&mut saved, |frame| frame.html = None);
                }
                // This is still raw HTML if the piece is on the line of the
                // closer, even after `html` is clear. That is text between
                // the delimiters of the nested comment and the closer, or text
                // after the closer. Both are part of the closing line, which
                // CommonMark keeps inside the block.
                let closing_line = html_closing_line == Some(piece.line);
                out.push(LineOut {
                    line: piece.line,
                    text: text.to_string(),
                    in_fence: st.fence.is_some() || was_html || closing_line,
                    opens: false,
                    cells: None,
                });
                continue;
            }
            // This comes from the line before, and it is cleared here, so it
            // applies exactly once. A piece that does not begin its own line
            // is the same line, so it must not consume the carry.
            let mut opens = after_block;
            after_block = false;
            // Did the line leave the container where an open HTML block or
            // table started? CommonMark ends both with their container, as
            // it does a fence. See `leaves_container`. This check comes
            // before anything below reads `html` or `in_table`. So a line
            // that already left holds neither state when either is asked.
            if st.html.is_some() && leaves_container(text, st.html_scope) {
                st.html = None;
                // See `close_html`. This line can be an interior line of a
                // nested comment. Such a line starts its own source line, but
                // it sits deeper than where the block opened. So the close
                // must reach each depth that still waits for it.
                close_html(&mut saved, |frame| frame.html = None);
            }
            if st.in_table && leaves_container(text, st.table_scope) {
                st.in_table = false;
                close_html(&mut saved, |frame| frame.in_table = false);
            }
            // Inside a raw HTML block nothing is Markdown. So no fence, list
            // or table opens here, and the text is not prose. A type-6 block
            // ends at a blank line, which is a block boundary itself and must
            // still be seen. The others end on the line that has their
            // closer.
            if let Some(kind) = st.html {
                let inside = strip_quote(text, top(&st.stack));
                if kind == "tag" && html_closes(inside, kind) {
                    st.html = None;
                    // See `close_html`. A blank line closes a type-6 block as
                    // a closer does. It can be an interior line of a nested
                    // comment in the same way.
                    close_html(&mut saved, |frame| frame.html = None);
                } else {
                    if html_closes(inside, kind) {
                        html_closing_line = Some(piece.line);
                        st.html = None;
                        close_html(&mut saved, |frame| frame.html = None);
                    }
                    out.push(LineOut {
                        line: piece.line,
                        text: text.to_string(),
                        in_fence: true,
                        opens: false,
                        cells: None,
                    });
                    continue;
                }
            }
            if st.fence.is_some() && leaves_container(text, st.scope) {
                st.fence = None;
            }
            if st.fence.is_none() {
                // A confirmed table is a block, and it ends the paragraph. A
                // bare pipe line is not, and it must not end it. The block
                // layer now has the structure to tell them apart.
                enclosing = top(&st.stack);
                let body = strip_quote(text, enclosing);
                // `table_scope` is set below, once `stack` has the marker of
                // this line. A table can open on the same line as its list
                // item ("- | h |"). It must not measure the column of the item
                // in the frame where the line arrived.
                let mut table_opened = false;
                if st.in_table {
                    if starts_block(body, enclosing, false, true) {
                        st.in_table = false;
                        // See `close_html`: this can be an interior line of a
                        // nested comment that ends the table.
                        close_html(&mut saved, |frame| frame.in_table = false);
                    }
                } else if table_header(&run, index, piece.nest, body, enclosing) {
                    st.in_table = true;
                    table_opened = true;
                }
                // Measured past the markers, in the frame that the removal
                // leaves. A code block can begin on the marker line itself.
                // "-     let x = compute();" is an item that holds four
                // columns of indented code, not a bullet with a defect in it.
                let (code_text, code_container, _, _) =
                    strip_containers(text, &st.stack, enclosing, st.paragraph);
                st.indented =
                    indented_code(code_text, code_container, st.paragraph, st.indented, doc);
                // `body` has lost its quote marker, so `starts_block` cannot
                // see one any more. And a quote is a block. The depth says so,
                // and it says so for the line that leaves a quote too, which
                // no marker on the line could.
                let before_quoted = st.quoted;
                // The paragraph state from before `update_containers` clears
                // it. An underline with no paragraph above it is a thematic
                // break or ordinary text, not a heading. Both the lazy test
                // and the Setext test need the state where this line arrived.
                let open_paragraph = st.paragraph;
                // Both are blocks, so neither leaves a paragraph open.
                let (stack, paragraph) = update_containers(
                    text,
                    &st.stack,
                    st.paragraph,
                    st.quoted,
                    st.in_table || st.indented,
                    doc,
                );
                st.stack = stack;
                st.paragraph = paragraph;
                let container = top(&st.stack);
                // The marker veto of `strip_containers` still applies past a
                // quote that lands back at the depth where the paragraph was
                // already open. To enter or leave a quote ends a paragraph,
                // not the marker behind it. So only the caller can drop the
                // veto here.
                marker_paragraph = open_paragraph && quote_depth(text, container) == before_quoted;
                // From the removal of the markers, as `prose_units` reads it.
                // "- > text" is a quote inside a list item. The raw line
                // reports depth zero, because the list marker comes first.
                // Then one loop believes that the line left the quote, and
                // the other does not.
                let (peeled_now, _, quoted, reached_column) =
                    strip_containers(text, &st.stack, container, marker_paragraph);
                st.quoted = quoted;
                // Read after `update_containers`, like `html_scope`. Take a
                // table that the list marker of this same line opened. Its
                // scope is the column that the marker just pushed, not the
                // frame where it arrived.
                if table_opened {
                    st.table_scope = (container, reached_column, st.quoted);
                }
                // A quoted paragraph can continue across a line with no ">"
                // of its own. That line leaves the quote by depth and stays
                // inside the block. A span cut there split one quoted
                // sentence in half. It failed the build on a marker that
                // Rustdoc renders inside a code span.
                let lazy = lazy_continuation(&LazyLine {
                    text,
                    peeled: peeled_now,
                    container: enclosing,
                    depth: st.quoted,
                    quoted: before_quoted,
                    paragraph: open_paragraph,
                    pieces: &run,
                    index,
                    nest: piece.nest,
                    doc,
                });
                // A confirmed table and a Setext heading are blocks that
                // `starts_block` cannot name. A table needs the piece after
                // this one to confirm it. A Setext underline needs the
                // paragraph above it. So neither is a fact about one line.
                // Rustdoc renders the two halves of a span literally across
                // either. Each row is its own boundary. "| x `open |" and
                // "| close` y |" are separate cells, and the backticks stay
                // literal in both.
                //
                // This applies to doc comments only. Rustdoc renders no `//`
                // comment. So a pipe row there is text, and a rule of "=" is
                // a banner that this tree draws under a plain heading.
                //
                // A Setext underline underlines the paragraph above it, and
                // only one in its own container. A line that leaves a quote
                // does not underline the quoted paragraph before it. Rustdoc
                // keeps "===" under a lazily continued quote as ordinary
                // text, in the paragraph, not as a heading rule.
                let setext = open_paragraph
                    && setext_underline(body, enclosing)
                    && st.quoted == before_quoted;
                // A leaf block that is exactly one line long. Rustdoc renders
                // "# Explain the `literal" as a heading, and the line under
                // it as its own paragraph. So a span cannot pair across the
                // two. And nothing on the line of the paragraph says that a
                // block began, because none did. It ended.
                after_block =
                    heading(body, enclosing) || thematic_break(body, enclosing) || (doc && setext);
                opens = opens
                    || starts_block(body, enclosing, open_paragraph, doc)
                    || (st.quoted != before_quoted && !lazy)
                    || (doc && (st.in_table || setext));
            }
            // Computed again in all cases. The block above is skipped while
            // `fence` is already open, and this line still needs a fresh
            // container to test the delimiter against.
            let container = top(&st.stack);
            let mut delimiter = fence_delimiter(
                text,
                container,
                st.fence.is_some(),
                st.scope.2,
                &st.stack,
                marker_paragraph,
            );
            // The whole line decides a delimiter, not this piece alone. Only
            // spaces can follow a closer. The info string of an opener runs
            // to the end of the line, where a backtick makes it invalid.
            // Text after a nested comment on the same line is part of both.
            if let Some(found) = &delimiter {
                let rest = line_tail(&run, index);
                // A backtick is forbidden in the info string of a backtick
                // fence, and nowhere else. "~~~rust `info`" is a valid
                // opener. To refuse it discarded the fence and reported its
                // content.
                let backtick = found.found.group(found.tail, 2).starts_with('`');
                // The info string is the rest of this piece past the
                // delimiter, plus any text after a nested comment on the same
                // line. A read of only the tail across pieces missed the
                // ordinary one-piece form, "```rust `info`".
                let info = format!("{}{rest}", tail(found.tail, found.found.end()));
                if (st.fence.is_some() && (!strip(&rest).is_empty() || !piece.line_end))
                    || (st.fence.is_none() && backtick && info.contains('`'))
                {
                    delimiter = None;
                }
            }
            if let Some(found) = delimiter {
                let before = st.fence;
                st.fence = fence_transition(st.fence, &found.found, found.tail);
                // A fence is a block, so it ends the paragraph before it.
                // Otherwise the opener leaves `paragraph` set, and the next
                // "22." is refused a container that it has a right to.
                st.paragraph = false;
                if before.is_none() && st.fence.is_some() {
                    // The container matters here too. A quote nested in a list
                    // item has an indent past column zero. A depth read
                    // without the container saves zero, and then the fence
                    // never sees its own closing delimiter. The saved column
                    // must come from the same frame as that depth. Both come
                    // from the removal of the markers, not from the raw line.
                    // A quote behind a list marker is invisible there, and a
                    // fence inside "- 1. " belongs to the inner item.
                    st.scope = (container, found.column, found.reached);
                }
                // Fence syntax only if it opened one, closed one, or sits
                // inside one. An invalid opener (```foo`bar) is ordinary
                // text, and it must still be scanned. To exempt it would hide
                // the defect that its refusal exists to expose.
                out.push(LineOut {
                    line: piece.line,
                    text: text.to_string(),
                    in_fence: st.fence.is_some() || before.is_some(),
                    opens: true,
                    cells: None,
                });
                continue;
            }
            // An HTML block opens here, outside any fence. Its own line is
            // ordinary text, because "<pre>" holds no defect. But each line
            // up to its closer is raw HTML.
            //
            // The markers of each container are removed, as for the fence
            // test. "- <pre>" and "> <pre>" both open the block that their
            // container holds. A removal of only quote markers leaves the
            // list marker in front of the tag and recognizes neither.
            let (peeled, _, reached_depth, reached_column) =
                strip_containers(text, &st.stack, container, marker_paragraph);
            // This applies to doc comments only, as indented code and Setext
            // do. Rustdoc renders no `//` comment, so "<pre>" in one is text,
            // not markup. To exempt the run took CH001 and CH002 off each
            // line inside it. A ``` fence stays exempt in any comment. That
            // is an author who says "this is an example", which a tag in a
            // comment that nothing renders is not.
            if st.fence.is_none() && html_block(peeled, container, doc) {
                // The container where this line opened the block, read as
                // the `scope` of a fence is. `leaves_container` ends the
                // block on the line where the container ends, with or
                // without a closer on that line.
                st.html_scope = (container, reached_column, reached_depth);
                let kind = html_kind(peeled);
                st.html = Some(kind);
                if kind != "tag" && html_closes(peeled, kind) {
                    // It closes on the piece of the opener itself. A nested
                    // comment can still split the rest of this physical line
                    // into later pieces, and those stay raw HTML too.
                    html_closing_line = Some(piece.line);
                    st.html = None;
                }
                // The line of the opener is inside the block that it opens.
                // Rustdoc renders `<pre>TODO: x</pre>` preformatted, so the
                // line rules must not read that marker as a defect.
                out.push(LineOut {
                    line: piece.line,
                    text: text.to_string(),
                    in_fence: true,
                    opens: true,
                    cells: None,
                });
                continue;
            }
            // A confirmed table row brings its cell boundaries with it. They
            // are the one block boundary that falls inside a line. So the
            // span pass cannot get them from `opens` alone.
            let cells =
                (st.in_table && doc && st.fence.is_none()).then(|| row_cell_spans(text, enclosing));
            out.push(LineOut {
                line: piece.line,
                text: text.to_string(),
                in_fence: st.fence.is_some() || st.indented,
                opens,
                cells,
            });
        }
    }
    out
}

/// The block state of `prose_units`.
// The flags match the state tuple of the Python original one to one.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Default)]
struct ProseState {
    fence: Option<Fence>,
    scope: Scope,
    /// The container where an open HTML block started. See `comment_lines`,
    /// which tracks this in the same way (#1383).
    html_scope: Scope,
    table_scope: Scope,
    stack: Vec<Container>,
    paragraph: bool,
    quoted: usize,
    in_list: bool,
    in_table: bool,
    indented: bool,
    html: Option<&'static str>,
}

/// A unit of comment prose, ready to split into sentences.
pub struct Unit {
    /// The joined text.
    pub text: String,
    /// The offset of each fragment in `text`, with its source line.
    pub spans: Vec<(usize, usize)>,
    /// The run is a doc comment.
    pub doc: bool,
}

/// The fragments of one unit, as they gather.
#[derive(Default)]
struct Gather {
    run: Vec<String>,
    lines: Vec<usize>,
}

impl Gather {
    fn flush(&mut self, units: &mut Vec<Unit>, doc: bool) {
        if !self.run.is_empty() {
            // The offset of each fragment in the joined text, with the
            // source line it came from. A join loses that otherwise. Then
            // each sentence in the unit reports the first line of the unit,
            // which points a contributor at an unrelated comment.
            let mut spans = Vec::with_capacity(self.run.len());
            let mut position = 0;
            for (fragment, &line) in self.run.iter().zip(&self.lines) {
                spans.push((position, line));
                position += fragment.len() + 1;
            }
            units.push(Unit {
                text: self.run.join(" "),
                spans,
                doc,
            });
        }
        self.run.clear();
        self.lines.clear();
    }
}

/// Comment prose as units, ready to split into sentences.
///
/// Fenced blocks, Markdown tables and headings are dropped. They are not
/// prose, and a word count over them means nothing. A list item starts its
/// own unit, so a rationale in bullets is measured per bullet.
///
/// Units never cross a comment run. So two adjacent trailing comments stay
/// two units. To join them on line adjacency alone would report two short
/// notes as one long sentence. That is a false CH007 on code that neither
/// author wrote as a sentence.
#[allow(clippy::too_many_lines)]
pub fn prose_units(pieces: &[&Piece]) -> Vec<Unit> {
    let mut units = Vec::new();
    // The Python original kept these as locals of the whole function. A
    // later line reads the value of an earlier one, so they stay outside
    // the loops.
    let mut open_paragraph = false;
    let mut table_opened = false;
    for block in comment_runs(pieces) {
        let mut gather = Gather::default();
        let mut st = ProseState::default();
        let mut saved: Vec<ProseState> = Vec::new();
        let mut nest = block[0].nest;
        // One run has one marker, because `comment_runs` splits where it
        // changes. The absolute rules need it to know if inline HTML in this
        // text is markup or only characters.
        let doc_run = is_doc(&block[0].marker);
        // See the matching comment in `comment_lines`. This is the line
        // where a closer was last found. So a piece that follows it on the
        // same line stays inside the block, even after `html` is clear.
        let mut html_closing_line: Option<usize> = None;
        for (index, &piece) in block.iter().enumerate() {
            if piece.nest != nest {
                // No flush. A sentence that crosses an inline nested comment
                // is still one sentence in the rendered documentation. The
                // delimiters and the text between them are literal. The nesting isolates only the block state.
                st = nesting_shift(&mut saved, piece.nest, st);
                nest = piece.nest;
            }
            let doc = is_doc(&piece.marker);
            let body = piece.text.as_str();
            // In the middle of a line, so it has no block marker of its own.
            // See `comment_lines`. The text is still part of the sentence of
            // the line, so it joins the run and does not start one.
            if !piece.line_start {
                if st.fence.is_some() || st.html.is_some() || html_closing_line == Some(piece.line)
                {
                    // The characters of the closer are literal HTML text. So
                    // they close the block wherever they sit. See
                    // `comment_lines`.
                    if let Some(kind) = st.html
                        && kind != "tag"
                        && html_closes(body, kind)
                    {
                        html_closing_line = Some(piece.line);
                        st.html = None;
                        close_html(&mut saved, |frame| frame.html = None);
                    }
                    gather.flush(&mut units, doc_run);
                    st.in_list = false;
                } else if !strip(body).is_empty() {
                    gather.run.push(strip(body).to_string());
                    gather.lines.push(piece.line);
                }
                continue;
            }
            // Did the line leave the container where an open HTML block or
            // table started? See `comment_lines`, which applies this in the
            // same way.
            if st.html.is_some() && leaves_container(body, st.html_scope) {
                st.html = None;
                // See `close_html`: this can be an interior line of a nested
                // comment.
                close_html(&mut saved, |frame| frame.html = None);
            }
            if st.in_table && leaves_container(body, st.table_scope) {
                st.in_table = false;
                close_html(&mut saved, |frame| frame.in_table = false);
            }
            // Raw HTML is not Markdown and not prose. See `comment_lines`,
            // which carries this state in the same way.
            if let Some(kind) = st.html {
                let inside = strip_quote(body, top(&st.stack));
                if kind == "tag" && html_closes(inside, kind) {
                    st.html = None;
                    close_html(&mut saved, |frame| frame.html = None);
                } else {
                    if html_closes(inside, kind) {
                        html_closing_line = Some(piece.line);
                        st.html = None;
                        close_html(&mut saved, |frame| frame.html = None);
                    }
                    gather.flush(&mut units, doc_run);
                    st.in_list = false;
                    continue;
                }
            }
            if st.fence.is_some() && leaves_container(body, st.scope) {
                st.fence = None;
            }
            if st.fence.is_none() {
                // The table is decided before the containers, because a
                // confirmed table is a block and ends the paragraph above
                // it. `comment_lines` did this first. Without it here, a
                // "22." after a table was refused its container, and the
                // fence under it went unseen.
                let enclosing = top(&st.stack);
                let peek = strip_quote(body, enclosing);
                // `table_scope` is set below, once `stack` has the marker of
                // this line. See the matching comment in `comment_lines`.
                table_opened = false;
                if st.in_table {
                    if starts_block(peek, enclosing, false, true) {
                        st.in_table = false;
                        close_html(&mut saved, |frame| frame.in_table = false);
                    }
                } else if table_header(&block, index, piece.nest, peek, enclosing) {
                    st.in_table = true;
                    table_opened = true;
                }
                let (code_text, code_container, _, _) =
                    strip_containers(body, &st.stack, enclosing, st.paragraph);
                st.indented =
                    indented_code(code_text, code_container, st.paragraph, st.indented, doc);
                // Saved before `update_containers` changes it, as in
                // `comment_lines`. A marker on this line is judged against
                // the paragraph where it arrived, not the one it leaves.
                open_paragraph = st.paragraph;
                // Both are blocks, so neither leaves a paragraph open.
                let (stack, paragraph) = update_containers(
                    body,
                    &st.stack,
                    st.paragraph,
                    st.quoted,
                    st.in_table || st.indented,
                    doc,
                );
                st.stack = stack;
                st.paragraph = paragraph;
            }
            let container = top(&st.stack);
            // The marker veto of `strip_containers` still applies past a
            // quote that lands back at the depth where the paragraph was
            // already open. See the matching comment in `comment_lines`.
            // `quoted` is still the value where this line arrived here. It is
            // not yet the value that it leaves with.
            let marker_paragraph = open_paragraph && quote_depth(body, container) == st.quoted;
            let mut delimiter = fence_delimiter(
                body,
                container,
                st.fence.is_some(),
                st.scope.2,
                &st.stack,
                marker_paragraph,
            );
            // The whole line decides a delimiter. See `comment_lines`.
            if let Some(found) = &delimiter {
                let rest = line_tail(&block, index);
                let backtick = found.found.group(found.tail, 2).starts_with('`');
                let info = format!("{}{rest}", tail(found.tail, found.found.end()));
                if (st.fence.is_some() && (!strip(&rest).is_empty() || !piece.line_end))
                    || (st.fence.is_none() && backtick && info.contains('`'))
                {
                    delimiter = None;
                }
            }
            if let Some(found) = delimiter {
                let before = st.fence;
                st.fence = fence_transition(st.fence, &found.found, found.tail);
                st.paragraph = false;
                if before.is_none() && st.fence.is_some() {
                    st.scope = (container, found.column, found.reached);
                }
                if st.fence.is_some() || before.is_some() {
                    gather.flush(&mut units, doc_run);
                    st.in_list = false;
                    continue;
                }
                // Not a valid fence: it falls through as prose.
            }
            if st.fence.is_some() {
                gather.flush(&mut units, doc_run);
                st.in_list = false;
                continue;
            }
            // A block quote is its own CommonMark block, so to cross into or
            // out of one ends the paragraph. Without this, an intro line and
            // the quote below it join into one sentence that neither author
            // wrote. Then a 25-word quoted sentence reports as 26.
            //
            // The markers are removed as the fence path removes them, for
            // the same reason. "- > text" has two markers. To strip only the
            // first leaves the other as a word of the sentence.
            let (peeled, _, depth, reached_column) =
                strip_containers(body, &st.stack, container, marker_paragraph);
            // Read after `update_containers`, like `html_scope`. See the
            // matching comment in `comment_lines`.
            if table_opened {
                st.table_scope = (container, reached_column, depth);
            }
            // A lazy continuation. A quoted paragraph can continue across a
            // line with no marker of its own, if that line is ordinary
            // paragraph text. A flush there splits one quoted sentence into
            // two short ones, and a long sentence gets past CH007. The test
            // is one predicate, shared with `comment_lines`. When only this
            // loop had it, the span pass reported a boundary on a line that
            // this loop knew was no boundary at all.
            let lazy = lazy_continuation(&LazyLine {
                text: body,
                peeled,
                container,
                depth,
                quoted: st.quoted,
                paragraph: !gather.run.is_empty(),
                pieces: &block,
                index,
                nest: piece.nest,
                doc,
            });
            if depth != st.quoted && !lazy {
                gather.flush(&mut units, doc_run);
                st.in_list = false;
                st.quoted = depth;
            }
            // Structure is read past the container markers, and so is the
            // prose. A container marker is not a word of the sentence.
            let body = strip_quote(body, container);
            // Indented code is a block and an example, not a sentence.
            if st.indented {
                gather.flush(&mut units, doc_run);
                st.in_list = false;
                continue;
            }
            if html_block(peeled, container, doc) {
                // The line of the opener is a block, and each line up to its
                // closer is raw HTML. `comment_lines` opens the block at the
                // same point, by the same test, on the same removal.
                st.html_scope = (container, reached_column, depth);
                let kind = html_kind(peeled);
                st.html = Some(kind);
                if kind != "tag" && html_closes(peeled, kind) {
                    // See the matching comment in `comment_lines`.
                    html_closing_line = Some(piece.line);
                    st.html = None;
                }
                gather.flush(&mut units, doc_run);
                st.in_list = false;
                continue;
            }
            // A Setext underline makes the run above it the title of a
            // heading, not a sentence. So the run is discarded, not flushed.
            // An ATX heading never reaches the run at all. A 26-word title
            // reported CH007 in one form and not in the other.
            //
            // This applies to doc comments only, for the reason that limits
            // indented code. Rustdoc renders no `//` comment, and this tree
            // closes a plain banner with a rule of hyphens. Sixty-six real
            // sentences sat above one. To discard them as heading titles
            // stopped the measure of prose that no renderer ever shows as a
            // title.
            //
            // And only a paragraph in its own container counts. A line that
            // leaves a quote underlines nothing. A discard of the run there
            // threw away a quoted sentence and the narrative aside inside it.
            if !gather.run.is_empty()
                && doc
                && setext_underline(peeled, container)
                && depth == st.quoted
            {
                gather.run.clear();
                gather.lines.clear();
                st.in_list = false;
                continue;
            }
            // Classified on the content past the markers, like the HTML test
            // beside it. Rustdoc renders "- # text" as a heading inside the
            // item. A read of the raw line here let the list branch add the
            // heading to the prose run as if it were a sentence.
            if st.in_table
                || heading(peeled, container)
                || separator(peeled)
                || strip(body).is_empty()
            {
                gather.flush(&mut units, doc_run);
                st.in_list = false;
                continue;
            }
            // `LIST_MARKER_RE`, not a second list pattern. Two patterns for
            // one concept drift. A second pattern here once missed the "1)"
            // ordered form. Then two short items merged into one sentence,
            // long enough to report a CH007 that neither author wrote.
            let mut marker = LIST_MARKER_RE.match_start(body);
            if marker
                .as_ref()
                .is_some_and(|m| closes_an_open_paren(m, body, &gather.run))
            {
                marker = None;
            }
            if let Some(m) = marker
                && (gather.run.is_empty() || st.in_list || interrupts_paragraph(&m, body))
            {
                gather.flush(&mut units, doc_run);
                // `peeled`, not the text after this one marker. The item
                // can hold a quote, or a second list, on the same line.
                // The removal applies the indent limit of CommonMark, and
                // this match does not. So a marker with more indent than
                // its container allows is a list here and indented code
                // there. The fallback keeps such a line as it always was,
                // and does not keep its bullet as a word of the sentence.
                let mut content = strip(if peeled.len() < body.len() {
                    peeled
                } else {
                    tail(body, m.end())
                });
                // A check box is not two words. Rustdoc renders "- [ ]
                // text" with an <input>. To keep the brackets adds two
                // words to each task item and reports a 24-word sentence
                // as 26. That is a CH007 regression on a comment that
                // complies.
                //
                // The marker binds to the list item that it opens. A quote
                // after the list marker means that the removal went
                // through it, and "[ ]" in quoted prose is two literal
                // words.
                if one_quote(tail(body, m.end())).is_none()
                    && let Some(end) = task_marker(content)
                {
                    content = strip(tail(content, end));
                }
                gather.run = vec![content.to_string()];
                gather.lines = vec![piece.line];
                st.in_list = true;
                continue;
            }
            gather.run.push(strip(body).to_string());
            gather.lines.push(piece.line);
        }
        gather.flush(&mut units, doc_run);
    }
    units
}
