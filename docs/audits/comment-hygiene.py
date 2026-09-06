#!/usr/bin/env python3
"""Folio corpus harness: comment hygiene across the Rust sources.

Deterministic, reproducible on any checkout -- pure filesystem, stdlib only,
no network and no build, so it runs in CI on every PR (see
docs/audits/README.md).

WHAT THIS DOES NOT DO
=====================

It does not shorten rationale. This codebase deliberately carries long
"why" comments that are load-bearing: the ABBA lock-ordering argument at
`materialize_due_child_timeout_deadlines`, the `cohort`-key correctness
argument in `partition.rs`, the codec-rotation scope guarantee that
CLAUDE.md cites as the proof exception #3 is safe. A reader who deletes
those to hit a word budget destroys the only record of why the code is
shaped the way it is. Length is not the defect; the defects below are.

So the rules split into two tiers, and the tier is the whole design:

TIER A -- absolute gates, must stay at zero
-------------------------------------------
Defects under any house style, mechanical to detect, few enough to have
been driven to zero when this harness landed. A new one fails the build.

  CH001 commented-out-code    Rust that was commented out instead of
                              deleted. Version control already remembers
                              it; a commented-out block rots silently
                              because no compiler or test ever reads it.
  CH002 todo-without-issue    A TODO/FIXME/XXX/HACK with no `#<issue>` or
                              URL. An unreferenced marker is a wish, not
                              a tracked commitment -- nothing will ever
                              route it to a person.
  CH003 narrative-aside       Deliberation left in the tree ("actually,
                              let's ...", "we'll ...", "not sure why").
                              It records the author thinking, not the
                              decision, and often contradicts the code
                              that shipped -- see the `ApiMethod::Delete`
                              aside this harness found in the CLI, which
                              argued against the variant the code beside
                              it already used.
  CH004 blank-comment-edge    A comment block that opens or closes on an
                              empty `//` line -- an editing artifact.

TIER B -- ratcheted against the merge base
------------------------------------------
Real style defects with a legacy population far too large to fix in one
change (CH007 alone is ~17.9k sentences). The rule for these is simply:
your change may not ADD one to a file it touches. Existing findings stay
until someone chooses to fix them.

The comparison reads the merge base out of git (`--base <ref>`, which CI
passes) and re-scans each changed file as it was there. There is no
checked-in baseline, deliberately -- a stored one goes stale the moment
the base branch moves (one merge adding a long comment anywhere turns
every open PR red for a file its author never opened), invites being
regenerated to launder a new violation, breaks on renames, and costs half
a megabyte of generated fingerprints in the tree.

Findings are matched by FINGERPRINT (rule + normalized text), not counted.
A count only answers "how many", so removing one legacy violation and
adding a different one in the same file nets to zero and passes. An
identity answers "which", so the new one is caught even though the total
never moved. A renamed file is compared against its own previous path; a
file the change adds has no merge-base version, so every Tier B finding in
it is the change's own.

Without `--base`, or when the diff cannot be computed (no git, an unknown
ref, a shallow clone with no reachable merge base), Tier B reports but
never fails: gating the whole corpus at the moment the tool cannot tell
what changed is the worst option available. Tier A is never scoped; it
gates everywhere, always.

  CH005 review-archaeology    "Codex round 8", "round-5", "P2 fix": the
                              review round that produced a change is
                              process trivia a future reader cannot look
                              up. The issue number is the durable handle;
                              cite that instead.
  CH006 contraction           ASD-STE100 forbids contractions ("isn't",
                              "let's"): they hurt non-native readers and
                              machine translation for no gain in brevity.
  CH007 long-sentence         Over 25 words, ASD-STE100's limit for
                              descriptive text. Applies per sentence, so
                              a long, well-structured rationale block
                              passes cleanly once it is written as
                              several sentences.

WHAT COUNTS AS A COMMENT
========================

Every Rust comment, found by a real lexer (`extract_comments`) rather than
a line regex: leading and trailing `//`, `///`, `//!`, and `/* */`
including the nested and doc forms. Text inside a string is NOT a comment,
which matters both ways round -- this corpus embeds Rust and SQL snippets
in raw strings (`det_check_tests.rs` fixtures carry `// harvest-suppress:`
lines; `chaos_catalogue_drift.rs` carries a literal commented-out call as
test data), and flagging one would fail CI on an innocent fixture.

Whole constructs are exempt because flagging them is meaningless, not
because they are above the rules: fenced code blocks (``` / ~~~) are
sample code, and markdown tables and headings are not prose.

Usage:
    python3 docs/audits/comment-hygiene.py [--json] [--tier-a-only]
    python3 docs/audits/comment-hygiene.py --self-test
    python3 docs/audits/comment-hygiene.py --base origin/trunk-dev
    python3 docs/audits/comment-hygiene.py --paths a.rs b.rs

Exit status is 1 on any Tier A finding or any Tier B regression, 0
otherwise.

KNOWN LIMITATIONS:

- Sentence splitting is regex-level (`[.!?]` + whitespace). "e.g." and
  "i.e." are excluded by name, because neither ever ends a sentence, and
  this corpus writes both constantly -- splitting there cut sentences in
  two and let a long one past CH007. Every other abbreviation still
  splits. That is deliberate: "etc." and "vs." DO end sentences, so
  excluding them would merge two real ones and over-report. A general fix
  (require a following capital) merges "... the row. Postgres ..." and is
  worse than the problem. Both directions are wrong; under-reporting is
  the safe one for a gate.

- A rule about what Rustdoc RENDERS applies to `///`, `//!`, `/**` and
  `/*!` only, because Rustdoc renders nothing else. Indented code, a
  Setext underline and an HTML block are all limited that way. A ```
  fence is not: an author writes one to mean "this is an example",
  whatever the marker, and every comment in this tree that carries an
  example carries a fence. Getting this wrong is silent -- it removes
  findings rather than adding them -- and the corpus diff, not the
  self-test, is what catches it.

- The Markdown block layer is hand-rolled and deliberately partial. Two
  known gaps are recorded in the follow-up issue rather than fixed here,
  and both UNDER-report, which is the safe direction for a gate: an HTML
  block is not scoped to the container that opened it, so leaving a quote
  does not end one; an HTML closer inside an inline nested comment is not
  seen, because a mid-line piece carries no block syntax; and a table is
  not scoped to its container either, so a quoted table survives the line
  that leaves the quote. None of those shapes occurs in this tree -- no
  `*.rs` comment here holds raw HTML or a table -- and every fix in this
  seam has cost more than it returned.

- CH001 is deliberately HIGH-PRECISION AND INCOMPLETE, and should stay
  that way. It recognizes commented-out Rust by line shape: item headers,
  `let`/`use`/assignment, attributes, closing braces, macro and call
  statements, and block-opening control flow. It does not recognize an
  expression with no terminator (`// foo(bar)`), because that shape is
  indistinguishable from prose naming a call, which this corpus does on
  thousands of lines.

  The asymmetry is intentional. A false positive fails CI on ordinary
  English, which is worse than missing one commented-out line, so every
  branch is anchored and terminator-bearing and every widening is measured
  against the corpus and the `--self-test` prose sweep first. Adding recall
  by relaxing an anchor has produced a false positive every time it has
  been tried here; add a new narrowly-anchored alternative instead.

- CH005/CH006 match inside inline code spans (`` `like this` ``), so a
  literal that happens to contain "round-3" or an apostrophe-s would be
  flagged. No current occurrence does; stripping code spans first would
  also hide genuine prose violations that merely sit next to one. CH002
  is the exception and blanks them, because it is absolute: a comment
  documenting the marker syntax ("Parse the `TODO:` prefix") must not
  fail the build, and that risk outweighs the one above.

- CH004 judges only runs of leading `//`. A trailing comment has no block
  to have edges, and a blank first line inside `/* */` is conventional
  formatting rather than the editing artifact the rule is about.

- The lexer is a lexer, not a parser: it tracks strings, raw strings, char
  literals and nested block comments, but knows nothing of macros or
  `cfg`. That is sufficient here, because every construct that can hide a
  `//` from a reader is lexical. `--self-test` pins the cases it must get
  right, including the backslash-newline continuation whose newline has to
  be counted or every later line number drifts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Directories that hold no first-party source.
SKIP_DIRS = {".git", "target", "node_modules", ".cargo"}

TIER_A = ("CH001", "CH002", "CH003", "CH004")
TIER_B = ("CH005", "CH006", "CH007")

RULE_TITLES = {
    "CH001": "commented-out code",
    "CH002": "TODO/FIXME without an issue reference",
    "CH003": "narrative aside",
    "CH004": "blank comment line at a block edge",
    "CH005": "review-round archaeology",
    "CH006": "contraction (ASD-STE100)",
    "CH007": "sentence over 25 words (ASD-STE100)",
}

RULE_HINTS = {
    "CH001": "Delete it -- git remembers. Keep it only inside a ``` fence as a documented example.",
    "CH002": "Add the tracking issue: TODO(#1234): ...",
    "CH003": "State the decision, not the deliberation.",
    "CH004": "Drop the empty `//` line at the edge of the block.",
    "CH005": "Cite the issue number (#1234), not the review round that found it.",
    "CH006": "Expand it: \"isn't\" -> \"is not\", \"let's\" -> \"To ... ,\".",
    "CH007": "Split it. One idea per sentence, 25 words or fewer.",
}

# --- Tier A patterns ---------------------------------------------------------

# `[\u2019']` wherever an apostrophe appears in a rule: an editor that
# substitutes a typographic apostrophe must not turn a gated construct into an
# invisible pass. This bit CH006 (contractions) and CH003 (narrative asides)
# independently, so it lives here rather than beside either one.
_APOS = r"[\u2019']"


# Commented-out Rust, by line shape. Anchored and terminator-bearing so that
# ordinary prose ("let the caller decide", "use the LATER definition") cannot
# match -- prose does not end in `;` or `{`.
#
# Every alternative binds its delimiter tight against the identifier, so a
# prose line that merely happens to end in `;` cannot match. `fn item and is
# therefore typo-proof ...;` is prose: after `fn` comes a name and then a
# space, never the `(` a real signature requires.
#
# An identifier may be RAW. `r#match` is a name, and `\w+` stops at the `#`,
# so a commented-out `fn r#match() {` was outside an absolute gate. This tree
# discusses `r#gen` in `det_check.rs`, so the form is not hypothetical here.
COMMENTED_CODE_RE = re.compile(
    r"""^(?:
        # The ABI name may carry a hyphen -- "C-unwind" and its siblings are
        # stable, and rustc 1.94.1 compiles them -- and it may be absent
        # altogether, since a bare `extern fn` means `extern "C" fn`. Both
        # are ordinary FFI, and `\w+` alone left both outside an absolute
        # gate.
        (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?
        (?:async\s+|unsafe\s+|const\s+|extern\s+(?:"[\w-]+"\s+)?)*
            fn\s+(?:r\#)?\w+\s*(?:<[^<>]*>)?\s*\(
            (?:
                 .*\)\s*(?:->\s*[^;{]+?)?\s*[{;]   # complete: ends in { or ;
               | \s*$                                # wrapped: `fn foo(` at EOL
               | (?=[^)]*(?::|\bself\b))            # wrapped: real params,
                 [\w\s:&'<>\[\](),.+;=*-]*,\s*$      #   trailing comma
            )\s*$
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?(?:struct|enum|trait|union)\s+(?:r\#)?\w+\s*(?:<[^<>]*>)?\s*[{;(]\s*$
      # A tuple struct, whose field list is on the line and terminated. Its
      # own alternative rather than a relaxation of the one above, which is
      # what this file's guidance asks for: the name is still anchored hard
      # against `struct`, and the line must end at the `;`.
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?(?:struct|union)\s+(?:r\#)?\w+\s*(?:<[^<>]*>)?
            # The `;` of "[u8; 32]" is part of an array type, not the end of
            # the statement -- the same guard the uninitialized-binding rule
            # has carried since round forty-one, which this alternative was
            # written without.
            \s*\((?:[^;{]|;(?=[^;]*\]))*\)\s*;\s*$
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?mod\s+(?:r\#)?\w+\s*[{;]\s*$
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?(?:const|static)\s+(?:mut\s+)?(?:r\#)?\w+\s*:[^;=]+=.*[;{]\s*$
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?type\s+(?:r\#)?\w+\s*(?:<[^<>]*>)?\s*=
            (?!\s*(?:\w+\s+){2,}\w+\s*;\s*$).*;\s*$
      | impl(?:\s*<[^<>]*>)?\s+
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)[\w:<>&'\s]+\{\s*$
      | let\s+(?:mut\s+)?(?:r\#)?\w+\s*(?::[^;=]+)?=
            (?!\s*(?:\w+\s+){2,}\w+\s*;\s*$)[^=].*;\s*$
      # Destructuring bindings. A tuple or slice pattern, or a struct/enum
      # pattern behind a Capitalised path -- all terminated, and none of them
      # a shape English produces. The plain `\w+` form above misses every one.
      | let\s+(?:mut\s+)?[(\[][\w\s,.:&*'()\[\]{}]*[)\]]\s*(?::[^;=]+)?
            =\s*[^=;]+(?:;|\s+else\s*\{)\s*$
      | let\s+(?:[\w]+::)*[A-Z]\w*\s*(?:\([^;]*\)|\{[^;]*\})\s*=\s*[^=;]+
            (?:;|\s+else\s*\{)\s*$
      # An uninitialized binding. No `=` to anchor on, so it needs a type
      # annotation to separate "let mut retries: usize;" from "let the reader
      # decide;" -- English does not put a colon between two bare words.
      | let\s+(?:mut\s+)?(?:r\#)?\w+\s*:
            (?!\s*(?:\w+\s+){2,}\w+\s*;\s*$)\s*
            # A Rust type, not a scalar name. An array length needs `;`, a
            # trait object needs `+`, a function pointer needs `->`, and a
            # raw pointer needs `*`; without them "let bytes: [u8; 32];" is
            # commented-out code that the absolute gate lets through. The
            # inner `;` is allowed only where a `]` closes before the next
            # one, so the terminator itself stays the end of the statement.
            # A hyphen is admitted only as `->`. A bare one let the prose
            # sweep through "let a::b is re-exported for callers;", and a
            # type has no other use for it.
            (?:[\w:<>&'\[\]\s,()+*!?]|->|;(?=[^;]*\]))+;\s*$
      # A re-export carries a visibility like any other item, and this
      # alternative was the one written without it -- `pub use crate::x;`
      # and `pub(crate) use crate::x;` are the forms this tree actually
      # writes, and neither could reach the rule.
      | (?:pub(?:\((?:in\s+)?[\w:]+\))?\s+)?
            use\s+(?:(?:r\#)?\w+::)*(?:(?:r\#)?\w+|\*|\{[\w:,\s*\#]+\})
            (?:\s+as\s+(?:r\#)?\w+)?;\s*$
      | \#!?\[[\w:()"'=,./\s-]+\]\s*$
      | \}[,;)]*\s*$
      | [\w:]+!(?:\(.*\)|\[.*\]|\{.*\})\s*;\s*$        # macro stmt, any delimiter
      # A macro DEFINITION, which ends at its brace rather than a `;`.
      # Anchored on the keyword, so no prose can reach it.
      | macro_rules!\s+(?:r\#)?\w+\s*\{.*\}\s*;?\s*$
      # Control flow opening a block. The lookahead rejects a condition made
      # of four or more consecutive plain words, which is a sentence, not an
      # expression: "if the queue is paused, the worker parks {".
      | (?:if|while|for|match|loop|unsafe)\b
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)[^;]*\{\s*$
      | [\w.\[\]:]+\s*(?:[-+*/%&|^]|<<|>>)?=\s*[^\s;]+\s*;\s*$  # (compound) assignment
      # A commented-out statement. Anchored hard: the call must open at the
      # very start, so prose that merely names a function ("call cleanup()
      # first") cannot reach it, and the line must end at the `;`. Measured
      # against all 176k corpus comments before adding: one hit, and it was
      # real commented-out code.
      | (?:return|break|continue)\b(?:\s+[^\s;{}]+)?\s*;\s*$
      | [\w:]+(?:::<[^;()]*>)?(?:\.[\w:]+(?:::<[^;()]*>)?)*
            \(.*\)(?:\s*\?|\s*\.await\s*\??)*\s*;\s*$
    )""",
    re.VERBOSE,
)

# A marker that OPENS a comment, or one punctuated as a marker (`TODO:`,
# `FIXME(...)`). Prose that merely refers to a marker elsewhere -- "see the
# `session_id` TODO above" -- is not itself an untracked commitment.
TODO_RE = re.compile(
    r"^(?:TODO|FIXME|XXX|HACK)\b|\b(?:TODO|FIXME|XXX|HACK)\s*[:(]", re.IGNORECASE
)
TODO_REF_RE = re.compile(r"#\d+|https?://")
# A reference that ABUTS the marker on its left. Anchored at the end, and
# opened either at the bound or at a clause separator, so "See #123 for the
# parser. TODO: x" is still untracked while "#123 - TODO: x" is not.
ADJACENT_REF_RE = re.compile(
    r"(?:^|[;.,(\[])[\s\-\u2010-\u2015:]*"
    r"(#\d+|https?://\S+)"
    r"[\s\-\u2010-\u2015:;,]*$"
)


def untracked_marker(text: str) -> bool:
    """Does `text` carry a marker with no reference of its OWN?

    Per marker, not per line. A line-wide search let one reference cover
    every marker beside it, in two shapes: an unrelated reference already
    on the line ("See #123 for the parser. TODO: add retries"), and a
    second, untracked marker after a tracked one ("TODO(#123): parser;
    TODO: add retries"). Both are untracked commitments the gate passed.

    A marker owns the text from itself to the next marker, or to the end
    of the line. That admits every form this tree writes -- "TODO(#123):",
    "TODO: ... (#123)", a bare URL -- and refuses only a reference that
    belongs to something else.
    """
    marks = list(TODO_RE.finditer(text))
    claimed = -1
    for index, mark in enumerate(marks):
        start = mark.start()
        end = marks[index + 1].start() if index + 1 < len(marks) else len(text)
        forward = TODO_REF_RE.search(text, start, end)
        if forward:
            # What this marker consumes, so the next one cannot reuse it.
            claimed = forward.start()
            continue
        # Backwards as well as forwards. "#123 - TODO: remove the fallback"
        # is a tracked commitment written the other way round, and reading
        # only forwards failed the build on it. The reference must ABUT the
        # marker: separators between the two and nothing else, so a
        # reference from an earlier clause still does not stand in for one.
        lower = marks[index - 1].end() if index else 0
        adjacent = ADJACENT_REF_RE.search(text[lower:start])
        # ONE reference tracks ONE marker. The previous marker's forward
        # search reaches into this same text, so "TODO: #123; TODO: x" let
        # the second marker borrow the first one's reference and pass an
        # absolute gate. Refuse the borrowed one; a DIFFERENT reference in
        # the same span -- "TODO(#1): a; #2 - TODO: b" -- still counts.
        if adjacent and lower + adjacent.start(1) != claimed:
            continue
        return True
    return False
# An inline code span. CH002 is absolute and at zero, so a comment that
# DOCUMENTS the marker syntax -- "Parse the `TODO:` prefix" -- must not fail
# the build. The span is blanked rather than deleted, so every offset after
# it still points at the same column.
# One pattern for both blankers. It was two, and the exact-run guard reached
# only the wrapped one until this round.
#
# A BACKSLASH-ESCAPED backtick is not an OPENER: Rustdoc renders "\\`literal"
# and a later "`" as literal text, and treating the escaped tick as an opener
# blanked the marker between them. Escapes are resolved BEFORE the spans are
# found, rather than guarded for in the pattern, so the parity works out.
#
# Inside a span there are no escapes at all: "`\\`" is a span holding one
# backslash -- checked against rustdoc, which renders it as <code>\\</code>.
# Masking escapes first gets that right too, because the mask never spans a
# delimiter it did not consume.


# The same span, allowed to wrap. Rustdoc renders "`literal" and "TODO:
# marker`" on consecutive lines as ONE code span, so a line-at-a-time blank
# never sees the opener and CH002 fails the build on the marker inside it. A
# blank line still ends a paragraph, and a span cannot cross one.
# Both runs must be EXACTLY as long as each other, so the backreference is
# fenced on both sides. A bare `\1` let a one-backtick opener close against a
# "``" pair -- against its first tick, and once that was guarded, against its
# second -- blanking text CommonMark leaves literal and taking an absolute
# rule off it. A run is only a delimiter when no backtick abuts it.
BACKTICK_RUN_RE = re.compile(r"`+")
BLANK_LINE_RE = re.compile(r"\n[ \t]*\n")


def escaped_offsets(text: str) -> set[int]:
    """Offsets of the characters an ODD run of backslashes escapes.

    Parity, counted left to right, because a lookbehind cannot: one backslash
    escapes the character after it, two escape each other and leave that
    character to do its job as usual. Rustdoc renders "\\\\`marker`" with the
    marker as code, and a single-character lookbehind refused that opener.

    Character-blind on purpose. A backtick and an inline "<code>" tag obey
    the same parity, and giving each its own scanner is how the two drifted
    apart.
    """
    escaped, index = set(), 0
    while index < len(text):
        if text[index] != "\\":
            index += 1
            continue
        run = index
        while index < len(text) and text[index] == "\\":
            index += 1
        if (index - run) % 2 and index < len(text):
            escaped.add(index)
            index += 1
    return escaped


def escaped_backticks(text: str) -> set[int]:
    """Offsets of the backticks an odd run of backslashes escapes."""
    return {index for index in escaped_offsets(text) if text[index] == "`"}


def code_span_ranges(text: str) -> list[tuple[int, int]]:
    """Where `text`'s inline code spans are.

    Scanned rather than matched, because the rule has three parts a single
    pattern kept getting wrong. A span's delimiter runs must be EXACTLY the
    same length. An escaped backtick cannot OPEN one -- but inside an open
    span there are no escapes at all, so "`\\`" is a span holding one
    backslash, which is what Rustdoc renders. And a blank line ends the
    paragraph, so no span reaches across one.
    """
    escaped = escaped_backticks(text)
    runs = [match.span() for match in BACKTICK_RUN_RE.finditer(text)]
    ranges = []
    index = 0
    while index < len(runs):
        start, end = runs[index]
        # An escape consumes exactly ONE backtick, not the run it opens.
        # Rustdoc renders "\\``TODO: x` suffix." with the first tick literal
        # and the second opening a one-tick span, so discarding the whole run
        # left the marker inside that span exposed to an absolute rule.
        if start in escaped:
            start += 1
        if start < end:
            for probe in range(index + 1, len(runs)):
                closer = runs[probe]
                if closer[1] - closer[0] != end - start:
                    continue
                if BLANK_LINE_RE.search(text, end, closer[0]):
                    break
                ranges.append((start, closer[1]))
                index = probe
                break
        index += 1
    return ranges


def replace_spans(text: str, filler: str) -> str:
    """`text` with each code span's characters replaced by `filler`.

    Newlines survive, so a multi-line text splits back into the same lines.
    """
    out = list(text)
    for start, end in code_span_ranges(text):
        for index in range(start, end):
            if out[index] != "\n":
                out[index] = filler
    return "".join(out)


# Inline HTML that renders as code. Rustdoc puts "<code>not sure why</code>"
# in a <code> element exactly as it does a backtick span, so the absolute
# rules must not read a narrative phrase or a marker inside one. Doc comments
# only: nothing renders a `//` comment, where this is literal text.
# CommonMark's open-tag grammar, not an approximation of it. Two rounds
# tried to bound this pattern by what a tag may not contain, and each
# accepted a shape Rustdoc escapes: "<code@example.com>" is a mail
# autolink, "<code-block>" is a different element, and "<code =oops>" has
# no attribute name where one is required. All three rendered the marker
# beside them as prose while the audit masked it.
#
# The direction is the reason to spell the grammar out. Every character
# this matcher takes loosely is an ABSOLUTE rule switched off to the end
# of a block, so a generous pattern does not misreport -- it goes silent.
_HTML_SPACE = r"[ \t\r\n\f]"
_HTML_ATTR = (
    _HTML_SPACE + r"+[A-Za-z_:][A-Za-z0-9_.:-]*"
    r"(?:" + _HTML_SPACE + r"*=" + _HTML_SPACE + r"*"
    r"(?:[^ \t\r\n\f\"'=<>`]+|'[^']*'|\"[^\"]*\"))?"
)
CODE_OPEN_RE = re.compile(
    r"<code(?:" + _HTML_ATTR + r")*" + _HTML_SPACE + r"*/?>", re.I
)
CODE_CLOSE_RE = re.compile(r"</code" + _HTML_SPACE + r"*>", re.I)


def inline_code_ranges(text: str) -> list[tuple[int, int]]:
    """Where `text`'s inline <code> elements are.

    Scanned rather than matched, for two reasons one pattern could not hold
    at once. An ESCAPED tag is not a tag: Rustdoc renders "\\<code>TODO: x"
    as literal characters, so the marker inside it is a real commitment.

    And an unclosed element runs to the END OF THE BLOCK, which is the one
    place this differs from a backtick span. An unmatched backtick opens
    nothing; an unmatched "<code>" opens an element the paragraph's own
    close tag ends, so "<code>a" leaves every later word on that paragraph
    rendered as code. Callers pass one block, so the end of `text` is the
    end of the block.
    """
    escaped = escaped_offsets(text)

    def unescaped(pattern, start):
        match = pattern.search(text, start)
        while match and match.start() in escaped:
            match = pattern.search(text, match.start() + 1)
        return match

    ranges: list[tuple[int, int]] = []
    index = 0
    while True:
        opener = unescaped(CODE_OPEN_RE, index)
        if not opener:
            return ranges
        # NESTED, because HTML nests. Rustdoc renders
        # "<code>outer <code>inner</code> TODO: x</code>" with the marker
        # still inside the OUTER element, and pairing the outer opener with
        # the inner closer exposed it and failed the build. The element ends
        # at the closer that returns the depth to zero.
        depth, cursor, end = 1, opener.end(), len(text)
        while True:
            closer = unescaped(CODE_CLOSE_RE, cursor)
            nested = unescaped(CODE_OPEN_RE, cursor)
            if nested and (closer is None or nested.start() < closer.start()):
                depth += 1
                cursor = nested.end()
                continue
            if closer is None:
                break
            depth -= 1
            cursor = closer.end()
            if not depth:
                end = closer.end()
                break
        ranges.append((opener.start(), end))
        if end == len(text):
            return ranges
        index = end


def replace_inline_code(text: str, filler: str) -> str:
    """`text` with each inline <code> element's characters set to `filler`.

    Newlines survive, so a multi-line text splits back into the same lines.
    """
    out = list(text)
    for start, end in inline_code_ranges(text):
        for index in range(start, end):
            if out[index] != "\n":
                out[index] = filler
    return "".join(out)


def blank_inline_code(text: str) -> str:
    """`text` with each inline <code> element replaced by spaces."""
    return replace_inline_code(text, " ")


def mask_inline_code(text: str) -> str:
    """`text` with each inline <code> element replaced by filler.

    Filler for finding sentence boundaries, spaces for matching, exactly as
    the backtick spans beside it. A full stop inside "<code>foo. not sure
    why</code>" is not a sentence end, and splitting there left a fragment
    the narrative rule read as deliberation.
    """
    return replace_inline_code(text, "x")


def blank_code_spans(text: str) -> str:
    """`text` with each inline code span replaced by spaces of equal width."""
    return replace_spans(text, " ")


def mask_code_spans(text: str) -> str:
    """`text` with each code span replaced by filler of the same width.

    Filler, not spaces. This copy is only used to FIND sentence boundaries,
    and blanking a span to whitespace lets the separator's own `\\s+` swallow
    it -- so a sentence that OPENS with a code span loses it, and reports one
    word short with the span missing from its text.
    """
    return replace_spans(text, "x")


def blank_spans_across(lines: list[str]) -> list[str]:
    """`lines` with code spans blanked, including ones that wrap.

    Judged over the joined text so a span opened on one line closes on the
    next, and blanked to spaces so every column still lines up. Newlines
    survive, so the result splits back into the same lines. An UNMATCHED
    backtick blanks nothing, which is also what CommonMark does with it.
    """
    return blank_code_spans("\n".join(lines)).split("\n")

# First-person deliberation. "Actually" must open a sentence: mid-sentence it is
# an ordinary adverb ("gated on THIS claimant actually, durably marking the
# row"). So must "lets just", for the same reason from the other direction --
# it is here as the misspelling of "let's just", but "the semaphore lets just
# one claimant proceed" is an ordinary verb and states behaviour, not
# deliberation. `we'd`/`isn't` and friends are left to CH006 -- they are
# contractions, not necessarily deliberation.
NARRATIVE_RE = re.compile(
    r"(?:^|(?<=[.!?;]\s))\s*(?:actually[,\s]|lets just\b)"
    r"|\b(?:let" + _APOS + r"s\b|we" + _APOS + r"ll\b|i think\b"
    r"|i" + _APOS + r"m not sure\b"
    r"|not sure (?:if|why|whether)\b|for now,|hmm\b|oops\b(?![\"'])|note to self\b"
    r"|as you can see\b|todo later\b)",
    re.IGNORECASE,
)

# --- Tier B patterns ---------------------------------------------------------

ARCHAEOLOGY_RE = re.compile(
    r"\b(?:round[- ]\d+|codex round|review round|P[0-4]\s+(?:fix|finding|review))\b",
    re.IGNORECASE,
)

# Real contractions only. Excludes the possessive/abbreviation apostrophe this
# corpus uses ("TTL'd overrides", "the row's bytes"), which STE permits.
#
# Deliberately NOT matched: the noun + `'s` forms (`one's`, `someone's`,
# `everything's`, ...). They are ambiguous -- "someone's waiting" is a
# contraction but "someone's row" is a possessive -- and measured over this
# corpus all 21 occurrences are possessives ("the previous one's outcome",
# "the worst one's"). Adding those stems would report 21 false positives
# against correct STE. The stems below are the unambiguous ones: `it's`,
# `who's`, `there's` and friends have distinct possessive spellings (`its`,
# `whose`, `theirs`), so a match is always a contraction.
CONTRACTION_RE = re.compile(
    r"\b(?:ca|is|are|was|were|do|does|did|would|could|should|will|has|have|had"
    r"|must|ai|wo|sha|need|ought|might|dare)n" + _APOS + r"t\b"
    r"|\b(?:it|that|there|here|what|who|how|where|when|why|let|he|she|we|they"
    r"|you|i)" + _APOS + r"(?:s|ll|re|ve|d|m)\b"
    r"|\b(?:should|could|would|must|might)" + _APOS + r"ve\b",
    re.IGNORECASE,
)

MAX_SENTENCE_WORDS = 25

# Indentation is captured, not bounded here: CommonMark's "at most three
# spaces" is relative to the enclosing block container, so a fence inside a
# list item is legitimately indented further. `fence_delimiter` applies the
# limit against the container.
FENCE_RE = re.compile(r"^([ \t]*)(`{3,}|~{3,})")
# CommonMark caps an ordered marker at nine digits, and the cap is load-bearing
# here: a longer run of digits is ordinary text, and accepting it opens a list
# -- and a fence allowance -- where the rendered document has neither.
LIST_MARKER_RE = re.compile(r"^([ \t]*)((?:[-*+]|\d{1,9}[.)]))([ \t]+|$)")
# A block quote is a container too, and Rustdoc uses it. Its marker is not
# indentation -- content inside the quote starts again at column zero -- so it
# is stripped before any container or fence judgement. Nested and space-less
# forms ("> >", ">>") both count.
#
# The lookahead is load-bearing, and only just wide enough. CommonMark needs
# no space after ">", so ">~~~rust" opens a quoted fence -- but a Rust comment
# wrapping onto a leading ">=" is an operator, and stripping its ">" silently
# rewrites the text every rule then judges, and the text a Tier B failure
# quotes back. So the exception is ">=" and nothing else.
BLOCKQUOTE_RE = re.compile(r"^([ \t]*)((?:>(?!=)[ \t]?)+)")
ONE_QUOTE_RE = re.compile(r"^([ \t]*)>(?!=)[ \t]?")
# A GFM table ROW, not merely a line that opens with a pipe. One pipe is
# ordinary prose -- "| foo" wraps a sentence like any other word -- and
# flushing there drops the rest of the sentence out of the unit.
DELIM_CELL_RE = re.compile(r":?-+:?")
# A cell boundary. An escaped pipe is content: Rustdoc renders
# "| a \\| b | c |" as two columns, the first of which contains a pipe.
CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")


def row_cells(text: str, container: int) -> list[str]:
    """The cells of a GFM table row.

    A row may open a list item, so one list marker is peeled first. Rustdoc
    renders "- | a | b |" as an item holding a two-column table, so the
    marker is not a cell. The leading and trailing pipes are optional.
    """
    content = list_content(text, container)
    if content:
        text = text[content[1]:]
    text = text.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    return CELL_SPLIT_RE.split(text)


def opens_fence(text: str, container: int) -> bool:
    """Is `text` a valid fence OPENER that its container would accept?

    Two rules, both of which `fence_delimiter` and its caller already apply
    and neither of which the lazy-continuation test applied. A delimiter
    sits within three columns of its container: `FENCE_RE` captures the
    indent rather than bounding it, exactly so each caller can measure it,
    and a caller that forgets asks an unbounded question. And a BACKTICK
    fence's info string may hold no backtick, so "``` TODO: x` suffix." is
    not an opener at all.

    Both matter to a lazy continuation, because a line that opens no fence
    opens no block, and Rustdoc keeps it inside the quoted paragraph.
    """
    match = FENCE_RE.match(text)
    if not match or len(match.group(1).expandtabs(4)) > container + 3:
        return False
    return not (match.group(2).startswith("`") and "`" in text[match.end():])


def row_cell_spans(text: str, container: int) -> list[tuple[int, int]]:
    """Where the cells of a GFM table row are, as offsets into `text`.

    The same peeling as `row_cells`, kept as offsets because the span pass
    has to blank in place: every column after a blanked span still has to
    line up with the source line the finding reports.

    Rustdoc parses each cell as its own inline context, so a backtick in one
    cell cannot pair with a backtick in the next. This is what tells the
    span pass where one context ends -- a boundary INSIDE a line, which is
    the one kind the block layer cannot express.
    """
    start = 0
    content = list_content(text, container)
    if content:
        start = content[1]
    while start < len(text) and text[start] in " \t":
        start += 1
    end = len(text)
    while end > start and text[end - 1] in " \t":
        end -= 1
    if start < end and text[start] == "|":
        start += 1
    if end - 1 > start and text[end - 1] == "|" and text[end - 2] != "\\":
        end -= 1
    spans, cursor = [], start
    for match in CELL_SPLIT_RE.finditer(text, start, end):
        spans.append((cursor, match.start()))
        cursor = match.end()
    spans.append((cursor, end))
    return spans


def blank_cells(text: str, spans: list[tuple[int, int]]) -> str:
    """`text` with code spans blanked INSIDE each cell, never across two."""
    out = list(text)
    for start, end in spans:
        blanked = blank_code_spans(text[start:end])
        out[start:end] = list(blanked)
    return "".join(out)


def table_delimiter(text: str, container: int, header: str) -> bool:
    """Is `text` a delimiter row for the row `header` above it?

    Three columns past the container, like every marker here. Stripping the
    line before judging it loses that, and an over-indented delimiter then
    turns the paragraph above it into a table and drops the sentence.

    A delimiter row is only a delimiter row for a header of the SAME width.
    Rustdoc renders "Intro | header |" over "| - |" as one paragraph, because
    two cells do not match one; reading it as a table there closes a paragraph
    the document still holds open. One hyphen in a cell is valid, and a
    single-cell "| - |" under a single-cell "| a |" IS a table -- checked
    against rustdoc 1.94, whose renderer is the one these comments target.
    """
    if leading_columns(text) > container + 3:
        return False
    stripped = text.strip()
    if "|" not in stripped or not stripped.strip(" \t|-:") == "":
        return False
    # EVERY cell needs its own hyphen run. "| | --- |" has an empty first
    # cell, so it is not a delimiter row and the pipe line above it is not a
    # header -- both are prose, and the sentence they carry must be counted.
    cells = row_cells(text, container)
    if not cells or not all(DELIM_CELL_RE.fullmatch(cell.strip()) for cell in cells):
        return False
    return len(cells) == len(row_cells(header, container))

# An ATX heading. The indent is a capture group because the indent decides
# whether this is a heading at all -- see `heading` below.
def line_tail(pieces: list, index: int) -> str:
    """The rest of the piece's LINE, past any nested comment on it.

    A fence delimiter is judged by the whole line, and a nested comment
    splits one line into several pieces. Rustdoc keeps a fence open across
    "``` /* note */" because a closing fence may be followed only by spaces,
    and closing it there reports a fenced example's own TODO -- a false
    positive on a gate that sits at zero.
    """
    tail = []
    for piece in pieces[index + 1:]:
        if piece.line != pieces[index].line or piece.line_start:
            break
        tail.append(piece.text)
    return "".join(tail)


# The markers Rustdoc renders as Markdown. `//` and `////` are ordinary code
# comments that no renderer ever sees, and `/*` is their block form.
DOC_MARKERS = ("///", "//!", "/**", "/*!")


def indented_code(
    text: str, container: int, paragraph: bool, inside: bool, doc: bool
) -> bool:
    """Is `text` a line of a CommonMark indented code block?

    Four columns past the container, and only where no paragraph is open --
    indented code cannot interrupt one, so a wrapped line indented under its
    own paragraph stays prose. Rustdoc renders the block form as a Rust
    example, so its content is an example and not commentary: without this,
    "///     let x = compute();" after a blank line fails CH001 and no
    unfenced rustdoc example can be added to this tree.

    A blank line stays inside an open block; only a line indented less than
    four columns ends one.

    DOC COMMENTS ONLY, and that limit is the point of the rule rather than a
    shortcut. Indented code is a fact about what Rustdoc renders, and Rustdoc
    renders no `//` comment at all: there, indentation is how this tree lays
    out a long argument, and exempting it would stop measuring nineteen real
    sentences. A ``` fence is different -- an author writes one to say "this
    is an example" whatever the marker -- so fences stay exempt everywhere.
    """
    if not doc:
        return False
    if inside:
        return not text.strip() or leading_columns(text) >= container + 4
    return (
        not paragraph and bool(text.strip())
        and leading_columns(text) >= container + 4
    )


def starts_block(text: str, container: int, paragraph: bool = False) -> bool:
    """Does `text` begin a CommonMark block other than a paragraph?

    `paragraph` says one is already open, which only an INTERRUPTING list
    marker may end. "2." cannot: Rustdoc renders "Intro `literal" and
    "2. TODO: x` suffix." as one paragraph with the marker inside a code
    span, and cutting the span there failed the build on it. Defaulted off,
    so the table-end callers keep asking the question they always asked.

    This is what ENDS a GFM table. A body row needs no pipe -- Rustdoc
    renders "ordinary row" under a table as a cell and fills the rest of
    them -- so the table runs to a blank line or the next block, and
    requiring a pipe in every row ended it one line early.
    """
    if not text.strip():
        return True
    return (
        opens_fence(text, container)
        or heading(text, container)
        or html_block(text, container)
        or thematic_break(text, container)
        or (
            bool(list_content(text, container))
            and (not paragraph or interrupts_paragraph(LIST_MARKER_RE.match(text)))
        )
        or bool(quote_marker(text, container))
    )


def next_row(pieces: list, index: int, nest: int):
    """The piece after `index`, if it is in the same comment.

    A delimiter row belongs to the header above it only when both are the
    same comment. Rustdoc renders a nested comment's delimiters literally,
    so "/* | --- | */" under "| h |" is paragraph text, and reading it as a
    delimiter turns a paragraph into a table and drops the sentence.
    """
    if index + 1 < len(pieces) and pieces[index + 1].nest == nest:
        return pieces[index + 1]
    return None


def table_header(pieces: list, index: int, nest: int, text: str, container: int) -> bool:
    """Is `text` a table header -- a pipe row with a delimiter row under it?

    The one place that decides it, for both scanners and for the lazy
    continuation test. A pipe alone is not a table: "| foo" wraps a sentence
    like any other word, and only the delimiter row below makes a block.
    """
    if "|" not in text:
        return False
    row = next_row(pieces, index, nest)
    return row is not None and table_delimiter(
        strip_quote(row.text, container), container, text
    )


HEADING_RE = re.compile(r"^([ \t]*)#{1,6}(?:[ \t]|$)")
# A thematic break -- one punctuation character repeated. CommonMark spells it
# "---"; this tree also draws section rules with box-drawing characters. Either
# way it separates blocks, so a sentence never runs across one.
SEPARATOR_RE = re.compile(
    "^[ \t]*([-_*=\u2500\u2501\u2550\u00b7])(?:[ \t]*\\1){2,}[ \t]*$"
)
# The CommonMark subset of the above. Only `-`, `_` and `*` make a thematic
# break; "===" and the box-drawing rules this tree draws sections with are
# ordinary paragraph text to Rustdoc. The broad set still ends a prose unit --
# a section rule is not a word of the sentence under it -- but only a real
# break may change container state, or a decorative line grants the next "22."
# a container, and its fence an allowance, that the rendered document has not.
# The indent here is unbounded on purpose, and it is the one pattern in this
# file that may be. A section rule is not a word of the sentence at any indent,
# and this pattern only ends a prose unit -- it opens no container and closes
# no paragraph, so an over-indented match costs a sentence nothing.
THEMATIC_BREAK_RE = re.compile("^([ \t]*)([-_*])(?:[ \t]*\\2){2,}[ \t]*$")
# A Setext underline: "=" under a paragraph line makes that paragraph a
# heading. Whether a run of "=" is a heading or ordinary text is decided by
# POSITION, not shape -- with a paragraph open it underlines one, and at the
# start of a block it is the decorative rule round thirty-three fixed.
SETEXT_RE = re.compile("^([ \t]*)(?:=+|-+)[ \t]*$")
# A GFM task-list marker, which Rustdoc renders as a checkbox rather than as
# text. Exactly "[ ]", "[x]" or "[X]", and a space must follow: rustdoc 1.94
# renders "[]", "[ ]no space" and "[y]" as literal words.
TASK_MARKER_RE = re.compile(r"^\[[ xX]\](?=[ \t]|$)")
# CommonMark's HTML-block tag names, types 1 and 6. A line opening with one
# of these is its own block and ends the paragraph above it -- Rustdoc renders
# "Intro." then "<pre>raw</pre>" as a paragraph and a block, so the "22." under
# them starts a list. Type 7 (any other complete tag alone on a line) is
# deliberately absent: it cannot interrupt a paragraph, and "<T>" in a Rust
# comment is a type parameter, not markup.
HTML_BLOCK_TAGS = (
    "address|article|aside|base|basefont|blockquote|body|caption|center|col|"
    "colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|"
    "footer|form|frame|frameset|h1|h2|h3|h4|h5|h6|head|header|hr|html|iframe|"
    "legend|li|link|main|menu|menuitem|nav|noframes|ol|optgroup|option|p|"
    "param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|"
    "track|ul|script|pre|style|textarea"
)
HTML_BLOCK_RE = re.compile(
    r"^([ \t]*)(?:<[/]?(?:" + HTML_BLOCK_TAGS + r")(?:[ \t/>]|$)"
    r"|<!--|<\?|<![A-Za-z]|<!\[CDATA\[)",
    re.IGNORECASE,
)


# What ends each kind of HTML block. Types 1 to 5 end ON the line carrying
# the closer, which may be the opening line itself ("<pre>raw</pre>"); type 6
# ends at a blank line, which is not part of it.
HTML_VERBATIM_RE = re.compile(r"^[ \t]*<(script|pre|style|textarea)(?:[ \t/>]|$)", re.I)
HTML_CLOSERS = {
    "script": ("</script>",),
    "pre": ("</pre>",),
    "style": ("</style>",),
    "textarea": ("</textarea>",),
    "comment": ("-->",),
    "instruction": ("?>",),
    "declaration": (">",),
    "cdata": ("]]>",),
}


def html_block(text: str, container: int) -> bool:
    """Does `text` open a CommonMark HTML block?

    Three columns past the container, like every marker here. An HTML block
    is a block, so it ends the paragraph above it and the next "22." may
    open a list the rendered document gives it.
    """
    match = HTML_BLOCK_RE.match(text)
    return bool(match) and len(match.group(1).expandtabs(4)) <= container + 3


def html_kind(text: str) -> str:
    """Which sort of HTML block `text` opens, which decides what closes it.

    A verbatim block is named by its OWN tag. Rustdoc keeps a "<pre>" block
    open across a literal "</style>" line, so accepting any of the four
    closing tags ended the block early and reported its content.
    """
    stripped = text.lstrip()
    verbatim = HTML_VERBATIM_RE.match(text)
    if verbatim:
        return verbatim.group(1).lower()
    if stripped.startswith("<!--"):
        return "comment"
    if stripped.startswith("<?"):
        return "instruction"
    if stripped.startswith("<![CDATA["):
        return "cdata"
    if stripped.startswith("<!"):
        return "declaration"
    return "tag"


def html_closes(text: str, kind: str) -> bool:
    """Does `text` end an open HTML block of `kind`?

    Its CONTENT is raw HTML, not Markdown -- Rustdoc renders a 26-word line
    between "<pre>" and "</pre>" preformatted, so counting it as a sentence
    reports a CH007 on something that is not prose.
    """
    if kind == "tag":
        return not text.strip()
    return any(closer in text.lower() for closer in HTML_CLOSERS[kind])


# A sentence boundary. The two abbreviations this corpus writes constantly
# are excluded, because neither ever ENDS a sentence in English -- splitting
# there cuts one sentence in two and a 27-word sentence carrying "e.g." then
# passes CH007. Only those two: "etc." and "vs." do end sentences, so
# excluding them would merge two real sentences and over-report instead.
# A closing delimiter may sit between the full stop and the space: a sentence
# ending in "**strong emphasis.**" puts two asterisks there, and requiring
# whitespace immediately after the stop merged it with the sentence that
# follows, reporting two compliant sentences as one long one.
#
# EMPHASIS ONLY, and every exclusion below was measured against the corpus
# rather than reasoned about. Each of the other closing delimiters produced
# false splits in this tree:
#
#   * A backtick: a code span carries punctuation constantly -- "`?`", "`.`",
#     "`foo.bar()`" -- so admitting it cut three hundred sentences at the
#     closing backtick.
#   * A closing paren: this tree writes "(first is `0`, second is `1`, ...)
#     -- so prefix explicitly", where the ellipsis is mid-sentence.
#   * A quote: "answer \"is `charge_card` held?\" during an incident" is one
#     sentence, and the question mark inside the quotation is not its end.
#
# A closing "**" after a full stop has no such second reading.
SENTENCE_SPLIT_RE = re.compile(
    r"(?<=[.!?])(?<!e\.g\.)(?<!E\.g\.)(?<!i\.e\.)(?<!I\.e\.)[*_]*\s+"
)


class Finding:
    __slots__ = ("rule", "path", "line", "text", "key")

    def __init__(
        self, rule: str, path: str, line: int, text: str, key: str | None = None
    ) -> None:
        self.rule = rule
        self.path = path
        self.line = line
        self.text = text
        # What identifies this finding across edits. Deliberately not the line
        # number, which moves whenever anything above it does.
        self.key = " ".join((key if key is not None else text).split())

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256(f"{self.rule}\x00{self.key}".encode()).hexdigest()
        return digest[:12]

    def as_dict(self) -> dict:
        return {"rule": self.rule, "path": self.path, "line": self.line, "text": self.text}


class Piece:
    """One line's worth of comment text, located and classified.

    `trailing` marks a comment that follows code on its line
    (`foo(); // note`). `block` marks `/* ... */` rather than `//`. A block
    comment spanning N lines yields N pieces, so every finding keeps a true
    line number.
    """

    __slots__ = (
        "line", "marker", "body", "trailing", "block", "group", "nest",
        "line_start", "line_end",
    )

    def __init__(
        self,
        line: int,
        marker: str,
        body: str,
        trailing: bool,
        block: bool,
        group: int = -1,
        nest: int = 0,
        line_start: bool = True,
    ) -> None:
        self.line = line
        self.marker = marker
        self.body = body
        self.trailing = trailing
        self.block = block
        # Which OUTERMOST `/* */` this piece came from. One block comment is
        # one comment however many lines it spans and however many comments
        # nest inside it, so its pieces stay in one run. -1 for line comments,
        # which are grouped by adjacency instead.
        self.group = group
        # How deep inside that comment the piece sits. A nested comment is
        # inside the enclosing one's fenced example if there is one, so it
        # inherits the fence; what it opens itself does not survive the close.
        self.nest = nest
        # Is this piece the end of its line? A nested comment's delimiters
        # are literal text in the rendered document, so anything after this
        # piece on the line -- even an EMPTY "/**/", which yields no piece at
        # all -- means the line does not end here. A closing fence may be
        # followed only by spaces, so this decides one.
        self.line_end = True
        # Does this piece begin its own line? A nested comment splits one
        # line into several pieces, and only the first of them starts at
        # column zero. Every Markdown BLOCK marker -- a fence, a list item, a
        # heading, a quote, a table row -- must start a line, so a piece that
        # begins after literal "/*" or "*/" text carries none of them. Only
        # the line rules apply to it, and its text joins the prose around it.
        self.line_start = line_start

    @property
    def text(self) -> str:
        """The comment body, leading `*` gutter and one space stripped."""
        body = self.body
        if self.block:
            body = re.sub(r"^\s*\*(?!\*)", "", body)
        return body[1:] if body.startswith(" ") else body


def gutter_only(body: str) -> bool:
    """Is `body` only a `*` gutter and whitespace?

    The same normalization `Piece.text` applies, asked as a question. A
    segment that reduces to nothing is not a blank LINE -- Rustdoc renders
    "* /* note */" as paragraph text, delimiters and all -- so emitting it
    ends a paragraph the document still holds open.
    """
    return not re.sub(r"^\s*\*(?!\*)", "", body).strip()


# A raw-string opener: r"", r#""#, br##""##, and the c"" / cr#""# forms.
RAW_OPEN_RE = re.compile(r"(?:b|c|br|cr|rb)?r(#*)\"")
IDENT_CHAR_RE = re.compile(r"[A-Za-z0-9_]")
# A char literal, as opposed to a lifetime tick. `'a'`, `'\n'`, `'\u{1F600}'`
# are literals; `'a` in `&'a str` is a lifetime and must not open a string.
CHAR_LIT_RE = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]{1,6}\}|[^\n])|[^\\'\n])'")


def extract_comments(source: str) -> list[Piece]:
    """Every Rust comment in `source`, and nothing that merely looks like one.

    A hand-rolled lexer rather than a line regex, because both halves matter:

    - Text inside a string is NOT a comment. This corpus embeds Rust and SQL
      snippets in raw strings constantly (fixtures, `include_str!` analogues,
      generated-source tests), and a `// ...` line inside one is string data.
      Flagging it would fail CI on a legitimate fixture -- and since Tier A
      gates at zero, that is a hard block on an innocent PR.
    - A comment is still a comment after code. `let n = 1; // TODO: fix` and
      `/* TODO: fix */` are exactly the defects this harness claims to gate,
      and a start-of-line regex never sees either.

    Rust specifics handled: nested block comments, raw strings with any hash
    count, byte/C-string prefixes, escapes, and the lifetime-vs-char-literal
    ambiguity.
    """
    pieces: list[Piece] = []
    i = 0
    n = len(source)
    line = 1
    code_on_line = False
    block_group = 0

    while i < n:
        ch = source[i]

        if ch == "\n":
            line += 1
            i += 1
            code_on_line = False
            continue

        # Line comment: runs to end of line.
        if source.startswith("//", i):
            end = source.find("\n", i)
            if end == -1:
                end = n
            raw = source[i:end]
            # The marker is the WHOLE leading slash run (plus `!` for `//!`).
            # `////` is an ordinary comment, not a doc comment, and taking only
            # three slashes left a stray `/` on the body that made the anchored
            # rules miss `//// let stale = compute();`.
            marker_len = len(raw) - len(raw.lstrip("/"))
            if raw[marker_len:marker_len + 1] == "!" and marker_len == 2:
                marker_len += 1
            marker = raw[:marker_len]
            pieces.append(Piece(line, marker, raw[marker_len:], code_on_line, False))
            i = end
            continue

        # Block comment: nests in Rust, so track depth rather than find("*/").
        if source.startswith("/*", i):
            marker = "/*"
            if source.startswith("/*!", i):
                marker = "/*!"
            elif source.startswith("/**", i) and not (
                source.startswith("/**/", i) or source.startswith("/***", i)
            ):
                # `/**/` is an empty comment, not a doc marker. Treating it as
                # one would eat the closing `*/` and swallow the rest of the
                # file as comment body.
                # `/***` is not one either: Rustdoc documents nothing for it,
                # checked by compiling one and finding the item's page empty.
                # Calling it a doc comment exempts its indented content as
                # rendered code and takes Tier A off a whole comment.
                marker = "/**"
            depth = 1
            block_group += 1
            root_group = block_group
            # Past the WHOLE marker: leaving the `!` of `/*!` on the body made
            # the anchored rules miss `/*! TODO */` and `/*! let x = 1; */`.
            i += len(marker)
            seg_start = i
            seg_line = line
            seg_nest = depth
            # The outer marker is not part of the rendered document, so the
            # text after it is still the first line's start. A nested `/*`
            # and a nested `*/` ARE rendered, literally, so text after either
            # of those is mid-line and can open no block.
            seg_start_of_line = True
            first = True
            line_started = False
            while i < n and depth > 0:
                if source.startswith("/*", i):
                    # Nested comment. End the segment here so the inner body
                    # starts a piece of its own; otherwise
                    # `/* outer /* let x = 1; */ */` hands the rules one string
                    # beginning "outer", and the nested code is never anchored.
                    # Only the gutter before the opener, on a line whose
                    # text carries on inside the nested comment. Round
                    # thirty-eight suppressed the empty segment a nested
                    # CLOSE leaves; this is the same defect on the opening
                    # side, and it ended the paragraph one line earlier.
                    if i > seg_start and not gutter_only(source[seg_start:i]):
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest, seg_start_of_line)
                        )
                        first = False
                    # The opener itself is text on this line, whether or not
                    # anything preceded it, so the line is no longer blank --
                    # and whatever came before it no longer ends the line.
                    line_started = True
                    if pieces and pieces[-1].line == line:
                        pieces[-1].line_end = False
                    # A nested comment stays in the enclosing comment's run,
                    # and `nest` records that it is inside it. The fence state
                    # is stacked per nesting level rather than reset here: the
                    # inner text is inside the outer fenced example if there
                    # is one, while a fence the inner text opens is discarded
                    # when it closes.
                    depth += 1
                    # Past the WHOLE nested marker, for the same reason the
                    # outer one does it: a retained `!` from `/*!` leaves the
                    # anchored rules staring at "! TODO".
                    nested = 2
                    if source.startswith("/*!", i):
                        nested = 3
                    elif source.startswith("/**", i) and not (
                        source.startswith("/**/", i) or source.startswith("/***", i)
                    ):
                        nested = 3
                    i += nested
                    seg_start = i
                    seg_line = line
                    seg_nest = depth
                    seg_start_of_line = False
                elif source.startswith("*/", i):
                    if depth > 1:
                        if i > seg_start and not gutter_only(source[seg_start:i]):
                            pieces.append(
                                Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest, seg_start_of_line)
                            )
                            first = False
                        line_started = True
                        # The nested "*/" is literal text too.
                        if pieces and pieces[-1].line == line:
                            pieces[-1].line_end = False
                    depth -= 1
                    i += 2
                    if depth > 0:
                        # Resuming the enclosing comment, same run and same
                        # nesting level it had before the nested comment.
                        seg_start = i
                        seg_line = line
                        seg_nest = depth
                        seg_start_of_line = False
                elif source[i] == "\n":
                    # A nested comment that closes at the end of a line leaves
                    # a zero-length segment behind. That is a blank SEGMENT,
                    # not a blank LINE, and emitting it ends the paragraph the
                    # line is still part of. A genuinely blank comment line
                    # emits nothing before the newline, so it still counts.
                    if i > seg_start or not line_started:
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest, seg_start_of_line)
                        )
                        first = False
                    line_started = False
                    line += 1
                    i += 1
                    seg_start = i
                    seg_line = line
                    seg_nest = depth
                    seg_start_of_line = True
                else:
                    i += 1
            tail_end = i - 2 if depth == 0 else i
            if tail_end > seg_start and not gutter_only(source[seg_start:tail_end]):
                pieces.append(
                    Piece(seg_line, marker, source[seg_start:tail_end], code_on_line and first, True, root_group, seg_nest, seg_start_of_line)
                )
            code_on_line = True
            continue

        # Raw string: no escapes, closed by `"` plus the opening hash count.
        raw_match = RAW_OPEN_RE.match(source, i)
        if raw_match and not (i > 0 and IDENT_CHAR_RE.match(source[i - 1])):
            closer = '"' + raw_match.group(1)
            end = source.find(closer, raw_match.end())
            end = n if end == -1 else end + len(closer)
            line += source.count("\n", i, end)
            i = end
            code_on_line = True
            continue

        # Ordinary string (and its b"" / c"" prefixed forms): honour escapes.
        if ch == '"':
            i += 1
            while i < n:
                if source[i] == "\\":
                    # A backslash-newline line continuation, which this corpus
                    # uses throughout its long SQL and `#[error(...)]` strings.
                    # The escaped character is a real newline and must still be
                    # counted, or every line number after it drifts.
                    if i + 1 < n and source[i + 1] == "\n":
                        line += 1
                    i += 2
                    continue
                if source[i] == '"':
                    i += 1
                    break
                if source[i] == "\n":
                    line += 1
                i += 1
            code_on_line = True
            continue

        # `'` is a char literal only when it actually closes; otherwise it is a
        # lifetime tick and the rest of the line is ordinary code.
        if ch == "'":
            lit = CHAR_LIT_RE.match(source, i)
            if lit:
                i = lit.end()
            else:
                i += 1
            code_on_line = True
            continue

        if not ch.isspace():
            code_on_line = True
        i += 1

    return pieces


def rust_sources(paths: list[str] | None) -> list[str]:
    """Every first-party .rs file, repo-relative and sorted for determinism."""
    if paths:
        out = []
        for p in paths:
            rel = os.path.relpath(os.path.abspath(p), REPO_ROOT)
            if rel.endswith(".rs") and os.path.isfile(os.path.join(REPO_ROOT, rel)):
                out.append(rel)
        return sorted(out)

    found = []
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for name in sorted(filenames):
            if name.endswith(".rs"):
                full = os.path.join(dirpath, name)
                found.append(os.path.relpath(full, REPO_ROOT))
    return sorted(found)


INFO_BACKTICK_RE = re.compile(r"`")


def closes_an_open_paren(marker: "re.Match[str]", run: list[str]) -> bool:
    """Is this "N)" the tail of a wrapped parenthesis rather than a marker?

    Accepting the "1)" ordered form makes every wrapped "(202)" and "(index
    1)" look like a list item on the line its digits land on. The paragraph so
    far says which: an unmatched "(" before it means the ")" closes that, and
    a sentence split there reports prose the author never wrote as two.
    """
    if marker.group(2).strip()[-1:] != ")":
        return False
    text = " ".join(run)
    return text.count("(") > text.count(")")


def interrupts_paragraph(marker: "re.Match[str]") -> bool:
    """May this list marker start an item in the middle of a paragraph?

    CommonMark's own rule. A wrapped "202)" or "503)" matches the ordered
    form, and splitting a paragraph there hides the long sentence it belongs
    to. Only a bullet, or the number one, may interrupt a paragraph; inside a
    list any number continues it.

    An item that is only its marker may not interrupt one at all: CommonMark
    requires an interrupting item's first line to carry content. It opens a
    list at the START of a block -- that is what round twenty-three added --
    but mid-paragraph a lone "-" stays paragraph text, and treating it as a
    container invents a fence allowance nothing in the document has.
    """
    if not marker.string[marker.end():].strip():
        return False
    marker_text = marker.group(2).strip()
    if not marker_text[:1].isdigit():
        return True
    # The NUMBER, not its spelling: CommonMark reads the marker's value, so
    # "01." starts at one and may interrupt exactly as "1." does.
    return int(marker_text[:-1]) == 1


def list_content(text: str, container: int) -> tuple[int, int] | None:
    """Where a list item's content starts on `text`: its column, and its index.

    Three CommonMark rules, each of which was a way to open a fence that is
    not there and silently exempt every comment after it:

    * The marker itself may be indented at most three columns past its
      container. Past that "    - ~~~rust" is an indented code line.
    * One to four columns after the marker are padding. Five or more means
      the content starts ONE column after the marker and the rest is
      indented code, so "-     ```rust" does not open a fence.
    * Padding is counted in COLUMNS, not characters. A tab is up to four
      columns wide, so a space and two tabs is three characters of padding
      and seven columns of it.
    """
    # A thematic break wins over a list item in CommonMark, and "* * *" or
    # "- - -" matches both. Reading one as an item opens a container -- and a
    # fence allowance -- where the rendered document has a horizontal rule.
    if thematic_break(text, container):
        return None
    marker = LIST_MARKER_RE.match(text)
    if not marker:
        return None
    indent = len(marker.group(1).expandtabs(4))
    if indent > container + 3:
        return None
    # Expanded from the start of the line: a tab's width depends on the column
    # it sits in, not on how many characters precede it.
    marker_column = indent + len(marker.group(2))
    padding = len(marker.group(0).expandtabs(4)) - marker_column
    # An item that is only its marker has no padding to measure. CommonMark
    # puts its content one column past the marker, same as the over-padded
    # case -- otherwise "-" alone opens no container and an indented fence
    # under it is recorded as top-level.
    if padding > 4 or not padding:
        return marker_column + 1, marker.start(3) + 1
    return marker_column + padding, marker.end()



def quote_marker(text: str, container: int) -> "re.Match[str] | None":
    """`text`'s block-quote marker, if it has one it is allowed to have.

    Three columns of indent, like every other CommonMark marker -- and like
    every other one, three columns PAST THE CONTAINER. A quote inside a list
    item whose content starts at column four is indented four and is still a
    quote; an absolute limit misses it and then misses the fence inside it.
    """
    match = BLOCKQUOTE_RE.match(text)
    if not match or len(match.group(1).expandtabs(4)) > container + 3:
        return None
    return match


def strip_quote(text: str, container: int = 0) -> str:
    """`text` past any block-quote marker.

    Every container judgement below measures from here, so a quoted fence,
    list or indent reads exactly as the unquoted form does.
    """
    match = quote_marker(text, container)
    return text[match.end():] if match else text


def setext_underline(text: str, container: int) -> bool:
    """Is `text` a Setext underline its container would accept?

    Three columns past the container, like every marker here. This is the
    fifth pattern in this file to need that rule and the fifth added without
    it, so: a marker pattern here is wrong by default until it measures
    against the container.
    """
    match = SETEXT_RE.match(text)
    return bool(match) and len(match.group(1).expandtabs(4)) <= container + 3


def heading(text: str, container: int) -> bool:
    """Is `text` an ATX heading its container would accept?

    Three columns past the container, like every marker here. This is the
    seventh pattern in this file to need that rule and the seventh written
    without it, so it is worth stating plainly: an unbounded indent in a
    marker pattern is a defect, and the pattern is wrong until it takes a
    container. Four columns into a paragraph "# text" is indented content,
    and reading it as a heading clears a paragraph the rendered document
    still holds open.
    """
    match = HEADING_RE.match(text)
    return bool(match) and len(match.group(1).expandtabs(4)) <= container + 3


def thematic_break(text: str, container: int) -> bool:
    """Is `text` a thematic break its container would accept?

    Three columns past the container, like every other CommonMark marker.
    Past that it is indented content, and treating it as a break clears a
    paragraph the rendered document still has open.
    """
    match = THEMATIC_BREAK_RE.match(text)
    return bool(match) and len(match.group(1).expandtabs(4)) <= container + 3


def strip_quote_levels(text: str, levels: int, container: int) -> str:
    """`text` past exactly `levels` block-quote markers, and no more.

    Inside a fence only the markers belonging to the fence's own container
    are continuation syntax. A deeper "> " is literal sample text, and
    stripping it turns an example's own quoted delimiter into a closer.
    """
    for _ in range(levels):
        match = ONE_QUOTE_RE.match(text)
        if not match or len(match.group(1).expandtabs(4)) > container + 3:
            break
        text = text[match.end():]
        # Inside the quote the content restarts at column zero.
        container = 0
    return text


def quote_depth(text: str, container: int = 0) -> int:
    """How many block-quote levels `text` opens with."""
    match = quote_marker(text, container)
    return match.group(0).count(">") if match else 0


def leading_columns(text: str) -> int:
    """The visual column `text`'s content starts at.

    Columns, not characters: a tab is up to four columns, so every
    indentation comparison here has to expand before comparing.
    """
    return len(text[: len(text) - len(text.lstrip())].expandtabs(4))


def container_at_depth(stack: list[tuple[int, int]], depth: int) -> int:
    """The innermost container column recorded at `depth`, else zero.

    Columns are only meaningful within one quote depth: an entry pushed while
    unquoted is a raw column, one pushed inside a quote is measured after the
    marker. A fence must remember the container of ITS OWN frame, or its body
    reads as dedented out of a list it was never in.
    """
    inner = 0
    for column, entry_depth in stack:
        if entry_depth == depth:
            inner = column
    return inner


def leaves_container(text: str, scope: tuple[int, int]) -> bool:
    """Has `text` dedented out of the container an open fence started in?

    CommonMark ends a fenced block with its container, closing delimiter or
    not. Without this an unclosed fence in one list item stays open over every
    later comment in the run -- a gate that silently stops gating, which is
    the failure this harness exists to prevent.
    """
    outer, container, depth = scope
    # Two frames, because a quote marker and the content behind it are not
    # measured the same way. `outer` is the container the MARKER sits in --
    # a quote inside a list item is indented to the item. `container` is the
    # fence's own container once that marker is stripped. Reading either in
    # the other's frame ends the fence on its own body.
    #
    # Columns only compare inside one quote depth: a shallower line has left
    # the quote outright, and a deeper one is nested INSIDE the container, so
    # its post-strip column says nothing about leaving it.
    line_depth = quote_depth(text, outer)
    if line_depth != depth:
        return line_depth < depth
    body = strip_quote(text, outer)
    if not body.strip():
        return False
    return leading_columns(body) < container


def update_containers(
    text: str,
    stack: list[tuple[int, int]],
    paragraph: bool,
    quoted: int = 0,
    table: bool = False,
) -> tuple[list[tuple[int, int]], bool]:
    """The open list containers after `text`, innermost last, and whether a
    paragraph is still open.

    Each entry is (content column, quote depth). A stack, not one column: a
    line that dedents out of a nested item lands back in its PARENT, and
    collapsing to zero there records any fence opened on it as top-level, so
    the fence outlives the list and swallows every later comment.

    The depth is what ends a quoted list. A list opened inside "> " does not
    survive the quote, and a stale container makes an over-indented top-level
    line look like a fence relative to a list that is no longer open.

    `paragraph` applies CommonMark's interruption rule to the marker itself,
    not just to sentence splitting: "2." partway through an item's paragraph
    continues that paragraph and does not open a nested list. Any pop clears
    it -- dedenting out of an item ends the paragraph inside it, which is why
    "2." on the line after "1." still starts an item rather than continuing
    one. Simplified in one way: any non-blank line opens a paragraph, so a
    table or heading does not close one here.
    """
    # Read against the container in force before this line: a quote's own
    # indent allowance is relative to whatever list item still holds it.
    enclosing = stack[-1][0] if stack else 0
    depth = quote_depth(text, enclosing)
    body = strip_quote(text, enclosing)
    # A block quote is its own block, so crossing into or out of one ends the
    # paragraph -- the prose path has done this since round twenty-four and
    # the container path never did, so a "22." after a quote was refused the
    # container the rendered document gives it.
    if depth != quoted:
        paragraph = False
    popped = len(stack)
    stack = [entry for entry in stack if entry[1] <= depth]
    if body.strip():
        indent = leading_columns(body)
        # Same-depth only, for the same reason. A block quote nested in a list
        # item sits INSIDE it, and its stripped body starts at column zero --
        # popping on that closes the item that contains the quote.
        while stack and stack[-1][1] == depth and stack[-1][0] > indent:
            stack.pop()
    if len(stack) < popped:
        paragraph = False

    marker = LIST_MARKER_RE.match(body)
    content = list_content(body, stack[-1][0] if stack else 0)
    if content and (not paragraph or interrupts_paragraph(marker)):
        stack.append((content[0], depth))
    # Only paragraph content leaves a paragraph open. A heading, a thematic
    # break or a table row is its own block, and treating one as a paragraph
    # refuses the next "22." a container it is entitled to. A list marker's
    # own text IS a paragraph, so those still count.
    # Only a CommonMark block ends a paragraph. A pipe-prefixed line is not
    # one -- tables are a GFM extension, and "| not a table" is prose, so
    # clearing here would hand the next "22." a container the rendered
    # document has not. A Setext underline IS one, but only with a paragraph
    # above it to underline.
    prose = bool(body.strip()) and not (
        table
        or heading(body, stack[-1][0] if stack else 0)
        or html_block(body, stack[-1][0] if stack else 0)
        or thematic_break(body, stack[-1][0] if stack else 0)
        or (paragraph and setext_underline(body, stack[-1][0] if stack else 0))
    )
    return stack, prose


def lazy_continuation(
    text: str,
    peeled: str,
    container: int,
    depth: int,
    quoted: int,
    paragraph: bool,
    pieces: list,
    index: int,
    nest: int,
) -> bool:
    """Does this line carry a quoted paragraph on without a marker of its own?

    CommonMark lets a paragraph inside a block quote continue on a line that
    has no ">" at all, so leaving the quote by depth is not always leaving
    the block. Rustdoc renders "> Explain the `literal" and an unmarked
    "TODO: issue required` suffix." as ONE quoted paragraph with the marker
    inside a code span.

    Anything that BEGINS a block ends the continuation instead -- a list
    marker, a confirmed table, a heading, an HTML block, a rule, a fence --
    which is the whole of the difference between this and "no marker here".
    """
    return (
        depth < quoted
        and paragraph
        and bool(text.strip())
        # On the RAW line, not the peel: `strip_containers` has already
        # taken the marker off `peeled`, so this test could never fire and
        # a list leaving a quote was read as a continuation of it.
        #
        # Not paragraph-gated, unlike `starts_block`. The quote's paragraph
        # is not open at the level this line lands on -- Rustdoc closes the
        # quote and opens "<ol start=\"2\">" -- so even a marker that could
        # not interrupt a paragraph starts a block here.
        and not list_content(text, container)
        and not table_header(pieces, index, nest, peeled, container)
        and not heading(peeled, container)
        and not html_block(peeled, container)
        # The COMMONMARK break, not the broad decorative rule. `SEPARATOR_RE`
        # answers "is this a section rule?" -- unbounded on purpose, because
        # a rule is not a word of a sentence at any indent, which is the
        # question `prose_units` asks. This asks whether a BLOCK begins, and
        # there the answers differ twice over: "===" is ordinary text to
        # CommonMark, and an over-indented "---" is indented content. Rustdoc
        # keeps both inside the quoted paragraph.
        and not thematic_break(peeled, container)
        and not opens_fence(peeled, container)
    )


def strip_containers(
    text: str, stack: list[tuple[int, int]], container: int
) -> tuple[str, int, int]:
    """Peel container markers off `text` until a fence delimiter could show.

    One line can open several containers at once: "- > ```rust" is a list item
    holding a block quote holding a fence, and "- 1. ```rust" is two lists.
    Each marker restarts the content column, so they are peeled one at a time
    and the next is measured in the frame the last one left.

    Which frame that is depends on the marker. A list marker on THIS line
    opens an item with nothing in it yet, so the next frame starts at zero.
    Crossing a quote enters a depth the stack may already have a container
    for -- a quoted list item, say -- and its column is the one recorded
    there.

    Returns the remaining text, the container to measure the delimiter
    against, the quote depth reached, and the column the content sits at.
    The last two are the fence's scope, and neither can be recovered from the
    raw line: a quote behind a list marker is invisible there, and a fence
    inside "- 1. " belongs to the INNER item, not the outer one.
    """
    depth = 0
    column = container
    origin = 0
    while True:
        quote = quote_marker(text, container)
        if quote:
            text = text[quote.end():]
            # One match can carry several markers (">>", "> >").
            depth += quote.group(2).count(">")
            container = column = container_at_depth(stack, depth)
            origin = 0
            continue
        content = list_content(text, container)
        if content:
            text = text[content[1]:]
            column = origin = origin + content[0]
            container = 0
            continue
        return text, container, depth, column


def fence_delimiter(
    text: str,
    container: int,
    in_fence: bool = False,
    depth: int = 0,
    stack: list[tuple[int, int]] | None = None,
) -> tuple["re.Match[str]", str, int, int] | None:
    """The fence delimiter on `text`, or None if the line is not one.

    Two things separate a delimiter from ordinary text. A fence may open on
    the same line as the container markers holding it -- a list item, a block
    quote, or several nested -- so those are peeled before matching. And
    CommonMark allows at most three columns of indent *relative to the
    container*, for closers as much as for openers: past that the line is
    indented content -- fenced content while a fence is open, an indented code
    line while one is not. Returns the match and the text it was matched
    against, which carries the info string.
    """
    # Markers are peeled only as far as they are syntax. Inside a fence the
    # sample text is literal: "- ```" is a hyphen and three backticks of
    # example content, not an item whose body closes the fence, and a "> "
    # deeper than the fence's own container is sample text too.
    if in_fence:
        tail, reached, column = strip_quote_levels(text, depth, container), depth, container
    else:
        tail, container, reached, column = strip_containers(text, stack or [], container)
    match = FENCE_RE.match(tail)
    if not match:
        return None
    if len(match.group(1).expandtabs(4)) > container + 3:
        return None
    return match, tail, reached, column



def fence_transition(
    fence: tuple[str, int] | None, match: "re.Match[str]", tail: str
) -> tuple[str, int] | None:
    """Apply one fence-delimiter line to the fence state.

    CommonMark, not "any three backticks toggle it". A closer must use the
    SAME character as its opener and be at least as long, with nothing after
    it. Otherwise a ``` line inside a ````-fenced example closes the fence
    early and the example's own sample text is then read as real comments.

    `tail` is what `fence_delimiter` matched -- the line past any list
    marker -- so `match.end()` indexes into it, not into the raw line.
    """
    delimiter = match.group(2)
    char, length = delimiter[0], len(delimiter)
    info = tail[match.end():]
    if fence is None:
        # Opening. An info string ("```rust") is allowed, but CommonMark
        # forbids a backtick inside a BACKTICK fence's info string -- so
        # "```foo`bar" is not a fence at all, and treating it as one opens a
        # fence that never closes and suppresses the rest of the run.
        if char == "`" and INFO_BACKTICK_RE.search(info):
            return None
        return char, length
    open_char, open_length = fence
    closes = char == open_char and length >= open_length and not tail[match.end():].strip()
    return None if closes else fence


def comment_runs(pieces: list[Piece]):
    """Group pieces into contiguous comment blocks.

    A run is what a reader sees as one comment: consecutive lines, same kind.
    A trailing comment is always its own run -- it is a note on its line, not
    a continuation of the note on the line above, even when the two are
    adjacent.
    """
    run: list[Piece] = []
    prev_line = -2
    for piece in pieces:
        same_block = (
            bool(run) and piece.block and run[-1].block and piece.group == run[-1].group
        )
        if run and not same_block and (
            piece.trailing
            or run[-1].trailing
            or piece.block != run[-1].block
            or piece.group != run[-1].group
            or piece.marker != run[-1].marker
            or piece.line != prev_line + 1
        ):
            yield run
            run = []
        run.append(piece)
        prev_line = piece.line
    if run:
        yield run


def nesting_shift(saved: list, nest: int, state: tuple) -> tuple[list, tuple]:
    """Carry block state across a nested comment boundary.

    Going deeper INHERITS the enclosing state -- a nested comment inside a
    fenced example is part of that example, so its text is sample text.
    Coming back out RESTORES what was saved, so nothing the nested comment
    opened leaks into the text that resumes after it closes.

    `state` is EVERY piece of block state the caller carries, not just the
    fence: a list marker inside a nested comment is ordinary paragraph text
    to Rustdoc, and letting it push a container onto the enclosing run's
    stack gives a later delimiter a fence allowance nothing opened.

    "Every" is checked by hand at each caller, and a variable added to a loop
    without adding it here leaks silently. `in_table` did exactly that: a
    table inside a nested comment left the enclosing run reading pipe lines
    as table rows, so the paragraph they belong to closed early.
    """
    saved = list(saved)
    while len(saved) < nest:
        saved.append(state)
    while len(saved) > nest:
        state = saved.pop()
    return saved, state


def comment_lines(pieces: list[Piece]):
    """Yield (lineno, text, in_fence) per comment piece, tracking ``` fences.

    Fence state is per RUN, never file-wide. An unclosed ``` in one doc block
    would otherwise leave the fence open for every later comment in the file,
    silently skipping all of them -- a gate that stops gating without failing.
    A fence delimiter is yielded with in_fence True so callers skip it along
    with the fenced body.
    """
    for run in comment_runs(pieces):
        fence: tuple[str, int] | None = None
        scope = (0, 0, 0)
        stack: list[tuple[int, int]] = []
        paragraph = False
        quoted = 0
        in_table = False
        indented = False
        html: str | None = None
        # A one-line block ENDED on the previous line, so this line begins a
        # new one. `starts_block` answers "does a block begin here", which
        # every multi-line block also answers on its own first line -- but a
        # heading is one line, and the paragraph under it says nothing.
        after_block = False
        saved: list = []
        nest = run[0].nest
        for index, piece in enumerate(run):
            if piece.nest != nest:
                saved, (
                    fence, scope, stack, paragraph, quoted, in_table, indented, html
                ) = nesting_shift(
                    saved,
                    piece.nest,
                    (
                        fence,
                        scope,
                        list(stack),
                        paragraph,
                        quoted,
                        in_table,
                        indented,
                        html,
                    ),
                )
                nest = piece.nest
            text = piece.text
            # Does this line BEGIN a block? A code span cannot reach across
            # one: Rustdoc renders "- a `literal" and "- TODO: x` suffix" as
            # two list items with literal backticks between them, and pairing
            # the two blanked a marker that is ordinary text.
            opens = False
            # A piece that does not begin its own line can open no block.
            # "Outer /* ```rust" puts the backticks after literal text, and
            # Rustdoc renders the whole line as a paragraph; opening a fence
            # there exempts every line until the next delimiter.
            if not piece.line_start:
                yield piece.line, text, fence is not None or html is not None, False, None
                continue
            # Carried from the previous LINE, and cleared here so it applies
            # exactly once. A piece that does not begin its own line is the
            # same line, so it must not consume the carry.
            opens, after_block = after_block, False
            # Inside a raw HTML block nothing is Markdown, so no fence, list
            # or table opens here and the text is not prose. A type-6 block
            # ends AT a blank line, which is a block boundary in its own
            # right and must still be seen; the others end on the line that
            # carries their closer.
            if html is not None:
                inside = strip_quote(text, stack[-1][0] if stack else 0)
                if html == "tag" and html_closes(inside, html):
                    html = None
                else:
                    if html_closes(inside, html):
                        html = None
                    yield piece.line, text, True, False, None
                    continue
            if fence is not None and leaves_container(text, scope):
                fence = None
            if fence is None:
                # A CONFIRMED table is a block and ends the paragraph. A bare
                # pipe line is not, and must not -- round thirty-four's
                # asymmetry, now with the structure to tell them apart.
                enclosing = stack[-1][0] if stack else 0
                body = strip_quote(text, enclosing)
                if in_table:
                    in_table = not starts_block(body, enclosing)
                else:
                    in_table = table_header(run, index, piece.nest, body, enclosing)
                # Peeled, and measured in the frame the peel leaves: a code
                # block may begin on the marker line itself, and
                # "-     let x = compute();" is an item holding four columns
                # of indented code, not a bullet with a defect in it.
                code_text, code_container = strip_containers(text, stack, enclosing)[:2]
                indented = indented_code(
                    code_text,
                    code_container,
                    paragraph,
                    indented,
                    piece.marker in DOC_MARKERS,
                )
                # `body` has had its quote marker peeled, so `starts_block`
                # can no longer see one -- and a quote is a block. The DEPTH
                # is what says so, and it says it for the line that LEAVES
                # one as well, which no marker on the line could.
                before_quoted = quoted
                # From the paragraph state BEFORE `update_containers` clears
                # it: an underline with no paragraph above it is a thematic
                # break or ordinary text, not a heading.
                # `update_containers` reassigns `paragraph` below, and both
                # the lazy test and the Setext test need the state this line
                # ARRIVED in.
                open_paragraph = paragraph
                # Both are blocks, so neither leaves a paragraph open.
                stack, paragraph = update_containers(
                    text, stack, paragraph, quoted, in_table or indented
                )
                # From the PEEL, as `prose_units` reads it. "- > text" is a
                # quote inside a list item, and reading the raw line reports
                # depth zero because the marker comes first -- one loop then
                # believes the line left the quote and the other does not.
                peeled_now, _, quoted, _ = strip_containers(
                    text, stack, stack[-1][0] if stack else 0
                )
                # A quoted paragraph may carry on across a line with no ">"
                # of its own, and that line LEAVES the quote by depth while
                # staying inside the block. Cutting the span there split one
                # quoted sentence in half and failed the build on a marker
                # Rustdoc renders inside a code span.
                lazy = lazy_continuation(
                    text,
                    peeled_now,
                    enclosing,
                    quoted,
                    before_quoted,
                    open_paragraph,
                    run,
                    index,
                    piece.nest,
                )
                # A confirmed table and a Setext heading are blocks that
                # `starts_block` cannot name. A table needs the piece after
                # this one to confirm it, and a Setext underline needs the
                # paragraph above it, so neither is a fact about one line.
                # Rustdoc renders a span's two halves literally across
                # either, and every ROW is its own boundary: "| x `open |"
                # and "| close` y |" are separate cells, and the backticks
                # stay literal in both.
                #
                # DOC COMMENTS ONLY, by the rule round forty-eight set for
                # Setext and round fifty-two for HTML. Rustdoc renders no
                # `//` comment, so a pipe row there is text and a rule of
                # "=" is a banner this tree draws under a plain heading.
                # A Setext underline underlines the paragraph ABOVE IT, and
                # only one in its own container. A line that leaves a quote
                # is not underlining the quoted paragraph it follows:
                # Rustdoc keeps "===" under a lazily continued quote as
                # ordinary text, in the paragraph, not as a heading rule.
                setext = (
                    open_paragraph
                    and setext_underline(body, enclosing)
                    and quoted == before_quoted
                )
                # A LEAF block that is exactly one line long. Rustdoc
                # renders "# Explain the `literal" as a heading and the line
                # under it as its own paragraph, so a span cannot pair across
                # the two -- and nothing on the paragraph's own line says a
                # block began, because none did. It ended.
                after_block = (
                    heading(body, enclosing)
                    or thematic_break(body, enclosing)
                    or (piece.marker in DOC_MARKERS and setext)
                )
                opens = (
                    opens
                    or starts_block(body, enclosing, open_paragraph)
                    or (quoted != before_quoted and not lazy)
                    or (
                        piece.marker in DOC_MARKERS
                        and (in_table or setext)
                    )
                )
            container = stack[-1][0] if stack else 0
            delimiter = fence_delimiter(text, container, fence is not None, scope[2], stack)
            # The whole LINE decides a delimiter, not this piece alone. A
            # closer may be followed only by spaces, and an opener's info
            # string runs to the end of the line, where a backtick makes it
            # invalid. Text after a nested comment on the same line is part
            # of both.
            if delimiter:
                tail = line_tail(run, index)
                # A backtick is forbidden in a BACKTICK fence's info string
                # and nowhere else: "~~~rust `info`" is a valid opener, and
                # rejecting it discarded the fence and reported its content.
                backtick = delimiter[0].group(2).startswith("`")
                # The info string is the rest of THIS piece past the
                # delimiter, plus whatever follows a nested comment on the
                # same line. Reading only the cross-piece tail missed the
                # ordinary one-piece form, "```rust `info`", entirely.
                info = delimiter[1][delimiter[0].end():] + tail
                if (
                    fence is not None and (tail.strip() or not piece.line_end)
                ) or (fence is None and backtick and "`" in info):
                    delimiter = None
            if delimiter:
                before = fence
                fence = fence_transition(fence, delimiter[0], delimiter[1])
                # A fence is a block, so it ends the paragraph before it.
                # Otherwise the opener leaves `paragraph` set and the next
                # "22." is refused a container it is entitled to.
                paragraph = False
                if before is None and fence is not None:
                    # The container matters here too: a quote nested in a
                    # list item is indented past column zero, so reading its
                    # depth without the container saves zero and the fence
                    # then never sees its own closing delimiter. The saved
                    # column has to come from the same frame as that depth.
                    # The depth comes from the PEEL, not from reading the
                    # raw line: once a quote follows a list marker on the
                    # same line, the unpeeled line reports depth zero and the
                    # fence never recognises its own quoted closer.
                    # Both from the PEEL, not from reading the raw line: a
                    # quote behind a list marker is invisible there, and a
                    # fence inside "- 1. " belongs to the inner item.
                    scope = (container, delimiter[3], delimiter[2])
                # Fence SYNTAX only if it opened one, closed one, or sits
                # inside one. An invalid opener (```foo`bar) is ordinary text
                # and must still be scanned -- exempting it would hide the
                # defect that rejecting it exists to expose.
                yield piece.line, text, fence is not None or before is not None, True, None
                continue
            # An HTML block opens here, outside any fence. Its own line is
            # ordinary text -- "<pre>" carries no defect -- but everything
            # until its closer is raw HTML.
            # Peeled through EVERY container, like the fence test: "- <pre>"
            # and "> <pre>" both open the block their container holds, and a
            # peel that takes only quote markers leaves the list marker in
            # front of the tag and recognizes neither.
            peeled = strip_containers(text, stack, container)[0]
            # DOC COMMENTS ONLY, as indented code and Setext are. Rustdoc
            # renders no `//` comment, so "<pre>" in one is text rather than
            # markup -- and exempting the run took CH001 and CH002 off every
            # line inside it. A ``` fence stays exempt in any comment: that
            # is an author saying "this is an example", which a tag in an
            # unrendered comment is not.
            if (
                fence is None
                and piece.marker in DOC_MARKERS
                and html_block(peeled, container)
            ):
                html = html_kind(peeled)
                if html != "tag" and html_closes(peeled, html):
                    html = None
                # The opener's line is inside the block it opens. Rustdoc
                # renders "<pre>TODO: x</pre>" preformatted, so the line
                # rules must not read that TODO as a defect.
                yield piece.line, text, True, True, None
                continue
            # A confirmed table row carries its cell boundaries with it.
            # They are the one block boundary that falls INSIDE a line, so
            # the span pass cannot derive them from `opens` alone.
            cells = (
                row_cell_spans(text, enclosing)
                if in_table and piece.marker in DOC_MARKERS and fence is None
                else None
            )
            yield piece.line, text, fence is not None or indented, opens, cells


def check_line_rules(path: str, pieces: list[Piece]) -> list[Finding]:
    """CH001/CH002/CH003/CH005/CH006 -- all single-line judgements."""
    findings = []
    lines = []
    spanless = []
    # PER RUN. A code span belongs to one comment, so joining the whole file
    # let an unmatched backtick in one comment pair with a backtick in an
    # unrelated one further down and blank everything between -- including
    # the intervening code's own comments, and an absolute rule with them.
    for run in comment_runs(pieces):
        run_lines = list(comment_lines(run))
        lines.extend(run_lines)
        # Fenced lines are emptied rather than dropped, so their backticks
        # cannot open a span over the prose after them and every index still
        # lines up.
        #
        # And the run is cut where a BLOCK begins. A span belongs to one
        # block, not merely to one comment: two list items are two blocks,
        # and pairing a backtick across them blanked a marker Rustdoc leaves
        # as ordinary text.
        doc = run[0].marker in DOC_MARKERS
        block: list[str] = []

        def flush_block(lines: list[str], cells: list) -> list[str]:
            # ACROSS the block, like the spans beside it. An unclosed
            # "<code>" runs to the paragraph's end, so a marker on the line
            # under it is rendered as code too, and blanking line by line
            # left that marker exposed and failed the build on it.
            #
            # A TABLE ROW is the exception, and the only one: its cells are
            # separate inline contexts, so a backtick in one cannot pair
            # with a backtick in the next. Every row is its own block, so
            # such a block is one line.
            if len(lines) == 1 and cells and cells[0]:
                blanked = [blank_cells(lines[0], cells[0])]
            else:
                blanked = blank_spans_across(lines)
            if not doc:
                return blanked
            return blank_inline_code("\n".join(blanked)).split("\n")

        block_cells: list = []
        for _, text, in_fence, opens, cells in run_lines:
            if opens and block:
                spanless.extend(flush_block(block, block_cells))
                block, block_cells = [], []
            block.append("" if in_fence else text)
            block_cells.append(None if in_fence else cells)
        spanless.extend(flush_block(block, block_cells))
    for index, (lineno, body, in_fence, _, _) in enumerate(lines):
        if in_fence:
            continue
        stripped = body.strip()
        if not stripped:
            continue

        # CH001 is anchored, so it has to see past any container marker: a
        # list or quote is not a code fence, and the documented exemption is
        # the fence. "- let stale = compute();" is commented-out code with a
        # bullet in front of it. Peeled without a container, so only markers
        # CommonMark would accept at the left margin are removed.
        # BLANKED first, as CH002 beside it is. A code span may wrap, so
        # "Demonstrates `" over "let x = compute();" renders the statement
        # as <code> -- an example of Rust, not Rust that was commented out,
        # and failing the build on it stops the example being written.
        spanless_body = spanless[index].strip()
        code_line = strip_containers(spanless_body, [], 0)[0].strip() or spanless_body
        if COMMENTED_CODE_RE.match(code_line):
            findings.append(Finding("CH001", path, lineno, stripped))

        # CH002 is anchored at the start of the line for its unpunctuated
        # form, so it needs the same peel CH001 does: "- TODO fix this" is a
        # commitment with a bullet in front of it, and only a FENCE exempts.
        # Blanked first: a marker inside an inline code span is the syntax
        # being described, not a commitment being made. CH005 and CH006 read
        # the raw text on purpose -- see KNOWN LIMITATIONS -- but those are
        # ratcheted, and this one fails the build outright.
        spanless_line = strip_containers(spanless[index].strip(), [], 0)[0].strip()
        # The reference is read from the BLANKED text too, not the raw
        # line. "The `#123` syntax" documents a marker's shape; it does not
        # track the commitment beside it, and reading the raw line let it
        # stand in for one.
        if untracked_marker(spanless_line or spanless[index].strip()):
            findings.append(Finding("CH002", path, lineno, stripped))

        archaeology = ARCHAEOLOGY_RE.search(stripped)
        if archaeology:
            findings.append(Finding("CH005", path, lineno, stripped))

        contraction = CONTRACTION_RE.search(stripped)
        if contraction:
            findings.append(Finding("CH006", path, lineno, stripped))

    return findings


def check_block_edges(path: str, pieces: list[Piece]) -> list[Finding]:
    """CH004 -- a comment run that opens or closes on an empty comment line.

    Only leading `//` runs are judged. A trailing comment has no "block" to
    have edges, and a `/* */` body's blank first line is conventional
    formatting rather than the editing artifact this rule is about.
    """
    findings = []
    for run in comment_runs(pieces):
        if run[0].block or run[0].trailing:
            continue
        if not run[0].text.strip():
            findings.append(
                Finding("CH004", path, run[0].line, "block opens on an empty comment line")
            )
        if len(run) > 1 and not run[-1].text.strip():
            findings.append(
                Finding("CH004", path, run[-1].line, "block closes on an empty comment line")
            )
    return findings


def prose_units(pieces: list[Piece]) -> list[tuple[int, str]]:
    """Comment prose as (lineno, text) units, ready to split into sentences.

    Fenced blocks, markdown tables and headings are dropped -- they are not
    prose and a word count over them means nothing. A list item starts its own
    unit so that a bulleted rationale is measured per bullet.

    Units never span a comment run, so two adjacent trailing comments stay two
    units. Joining them on line adjacency alone would report two short notes
    as one long sentence -- a false CH007 on code neither author wrote as a
    sentence.
    """
    units: list[tuple[int, str, list[tuple[int, int]]]] = []

    for block in comment_runs(pieces):
        run: list[str] = []
        run_lines: list[int] = []
        fence: tuple[str, int] | None = None
        scope = (0, 0, 0)
        stack: list[tuple[int, int]] = []
        paragraph = False
        in_list = False
        quoted = 0
        saved: list = []
        nest = block[0].nest

        def flush():
            nonlocal run, run_lines
            if run:
                # Each fragment's offset in the joined text, paired with the
                # source line it came from. Joining loses that otherwise, and
                # every sentence in the unit then reports the unit's FIRST
                # line -- pointing a contributor at an unrelated comment.
                spans, position = [], 0
                for fragment, line in zip(run, run_lines):
                    spans.append((position, line))
                    position += len(fragment) + 1
                units.append(
                    (run_lines[0], " ".join(run), spans, run_marker in DOC_MARKERS)
                )
            run, run_lines = [], []

        in_table = False
        indented = False
        html: str | None = None
        # One run carries one marker -- `comment_runs` splits where it
        # changes -- and the absolute rules need it to know whether inline
        # HTML in this text is markup or just characters.
        run_marker = block[0].marker
        for index, piece in enumerate(block):
            if piece.nest != nest:
                # No flush: a sentence that crosses an inline nested comment
                # is still one sentence in the rendered documentation, since
                # the delimiters and the text between them are literal. Only
                # the BLOCK state is isolated by the nesting.
                saved, (
                    fence,
                    scope,
                    stack,
                    paragraph,
                    quoted,
                    in_list,
                    in_table,
                    indented,
                    html,
                ) = nesting_shift(
                    saved,
                    piece.nest,
                    (
                        fence,
                        scope,
                        list(stack),
                        paragraph,
                        quoted,
                        in_list,
                        in_table,
                        indented,
                        html,
                    ),
                )
                nest = piece.nest
            body = piece.text
            # Mid-line, so no block marker of its own -- see `comment_lines`.
            # The text is still part of the line's sentence, so it joins the
            # run rather than starting one.
            if not piece.line_start:
                if fence is not None or html is not None:
                    flush()
                    in_list = False
                elif body.strip():
                    run.append(body.strip())
                    run_lines.append(piece.line)
                continue
            # Raw HTML is not Markdown and not prose -- see `comment_lines`,
            # which carries this state the same way.
            if html is not None:
                inside = strip_quote(body, stack[-1][0] if stack else 0)
                if html == "tag" and html_closes(inside, html):
                    html = None
                else:
                    if html_closes(inside, html):
                        html = None
                    flush()
                    in_list = False
                    continue
            if fence is not None and leaves_container(body, scope):
                fence = None
            if fence is None:
                # The table is decided BEFORE the containers, because a
                # confirmed table is a block and ends the paragraph above
                # it. `comment_lines` has done this since round thirty-eight
                # and this loop did not, so a "22." after a table was
                # refused its container and the fence under it went unseen.
                enclosing = stack[-1][0] if stack else 0
                peek = strip_quote(body, enclosing)
                if in_table:
                    in_table = not starts_block(peek, enclosing)
                else:
                    in_table = table_header(block, index, piece.nest, peek, enclosing)
                code_text, code_container = strip_containers(body, stack, enclosing)[:2]
                indented = indented_code(
                    code_text,
                    code_container,
                    paragraph,
                    indented,
                    piece.marker in DOC_MARKERS,
                )
                # Both are blocks, so neither leaves a paragraph open.
                stack, paragraph = update_containers(
                    body, stack, paragraph, quoted, in_table or indented
                )
            container = stack[-1][0] if stack else 0
            delimiter = fence_delimiter(body, container, fence is not None, scope[2], stack)
            # The whole LINE decides a delimiter, not this piece alone. A
            # closer may be followed only by spaces, and an opener's info
            # string runs to the end of the line, where a backtick makes it
            # invalid. Text after a nested comment on the same line is part
            # of both.
            if delimiter:
                tail = line_tail(block, index)
                # A backtick is forbidden in a BACKTICK fence's info string
                # and nowhere else: "~~~rust `info`" is a valid opener, and
                # rejecting it discarded the fence and reported its content.
                backtick = delimiter[0].group(2).startswith("`")
                # The info string is the rest of THIS piece past the
                # delimiter, plus whatever follows a nested comment on the
                # same line. Reading only the cross-piece tail missed the
                # ordinary one-piece form, "```rust `info`", entirely.
                info = delimiter[1][delimiter[0].end():] + tail
                if (
                    fence is not None and (tail.strip() or not piece.line_end)
                ) or (fence is None and backtick and "`" in info):
                    delimiter = None
            if delimiter:
                before = fence
                fence = fence_transition(fence, delimiter[0], delimiter[1])
                paragraph = False
                if before is None and fence is not None:
                    scope = (container, delimiter[3], delimiter[2])
                if fence is not None or before is not None:
                    flush()
                    in_list = False
                    continue
                # Not a valid fence: fall through and treat it as prose.
            if fence is not None:
                flush()
                in_list = False
                continue
            # A block quote is its own CommonMark block, so crossing into or
            # out of one ends the paragraph. Without this an intro line and
            # the quote below it join into one sentence that neither author
            # wrote, and a 25-word quoted sentence reports as 26.
            # Peeled the same way the fence path peels, and for the same
            # reason: "- > text" carries two markers, and stripping only the
            # one that comes first leaves the other as a word of the sentence.
            peeled, _, depth, _ = strip_containers(body, stack, container)
            # Lazy continuation: a quoted paragraph may carry on across a line
            # that has no marker of its own, so long as that line is ordinary
            # paragraph text. Flushing there splits one quoted sentence into
            # two short ones and a long sentence slips past CH007.
            # One predicate, shared with `comment_lines`. It was written
            # here and only here, and the span pass then reported a boundary
            # on a line this loop knew was no boundary at all.
            lazy = lazy_continuation(
                body, peeled, container, depth, quoted, bool(run), block, index, piece.nest
            )
            if depth != quoted and not lazy:
                flush()
                in_list = False
                quoted = depth
            # Structure is read past the container markers, and so is the
            # prose: a container marker is not a word of the sentence.
            body = strip_quote(body, container)
            # A table is a header row with a DELIMITER row under it, and then
            # the rows that follow -- decided here rather than up front,
            # because the delimiter's indent is measured against whatever
            # container is open at that point.
            # Indented code is a block and an example, not a sentence.
            if indented:
                flush()
                in_list = False
                continue
            if piece.marker in DOC_MARKERS and html_block(peeled, container):
                # The opener's own line is a block, and everything to its
                # closer is raw HTML. `comment_lines` opens the block at the
                # same point and by the same test, on the same full peel.
                html = html_kind(peeled)
                if html != "tag" and html_closes(peeled, html):
                    html = None
                flush()
                in_list = False
                continue
            # A Setext underline makes the run above it a heading's TITLE,
            # not a sentence, so it is discarded rather than flushed. An ATX
            # heading never reaches the run at all, and a 26-word title
            # reported CH007 in one form and not the other.
            #
            # DOC COMMENTS ONLY, for the reason round forty-eight limited
            # indented code: Rustdoc renders no `//` comment, and this tree
            # closes a plain banner with a rule of hyphens. Sixty-six real
            # sentences sat above one, and discarding them as heading titles
            # stopped measuring prose that no renderer ever sees as a title.
            if (
                run
                and piece.marker in DOC_MARKERS
                and setext_underline(peeled, container)
            ):
                run, run_lines = [], []
                in_list = False
                continue
            # Classified on the PEELED content, like the HTML test beside it.
            # Rustdoc renders "- # text" as a heading inside the item, and
            # reading the unpeeled line here let the list branch add the
            # heading to the prose run as though it were a sentence.
            if (
                in_table
                or heading(peeled, container)
                or SEPARATOR_RE.match(peeled)
                or not body.strip()
            ):
                flush()
                in_list = False
                continue
            # LIST_MARKER_RE, not a second list pattern of its own. Two
            # patterns for one concept drift: the previous one here missed the
            # "1)" ordered form, so two short items merged into one sentence
            # long enough to report a CH007 neither author wrote.
            marker = LIST_MARKER_RE.match(body)
            if marker and closes_an_open_paren(marker, run):
                marker = None
            if marker and (not run or in_list or interrupts_paragraph(marker)):
                flush()
                # `peeled` rather than the text after this one marker: the
                # item may carry a quote, or a second list, on the same line.
                # The peel applies CommonMark's indent limit and this match
                # does not, so a marker indented past a container it has not
                # got is a list HERE and indented code THERE. Falling back
                # keeps such a line reading as it always has, rather than
                # keeping its bullet as a word of the sentence.
                content = (
                    peeled if len(peeled) < len(body) else body[marker.end():]
                ).strip()
                # A checkbox is not two words. Rustdoc renders "- [ ] text"
                # with an <input>, so keeping the brackets adds two words to
                # every task item and reports a 24-word sentence as 26 --
                # a CH007 regression on a comment that complies.
                # The marker binds to the list item it opens. A quote after
                # the list marker means the peel went through it, and "[ ]"
                # in quoted prose is two literal words.
                if not ONE_QUOTE_RE.match(body[marker.end():]):
                    task = TASK_MARKER_RE.match(content)
                    if task:
                        content = content[task.end():].strip()
                run = [content]
                run_lines = [piece.line]
                in_list = True
                continue
            run.append(body.strip())
            run_lines.append(piece.line)
        flush()
    return units


def split_sentences(unit: str, boundaries: str | None = None):
    """(offset, sentence) pairs. `re.split` drops the offsets, and the offset
    is what maps a sentence back to the line it was written on.

    `boundaries` is where the full stops are LOOKED FOR, when that is not the
    text itself. A period inside a code span is not a sentence end -- Rustdoc
    renders "`foo. not sure why`" as one span -- and splitting there left a
    fragment that CH003 read as deliberation. The sentence returned is always
    sliced from `unit`, so the text reported and the words counted are the
    ones the author wrote.
    """
    source = unit if boundaries is None else boundaries
    start = 0
    for match in SENTENCE_SPLIT_RE.finditer(source):
        yield start, unit[start:match.start()]
        start = match.end()
    yield start, unit[start:]


def line_of(spans: list[tuple[int, int]], offset: int) -> int:
    """The source line of the fragment covering `offset` in a joined unit."""
    line = spans[0][1]
    for position, candidate in spans:
        if position > offset:
            break
        line = candidate
    return line


def check_prose_rules(path: str, pieces: list[Piece]) -> list[Finding]:
    """CH003 and CH007 -- both judge a whole sentence, not a wrapped line.

    Line-level matching is wrong for these. Comment prose wraps mid-sentence,
    so a continuation line routinely *begins* with a word that is only a
    defect sentence-initially -- "... proves the append\\n// actually landed"
    is ordinary prose, not deliberation.
    """
    findings = []
    for _, unit, spans, doc in prose_units(pieces):
        boundaries = mask_code_spans(unit)
        if doc:
            boundaries = mask_inline_code(boundaries)
        for offset, sentence in split_sentences(unit, boundaries):
            sentence = sentence.strip()
            if not sentence:
                continue
            lineno = line_of(spans, offset)
            # Blanked for CH003, as CH002 blanks them, and for the same
            # reason: both are absolute. "Parse the `let's` token" documents
            # a literal, and failing the build on it stops the literal being
            # documented. CH006 still reads the raw text -- it is ratcheted,
            # and KNOWN LIMITATIONS records that choice.
            spanless = blank_code_spans(sentence)
            if doc:
                spanless = blank_inline_code(spanless)
            if NARRATIVE_RE.search(spanless):
                findings.append(Finding("CH003", path, lineno, sentence[:100]))
            words = sentence.split()
            if len(words) > MAX_SENTENCE_WORDS:
                findings.append(
                    Finding(
                        "CH007", path, lineno, f"{len(words)} words: {sentence[:90]}", sentence
                    )
                )
    return findings


def scan(paths: list[str] | None) -> list[Finding]:
    findings: list[Finding] = []
    for rel in rust_sources(paths):
        full = os.path.join(REPO_ROOT, rel)
        try:
            with open(full, encoding="utf-8", errors="replace") as handle:
                source = handle.read()
        except OSError as exc:
            print(f"comment-hygiene: cannot read {rel}: {exc}", file=sys.stderr)
            continue
        findings.extend(findings_for_source(rel, source))
    findings.sort(key=lambda f: (f.rule, f.path, f.line))
    return findings


def tally(findings: list[Finding]) -> dict:
    """Per-rule, per-file fingerprint lists.

    Fingerprints, not counts. A count only answers "how many", so a change
    that removes one legacy violation and adds a different one in the same
    file nets to zero and passes, despite introducing exactly the defect the
    rule exists to stop. Identities answer "which", so the new one is a
    regression even though the total never moved.
    """
    grouped: dict = defaultdict(lambda: defaultdict(list))
    for f in findings:
        grouped[f.rule][f.path].append(f.fingerprint)
    return {
        rule: {path: sorted(fps) for path, fps in sorted(files.items())}
        for rule, files in sorted(grouped.items())
    }


def findings_for_source(path: str, source: str) -> list[Finding]:
    pieces = extract_comments(source)
    return (
        check_line_rules(path, pieces)
        + check_block_edges(path, pieces)
        + check_prose_rules(path, pieces)
    )


def baseline_from_merge_base(
    merge_base: str, scope: set[str], renames: dict[str, str]
) -> dict:
    """Tier B findings as they stand at the merge base, for the changed files.

    Read out of git rather than a checked-in baseline file, which removes the
    whole class of problems a stored baseline has. It cannot go stale when the
    base branch moves. It needs no regeneration ritual, and so cannot be
    regenerated to launder a new violation. It is rename-stable, because a
    moved file is compared against its own previous path. And it keeps half a
    megabyte of generated fingerprints out of the tree.
    """
    grouped: dict = defaultdict(lambda: defaultdict(list))
    for path in sorted(p for p in scope if p.endswith(".rs")):
        was_path = renames.get(path, path)
        if not was_path.endswith(".rs"):
            # Renamed INTO the Rust corpus. It has no audited history, so it
            # gets no allowance and every finding in it belongs to this change.
            continue
        try:
            done = subprocess.run(
                ("git", "show", f"{merge_base}:{was_path}"),
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, ValueError):
            continue
        if done.returncode != 0:
            # Absent at the base: a file this change adds. Nothing is allowed,
            # so every Tier B finding in it is this change's own.
            continue
        for finding in findings_for_source(path, done.stdout):
            if finding.rule in TIER_B:
                grouped[finding.rule][path].append(finding.fingerprint)
    return {
        rule: {path: sorted(fps) for path, fps in sorted(files.items())}
        for rule, files in sorted(grouped.items())
    }


def diff_context(base_ref: str) -> tuple[str, set[str], dict[str, str]] | None:
    """What this branch changed since its merge base with `base_ref`.

    Returns (merge-base sha, changed paths, {new path: old path} for renames).
    None when the answer cannot be trusted -- no git, an unknown ref, or a
    shallow clone whose history does not reach the merge base. Callers treat
    None as "cannot scope", never as "nothing changed".

    The sha matters: comparing against the branch TIP would blame this change
    for comments the base branch added after the fork point. Renames are
    tracked because a moved file is compared against its own previous path;
    without that, every legacy finding in it looks new and an ordinary module
    rename fails CI over debt it did not introduce.
    """
    def git(*args: str) -> str | None:
        try:
            done = subprocess.run(
                ("git", *args),
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, ValueError):
            return None
        return done.stdout.strip() if done.returncode == 0 else None

    merge_base = git("merge-base", base_ref, "HEAD")
    if not merge_base:
        return None
    listing = git("diff", "--name-status", "--find-renames", merge_base, "HEAD")
    if listing is None:
        return None

    changed: set[str] = set()
    renames: dict[str, str] = {}
    for row in listing.split("\n"):
        if not row.strip():
            continue
        fields = row.split("\t")
        if fields[0].startswith("R") and len(fields) >= 3:
            changed.add(fields[2])
            renames[fields[2]] = fields[1]
        elif len(fields) >= 2:
            changed.add(fields[1])
    return merge_base, changed, renames


def index_findings(findings: list[Finding]) -> dict:
    """(rule, path) -> {fingerprint: [Finding]}, so a regression can be located.

    A count tells a contributor that something moved; it does not tell them
    what to fix. In a file carrying 30 legacy findings, "30 total vs 29" means
    bisecting by hand. Keeping the Finding objects lets the failure name the
    line.
    """
    index: dict = defaultdict(lambda: defaultdict(list))
    for f in findings:
        index[(f.rule, f.path)][f.fingerprint].append(f)
    return index


def compare_tier_b(
    current: dict,
    baseline: dict,
    scope: set[str] | None = None,
    renames: dict[str, str] | None = None,
    index: dict | None = None,
) -> list[str]:
    """Regressions only: a count that rose, or a file newly in violation.

    `scope` limits the gate to the files a change actually touches. That is
    not a softening, it is what makes the ratchet usable: a whole-corpus
    count is a shared mutable number, so one merge that adds a long comment
    anywhere turns every open PR red for a file its author never opened. The
    predictable response is to regenerate the baseline, which defeats the
    ratchet entirely. Scoping to changed files makes each PR answerable only
    for its own work, and leaves the baseline a stable record of legacy debt
    rather than a contended counter.
    """
    renames = renames or {}
    regressions = []
    for rule in TIER_B:
        now = current.get(rule, {})
        was = baseline.get(rule, {})
        for path in sorted(now):
            if scope is not None and path not in scope:
                continue
            allowed = Counter(was.get(path) or was.get(renames.get(path, path), []))
            added = Counter(now[path]) - allowed
            total = sum(added.values())
            if not total:
                continue
            regressions.append(
                f"{rule} {path}: {total} new finding(s), "
                f"{len(now[path])} total vs {sum(allowed.values())} at the merge base "
                f"[{RULE_TITLES[rule]}]"
            )
            # Name the actual lines. Without this the contributor is told a
            # count moved and left to find which comment did it.
            located = (index or {}).get((rule, path), {})
            for fingerprint, count in sorted(added.items()):
                sites = located.get(fingerprint, [])
                # When the merge base already carried this exact text, the
                # occurrences are indistinguishable -- the fingerprint IS the
                # text. Naming the first one names the legacy line and sends
                # the contributor to edit a comment they never wrote, so name
                # every candidate and say how many of them are new.
                if len(sites) > count:
                    regressions.append(
                        f"    {count} of these {len(sites)} identical comments "
                        f"{'is' if count == 1 else 'are'} new:"
                    )
                for finding in sites:
                    regressions.append(f"    {path}:{finding.line}: {finding.text[:100]}")
    return regressions


def tier_b_gate(
    current: dict,
    baseline: dict,
    scope: set[str] | None,
    renames: dict[str, str] | None,
    findings: list[Finding],
    tier_a_only: bool,
) -> list[str]:
    """The Tier B regressions, or none when only the absolute gates apply.

    `--tier-a-only` promises to check ONLY the absolute gates. The text
    report has kept that promise since it was written, by returning before
    it reaches Tier B. The JSON path ran the comparison anyway and put a
    Tier B regression into its exit status, so the same two flags gave two
    answers, and the machine-readable one was the wrong answer.
    """
    if tier_a_only:
        return []
    return compare_tier_b(current, baseline, scope, renames, index_findings(findings))


def report(
    findings: list[Finding],
    baseline: dict,
    tier_a_only: bool,
    scope: set[str] | None,
    scope_note: str,
    renames: dict[str, str] | None = None,
) -> int:
    by_rule: dict = defaultdict(list)
    for f in findings:
        by_rule[f.rule].append(f)

    failed = False

    print("Tier A -- absolute gates")
    for rule in TIER_A:
        hits = by_rule.get(rule, [])
        status = "OK  " if not hits else "FAIL"
        print(f"  [{status}] {rule} {RULE_TITLES[rule]}: {len(hits)}")
        if hits:
            failed = True
            for f in hits[:20]:
                print(f"         {f.path}:{f.line}: {f.text[:100]}")
            if len(hits) > 20:
                print(f"         ... and {len(hits) - 20} more")
            print(f"         fix: {RULE_HINTS[rule]}")

    if tier_a_only:
        return 1 if failed else 0

    current = tally(findings)
    print(f"\nTier B -- ratcheted against the merge base ({scope_note})")
    for rule in TIER_B:
        corpus = sum(len(v) for v in current.get(rule, {}).values())
        allowed = sum(len(v) for v in baseline.get(rule, {}).values())
        in_scope = sum(
            len(v)
            for path, v in current.get(rule, {}).items()
            if scope is None or path in scope
        )
        print(
            f"  {rule} {RULE_TITLES[rule]}: {corpus} in the corpus; "
            f"{in_scope} in changed files ({allowed} at the merge base)"
        )

    regressions = compare_tier_b(current, baseline, scope, renames, index_findings(findings))
    if regressions:
        failed = True
        # Indented entries are located lines under a summary, not extra
        # regressions.
        count = sum(1 for line in regressions if not line.startswith(" "))
        print(f"\n{count} Tier B regression(s) -- these files gained violations:")
        for line in regressions[:40]:
            print(f"    {line}")
        if len(regressions) > 40:
            print(f"    ... and {len(regressions) - 40} more line(s)")
        print(
            "\n  New and edited comments must satisfy the rule. Fix the flagged\n"
            "  lines rather than regenerating the baseline -- the baseline exists\n"
            "  to freeze the legacy corpus, not to absorb new debt."
        )

    if not failed:
        print("\nOK: no Tier A findings and no Tier B regressions.")
    return 1 if failed else 0


SELF_TESTS = [
    ("let n = 1; // TODO: fix\n", ["TODO: fix"], "trailing line comment"),
    ("/* TODO: remove */\n", ["TODO: remove"], "block comment"),
    ('let s = r#"\n// TODO: not a comment\n"#;\n', [], "raw string contents"),
    ('let s = "// also not a comment";\n', [], "string contents"),
    ('// real\nlet s = "not // this";\n', ["real"], "comment then string"),
    (
        "/* outer /* nested */ still outer */ x();\n",
        ["outer", "nested", "still outer"],
        "nested block yields the inner body as its own piece",
    ),
    ("let c = '\\''; // after char lit\n", ["after char lit"], "escaped char literal"),
    ("fn f<'a>(x: &'a str) {} // after lifetime\n", ["after lifetime"], "lifetime tick"),
    ('let s = "esc \\" // no"; // yes\n', ["yes"], "escaped quote"),
    ("/// doc\n//! inner\n", ["doc", "inner"], "doc markers"),
    ('let s = br#"// bytes"#; // trailing\n', ["trailing"], "byte raw string"),
    ('let s = r##"a "# b"##; // hashes\n', ["hashes"], "multi-hash raw string"),
    # A backslash-newline continuation is a real newline. Miss it and every
    # line number after it drifts, which is how this was originally caught.
    ('let e = "a \\\n b"; // after continuation\n', ["after continuation"], "line continuation"),
]


# Rule-level fixtures: (source, expected {(rule, line)}, name). These pin
# behaviour the lexer alone cannot express -- how comment runs bound fence
# state and prose units.
RULE_TESTS = [
    (
        "/// Example:\n/// ```rust\n/// let x = 1;\npub fn a() {}\n\n"
        "// TODO: issue required\n// let stale = compute();\npub fn b() {}\n",
        {("CH002", 6), ("CH001", 7)},
        "an unclosed fence does not leak past its own block",
    ),
    (
        "/// Example:\n/// ```rust\n/// let x = compute();\n/// ```\npub fn a() {}\n",
        set(),
        "fenced example code stays exempt",
    ),
    (
        "//// let stale = compute();\n",
        {("CH001", 1)},
        "//// is an ordinary comment, not a doc comment",
    ),
    (
        "// fn resolve_call(the caller name appears in diagnostics\n",
        set(),
        "prose after an open paren is not a signature",
    ),
    (
        "// fn foo(\n//     a: u8,\n// ) {}\n",
        {("CH001", 1)},
        "a wrapped commented-out signature is caught on its opening line",
    ),
    (
        "/// ```rust\n// TODO: issue required\n",
        {("CH002", 2)},
        "a change of comment marker ends the run",
    ),
    (
        "/* outer /* let stale = compute(); */ */\n",
        {("CH001", 1)},
        "a nested block comment body is inspected",
    ),
    (
        "/// ```rust\n/// ~~~\n/// TODO: fixture placeholder\n/// ```\n",
        set(),
        "a ~~~ line inside a ``` fence is content, not a closer",
    ),
    (
        "/// ````rust\n/// ```\n/// TODO: fixture placeholder\n/// ````\n",
        set(),
        "a short closer does not close a longer fence",
    ),
    (
        "/// - Example:\n///\n///     ```rust\n///     TODO: fixture placeholder\n///     ```\n",
        set(),
        "a fence indented inside a list item is still a fence",
    ),
    (
        "///     ```rust\n/// TODO: issue required\n",
        {("CH002", 2)},
        "four spaces is an indented code line, not a fence opener",
    ),
    (
        "///    ```rust\n///    let x = compute();\n///    ```\n",
        set(),
        "three spaces still opens a fence",
    ),
    (
        "/// ```rust\n///     ```\n/// TODO: fixture placeholder\n/// ```\n",
        set(),
        "an over-indented ``` inside a fence is content, not a closer",
    ),
    (
        "/// - ```rust\n///   TODO: fixture placeholder\n///   ```\n",
        set(),
        "a fence opens on its own list-marker line",
    ),
    (
        "/// - ```rust\n///   let x = compute();\n///   ```\n/// TODO: issue required\n",
        {("CH002", 4)},
        "a fence opened on a list-marker line still closes",
    ),
    (
        "/// > ~~~rust\n/// > TODO: fixture placeholder\n/// > ~~~\n",
        set(),
        "a fence inside a block quote is a fence",
    ),
    (
        "/// > > ```rust\n/// > > TODO: fixture placeholder\n/// > > ```\n",
        set(),
        "so is one inside a nested block quote",
    ),
    (
        "/// > - Example:\n/// >\n/// >     ```rust\n/// >     TODO: fixture placeholder\n/// >     ```\n",
        set(),
        "a quoted list item still opens a container for its fence",
    ),
    (
        "/// > ```rust\n/// > let x = compute();\n/// > ```\n/// > TODO: issue required\n",
        {("CH002", 4)},
        "a quoted fence closes, and quoted prose after it is scanned",
    ),
    (
        "/// > TODO: fix this\n",
        {("CH002", 1)},
        "a quote marker does not exempt the line it carries",
    ),
    (
        "/// 1) alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu\n"
        "/// 2) xi omicron pi rho sigma tau upsilon phi chi psi omega alpha beta\n",
        set(),
        "parenthesized ordered-list items are separate sentences",
    ),
    (
        "// --------------------\n"
        "// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        set(),
        "a thematic break is not a word of the sentence below it",
    ),
    (
        "// It pins branch B (index\n"
        "// 1) as word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20.\n",
        {("CH007", 1)},
        "a wrapped \"1)\" closing a paren does not split the sentence",
    ),
    (
        "/// -     ```rust\n/// TODO: issue required\n",
        {("CH002", 2)},
        "five spaces after a list marker is indented code, not a fence",
    ),
    (
        "/// - \t\t~~~rust\n/// TODO: issue required\n",
        {("CH002", 2)},
        "list padding is counted in columns, so tabs cannot smuggle a fence",
    ),
    (
        "///     - ~~~rust\n/// TODO: issue required\n",
        {("CH002", 2)},
        "four spaces before a top-level marker is indented code, not a list",
    ),
    (
        "///    - ~~~rust\n///      TODO: fixture placeholder\n///      ~~~\n",
        set(),
        "three spaces before a marker is still a list",
    ),
    (
        "/// - ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        {("CH002", 3)},
        "an unclosed fence ends when its list item does",
    ),
    (
        "/// > ~~~rust\n/// > let x = 1;\n/// TODO: issue required\n",
        {("CH002", 3)},
        "an unclosed fence ends when its block quote does",
    ),
    (
        "/// - ~~~rust\n///\n///   TODO: fixture placeholder\n///   ~~~\n",
        set(),
        "a blank line does not end the list item a fence sits in",
    ),
    (
        "/// -   ~~~rust\n/// \tTODO: fixture placeholder\n/// \t~~~\n",
        set(),
        "a tab indenting a fence body is measured in columns, not characters",
    ),
    (
        "/// - outer\n///   - inner\n///   ~~~rust\n///   let x = 1;\n"
        "/// TODO: issue required\n",
        {("CH002", 5)},
        "leaving a nested list item lands in its parent, not at top level",
    ),
    (
        "/// - outer\n///   - inner\n///     ~~~rust\n///     TODO: fixture\n"
        "///     ~~~\n",
        set(),
        "a fence inside a nested list item is still a fence",
    ),
    (
        "/// > - quoted item\n///\n///     ~~~rust\n/// TODO: issue required\n",
        {("CH002", 4)},
        "a list container does not outlive the block quote it opened in",
    ),
    (
        "/// - outer paragraph\n///   2. still the same paragraph\n"
        "///      ~~~rust\n///   TODO: fixture placeholder\n///      ~~~\n",
        set(),
        "a mid-paragraph \"2.\" does not open a nested container",
    ),
    (
        "/// 1. first item\n/// 2. second item\n///    ~~~rust\n"
        "///    TODO: fixture placeholder\n///    ~~~\n",
        set(),
        "but \"2.\" after \"1.\" still opens its own item",
    ),
    (
        "/// ~~~rust\n/// let x = 1;\n/// ~~~\n/// 22. item\n"
        "///      ~~~rust\n///      TODO: fixture placeholder\n///      ~~~\n",
        set(),
        "a fence ends the paragraph before it, so \"22.\" may open an item",
    ),
    (
        "/// - outer\n///   > quoted\n///   ~~~rust\n///   let x = 1;\n"
        "/// TODO: issue required\n",
        {("CH002", 5)},
        "a quote nested in a list item does not close the item",
    ),
    (
        "/// ```rust\n/// - ```\n/// TODO: fixture placeholder\n/// ```\n",
        set(),
        "a list marker inside a fence is sample text, not a container",
    ),
    (
        "/// -\n///   ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        {("CH002", 4)},
        "a marker alone on its line still opens a list item",
    ),
    (
        "/// -   outer\n///     > ```rust\n///     > TODO: fixture placeholder\n"
        "///     > ```\n",
        set(),
        "a quote marker is indented relative to its list container",
    ),
    (
        "/// ```rust\n/// > ```\n/// TODO: fixture placeholder\n/// ```\n",
        set(),
        "a quoted delimiter deeper than the fence is sample text",
    ),
    (
        "/// -   outer\n///     > ```rust\n///     > let x = 1;\n"
        "///     > ```\n///     TODO: issue required\n",
        {("CH002", 5)},
        "a quoted fence inside a list item still sees its own closer",
    ),
    (
        "/// - 1. ```rust\n///      TODO: fixture placeholder\n///      ```\n",
        set(),
        "two list markers on one line both open containers for the fence",
    ),
    (
        "/// - > ~~~rust\n///   > let x = 1;\n///   > ~~~\n"
        "///   TODO: issue required\n",
        {("CH002", 4)},
        "a fence opened behind a list and a quote knows its own depth",
    ),
    (
        "/// # Heading\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "a heading is not a paragraph, so \"22.\" may open an item after it",
    ),
    (
        "/// -----\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "nor is a thematic break",
    ),
    (
        "/// > > ~~~rust\n/// > > let x = 1;\n/// > > ~~~\n"
        "/// > TODO: issue required\n",
        {("CH002", 4)},
        "two quote markers in one match count as two levels",
    ),
    (
        "/// - 1. ~~~rust\n///      let x = 1;\n///      ~~~\n"
        "///   TODO: issue required\n",
        {("CH002", 4)},
        "a fence inside a nested item is scoped to the inner one",
    ),
    (
        "/**\n```rust\n/* TODO: fixture placeholder */\n```\n*/\npub fn a() {}\n",
        set(),
        "a nested comment inside a fenced example is part of the example",
    ),
    (
        "/**\n * Intro\n * /* - inner */\n *     ~~~rust\n"
        " *   TODO: issue required\n */\n",
        {("CH002", 5)},
        "a list marker inside a nested comment does not escape it",
    ),
    (
        "/// - let stale = compute();\n",
        {("CH001", 1)},
        "a bullet does not exempt commented-out code",
    ),
    (
        "/// > let stale = compute();\n",
        {("CH001", 1)},
        "and neither does a quote",
    ),
    (
        "/// - a bullet of ordinary prose\n/// - the worker parks;\n",
        set(),
        "but ordinary bulleted prose stays clean",
    ),
    (
        "/// - TODO fix this\n",
        {("CH002", 1)},
        "nor does a bullet exempt an unreferenced TODO",
    ),
    (
        "/// > TODO fix this\n",
        {("CH002", 1)},
        "nor a quote",
    ),
    (
        "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20\n/// | w1 w2 w3 w4 w5 w6 w7 w8 w9 w10.\n",
        {("CH007", 1)},
        "one pipe is prose, so the sentence carries on across it",
    ),
    (
        "/// Heading\n/// --\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "a Setext underline may be hyphens",
    ),
    (
        "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21\n"
        "/// | while this second pipe-delimited fragment adds another ten words |\n",
        {("CH007", 1)},
        "two pipes are not a table either, without a delimiter row",
    ),
    (
        "/// -   Heading\n///     --\n///     22. item\n"
        "///         ~~~rust\n///         TODO: fixture placeholder\n"
        "///         ~~~\n",
        set(),
        "a Setext underline is measured against its list container",
    ),
    (
        "/// Intro\n/// > Quote\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "a block quote ends the paragraph for containers, not only for prose",
    ),
    (
        "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 | tail\n///     --- | ---\n",
        {("CH007", 1)},
        "an over-indented delimiter row does not make a table",
    ),
    (
        "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 | tail\n/// | | --- |\n",
        {("CH007", 1)},
        "nor does one with an empty cell",
    ),
    (
        "/// | Header |\n/// | --- |\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "but a confirmed table does end the paragraph above the list",
    ),
    (
        "/**\n * Intro /* > inner */\n * 22. item\n *     ~~~rust\n"
        " *     TODO: issue required\n */\n",
        {("CH002", 5)},
        "a quote inside a nested comment does not escape it",
    ),
    (
        "/** word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 /* inserted note */ w1 w2 w3 w4 w5 w6 w7 w8 w9 w10. */\npub fn a() {}\n",
        {("CH007", 1)},
        "a sentence crossing an inline nested comment is one sentence",
    ),
    (
        "/// Intro\n/// -\n///     ~~~rust\n///   TODO: issue required\n",
        {("CH002", 4)},
        "a marker with no content cannot interrupt a paragraph",
    ),
    (
        "/// 1234567890. ~~~rust\n///             TODO: issue required\n",
        {("CH002", 2)},
        "ten digits is too many for an ordered marker",
    ),
    (
        "/// 123456789. ~~~rust\n///            TODO: fixture placeholder\n"
        "///            ~~~\n",
        set(),
        "nine is not",
    ),
    (
        "/// Intro paragraph\n/// 01. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "a marker's value decides interruption, so \"01.\" starts at one",
    ),
    (
        "/// * * *\n///     ~~~rust\n///   TODO: issue required\n",
        {("CH002", 3)},
        "a spaced thematic break is a break, not a list item",
    ),
    (
        "/// #\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "a marker-only heading is still a heading",
    ),
    (
        "/// ===\n/// 22. item\n///     ~~~rust\n///     TODO: issue required\n",
        {("CH002", 4)},
        "a decorative rule is paragraph text, not a thematic break",
    ),
    (
        "/// Heading\n/// ===\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: fixture placeholder\n///     ~~~\n",
        set(),
        "but the same rule under a paragraph is a Setext heading",
    ),
    (
        "/// >~~~rust\n/// >TODO: fixture placeholder\n/// >~~~\n",
        set(),
        "a quote marker needs no space after it",
    ),
    (
        "/// Only defer when it too is\n/// >= end_at and the slot is free.\n",
        set(),
        "but \">=\" is still an operator, not a quote",
    ),
    (
        "/// Intro\n///     * * *\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: issue required\n",
        {("CH002", 5)},
        "an over-indented thematic break is indented content",
    ),
    (
        "/// Intro\n/// | not a table\n/// 22. item\n///     ~~~rust\n"
        "///     TODO: issue required\n",
        {("CH002", 5)},
        "a pipe-prefixed line is prose, not a block that ends a paragraph",
    ),
    (
        "/// - > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        set(),
        "a quote behind a list marker is not a word of the sentence",
    ),
    (
        "/// - 1. word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        set(),
        "nor is a second list marker",
    ),
    (
        "/// Intro\n/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        set(),
        "entering a block quote ends the paragraph before it",
    ),
    (
        "/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20\n/// w1 w2 w3 w4 w5 w6 w7 w8 w9 w10.\n",
        {("CH007", 1)},
        "but a quoted paragraph continues lazily onto an unmarked line",
    ),
    (
        "/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25\n/// - and a list item here.\n",
        set(),
        "leaving a quote onto a block start does flush",
    ),
    (
        "/// -    ```rust\n///      TODO: fixture placeholder\n///      ```\n",
        set(),
        "four spaces after a list marker is still valid padding",
    ),
    (
        "// The semaphore lets just one claimant proceed.\n",
        set(),
        "\"lets\" as a verb is behaviour, not deliberation",
    ),
    (
        "// The row is held. Lets just skip it.\n",
        {("CH003", 1)},
        "\"Lets just\" opening a sentence is still an aside",
    ),
    (
        "// Short sentence.\n// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        {("CH007", 2)},
        "a sentence reports its own line, not the paragraph's first",
    ),
    # Orthogonal-axis fixtures. Two rules were checked for the RIGHT WORDS
    # while the apostrophe character and the marker's case were left assumed,
    # which is how `can\u2019t` and `todo:` both slipped a gate that had just been
    # "enumerated". Each rule is now pinned on those axes, not only on content.
    (
        "// todo: fix this\n",
        {("CH002", 1)},
        "the TODO marker is case-insensitive",
    ),
    (
        "// fixme: fix this\n",
        {("CH002", 1)},
        "so is FIXME",
    ),
    (
        "// We\u2019ll group stats by queue name.\n",
        {("CH003", 1), ("CH006", 1)},
        "a narrative aside survives a typographic apostrophe",
    ),
    (
        "/// ```foo`bar TODO: remove this\n",
        {("CH002", 1)},
        "an invalid fence opener is scanned, not exempted",
    ),
    (
        "/* ```rust */\n/* TODO: issue required */\n",
        {("CH002", 2)},
        "two distinct block comments are two runs",
    ),
    (
        "let x = 1; /* ```rust\nTODO: fixture placeholder\n``` */\n",
        set(),
        "a multiline block comment starting after code is one run",
    ),
    (
        "/* outer /* ```rust */ TODO: issue required */\n",
        {("CH002", 1)},
        "a nested comment's fence does not leak into the enclosing one",
    ),
    (
        "/* outer /*! TODO */ */\n",
        {("CH002", 1)},
        "a nested inner-doc marker is stripped too",
    ),
    (
        "fn f() {\n"
        "    let a = 1; // the first of two short trailing notes that each stay well under the limit here\n"
        "    let b = 2; // the second of two short trailing notes that also stays under it comfortably\n"
        "}\n",
        set(),
        "adjacent trailing comments are not merged into one sentence",
    ),
    (
        "/// Paragraph.\n"
        "///     # indented\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        {("CH002", 5)},
        "a '#' four columns into a paragraph is content, not a heading",
    ),
    (
        "/// Paragraph.\n"
        "///   # heading\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        set(),
        "a '#' three columns in is still a heading",
    ),
    (
        "/// -   intro paragraph\n"
        "///     | h | i |\n"
        "///     | --- | --- |\n"
        "///     22. item\n"
        "///         ```\n"
        "///         TODO: issue required\n",
        set(),
        "a table nested in a list item is read in its own container",
    ),
    (
        "/* intro paragraph\n"
        "/* | h | i |\n"
        "| --- | --- | */\n"
        "| still prose\n"
        "22. item\n"
        "     ```\n"
        "     TODO: issue required\n"
        "*/\n",
        {("CH002", 7)},
        "a table inside a nested comment does not leak out of it",
    ),
    (
        "/// Intro | header |\n"
        "/// | - |\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        {("CH002", 5)},
        "a one-cell delimiter under a two-cell header is not a table",
    ),
    (
        "/// | a |\n"
        "/// | - |\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        set(),
        "one hyphen in a cell is a valid delimiter row",
    ),
    (
        "/// - | a | b |\n"
        "///   | - | - |\n"
        "///   22. item\n"
        "///       ```\n"
        "///       TODO: issue required\n",
        set(),
        "a list marker on the header row is not a cell",
    ),
    (
        "/// | a \\| b | c |\n"
        "/// | - | - |\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        set(),
        "an escaped pipe is content, not a cell boundary",
    ),
    (
        "/// - [ ] " + " ".join(f"word{n}" for n in range(1, 26)) + ".\n",
        set(),
        "a task-list checkbox is not two words",
    ),
    (
        "/// - [y] " + " ".join(f"word{n}" for n in range(1, 26)) + ".\n",
        {("CH007", 1)},
        "'[y]' is not a task marker, so it stays two words",
    ),
    (
        "/** Intro paragraph\n"
        " * /* inserted note */\n"
        " * 22. item\n"
        " *     ```\n"
        " *     TODO: issue required\n"
        " */\n"
        "pub struct A;\n",
        {("CH002", 5)},
        "a gutter before a nested opener is not a blank line",
    ),
    (
        "/** Intro paragraph\n"
        " *\n"
        " * 22. item\n"
        " *     ```\n"
        " *     TODO: issue required\n"
        " */\n"
        "pub struct A;\n",
        set(),
        "a genuinely blank comment line still ends the paragraph",
    ),
    (
        "/** Intro paragraph\n"
        " * | h |\n"
        " * /* | --- | */\n"
        " * 22. item\n"
        " *     ```\n"
        " *     TODO: issue required\n"
        " */\n"
        "pub struct A;\n",
        {("CH002", 6)},
        "a delimiter row inside a nested comment is not the outer header's",
    ),
    (
        "/// > " + " ".join(f"word{n}" for n in range(1, 21)) + "\n"
        "/// continued | " + " ".join(f"word{n}" for n in range(21, 27)) + ".\n",
        {("CH007", 1)},
        "a pipe does not end a lazy quote continuation",
    ),
    (
        "/// > quoted sentence here\n"
        "/// | h |\n"
        "/// | --- |\n"
        "/// 22. item\n"
        "///     ```\n"
        "///     TODO: issue required\n",
        set(),
        "a confirmed table still ends a lazy quote continuation",
    ),
    (
        "/** Outer /* ```rust\n"
        " * TODO: issue required\n"
        " * ``` */ tail. */\n"
        "pub struct A;\n",
        {("CH002", 2)},
        "backticks after a nested opener are mid-line, so no fence",
    ),
    (
        "/** Outer /* text\n"
        " * ```rust\n"
        " * TODO: issue required\n"
        " * ``` */ tail. */\n"
        "pub struct A;\n",
        set(),
        "a nested fence that starts its own line still opens",
    ),
    (
        "/** ```rust\n"
        " * sample();\n"
        " * ``` /* note */\n"
        " * TODO: issue required\n"
        " * ``` */\n"
        "pub struct A;\n",
        set(),
        "a closer followed by a nested comment does not close",
    ),
    (
        "//! Intro paragraph.\n"
        "//! <pre>raw</pre>\n"
        "//! 22. item\n"
        "//!     ```\n"
        "//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n"
        "//!     ```\n",
        set(),
        "an HTML block ends the paragraph above it",
    ),
    (
        "//! Intro paragraph.\n"
        "//! <T> is the type parameter.\n"
        "//! 22. item\n"
        "//!     ```\n"
        "//!     TODO: issue required\n",
        {("CH002", 5)},
        "'<T>' is a type parameter, not an HTML block",
    ),
    (
        "//! | a | b |\n"
        "//! | - | - |\n"
        "//! 22. item\n"
        "//!     ```\n"
        "//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n"
        "//!     ```\n",
        set(),
        "the prose scanner ends the paragraph at a table too",
    ),
    (
        "// Intro e.g. " + " ".join(f"word{n}" for n in range(1, 25)) + " end.\n",
        {("CH007", 1)},
        "'e.g.' does not end a sentence",
    ),
    (
        "// Short one. Short two.\n",
        set(),
        "an ordinary period still ends one",
    ),
    (
        "//! | a | b |\n"
        "//! | - | - |\n"
        "//! ordinary row with no separator\n"
        "//! 22. item\n"
        "//!     ```\n"
        "//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n"
        "//!     ```\n",
        set(),
        "a table body row needs no pipe",
    ),
    (
        "//! <pre>\n"
        "//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n"
        "//! </pre>\n",
        set(),
        "an HTML block runs to its closer",
    ),
    (
        "//! Intro.\n"
        "//! <pre>raw</pre>\n"
        "//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        {("CH007", 3)},
        "an HTML block closed on its own line ends there",
    ),
    (
        "//! <div>\n"
        "//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n"
        "//!\n"
        "//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        {("CH007", 4)},
        "a tag-name HTML block ends at a blank line",
    ),
    (
        "/// > <pre>\n"
        "/// > TODO: fixture placeholder\n"
        "/// > </pre>\n",
        set(),
        "a quoted HTML block is peeled before it is recognized",
    ),
    (
        "/// <pre>\n"
        "/// </style>\n"
        "/// TODO: fixture placeholder\n"
        "/// </pre>\n",
        set(),
        "a verbatim block closes on its own tag and no other",
    ),
    (
        "/// <pre>TODO: fixture placeholder</pre>\n",
        set(),
        "a raw block complete on one line exempts that line",
    ),
    (
        "/// <pre>\n"
        "/// TODO: one\n"
        "/// </pre>\n"
        "/// TODO: two\n",
        {("CH002", 4)},
        "and the line after its closer is prose again",
    ),
    (
        "/** ```rust\n"
        " * sample();\n"
        " * ``` /**/\n"
        " * TODO: fixture placeholder\n"
        " * ``` */\n"
        "pub struct A;\n",
        set(),
        "an empty nested comment still follows a closing fence",
    ),
    (
        "/// - > intro\n"
        "///   22. item\n"
        "///       ```\n"
        "///       TODO: issue required\n",
        set(),
        "quote depth comes from the peel in both scanners",
    ),
    (
        "/// - <pre>\n"
        "///   TODO: issue required\n",
        set(),
        "a list marker is peeled before the HTML opener too",
    ),
    (
        "/// Intro.\n"
        "///\n"
        "///     let x = compute();\n"
        "///     TODO: fixture placeholder\n",
        set(),
        "an indented code block in a doc comment is an example",
    ),
    (
        "// Intro.\n"
        "//\n"
        "//     let x = compute();\n"
        "//     TODO: issue required\n",
        {("CH001", 3), ("CH002", 4)},
        "a plain // comment renders nowhere, so indentation is not code",
    ),
    (
        "/// wrapped line one\n"
        "///     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        {("CH007", 1)},
        "indented code cannot interrupt a paragraph",
    ),
    (
        "/// Intro.\n"
        "///\n"
        "///     let x = compute();\n"
        "///\n"
        "/// TODO: issue required\n",
        {("CH002", 5)},
        "and it ends where the indent does",
    ),
    (
        "/// -     let x = compute();\n"
        "///       TODO: fixture placeholder\n",
        set(),
        "a code block may begin on the list marker's own line",
    ),
    (
        "/// - item\n"
        "/// - TODO: issue required\n",
        {("CH002", 2)},
        "an ordinary bullet is not indented code",
    ),
    (
        "/*** not a doc comment\n"
        " *\n"
        " *     let x = compute();\n"
        " *     TODO: issue required\n"
        " */\n"
        "pub struct A;\n",
        {("CH001", 3), ("CH002", 4)},
        "'/***' is an ordinary comment, so its indent is not code",
    ),
    (
        "/** a real doc comment\n"
        " *\n"
        " *     let x = compute();\n"
        " *     TODO: fixture placeholder\n"
        " */\n"
        "pub struct B;\n",
        set(),
        "and '/**' is still a doc comment",
    ),
    (
        "// This first sentence ends with **word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 emphasis.**"
        " This second sentence has word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 here.\n",
        set(),
        "a sentence may end inside emphasis",
    ),
    (
        "// A `?` straight out of any of these returns early from the caller here.\n",
        set(),
        "a code span's punctuation is not a sentence end",
    ),
    (
        "// Used to answer \"is `charge_card` held?\" during an incident"
        " and word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18.\n",
        {("CH007", 1)},
        "nor is a question mark inside a quotation",
    ),
    (
        "// pub(in crate::foo) fn bar() {\n",
        {("CH001", 1)},
        "pub(in path) is a visibility",
    ),
    (
        "// Parse the `TODO:` prefix from input.\n",
        set(),
        "a marker inside a code span is documentation",
    ),
    (
        "// TODO: fix the parser\n",
        {("CH002", 1)},
        "and a bare one is still a commitment",
    ),
    (
        "/** ~~~rust /* note */ `info`\n"
        " * TODO: fixture placeholder\n"
        " * ~~~\n"
        " */\n"
        "pub struct C;\n",
        set(),
        "only a backtick fence forbids a backtick in its info string",
    ),
    (
        "/** ```rust /* note */ `info`\n"
        " * TODO: issue required\n"
        " */\n"
        "pub struct D;\n",
        {("CH002", 2)},
        "and a backtick fence still does",
    ),
    (
        "/// - # word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n",
        set(),
        "a heading on a list-marker line is a heading",
    ),
    (
        "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n/// ===\n",
        set(),
        "a Setext title is a heading, not a sentence",
    ),
    (
        "// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n// ---------------------------\n",
        {("CH007", 1)},
        "but a banner rule in a // comment underlines nothing",
    ),
    (
        "/// Parse the `let's` token from the input.\n",
        {("CH006", 1)},
        "a narrative phrase inside a code span is a literal",
    ),
    (
        "// Actually, let's just skip the retry here.\n",
        {("CH003", 1), ("CH006", 1)},
        "and outside one it is still deliberation",
    ),
    (
        "/// Explain the `literal\n"
        "/// TODO: marker` syntax.\n",
        set(),
        "a code span may wrap onto the next line",
    ),
    (
        "// A stray ` here\n"
        "// TODO: fix the parser\n",
        {("CH002", 2)},
        "an unmatched backtick opens no span",
    ),
    (
        "/// A stray ` here\n"
        "///\n"
        "/// TODO: fix the parser\n",
        {("CH002", 3)},
        "and a blank line ends a paragraph, so it ends a span",
    ),
    (
        "/// Explain `literal TODO: issue required`` suffix.\n"
        "pub struct A;\n",
        {("CH002", 1)},
        "a one-backtick opener does not close on a two-backtick run",
    ),
    (
        "/// Explain ``TODO: marker`` here.\n",
        set(),
        "and a two-backtick span still closes on its own run",
    ),
    (
        "/// Explain the `literal\n"
        "pub struct B;\n"
        "\n"
        "/// TODO: issue required` suffix.\n"
        "pub struct C;\n",
        {("CH002", 4)},
        "a span cannot reach out of its own comment",
    ),
    (
        "/// Parse the `foo. not sure why` token.\n",
        set(),
        "a period inside a code span is not a sentence end",
    ),
    (
        "// End here. `%` is the next sentence and it stays whole.\n",
        set(),
        "and a sentence that opens with a span keeps it",
    ),
    (
        "/// Explain the \\`literal\n"
        "/// TODO: issue required` syntax.\n",
        {("CH002", 2)},
        "an escaped backtick opens no span",
    ),
    (
        "/// The URL parser treats `\\` as a path separator. TODO: issue required\n",
        {("CH002", 1)},
        "but a backslash inside one does not stop it closing",
    ),
    (
        "/// Explain the \\\\`TODO: marker` syntax.\n",
        set(),
        "two backslashes escape each other, so the span opens",
    ),
    (
        "// <pre>\n"
        "// TODO: issue required\n"
        "// let stale = compute();\n"
        "// </pre>\n",
        {("CH001", 3), ("CH002", 2)},
        "'<pre>' in a // comment is text, and exempts nothing",
    ),
    (
        "// ```rust\n"
        "// TODO: issue required\n"
        "// ```\n",
        set(),
        "but a fence is an example in any comment",
    ),
    (
        "/// - Explain the `literal\n"
        "/// - TODO: issue required` suffix.\n",
        {("CH002", 2)},
        "a code span does not reach across two list items",
    ),
    (
        "/// - Explain the `literal\n"
        "///   TODO: marker` syntax.\n",
        set(),
        "but it does reach across one item's own lines",
    ),
    (
        "/// Explain the `literal\n"
        "/// > TODO: issue required` suffix.\n",
        {("CH002", 2)},
        "nor across the edge of a block quote",
    ),
    (
        "/// > Explain the `literal\n"
        "/// > TODO: marker` syntax.\n",
        set(),
        "though it does reach within one",
    ),
    (
        "/// Parse the <code>not sure why</code> token literally.\n",
        set(),
        "an inline <code> element is code in a doc comment",
    ),
    (
        "// Parse the <code>not sure why</code> token literally.\n",
        {("CH003", 1)},
        "and characters in a // comment",
    ),
    (
        "/// Parse the <code>foo. not sure why</code> token literally.\n",
        set(),
        "a period inside an inline <code> element ends no sentence",
    ),
    (
        "// Parse the <code>foo. not sure why</code> token literally.\n",
        {("CH003", 1)},
        "and in a // comment it still does",
    ),
    (
        "/// Explain the `literal\n"
        "/// | h |\n"
        "/// | - |\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 4)},
        "a confirmed table ends the paragraph above it, so a span stops",
    ),
    (
        "/// Explain the `literal\n"
        "/// Heading\n"
        "/// ===\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 4)},
        "and a Setext underline ends the heading it makes",
    ),
    (
        "/// | a | b |\n"
        "/// | - | - |\n"
        "/// | x `open | y |\n"
        "/// | z TODO: issue required` w |\n",
        {("CH002", 4)},
        "each table row is its own boundary as well",
    ),
    (
        "/// | a |\n"
        "/// | - |\n"
        "/// | x `open TODO: marker` y |\n",
        set(),
        "but within one cell a span still pairs",
    ),
    (
        "// Explain the `literal\n"
        "// | h |\n"
        "// | - |\n"
        "// TODO: issue required` suffix.\n",
        set(),
        "neither boundary applies in a // comment, which renders nothing",
    ),
    (
        "/// \\<code>TODO: add retry\\</code>\n",
        {("CH002", 1)},
        "an escaped <code> tag is literal text, not an element",
    ),
    (
        "/// \\\\<code>TODO: add retry</code>\n",
        set(),
        "two backslashes escape each other, so the element opens",
    ),
    (
        "/// <code>a\\</code> TODO: add retry\n",
        set(),
        "an escaped closer leaves the element open to the block end",
    ),
    (
        "/// <code>a\n"
        "/// TODO: add retry\n",
        set(),
        "and an unclosed element covers the line under it",
    ),
    (
        "/// <code>a\n"
        "///\n"
        "/// TODO: issue required\n",
        {("CH002", 3)},
        "though the paragraph's own end closes it",
    ),
    (
        "/// Demonstrates `\n"
        "/// let x = compute();\n"
        "/// ` inline.\n",
        set(),
        "a wrapped code span is an example, not commented-out code",
    ),
    (
        "/// Demonstrates it.\n"
        "/// let x = compute();\n",
        {("CH001", 2)},
        "but the same statement outside one still is",
    ),
    (
        "// See #123 for the parser. TODO: add retries\n",
        {("CH002", 1)},
        "a reference belongs to the marker it sits with, not to the line",
    ),
    (
        "// TODO(#123): parser; TODO: add retries\n",
        {("CH002", 1)},
        "so a tracked marker does not cover an untracked one after it",
    ),
    (
        "/// The `#123` syntax. TODO: add retries\n",
        {("CH002", 1)},
        "and a reference inside a code span is syntax, not a reference",
    ),
    (
        "// TODO: add retries (#123)\n",
        set(),
        "a marker's own reference may follow it",
    ),
    (
        "// TODO(#1): a; TODO(#2): b\n",
        set(),
        "and two markers each carrying one are both tracked",
    ),
    (
        "/// Contact <code@example.com>. TODO: add retry\n",
        {("CH002", 1)},
        "an e-mail autolink is not a <code> opener",
    ),
    (
        "/// A <code-block> element. TODO: add retry\n",
        {("CH002", 1)},
        "nor is a tag whose name merely starts with code",
    ),
    (
        "/// Spaced <code > here</code>. TODO: add retry\n",
        {("CH002", 1)},
        "a closed element still ends where its closer does",
    ),
    (
        "/// Empty <code/> here. TODO: add retry\n",
        set(),
        "but a self-closing tag opens the element HTML gives it",
    ),
    (
        "/// # Explain the `literal\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 2)},
        "a heading is one line, so the line under it is a new block",
    ),
    (
        "/// # A `literal TODO: marker` heading\n",
        set(),
        "but a span within one heading still pairs",
    ),
    (
        "/// Explain the `literal\n"
        "/// ***\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 3)},
        "and a thematic break ends the paragraph on each side of it",
    ),
    (
        "/// Explain <code =bad>TODO: add retry</code> here.\n",
        {("CH002", 1)},
        "an attribute must have a name, so this opens no element",
    ),
    (
        "/// Explain <code class=\"x\">TODO: add retry</code> here.\n",
        set(),
        "a quoted attribute value is one",
    ),
    (
        "/// Explain <code data-x=y>TODO: add retry</code> here.\n",
        set(),
        "and so is an unquoted one",
    ),
    (
        "/// Explain <code title=\"a>b\">TODO: add retry</code> here.\n",
        set(),
        "a quoted value may hold the bracket that would end the tag",
    ),
    (
        "// extern \"C-unwind\" fn stale() {\n",
        {("CH001", 1)},
        "a hyphenated ABI is still a commented-out signature",
    ),
    (
        "// extern fn stale() {\n",
        {("CH001", 1)},
        "and so is one with no ABI string at all",
    ),
    (
        "// Use the extern \"C-unwind\" convention for the callback here.\n",
        set(),
        "but prose that names an ABI is prose",
    ),
    (
        "/// > Explain the `literal\n"
        "/// TODO: issue required` suffix.\n",
        set(),
        "a lazy continuation stays inside the quote, so the span pairs",
    ),
    (
        "/// > Explain the `literal\n"
        "///\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 3)},
        "but a blank line really does leave it",
    ),
    (
        "/// > Explain the `literal\n"
        "/// # Heading\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 3)},
        "and a line that begins a block is no continuation",
    ),
    (
        "// pub use crate::foo;\n",
        {("CH001", 1)},
        "a re-export carries a visibility like any other item",
    ),
    (
        "// pub(crate) use crate::foo;\n",
        {("CH001", 1)},
        "including a restricted one",
    ),
    (
        "// fn r#match() {\n",
        {("CH001", 1)},
        "a raw identifier is an identifier",
    ),
    (
        "// pub struct r#type;\n",
        {("CH001", 1)},
        "in a type name as well as a function name",
    ),
    (
        "// Use the LATER definition of the shard map here.\n",
        set(),
        "but prose that opens with the word is prose",
    ),
    (
        "// pub use the cached resolver when the shard map is warm.\n",
        set(),
        "and a path is one word, never a sentence",
    ),
    (
        "// #123 - TODO: remove the legacy fallback\n",
        set(),
        "a reference abutting a marker on its left still tracks it",
    ),
    (
        "// https://example.com/x - TODO: remove the legacy fallback\n",
        set(),
        "a URL does the same",
    ),
    (
        "// TODO(#1): a; #2 - TODO: b\n",
        set(),
        "and a clause separator opens the run, so both markers are tracked",
    ),
    (
        "// Fixes #123. TODO: add retries\n",
        {("CH002", 1)},
        "but a reference in an earlier clause is not adjacent",
    ),
    (
        "// let r#match = parse();\n",
        {("CH001", 1)},
        "a raw identifier binds like any other",
    ),
    (
        "// let r#match: usize;\n",
        {("CH001", 1)},
        "with or without an initializer",
    ),
    (
        "// TODO: #123; TODO: add retries\n",
        {("CH002", 1)},
        "one reference tracks one marker, never the next one too",
    ),
    (
        "// pub use inner::{r#type};\n",
        {("CH001", 1)},
        "a raw identifier inside a grouped use tree",
    ),
    (
        "/// <code>outer <code>inner</code> TODO: add retry</code>\n"
        "/// TODO: add retry\n",
        {("CH002", 2)},
        "an inner closer ends the inner element, not the outer one",
    ),
    (
        "/// Explain \\``TODO: issue required` suffix.\n",
        set(),
        "an escape takes one backtick, leaving the rest of the run",
    ),
    (
        "/// Intro `literal\n"
        "/// 2. TODO: issue required` suffix.\n",
        set(),
        "a marker that cannot interrupt a paragraph opens no block",
    ),
    (
        "/// Intro `literal\n"
        "///\n"
        "/// 2. TODO: issue required` suffix.\n",
        {("CH002", 3)},
        "but after a blank line the same marker does",
    ),
    (
        "// macro_rules! stale { () => {}; }\n"
        "// The macro_rules! form is described in the guide below.\n",
        {("CH001", 1)},
        "a macro definition ends at its brace, and prose naming one does not",
    ),
    (
        "/// > Explain the `literal\n"
        "/// 2. TODO: issue required` suffix.\n",
        {("CH002", 2)},
        "a list leaving a quote is no lazy continuation of it",
    ),
    (
        "/// > Explain the `literal\n"
        "/// - TODO: issue required` suffix.\n",
        {("CH002", 2)},
        "a bullet does the same",
    ),
    (
        "/// | `literal | TODO: issue required` |\n"
        "/// | - | - |\n",
        {("CH002", 1)},
        "a code span cannot pair across two cells of a header row",
    ),
    (
        "/// | a | b |\n"
        "/// | - | - |\n"
        "/// | `literal | TODO: issue required` |\n",
        {("CH002", 3)},
        "nor across two cells of a body row",
    ),
    (
        "/// | a |\n"
        "/// | - |\n"
        "/// | `code TODO: marker` here |\n",
        set(),
        "but within one cell it still pairs",
    ),
    (
        "// | `literal | TODO: issue required` |\n"
        "// | - | - |\n",
        set(),
        "and a // comment has no table, so the span reaches across",
    ),
    (
        "/// > Explain the `literal\n"
        "///     ``` TODO: issue required` suffix.\n",
        set(),
        "an over-indented delimiter opens no fence, so the quote carries on",
    ),
    (
        "/// > Explain the `literal\n"
        "/// ``` TODO: issue required` suffix.\n",
        set(),
        "nor does one whose info string holds a backtick",
    ),
    (
        "/// > Explain the text\n"
        "/// ```\n"
        "/// let x = 1;\n"
        "/// ```\n",
        set(),
        "but a real fence does, and exempts its own content",
    ),
    (
        "/// > Explain the text\n"
        "/// ```\n"
        "/// TODO: issue required\n",
        set(),
        "including an unclosed one, which runs to the end of the run",
    ),
    (
        "/// > Explain the `literal\n"
        "///     ===\n"
        "/// TODO: issue required` suffix.\n",
        set(),
        "an over-indented rule is content, so the quote carries on",
    ),
    (
        "/// > Explain the `literal\n"
        "/// ===\n"
        "/// TODO: issue required` suffix.\n",
        set(),
        "and \"===\" underlines no paragraph it does not share a container with",
    ),
    (
        "/// > Explain the `literal\n"
        "/// ---\n"
        "/// TODO: issue required` suffix.\n",
        {("CH002", 3)},
        "but a thematic break really does end the quote",
    ),
    (
        "/// > Explain the `literal\n"
        "///     ---\n"
        "/// TODO: issue required` suffix.\n",
        set(),
        "unless it is indented past its container",
    ),
]


# CH001's boundary: Rust that was commented out, versus prose that merely
# opens with a Rust keyword. Two review rounds landed false positives here, so
# both sides are pinned. A false positive is the worse failure -- it fails CI
# on ordinary English -- but a rule that catches nothing is not a rule.
CODE_SHAPE_TESTS = [
    # (line, is commented-out code)
    ("let (left, right) = split();", True),
    ("let [a, b] = arr;", True),
    ("let mut (a, b) = t;", True),
    ("let (a, b): (u8, u8) = t;", True),
    ("let Foo { x, y } = value;", True),
    ("let crate::Foo { x } = v;", True),
    ("let Some(v) = opt else {", True),
    ("let Ok(row) = fetch() else {", True),
    ("let (Some(a), Some(b)) = pair else {", True),
    ("let [first, ..] = slice else {", True),
    ("let mut retries: usize;", True),
    ("let buf: Vec<u8>;", True),
    ("let handle: &'a mut Worker;", True),
    ("let T: Send is required here;", False),
    ("let this be the rule: the worker parks;", False),
    ("let the caller decide;", False),
    ("let (or rather, allow) the worker retry;", False),
    ("let us assume the queue is paused;", False),
    ("let Some values be missing here;", False),
    ("let the reader see (a) the claim and (b) the release;", False),
    ("fn foo() {", True),
    ("pub fn bar();", True),
    ("fn foo(", True),
    ("fn foo(a: u8,", True),
    ("fn f(x: impl Send + Sync,", True),
    ("fn f(x: [u8; 4],", True),
    ("fn f(&self,", True),
    ("fn foo(the caller name appears, and so on,", False),
    ("struct Foo {", True),
    ("impl Foo {", True),
    ("use a::b;", True),
    ("use a::{b, c};", True),
    ("use crate::x as y;", True),
    ("let x = 1;", True),
    ("let mut v = Vec::new();", True),
    ("type A = B;", True),
    ("}", True),
    ("});", True),
    ("#[derive(Debug)]", True),
    ("assert_eq!(a, b);", True),
    ('anyhow::bail!("oops");', True),
    ("if ready {", True),
    ("for item in items {", True),
    ("while let Some(x) = it.next() {", True),
    ("match state {", True),
    ("impl Foo {", True),
    ("impl<T> Trait for Foo {", True),
    ("value = compute();", True),
    ("self.count = 0;", True),
    ("count += 1;", True),
    ("retries -= 1;", True),
    ("flags |= READY;", True),
    ("bits <<= 2;", True),
    ("vec![1, 2];", True),
    ("my_macro!{ a: 1 };", True),
    ("see vec![1, 2] for the shape;", False),
    ("if the queue is paused, the worker parks {", False),
    ("impl the row has already been deleted by retention {", False),
    ("cleanup();", True),
    ("client.send(value).await?;", True),
    ("Type::method::<T>(value);", True),
    ("iter.collect::<Vec<_>>();", True),
    ("foo::<u8>(1)?;", True),
    ("see collect::<Vec<_>>() for the shape;", False),
    ("self.flush()?;", True),
    ("return Err(error);", True),
    ("break;", True),
    ("call cleanup() first, then retry;", False),
    ("see compute() for the details;", False),
    ("fn foo() is called by the wrapper.", False),
    ("fn resolve_call(the caller name appears in diagnostics", False),
    ("fn build() constructs the program; see below.", False),
    ("let the caller decide, since the row may be gone;", False),
    ("let x = the value the operator supplied;", False),
    ("type x = whatever the operator decided to configure;", False),
    ("use the LATER definition on a duplicate name;", False),
    ("use Foo, which the macro expands to;", False),
    ("struct directly; a malicious body must not flip it.", False),
    ("mod bar is documented in docs/architecture.md.", False),
    # Compound types in an uninitialized binding. Each needs a character the
    # scalar form does not: an array length needs `;`, a trait object `+`, a
    # function pointer `->`, a raw pointer `*`.
    ("let bytes: [u8; 32];", True),
    ("let callback: Box<dyn Fn() + Send>;", True),
    ("let function: fn(u8) -> u8;", True),
    ("let ptr: *const u8;", True),
    ("let v: Vec<Box<dyn Error + Send + Sync>>;", True),
    ("let s: &'a [u8];", True),
    # The hyphen those admit is `->` and nothing else.
    ("let a::b is re-exported for callers;", False),
    # Tuple structs, whose field list sits on the line.
    ("struct CountingLayer(Arc<Mutex<u64>>);", True),
    ("struct Wrapper(u8);", True),
    ("pub struct Foo<T>(T, T);", True),
    ("struct fields are described below;", False),
    ("struct (or enum) definitions live here;", False),
    ("struct Packet([u8; 32]);", True),
]


# The adversarial prose sweep. Every Rust keyword CH001 keys on, crossed with
# the sentence shapes this corpus actually writes. None of these is code, so a
# match is a false positive -- and a false positive on a Tier A rule fails CI
# on ordinary English, which is worse than missing a defect.
#
# This exists because hand-editing CH001 produced a false positive in three
# separate review rounds. Generating the cross-product catches them before a
# reviewer does: it found six on one pass and four on another, each time in a
# branch that looked correct in isolation.
# Every English contraction CH006 is expected to catch, and the possessive
# forms it must not. CH006 produced three findings in three review rounds --
# `can't`, the modal perfects, the interrogatives -- each because the rule was
# spot-checked rather than enumerated. This is the enumeration.
CONTRACTIONS_EXPECTED = [
    "can't", "won't", "don't", "doesn't", "didn't", "isn't", "aren't", "wasn't",
    "weren't", "hasn't", "haven't", "hadn't", "wouldn't", "couldn't",
    "shouldn't", "mustn't", "needn't", "oughtn't", "mightn't", "shan't",
    "ain't", "daren't",
    "it's", "that's", "there's", "here's", "what's", "who's", "how's",
    "where's", "when's", "why's", "he's", "she's", "let's",
    "we're", "they're", "you're",
    "we've", "they've", "you've", "i've", "should've", "could've", "would've",
    "must've", "might've",
    "we'll", "they'll", "you'll", "i'll", "it'll", "he'll", "she'll",
    "we'd", "they'd", "you'd", "i'd", "he'd", "she'd", "i'm",
    # Typographic apostrophes. An editor that substitutes these must not turn
    # a gated contraction into an invisible pass.
    "can\u2019t", "isn\u2019t", "we\u2019re", "it\u2019s", "should\u2019ve", "won\u2019t",
]
# Possessives and abbreviations STE permits. A match here is a false positive.
CONTRACTIONS_EXCLUDED = [
    "one's", "someone's", "everyone's", "nobody's", "everything's",
    "something's", "nothing's", "the row's", "the queue's", "TTL'd",
    # `world` was in the stem list and produced a false positive on this
    # ordinary possessive, contradicting the rule's own rationale.
    "the world's",
]

SWEEP_KEYWORDS = [
    "fn", "struct", "enum", "trait", "union", "mod", "const", "static", "type",
    "impl", "let", "use", "pub fn", "async fn", "if", "while", "for", "match",
    "loop", "unsafe", "return", "break", "continue", "self", "ctx", "cleanup",
    "value", "count",
]
SWEEP_TAILS = [
    "{n} handles the retry path.",
    "{n} is called by the wrapper.",
    "{n} resolve_call(the caller name appears in diagnostics",
    "{n} foo() returns a Resolution; see below.",
    "{n} Foo {{ .. }} is the shape we persist.",
    "{n} the caller decide, since the row may be gone;",
    "{n} bar is documented in docs/architecture.md.",
    "{n} Foo, which the macro expands to;",
    "{n} x = the value the operator supplied;",
    "{n} T: Send is required here;",
    "{n} this module owns the sweep;",
    "{n} `Foo` implements Display for the CLI.",
    "{n} x = whatever the operator decided to configure;",
    "{n} a::b is re-exported for callers;",
    "{n} foo(the caller name appears, and so on,",
    "{n} the row, the timer, and the task,",
    "{n} cleanup() first, then retry;",
    "{n} runs before the sweep completes;",
    "{n} early, before the lock is taken;",
    "{n} the queue is paused, the worker parks {{",
    "{n} = the number of rows the sweep deleted;",
    "{n} the row has already been deleted by retention {{",
    "{n} we only ever see this on a cold start {{",
]


def prose_sweep() -> list[str]:
    """Generated prose lines CH001 must never match."""
    return [
        tail.format(n=keyword)
        for keyword in SWEEP_KEYWORDS
        for tail in SWEEP_TAILS
    ]


def ratchet_reporting_test() -> int:
    """A Tier B failure must name the line the contributor has to fix.

    The hard case is a duplicate: the fingerprint IS the comment text, so two
    identical comments are one fingerprint with a count. Reporting the first
    occurrence then points at the legacy line whenever the merge base already
    carried a copy -- a gate telling someone to edit a comment they never
    wrote. Every candidate is named instead, with a count of how many are new.
    """
    legacy = "// It doesn\u2019t matter here.\n"
    base_source = "// filler one\n" + legacy + "// filler\n" * 12
    head_source = base_source + "// tail filler\n" + legacy

    def tier_b(source: str) -> list[Finding]:
        return [f for f in findings_for_source("f.rs", source) if f.rule in TIER_B]

    def grouped(items: list[Finding]) -> dict:
        out: dict = defaultdict(lambda: defaultdict(list))
        for f in items:
            out[f.rule][f.path].append(f.fingerprint)
        return out

    head = tier_b(head_source)
    lines = compare_tier_b(
        grouped(head), grouped(tier_b(base_source)), {"f.rs"}, {}, index_findings(head)
    )
    named = {int(line.split(":")[1]) for line in lines if line.startswith("    f.rs:")}
    ok = named == {2, 16} and any("1 of these 2" in line for line in lines)
    print(f"  [{'ok  ' if ok else 'FAIL'}] a duplicated Tier B finding names every candidate line")
    if not ok:
        for line in lines:
            print(f"         {line}")
    return 0 if ok else 1


def tier_a_only_test() -> int:
    """`--tier-a-only` must mean the same thing with and without `--json`.

    The two flags together asked for the absolute gates in machine-readable
    form and got a Tier B regression in the exit status. This is not a
    lexer question, so it cannot be a fixture: it is the flag pair, checked
    on the one function that now answers for both paths.
    """
    legacy = "// It doesn\u2019t matter here.\n"

    def tier_b(source: str) -> list[Finding]:
        return [f for f in findings_for_source("f.rs", source) if f.rule in TIER_B]

    def grouped(items: list[Finding]) -> dict:
        out: dict = defaultdict(lambda: defaultdict(list))
        for f in items:
            out[f.rule][f.path].append(f.fingerprint)
        return out

    head = tier_b("// filler\n" + legacy)
    current, baseline = grouped(head), grouped(tier_b("// filler\n"))
    gated = tier_b_gate(current, baseline, {"f.rs"}, {}, head, False)
    skipped = tier_b_gate(current, baseline, {"f.rs"}, {}, head, True)
    ok = bool(gated) and skipped == []
    print(f"  [{'ok  ' if ok else 'FAIL'}] --tier-a-only skips the Tier B ratchet")
    if not ok:
        print(f"         gated {gated!r}\n         skipped {skipped!r}")
    return 0 if ok else 1


def self_test() -> int:
    """Prove the lexer still handles the Rust forms the rules depend on."""
    failures = 0
    for source, expected, name in SELF_TESTS:
        got = [p.text.strip() for p in extract_comments(source) if p.text.strip()]
        ok = got == expected
        failures += 0 if ok else 1
        print(f"  [{'ok  ' if ok else 'FAIL'}] {name}")
        if not ok:
            print(f"         expected {expected!r}\n         got      {got!r}")

    # Line numbers must point at the line the comment is really on.
    source = 'let e = "a \\\n b";\n// target\n'
    pieces = [p for p in extract_comments(source) if p.text.strip()]
    ok = len(pieces) == 1 and pieces[0].line == 3
    failures += 0 if ok else 1
    print(f"  [{'ok  ' if ok else 'FAIL'}] line number survives a continuation")
    if not ok:
        print(f"         expected line 3, got {[(p.line, p.text) for p in pieces]!r}")

    missed = [c for c in CONTRACTIONS_EXPECTED if not CONTRACTION_RE.search(f"The row {c} ready.")]
    wrong = [c for c in CONTRACTIONS_EXCLUDED if CONTRACTION_RE.search(f"The row {c} ready.")]
    failures += len(missed) + len(wrong)
    if missed or wrong:
        print(f"  [FAIL] CH006 contraction inventory: {len(missed)} missed, {len(wrong)} false")
        for c in missed:
            print(f"         missed: {c!r}")
        for c in wrong:
            print(f"         false positive on possessive: {c!r}")
    else:
        print(
            f"  [ok  ] CH006 contraction inventory "
            f"({len(CONTRACTIONS_EXPECTED)} matched, {len(CONTRACTIONS_EXCLUDED)} correctly ignored)"
        )

    sweep = prose_sweep()
    sweep_fps = [line for line in sweep if COMMENTED_CODE_RE.match(line)]
    failures += len(sweep_fps)
    if sweep_fps:
        print(f"  [FAIL] CH001 adversarial prose sweep: {len(sweep_fps)} false positive(s)")
        for line in sweep_fps[:10]:
            print(f"         {line!r}")
    else:
        print(f"  [ok  ] CH001 adversarial prose sweep ({len(sweep)} generated prose lines)")

    shape_failures = 0
    for line, want in CODE_SHAPE_TESTS:
        if bool(COMMENTED_CODE_RE.match(line)) != want:
            shape_failures += 1
            kind = "false positive" if not want else "missed"
            print(f"  [FAIL] CH001 {kind}: {line!r}")
    failures += shape_failures
    if not shape_failures:
        print(f"  [ok  ] CH001 code-vs-prose boundary ({len(CODE_SHAPE_TESTS)} shapes)")

    for source, expected, name in RULE_TESTS:
        got = {(f.rule, f.line) for f in findings_for_source("t.rs", source)}
        ok = got == expected
        failures += 0 if ok else 1
        print(f"  [{'ok  ' if ok else 'FAIL'}] {name}")
        if not ok:
            print(f"         expected {sorted(expected)!r}\n         got      {sorted(got)!r}")

    failures += ratchet_reporting_test()
    failures += tier_a_only_test()

    print("\nOK: self-test passed." if not failures else f"\n{failures} self-test failure(s).")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--json", action="store_true", help="emit findings as JSON")
    parser.add_argument(
        "--tier-a-only", action="store_true", help="check only the absolute gates"
    )
    parser.add_argument(
        "--self-test", action="store_true", help="check the lexer against its fixtures"
    )
    parser.add_argument(
        "--base",
        metavar="REF",
        help=(
            "gate Tier B only on files changed since the merge base with REF "
            "(e.g. origin/trunk-dev). Without it, Tier B is reported over the "
            "whole corpus but never fails, because a whole-corpus count is not "
            "this change's to answer for."
        ),
    )
    parser.add_argument("--paths", nargs="*", help="limit the scan to these .rs files")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    findings = scan(args.paths)

    # Tier B gates only what this change touched. Unscoped, it reports and
    # never fails: the alternative is failing a PR for a file it never opened.
    scope: set[str] | None = None
    renames: dict[str, str] = {}
    merge_base = ""
    if args.base:
        context = diff_context(args.base)
        scope = None if context is None else context[1]
        if context is not None:
            merge_base, _, renames = context
        if scope is None:
            print(
                f"comment-hygiene: cannot diff against {args.base!r} (unknown ref, "
                "no git, or a shallow clone). Tier B is report-only for this run.",
                file=sys.stderr,
            )
            # An empty scope gates nothing. Falling back to gating the WHOLE
            # corpus would be the worst option available: it fails the build
            # over legacy debt the change never touched, precisely when the
            # tool has already admitted it cannot tell what changed.
            scope = set()
            scope_note = f"could not diff against {args.base}; report-only"
        else:
            rust = sorted(p for p in scope if p.endswith(".rs"))
            moved = sum(1 for p in renames if p.endswith(".rs"))
            scope_note = (
                f"gating {len(rust)} changed .rs file(s) vs {args.base}"
                + (f", {moved} renamed" if moved else "")
            )
    else:
        scope = set()
        scope_note = "no --base given; report-only"

    baseline = {}
    # Not computed when only the absolute gates apply: nothing reads it on
    # that path, and building it walks the merge base for every file in
    # scope.
    if args.base and scope and not args.tier_a_only:
        baseline = baseline_from_merge_base(merge_base, scope, renames)

    if args.json:
        current = tally(findings)
        regressions = tier_b_gate(
            current, baseline, scope, renames, findings, args.tier_a_only
        )
        print(
            json.dumps(
                {
                    "findings": [f.as_dict() for f in findings],
                    "counts": current,
                    "scope": sorted(scope) if scope is not None else None,
                    "merge_base": merge_base,
                    "tier_b_regressions": regressions,
                },
                indent=2,
            )
        )
        tier_a = [f for f in findings if f.rule in TIER_A]
        return 1 if tier_a or regressions else 0

    return report(findings, baseline, args.tier_a_only, scope, scope_note, renames)


if __name__ == "__main__":
    sys.exit(main())
