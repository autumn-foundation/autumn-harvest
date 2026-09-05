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

- Sentence splitting is regex-level (`[.!?]` + whitespace), so an
  abbreviation that ends in a period ("e.g. ", "i.e. ", "vs. ") splits a
  sentence early and can under-report CH007. The corpus writes "e.g."
  and "i.e." constantly, so a naive fix (require a following capital)
  would instead MERGE sentences across "... the row. Postgres ..." and
  over-report. Both directions are wrong; under-reporting is the safe one
  for a gate, so the split stays naive and CH007's baseline absorbs the
  difference.

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
  also hide genuine prose violations that merely sit next to one.

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
COMMENTED_CODE_RE = re.compile(
    r"""^(?:
        (?:pub(?:\([\w:]+\))?\s+)?(?:async\s+|unsafe\s+|const\s+|extern\s+"\w+"\s+)*
            fn\s+\w+\s*(?:<[^<>]*>)?\s*\(
            (?:
                 .*\)\s*(?:->\s*[^;{]+?)?\s*[{;]   # complete: ends in { or ;
               | \s*$                                # wrapped: `fn foo(` at EOL
               | (?=[^)]*(?::|\bself\b))            # wrapped: real params,
                 [\w\s:&'<>\[\](),.+;=*-]*,\s*$      #   trailing comma
            )\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?(?:struct|enum|trait|union)\s+\w+\s*(?:<[^<>]*>)?\s*[{;(]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?mod\s+\w+\s*[{;]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?(?:const|static)\s+(?:mut\s+)?\w+\s*:[^;=]+=.*[;{]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?type\s+\w+\s*(?:<[^<>]*>)?\s*=
            (?!\s*(?:\w+\s+){2,}\w+\s*;\s*$).*;\s*$
      | impl(?:\s*<[^<>]*>)?\s+
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)[\w:<>&'\s]+\{\s*$
      | let\s+(?:mut\s+)?\w+\s*(?::[^;=]+)?=
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
      | let\s+(?:mut\s+)?\w+\s*:
            (?!\s*(?:\w+\s+){2,}\w+\s*;\s*$)\s*[\w:<>&'\[\]\s,()]+;\s*$
      | use\s+(?:\w+::)*(?:\w+|\*|\{[\w:,\s*]+\})(?:\s+as\s+\w+)?;\s*$
      | \#!?\[[\w:()"'=,./\s-]+\]\s*$
      | \}[,;)]*\s*$
      | [\w:]+!(?:\(.*\)|\[.*\]|\{.*\})\s*;\s*$        # macro stmt, any delimiter
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
def table_delimiter(text: str, container: int) -> bool:
    """Is `text` a GFM table's delimiter row -- "|---|:--:|" and its kin?

    Three columns past the container, like every marker here. Stripping the
    line before judging it loses that, and an over-indented delimiter then
    turns the paragraph above it into a table and drops the sentence.
    """
    if leading_columns(text) > container + 3:
        return False
    stripped = text.strip()
    return "|" in stripped and "-" in stripped and not stripped.strip(" \t|-:")

HEADING_RE = re.compile(r"^\s*#{1,6}(?:\s|$)")
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
THEMATIC_BREAK_RE = re.compile("^([ \t]*)([-_*])(?:[ \t]*\\2){2,}[ \t]*$")
# A Setext underline: "=" under a paragraph line makes that paragraph a
# heading. Whether a run of "=" is a heading or ordinary text is decided by
# POSITION, not shape -- with a paragraph open it underlines one, and at the
# start of a block it is the decorative rule round thirty-three fixed.
SETEXT_RE = re.compile("^([ \t]*)(?:=+|-+)[ \t]*$")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


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

    __slots__ = ("line", "marker", "body", "trailing", "block", "group", "nest")

    def __init__(
        self,
        line: int,
        marker: str,
        body: str,
        trailing: bool,
        block: bool,
        group: int = -1,
        nest: int = 0,
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

    @property
    def text(self) -> str:
        """The comment body, leading `*` gutter and one space stripped."""
        body = self.body
        if self.block:
            body = re.sub(r"^\s*\*(?!\*)", "", body)
        return body[1:] if body.startswith(" ") else body


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
            elif source.startswith("/**", i) and not source.startswith("/**/", i):
                # `/**/` is an empty comment, not a doc marker. Treating it as
                # one would eat the closing `*/` and swallow the rest of the
                # file as comment body.
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
            first = True
            while i < n and depth > 0:
                if source.startswith("/*", i):
                    # Nested comment. End the segment here so the inner body
                    # starts a piece of its own; otherwise
                    # `/* outer /* let x = 1; */ */` hands the rules one string
                    # beginning "outer", and the nested code is never anchored.
                    if i > seg_start:
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest)
                        )
                        first = False
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
                    elif source.startswith("/**", i) and not source.startswith("/**/", i):
                        nested = 3
                    i += nested
                    seg_start = i
                    seg_line = line
                    seg_nest = depth
                elif source.startswith("*/", i):
                    if depth > 1 and i > seg_start:
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest)
                        )
                        first = False
                    depth -= 1
                    i += 2
                    if depth > 0:
                        # Resuming the enclosing comment, same run and same
                        # nesting level it had before the nested comment.
                        seg_start = i
                        seg_line = line
                        seg_nest = depth
                elif source[i] == "\n":
                    pieces.append(
                        Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, root_group, seg_nest)
                    )
                    first = False
                    line += 1
                    i += 1
                    seg_start = i
                    seg_line = line
                    seg_nest = depth
                else:
                    i += 1
            tail_end = i - 2 if depth == 0 else i
            if tail_end > seg_start:
                pieces.append(
                    Piece(seg_line, marker, source[seg_start:tail_end], code_on_line and first, True, root_group, seg_nest)
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
    text: str, stack: list[tuple[int, int]], paragraph: bool, quoted: int = 0
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
        HEADING_RE.match(body)
        or thematic_break(body, stack[-1][0] if stack else 0)
        or (paragraph and setext_underline(body, stack[-1][0] if stack else 0))
    )
    return stack, prose


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
        saved: list = []
        nest = run[0].nest
        for piece in run:
            if piece.nest != nest:
                saved, (fence, scope, stack, paragraph) = nesting_shift(
                    saved, piece.nest, (fence, scope, list(stack), paragraph)
                )
                nest = piece.nest
            text = piece.text
            if fence is not None and leaves_container(text, scope):
                fence = None
            if fence is None:
                stack, paragraph = update_containers(text, stack, paragraph, quoted)
                quoted = quote_depth(text, stack[-1][0] if stack else 0)
            container = stack[-1][0] if stack else 0
            delimiter = fence_delimiter(text, container, fence is not None, scope[2], stack)
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
                yield piece.line, text, fence is not None or before is not None
                continue
            yield piece.line, text, fence is not None


def check_line_rules(path: str, pieces: list[Piece]) -> list[Finding]:
    """CH001/CH002/CH003/CH005/CH006 -- all single-line judgements."""
    findings = []
    for lineno, body, in_fence in comment_lines(pieces):
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
        code_line = strip_containers(stripped, [], 0)[0].strip() or stripped
        if COMMENTED_CODE_RE.match(code_line):
            findings.append(Finding("CH001", path, lineno, stripped))

        # CH002 is anchored at the start of the line for its unpunctuated
        # form, so it needs the same peel CH001 does: "- TODO fix this" is a
        # commitment with a bullet in front of it, and only a FENCE exempts.
        todo = TODO_RE.search(code_line)
        if todo and not TODO_REF_RE.search(stripped):
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
                units.append((run_lines[0], " ".join(run), spans))
            run, run_lines = [], []

        in_table = False
        for index, piece in enumerate(block):
            if piece.nest != nest:
                flush()
                saved, (fence, scope, stack, paragraph, quoted, in_list) = (
                    nesting_shift(
                        saved,
                        piece.nest,
                        (fence, scope, list(stack), paragraph, quoted, in_list),
                    )
                )
                nest = piece.nest
            body = piece.text
            if fence is not None and leaves_container(body, scope):
                fence = None
            if fence is None:
                stack, paragraph = update_containers(body, stack, paragraph, quoted)
            container = stack[-1][0] if stack else 0
            delimiter = fence_delimiter(body, container, fence is not None, scope[2], stack)
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
            lazy = (
                depth < quoted
                and bool(run)
                and body.strip()
                and not LIST_MARKER_RE.match(peeled)
                and "|" not in peeled
                and not HEADING_RE.match(peeled)
                and not SEPARATOR_RE.match(peeled)
                and not FENCE_RE.match(peeled)
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
            has_pipe = "|" in body
            if not has_pipe:
                in_table = False
            elif not in_table and index + 1 < len(block):
                in_table = table_delimiter(
                    strip_quote(block[index + 1].text, container), container
                )
            if (
                (has_pipe and in_table)
                or HEADING_RE.match(body)
                or SEPARATOR_RE.match(body)
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
                run = [(peeled if len(peeled) < len(body) else body[marker.end():]).strip()]
                run_lines = [piece.line]
                in_list = True
                continue
            run.append(body.strip())
            run_lines.append(piece.line)
        flush()
    return units


def split_sentences(unit: str):
    """(offset, sentence) pairs. `re.split` drops the offsets, and the offset
    is what maps a sentence back to the line it was written on."""
    start = 0
    for match in SENTENCE_SPLIT_RE.finditer(unit):
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
    for _, unit, spans in prose_units(pieces):
        for offset, sentence in split_sentences(unit):
            sentence = sentence.strip()
            if not sentence:
                continue
            lineno = line_of(spans, offset)
            if NARRATIVE_RE.search(sentence):
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
    if args.base and scope:
        baseline = baseline_from_merge_base(merge_base, scope, renames)

    if args.json:
        current = tally(findings)
        regressions = compare_tier_b(current, baseline, scope, renames, index_findings(findings))
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
