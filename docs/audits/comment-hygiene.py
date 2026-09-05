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
      | use\s+(?:\w+::)*(?:\w+|\*|\{[\w:,\s*]+\})(?:\s+as\s+\w+)?;\s*$
      | \#!?\[[\w:()"'=,./\s-]+\]\s*$
      | \}[,;)]*\s*$
      | [\w:]+!\(.*\)\s*;\s*$                       # any macro statement
      # Control flow opening a block. The lookahead rejects a condition made
      # of four or more consecutive plain words, which is a sentence, not an
      # expression: "if the queue is paused, the worker parks {".
      | (?:if|while|for|match|loop|unsafe)\b
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)[^;]*\{\s*$
      | [\w.\[\]:]+\s*=\s*[^\s;]+\s*;\s*$             # assignment, single-token RHS
      # A commented-out statement. Anchored hard: the call must open at the
      # very start, so prose that merely names a function ("call cleanup()
      # first") cannot reach it, and the line must end at the `;`. Measured
      # against all 176k corpus comments before adding: one hit, and it was
      # real commented-out code.
      | (?:return|break|continue)\b(?:\s+[^\s;{}]+)?\s*;\s*$
      | [\w:]+(?:\.[\w:]+)*\(.*\)(?:\s*\?|\s*\.await\s*\??)*\s*;\s*$
    )""",
    re.VERBOSE,
)

# A marker that OPENS a comment, or one punctuated as a marker (`TODO:`,
# `FIXME(...)`). Prose that merely refers to a marker elsewhere -- "see the
# `session_id` TODO above" -- is not itself an untracked commitment.
TODO_RE = re.compile(r"^(?:TODO|FIXME|XXX|HACK)\b|\b(?:TODO|FIXME|XXX|HACK)\s*[:(]")
TODO_REF_RE = re.compile(r"#\d+|https?://")

# First-person deliberation. "Actually" must open a sentence: mid-sentence it is
# an ordinary adverb ("gated on THIS claimant actually, durably marking the
# row"). `we'd`/`isn't` and friends are left to CH006 -- they are contractions,
# not necessarily deliberation.
NARRATIVE_RE = re.compile(
    r"(?:^|(?<=[.!?;]\s))\s*actually[,\s]"
    r"|\b(?:let's\b|lets just\b|we'll\b|i think\b|i'm not sure\b"
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
CONTRACTION_RE = re.compile(
    r"\b(?:ca|is|are|was|were|do|does|did|would|could|should|will|has|have|had"
    r"|must|ai|wo|sha|need|ought|might)n't\b"
    r"|\b(?:it|that|there|here|what|who|let|he|she|we|they|you|i|world)'"
    r"(?:s|ll|re|ve|d|m)\b"
    r"|\b(?:should|could|would|must|might)'ve\b",
    re.IGNORECASE,
)

MAX_SENTENCE_WORDS = 25

FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
TABLE_RE = re.compile(r"^\s*\|")
HEADING_RE = re.compile(r"^\s*#{1,6}\s")
LIST_ITEM_RE = re.compile(r"^\s*(?:[-*+]|\d+\.)\s+")
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

    __slots__ = ("line", "marker", "body", "trailing", "block", "group")

    def __init__(
        self, line: int, marker: str, body: str, trailing: bool, block: bool, group: int = -1
    ) -> None:
        self.line = line
        self.marker = marker
        self.body = body
        self.trailing = trailing
        self.block = block
        # Which `/* */` this piece came from. One block comment is one comment
        # however many lines it spans, so its pieces stay in one run even when
        # the block opens after code on its first line. -1 for line comments,
        # which are grouped by adjacency instead.
        self.group = group

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
            # Past the WHOLE marker: leaving the `!` of `/*!` on the body made
            # the anchored rules miss `/*! TODO */` and `/*! let x = 1; */`.
            i += len(marker)
            seg_start = i
            seg_line = line
            first = True
            while i < n and depth > 0:
                if source.startswith("/*", i):
                    # Nested comment. End the segment here so the inner body
                    # starts a piece of its own; otherwise
                    # `/* outer /* let x = 1; */ */` hands the rules one string
                    # beginning "outer", and the nested code is never anchored.
                    if i > seg_start:
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, block_group)
                        )
                        first = False
                    depth += 1
                    # A nested comment is its own comment: give it a fresh
                    # group so its fence state cannot leak into the text that
                    # resumes after it closes.
                    block_group += 1
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
                elif source.startswith("*/", i):
                    if depth > 1 and i > seg_start:
                        pieces.append(
                            Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, block_group)
                        )
                        first = False
                    depth -= 1
                    i += 2
                    if depth > 0:
                        # Resuming the enclosing comment. New group again, so
                        # the text after a nested block is judged on its own.
                        block_group += 1
                        seg_start = i
                        seg_line = line
                elif source[i] == "\n":
                    pieces.append(
                        Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True, block_group)
                    )
                    first = False
                    line += 1
                    i += 1
                    seg_start = i
                    seg_line = line
                else:
                    i += 1
            tail_end = i - 2 if depth == 0 else i
            if tail_end > seg_start:
                pieces.append(
                    Piece(seg_line, marker, source[seg_start:tail_end], code_on_line and first, True, block_group)
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


def fence_transition(
    fence: tuple[str, int] | None, match: "re.Match[str]", text: str
) -> tuple[str, int] | None:
    """Apply one fence-delimiter line to the fence state.

    CommonMark, not "any three backticks toggle it". A closer must use the
    SAME character as its opener and be at least as long, with nothing after
    it. Otherwise a ``` line inside a ````-fenced example closes the fence
    early and the example's own sample text is then read as real comments.
    """
    delimiter = match.group(1)
    char, length = delimiter[0], len(delimiter)
    info = text[match.end():]
    if fence is None:
        # Opening. An info string ("```rust") is allowed, but CommonMark
        # forbids a backtick inside a BACKTICK fence's info string -- so
        # "```foo`bar" is not a fence at all, and treating it as one opens a
        # fence that never closes and suppresses the rest of the run.
        if char == "`" and INFO_BACKTICK_RE.search(info):
            return None
        return char, length
    open_char, open_length = fence
    closes = char == open_char and length >= open_length and not text[match.end():].strip()
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
        for piece in run:
            text = piece.text
            match = FENCE_RE.match(text)
            if match:
                fence = fence_transition(fence, match, text)
                yield piece.line, text, True
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

        if COMMENTED_CODE_RE.match(stripped):
            findings.append(Finding("CH001", path, lineno, stripped))

        todo = TODO_RE.search(stripped)
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
    units: list[tuple[int, str]] = []

    for block in comment_runs(pieces):
        run: list[str] = []
        run_line = 0
        fence: tuple[str, int] | None = None

        def flush():
            nonlocal run
            if run:
                units.append((run_line, " ".join(run)))
                run = []

        for piece in block:
            body = piece.text
            match = FENCE_RE.match(body)
            if match:
                fence = fence_transition(fence, match, body)
                flush()
                continue
            if fence is not None:
                flush()
                continue
            if TABLE_RE.match(body) or HEADING_RE.match(body) or not body.strip():
                flush()
                continue
            if LIST_ITEM_RE.match(body):
                flush()
                run_line = piece.line
                run = [LIST_ITEM_RE.sub("", body).strip()]
                continue
            if not run:
                run_line = piece.line
            run.append(body.strip())
        flush()
    return units


def check_prose_rules(path: str, pieces: list[Piece]) -> list[Finding]:
    """CH003 and CH007 -- both judge a whole sentence, not a wrapped line.

    Line-level matching is wrong for these. Comment prose wraps mid-sentence,
    so a continuation line routinely *begins* with a word that is only a
    defect sentence-initially -- "... proves the append\\n// actually landed"
    is ordinary prose, not deliberation.
    """
    findings = []
    for lineno, unit in prose_units(pieces):
        for sentence in SENTENCE_SPLIT_RE.split(unit):
            sentence = sentence.strip()
            if not sentence:
                continue
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


def compare_tier_b(
    current: dict,
    baseline: dict,
    scope: set[str] | None = None,
    renames: dict[str, str] | None = None,
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
            if total:
                regressions.append(
                    f"{rule} {path}: {total} new finding(s), "
                    f"{len(now[path])} total vs {sum(allowed.values())} at the merge base "
                    f"[{RULE_TITLES[rule]}]"
                )
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

    regressions = compare_tier_b(current, baseline, scope, renames)
    if regressions:
        failed = True
        print(f"\n{len(regressions)} Tier B regression(s) -- these files gained violations:")
        for line in regressions[:40]:
            print(f"    {line}")
        if len(regressions) > 40:
            print(f"    ... and {len(regressions) - 40} more")
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
    ("if the queue is paused, the worker parks {", False),
    ("impl the row has already been deleted by retention {", False),
    ("cleanup();", True),
    ("client.send(value).await?;", True),
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
        regressions = compare_tier_b(current, baseline, scope, renames)
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
