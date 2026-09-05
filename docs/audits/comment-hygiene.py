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

TIER B -- ratcheted against docs/audits/comment-hygiene-baseline.json
---------------------------------------------------------------------
Real style defects with a legacy population far too large to fix in one
change (CH007 alone is ~18k sentences). Freezing them as a per-file,
per-rule count means new code is held to the rule while old code is
merely forbidden from getting worse. Counts may fall freely; any increase,
or any file that gains its first violation, fails the build.

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
    python3 docs/audits/comment-hygiene.py --write-baseline
    python3 docs/audits/comment-hygiene.py --self-test
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

- CH001 recognizes commented-out Rust by line shape (a `let` binding, an
  item header, a `use`, an attribute, a lone closing brace). A single
  commented-out expression with no terminator -- `// foo(bar)` -- is not
  matched, because that shape is indistinguishable from prose naming a
  call, which this corpus does on thousands of lines.

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
import json
import os
import re
import sys
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASELINE_PATH = os.path.join(REPO_ROOT, "docs", "audits", "comment-hygiene-baseline.json")

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
            fn\s+\w+\s*(?:<[^<>]*>)?\s*\(.*$
      | (?:pub(?:\([\w:]+\))?\s+)?(?:struct|enum|trait|union)\s+\w+\s*(?:<[^<>]*>)?\s*[{;(]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?mod\s+\w+\s*[{;]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?(?:const|static)\s+(?:mut\s+)?\w+\s*:[^;=]+=.*[;{]\s*$
      | (?:pub(?:\([\w:]+\))?\s+)?type\s+\w+\s*(?:<[^<>]*>)?\s*=.*;\s*$
      | impl(?:\s*<[^<>]*>)?\s+[\w:<>&'\s]+\{\s*$
      | let\s+(?:mut\s+)?\w+\s*(?::[^;=]+)?=[^=].*;\s*$
      | use\s+[\w:{}, *]+;\s*$
      | \#!?\[[\w:()"'=,./\s-]+\]\s*$
      | \}[,;)]?\s*$
      | (?:assert|assert_eq|assert_ne|panic|println|dbg|unreachable)!\(.*\)\s*;\s*$
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
    r"\b(?:can|is|are|was|were|do|does|did|would|could|should|will|has|have|had"
    r"|must|ai|wo|sha|need|ought|might)n't\b"
    r"|\b(?:it|that|there|here|what|who|let|he|she|we|they|you|i|world)'"
    r"(?:s|ll|re|ve|d|m)\b",
    re.IGNORECASE,
)

MAX_SENTENCE_WORDS = 25

FENCE_RE = re.compile(r"^\s*(?:```|~~~)")
TABLE_RE = re.compile(r"^\s*\|")
HEADING_RE = re.compile(r"^\s*#{1,6}\s")
LIST_ITEM_RE = re.compile(r"^\s*(?:[-*+]|\d+\.)\s+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


class Finding:
    __slots__ = ("rule", "path", "line", "text")

    def __init__(self, rule: str, path: str, line: int, text: str) -> None:
        self.rule = rule
        self.path = path
        self.line = line
        self.text = text

    def as_dict(self) -> dict:
        return {"rule": self.rule, "path": self.path, "line": self.line, "text": self.text}


class Piece:
    """One line's worth of comment text, located and classified.

    `trailing` marks a comment that follows code on its line
    (`foo(); // note`). `block` marks `/* ... */` rather than `//`. A block
    comment spanning N lines yields N pieces, so every finding keeps a true
    line number.
    """

    __slots__ = ("line", "marker", "body", "trailing", "block")

    def __init__(self, line: int, marker: str, body: str, trailing: bool, block: bool) -> None:
        self.line = line
        self.marker = marker
        self.body = body
        self.trailing = trailing
        self.block = block

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
            marker = "//"
            for candidate in ("///", "//!"):
                if raw.startswith(candidate):
                    marker = candidate
                    break
            pieces.append(Piece(line, marker, raw[len(marker):], code_on_line, False))
            i = end
            continue

        # Block comment: nests in Rust, so track depth rather than find("*/").
        if source.startswith("/*", i):
            marker = "/*"
            for candidate in ("/**", "/*!"):
                if source.startswith(candidate, i):
                    marker = candidate
                    break
            depth = 1
            i += 2
            seg_start = i
            seg_line = line
            first = True
            while i < n and depth > 0:
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                elif source[i] == "\n":
                    pieces.append(
                        Piece(seg_line, marker, source[seg_start:i], code_on_line and first, True)
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
                    Piece(seg_line, marker, source[seg_start:tail_end], code_on_line and first, True)
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


def comment_lines(pieces: list[Piece]):
    """Yield (lineno, text, in_fence) per comment piece, tracking ``` fences.

    A fence delimiter is yielded with in_fence True so callers skip it along
    with the fenced body.
    """
    fence = False
    for piece in pieces:
        text = piece.text
        if FENCE_RE.match(text):
            fence = not fence
            yield piece.line, text, True
            continue
        yield piece.line, text, fence


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
    run: list[Piece] = []
    prev_line = -2

    def close():
        if not run:
            return
        if not run[0].text.strip():
            findings.append(
                Finding("CH004", path, run[0].line, "block opens on an empty comment line")
            )
        if len(run) > 1 and not run[-1].text.strip():
            findings.append(
                Finding("CH004", path, run[-1].line, "block closes on an empty comment line")
            )

    for piece in pieces:
        if piece.block or piece.trailing or piece.line != prev_line + 1:
            close()
            run = []
        prev_line = piece.line
        if not (piece.block or piece.trailing):
            run.append(piece)
    close()
    return findings


def prose_units(pieces: list[Piece]) -> list[tuple[int, str]]:
    """Comment prose as (lineno, text) units, ready to split into sentences.

    Fenced blocks, markdown tables and headings are dropped -- they are not
    prose and a word count over them means nothing. A list item starts its own
    unit so that a bulleted rationale is measured per bullet.
    """
    units: list[tuple[int, str]] = []
    run: list[str] = []
    run_line = 0
    prev_lineno = -2

    def flush():
        nonlocal run
        if run:
            units.append((run_line, " ".join(run)))
            run = []

    for lineno, body, in_fence in comment_lines(pieces):
        # A gap means intervening code: two comment blocks either side of a
        # statement are unrelated prose, and joining them would both misreport
        # the line number and manufacture sentences neither author wrote.
        if lineno != prev_lineno + 1:
            flush()
        prev_lineno = lineno
        if in_fence:
            flush()
            continue
        if TABLE_RE.match(body) or HEADING_RE.match(body) or not body.strip():
            flush()
            continue
        if LIST_ITEM_RE.match(body):
            flush()
            run_line = lineno
            run = [LIST_ITEM_RE.sub("", body).strip()]
            continue
        if not run:
            run_line = lineno
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
                    Finding("CH007", path, lineno, f"{len(words)} words: {sentence[:90]}")
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
        pieces = extract_comments(source)
        findings.extend(check_line_rules(rel, pieces))
        findings.extend(check_block_edges(rel, pieces))
        findings.extend(check_prose_rules(rel, pieces))
    findings.sort(key=lambda f: (f.rule, f.path, f.line))
    return findings


def tally(findings: list[Finding]) -> dict:
    """Per-rule, per-file counts -- the baseline's shape."""
    counts: dict = defaultdict(lambda: defaultdict(int))
    for f in findings:
        counts[f.rule][f.path] += 1
    return {rule: dict(sorted(files.items())) for rule, files in sorted(counts.items())}


def load_baseline() -> dict:
    if not os.path.exists(BASELINE_PATH):
        return {}
    with open(BASELINE_PATH, encoding="utf-8") as handle:
        return json.load(handle).get("counts", {})


def compare_tier_b(current: dict, baseline: dict) -> list[str]:
    """Regressions only: a count that rose, or a file newly in violation."""
    regressions = []
    for rule in TIER_B:
        now = current.get(rule, {})
        was = baseline.get(rule, {})
        for path in sorted(now):
            before = was.get(path, 0)
            after = now[path]
            if after > before:
                regressions.append(
                    f"{rule} {path}: {before} -> {after} "
                    f"(+{after - before}) [{RULE_TITLES[rule]}]"
                )
    return regressions


def write_baseline(findings: list[Finding]) -> None:
    counts = tally(findings)
    payload = {
        "_comment": (
            "Frozen Tier B counts for docs/audits/comment-hygiene.py. Counts may "
            "fall freely; any increase fails CI. Regenerate with "
            "`python3 docs/audits/comment-hygiene.py --write-baseline` ONLY when "
            "lowering a count or when a rule's definition changes."
        ),
        "counts": {rule: counts.get(rule, {}) for rule in TIER_B},
    }
    with open(BASELINE_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False)
        handle.write("\n")
    total = sum(sum(f.values()) for f in payload["counts"].values())
    print(f"Wrote {BASELINE_PATH} ({total} Tier B findings frozen).")


def report(findings: list[Finding], baseline: dict, tier_a_only: bool) -> int:
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
    print("\nTier B -- ratcheted against the baseline")
    for rule in TIER_B:
        now = sum(current.get(rule, {}).values())
        was = sum(baseline.get(rule, {}).values())
        delta = now - was
        arrow = "=" if delta == 0 else (f"+{delta}" if delta > 0 else str(delta))
        print(f"  {rule} {RULE_TITLES[rule]}: {now} (baseline {was}, {arrow})")

    regressions = compare_tier_b(current, baseline)
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
    ("/* outer /* nested */ still outer */ x();\n", ["outer /* nested */ still outer"], "nested block"),
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

    print("\nOK: lexer self-test passed." if not failures else f"\n{failures} self-test failure(s).")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--json", action="store_true", help="emit findings as JSON")
    parser.add_argument(
        "--write-baseline", action="store_true", help="freeze current Tier B counts"
    )
    parser.add_argument(
        "--tier-a-only", action="store_true", help="check only the absolute gates"
    )
    parser.add_argument(
        "--self-test", action="store_true", help="check the lexer against its fixtures"
    )
    parser.add_argument("--paths", nargs="*", help="limit the scan to these .rs files")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    findings = scan(args.paths)

    if args.write_baseline:
        if args.paths:
            print("--write-baseline needs a full scan; drop --paths.", file=sys.stderr)
            return 2
        write_baseline(findings)
        return 0

    if args.json:
        current = tally(findings)
        print(
            json.dumps(
                {
                    "findings": [f.as_dict() for f in findings],
                    "counts": current,
                    "tier_b_regressions": compare_tier_b(current, load_baseline()),
                },
                indent=2,
            )
        )
        tier_a = [f for f in findings if f.rule in TIER_A]
        return 1 if tier_a or compare_tier_b(current, load_baseline()) else 0

    return report(findings, load_baseline(), args.tier_a_only)


if __name__ == "__main__":
    sys.exit(main())
