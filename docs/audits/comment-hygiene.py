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

Whole constructs are exempt because flagging them is meaningless, not
because they are above the rules: fenced code blocks (``` / ~~~) are
sample code, and markdown tables and headings are not prose.

Usage:
    python3 docs/audits/comment-hygiene.py [--json] [--tier-a-only]
    python3 docs/audits/comment-hygiene.py --write-baseline
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
COMMENT_RE = re.compile(r"^(///|//!|//)(.*)$")
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


def comment_lines(lines: list[str]):
    """Yield (lineno, body, in_fence) for every comment line.

    `body` is the text after the `//`/`///`/`//!` marker. Fence delimiters are
    yielded with in_fence True so callers can skip them along with the fenced
    body.
    """
    fence = False
    for lineno, raw in enumerate(lines, 1):
        match = COMMENT_RE.match(raw.strip())
        if not match:
            continue
        body = match.group(2)
        if body.startswith(" "):
            body = body[1:]
        if FENCE_RE.match(body):
            fence = not fence
            yield lineno, body, True
            continue
        yield lineno, body, fence


def check_line_rules(path: str, lines: list[str]) -> list[Finding]:
    """CH001/CH002/CH003/CH005/CH006 -- all single-line judgements."""
    findings = []
    for lineno, body, in_fence in comment_lines(lines):
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


def check_block_edges(path: str, lines: list[str]) -> list[Finding]:
    """CH004 -- a comment run that opens or closes on an empty comment line."""
    findings = []
    run: list[tuple[int, str]] = []

    def close(run):
        if not run:
            return
        if not run[0][1].strip():
            findings.append(Finding("CH004", path, run[0][0], "block opens on an empty comment line"))
        if len(run) > 1 and not run[-1][1].strip():
            findings.append(Finding("CH004", path, run[-1][0], "block closes on an empty comment line"))

    for lineno, raw in enumerate(lines, 1):
        match = COMMENT_RE.match(raw.strip())
        if match:
            body = match.group(2)
            run.append((lineno, body[1:] if body.startswith(" ") else body))
        else:
            close(run)
            run = []
    close(run)
    return findings


def prose_units(lines: list[str]) -> list[tuple[int, str]]:
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

    for lineno, body, in_fence in comment_lines(lines):
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


def check_prose_rules(path: str, lines: list[str]) -> list[Finding]:
    """CH003 and CH007 -- both judge a whole sentence, not a wrapped line.

    Line-level matching is wrong for these. Comment prose wraps mid-sentence,
    so a continuation line routinely *begins* with a word that is only a
    defect sentence-initially -- "... proves the append\\n// actually landed"
    is ordinary prose, not deliberation.
    """
    findings = []
    for lineno, unit in prose_units(lines):
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
                lines = handle.read().split("\n")
        except OSError as exc:
            print(f"comment-hygiene: cannot read {rel}: {exc}", file=sys.stderr)
            continue
        findings.extend(check_line_rules(rel, lines))
        findings.extend(check_block_edges(rel, lines))
        findings.extend(check_prose_rules(rel, lines))
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--json", action="store_true", help="emit findings as JSON")
    parser.add_argument(
        "--write-baseline", action="store_true", help="freeze current Tier B counts"
    )
    parser.add_argument(
        "--tier-a-only", action="store_true", help="check only the absolute gates"
    )
    parser.add_argument("--paths", nargs="*", help="limit the scan to these .rs files")
    args = parser.parse_args()

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
