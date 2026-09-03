#!/usr/bin/env python3
"""Folio corpus harness: internal link check + orphan scan for docs/.

Deterministic, reproducible on any checkout — no network access required.
Two Tier-1 audits (see docs/audits/README.md):

1. Link check — every relative markdown link found in docs/**/*.md and the
   top-level *.md files is resolved against the filesystem. This covers
   both link forms this corpus actually uses: inline (`[text](path#anchor)`)
   and reference-style (`[text][ref]`, `[text][]`, or the bare shortcut
   `[ref]`, resolved against that file's own `[ref]: path#anchor`
   definitions — e.g. `docs/replay-drift-gate.md`'s `[replay canary]`). A
   missing target file, an undefined reference, or a `#anchor` that doesn't
   match any heading in the target (or, for a same-file `#anchor`, in the
   source), is reported as broken. Anchors are matched using GitHub's
   heading-slug rule (lowercase, spaces to hyphens, punctuation other than
   `-`/`_` stripped), which is what every internal link in this corpus was
   written against.

2. Orphan scan — pages under docs/ with zero inbound links from any other
   page in the corpus (docs/**/*.md or a top-level *.md). An unreachable
   page is a coverage defect in disguise: the answer may be correct and
   present, but nothing points a reader at it.

External links (http/https/mailto) are intentionally NOT checked — that
needs network access and is a different audit; this script is pure
filesystem, so it can run in CI on every PR.

Usage:
    python3 docs/audits/corpus-link-check.py [--json]

Exit code is 1 if a broken link is found in the reader corpus (orphans are
reported but do not fail the run — an orphan is a findability defect to
triage, not a hard error). A broken link inside a process/working-artifact
subtree (docs/plans/, docs/changelog.d/, docs/rnd/, docs/assays/,
docs/perf-artifacts/ — see PROCESS_ARTIFACT_PREFIXES) is still reported but
does NOT fail the run: those are historical records a reader doesn't land
on mid-task, not the corpus this gate protects. Run from anywhere; paths
are resolved relative to the repo root.
"""
import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Top-level *.md files that participate in the link graph as inbound-link
# sources (e.g. README.md points into docs/), but are not themselves part of
# "the corpus" the orphan scan grades — they're the front door (Onramp's
# territory) or process docs (CLAUDE.md, AGENTS.md, DESIGN-*.md,
# CHANGELOG.md, RELEASE_NOTES.md), not reader-facing reference pages.
TOP_LEVEL_SOURCES = ["README.md", "CHANGELOG.md", "RELEASE_NOTES.md"]

# Subtrees under docs/ that are process/working artifacts, not indexed
# reader reference pages: engineering plans (docs/plans/), the changelog
# fragment inbox (docs/changelog.d/ — CLAUDE.md calls this "a fragment
# file, never the shared phase list", i.e. write-only until release),
# R&D writeups (docs/rnd/), and dated performance investigations
# (docs/assays/, docs/perf-artifacts/, which is raw EXPLAIN/txt output, not
# prose). A reader debugging a live system doesn't land on these searching
# for an answer, so the orphan scan doesn't grade them, and a broken link
# found inside one doesn't fail CI — but they still participate as link
# SOURCES (a plan or assay legitimately links out to the reference page
# it's about) and their broken links are still reported, for a future
# pruning pass.
PROCESS_ARTIFACT_PREFIXES = (
    "docs/plans/",
    "docs/changelog.d/",
    "docs/rnd/",
    "docs/assays/",
    "docs/perf-artifacts/",
)

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
# Reference-style: `[text][ref]` / collapsed `[text][]` (ref == text).
REF_USE_RE = re.compile(r"\[([^\]]*)\]\[([^\]]*)\]")
# Bare shortcut reference `[ref]` — only counts if `ref` matches a real
# definition (checked against REF_DEF_RE's output below); otherwise a
# `[bracketed]` phrase is just prose (e.g. `[FAILED]` in a log excerpt),
# per CommonMark. The lookahead excludes the label half of `[text](url)`
# and `[text][ref]`/`[text][]`, which are always immediately followed by
# `(` or `[` — so this never double-matches those.
SHORTCUT_RE = re.compile(r"\[([^\]]+)\](?!\(|\[)")
# Reference definition line: `[ref]: target "optional title"`, optionally
# indented up to 3 spaces, optionally angle-bracketed target.
REF_DEF_RE = re.compile(
    r'(?m)^[ \t]{0,3}\[([^\]]+)\]:[ \t]*<?([^\s>]+)>?'
    r'(?:[ \t]+(?:"[^"]*"|\'[^\']*\'|\([^)]*\)))?[ \t]*$'
)
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*$", re.MULTILINE)
FENCE_RE = re.compile(r"^```")


def slugify(heading: str) -> str:
    # GitHub heading-slug algorithm: lowercase, drop anything but word
    # chars/spaces/hyphens, then replace each remaining space with a
    # hyphen ONE FOR ONE — runs of spaces are NOT collapsed first. A
    # dropped character (e.g. the em dash in "runtime — file") leaves its
    # surrounding spaces behind, which is why "runtime — file" slugs to
    # "runtime--file" (double hyphen) rather than "runtime-file". Collapsing
    # whitespace before the space->hyphen pass — the obvious-looking
    # shortcut — produces single hyphens and reports every such heading's
    # anchor as broken; this repo's docs contain dozens of them (em dashes,
    # "/" as in "Signal / approval gates").
    s = heading.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = s.replace(" ", "-")
    return s


def corpus_files():
    files = sorted(REPO_ROOT.glob("docs/**/*.md"))
    for name in TOP_LEVEL_SOURCES:
        p = REPO_ROOT / name
        if p.exists():
            files.append(p)
    return files


def headings_of(path: Path) -> set:
    text = path.read_text(encoding="utf-8", errors="replace")
    # Drop fenced code blocks so a `#` comment inside a snippet isn't read
    # as a heading.
    lines = text.splitlines()
    out, in_fence = [], False
    for line in lines:
        if FENCE_RE.match(line.strip()):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    body = "\n".join(out)
    slugs = set()
    seen = {}
    for _, title in HEADING_RE.findall(body):
        base = slugify(title)
        n = seen.get(base, 0)
        slugs.add(base if n == 0 else f"{base}-{n}")
        seen[base] = n + 1
    return slugs


CODE_SPAN_RE = re.compile(r"(`+).*?\1")


def strip_code_spans_and_fences(text: str) -> str:
    lines = text.splitlines()
    out, in_fence = [], False
    for line in lines:
        if FENCE_RE.match(line.strip()):
            in_fence = not in_fence
            out.append("")
            continue
        if in_fence:
            out.append("")
            continue
        # Inline code spans (`` `code` ``) commonly hold regex character
        # classes, JSON paths, or Rust slice syntax — e.g. `[A-Za-z0-9_-]`
        # or `["execution"]["id"]` — which look exactly like a markdown
        # reference-style link (`[a][b]`) to a regex with no concept of
        # code spans. Strip these BEFORE the link/reference regexes run, or
        # every such snippet in the corpus reports as an "undefined
        # reference" false positive (found in review: docs/shipped-work.md
        # has several).
        out.append(CODE_SPAN_RE.sub("", line))
    return "\n".join(out)


def is_external_or_special(target: str) -> bool:
    return (
        target.startswith("http://")
        or target.startswith("https://")
        or target.startswith("mailto:")
        or target.startswith("data:")
    )


def extract_link_targets(text: str):
    """Return (targets, undefined_refs) for one file's (fence-stripped) text.

    targets: raw link-target strings, from inline links, reference-style
    links, and shortcut references, each resolved to its actual URL/path.
    undefined_refs: raw `[text][ref]` / `[ref]` strings whose reference was
    never defined in this file — a broken link in its own right (rung of
    "accuracy", same as a dead file path).
    """
    defs = {}

    def collect_def(m):
        defs[m.group(1).strip().lower()] = m.group(2)
        return ""  # remove the definition line so it can't also match as prose

    text = REF_DEF_RE.sub(collect_def, text)

    targets = []
    undefined_refs = []
    consumed = []  # spans already claimed by inline/reference-style matches

    for m in LINK_RE.finditer(text):
        targets.append(m.group(1))
        consumed.append(m.span())

    for m in REF_USE_RE.finditer(text):
        label, ref = m.group(1), m.group(2)
        key = (ref if ref else label).strip().lower()
        consumed.append(m.span())
        if key in defs:
            targets.append(defs[key])
        else:
            undefined_refs.append(m.group(0))

    def overlaps(span):
        s, e = span
        return any(cs < e and s < ce for cs, ce in consumed)

    for m in SHORTCUT_RE.finditer(text):
        if overlaps(m.span()):
            continue
        key = m.group(1).strip().lower()
        if key in defs:
            targets.append(defs[key])
            consumed.append(m.span())
        # else: not a defined reference, so per CommonMark it's plain
        # bracketed prose, not a link — not reported.

    return targets, undefined_refs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    files = corpus_files()
    file_set = set(files)
    heading_cache = {}

    def headings(p: Path) -> set:
        if p not in heading_cache:
            heading_cache[p] = headings_of(p)
        return heading_cache[p]

    broken = []  # (source, raw_target, reason)
    inbound = {p: 0 for p in files}

    for src in files:
        text = strip_code_spans_and_fences(
            src.read_text(encoding="utf-8", errors="replace")
        )
        link_targets, undefined_refs = extract_link_targets(text)

        for raw_ref in undefined_refs:
            broken.append((src, raw_ref, "undefined reference"))

        for target in link_targets:
            if is_external_or_special(target):
                continue
            path_part, _, anchor = target.partition("#")

            if path_part == "":
                # Same-file anchor, e.g. [x](#some-heading)
                resolved = src
            else:
                resolved = (src.parent / path_part).resolve()

            if path_part != "":
                if not resolved.exists():
                    broken.append((src, target, "missing file"))
                    continue
                if resolved in file_set:
                    inbound[resolved] = inbound.get(resolved, 0) + 1

            if anchor:
                target_for_headings = resolved if resolved.exists() else None
                if target_for_headings is None:
                    continue  # already reported as missing file
                if anchor not in headings(target_for_headings):
                    broken.append((src, target, "missing anchor"))

    def rel_posix(p: Path) -> str:
        return p.relative_to(REPO_ROOT).as_posix()

    def is_process_artifact(p: Path) -> bool:
        return rel_posix(p).startswith(PROCESS_ARTIFACT_PREFIXES)

    orphans = [
        p
        for p in files
        if p.is_relative_to(REPO_ROOT / "docs")
        and inbound.get(p, 0) == 0
        and not is_process_artifact(p)
    ]

    broken_corpus = [(s, t, r) for s, t, r in broken if not is_process_artifact(s)]
    broken_process = [(s, t, r) for s, t, r in broken if is_process_artifact(s)]

    def rel(p: Path) -> str:
        return str(p.relative_to(REPO_ROOT))

    if args.json:
        print(
            json.dumps(
                {
                    "files_scanned": len(files),
                    "broken_links_corpus": [
                        {"source": rel(s), "target": t, "reason": r}
                        for s, t, r in broken_corpus
                    ],
                    "broken_links_process_artifacts": [
                        {"source": rel(s), "target": t, "reason": r}
                        for s, t, r in broken_process
                    ],
                    "orphans": [rel(p) for p in orphans],
                },
                indent=2,
            )
        )
    else:
        print(f"Folio corpus link check — {len(files)} files scanned\n")
        print(f"Broken links (corpus, fails CI): {len(broken_corpus)}")
        for s, t, r in broken_corpus:
            print(f"  {rel(s)}: ({t}) — {r}")
        print(
            f"\nBroken links (process artifacts — plans/changelog.d/rnd/assays/"
            f"perf-artifacts, reported only): {len(broken_process)}"
        )
        for s, t, r in broken_process:
            print(f"  {rel(s)}: ({t}) — {r}")
        print(f"\nOrphan pages (0 inbound links): {len(orphans)}")
        for p in orphans:
            print(f"  {rel(p)}")

    return 1 if broken_corpus else 0


if __name__ == "__main__":
    sys.exit(main())
