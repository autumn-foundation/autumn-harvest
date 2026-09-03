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

KNOWN LIMITATIONS:

- A 4-space-indented, non-fenced CommonMark code block is not excluded from
  link/heading extraction — only fenced (``` or ~~~) blocks and inline code
  spans are. A correct fix needs container-relative indentation tracking (4
  spaces means "code block" only when it is NOT a list-item continuation,
  which this corpus uses constantly — e.g. docs/performance.md:1164 is a
  real, working link at 4-space indent inside a `*` list item). A naive
  "blank every 4-space-indented line" fix would silently stop checking
  links like that one. Left unfixed because no genuine top-level indented
  code block containing link- or heading-looking text currently exists in
  the corpus (checked) — the risk of the naive fix outweighs a gap with
  zero current impact.

- `slugify()` operates on a heading's raw Markdown source, not its
  GitHub-RENDERED text. This is invisible for the markup this corpus
  actually puts in headings (`**bold**`, `` `code` ``, emoji — all pass
  through unchanged or get correctly stripped as punctuation either way),
  but a heading that is itself a link — `## [Guide](guide.md)`, which
  GitHub renders and slugs as just "Guide" — would slug from the raw
  `[Guide](guide.md)` text instead, producing garbage. Properly rendering
  arbitrary inline Markdown (links, images, entities) to plain text before
  slugging is a materially bigger piece of work than the regex-level fixes
  above — effectively a small inline-Markdown-to-text renderer — and no
  heading anywhere in the corpus is currently written as a link (checked),
  so this is left unfixed rather than half-implemented.

- A fenced code block nested inside a blockquote (each line prefixed with
  `>`, e.g. `> \`\`\`bash`) is not recognized as a fence, because
  FENCE_OPEN_RE looks for the delimiter after up to 3 spaces, not after a
  blockquote marker. This corpus does this at least once
  (docs/runbooks/triage-pending-tasks-idle-workers.md's `harvest workflow
  diagnose` example), but correctly: nothing inside that specific block
  looks like a link or heading, so it causes no current false result. A
  correct fix needs to track the active blockquote-prefix depth alongside
  fence state (open/body/close must share the same `>` nesting) — real
  scope, not a regex tweak, for a gap this corpus doesn't currently trip
  over. Left unfixed for the same reason as the two limitations above.

Usage:
    python3 docs/audits/corpus-link-check.py [--json]

Exit code is 1 if a broken link is found in the reader corpus (orphans are
reported but do not fail the run — an orphan is a findability defect to
triage, not a hard error). A broken link inside a process/working-artifact
subtree (docs/plans/, docs/changelog.d/, docs/rnd/, docs/assays/,
docs/perf-artifacts/ — see PROCESS_ARTIFACT_PREFIXES) is still reported but
does NOT fail the run, UNLESS a reader can actually reach that page by
following crosslinks from the real corpus (transitively) — a page under one
of those prefixes that a corpus page cites as required reading (e.g.
docs/rnd/determinism-static-analysis.md, cited from docs/harvest-verify.md)
is graded as corpus, not exempted, because a reader really does land there.
Run from anywhere; paths are resolved relative to the repo root.
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

# Inline link `[label](dest)`. `label` allows ONE level of nested `[...]`
# (bare, no further nesting) so a linked image `[![alt](src)](dest)` — this
# corpus's badge-link idiom, e.g. the license badge in README.md — matches
# as a single link whose captured target is the OUTER `dest`, not the inner
# image `src`. Without this, regex non-overlap means the inner `![alt](src)`
# consumes the match and `dest` — the thing a reader actually navigates
# to — is never examined (found in review: README.md's `#license` badge
# link would go unchecked).
_LABEL = r"(?:[^\[\]]|\[[^\[\]]*\])*"
# CommonMark's optional link/image title accepts three delimiter forms —
# "double", 'single', or (parenthesized) — not just double quotes. A
# double-quote-only pattern fails to match the WHOLE link on a single- or
# paren-quoted title (the destination capture needs the trailing `)` right
# after it, which isn't there), so a link written `[x](missing.md 'title')`
# would be entirely invisible to this checker — silently exempt from ever
# being reported broken. Same three-form set REF_DEF_RE already used below.
_TITLE = r'(?:"[^"]*"|\'[^\']*\'|\([^)]*\))'
# CommonMark also allows an angle-bracketed destination — `[x](<a b.md>)` —
# specifically so a path containing a space can be written at all (the bare
# form otherwise stops at the first space). Captured as one alternative
# inside the same group; `unwrap_angle_dest` strips the brackets afterward.
# Without this, `<management-api.md>` (brackets included) is what gets
# resolved against the filesystem, which never exists — a real file falsely
# reported missing — and a bracketed destination containing a space isn't
# matched by the bare form at all, so it's silently skipped.
#
# The bare form itself allows one level of BALANCED parens (same nested-once
# shape as `_LABEL`'s bracket handling above) — CommonMark permits a literal
# `(`/`)` pair inside an unbracketed destination, e.g. `[x](guide(v2).md)`.
# Stopping at the first `)` (the obvious-looking `[^)\s]+`) truncates that
# to `guide(v2` and reports a real file as missing.
_DEST = r"(<[^<>]*>|(?:[^()\s]|\([^()]*\))+)"
# `(?<!\\)` before the opening `[`: CommonMark renders `\[not a link](x)` as
# literal text (the backslash escapes the bracket), not a link — relevant
# for prose that demonstrates Markdown syntax rather than using it. Doesn't
# attempt full odd/even backslash-run accounting (`\\[real link]`, an
# escaped backslash followed by a real link, would be misread) — that's a
# corpus that doesn't exist here; this handles the actual pattern in play.
LINK_RE = re.compile(rf"(?<!\\)\[{_LABEL}\]\({_DEST}(?:\s+{_TITLE})?\)")
# Bare image `![alt](src)`, checked independently so a *local* image's `src`
# still gets a missing-file check even when the image is ALSO wrapped in an
# outer link (and so consumed into LINK_RE's label, per above).
IMAGE_RE = re.compile(rf"(?<!\\)!\[[^\]]*\]\({_DEST}(?:\s+{_TITLE})?\)")


def unwrap_angle_dest(target: str) -> str:
    if target.startswith("<") and target.endswith(">"):
        return target[1:-1]
    return target


def normalize_ref_label(label: str) -> str:
    # CommonMark reference-label matching case-folds AND collapses runs of
    # internal whitespace to one space, not just leading/trailing — so
    # `[x][foo bar]` matches a definition written `[foo  bar]: ...` (or
    # split across a line wrap). Comparing with only `.strip().lower()`
    # treats those as different keys and reports a real, defined reference
    # as "undefined".
    return re.sub(r"\s+", " ", label.strip()).lower()
# Reference-style: `[text][ref]` / collapsed `[text][]` (ref == text).
# `(?<!\\)` guards the opener the same way LINK_RE does — `\[literal](x)`
# style escaping applies here too.
REF_USE_RE = re.compile(r"(?<!\\)\[([^\]]*)\]\[([^\]]*)\]")
# Bare shortcut reference `[ref]` — only counts if `ref` matches a real
# definition (checked against REF_DEF_RE's output below); otherwise a
# `[bracketed]` phrase is just prose (e.g. `[FAILED]` in a log excerpt),
# per CommonMark. The lookahead excludes the label half of `[text](url)`
# and `[text][ref]`/`[text][]`, which are always immediately followed by
# `(` or `[` — so this never double-matches those.
SHORTCUT_RE = re.compile(r"(?<!\\)\[([^\]]+)\](?!\(|\[)")
# Reference definition line: `[ref]: target "optional title"`, optionally
# indented up to 3 spaces. Target is either bare (no whitespace) or
# angle-bracketed — the latter is CommonMark's only way to put a space in a
# reference-definition target (`[ref]: <a b.md>`), so the destination
# alternation must accept it (same shape as `_DEST` above); `unwrap_angle_dest`
# strips the brackets once captured, same as for inline links.
REF_DEF_RE = re.compile(
    r"(?m)^[ \t]{0,3}\[([^\]]+)\]:[ \t]*(<[^<>]*>|\S+)"
    r'(?:[ \t]+(?:"[^"]*"|\'[^\']*\'|\([^)]*\)))?[ \t]*$'
)
# CommonMark allows up to 3 leading spaces on an ATX heading (4+ makes it an
# indented code block instead) — including headings nested inside a list
# item, which is exactly how this corpus uses them (docs/shipped-work.md has
# 167 of them, e.g. "  ### What shipped" under a `- **Phase N**` bullet).
# Requiring column 0 misses all of those and reports every link into one as
# a "missing anchor" false positive.
HEADING_RE = re.compile(r"^[ \t]{0,3}(#{1,6})\s+(.+?)\s*#*$", re.MULTILINE)
# Setext headings: a non-blank text line immediately followed (no blank
# line between) by a line of only `=` (H1) or 2+ `-` (H2) — CommonMark's
# other heading syntax, e.g.
#     My title
#     ========
# Excludes a text line that looks like a list item, table row, or ATX
# heading itself, to avoid misreading an ordinary `---` divider or table
# separator as a heading underline for unrelated prose above it. The
# underline itself may carry up to 3 leading spaces, same CommonMark
# allowance as an ATX `#` (HEADING_RE above) or a fence delimiter.
SETEXT_RE = re.compile(
    r"(?m)^(?!\s*$)(?!#)(?!\s*[-*+]\s)(?!\s*\|)([^\n]+)\n[ \t]{0,3}(?:=+|-{2,})[ \t]*$"
)
# CommonMark fenced code blocks may open with EITHER 3+ backticks OR 3+
# tildes, not just backticks — and the block only closes on a run of the
# SAME character at least as long as the opener (so a ``` example quoted
# inside a longer ```` fence, or a backtick fence nested inside a tilde
# one, doesn't false-close the block early). `mask_fenced_lines` tracks
# both the character and the run length across the whole file, not just a
# same/different toggle.
FENCE_OPEN_RE = re.compile(r"^[ \t]{0,3}(`{3,}|~{3,})")


def mask_fenced_lines(lines):
    """Blank every line of every fenced code block (open, body, close)."""
    out = []
    fence_char = None
    fence_len = 0
    for line in lines:
        if fence_char is None:
            m = FENCE_OPEN_RE.match(line)
            if m:
                fence_char, fence_len = m.group(1)[0], len(m.group(1))
                out.append("")
                continue
            out.append(line)
            continue
        if re.match(
            rf"^[ \t]{{0,3}}{re.escape(fence_char)}{{{fence_len},}}[ \t]*$", line
        ):
            fence_char, fence_len = None, 0
        out.append("")
    return out


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
    # Blank out fenced code blocks so a `#` comment (or a divider line that
    # would misread as a Setext underline) inside a snippet isn't read as a
    # heading. Blanked, not dropped: dropping the lines entirely would pull
    # a paragraph and a later `===`/`---` into false adjacency across a
    # removed fence, misdetecting a Setext heading that was never there.
    body = "\n".join(mask_fenced_lines(text.splitlines()))

    # ATX and Setext headings interleaved, in document order — duplicate-
    # slug numbering (the `-1`, `-2` suffix GitHub appends) depends on the
    # order headings actually appear in, not on which syntax found them.
    matches = [(m.start(), m.group(2)) for m in HEADING_RE.finditer(body)]
    matches += [(m.start(), m.group(1).strip()) for m in SETEXT_RE.finditer(body)]
    matches.sort(key=lambda pair: pair[0])

    slugs = set()
    seen = {}
    for _, title in matches:
        base = slugify(title)
        n = seen.get(base, 0)
        slugs.add(base if n == 0 else f"{base}-{n}")
        seen[base] = n + 1
    return slugs


# A code span's closer must be a run of EXACTLY as many backticks as the
# opener — not merely contain that many. `` `[x](y)`` `` (one backtick,
# then later a two-backtick run) is not a code span at all under
# CommonMark, because no run of exactly one backtick closes it; the naive
# `(`+).*?\1` backreference happily matches `\1` (one backtick) as a
# substring of the LONGER two-backtick run, incorrectly turning a real
# link into "code" that never gets checked — a link could go broken with
# this gate reporting green. The `(?!`)` after the opening capture locks it
# to its maximal length (blocks backtracking to a shorter opener); the
# `(?<!`)`/`(?!`)` around the closer's `\1` require it to be its own
# maximal run, not a prefix or suffix of a longer one.
CODE_SPAN_RE = re.compile(r"(`+)(?!`).*?(?<!`)\1(?!`)")


def strip_code_spans_and_fences(text: str) -> str:
    # Inline code spans (`` `code` ``) commonly hold regex character
    # classes, JSON paths, or Rust slice syntax — e.g. `[A-Za-z0-9_-]`
    # or `["execution"]["id"]` — which look exactly like a markdown
    # reference-style link (`[a][b]`) to a regex with no concept of
    # code spans. Strip these BEFORE the link/reference regexes run, or
    # every such snippet in the corpus reports as an "undefined
    # reference" false positive (found in review: docs/shipped-work.md
    # has several).
    lines = mask_fenced_lines(text.splitlines())
    return "\n".join(CODE_SPAN_RE.sub("", line) for line in lines)


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
        defs[normalize_ref_label(m.group(1))] = unwrap_angle_dest(m.group(2))
        return ""  # remove the definition line so it can't also match as prose

    text = REF_DEF_RE.sub(collect_def, text)

    targets = []
    undefined_refs = []
    consumed = []  # spans already claimed by inline/reference-style matches

    for m in LINK_RE.finditer(text):
        targets.append(unwrap_angle_dest(m.group(1)))
        consumed.append(m.span())

    # Independent of the spans above: a *local* image's own `src` should
    # still get a missing-file check even when the image sits inside an
    # outer link's label (and so was consumed into that link's single
    # match above, per LINK_RE's docstring) — e.g. a linked local
    # screenshot. No anchor is possible on an image target, so this only
    # ever contributes a "missing file" check, never "missing anchor".
    for m in IMAGE_RE.finditer(text):
        targets.append(unwrap_angle_dest(m.group(1)))

    for m in REF_USE_RE.finditer(text):
        label, ref = m.group(1), m.group(2)
        key = normalize_ref_label(ref if ref else label)
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
        key = normalize_ref_label(m.group(1))
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
    top_level_sources = {
        REPO_ROOT / name for name in TOP_LEVEL_SOURCES if (REPO_ROOT / name).exists()
    }
    heading_cache = {}

    def headings(p: Path) -> set:
        if p not in heading_cache:
            heading_cache[p] = headings_of(p)
        return heading_cache[p]

    broken = []  # (source, raw_target, reason)
    inbound_sources = {p: set() for p in files}  # target -> {source pages}

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
                if resolved.is_dir():
                    # A link to a directory (e.g. `dashboards/`) renders on
                    # GitHub as that directory's README — credit the README
                    # for inbound-link purposes, or a page reachable only
                    # by a directory link false-reports as an orphan (found
                    # in review: docs/dashboards/README.md, linked from
                    # docs/migrating-from-temporal.md as `dashboards/`).
                    readme = resolved / "README.md"
                    if readme in file_set:
                        resolved = readme
                if resolved in file_set:
                    inbound_sources.setdefault(resolved, set()).add(src)

            if anchor:
                target_for_headings = resolved if resolved.exists() else None
                if target_for_headings is None:
                    continue  # already reported as missing file
                if anchor not in headings(target_for_headings):
                    broken.append((src, target, "missing anchor"))

    def rel_posix(p: Path) -> str:
        return p.relative_to(REPO_ROOT).as_posix()

    def under_process_prefix(p: Path) -> bool:
        return rel_posix(p).startswith(PROCESS_ARTIFACT_PREFIXES)

    # A page under a process-artifact prefix is graded as "process" (exempt
    # from CI-blocking, exempt from orphan grading) ONLY if a reader
    # actually has no path to it via a real markdown LINK from the corpus.
    # Being under docs/plans/ or docs/rnd/ is not enough on its own: review
    # found a counter-example the blanket prefix-only rule got wrong —
    # docs/rnd/determinism-static-analysis.md is required reading, actually
    # hyperlinked (with anchors) repeatedly from docs/harvest-verify.md and
    # docs/workflow-determinism-guide.md, not just mentioned in prose; the
    # same is true of docs/rnd/sqlite-feasibility.md from
    # docs/sqlite-backend.md. Both are corpus pages sending readers into a
    # "process" subtree on purpose, so the target is corpus too — and so is
    # anything THAT page in turn links to (transitive: a reader keeps
    # following crosslinks — this is also why docs/rnd/wasm-activities-spike.md
    # is reclassified, one hop further out, via docs/shipped-work.md).
    # `docs/assays/0001-redis-adapter-throughput-ceiling.md` and
    # `docs/plans/2026-09-01-e2e-benchmark-suite.md`, by contrast, are only
    # ever CITED as plain backtick-quoted paths in prose (docs/
    # autumn-workflow-architecture.md, docs/benchmarks.md) — not a real
    # `[...](...)` a reader can click — so they correctly stay exempt; that
    # citation-without-a-link gap is itself a findability defect, just a
    # different one than this harness fixes here. Computed as a
    # reachability closure seeded from every non-process page, rather than
    # hand-maintaining an exception list that silently goes stale the next
    # time someone adds or removes a crosslink.
    corpus_reachable = {
        p
        for p in files
        if (p.is_relative_to(REPO_ROOT / "docs") or p in top_level_sources)
        and not under_process_prefix(p)
    }
    worklist = list(corpus_reachable)
    while worklist:
        cur = worklist.pop()
        for target, sources in inbound_sources.items():
            if cur in sources and target not in corpus_reachable:
                corpus_reachable.add(target)
                worklist.append(target)

    def is_process_artifact(p: Path) -> bool:
        return under_process_prefix(p) and p not in corpus_reachable

    orphans = [
        p
        for p in files
        if p.is_relative_to(REPO_ROOT / "docs")
        and len(inbound_sources.get(p, ())) == 0
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
