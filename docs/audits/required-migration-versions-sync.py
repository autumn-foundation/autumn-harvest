#!/usr/bin/env python3
"""Keep the required-migration-versions clone class in step (issue #2358).

`autumn-harvest-plugin/src/preflight.rs` and
`autumn-harvest-plugin/src/shard_health.rs` each define
`required_migration_versions()`. The two bodies are byte-identical. Two
copies are below the rule-of-three merge bar, so they stay linked, not merged.

This script extracts each body by brace matching. It fails when the bodies
differ, when a copy is missing or not unique, or when the sibling comment
that names this issue is gone. It does no normalization.

Usage:
    python3 docs/audits/required-migration-versions-sync.py
    python3 docs/audits/required-migration-versions-sync.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ISSUE = "#2358"
FN_NAME = "required_migration_versions"
SIGNATURE = re.compile(
    r"^fn " + FN_NAME + r"\(\) -> Result<Vec<String>, String> \{", re.M
)
# A char literal, a string literal or a line comment. Braces inside these
# do not count toward brace matching.
SKIP = re.compile(r"'(?:\\.|[^\\'])'|\"(?:\\.|[^\\\"])*\"|//[^\n]*")

TARGETS = (
    "autumn-harvest-plugin/src/preflight.rs",
    "autumn-harvest-plugin/src/shard_health.rs",
)


def extract_fn(source):
    """Return the text of the fn, from `fn` to its closing brace.

    Return None when the function is missing, is not unique, or has no
    closing brace.
    """
    matches = list(SIGNATURE.finditer(source))
    if len(matches) != 1:
        return None
    pos = matches[0].end()
    depth = 1
    while depth > 0:
        skipped = SKIP.match(source, pos)
        if skipped:
            pos = skipped.end()
            continue
        if pos >= len(source):
            return None
        depth += {"{": 1, "}": -1}.get(source[pos], 0)
        pos += 1
    return source[matches[0].start():pos]


def has_sibling_comment(source):
    """True when the doc comment above the fn names this issue."""
    match = SIGNATURE.search(source)
    if match is None:
        return False
    comment = []
    for line in reversed(source[:match.start()].splitlines()):
        if not line.startswith("//"):
            break
        comment.append(line)
    return ISSUE in " ".join(comment)


def findings(sources):
    """`sources` maps a path to its text. Return a list of messages."""
    found = []
    bodies = {}
    for path, source in sources.items():
        body = extract_fn(source)
        if body is None:
            found.append(f"{path}: fn {FN_NAME} is missing, not unique or unclosed.")
            continue
        bodies[path] = body
        if not has_sibling_comment(source):
            found.append(
                f"{path}: the comment above fn {FN_NAME} must name issue {ISSUE}."
            )
    if len(set(bodies.values())) > 1:
        found.append(
            f"{' and '.join(bodies)} have diverged. "
            f"Change both the same way, or merge them (issue {ISSUE})."
        )
    return found


def run():
    sources = {rel: (ROOT / rel).read_text(encoding="utf-8") for rel in TARGETS}
    found = findings(sources)
    for message in found:
        print(message)
    print(f"required-migration-versions-sync: {len(found)} findings")
    return 1 if found else 0


def self_test():
    body = (
        "fn required_migration_versions() -> Result<Vec<String>, String> {\n"
        "    let a = '_';\n"
        "    let b = \"}\"; // }\n"
        "    Ok(vec![])\n"
        "}\n"
    )
    doc = "/// Sibling of the other copy (issue #2358).\n"
    good = doc + body + "\nfn other() {}\n"
    assert extract_fn(good) == body.rstrip("\n"), extract_fn(good)
    assert has_sibling_comment(good)

    # A brace in a string, char or comment does not end the body early.
    assert extract_fn(good).endswith("Ok(vec![])\n}")

    # Missing, duplicated and unclosed copies yield no body.
    assert extract_fn("fn other() {}\n") is None
    assert extract_fn(body + body) is None
    assert extract_fn(body[: body.rindex("}")]) is None

    # The sibling comment is required, and it must be adjacent.
    assert not has_sibling_comment(body)
    assert not has_sibling_comment(doc + "\n" + body)
    assert not has_sibling_comment("/// Other issue #1.\n" + body)

    sources = {"a.rs": good, "b.rs": good}
    assert findings(sources) == [], findings(sources)

    drifted = {"a.rs": good, "b.rs": good.replace("'_'", "'-'")}
    assert any("diverged" in m for m in findings(drifted)), findings(drifted)

    gone = {"a.rs": good, "b.rs": body}
    assert any("must name issue" in m for m in findings(gone)), findings(gone)

    absent = {"a.rs": good, "b.rs": "fn other() {}\n"}
    assert any("missing" in m for m in findings(absent)), findings(absent)

    print("required-migration-versions-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
