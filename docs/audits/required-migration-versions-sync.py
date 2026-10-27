#!/usr/bin/env python3
"""Keep the required-migration-versions clone class in step (issue #2358).

`autumn-harvest-plugin/src/preflight.rs` and
`autumn-harvest-plugin/src/shard_health.rs` each define
`required_migration_versions()`. The two bodies are byte-identical. Two
copies are below the rule-of-three merge bar, so they stay linked, not merged.

This script extracts each body by brace matching. It does no normalization.
It fails in four cases:
- The bodies differ.
- A copy is missing, not unique or unclosed.
- A body has a block comment or a raw string. The brace matcher cannot read
  them, so the script refuses them.
- The sibling comment that names this issue is gone.

Usage:
    python3 docs/audits/required-migration-versions-sync.py
    python3 docs/audits/required-migration-versions-sync.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ISSUE = "#2358"
ISSUE_RE = re.compile(re.escape(ISSUE) + r"\b")
FN_NAME = "required_migration_versions"
SIGNATURE = re.compile(
    r"^fn " + FN_NAME + r"\(\) -> Result<Vec<String>, String> \{", re.M
)
# A char literal, a string literal or a line comment. Braces inside these
# do not count toward brace matching.
SKIP = re.compile(r"'(?:\\.|[^\\'])'|\"(?:\\.|[^\\\"])*\"|//[^\n]*")
# Constructs that SKIP cannot read. A body with one fails closed.
UNSUPPORTED = re.compile(r"/\*|\br#*\"")

TARGETS = (
    "autumn-harvest-plugin/src/preflight.rs",
    "autumn-harvest-plugin/src/shard_health.rs",
)


def find_fn(source):
    """Return (start, body) of the fn, from `fn` to its closing brace.

    Return None when the function is missing, is not unique, or has no
    closing brace.
    """
    matches = list(SIGNATURE.finditer(source))
    if len(matches) != 1:
        return None
    start = matches[0].start()
    pos = matches[0].end()
    depth = 1
    while depth > 0:
        skipped = SKIP.match(source, pos)
        if skipped:
            pos = skipped.end()
            continue
        if pos >= len(source):
            return None
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    return start, source[start:pos]


def has_sibling_comment(source, start, counterpart):
    """True when the comment block just above `start` names this issue.

    The block must also name the `counterpart` file.
    """
    comment = []
    for line in reversed(source[:start].splitlines()):
        if not line.startswith("//"):
            break
        comment.append(line)
    text = "\n".join(comment)
    return ISSUE_RE.search(text) is not None and counterpart in text


def findings(sources):
    """`sources` maps a path to its text. Return a list of messages."""
    found = []
    bodies = {}
    for path, source in sources.items():
        located = find_fn(source)
        if located is None:
            found.append(f"{path}: fn {FN_NAME} is missing, not unique or unclosed.")
            continue
        start, body = located
        if UNSUPPORTED.search(body):
            found.append(
                f"{path}: fn {FN_NAME} has a block comment or a raw string. "
                "Use line comments and plain strings."
            )
            continue
        bodies[path] = body
        counterpart = next(Path(t).name for t in TARGETS if t != path)
        if not has_sibling_comment(source, start, counterpart):
            found.append(
                f"{path}: the comment above fn {FN_NAME} must name "
                f"issue {ISSUE} and {counterpart}."
            )
    if len(set(bodies.values())) > 1:
        found.append(
            f"{' and '.join(bodies)} differ. "
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
        "}"
    )
    doc = "/// Sibling of `other.rs`, `preflight.rs` and `shard_health.rs` (#2358).\n"
    good = doc + body + "\n\nfn other() {}\n"

    # A brace in a string, char or comment does not end the body early.
    start, found_body = find_fn(good)
    assert found_body == body, found_body
    assert has_sibling_comment(good, start, "other.rs")
    assert not has_sibling_comment(good, start, "third.rs")

    # Missing, duplicated and unclosed copies yield no body.
    assert find_fn("fn other() {}\n") is None
    assert find_fn(body + "\n" + body) is None
    assert find_fn(body[: body.rindex("}")]) is None

    # The sibling comment must name this issue and must touch the fn.
    for text in (
        body,
        doc + "\n" + body,
        "/// other.rs #23581.\n" + body,
        "/// other.rs #1.\n" + body,
    ):
        assert not has_sibling_comment(text, find_fn(text)[0], "other.rs"), text

    sources = {TARGETS[0]: good, TARGETS[1]: good}
    assert findings(sources) == [], findings(sources)

    drifted = {TARGETS[0]: good, TARGETS[1]: good.replace("'_'", "'-'")}
    assert any("differ" in m for m in findings(drifted)), findings(drifted)

    gone = {TARGETS[0]: good, TARGETS[1]: body}
    assert any("must name" in m for m in findings(gone)), findings(gone)

    absent = {TARGETS[0]: good, TARGETS[1]: "fn other() {}\n"}
    assert any("missing" in m for m in findings(absent)), findings(absent)

    # Constructs the brace matcher cannot read fail closed.
    for risky in ("/* } */", "/* /* } */ */", 'let s = r#"a"}"#;'):
        text = good.replace("Ok(vec![])", f"{risky}\n    Ok(vec![])")
        refused = findings({TARGETS[0]: text, TARGETS[1]: good})
        assert any("raw string" in m for m in refused), (risky, refused)

    print("required-migration-versions-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
