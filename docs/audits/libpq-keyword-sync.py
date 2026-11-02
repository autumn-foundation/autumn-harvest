#!/usr/bin/env python3
"""Keep the libpq-keyword-list clone class in step (issue #2521).

`autumn-harvest-cli/src/lib.rs` and `autumn-harvest-plugin/src/dev/dsn.rs`
each define `is_connection_keyword(key: &str) -> bool`, with a `const
KEYWORDS: &[&str]` listing the 46 libpq connection-string parameter names
and the same case-insensitive lookup over it. Issue #1322 found that the
`dev::dsn` copy was missing the CLI's existing guard; PR #1601 copied the
function (KEYWORDS list included) across to close it. The two copies are
below the rule-of-three merge bar, and a shared home for them would be a
public-API change to the `autumn-harvest` core crate that needs a human
decision (see issue #2521), so for now they stay linked, not merged.

This script extracts each function's body by brace matching. It fails in
four cases:
- The bodies differ once the one allowed difference (the `pub(super) `
  visibility prefix and the wording of line comments) is normalized away.
- A copy is missing, not unique or unclosed.
- A body has a block comment or a raw string. The brace matcher cannot read
  them, so the script refuses them.
- The sibling comment that names this issue is gone.

Usage:
    python3 docs/audits/libpq-keyword-sync.py
    python3 docs/audits/libpq-keyword-sync.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ISSUE = "#2521"
ISSUE_RE = re.compile(re.escape(ISSUE) + r"\b")
FN_NAME = "is_connection_keyword"
SIGNATURE = re.compile(
    r"^(?:pub\(super\) )?fn " + FN_NAME + r"\(key: &str\) -> bool \{", re.M
)
# A char literal, a string literal or a line comment. Braces inside these
# do not count toward brace matching.
SKIP = re.compile(r"'(?:\\.|[^\\'])'|\"(?:\\.|[^\\\"])*\"|//[^\n]*")
# Constructs that SKIP cannot read. A body with one fails closed.
UNSUPPORTED = re.compile(r"/\*|\br#*\"")
# A line comment's text is the one place the two copies are allowed to
# differ in wording; collapse each to a fixed marker before comparing.
LINE_COMMENT = re.compile(r"//[^\n]*")

TARGETS = (
    "autumn-harvest-cli/src/lib.rs",
    "autumn-harvest-plugin/src/dev/dsn.rs",
)


def find_fn(source):
    """Return (start, body) of the fn, from `fn`/`pub(super) fn` to its
    closing brace.

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


def normalize(body):
    """Collapse the one allowed difference so the rest compares exact."""
    body = body.replace("pub(super) fn", "fn", 1)
    return LINE_COMMENT.sub("// <comment>", body)


def has_sibling_comment(source, start, counterpart):
    """True when the doc comment just above `start` names this issue.

    The block must also name the `counterpart` file.
    """
    comment = []
    for line in reversed(source[:start].splitlines()):
        if not line.startswith("///") and not line.startswith("//!"):
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
        bodies[path] = normalize(body)
        counterpart = next(Path(t).name for t in TARGETS if t != path)
        if not has_sibling_comment(source, start, counterpart):
            found.append(
                f"{path}: the doc comment above fn {FN_NAME} must name "
                f"issue {ISSUE} and {counterpart}."
            )
    if len(set(bodies.values())) > 1:
        found.append(
            f"{' and '.join(bodies)} differ beyond the visibility prefix and "
            f"comment wording. Change both the same way, or merge them "
            f"(issue {ISSUE})."
        )
    return found


def run():
    sources = {rel: (ROOT / rel).read_text(encoding="utf-8") for rel in TARGETS}
    found = findings(sources)
    for message in found:
        print(message)
    print(f"libpq-keyword-sync: {len(found)} findings")
    return 1 if found else 0


def self_test():
    doc_a = (
        "/// Sibling of `dsn.rs` (#2521).\n"
    )
    body_a = (
        "fn is_connection_keyword(key: &str) -> bool {\n"
        "    const KEYWORDS: &[&str] = &[\"host\", \"port\"];\n"
        "    // comment A wording\n"
        "    KEYWORDS.iter().any(|k| key.eq_ignore_ascii_case(k))\n"
        "}\n"
    )
    doc_b = (
        "/// Sibling of `lib.rs` (#2521).\n"
    )
    body_b = (
        "pub(super) fn is_connection_keyword(key: &str) -> bool {\n"
        "    const KEYWORDS: &[&str] = &[\"host\", \"port\"];\n"
        "    // comment B, worded differently\n"
        "    KEYWORDS.iter().any(|k| key.eq_ignore_ascii_case(k))\n"
        "}\n"
    )
    good_a = doc_a + body_a + "\nfn other() {}\n"
    good_b = doc_b + body_b + "\nfn other() {}\n"

    start_a, found_a = find_fn(good_a)
    start_b, found_b = find_fn(good_b)
    assert found_a == body_a.rstrip("\n"), found_a
    assert found_b == body_b.rstrip("\n"), found_b
    assert has_sibling_comment(good_a, start_a, "dsn.rs")
    assert has_sibling_comment(good_b, start_b, "lib.rs")
    assert not has_sibling_comment(good_a, start_a, "third.rs")

    assert normalize(found_a) == normalize(found_b), (
        normalize(found_a),
        normalize(found_b),
    )

    assert find_fn("fn other() {}\n") is None
    assert find_fn(body_a + "\n" + body_a) is None
    assert find_fn(body_a[: body_a.rindex("}")]) is None

    for text in (
        body_a,
        "/// dsn.rs #25211.\n" + body_a,
        "/// dsn.rs #1.\n" + body_a,
    ):
        start = find_fn(text)[0]
        assert not has_sibling_comment(text, start, "dsn.rs"), text

    sources = {TARGETS[0]: good_a, TARGETS[1]: good_b}
    assert findings(sources) == [], findings(sources)

    drifted_b = good_b.replace('&["host", "port"]', '&["host"]')
    drifted = {TARGETS[0]: good_a, TARGETS[1]: drifted_b}
    assert any("differ" in m for m in findings(drifted)), findings(drifted)

    gone = {TARGETS[0]: good_a, TARGETS[1]: body_b + "\nfn other() {}\n"}
    assert any("must name" in m for m in findings(gone)), findings(gone)

    absent = {TARGETS[0]: good_a, TARGETS[1]: "fn other() {}\n"}
    assert any("missing" in m for m in findings(absent)), findings(absent)

    for risky in ("/* } */", "/* /* } */ */", 'let s = r#"a"}"#;'):
        text = good_a.replace(
            "KEYWORDS.iter()", f"{risky}\n    KEYWORDS.iter()"
        )
        refused = findings({TARGETS[0]: text, TARGETS[1]: good_b})
        assert any("raw string" in m for m in refused), (risky, refused)

    print("libpq-keyword-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
