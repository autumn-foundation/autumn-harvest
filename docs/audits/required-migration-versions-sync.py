#!/usr/bin/env python3
"""Keep the required_migration_versions clone class in step (issue #2358).

`autumn-harvest-plugin/src/preflight.rs` and
`autumn-harvest-plugin/src/shard_health.rs` each define a private
`required_migration_versions` function that derives the crate's required
migration version list from `autumn_harvest::MIGRATIONS`. The two bodies are
byte-identical today (the second copied the first when `shard_health.rs` was
added — see issue #2358) but fall short of the rule-of-three merge bar, so
they stay linked rather than merged. This script is the substitute for the
abstraction the numerosity gate did not clear: it extracts each function's
full text by brace matching and fails the build the moment the two stop
being byte-identical.

Usage:
    python3 docs/audits/required-migration-versions-sync.py
    python3 docs/audits/required-migration-versions-sync.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TARGETS = (
    "autumn-harvest-plugin/src/preflight.rs",
    "autumn-harvest-plugin/src/shard_health.rs",
)

FN_NAME = "required_migration_versions"


def extract_fn_body(source, fn_name=FN_NAME):
    """Return the named fn's full text (signature through closing brace), by
    brace matching from the signature line. `None` when the function is not
    found.
    """
    match = re.search(
        r"^fn " + re.escape(fn_name) + r"\(\) -> Result<Vec<String>, String> \{",
        source,
        re.M,
    )
    if match is None:
        return None
    i = match.start()
    end = match.end()
    depth = 1
    while depth > 0 and end < len(source):
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    return source[i:end]


def findings():
    bodies = []
    for rel_path in TARGETS:
        path = ROOT / rel_path
        source = path.read_text(encoding="utf-8")
        body = extract_fn_body(source)
        if body is None:
            return [f"{rel_path}: could not find fn {FN_NAME}() -> Result<Vec<String>, String>"]
        bodies.append((rel_path, body))

    (first_path, first_body), (second_path, second_body) = bodies
    if first_body != second_body:
        return [
            f"{first_path} and {second_path} have diverged; update both the "
            "same way or merge them (issue #2358)."
        ]
    return []


def run():
    found = findings()
    for message in found:
        print(message)
    print(f"required-migration-versions-sync: {len(found)} findings")
    return 1 if found else 0


def self_test():
    in_sync_a = (
        "fn required_migration_versions() -> Result<Vec<String>, String> {\n"
        "    Ok(Vec::new())\n"
        "}\n"
    )
    in_sync_b = (
        "fn required_migration_versions() -> Result<Vec<String>, String> {\n"
        "    Ok(Vec::new())\n"
        "}\n"
    )
    body_a = extract_fn_body(in_sync_a)
    body_b = extract_fn_body(in_sync_b)
    assert body_a is not None and body_a.endswith("}"), body_a
    assert body_b is not None and body_b.endswith("}"), body_b
    assert body_a == body_b, (body_a, body_b)

    drifted_b = in_sync_b.replace("Ok(Vec::new())", "Ok(vec![String::new()])")
    body_b2 = extract_fn_body(drifted_b)
    assert body_a != body_b2, "a drifted body must be caught"

    assert extract_fn_body(in_sync_a, "no_such_fn") is None

    print("required-migration-versions-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
