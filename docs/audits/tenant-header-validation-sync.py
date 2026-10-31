#!/usr/bin/env python3
"""Keep the tenant-header-validation clone class in step (issue #2341).

`autumn-harvest-plugin/src/authz.rs`'s `tenant_key` and
`autumn-harvest-plugin/src/tenant.rs`'s `declared_tenant` parse and validate
the `x-harvest-tenant` header the same way: reject a repeated header, trim,
then reject empty or over-length. The two instances are real duplication
(the second copied the first when `tenant.rs` was added — see issue #2341)
but fall short of the rule-of-three merge bar, so they stay linked rather
than merged. This script is the substitute for the abstraction the
numerosity gate did not clear: it extracts each function's body by brace
matching, normalizes the one difference the two files are allowed to have
(the function's own name, and the length constant's spelling — `authz.rs`
reads its local alias `MAX_TENANT_KEY_LEN`, `tenant.rs` reads
`autumn_harvest::tenant::MAX_TENANT_LEN` directly, and the two are the same
value), and fails the build the moment anything else diverges.

Usage:
    python3 docs/audits/tenant-header-validation-sync.py
    python3 docs/audits/tenant-header-validation-sync.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TARGETS = (
    ("autumn-harvest-plugin/src/authz.rs", "tenant_key", "MAX_TENANT_KEY_LEN"),
    (
        "autumn-harvest-plugin/src/tenant.rs",
        "declared_tenant",
        "autumn_harvest::tenant::MAX_TENANT_LEN",
    ),
)

def extract_fn_body(source, fn_name):
    """Return the named fn's full text (signature through closing brace), by
    brace matching from the signature line. `None` when the function, or a
    line that opens its body on a different signature shape, is not found.
    """
    match = re.search(
        r"^fn " + re.escape(fn_name) + r"\(request: &Request\) -> Result<Option<String>, \(\)> \{",
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


def normalize(body, fn_name, const_expr):
    """Collapse the one allowed difference so the rest compares exact."""
    body = body.replace(fn_name, "<FN>", 1)
    return body.replace(const_expr, "<MAX_LEN>")


def findings():
    bodies = []
    for rel_path, fn_name, const_expr in TARGETS:
        path = ROOT / rel_path
        source = path.read_text(encoding="utf-8")
        body = extract_fn_body(source, fn_name)
        if body is None:
            return [f"{rel_path}: could not find fn {fn_name}(request: &Request) -> ..."]
        bodies.append((rel_path, normalize(body, fn_name, const_expr)))

    (first_path, first_norm), (second_path, second_norm) = bodies
    if first_norm != second_norm:
        return [
            f"{first_path} and {second_path} have diverged beyond the function "
            "name and the length-constant spelling; update both the same way "
            "or merge them (issue #2341)."
        ]
    return []


def run():
    found = findings()
    for message in found:
        print(message)
    print(f"tenant-header-validation-sync: {len(found)} findings")
    return 1 if found else 0


def self_test():
    in_sync_a = (
        "fn tenant_key(request: &Request) -> Result<Option<String>, ()> {\n"
        "    let value = 1;\n"
        "    if value > MAX_TENANT_KEY_LEN {\n"
        "        return Err(());\n"
        "    }\n"
        "}\n"
    )
    in_sync_b = (
        "fn declared_tenant(request: &Request) -> Result<Option<String>, ()> {\n"
        "    let value = 1;\n"
        "    if value > autumn_harvest::tenant::MAX_TENANT_LEN {\n"
        "        return Err(());\n"
        "    }\n"
        "}\n"
    )
    body_a = extract_fn_body(in_sync_a, "tenant_key")
    body_b = extract_fn_body(in_sync_b, "declared_tenant")
    assert body_a is not None and body_a.endswith("}"), body_a
    assert body_b is not None and body_b.endswith("}"), body_b
    norm_a = normalize(body_a, "tenant_key", "MAX_TENANT_KEY_LEN")
    norm_b = normalize(body_b, "declared_tenant", "autumn_harvest::tenant::MAX_TENANT_LEN")
    assert norm_a == norm_b, (norm_a, norm_b)

    drifted_b = in_sync_b.replace("if value > autumn_harvest::tenant::MAX_TENANT_LEN {", "if value >= autumn_harvest::tenant::MAX_TENANT_LEN {")
    body_b2 = extract_fn_body(drifted_b, "declared_tenant")
    norm_b2 = normalize(body_b2, "declared_tenant", "autumn_harvest::tenant::MAX_TENANT_LEN")
    assert norm_a != norm_b2, "a drifted comparison operator must be caught"

    assert extract_fn_body(in_sync_a, "no_such_fn") is None

    print("tenant-header-validation-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
