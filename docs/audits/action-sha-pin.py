#!/usr/bin/env python3
"""Supply-chain harness: every action `uses:` pins a commit SHA.

A tag or a branch is a mutable ref. Its owner can move it to new code, and
the next run executes that code with the workflow's token. A 40-hex commit
SHA cannot move. Issue #1826.

Each `uses` key in `.github/workflows/*.y*ml` and `.github/actions/**/
action.y*ml` must take one of three forms:

- `owner/repo[/path]@<40 lowercase hex> # <tag>`. The comment names the
  source tag of the SHA. Dependabot updates the SHA and the comment together.
- `./path`, a local action in this repository.
- `docker://image@sha256:<64 hex>`, an image pinned by digest.

The script finds each `uses` key in the parsed YAML tree, so a flow mapping,
an escaped key or a folded value cannot hide one. A parser drops comments, so
the script then reads the raw line of each key for the `# <tag>` comment. A
key that is not on one line in the plain form above is a finding. Text in a
`run:` block is not a key, so it is never a finding.

The script does not check that the SHA matches the tag. That needs network
access. Dependabot rewrites each pair that it bumps. A hand edit is not
checked.

`--self-test` runs the fixtures. The script needs PyYAML and no network. A
missing PyYAML is a hard failure, so the check never skips silently.
"""
import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    print("action-sha-pin: PyYAML is required", file=sys.stderr)
    sys.exit(2)

ROOT = pathlib.Path(__file__).resolve().parents[2]

# The one line form that the script reads: an optional list dash, the plain
# key, an optionally quoted value, and an optional comment.
PLAIN = re.compile(
    r"""^\s*(?:-\s+)?uses:\s*(?P<q>["']?)(?P<value>[^\s"'#]+)(?P=q)"""
    r"""(?:\s+#\s*(?P<comment>\S.*?))?\s*$"""
)
REMOTE = re.compile(
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@(?P<ref>.+)$"
)
SHA = re.compile(r"^[0-9a-f]{40}$")
# A version tag: `v1`, `v4.4.0`, `2.0.1`. Dependabot cannot update other text.
TAG = re.compile(r"^v?\d[\w.+-]*(?:\s|$)")
DOCKER = re.compile(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$")


def uses_nodes(text):
    """Returns (line number, value) for each `uses` key in the YAML tree.

    A `uses` key directly under `with:` is an action input, not an action
    reference, so the walk skips it.
    """
    out = []

    def walk(node, parent_key):
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                name = key.value if isinstance(key, yaml.ScalarNode) else None
                if name == "uses" and parent_key != "with":
                    out.append((key.start_mark.line + 1, value))
                walk(value, name)
        elif isinstance(node, yaml.SequenceNode):
            for item in node.value:
                walk(item, parent_key)

    for doc in yaml.compose_all(text, Loader=yaml.SafeLoader):
        walk(doc, None)
    return out


def check_pin(value, comment):
    """Returns a finding message for one plain `uses:` line, or None."""
    if value.startswith("./"):
        return None
    if value.startswith("docker://"):
        if DOCKER.match(value):
            return None
        return f"`{value}` must pin the image by `@sha256:<digest>`"
    remote = REMOTE.match(value)
    if not remote:
        return f"`{value}` is not `owner/repo[/path]@<ref>`"
    if not SHA.match(remote.group("ref")):
        return f"`{value}` must pin a 40-hex commit SHA, not a tag or branch"
    if not comment or not TAG.match(comment):
        return f"`{value}` needs a `# <tag>` comment, such as `# v4.4.0`"
    return None


def scan_text(text):
    """Returns (line number, message) for each finding in `text`."""
    try:
        nodes = uses_nodes(text)
    except yaml.YAMLError as err:
        return [(0, f"does not parse as YAML: {err}")]
    lines = text.splitlines()
    out = []
    for number, value in nodes:
        line = lines[number - 1] if number <= len(lines) else ""
        m = PLAIN.match(line)
        plain = (
            isinstance(value, yaml.ScalarNode)
            and m is not None
            and m.group("value") == value.value
            and value.start_mark.line + 1 == number
        )
        if not plain:
            out.append(
                (number, "write this `uses:` on one line: `uses: owner/repo@<sha> # <tag>`")
            )
            continue
        message = check_pin(m.group("value"), m.group("comment"))
        if message:
            out.append((number, message))
    return out


def files():
    gh = ROOT / ".github"
    found = sorted((gh / "workflows").glob("*.y*ml"))
    found += sorted((gh / "actions").glob("**/action.y*ml"))
    return found


SHA_A = "11d5960a326750d5838078e36cf38b85af677262"
DIGEST = "0" * 64


def step(line):
    """A one-step workflow around `line`, which sits at step-item indent."""
    return f"jobs:\n  a:\n    steps:\n{line}\n"


# (YAML text, finding count). Each fixture names one form the scan decides.
FIXTURES = [
    (step(f"      - uses: actions/checkout@{SHA_A} # v4.4.0"), 0),
    (step(f"      - uses: actions/checkout@{SHA_A}  #  v4.4.0  "), 0),
    (step(f"      - uses: 'actions/checkout@{SHA_A}' # v4.4.0"), 0),
    (step(f'      - uses: "github/codeql-action/init@{SHA_A}" # v3.29.0'), 0),
    (step(f"      - uses: dtolnay/rust-toolchain@{SHA_A} # v1"), 0),
    (step(f"      - uses: actions/checkout@{SHA_A} # v4 # trailing"), 0),
    (step(f"      - name: x\n        uses: actions/checkout@{SHA_A} # v4.4.0"), 0),
    (step("      - uses: ./.github/actions/setup"), 0),
    (step(f"      - uses: docker://alpine@sha256:{DIGEST}"), 0),
    (step("      # uses: actions/checkout@v4 is only a comment\n      - run: true"), 0),
    (step("      - name: 'cache uses: none'\n        run: true"), 0),
    (step("      - run: |\n          cat <<EOF\n          uses: foo\n          EOF"), 0),
    (step("      - uses: ./a\n        with:\n          uses: an-input-named-uses"), 0),
    (f"jobs:\n  call:\n    uses: o/r/.github/workflows/w.yml@{SHA_A} # v1.2.0\n", 0),
    (step("      - uses: actions/checkout@v4"), 1),
    (step("      - uses: actions/checkout@v4 # v4"), 1),
    (step("      - uses: dtolnay/rust-toolchain@stable"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A}"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A} #"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A} # latest"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A.upper()} # v4.4.0"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A[:7]} # v4.4.0"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A}0 # v4.4.0"), 1),
    (step("      - uses: docker://alpine:3.20"), 1),
    (step("      - uses: actions/checkout"), 1),
    ("jobs:\n  call:\n    uses: o/r/.github/workflows/w.yml@main\n", 1),
    # Forms that a line scan alone misses. The parsed tree finds each one.
    (step(f"      - {{ uses: actions/checkout@{SHA_A} }}"), 1),
    (step("      - {name: x, with: {a: b}, uses: actions/checkout@v4}"), 1),
    (step("      - {\n        name: x, uses: actions/checkout@v4\n        }"), 1),
    ("jobs:\n  a:\n    steps: [uses: actions/checkout@v4]\n", 1),
    (step('      - "u\\x73es": actions/checkout@v4'), 1),
    (step(f'      - "uses": actions/checkout@{SHA_A} # v4.4.0'), 1),
    (step("      - ? uses\n        : actions/checkout@v4"), 1),
    (step("      - &k uses: actions/checkout@v4"), 1),
    (step("      - uses: >-\n          actions/checkout@v4"), 1),
    (step(f"      - uses: actions/checkout@{SHA_A} # v4.4.0\n      - uses: a/b@v1"), 1),
    ("jobs: [unclosed\n", 1),
]


def self_test():
    bad = 0
    for text, expected in FIXTURES:
        got = scan_text(text)
        if len(got) != expected:
            print(f"self-test: expected {expected} finding(s), got {got}:\n{text}")
            bad += 1
    print(f"action-sha-pin self-test: {len(FIXTURES)} fixtures, {bad} wrong")
    return 1 if bad else 0


def main(argv):
    if "--self-test" in argv:
        return self_test()
    total = 0
    paths = files()
    for path in paths:
        for number, message in scan_text(path.read_text(encoding="utf-8")):
            print(f"{path.relative_to(ROOT)}:{number}: {message}")
            total += 1
    print(f"action-sha-pin: {len(paths)} files, {total} unpinned")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
