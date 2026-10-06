#!/usr/bin/env python3
"""Supply-chain harness: every action `uses:` is pinned to a commit SHA.

A tag or a branch is a mutable ref. Its owner can move it to new code, and
the next run executes that code with the workflow's token. A 40-hex commit
SHA cannot move. Issue #1826.

Each `uses:` line in `.github/workflows/*.y*ml` and `.github/actions/**/
action.y*ml` must take one of three forms:

- `owner/repo[/path]@<40 lowercase hex> # <ref>`. The comment names the tag
  that the SHA came from. Dependabot updates the SHA and the comment together.
- `./path`, a local action in this repository.
- `docker://image@sha256:<64 hex>`, an image pinned by digest.

The script reads raw lines, not parsed YAML, because a YAML parser drops the
version comment. Any other line with a `uses:` key is a finding, so an
unusual form (a flow mapping, a quoted key) fails closed.

The script does not check that the SHA matches the tag. That needs network
access. Dependabot keeps the pair in step.

`--self-test` runs the fixtures. Pure stdlib, no network.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

# A `uses` key on a line: a step key, a list item, a quoted key, or an entry
# of a flow mapping. Each hit must pass the strict form below, or it is a
# finding. Text after `run:` or `name:` is not a key, so it is not a hit.
USES_KEY = re.compile(
    r"""^\s*(?:-\s+)?["']?uses["']?\s*:|\{(?:[^}]*,)?\s*["']?uses["']?\s*:"""
)
STRICT = re.compile(
    r"""^\s*(?:-\s+)?uses:\s*(?P<q>["']?)(?P<value>[^\s"'#]+)(?P=q)"""
    r"""(?:\s+#\s*(?P<comment>\S.*?))?\s*$"""
)
REMOTE = re.compile(
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@(?P<ref>.+)$"
)
SHA = re.compile(r"^[0-9a-f]{40}$")
DOCKER = re.compile(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$")


def check_line(line):
    """Returns a finding message for one line, or None when it passes."""
    stripped = line.lstrip()
    if stripped.startswith("#") or not USES_KEY.search(line):
        return None
    m = STRICT.match(line)
    if not m:
        return "unsupported `uses:` form; write `uses: owner/repo@<sha> # <ref>`"
    value, comment = m.group("value"), m.group("comment")
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
    if not comment:
        return f"`{value}` needs a `# <ref>` comment that names its tag"
    return None


def scan_text(text):
    """Returns (line number, message) for each finding in `text`."""
    out = []
    for number, line in enumerate(text.splitlines(), start=1):
        message = check_line(line)
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

# (line, passes?) pairs. Each fixture names one form the scan must decide.
FIXTURES = [
    (f"      - uses: actions/checkout@{SHA_A} # v4.4.0", True),
    (f"        uses: actions/checkout@{SHA_A}  #  v4.4.0  ", True),
    (f"      - uses: 'actions/checkout@{SHA_A}' # v4.4.0", True),
    (f'      - uses: "github/codeql-action/init@{SHA_A}" # v3.29.0', True),
    ("      - uses: ./.github/actions/setup", True),
    (f"      - uses: docker://alpine@sha256:{DIGEST}", True),
    ("      # uses: actions/checkout@v4 is only a comment", True),
    ("        run: echo 'this step uses: nothing'", True),
    ("      - name: cache uses: none", True),
    ("      - uses: actions/checkout@v4", False),
    ("      - uses: actions/checkout@v4 # v4", False),
    ("      - uses: dtolnay/rust-toolchain@stable", False),
    (f"      - uses: actions/checkout@{SHA_A}", False),
    (f"      - uses: actions/checkout@{SHA_A} #", False),
    (f"      - uses: actions/checkout@{SHA_A.upper()} # v4.4.0", False),
    (f"      - uses: actions/checkout@{SHA_A[:7]} # v4.4.0", False),
    ("      - uses: docker://alpine:3.20", False),
    ("      - uses: actions/checkout", False),
    (f"      - {{ uses: actions/checkout@{SHA_A} }}", False),
    (f'      - "uses": actions/checkout@{SHA_A} # v4.4.0', False),
    (f"      - uses: actions/checkout@{SHA_A} # v4 # trailing", True),
]


def self_test():
    bad = 0
    for line, passes in FIXTURES:
        got = check_line(line) is None
        if got != passes:
            want = "pass" if passes else "fail"
            print(f"self-test: expected {want}: {line!r}")
            bad += 1
    text = "\n".join(line for line, _ in FIXTURES)
    expected = sum(1 for _, passes in FIXTURES if not passes)
    if len(scan_text(text)) != expected:
        print(f"self-test: scan_text found {len(scan_text(text))}, expected {expected}")
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
