#!/usr/bin/env python3
"""Mixed-version contract guard (issue #1828).

The rolling-deploy contract has two halves. `docs/upgrading/README.md`
states it. The `mixed-version-smoke` CI job proves it. This guard fails
when either half goes missing, or when the two stop naming each other.

Checks:
1. `docs/upgrading/README.md` has each contract section.
2. The page names the smoke script and the CI job (as `` `job` ``).
3. `ci.yml` has a `mixed-version-smoke` job that runs the script.
4. The script exists and is executable.
5. The smoke crate declares its own `[workspace]`. Without it, Cargo
   rejects the crate as an unlisted member of the root workspace.

`--self-test` runs the checks against fixtures, so a broken check
cannot pass silently.

Usage:
    python3 docs/audits/mixed-version-contract.py [--self-test]
"""
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = "docs/upgrading/README.md"
CI = ".github/workflows/ci.yml"
SCRIPT = "scripts/run-mixed-version-smoke.sh"
CRATE = "scripts/mixed-version-smoke/Cargo.toml"
JOB = "mixed-version-smoke"

# Each heading holds one part of the contract. The doc may add others.
SECTIONS = (
    "## Supported version skew",
    "## Schema changes: expand, then contract",
    "## Event and codec formats",
    "## Rolling deploy order",
    "## How CI proves the contract",
)


def check_doc(text):
    """Return the findings for the contract page."""
    if text is None:
        return [f"{DOC}: missing"]
    found = []
    lines = {line.rstrip() for line in text.splitlines()}
    for heading in SECTIONS:
        if heading not in lines:
            found.append(f"{DOC}: no `{heading}` heading")
    # The job name is part of the script name, so match it as a code span.
    for name in (SCRIPT, f"`{JOB}`"):
        if name not in text:
            found.append(f"{DOC}: does not name {name}")
    return found


def job_block(ci_text):
    """Return the text of the smoke job, or None."""
    match = re.search(
        rf"^  {re.escape(JOB)}:\n((?:(?:    .*)?\n)*)", ci_text, re.MULTILINE
    )
    return match.group(1) if match else None


def check_ci(text):
    """Return the findings for the CI workflow."""
    if text is None:
        return [f"{CI}: missing"]
    block = job_block(text)
    if block is None:
        return [f"{CI}: no `{JOB}` job"]
    if SCRIPT not in block:
        return [f"{CI}: the `{JOB}` job does not run `{SCRIPT}`"]
    return []


def check_script(exists, executable):
    """Return the findings for the smoke script."""
    if not exists:
        return [f"{SCRIPT}: missing"]
    if not executable:
        return [f"{SCRIPT}: not executable"]
    return []


def check_crate(text):
    """Return the findings for the smoke crate manifest."""
    if text is None:
        return [f"{CRATE}: missing"]
    if not re.search(r"^\[workspace\]\s*$", text, re.MULTILINE):
        return [f"{CRATE}: no `[workspace]` table"]
    return []


def read(rel):
    path = ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def run():
    script = ROOT / SCRIPT
    findings = (
        check_doc(read(DOC))
        + check_ci(read(CI))
        + check_script(script.is_file(), os.access(script, os.X_OK))
        + check_crate(read(CRATE))
    )
    for finding in findings:
        print(finding)
    print(f"mixed-version-contract: {len(findings)} findings")
    return 1 if findings else 0


def self_test():
    good_doc = "\n".join(SECTIONS) + f"\nRun `{SCRIPT}` in job `{JOB}`.\n"
    good_ci = f"jobs:\n  {JOB}:\n    steps:\n      - run: {SCRIPT}\n  other:\n"
    cases = [
        (check_doc(good_doc), 0),
        (check_doc(None), 1),
        (check_doc(good_doc.replace(SECTIONS[2], "## Formats")), 1),
        (check_doc(good_doc.replace(SCRIPT, "x.sh")), 1),
        (check_doc(good_doc.replace(f"`{JOB}`", "`x`")), 1),
        (check_ci(good_ci), 0),
        (check_ci(None), 1),
        (check_ci("jobs:\n  lint:\n    steps: []\n"), 1),
        # The script named in another job does not count.
        (check_ci(f"jobs:\n  {JOB}:\n    steps: []\n  b:\n    run: {SCRIPT}\n"), 1),
        (check_script(True, True), 0),
        (check_script(False, False), 1),
        (check_script(True, False), 1),
        (check_crate("[package]\n\n[workspace]\n"), 0),
        (check_crate("[package]\n"), 1),
        (check_crate(None), 1),
    ]
    bad = [i for i, (got, want) in enumerate(cases) if len(got) != want]
    for i in bad:
        print(f"self-test case {i}: got {cases[i][0]}")
    print(f"mixed-version-contract self-test: {len(cases)} cases, {len(bad)} failed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
