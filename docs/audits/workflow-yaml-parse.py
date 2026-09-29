#!/usr/bin/env python3
"""Semaphore CI harness: every `.github/workflows/*.yml` must parse as YAML.

GitHub does not reject a workflow file it cannot parse. It runs the file as a
zero-job workflow named after its path, and reports a failed run on every
push. The triggers in the file never fire, so a scheduled suite silently
never runs (`chaos.yml` was in this state from its first commit).

This script parses each workflow with PyYAML and exits 1 on the first
unparsable file set. A missing PyYAML is a hard failure, not a skip: a
skipped check would report green over an unchecked file.
"""
import pathlib
import sys

try:
    import yaml
except ImportError:
    print("workflow-yaml-parse: PyYAML is required", file=sys.stderr)
    sys.exit(2)

ROOT = pathlib.Path(__file__).resolve().parents[2]
bad = 0
files = sorted((ROOT / ".github" / "workflows").glob("*.y*ml"))
for path in files:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as err:
        print(f"{path.relative_to(ROOT)}: {err}")
        bad += 1
        continue
    if not isinstance(doc, dict) or "jobs" not in doc:
        print(f"{path.relative_to(ROOT)}: no top-level `jobs` mapping")
        bad += 1
print(f"workflow-yaml-parse: {len(files)} files, {bad} unparsable")
sys.exit(1 if bad else 0)
