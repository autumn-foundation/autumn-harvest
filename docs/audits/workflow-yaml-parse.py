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


class UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects a repeated mapping key.

    PyYAML keeps the last value silently. GitHub rejects the workflow.
    """

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=True)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None,
                    None,
                    f"duplicate mapping key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep)


bad = 0
files = sorted((ROOT / ".github" / "workflows").glob("*.y*ml"))
for path in files:
    try:
        doc = yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader)
    except yaml.YAMLError as err:
        print(f"{path.relative_to(ROOT)}: {err}")
        bad += 1
        continue
    jobs = doc.get("jobs") if isinstance(doc, dict) else None
    if not isinstance(jobs, dict) or not jobs:
        print(f"{path.relative_to(ROOT)}: `jobs` is not a non-empty mapping")
        bad += 1
print(f"workflow-yaml-parse: {len(files)} files, {bad} unparsable")
sys.exit(1 if bad else 0)
