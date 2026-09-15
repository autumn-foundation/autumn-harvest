#!/usr/bin/env python3
"""Print a free Grafana panel id for starter-pack-v0.1.0.json.

Hand-incrementing near the current maximum caused five duplicate-id
collisions between concurrently-drafted PRs (ids 956, 957, 958, 959,
960). Two branches that each read the same maximum and add one land on
the same id; the JSON merges cleanly either way, so nothing conflicts
until `panel_structure_is_grafana10_clean` runs against the merged
file. Drawing from a wide, mostly-empty range instead makes that
collision astronomically unlikely rather than near-certain.

Usage: python3 docs/dashboards/next-panel-id.py
"""

import json
import pathlib
import random
import sys

DASHBOARD = pathlib.Path(__file__).parent / "starter-pack-v0.1.0.json"
ID_RANGE = (100_000, 999_999)


def used_ids(panels):
    ids = set()
    for panel in panels:
        if "id" in panel:
            ids.add(panel["id"])
        ids |= used_ids(panel.get("panels", []))
    return ids


def main():
    dashboard = json.loads(DASHBOARD.read_text())
    taken = used_ids(dashboard.get("panels", []))
    low, high = ID_RANGE
    for _ in range(1000):
        candidate = random.randint(low, high)
        if candidate not in taken:
            print(candidate)
            return
    print("could not find a free id after 1000 tries", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
