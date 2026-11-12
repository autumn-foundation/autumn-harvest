"""Summarize e2e_bench runs: per-run metrics and medians per build."""
import re
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
census = {}
for line in (root / "census.txt").read_text().splitlines():
    label = line.split()[0]
    census[label] = dict(kv.split("=") for kv in line.split()[1:])

rows = {}
prefix = sys.argv[3] if len(sys.argv) > 3 else ""
for md in sorted(root.glob(prefix + "*.md")):
    label = md.stem
    metrics = {}
    for m in re.finditer(r"^\| `(\w+)` \| 1 \| `(\w+)` \| ([\d.]+) \|", md.read_text(), re.M):
        metrics[f"{m.group(1)}.{m.group(2)}"] = float(m.group(3))
    metrics["empty_polls_per_s"] = float(census[label]["empty_polls_per_s"])
    metrics["empty_per_claim"] = int(census[label]["empty_polls"]) / int(census[label]["claimed_rows"])
    rows[label] = metrics

keys = sys.argv[2].split(",")
print("| run | " + " | ".join(keys) + " |")
print("|:--|" + "--:|" * len(keys))
order = sorted(rows, key=lambda l: (re.sub(r"_\d+_.*", "", l), int(re.search(r"_(\d+)_", l).group(1))))
for label in order:
    print(f"| {label} | " + " | ".join(f"{rows[label].get(k, float('nan')):.2f}" for k in keys) + " |")
builds = sorted({l.rsplit("_", 1)[1] for l in rows})
print()
print("| median | " + " | ".join(keys) + " |")
print("|:--|" + "--:|" * len(keys))
for b in builds:
    vals = [statistics.median([rows[l][k] for l in rows if l.endswith("_" + b) and k in rows[l]] or [float("nan")]) for k in keys]
    print(f"| {b} | " + " | ".join(f"{v:.2f}" for v in vals) + " |")
