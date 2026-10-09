#!/usr/bin/env python3
"""Grade the claim-concurrency sweep from its raw output.

Usage: grade.py <raw directory>

Prints one Markdown table per measure:

- workflows/sec per cell (mean over valid runs, with each run);
- Temporal over harvest per depth and claim cap;
- claims in flight: claim count x mean claim time / elapsed time;
- the mean claim time;
- the mean pool connections in use.
"""

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CELL = re.compile(r"^cell (.*)$")
TEMPORAL = re.compile(r"^rep \d+: ([0-9.]+) workflows/sec .*correctness (\w+)")


def parse_kv(text):
    return dict(part.split("=", 1) for part in text.split() if "=" in part)


def load(directory):
    harvest = defaultdict(list)
    temporal = defaultdict(list)
    for path in sorted(Path(directory).glob("r*.txt")):
        name = path.name
        for line in path.read_text().splitlines():
            m = CELL.match(line)
            if m:
                kv = parse_kv(m.group(1))
                if kv.get("valid") != "PASS" or kv.get("truncated") != "false":
                    continue
                harvest[(kv["tree"], int(kv["depth"]))].append(kv)
                continue
            m = TEMPORAL.match(line)
            if m and m.group(2) == "PASS":
                depth = int(re.search(r"-d(\d+)\.txt$", name).group(1))
                temporal[depth].append(float(m.group(1)))
    return harvest, temporal


def in_flight(kv):
    claims = int(kv["claim_n"])
    mean_ms = float(kv.get("claim_mean_ms", "0"))
    return claims * mean_ms / 1000.0 / float(kv["elapsed"])


def main(argv):
    harvest, temporal = load(argv[1])
    trees = sorted({tree for tree, _ in harvest}, key=lambda t: int(t.replace("claims", "")))
    depths = sorted({depth for _, depth in harvest} | set(temporal))

    print("### Workflows/sec (mean of valid runs; runs in brackets)\n")
    print("| depth | Temporal | " + " | ".join(trees) + " |")
    print("|--:|--:|" + "--:|" * len(trees))
    for depth in depths:
        row = [str(depth)]
        t = temporal.get(depth, [])
        row.append(f"{statistics.mean(t):.2f} ({len(t)})" if t else "—")
        for tree in trees:
            runs = harvest.get((tree, depth), [])
            if runs:
                rates = [float(kv["wfps"]) for kv in runs]
                each = " / ".join(f"{r:.2f}" for r in rates)
                row.append(f"{statistics.mean(rates):.2f} [{each}]")
            else:
                row.append("—")
        print("| " + " | ".join(row) + " |")

    print("\n### Temporal over harvest (from unrounded means)\n")
    print("| depth | " + " | ".join(trees) + " |")
    print("|--:|" + "--:|" * len(trees))
    for depth in depths:
        t = temporal.get(depth, [])
        row = [str(depth)]
        for tree in trees:
            runs = harvest.get((tree, depth), [])
            if t and runs:
                ratio = statistics.mean(t) / statistics.mean(float(kv["wfps"]) for kv in runs)
                row.append(f"{ratio:.2f}x")
            else:
                row.append("—")
        print("| " + " | ".join(row) + " |")

    for title, measure in (
        ("Claims in flight (claim count x mean claim ms / elapsed)", in_flight),
        ("Mean claim time, ms", lambda kv: float(kv.get("claim_mean_ms", "0"))),
        ("Pool connections in use, mean", lambda kv: float(kv["in_use_mean"])),
    ):
        print(f"\n### {title}\n")
        print("| depth | " + " | ".join(trees) + " |")
        print("|--:|" + "--:|" * len(trees))
        for depth in depths:
            row = [str(depth)]
            for tree in trees:
                runs = harvest.get((tree, depth), [])
                row.append(f"{statistics.mean(measure(kv) for kv in runs):.2f}" if runs else "—")
            print("| " + " | ".join(row) + " |")


if __name__ == "__main__":
    main(sys.argv)
