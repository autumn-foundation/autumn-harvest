#!/usr/bin/env python3
"""Grade the group commit bench rounds (issue #2256).

Reads results/<sync>-r<n>.txt and prints, per mode and writer count, the
median over rounds of the criterion estimate (wall ms per decision) and of
the p50 decision latency. Usage: python3 grade.py [results-dir]
"""
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "results")
UNIT = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1e3}

for sync in ("sync-on", "sync-off"):
    wall = defaultdict(list)
    p50 = defaultdict(list)
    batch = defaultdict(list)
    ratios = defaultdict(list)
    p50_ratios = defaultdict(list)
    ab = defaultdict(list)
    ab_ratios = defaultdict(list)
    depth = defaultdict(list)
    files = sorted(root.glob(f"{sync}-*r*.txt"))
    if not files:
        continue
    for path in files:
        current = None
        one_wall = {}
        one_p50 = {}
        one_ab = {}
        for line in path.read_text().splitlines():
            m = re.match(r"decision_commit/(\w+)/writers/(\d+)", line.strip())
            if m:
                current = (m.group(1), int(m.group(2)))
            m = re.search(r"time:\s+\[\S+ \S+ ([\d.]+) (\S+) ", line)
            if m and current:
                value = float(m.group(1)) * UNIT[m.group(2)]
                wall[current].append(value)
                one_wall[current] = value
            m = re.search(r"depth_ab writers=(\d+) pair=\d+ solo_per_s=([\d.]+) group_per_s=([\d.]+) ratio=([\d.]+)", line)
            if m:
                depth[int(m.group(1))].append((float(m.group(2)), float(m.group(3)), float(m.group(4))))
            m = re.search(r" ab mode=(\w+) decisions=\d+ p50_ms=([\d.]+)", line)
            if m:
                ab[m.group(1)].append(float(m.group(2)))
                one_ab[m.group(1)] = float(m.group(2))
            m = re.search(r"mode=(\w+) writers=(\d+) .* p50_ms=([\d.]+) .* mean_batch=([\d.]+)", line)
            if m:
                key = (m.group(1), int(m.group(2)))
                p50[key].append(float(m.group(3)))
                batch[key].append(float(m.group(4)))
                one_p50[key] = float(m.group(3))
        for mode in ("direct", "group"):
            if mode in one_ab and "solo" in one_ab:
                ab_ratios[mode].append(one_ab[mode] / one_ab["solo"])
        for (mode, writers), value in one_wall.items():
            if mode == "group" and ("solo", writers) in one_wall:
                ratios[writers].append(value / one_wall[("solo", writers)])
            if mode == "group" and ("solo", writers) in one_p50:
                p50_ratios[writers].append(one_p50[("group", writers)] / one_p50[("solo", writers)])
    criterion_rounds = sum(1 for f in files if "time:" in f.read_text())
    print(f"## {sync}: {criterion_rounds} criterion rounds, {len(files)} files\n")
    print("| writers | direct ms | solo ms | group ms | group / solo | solo p50 | group p50 | mean batch |")
    print("|--:|--:|--:|--:|--:|--:|--:|--:|")
    for writers in sorted({w for _, w in wall}):
        med = lambda d, mode: statistics.median(d[(mode, writers)]) if d[(mode, writers)] else float("nan")
        solo, group = med(wall, "solo"), med(wall, "group")
        print(
            f"| {writers} | {med(wall, 'direct'):.3f} | {solo:.3f} | {group:.3f} | "
            f"{group / solo:.2f} | {med(p50, 'solo'):.3f} | {med(p50, 'group'):.3f} | "
            f"{med(batch, 'group'):.2f} |"
        )
    print()
    if ab:
        print("Low-load A/B, one writer, modes in turn (p50 ms):\n")
        print("| mode | p50, median | p50 range | ratio to solo, median | ratio range |")
        print("|:--|--:|--:|--:|--:|")
        for mode in ("direct", "solo", "group"):
            r = ab_ratios.get(mode, [1.0])
            print(
                f"| {mode} | {statistics.median(ab[mode]):.3f} | {min(ab[mode]):.3f} to {max(ab[mode]):.3f} | "
                f"{statistics.median(r):.3f} | {min(r):.3f} to {max(r):.3f} |"
            )
        print()
    if depth:
        print("Depth A/B, alternating bursts (decisions per second):\n")
        print("| writers | pairs | solo, median | group, median | ratio, median | ratio range | pairs faster |")
        print("|--:|--:|--:|--:|--:|--:|--:|")
        for writers in sorted(depth):
            rows = depth[writers]
            ratio = [r for _, _, r in rows]
            print(
                f"| {writers} | {len(rows)} | {statistics.median(a for a, _, _ in rows):.1f} | "
                f"{statistics.median(b for _, b, _ in rows):.1f} | {statistics.median(ratio):.3f} | "
                f"{min(ratio):.3f} to {max(ratio):.3f} | {sum(1 for x in ratio if x > 1)} of {len(rows)} |"
            )
        print()
    print("Per-round ratios, group over solo (below 1 is faster):\n")
    print("| writers | wall ratio, median | wall ratio, range | rounds faster | p50 ratio, median |")
    print("|--:|--:|--:|--:|--:|")
    for writers in sorted(ratios):
        r = ratios[writers]
        print(
            f"| {writers} | {statistics.median(r):.2f} | {min(r):.2f} to {max(r):.2f} | "
            f"{sum(1 for x in r if x < 1)} of {len(r)} | {statistics.median(p50_ratios[writers]):.2f} |"
        )
    print()
