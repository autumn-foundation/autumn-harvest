"""Medians and spread of the claim statement latency per variant.

Reports the raw median, min and max per variant, and the median of the
per-round differences against the base variant of the same round (the
rounds alternate the variants, so a paired difference removes slow drift).
usage: python3 median.py <out-dir>/summary.txt"""
import sys, statistics, collections
d = collections.defaultdict(dict); t = collections.defaultdict(dict)
for line in open(sys.argv[1]):
    r, v, lat, tps = line.split()
    d[v][int(r)] = float(lat.split('=')[1]); t[v][int(r)] = float(tps.split('=')[1])
base = d.get('base')
for v in d:
    xs = list(d[v].values())
    m = statistics.median(xs)
    out = f"{v:6s} n={len(xs):2d} stmt median {m:.3f} ms  min {min(xs):.3f}  max {max(xs):.3f}  tps median {statistics.median(t[v].values()):.0f}"
    if base and v != 'base':
        diffs = sorted(d[v][r] - base[r] for r in d[v] if r in base)
        wins = sum(1 for x in diffs if x < 0)
        out += f"  | paired vs base: median {statistics.median(diffs):+.3f} ms, range {diffs[0]:+.3f}..{diffs[-1]:+.3f}, faster in {wins}/{len(diffs)} rounds"
    print(out)
