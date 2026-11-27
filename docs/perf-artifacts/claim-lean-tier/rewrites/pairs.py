"""Paired differences between variants of one summary file.
usage: python3 pairs.py <summary.txt> a-b [a-b ...]"""
import sys, statistics, collections
d = collections.defaultdict(dict)
for line in open(sys.argv[1]):
    r, v, lat, tps = line.split(); d[v][int(r)] = float(lat.split('=')[1])
for spec in sys.argv[2:]:
    a, b = spec.split('-')
    diffs = sorted(d[a][r] - d[b][r] for r in d[a] if r in d[b])
    print(f"{a} - {b}: median {statistics.median(diffs):+.4f} ms, mean {statistics.mean(diffs):+.4f}, sd {statistics.stdev(diffs):.4f}, "
          f"range {diffs[0]:+.3f}..{diffs[-1]:+.3f}, {a} faster in {sum(1 for x in diffs if x < 0)}/{len(diffs)} rounds")
