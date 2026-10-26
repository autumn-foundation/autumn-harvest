#!/usr/bin/env python3
"""Sum buffers and heap rows read from the captured EXPLAIN files.

Usage: python3 -I summarize-plans.py before-explain.txt after-explain.txt
Rows read are the sum of `actual rows x loops` over every scan node on
`harvest_task_queue` (including the aliased inner scans of the gates).
"""
import re
import sys

NODE = re.compile(
    r"(?:Index Scan|Index Only Scan|Bitmap Heap Scan|Seq Scan) (?:using \S+ )?"
    r"on public\.harvest_task_queue \S*.*?actual time=[\d.]+\.\.[\d.]+ "
    r"rows=(\d+) loops=(\d+)"
)
for path in sys.argv[1:]:
    for block in open(path).read().split("=== ")[1:]:
        title = block.split("\n")[0]
        root = re.search(r"Buffers: shared hit=(\d+)(?: read=(\d+))?", block)
        buffers = int(root.group(1)) + int(root.group(2) or 0)
        rows = sum(int(m.group(1)) * int(m.group(2)) for m in NODE.finditer(block))
        print(f"{title}: buffers={buffers} heap_rows_read={rows}")
