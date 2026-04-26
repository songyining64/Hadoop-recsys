#!/usr/bin/env python3
"""Within each category, emit item pairs (capped for scale)."""
import sys

MAX_ITEMS = 120

cur = None
buf = []


def emit_pairs(items):
    items = sorted(set(items))
    if len(items) > MAX_ITEMS:
        items = items[:MAX_ITEMS]
    n = len(items)
    for a in range(n):
        for b in range(a + 1, n):
            i, j = items[a], items[b]
            if i > j:
                i, j = j, i
            print("{0}\t{1}\t1".format(i, j))


for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 2:
        continue
    cat, it = parts[0], parts[1]
    if cur is None:
        cur = cat
    if cat != cur:
        emit_pairs(buf)
        buf = []
        cur = cat
    buf.append(it)
if cur is not None:
    emit_pairs(buf)
