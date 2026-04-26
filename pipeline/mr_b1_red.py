#!/usr/bin/env python3
"""B1 reduce: for each user emit co-occurring item pairs i,j with count 1 each."""
import sys


def emit_pairs(items):
    items = sorted(set(items))
    if len(items) > 200:
        items = items[:200]
    n = len(items)
    for a in range(n):
        for b in range(a + 1, n):
            i, j = items[a], items[b]
            if i > j:
                i, j = j, i
            print("{0}\t{1}\t1".format(i, j))


cur_user = None
buf = []
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 2:
        continue
    u, it = parts[0], parts[1]
    if cur_user is None:
        cur_user = u
    if u != cur_user:
        emit_pairs(buf)
        buf = []
        cur_user = u
    buf.append(it)
if cur_user is not None:
    emit_pairs(buf)
