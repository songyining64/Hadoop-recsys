#!/usr/bin/env python3
"""Emit item \\t score from user#item \\t score lines."""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    uk, _, sc = line.partition("\t")
    if "#" not in uk:
        continue
    _, item = uk.split("#", 1)
    try:
        v = float(sc)
    except ValueError:
        continue
    print("{0}\t{1}".format(item, v))
