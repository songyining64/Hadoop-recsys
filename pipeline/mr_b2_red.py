#!/usr/bin/env python3
"""Identity reducer for B2 (one line per pair)."""
import sys

for line in sys.stdin:
    line = line.strip()
    if line:
        print(line)
