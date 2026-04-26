#!/usr/bin/env python3
"""Compute most frequent item_id per category (proxy co-view anchor for sparse logs)."""
import csv
import sys
from collections import defaultdict


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: compute_cat_top1.py social_ecommerce_data.csv\n")
        sys.exit(1)
    path = sys.argv[1]
    counts = defaultdict(lambda: defaultdict(int))
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cat = row.get("category") or ""
            iid = row.get("item_id") or ""
            if not cat or not iid:
                continue
            counts[cat][iid] += 1
    for cat, d in counts.items():
        top = max(d.items(), key=lambda x: x[1])[0]
        line = "{0}\t{1}\n".format(cat, top)
        buf = getattr(sys.stdout, "buffer", None)
        if buf is not None:
            buf.write(line.encode("utf-8"))
        else:
            sys.stdout.write(line)


if __name__ == "__main__":
    main()
