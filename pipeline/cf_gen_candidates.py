#!/usr/bin/env python3
"""
Generate per-user Top-N CF candidates from:
  - ratings path: lines user#item \\t score
  - purchases path: lines user#item \\t 1
  - topn path: lines item \\t neighbor \\t sim
Output: user \\t item \\t cf_score
"""
import sys
from collections import defaultdict


def load_set(path):
    s = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            key = line.split("\t", 1)[0]
            s.add(key)
    return s


def load_user_items(path):
    u2i = defaultdict(set)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "#" not in line:
                continue
            uk, _, _ = line.partition("\t")
            uid, iid = uk.split("#", 1)
            u2i[uid].add(iid)
    return u2i


def load_neighbors(path):
    nbr = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            i, j, sim = parts[0], parts[1], float(parts[2])
            nbr[i].append((j, sim))
    return nbr


def main():
    if len(sys.argv) != 4:
        print("usage: cf_gen_candidates.py ratings_merge purchases_merge topn_merge", file=sys.stderr)
        sys.exit(1)
    ratings_m, purch_m, topn_m = sys.argv[1:4]
    purchases = load_set(purch_m)
    u_items = load_user_items(ratings_m)
    nbr = load_neighbors(topn_m)

    topn = 10
    for u, items in u_items.items():
        bought = set()
        for iid in items:
            if "{0}#{1}".format(u, iid) in purchases:
                bought.add(iid)
        cand = {}
        for i in items:
            for j, sim in nbr.get(i, []):
                if j in bought or j in items:
                    continue
                cand[j] = max(cand.get(j, 0.0), sim)
        for j, sc in sorted(cand.items(), key=lambda x: -x[1])[:topn]:
            print("{0}\t{1}\t{2}".format(u, j, sc))


if __name__ == "__main__":
    main()
