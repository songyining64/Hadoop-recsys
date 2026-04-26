#!/usr/bin/env python3
"""Job A1: raw CSV -> user#item \\t score (implicit rating from one row)."""
import csv
import sys


def i_(x):
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return 0


def main():
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw or raw.startswith("user_id"):
            continue
        row = next(csv.reader([raw]))
        if len(row) < 32:
            continue
        uid, iid = row[0], row[1]
        like_num = i_(row[17])
        collect_num = i_(row[19])
        add2cart = i_(row[22])
        coupon_used = i_(row[24])
        label = i_(row[31])
        score = 0
        if label == 1:
            score += 5
        if add2cart != 0:
            score += 4
        if coupon_used != 0:
            score += 3
        if collect_num > 0:
            score += 2
        if like_num > 0:
            score += 1
        if score > 0:
            print("{0}#{1}\t{2}".format(uid, iid, score))


if __name__ == "__main__":
    main()
