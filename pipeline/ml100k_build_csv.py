#!/usr/bin/env python3
"""
Build unified Spark/MR CSV from MovieLens 100K (tabular schema used by hive_compat + MR).
``event_ts`` is Unix time from u.data for time-based evaluation in Spark.

Expected files under ML100K_DIR (names may be lowercase after unzip):
  u.data   user id | item id | rating | timestamp (tab)
  u.item   pipe-separated, genres at end (see Grouplens spec)
  u.user   user id | age | gender | occupation | zip (tab)

Usage:
  python3 ml100k_build_csv.py /path/to/ml-100k /path/to/recsys_ml100k.csv
"""
import csv
import os
import sys

GENRE_NAMES = (
    "unknown",
    "Action",
    "Adventure",
    "Animation",
    "Children's",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Fantasy",
    "Film-Noir",
    "Horror",
    "Musical",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "War",
    "Western",
)


def find_file(root, name):
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.lower() == name.lower():
                return os.path.join(dirpath, f)
    return None


def load_users(path):
    users = {}
    with open(path, "r", encoding="latin-1", errors="replace") as f:
        for line in f:
            parts = line.strip().split("|")
            if len(parts) < 5:
                continue
            uid = parts[0].strip()
            try:
                age = int(parts[1])
            except ValueError:
                age = 25
            g = parts[2].strip().upper()
            gender = 1 if g == "F" else 0
            try:
                occ = int(parts[3])
            except ValueError:
                occ = 0
            users[uid] = {"age": age, "gender": gender, "user_level": occ}
    return users


def load_items(path):
    items = {}
    with open(path, "r", encoding="latin-1", errors="replace") as f:
        for line in f:
            parts = [p.strip() for p in line.rstrip().split("|")]
            if len(parts) < 6:
                continue
            mid = parts[0]
            title = parts[1] if len(parts) > 1 else ""
            genres = parts[5:]
            primary = "unknown"
            for i, gflag in enumerate(genres[: len(GENRE_NAMES)]):
                if gflag == "1" and i < len(GENRE_NAMES):
                    primary = GENRE_NAMES[i]
                    break
            items[mid] = {"title": title, "category": primary}
    return items


def load_ratings(path):
    rows = []
    with open(path, "r", encoding="latin-1", errors="replace") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            uid, iid, rating_s = parts[0], parts[1], parts[2]
            ts = int(parts[3]) if len(parts) > 3 else 0
            try:
                rating = int(rating_s)
            except ValueError:
                continue
            rows.append((uid, iid, rating, ts))
    return rows


HEADER = [
    "user_id",
    "item_id",
    "age",
    "gender",
    "user_level",
    "purchase_freq",
    "total_spend",
    "register_days",
    "follow_num",
    "fans_num",
    "price",
    "discount_rate",
    "category",
    "title_length",
    "title_emo_score",
    "img_count",
    "has_video",
    "like_num",
    "comment_num",
    "share_num",
    "collect_num",
    "is_follow_author",
    "add2cart",
    "coupon_received",
    "coupon_used",
    "pv_count",
    "last_click_gap",
    "interaction_rate",
    "purchase_intent",
    "freshness_score",
    "social_influence",
    "event_ts",
    "label",
]


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
            "usage: ml100k_build_csv.py ML100K_DIR OUT_CSV\n"
        )
        sys.exit(1)
    root = sys.argv[1]
    out_path = sys.argv[2]

    udata = find_file(root, "u.data")
    uitem = find_file(root, "u.item")
    uuser = find_file(root, "u.user")
    if not (udata and uitem and uuser):
        sys.stderr.write(
            "missing u.data / u.item / u.user under {}\n".format(root)
        )
        sys.exit(1)

    users = load_users(uuser)
    items = load_items(uitem)
    ratings = load_ratings(udata)

    uid_counts = {}
    uid_ts_min = {}
    uid_ts_max = {}
    uid_spend = {}
    for uid, iid, rating, ts in ratings:
        uid_counts[uid] = uid_counts.get(uid, 0) + 1
        uid_spend[uid] = uid_spend.get(uid, 0) + rating
        if uid not in uid_ts_min or ts < uid_ts_min[uid]:
            uid_ts_min[uid] = ts
        if uid not in uid_ts_max or ts > uid_ts_max[uid]:
            uid_ts_max[uid] = ts

    global_min_ts = min(t for _, _, _, t in ratings) if ratings else 0

    iid_sum = {}
    iid_cnt = {}
    for uid, iid, rating, ts in ratings:
        iid_sum[iid] = iid_sum.get(iid, 0) + rating
        iid_cnt[iid] = iid_cnt.get(iid, 0) + 1

    iid_avg_rating = {
        i: float(iid_sum[i]) / iid_cnt[i] for i in iid_sum
    }

    with open(out_path, "w", encoding="utf-8", newline="") as fout:
        w = csv.writer(fout)
        w.writerow(HEADER)
        for uid, iid, rating, ts in ratings:
            u = users.get(uid, {"age": 25, "gender": 0, "user_level": 0})
            it = items.get(iid, {"title": "", "category": "unknown"})
            pf = uid_counts.get(uid, 1)
            reg = 0
            if uid in uid_ts_min and global_min_ts:
                reg = max(0, int((uid_ts_min[uid] - global_min_ts) / 86400))
            label = 1 if rating >= 4 else 0
            avg_r = iid_avg_rating.get(iid, float(rating))
            price = round(avg_r * 18.0, 2)
            title = it["title"]
            row = [
                "U{}".format(uid) if not uid.startswith("U") else uid,
                "I{}".format(iid) if not iid.startswith("I") else iid,
                u["age"],
                u["gender"],
                u["user_level"],
                pf,
                round(uid_spend.get(uid, rating) * 88.0, 2),
                reg,
                0,
                0,
                price,
                0.0,
                it["category"],
                len(title),
                0.5,
                0,
                0,
                rating,
                0,
                0,
                max(0, rating - 3),
                0,
                1 if rating >= 3 else 0,
                0,
                0,
                1,
                0,
                round(rating / 5.0 * 100.0, 3),
                float(rating),
                0.5,
                0,
                int(ts),
                label,
            ]
            w.writerow(row)

    sys.stderr.write(
        "wrote {} rows from {} to {}\n".format(len(ratings), udata, out_path)
    )


if __name__ == "__main__":
    main()
