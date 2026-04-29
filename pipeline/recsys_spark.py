#!/usr/bin/env python3
"""
Spark: HiveQL-compat views, train/val protocol, negative sampling, LR/GBT/RF,
fusion alpha sweep, AUC/PR/NDCG, stratified metrics, bootstrap CI, diversity.

Evaluation: time-last or LOO positives; optional Movielens *.base/*.test via
RECSYS_ML100K_OFFICIAL_SPLIT. Test positives missing from MR CF are injected (cf_score=0) so
Strict/NDCG see the ground-truth item; metrics also report CF top-M hit before injection.
"""
import os
import time
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)


def setup_plot_style():
    plt.rcParams["font.sans-serif"] = [
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Noto Sans CJK TC",
        "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.dpi"] = 120


def dcg_at_k(rels, k):
    rels = np.asarray(rels, dtype=float)[:k]
    if rels.size == 0:
        return 0.0
    return float(np.sum(rels / np.log2(np.arange(2, rels.size + 2))))


def ndcg_at_k(rels, k):
    rels = np.asarray(rels, dtype=float)
    d = dcg_at_k(rels, k)
    ideal = np.sort(rels)[::-1]
    i = dcg_at_k(ideal, k)
    return (d / i) if i > 0 else 0.0


def diversify_topk(pdf, item_to_cat, k=10, max_per_cat=4):
    rows = []
    for uid, g in pdf.groupby("user_id"):
        g = g.sort_values("score", ascending=False)
        picked = []
        cc = defaultdict(int)
        for _, r in g.iterrows():
            if len(picked) >= k:
                break
            c = item_to_cat.get(str(r["item_id"]), "?")
            if cc[c] >= max_per_cat:
                continue
            picked.append(r)
            cc[c] += 1
        if len(picked) < k:
            for _, r in g.iterrows():
                if len(picked) >= k:
                    break
                if any((r["item_id"] == p["item_id"]) for p in picked):
                    continue
                picked.append(r)
        for p in picked[:k]:
            rows.append(p)
    return pd.DataFrame(rows)


def bootstrap_mean(vals, n_boot=400, seed=42):
    rng = np.random.default_rng(seed)
    a = np.asarray(vals, dtype=float)
    if a.size == 0:
        return 0.0, (0.0, 0.0)
    means = [float(rng.choice(a, size=a.size, replace=True).mean()) for _ in range(n_boot)]
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(a.mean()), (float(lo), float(hi))


def main():
    t0 = time.time()

    def _phase(msg):
        print("[recsys_spark] {}".format(msg), flush=True)

    out_dir = Path(os.environ.get("RECSYS_OUT", "/workspace/output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    setup_plot_style()
    pipe_dir = Path("/workspace/pipeline")
    spark = (
        SparkSession.builder.appName("recsys_full")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    # --- Load raw; train/test split (time-last, LOO-positive, or legacy user-random) ---
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("hdfs://namenode:9000/recsys/raw.csv")
    )
    has_event_ts = "event_ts" in raw.columns
    eval_mode = os.environ.get("RECSYS_EVAL", "").strip().lower()
    if not eval_mode:
        eval_mode = "time_last" if has_event_ts else "loo_pos"

    train_raw = None
    test_raw = None
    wrk = Path(os.environ.get("RECSYS_WORKSPACE", "/workspace"))
    split_dir = Path(
        os.environ.get("RECSYS_ML100K_SPLIT_DIR", str(wrk / "data/ml100k/ml-100k"))
    )
    official = os.environ.get("RECSYS_ML100K_OFFICIAL_SPLIT", "").strip().lower()
    if official:
        bf = split_dir / "{}.base".format(official)
        tf = split_dir / "{}.test".format(official)
        if bf.is_file() and tf.is_file():
            bpath = bf.resolve().as_posix()
            tpath = tf.resolve().as_posix()

            def _pair_keys(path):
                return (
                    spark.read.option("header", False)
                    .option("sep", "\t")
                    .csv("file://{}".format(path))
                    .select(
                        F.concat(F.lit("U"), F.col("_c0").cast("string")).alias("user_id"),
                        F.concat(F.lit("I"), F.col("_c1").cast("string")).alias("item_id"),
                    )
                    .distinct()
                )

            train_raw = raw.join(_pair_keys(bpath), ["user_id", "item_id"], "inner")
            test_raw = raw.join(_pair_keys(tpath), ["user_id", "item_id"], "inner")
            protocol_train_test = (
                "Train/test: MovieLens official {}.base / {}.test; profiles from train split only."
            ).format(official, official)
        else:
            print(
                "RECSYS_ML100K_OFFICIAL_SPLIT={} but missing {} or {}".format(
                    official, bf, tf
                )
            )

    if train_raw is None:
        if eval_mode == "user_random":
            all_u = raw.select("user_id").distinct()
            train_u, test_u = all_u.randomSplit([0.75, 0.25], seed=42)
            train_raw = raw.join(train_u, "user_id", "inner")
            test_raw = raw.join(test_u, "user_id", "inner")
            protocol_train_test = (
                "Train/test: USER-level random 75/25 (seed=42); profiles from train users only."
            )
        elif eval_mode == "time_last" and has_event_ts:
            w_last = Window.partitionBy("user_id").orderBy(
                F.col("event_ts").desc(),
                F.col("item_id").asc(),
            )
            ranked = raw.withColumn("rn_last", F.row_number().over(w_last))
            test_raw = ranked.filter(F.col("rn_last") == 1).drop("rn_last")
            train_raw = ranked.filter(F.col("rn_last") > 1).drop("rn_last")
            u_tr = train_raw.select("user_id").distinct()
            test_raw = test_raw.join(u_tr, "user_id", "inner")
            protocol_train_test = (
                "Train/test: TIME — last event per user is test (tie-break by item_id); "
                "profiles from strictly prior train history only."
            )
        else:
            if eval_mode == "time_last" and not has_event_ts:
                print("RECSYS_EVAL=time_last but no event_ts column; using loo_pos.")
            elif eval_mode not in ("loo_pos", "time_last"):
                print(
                    "Unknown RECSYS_EVAL={!r}; using loo_pos.".format(
                        os.environ.get("RECSYS_EVAL", "")
                    )
                )
            eval_mode = "loo_pos"
            pos = raw.filter(F.col("label") == 1)
            w_loo = Window.partitionBy("user_id").orderBy(F.col("item_id").asc())
            pos_r = pos.withColumn("rn_hold", F.row_number().over(w_loo))
            hold = pos_r.filter(F.col("rn_hold") == 1).drop("rn_hold")
            train_raw = raw.join(
                hold.select("user_id", "item_id"),
                ["user_id", "item_id"],
                "left_anti",
            )
            test_raw = hold
            u_tr = train_raw.select("user_id").distinct()
            test_raw = test_raw.join(u_tr, "user_id", "inner")
            protocol_train_test = (
                "Train/test: LOO — one held-out positive per user (min item_id among positives); "
                "profiles from remaining train rows only."
            )

    train_raw.createOrReplaceTempView("raw_events")
    hive_sql = (pipe_dir / "hive_compat.sql").read_text(encoding="utf-8")
    for stmt in hive_sql.split(";"):
        lines = [
            ln
            for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        s = "\n".join(lines).strip()
        if not s:
            continue
        try:
            spark.sql(s)
        except Exception as ex:
            print("hive_compat.sql warning: {}".format(ex))

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW user_profile AS
        SELECT user_id,
               max(age) AS up_age,
               max(gender) AS up_gender,
               max(user_level) AS up_user_level,
               avg(purchase_freq) AS up_purchase_freq,
               avg(register_days) AS up_register_days
        FROM raw_events
        GROUP BY user_id
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW item_profile AS
        SELECT item_id,
               first(category) AS ip_category,
               avg(price) AS ip_price,
               avg(like_num) AS ip_like_num,
               avg(interaction_rate) AS ip_interaction_rate
        FROM raw_events
        GROUP BY item_id
        """
    )

    ratings = spark.read.text("hdfs://namenode:9000/recsys/ratings/*")
    rat_df = (
        ratings.select(
            F.split(F.col("value"), "\t").getItem(0).alias("uk"),
            F.split(F.col("value"), "\t").getItem(1).alias("score_s"),
        )
        .where(F.col("uk").contains("#"))
        .select(
            F.split(F.col("uk"), "#").getItem(0).alias("user_id"),
            F.split(F.col("uk"), "#").getItem(1).alias("item_id"),
            F.col("score_s").cast("double").alias("implicit"),
        )
    )

    up = spark.table("user_profile")
    ip = spark.table("item_profile")

    def build_joined(events_df):
        j = events_df.join(rat_df, ["user_id", "item_id"], "inner")
        j = j.join(up, on="user_id", how="left").join(ip, on="item_id", how="left")
        j = j.withColumn(
            "ip_log_price",
            F.log1p(F.coalesce(F.col("ip_price").cast("double"), F.lit(0.0))),
        ).withColumn(
            "ip_log_like",
            F.log1p(F.coalesce(F.col("ip_like_num").cast("double"), F.lit(0.0))),
        ).withColumn(
            "ip_interaction_rate",
            F.coalesce(F.col("ip_interaction_rate").cast("double"), F.lit(0.0)),
        )
        for c in (
            "up_age",
            "up_gender",
            "up_user_level",
            "up_purchase_freq",
            "up_register_days",
        ):
            j = j.withColumn(
                c, F.coalesce(F.col(c).cast("double"), F.lit(0.0))
            )
        return j

    feat_cols = [
        "up_age",
        "up_gender",
        "up_user_level",
        "up_purchase_freq",
        "up_register_days",
        "ip_log_price",
        "ip_log_like",
        "ip_interaction_rate",
    ]

    train_df_full = build_joined(train_raw)
    test_df = build_joined(test_raw)
    n_users_train = int(train_raw.select("user_id").distinct().count())
    n_users_test = int(test_raw.select("user_id").distinct().count())

    # --- Negative sampling on train (target ~1:2 pos:neg) ---
    pos = train_df_full.filter(F.col("label") == 1)
    neg = train_df_full.filter(F.col("label") == 0)
    pc = pos.count()
    nc = max(neg.count(), 1)
    target_neg = min(nc, pc * 3)
    sample_frac = float(target_neg) / float(nc) if nc else 1.0
    sample_frac = min(1.0, sample_frac)
    neg_s = neg.sample(False, sample_frac, seed=7)
    train_df = pos.unionByName(neg_s)

    train_sub, val_sub = train_df.randomSplit([0.85, 0.15], seed=11)
    train_sub = train_sub.withColumn(
        "weight",
        F.when(F.col("label") == 1, F.lit(2.0)).otherwise(F.lit(1.0)),
    )

    assembler = VectorAssembler(
        inputCols=feat_cols, outputCol="features", handleInvalid="skip"
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        maxIter=45,
        regParam=0.05,
    )
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=12,
        maxDepth=4,
        seed=42,
    )
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=40,
        maxDepth=6,
        seed=42,
        weightCol="weight",
    )

    model_lr = Pipeline(stages=[assembler, lr]).fit(train_sub)
    model_gbt = Pipeline(stages=[assembler, gbt]).fit(train_sub)
    model_rf = Pipeline(stages=[assembler, rf]).fit(train_sub)

    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="label",
        metricName="areaUnderROC",
    )

    def auc_model(model, df):
        if df.limit(1).count() == 0:
            return 0.0
        pred = model.transform(df)
        try:
            return float(evaluator.evaluate(pred))
        except Exception:
            return 0.0

    auc_lr = auc_model(model_lr, val_sub)
    auc_gbt = auc_model(model_gbt, val_sub)
    auc_rf = auc_model(model_rf, val_sub)

    best_name, best_model = max(
        [("LR", model_lr), ("GBT", model_gbt), ("RF", model_rf)],
        key=lambda x: auc_model(x[1], val_sub),
    )

    cf = (
        spark.read.option("header", False)
        .option("inferSchema", True)
        .option("sep", "\t")
        .csv("hdfs://namenode:9000/recsys/cf_candidates/*")
        .toDF("user_id", "item_id", "cf_score")
    )

    truth_pos = (
        test_df.filter(F.col("label") == 1).select("user_id", "item_id").distinct()
    )

    cf_top_m = max(10, min(500, int(os.environ.get("RECSYS_CF_HIT_M", "50"))))
    cf_utest = cf.join(test_df.select("user_id").distinct(), "user_id", "inner")
    w_tm = Window.partitionBy("user_id").orderBy(F.col("cf_score").desc())
    cf_band = cf_utest.withColumn("rk_cf", F.row_number().over(w_tm)).filter(
        F.col("rk_cf") <= F.lit(cf_top_m)
    )
    nt_pos = truth_pos.count()
    n_in_cf_band = truth_pos.alias("tp").join(
        cf_band.select("user_id", "item_id").alias("cf"),
        ["user_id", "item_id"],
        "inner",
    ).count()
    cf_only_topm_hit = (
        float(n_in_cf_band) / float(nt_pos)
        if nt_pos
        else 0.0
    )

    cand = cf.join(up, "user_id", "left").join(ip, "item_id", "left")
    cand = cand.withColumn(
        "ip_log_price",
        F.log1p(F.coalesce(F.col("ip_price").cast("double"), F.lit(0.0))),
    ).withColumn(
        "ip_log_like",
        F.log1p(F.coalesce(F.col("ip_like_num").cast("double"), F.lit(0.0))),
    ).withColumn(
        "ip_interaction_rate",
        F.coalesce(F.col("ip_interaction_rate").cast("double"), F.lit(0.0)),
    )
    for c in (
        "up_age",
        "up_gender",
        "up_user_level",
        "up_purchase_freq",
        "up_register_days",
    ):
        cand = cand.withColumn(
            c, F.coalesce(F.col(c).cast("double"), F.lit(0.0))
        )

    cand_keys = cand.select("user_id", "item_id").distinct()
    missing_gt = truth_pos.join(cand_keys, ["user_id", "item_id"], "left_anti")
    n_gt_not_in_cf = int(missing_gt.count())
    if n_gt_not_in_cf:
        filler = (
            test_df.join(missing_gt, ["user_id", "item_id"], "inner")
            .withColumn("cf_score", F.lit(0.0))
        )
        cand_col_list = cand.columns
        for col in cand_col_list:
            if col not in filler.columns:
                filler = filler.withColumn(
                    col,
                    F.lit(None).cast(cand.schema[col].dataType),
                )
        cand = cand.unionByName(filler.select(*cand_col_list))

    cand_labeled = cand.join(
        raw.select("user_id", "item_id", "label"),
        ["user_id", "item_id"],
        "left",
    ).withColumn("label", F.coalesce(F.col("label"), F.lit(0)))

    scored = best_model.transform(cand_labeled)
    scored = scored.withColumn("p_buy", vector_to_array(F.col("probability"))[1])

    wu = Window.partitionBy("user_id")
    scored = scored.withColumn("cf_max", F.max("cf_score").over(wu)).withColumn(
        "cf_min", F.min("cf_score").over(wu)
    )
    scored = scored.withColumn(
        "cf_norm",
        (F.col("cf_score") - F.col("cf_min"))
        / (F.col("cf_max") - F.col("cf_min") + F.lit(1e-9)),
    )

    val_users_df = val_sub.select("user_id").distinct()
    _phase(
        "scored all candidates built; pulling val-users rows to pandas (may take "
        "several minutes on large MR candidate lists)…"
    )
    cand_val_pdf = (
        scored.join(val_users_df, "user_id", "inner")
        .select("user_id", "item_id", "p_buy", "cf_norm", "label")
        .toPandas()
    )

    alphas = np.linspace(0.0, 1.0, 11)
    auc_by_alpha = []
    if len(cand_val_pdf) > 5 and cand_val_pdf["label"].nunique() > 1:
        y = cand_val_pdf["label"].values
        for a in alphas:
            s = a * cand_val_pdf["p_buy"].values + (1.0 - a) * cand_val_pdf[
                "cf_norm"
            ].values
            try:
                auc_by_alpha.append(roc_auc_score(y, s))
            except ValueError:
                auc_by_alpha.append(0.0)
    else:
        auc_by_alpha = [0.0] * len(alphas)

    auc_val_p = 0.0
    auc_val_cf = 0.0
    if len(cand_val_pdf) > 5 and cand_val_pdf["label"].nunique() > 1:
        yv = cand_val_pdf["label"].values
        try:
            auc_val_p = float(roc_auc_score(yv, cand_val_pdf["p_buy"].values))
            auc_val_cf = float(roc_auc_score(yv, cand_val_pdf["cf_norm"].values))
        except ValueError:
            pass

    best_alpha = float(alphas[int(np.argmax(auc_by_alpha))]) if auc_by_alpha else 0.55
    if max(auc_by_alpha) == 0:
        best_alpha = 0.55

    scored = scored.withColumn(
        "fused",
        F.lit(best_alpha) * F.col("p_buy")
        + (F.lit(1.0) - F.lit(best_alpha)) * F.col("cf_norm"),
    )

    truth_rows = test_df.filter(F.col("label") == 1).select("user_id", "item_id")
    truth_map = {
        r.user_id: set(r.truth_items)
        for r in truth_rows.groupBy("user_id")
        .agg(F.collect_set("item_id").alias("truth_items"))
        .collect()
    }
    global_pos_items = set(
        r.item_id for r in truth_rows.select("item_id").distinct().collect()
    )

    def topk_pdf(df, score_col, k=10):
        w = Window.partitionBy("user_id").orderBy(F.col(score_col).desc())
        r = (
            df.withColumn("rk", F.row_number().over(w))
            .where(F.col("rk") <= k)
            .select("user_id", "item_id", F.col(score_col).alias("score"))
        )
        return r.toPandas()

    _phase("computing per-user top-10 by fused (Spark window + shuffle)…")
    pdf_fused = topk_pdf(scored, "fused", 10)
    item_cat_pdf = raw.groupBy("item_id").agg(F.max("category").alias("category")).toPandas()
    item_to_cat = dict(zip(item_cat_pdf["item_id"].astype(str), item_cat_pdf["category"].astype(str)))
    pdf_div = diversify_topk(pdf_fused, item_to_cat, k=10, max_per_cat=4)

    def precision_recall_strict(pdf_top):
        precs, recs = [], []
        for uid, g in pdf_top.groupby("user_id"):
            gt = truth_map.get(uid)
            if not gt:
                continue
            items_pred = set(g["item_id"].tolist())
            hit = len(items_pred & gt)
            precs.append(hit / 10.0)
            recs.append(hit / max(len(gt), 1))
        if not precs:
            return 0.0, 0.0
        return float(np.mean(precs)), float(np.mean(recs))

    def hit_rate_global_pool(pdf_top):
        hits, total = 0, 0
        for _, g in pdf_top.groupby("user_id"):
            for it in g["item_id"].tolist():
                total += 1
                if it in global_pos_items:
                    hits += 1
        return (hits / total) if total else 0.0

    def recall_at_k(pdf_top, k=10):
        recalls = []
        for uid, g in pdf_top.groupby("user_id"):
            gt = truth_map.get(uid)
            if not gt:
                continue
            pred = set(g["item_id"].tolist()[:k])
            recalls.append(len(pred & gt) / max(len(gt), 1))
        return float(np.mean(recalls)) if recalls else 0.0

    def mean_ndcg(pdf_top, k=10):
        nds = []
        for uid, g in pdf_top.groupby("user_id"):
            gt = truth_map.get(uid)
            if not gt:
                continue
            rels = [1.0 if r["item_id"] in gt else 0.0 for _, r in g.iterrows()]
            nds.append(ndcg_at_k(rels, k))
        return float(np.mean(nds)) if nds else 0.0

    p_f, r_f = precision_recall_strict(pdf_div)
    _phase("computing per-user top-10 by cf_norm…")
    pdf_cf_top = topk_pdf(scored, "cf_norm", 10)
    g_cf = hit_rate_global_pool(pdf_cf_top)
    g_f = hit_rate_global_pool(pdf_div)
    r10 = recall_at_k(pdf_div, 10)
    n10 = mean_ndcg(pdf_div, 10)

    user_hits = []
    for uid, g in pdf_div.groupby("user_id"):
        items_pred = set(g["item_id"].tolist())
        gt = truth_map.get(uid, set())
        user_hits.append(len(items_pred & gt) / 10.0)
    boot_mean, boot_ci = bootstrap_mean(user_hits, n_boot=400, seed=99)

    # PR / ROC on labeled candidates (test users only)
    test_users_df = test_df.select("user_id").distinct()
    _phase(
        "pulling ALL test-user candidate pairs to pandas for ROC/AP (often the "
        "slowest step: size ≈ (#test users)×(MR candidates per user))…"
    )
    cand_test_pdf = (
        scored.join(test_users_df, "user_id", "inner")
        .select("p_buy", "cf_norm", "fused", "label")
        .toPandas()
    )
    if len(cand_test_pdf) > 10 and cand_test_pdf["label"].nunique() > 1:
        y = cand_test_pdf["label"].values
        try:
            auc_f = roc_auc_score(y, cand_test_pdf["fused"].values)
            ap_f = average_precision_score(y, cand_test_pdf["fused"].values)
        except ValueError:
            auc_f, ap_f = 0.0, 0.0
        fpr, tpr, _ = roc_curve(y, cand_test_pdf["fused"].values)
        prec, rec, _ = precision_recall_curve(y, cand_test_pdf["fused"].values)
        fig, ax = plt.subplots(1, 2, figsize=(10, 4))
        ax[0].plot(fpr, tpr, label="ROC AUC={:.3f}".format(auc_f))
        ax[0].plot([0, 1], [0, 1], "k--")
        ax[0].set_xlabel("FPR")
        ax[0].set_ylabel("TPR")
        ax[0].legend()
        ax[0].set_title("ROC（候选对上的 fused 分数）")
        ax[1].plot(rec, prec, label="AP={:.3f}".format(ap_f))
        ax[1].set_xlabel("Recall")
        ax[1].set_ylabel("Precision")
        ax[1].legend()
        ax[1].set_title("PR 曲线")
        fig.tight_layout()
        fig.savefig(out_dir / "chart_roc_pr.png", dpi=150, bbox_inches="tight")
        plt.close(fig)
    else:
        auc_f, ap_f = 0.0, 0.0

    _phase("writing α-sweep chart…")
    # Alpha curve
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(alphas, auc_by_alpha, "o-", color="#264653")
    ax.axvline(best_alpha, color="red", linestyle="--", label="best α={:.2f}".format(best_alpha))
    ax.set_xlabel("融合权重 α（所选分类器概率 p_buy）")
    ax.set_ylabel("验证集 AUC（候选对）")
    ax.set_title("α 扫描（验证集）")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_alpha_sweep.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    _phase("stratified tables (Spark joins + pandas)…")
    # Stratified: gender
    gen_pdf = (
        spark.createDataFrame(pdf_div)
        .join(raw.select("user_id", "gender").distinct(), "user_id", "left")
        .toPandas()
    )
    strat_rows = []
    for gval, g in gen_pdf.groupby("gender"):
        hrs = []
        for uid, gg in g.groupby("user_id"):
            st = set(gg["item_id"].tolist())
            hrs.append(len(st & global_pos_items) / 10.0)
        if hrs:
            strat_rows.append({"bucket": "gender={}".format(gval), "mean_hit10": float(np.mean(hrs)), "n_users": len(hrs)})
    pd.DataFrame(strat_rows).to_csv(out_dir / "stratified_gender.csv", index=False)

    # Stratified: activity (event count tertiles from hive_user_rollups)
    _phase("loading hive_user_rollups to pandas…")
    act_pdf = spark.table("hive_user_rollups").select("user_id", "event_cnt").toPandas()
    am = pdf_div.merge(act_pdf, on="user_id", how="left")
    am["event_cnt"] = am["event_cnt"].fillna(0.0)
    try:
        am["act_bucket"] = pd.qcut(
            am["event_cnt"],
            q=3,
            labels=["low_act", "mid_act", "high_act"],
            duplicates="drop",
        )
    except (ValueError, TypeError):
        am["act_bucket"] = "all"
    strat_act = []
    for bname, g in am.groupby("act_bucket", dropna=False):
        hrs = []
        for uid, gg in g.groupby("user_id"):
            st = set(gg["item_id"].tolist())
            hrs.append(len(st & global_pos_items) / 10.0)
        if hrs:
            strat_act.append(
                {
                    "bucket": str(bname),
                    "mean_hit10": float(np.mean(hrs)),
                    "n_users": len(hrs),
                }
            )
    pd.DataFrame(strat_act).to_csv(out_dir / "stratified_activity.csv", index=False)

    # Stratified: dominant category (most frequent category in user history)
    _phase("dominant category per user (window over raw)…")
    uc = raw.groupBy("user_id", "category").count().withColumnRenamed("count", "cc")
    wdom = Window.partitionBy("user_id").orderBy(F.desc("cc"), F.asc("category"))
    dom_pdf = (
        uc.withColumn("rn", F.row_number().over(wdom))
        .where(F.col("rn") == 1)
        .select("user_id", F.col("category").alias("dom_category"))
        .toPandas()
    )
    cm = pdf_div.merge(dom_pdf, on="user_id", how="left")
    strat_dom = []
    for cval, g in cm.groupby("dom_category", dropna=False):
        hrs = []
        for uid, gg in g.groupby("user_id"):
            st = set(gg["item_id"].tolist())
            hrs.append(len(st & global_pos_items) / 10.0)
        if hrs:
            strat_dom.append(
                {
                    "bucket": "dom_cat={}".format(cval),
                    "mean_hit10": float(np.mean(hrs)),
                    "n_users": len(hrs),
                }
            )
    pd.DataFrame(strat_dom).to_csv(out_dir / "stratified_dom_category.csv", index=False)

    lines = [
        "=== Protocol ===",
        protocol_train_test,
        "Features (classifiers): train-only user_profile + item_profile aggregates only; "
        "no row-level like_num/rating, interaction_rate, purchase_intent, or MR implicit.",
        "Default RECSYS_EVAL: time_last if CSV has event_ts, else loo_pos. "
        "Override: time_last | loo_pos | user_random.",
        "Optional MovieLens fixed split: RECSYS_ML100K_OFFICIAL_SPLIT=u1|u2|...|ua|ub "
        "(reads {split}.base / {split}.test under RECSYS_ML100K_SPLIT_DIR, default data/ml100k/ml-100k).",
        "Train internal: 85/15 train/val (seed=11) on train split rows for model & α.",
        "Negative sampling: train negatives downsampled toward ~1:3 pos:neg on train split.",
        "Item-CF: user-level co-occurrence with category proxy item (see report).",
        "Fusion: best model among LR/GBT/RF by val AUC; α chosen by val AUC on labeled candidates.",
        "",
        "=== Candidates (ranking coverage) ===",
        "Plain CF: fraction of test (user,item) positives in MR CF candidate list ranked top-{:d} "
        "by cf_score (among test users): {:.4f}.".format(cf_top_m, cf_only_topm_hit),
        "Rows injected before model scoring (truth in test but missing from MR CF list): {}".format(
            n_gt_not_in_cf
        ),
        "",
        "=== Models (val AUC) ===",
        "LR={:.4f} GBT={:.4f} RF={:.4f} | selected={}".format(
            auc_lr, auc_gbt, auc_rf, best_name
        ),
        "Validation ROC-AUC: p_buy={:.4f}  cf_norm={:.4f} (candidate rows pooled with fused α scan)".format(
            auc_val_p,
            auc_val_cf,
        ),
        "best_alpha={:.4f}".format(best_alpha),
        "",
        "=== Test users (diversified Top10, max 4/category) ===",
        "Strict Precision@10={:.4f} Recall@10={:.4f}".format(p_f, r_f),
        "Global hit rate@10 fused={:.4f} cf_only={:.4f} Recall@10(rel)={:.4f} NDCG@10={:.4f}".format(
            g_f, g_cf, r10, n10
        ),
        "Bootstrap mean strict-P@10: {:.4f} 95% CI [{:.4f}, {:.4f}]".format(
            boot_mean, boot_ci[0], boot_ci[1]
        ),
        "",
        "=== Candidates ROC (test users, fused) ===",
        "AUC={:.4f} AP={:.4f}".format(auc_f, ap_f),
        "",
        "=== Cold start (engineering note) ===",
        "New item: fallback to category top-popular from hive_item_rollups / compute_cat_top1.",
        "New user: demographic average features + popular-in-category candidates.",
        "Diversity: greedy cap 4 items per category in Top-10 (post-rank).",
        "",
        "=== Hive / SQL ===",
        "hive_compat.sql runs in Spark SQL (HiveQL-compatible); no HiveServer required for this repo.",
        "Rollup views: hive_user_rollups, hive_item_rollups (see pipeline/hive_compat.sql).",
        "",
        "=== Caveat (temporal vs MR) ===",
        "Co-occurrence / CF MR jobs read full HDFS raw.csv; time-based evaluation removes label/feature "
        "leakage in Spark, but CF still exploits future interactions vs the test holdout unless MR is "
        "re-run on train-only rows.",
        "",
        "=== Hadoop / YARN (course) ===",
        "MapReduce: set RECSYS_USE_YARN=1 in pipeline/run_mapreduce.sh context; YARN UI http://localhost:8088 .",
        "Docker compose sets HADOOP_OPTS=-Djava.net.preferIPv4Stack=true to ease RM host resolution.",
        "MR stage timings: output/pipeline_mr_timing.txt ; container memory: see hadoop-docker/hadoop.env (map 512MB, reduce 1536MB heap).",
        "",
        "=== Runtime ===",
        "spark_pipeline_sec={:.1f}".format(time.time() - t0),
    ]
    (out_dir / "metrics.txt").write_text("\n".join(lines), encoding="utf-8")

    _phase("writing remaining charts…")
    # Category chart (diversified)
    fused_items = pdf_div.merge(item_cat_pdf, on="item_id", how="left")
    hit_cat = fused_items.groupby("category").size().reset_index(name="rec_count")
    top_cat = hit_cat.sort_values("rec_count", ascending=False).head(12)
    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(top_cat))
    ax.bar(x, top_cat["rec_count"].values, color="#457b9d", edgecolor="#1d3557", linewidth=0.5)
    ax.set_xticks(list(x))
    ax.set_xticklabels(top_cat["category"].astype(str).tolist(), rotation=40, ha="right")
    ax.set_xlabel("商品品类")
    ax.set_ylabel("进入 Top-10 次数（多样性截断后）")
    ax.set_title("Top-10 品类分布（同品类≤4）")
    ax.set_ylim(0, max(top_cat["rec_count"].max() * 1.15, 1))
    fig.tight_layout()
    fig.savefig(out_dir / "chart_hitrate_category.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # User level
    ul = pdf_div.merge(raw.select("user_id", "user_level").distinct().toPandas(), on="user_id", how="left")
    rows = []
    for lvl, g in ul.groupby("user_level"):
        hrs = []
        for _, gg in g.groupby("user_id"):
            st = set(gg["item_id"].tolist())
            hrs.append(len(st & global_pos_items) / 10.0)
        if hrs:
            rows.append((float(lvl), float(np.mean(hrs))))
    lvl_acc = pd.DataFrame(rows, columns=["user_level", "hit"]).sort_values("user_level")
    fig, ax = plt.subplots(figsize=(7, 4))
    if len(lvl_acc):
        ax.plot(lvl_acc["user_level"], lvl_acc["hit"], "o-", color="#e63946", linewidth=2)
    ax.set_xlabel("user_level")
    ax.set_ylabel("平均全局命中率@10")
    ax.set_title("用户等级 vs 命中率")
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_user_level_accuracy.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4.5))
    vals = [g_cf, g_f]
    bars = ax.bar(
        ["仅 CF\n(cf_norm)", "融合\n(α={:.2f})".format(best_alpha)],
        vals,
        color=["#6a8caf", "#2a9d8f"],
        edgecolor="#264653",
    )
    ax.set_ylabel("Global hit rate @10")
    ax.set_title("CF vs 融合（测试集；CF=纯协同归一化排序）")
    ax.set_ylim(0, max(0.12, max(vals) * 1.35))
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.001, "{:.2%}".format(v), ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(out_dir / "chart_cf_vs_fused.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # --- Extra charts for report ---
    fig, ax = plt.subplots(figsize=(5.5, 4))
    mnames = ["LR", "GBT", "RF"]
    mvals = [auc_lr, auc_gbt, auc_rf]
    bar_colors = ["#457b9d", "#457b9d", "#457b9d"]
    bi = mnames.index(best_name) if best_name in mnames else 0
    bar_colors[bi] = "#e63946"
    ax.bar(mnames, mvals, color=bar_colors, edgecolor="#1d3557", linewidth=0.6)
    ax.set_ylabel("验证集 AUC")
    ax.set_title("分类器对比（验证集，选中={}）".format(best_name))
    ax.set_ylim(0, min(1.08, max(mvals) * 1.12 + 0.02) if mvals else 1.0)
    ax.grid(True, axis="y", alpha=0.3)
    for i, v in enumerate(mvals):
        ax.text(i, v + 0.02, "{:.3f}".format(v), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_model_val_auc.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    def _plot_strat_bar(df_strat, ax, title, max_rows=10):
        if df_strat is None or len(df_strat) == 0:
            ax.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(title)
            return
        d = df_strat.sort_values("mean_hit10", ascending=True).tail(max_rows)
        y = np.arange(len(d))
        ax.barh(y, d["mean_hit10"].values, color="#2a9d8f", edgecolor="#264653", height=0.65)
        ax.set_yticks(y)
        lbl = d["bucket"].astype(str).str.replace("dom_cat=", "").str.replace("gender=", "")
        ax.set_yticklabels(lbl, fontsize=8)
        ax.set_xlabel("平均全局命中率@10")
        ax.set_title(title)
        ax.set_xlim(0, max(0.05, d["mean_hit10"].max() * 1.15))

    df_sg = pd.DataFrame(strat_rows) if strat_rows else pd.DataFrame()
    df_sa = pd.DataFrame(strat_act) if strat_act else pd.DataFrame()
    df_sd = pd.DataFrame(strat_dom) if strat_dom else pd.DataFrame()
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2))
    _plot_strat_bar(df_sg, axes[0], "按性别")
    _plot_strat_bar(df_sa, axes[1], "按活跃度分桶")
    _plot_strat_bar(df_sd, axes[2], "按主导品类（Top10 桶）", max_rows=10)
    fig.suptitle("分层：全局命中率@10（测试用户）", y=1.02, fontsize=12)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_stratified_global_hit.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    metric_names = ["Strict\nP@10", "Strict\nR@10", "Global\nhit@10", "Recall@10\n(rel)", "NDCG@10"]
    metric_vals = [p_f, r_f, g_f, r10, n10]
    x = np.arange(len(metric_names))
    ax.bar(x, metric_vals, color=["#457b9d", "#457b9d", "#2a9d8f", "#e9c46a", "#f4a261"], edgecolor="#264653")
    ax.set_xticks(x)
    ax.set_xticklabels(metric_names, fontsize=9)
    ax.set_ylabel("数值")
    ax.set_title("测试集排序 / 命中指标汇总（多样性 Top10）")
    ax.set_ylim(0, max(0.08, max(metric_vals) * 1.25) if metric_vals else 1.0)
    ax.grid(True, axis="y", alpha=0.3)
    for i, v in enumerate(metric_vals):
        ax.text(i, v + max(metric_vals) * 0.02 if metric_vals else 0.02, "{:.3f}".format(v), ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_ranking_metrics.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(user_hits, bins=11, range=(0, 1.05), color="#9b5de5", edgecolor="#3d1f5c", alpha=0.85)
    ax.set_xlabel("每用户 Strict Precision@10")
    ax.set_ylabel("用户数")
    ax.set_title("测试用户：Strict P@10 分布（Bootstrap mean={:.4f}）".format(boot_mean))
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "chart_strict_p_hist.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(
        ["训练用户", "测试用户"],
        [n_users_train, n_users_test],
        color=["#264653", "#2a9d8f"],
        edgecolor="#1d3557",
    )
    ax.set_ylabel("用户数（train_raw / test_raw 去重 user_id）")
    ax.set_title("Train / test 用户数（当前切分协议）")
    for i, v in enumerate([n_users_train, n_users_test]):
        ax.text(i, v + max(n_users_train, n_users_test) * 0.02, str(v), ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(out_dir / "chart_train_test_users.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    if len(cand_test_pdf) > 10 and cand_test_pdf["label"].nunique() > 1:
        fig, ax = plt.subplots(figsize=(6, 4))
        s0 = cand_test_pdf.loc[cand_test_pdf["label"] == 0, "fused"]
        s1 = cand_test_pdf.loc[cand_test_pdf["label"] == 1, "fused"]
        ax.hist(
            [s0.dropna().values, s1.dropna().values],
            bins=25,
            label=["label=0", "label=1"],
            color=["#8d99ae", "#e63946"],
            alpha=0.75,
            edgecolor="white",
        )
        ax.set_xlabel("fused 分数")
        ax.set_ylabel("候选对条数")
        ax.set_title("测试集候选：fused 分数分布（按标签）")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_dir / "chart_fused_score_by_label.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

    spark.stop()
    print("Wrote outputs to {}".format(out_dir))


if __name__ == "__main__":
    main()
