#!/usr/bin/env python3
"""
Spark: HiveQL-compat views, train/val protocol, negative sampling, LR/GBT/RF,
fusion alpha sweep, AUC/PR/NDCG, stratified metrics, bootstrap CI, diversity.
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

    # --- Hive-compatible SQL (same dialect as hive_compat.sql) ---
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("hdfs://namenode:9000/recsys/raw.csv")
    )
    raw.createOrReplaceTempView("raw_events")
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

    joined = raw.join(rat_df, ["user_id", "item_id"], "inner")
    joined = joined.join(
        spark.table("user_profile"), on="user_id", how="left"
    ).join(spark.table("item_profile"), on="item_id", how="left")

    joined = joined.withColumn(
        "log_price", F.log1p(F.col("price").cast("double"))
    ).withColumn("log_like", F.log1p(F.col("like_num").cast("double")))

    feat_cols = [
        "age",
        "gender",
        "user_level",
        "purchase_freq",
        "register_days",
        "log_price",
        "log_like",
        "interaction_rate",
        "implicit",
    ]
    for c in feat_cols:
        joined = joined.withColumn(
            c, F.coalesce(F.col(c).cast("double"), F.lit(0.0))
        )

    # --- User-level train / test (avoid same user in both) ---
    all_users = joined.select("user_id").distinct()
    train_u, test_u = all_users.randomSplit([0.75, 0.25], seed=42)
    train_df_full = joined.join(train_u, "user_id", "inner")
    test_df = joined.join(test_u, "user_id", "inner")

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

    users = raw.groupBy("user_id").agg(
        F.max(F.col("age")).alias("age"),
        F.max(F.col("gender")).alias("gender"),
        F.max(F.col("user_level")).alias("user_level"),
        F.avg(F.col("purchase_freq")).alias("purchase_freq"),
        F.avg(F.col("register_days")).alias("register_days"),
    )
    items = raw.groupBy("item_id").agg(
        F.avg(F.col("price")).alias("price"),
        F.avg(F.col("like_num")).alias("like_num"),
        F.avg(F.col("interaction_rate")).alias("interaction_rate"),
    )

    cand = cf.join(users, "user_id", "left").join(items, "item_id", "left")
    cand = cand.withColumn(
        "log_price", F.log1p(F.col("price").cast("double"))
    ).withColumn("log_like", F.log1p(F.col("like_num").cast("double")))
    cand = cand.withColumn("implicit", F.lit(0.0))
    for c in feat_cols:
        cand = cand.withColumn(
            c, F.coalesce(F.col(c).cast("double"), F.lit(0.0))
        )

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
        "Train/test: USER-level split 75/25 (seed=42); no user in both sets.",
        "Train internal: 85/15 train/val (seed=11) for model & alpha selection.",
        "Negative sampling: train negatives downsampled toward ~1:3 pos:neg on train users.",
        "Item-CF: user-level co-occurrence with category proxy item (see report).",
        "Fusion: best model among LR/GBT/RF by val AUC; α chosen by val AUC on labeled candidates.",
        "",
        "=== Models (val AUC) ===",
        "LR={:.4f} GBT={:.4f} RF={:.4f} | selected={}".format(auc_lr, auc_gbt, auc_rf, best_name),
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
        "=== Hadoop / YARN (course) ===",
        "MapReduce: set RECSYS_USE_YARN=1 in pipeline/run_mapreduce.sh context; YARN UI http://localhost:8088 .",
        "Docker compose sets HADOOP_OPTS=-Djava.net.preferIPv4Stack=true to ease RM host resolution.",
        "MR stage timings: output/pipeline_mr_timing.txt ; container memory: see hadoop-docker/hadoop.env (map 512MB, reduce 1536MB heap).",
        "",
        "=== Runtime ===",
        "spark_pipeline_sec={:.1f}".format(time.time() - t0),
    ]
    (out_dir / "metrics.txt").write_text("\n".join(lines), encoding="utf-8")

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

    spark.stop()
    print("Wrote outputs to {}".format(out_dir))


if __name__ == "__main__":
    main()
