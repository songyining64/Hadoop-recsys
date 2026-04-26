#!/usr/bin/env bash
set -euo pipefail
export PYTHONIOENCODING=utf-8
export PATH="/opt/hadoop-3.2.1/bin:/opt/hadoop-3.2.1/sbin:${PATH:-}"
STREAM_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
ROOT="/workspace"
PIPE="${ROOT}/pipeline"
RAW_HDFS="/recsys/raw.csv"
TMP="${PIPE}/.mr_tmp"
mkdir -p "${TMP}" "${ROOT}/output"
TIME_LOG="${ROOT}/output/pipeline_mr_timing.txt"

logt() { echo "[$(date -Iseconds)] $*" | tee -a "${TIME_LOG}"; }

if [[ "${RECSYS_USE_YARN:-0}" == "1" ]]; then
  MR_LOCAL=()
  logt "MapReduce framework: YARN"
else
  MR_LOCAL=(-D mapreduce.framework.name=local)
  logt "MapReduce framework: local (set RECSYS_USE_YARN=1 for YARN)"
fi

T_ALL=$(date +%s)
logt "=== MapReduce pipeline start ==="

hdfs dfs -rm -r -f /recsys || true
hdfs dfs -mkdir -p /recsys
hdfs dfs -put -f "${ROOT}/social_ecommerce_data.csv" "${RAW_HDFS}"

echo "=== Category top-1 item (proxy for user-level co-occurrence) ==="
T=$(date +%s)
python3 "${PIPE}/compute_cat_top1.py" "${ROOT}/social_ecommerce_data.csv" > "${TMP}/cat_top1.tsv"
hdfs dfs -put -f "${TMP}/cat_top1.tsv" /recsys/cat_top1.tsv
logt "cat_top1 local+hdfs $(( $(date +%s) - T ))s"

echo "=== Job A1 ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/a1 || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_A1 \
  -D stream.num.map.output.key.fields=1 \
  -files "${PIPE}/mr_a1_map.py,${PIPE}/mr_a1_red.py" \
  -mapper "python3 mr_a1_map.py" \
  -reducer "python3 mr_a1_red.py" \
  -input "${RAW_HDFS}" \
  -output /recsys/a1
logt "Job A1 $(( $(date +%s) - T ))s"

echo "=== Job A2 ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/ratings || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_A2 \
  -D stream.num.map.output.key.fields=1 \
  -files "${PIPE}/mr_a2_map.py,${PIPE}/mr_a2_red.py" \
  -mapper "python3 mr_a2_map.py" \
  -reducer "python3 mr_a2_red.py" \
  -input /recsys/a1 \
  -output /recsys/ratings
logt "Job A2 $(( $(date +%s) - T ))s"

echo "=== Item totals (for cosine) ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/item_totals || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_item_totals \
  -D stream.num.map.output.key.fields=1 \
  -files "${PIPE}/mr_item_totals_map.py,${PIPE}/mr_item_totals_red.py" \
  -mapper "python3 mr_item_totals_map.py" \
  -reducer "python3 mr_item_totals_red.py" \
  -input /recsys/ratings \
  -output /recsys/item_totals
logt "item_totals $(( $(date +%s) - T ))s"

echo "=== B1a user-level co-occurrence (real item + category proxy item) ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/b1a || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_B1a_user_proxy \
  -D stream.num.map.output.key.fields=1 \
  -files "${TMP}/cat_top1.tsv#cat_top1.tsv,${PIPE}/mr_b1_user_proxy_map.py,${PIPE}/mr_b1_red.py" \
  -mapper "python3 mr_b1_user_proxy_map.py" \
  -reducer "python3 mr_b1_red.py" \
  -input "${RAW_HDFS}" \
  -output /recsys/b1a
logt "B1a $(( $(date +%s) - T ))s"

echo "=== B1b sum co-occurrence ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/cooc || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_B1b_cooc \
  -D stream.num.map.output.key.fields=2 \
  -files "${PIPE}/mr_cooc_sum_red.py" \
  -mapper "cat" \
  -reducer "python3 mr_cooc_sum_red.py" \
  -input /recsys/b1a \
  -output /recsys/cooc
logt "B1b $(( $(date +%s) - T ))s"

echo "=== Purchased pairs ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/purchases || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_purchases \
  -D stream.num.map.output.key.fields=1 \
  -files "${PIPE}/mr_purchase_map.py,${PIPE}/mr_purchase_red.py" \
  -mapper "python3 mr_purchase_map.py" \
  -reducer "python3 mr_purchase_red.py" \
  -input "${RAW_HDFS}" \
  -output /recsys/purchases
logt "purchases $(( $(date +%s) - T ))s"

hdfs dfs -getmerge -nl /recsys/item_totals "${TMP}/item_totals.txt"

echo "=== B2 cosine similarity ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/item_sim || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_B2_cosine \
  -D stream.num.map.output.key.fields=2 \
  -files "${TMP}/item_totals.txt#item_totals.txt,${PIPE}/mr_b2_map.py,${PIPE}/mr_b2_red.py" \
  -mapper "python3 mr_b2_map.py" \
  -reducer "python3 mr_b2_red.py" \
  -input /recsys/cooc \
  -output /recsys/item_sim
logt "B2 $(( $(date +%s) - T ))s"

hdfs dfs -getmerge -nl /recsys/item_sim "${TMP}/item_sim_merge.txt"
python3 "${PIPE}/cf_build_topn.py" < "${TMP}/item_sim_merge.txt" > "${TMP}/item_topn.txt"
hdfs dfs -rm -r -f /recsys/item_topn || true
hdfs dfs -mkdir -p /recsys/item_topn
hdfs dfs -put -f "${TMP}/item_topn.txt" /recsys/item_topn/part-00000

hdfs dfs -getmerge -nl /recsys/ratings "${TMP}/ratings_merge.txt"
hdfs dfs -getmerge -nl /recsys/purchases "${TMP}/purchases_merge.txt"
hdfs dfs -getmerge -nl /recsys/item_topn "${TMP}/topn_merge.txt"

echo "=== B3 CF candidates ==="
T=$(date +%s)
python3 "${PIPE}/cf_gen_candidates.py" \
  "${TMP}/ratings_merge.txt" \
  "${TMP}/purchases_merge.txt" \
  "${TMP}/topn_merge.txt" \
  > "${TMP}/cf_candidates.tsv"

hdfs dfs -rm -r -f /recsys/cf_candidates || true
hdfs dfs -mkdir -p /recsys/cf_candidates
hdfs dfs -put -f "${TMP}/cf_candidates.tsv" /recsys/cf_candidates/part-00000
logt "cf_candidates $(( $(date +%s) - T ))s"

logt "=== MapReduce total $(( $(date +%s) - T_ALL ))s ==="
hdfs dfs -ls -R /recsys >> "${TIME_LOG}" 2>&1 || true
