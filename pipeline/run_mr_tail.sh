#!/usr/bin/env bash
# Continue pipeline from B2 (assumes /recsys/cooc, ratings, purchases, item_totals exist).
set -euo pipefail
export PYTHONIOENCODING=utf-8
export PYTHONUNBUFFERED=1
PY="${PY:-python3 -u}"
export PATH="/opt/hadoop-3.2.1/bin:/opt/hadoop-3.2.1/sbin:${PATH:-}"
STREAM_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
ROOT="/workspace"
PIPE="${ROOT}/pipeline"
TMP="${PIPE}/.mr_tmp"
mkdir -p "${TMP}" "${ROOT}/output"
TIME_LOG="${ROOT}/output/pipeline_mr_timing.txt"
logt() { echo "[$(date -Iseconds)] $*" | tee -a "${TIME_LOG}"; }

if [[ "${RECSYS_USE_YARN:-0}" == "1" ]]; then
  MR_LOCAL=()
else
  MR_LOCAL=(-D mapreduce.framework.name=local)
fi

T_ALL=$(date +%s)
hdfs dfs -getmerge -nl /recsys/item_totals "${TMP}/item_totals.txt"

echo "=== B2 cosine similarity ==="
T=$(date +%s)
hdfs dfs -rm -r -f /recsys/item_sim || true
hadoop jar "${STREAM_JAR}" \
  "${MR_LOCAL[@]}" \
  -D mapreduce.job.name=recsys_B2_cosine \
  -D stream.num.map.output.key.fields=2 \
  -files "${TMP}/item_totals.txt#item_totals.txt,${PIPE}/mr_b2_map.py,${PIPE}/mr_b2_red.py" \
  -mapper "${PY} mr_b2_map.py" \
  -reducer "${PY} mr_b2_red.py" \
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

logt "=== MR tail total $(( $(date +%s) - T_ALL ))s ==="
