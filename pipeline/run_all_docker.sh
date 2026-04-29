#!/usr/bin/env bash
# Run from project root on the host (not inside a container):
#   bash pipeline/run_all_docker.sh
# MovieLens 100K (place u.data, u.item, u.user under data/ml100k or set ML100K_DIR):
#   RECSYS_DATASET=ml100k bash pipeline/run_all_docker.sh
# Optional: run MapReduce on YARN instead of local mode:
#   RECSYS_USE_YARN=1 bash pipeline/run_all_docker.sh
# Skip image rebuild if layers already local (Hub 超时时可先 docker pull 再设此变量):
#   SKIP_DOCKER_BUILD=1 bash pipeline/run_all_docker.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "${RECSYS_DATASET:-ecommerce}" == "ml100k" ]]; then
  ML100K_DIR="${ML100K_DIR:-${ROOT}/data/ml100k}"
  echo "== Build recsys CSV from MovieLens 100K: ${ML100K_DIR} =="
  python3 "${ROOT}/pipeline/ml100k_build_csv.py" "${ML100K_DIR}" "${ROOT}/recsys_ml100k.csv"
fi

cd "${ROOT}/hadoop-docker"

MR_ENV=( -e "RECSYS_USE_YARN=${RECSYS_USE_YARN:-0}" )
if [[ "${RECSYS_DATASET:-ecommerce}" == "ml100k" ]]; then
  MR_ENV+=( -e "RECSYS_RAW=/workspace/recsys_ml100k.csv" )
fi

SPARK_ENV=()
[[ -n "${RECSYS_ML100K_OFFICIAL_SPLIT:-}" ]] && SPARK_ENV+=( -e "RECSYS_ML100K_OFFICIAL_SPLIT=${RECSYS_ML100K_OFFICIAL_SPLIT}" )
[[ -n "${RECSYS_ML100K_SPLIT_DIR:-}" ]] && SPARK_ENV+=( -e "RECSYS_ML100K_SPLIT_DIR=${RECSYS_ML100K_SPLIT_DIR}" )
[[ -n "${RECSYS_EVAL:-}" ]] && SPARK_ENV+=( -e "RECSYS_EVAL=${RECSYS_EVAL}" )
[[ -n "${RECSYS_CF_HIT_M:-}" ]] && SPARK_ENV+=( -e "RECSYS_CF_HIT_M=${RECSYS_CF_HIT_M}" )

echo "== Rebuild / start stack (namenode+python, nodemanager+python, spark) =="
if [[ "${SKIP_DOCKER_BUILD:-0}" == "1" ]]; then
  echo "(SKIP_DOCKER_BUILD=1: skipping docker compose build)"
else
  docker compose build namenode nodemanager spark-runner
fi
docker compose up -d

echo "== Wait for HDFS (RPC + safe mode off) =="
for i in $(seq 1 120); do
  sm="$(docker compose exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null || true)"
  if echo "${sm}" | grep -qi "Safe mode is OFF"; then
    echo "HDFS ready (attempt ${i})"
    break
  fi
  if [[ "${i}" -eq 120 ]]; then
    echo "ERROR: Namenode stayed in safe mode or unreachable." >&2
    echo "${sm}" >&2
    exit 1
  fi
  sleep 2
done

# Docker: RECSYS_DOCKER_EXEC_TTY=1 forces -t (line-buffered logs); 0 forces -T; default auto=tty iff stdout is a tty.
echo "== MapReduce pipeline (namenode; RECSYS_USE_YARN=${RECSYS_USE_YARN:-0}; dataset=${RECSYS_DATASET:-ecommerce}) =="
DOCKER_IT=( -T )
case "${RECSYS_DOCKER_EXEC_TTY:-auto}" in
  1|yes|true|on|force) DOCKER_IT=( -t ) ;;
  0|no|false|off) DOCKER_IT=( -T ) ;;
  auto)
    [[ -t 1 ]] && DOCKER_IT=( -t )
    ;;
esac
docker compose exec "${DOCKER_IT[@]}" "${MR_ENV[@]}" namenode bash /workspace/pipeline/run_mapreduce.sh

echo "== Spark / LR / fusion / plots (spark-runner) =="
docker compose exec "${DOCKER_IT[@]}" spark-runner "${SPARK_ENV[@]}" bash /workspace/pipeline/run_spark.sh

echo "Done. Outputs: ${ROOT}/output (if volume writable) or check /workspace/output inside spark-runner."
ls -la "${ROOT}/output" 2>/dev/null || docker compose exec -T spark-runner ls -la /workspace/output
