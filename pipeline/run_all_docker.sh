#!/usr/bin/env bash
# Run from project root on the host (not inside a container):
#   bash pipeline/run_all_docker.sh
# Optional: run MapReduce on YARN instead of local mode:
#   RECSYS_USE_YARN=1 bash pipeline/run_all_docker.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}/hadoop-docker"

echo "== Rebuild / start stack (namenode+python, nodemanager+python, spark) =="
docker compose build namenode nodemanager spark-runner
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

echo "== MapReduce pipeline (namenode; RECSYS_USE_YARN=${RECSYS_USE_YARN:-0}) =="
docker compose exec -T -e "RECSYS_USE_YARN=${RECSYS_USE_YARN:-0}" namenode bash /workspace/pipeline/run_mapreduce.sh

echo "== Spark / LR / fusion / plots (spark-runner) =="
docker compose exec -T spark-runner bash /workspace/pipeline/run_spark.sh

echo "Done. Outputs: ${ROOT}/output (if volume writable) or check /workspace/output inside spark-runner."
ls -la "${ROOT}/output" 2>/dev/null || docker compose exec -T spark-runner ls -la /workspace/output
