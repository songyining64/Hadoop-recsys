#!/usr/bin/env bash
set -euo pipefail
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop-conf}"
export RECSYS_OUT="${RECSYS_OUT:-/workspace/output}"
mkdir -p "${RECSYS_OUT}"

/opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --driver-memory 3g \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /workspace/pipeline/recsys_spark.py
