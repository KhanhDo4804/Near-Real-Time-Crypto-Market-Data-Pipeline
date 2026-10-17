#!/usr/bin/env bash
set -euo pipefail

# Hadoop in this Spark image is 3.3.4; hadoop-aws must use the same version.
exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/jobs/read_kafka.py
