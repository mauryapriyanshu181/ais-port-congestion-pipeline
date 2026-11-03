#!/usr/bin/env bash
# Runs a PySpark job with the Kafka and Delta Lake connectors on the classpath.
# Usage (inside a Spark container): bash /opt/jobs/submit.sh /opt/jobs/<job>.py
set -euo pipefail

# TIMESTAMP_MICROS writes standard Parquet timestamps instead of the legacy INT96
# type, so non-Spark readers like the dashboard can filter on them.

exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1" \
  --conf "spark.jars.ivy=${IVY_DIR:-/opt/ivy}" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS" \
  --conf "spark.sql.shuffle.partitions=4" \
  "$@"
