#!/usr/bin/env bash
# Resolve and cache the pinned lakehouse jars into spark.jars.ivy (/tmp/.ivy2).
# Pins match run_bronze.sh / run_silver.sh / run_catalog_probe.sh. No SQL work.
set -euo pipefail

mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars /tmp/.ivy2/local

packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
packages="${packages},org.apache.hadoop:hadoop-aws:3.3.4"
packages="${packages},com.amazonaws:aws-java-sdk-bundle:1.12.262"
packages="${packages},org.postgresql:postgresql:42.7.14"

# spark-sql writes derby.log, metastore_db, and spark-warehouse to the current
# directory. The runtime image user cannot write the mounted repo, so use the
# container scratch directory (the earlier Spark image used its work-dir).
cd "${TMPDIR:-/tmp}"

exec /opt/spark/bin/spark-sql \
  --master "local[1]" \
  --packages "${packages}" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  -e "SELECT 1 AS ivy_warm;"
