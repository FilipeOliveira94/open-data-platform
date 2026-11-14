#!/usr/bin/env bash
# One-shot Spark SQL probe. Credentials come from the container environment.
# S3 keys are a properties file, not --conf values.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${ODP_WAREHOUSE_BUCKET:?ODP_WAREHOUSE_BUCKET is required}"
: "${MINIO_REGION:=us-east-1}"

# shellcheck source=credentials.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/credentials.sh"
SECRET_PROPS="$(odp_write_secret_properties s3)"

# The Spark 3.5.8 distribution ships Hadoop 3.3.4 client jars without S3A.
# hadoop-aws 3.3.4 declares aws-java-sdk-bundle 1.12.262.
# spark-sql writes derby.log, metastore_db, and spark-warehouse to the current
# directory. The runtime image user cannot write the mounted repo, so use the
# container scratch directory (the earlier Spark image used its work-dir).
cd "${TMPDIR:-/tmp}"

exec /opt/spark/bin/spark-sql \
  --master "local[*]" \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --properties-file "${SECRET_PROPS}" \
  --conf "spark.redaction.regex=(?i)secret|password|token|access[.]key" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.defaultCatalog=local" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=s3a://${ODP_WAREHOUSE_BUCKET}/poc" \
  --conf "spark.sql.catalog.local.cache-enabled=false" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${MINIO_REGION}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  -f "${SCRIPT_DIR}/poc_probe.sql"
