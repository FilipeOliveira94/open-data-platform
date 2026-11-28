#!/usr/bin/env bash
# P11-04: run the Spark Structured Streaming on Kafka probe (kafka_probe.py).
# Same catalog, S3A, and secret handling as run_bronze.sh: the JDBC password
# and S3 keys are a properties file, not --conf values.
# Arguments are passed to kafka_probe.py (run, drain, rewind-commit, check, drop).
set -euo pipefail

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${ODP_WAREHOUSE_BUCKET:?ODP_WAREHOUSE_BUCKET is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${ICEBERG_CATALOG_DB:?ICEBERG_CATALOG_DB is required}"
: "${MINIO_REGION:=us-east-1}"

case "${ICEBERG_CATALOG_DB}" in
  *[!a-z0-9_]*)
    echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2
    exit 1
    ;;
esac

: "${ODP_SPARK_CORES:=2}"
: "${ODP_SPARK_DRIVER_MEMORY:=2g}"
case "${ODP_SPARK_CORES}" in
  "" | 0* | *[!0-9]*)
    echo "ODP_SPARK_CORES must be a positive integer" >&2
    exit 1
    ;;
esac
case "${ODP_SPARK_DRIVER_MEMORY}" in
  [1-9]*[mg])
    case "${ODP_SPARK_DRIVER_MEMORY%?}" in
      *[!0-9]*)
        echo "ODP_SPARK_DRIVER_MEMORY must look like 2g or 1536m" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "ODP_SPARK_DRIVER_MEMORY must look like 2g or 1536m" >&2
    exit 1
    ;;
esac

spark_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=credentials.sh
source "${spark_dir}/credentials.sh"
SECRET_PROPS="$(odp_write_secret_properties jdbc)"

# The lakehouse jars of run_bronze.sh plus the Kafka source. Every package
# here is cached by warm_ivy.sh (make spark-ivy-warm).
packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
packages="${packages},org.apache.hadoop:hadoop-aws:3.3.4"
packages="${packages},com.amazonaws:aws-java-sdk-bundle:1.12.262"
packages="${packages},org.postgresql:postgresql:42.7.14"
packages="${packages},org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

cd "${TMPDIR:-/tmp}"

# No exec: under `compose run` the exec'd JVM would be PID 1, and the kernel
# ignores a SIGKILL sent to PID 1 from inside its namespace. As a child of
# this shell the driver JVM can be killed by `run --kill-in-flight`.
/opt/spark/bin/spark-submit \
  --master "local[${ODP_SPARK_CORES}]" \
  --driver-memory "${ODP_SPARK_DRIVER_MEMORY}" \
  --packages "${packages}" \
  --properties-file "${SECRET_PROPS}" \
  --conf "spark.redaction.regex=(?i)secret|password|token|access[.]key" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.defaultCatalog=local" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=jdbc" \
  --conf "spark.sql.catalog.local.uri=jdbc:postgresql://postgres:5432/${ICEBERG_CATALOG_DB}" \
  --conf "spark.sql.catalog.local.jdbc.user=${POSTGRES_USER}" \
  --conf "spark.sql.catalog.local.jdbc.schema-version=V1" \
  --conf "spark.sql.catalog.local.jdbc.init-catalog-tables=true" \
  --conf "spark.sql.catalog.local.warehouse=s3a://${ODP_WAREHOUSE_BUCKET}/catalog" \
  --conf "spark.sql.catalog.local.cache-enabled=false" \
  --conf "spark.sql.caseSensitive=true" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${MINIO_REGION}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  "${spark_dir}/kafka_probe.py" "$@"
