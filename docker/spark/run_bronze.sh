#!/usr/bin/env bash
# Load Bronze tables through the Iceberg JDBC catalog.
# Credentials come from the container environment.
# The JDBC password and S3 keys are a properties file, not --conf values.
# The caller sets ODP_SPARK_SQL to the SQL file path.
# Table locations are set in that SQL. The catalog warehouse stays /catalog.
set -euo pipefail

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${ODP_WAREHOUSE_BUCKET:?ODP_WAREHOUSE_BUCKET is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${ICEBERG_CATALOG_DB:?ICEBERG_CATALOG_DB is required}"
: "${MINIO_REGION:=us-east-1}"

SQL_FILE="${ODP_SPARK_SQL:?ODP_SPARK_SQL is required}"
if [[ ! -f "${SQL_FILE}" ]]; then
  echo "SQL file is not mounted at ${SQL_FILE}" >&2
  exit 1
fi

case "${ICEBERG_CATALOG_DB}" in
  *[!a-z0-9_]*)
    echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2
    exit 1
    ;;
esac

# P3-10: Spark local mode runs inside the scheduler (or spark) container and
# shares its CPU and memory with Airflow. These two values cap it.
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

# shellcheck source=credentials.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/credentials.sh"
SECRET_PROPS="$(odp_write_secret_properties jdbc)"

# The Spark 3.5.8 distribution ships Hadoop 3.3.4 client jars without S3A.
# hadoop-aws 3.3.4 declares aws-java-sdk-bundle 1.12.262.
# The PostgreSQL driver is not in the image. Iceberg loads it through DriverManager.
packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
packages="${packages},org.apache.hadoop:hadoop-aws:3.3.4"
packages="${packages},com.amazonaws:aws-java-sdk-bundle:1.12.262"
packages="${packages},org.postgresql:postgresql:42.7.14"

# spark-sql writes derby.log, metastore_db, and spark-warehouse to the current
# directory. The runtime image user cannot write the mounted repo, so use the
# container scratch directory (the earlier Spark image used its work-dir).
cd "${TMPDIR:-/tmp}"

# P3-08: medallion_build sets both paths to a mode-700 directory of the run.
# ODP_SPARK_EVENT_DIR receives the Spark event log (scan files, bytes, and
# rows); ODP_SPARK_METRICS_FILE receives the Iceberg commit reports. The
# caller parses the counts and deletes both. Without them, Spark keeps its
# default logging and writes no event log.
metrics_opts=(--conf "spark.eventLog.enabled=false")
if [[ -n "${ODP_SPARK_METRICS_FILE:-}" || -n "${ODP_SPARK_EVENT_DIR:-}" ]]; then
  for metrics_path in "${ODP_SPARK_METRICS_FILE:?}" "${ODP_SPARK_EVENT_DIR:?}"; do
    case "${metrics_path}" in
      /*[!A-Za-z0-9._/-]* | [!/]*)
        echo "spark metrics paths must be plain absolute paths" >&2
        exit 1
        ;;
    esac
  done
  log4j_conf="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/log4j2-metrics.properties"
  metrics_opts=(
    --driver-java-options "-Dlog4j.configurationFile=${log4j_conf} -Dodp.metrics.file=${ODP_SPARK_METRICS_FILE}"
    --conf "spark.sql.catalog.local.metrics-reporter-impl=org.apache.iceberg.metrics.LoggingMetricsReporter"
    --conf "spark.eventLog.enabled=true"
    --conf "spark.eventLog.dir=file://${ODP_SPARK_EVENT_DIR}"
    --conf "spark.eventLog.compress=false"
  )
fi

exec /opt/spark/bin/spark-sql \
  --master "local[${ODP_SPARK_CORES}]" \
  --driver-memory "${ODP_SPARK_DRIVER_MEMORY}" \
  --packages "${packages}" \
  --properties-file "${SECRET_PROPS}" \
  "${metrics_opts[@]}" \
  --conf "spark.redaction.regex=(?i)secret|password|token|access[.]key" \
  --conf "spark.ui.enabled=false" \
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
  -f "${SQL_FILE}"
