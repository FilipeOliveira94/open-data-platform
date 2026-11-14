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

exec /opt/spark/bin/spark-sql \
  --master "local[*]" \
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
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${MINIO_REGION}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  -f "${SQL_FILE}"
