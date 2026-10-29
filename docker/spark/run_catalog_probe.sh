#!/usr/bin/env bash
# One-shot Spark SQL probe against the Iceberg JDBC catalog.
# Credentials come from the container environment.
# The JDBC catalog_name stored in iceberg_tables is the Spark catalog name "local".
# Trino must use that same name.
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

# The official Spark image ships Hadoop 3.3.4 client jars without S3A.
# hadoop-aws 3.3.4 declares aws-java-sdk-bundle 1.12.262.
# The PostgreSQL driver is not in the image. Iceberg loads it through DriverManager.
packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
packages="${packages},org.apache.hadoop:hadoop-aws:3.3.4"
packages="${packages},com.amazonaws:aws-java-sdk-bundle:1.12.262"
packages="${packages},org.postgresql:postgresql:42.7.14"

exec /opt/spark/bin/spark-sql \
  --master "local[*]" \
  --packages "${packages}" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.defaultCatalog=local" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=jdbc" \
  --conf "spark.sql.catalog.local.uri=jdbc:postgresql://postgres:5432/${ICEBERG_CATALOG_DB}" \
  --conf "spark.sql.catalog.local.jdbc.user=${POSTGRES_USER}" \
  --conf "spark.sql.catalog.local.jdbc.password=${POSTGRES_PASSWORD}" \
  --conf "spark.sql.catalog.local.jdbc.schema-version=V1" \
  --conf "spark.sql.catalog.local.jdbc.init-catalog-tables=true" \
  --conf "spark.sql.catalog.local.warehouse=s3a://${ODP_WAREHOUSE_BUCKET}/catalog" \
  --conf "spark.sql.catalog.local.cache-enabled=false" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD}" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${MINIO_REGION}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  -f /opt/odp/catalog_probe.sql
