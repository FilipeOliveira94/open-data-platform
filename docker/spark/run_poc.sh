#!/usr/bin/env bash
# One-shot Spark SQL probe. Credentials come from the container environment.
set -euo pipefail

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${ODP_WAREHOUSE_BUCKET:?ODP_WAREHOUSE_BUCKET is required}"
: "${MINIO_REGION:=us-east-1}"

# The official Spark image ships Hadoop 3.3.4 client jars without S3A.
# hadoop-aws 3.3.4 declares aws-java-sdk-bundle 1.12.262.
exec /opt/spark/bin/spark-sql \
  --master "local[*]" \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
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
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD}" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=${MINIO_REGION}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  -f /opt/odp/poc_probe.sql
