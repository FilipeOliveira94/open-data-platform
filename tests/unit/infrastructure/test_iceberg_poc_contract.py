"""Contract checks for the P2-01 probe files. No Spark or MinIO required."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PROBE_SQL = ROOT / "docker" / "spark" / "poc_probe.sql"
RUN_SCRIPT = ROOT / "docker" / "spark" / "run_poc.sh"
COMPOSE = ROOT / "docker-compose.yml"

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
SPARK_IMAGE = "spark:3.5.8-java17"


def test_probe_sql_overwrites_two_fixed_rows() -> None:
    sql = PROBE_SQL.read_text(encoding="utf-8")
    assert "CREATE NAMESPACE IF NOT EXISTS poc;" in sql
    assert "CREATE TABLE IF NOT EXISTS poc.probe (" in sql
    assert "USING iceberg;" in sql
    assert "INSERT OVERWRITE poc.probe VALUES (1, 'alpha'), (2, 'beta');" in sql
    assert "SELECT COUNT(*) AS row_count FROM poc.probe;" in sql


def test_run_script_pins_iceberg_and_reads_credentials_from_env() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")
    assert ICEBERG_RUNTIME in script
    assert HADOOP_AWS in script
    assert AWS_SDK_BUNDLE in script
    assert "spark.sql.catalog.local.type=hadoop" in script
    assert "${MINIO_ROOT_USER}" in script
    assert "${MINIO_ROOT_PASSWORD}" in script
    assert "minioadmin" not in script
    assert "java11" not in script.lower()


def test_spark_service_is_lakehouse_profile_only() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    assert f"image: {SPARK_IMAGE}" in compose
    assert "profiles: [lakehouse]" in compose
    assert "spark:3.5.8\n" not in compose
    assert "java11" not in compose.lower()
    postgres = compose.split("minio:", 1)[0]
    assert "profiles:" not in postgres
