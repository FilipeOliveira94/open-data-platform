"""Contract checks for the P2-02 JDBC catalog probe. No Spark, Trino, or MinIO."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PROBE_SQL = ROOT / "docker" / "spark" / "catalog_probe.sql"
RUN_SCRIPT = ROOT / "docker" / "spark" / "run_catalog_probe.sh"
HADOOP_SCRIPT = ROOT / "docker" / "spark" / "run_poc.sh"
TRINO_CATALOG = ROOT / "docker" / "trino" / "catalog" / "iceberg.properties"
COMPOSE = ROOT / "docker-compose.yml"
MAKEFILE = ROOT / "Makefile"
ENV_EXAMPLE = ROOT / ".env.example"

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
POSTGRES_JDBC = "org.postgresql:postgresql:42.7.14"
TRINO_IMAGE = "trinodb/trino:484"
TRINO_COUNT = "SELECT COUNT(*) AS row_count FROM iceberg.catalog_probe.probe"


def test_catalog_sql_overwrites_two_rows_at_format_v2() -> None:
    sql = PROBE_SQL.read_text(encoding="utf-8")
    assert "CREATE NAMESPACE IF NOT EXISTS catalog_probe;" in sql
    assert "CREATE TABLE IF NOT EXISTS catalog_probe.probe (" in sql
    assert "USING iceberg" in sql
    assert "'format-version' = '2'" in sql
    assert (
        "INSERT OVERWRITE catalog_probe.probe VALUES (1, 'alpha'), (2, 'beta');" in sql
    )
    assert "SELECT COUNT(*) AS row_count FROM catalog_probe.probe;" in sql


def test_catalog_run_script_uses_jdbc_and_env_credentials() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")
    assert ICEBERG_RUNTIME in script
    assert HADOOP_AWS in script
    assert AWS_SDK_BUNDLE in script
    assert POSTGRES_JDBC in script
    assert "spark.sql.catalog.local.type=jdbc" in script
    assert "spark.sql.catalog.local.jdbc.schema-version=V1" in script
    assert "spark.sql.catalog.local.jdbc.init-catalog-tables=true" in script
    assert "s3a://${ODP_WAREHOUSE_BUCKET}/catalog" in script
    assert "jdbc:postgresql://postgres:5432/${ICEBERG_CATALOG_DB}" in script
    assert "${POSTGRES_USER}" in script
    assert "${POSTGRES_PASSWORD}" in script
    assert "minioadmin" not in script
    assert "java11" not in script.lower()


def test_trino_catalog_reads_the_same_jdbc_warehouse() -> None:
    catalog = TRINO_CATALOG.read_text(encoding="utf-8")
    assert "connector.name=iceberg" in catalog
    assert "iceberg.catalog.type=jdbc" in catalog
    assert "iceberg.jdbc-catalog.catalog-name=local" in catalog
    assert "iceberg.jdbc-catalog.schema-version=V1" in catalog
    assert "fs.s3.enabled=true" in catalog
    assert "s3.expect-continue-enabled=false" in catalog
    assert "s3.chunked-encoding-enabled=false" in catalog
    assert "s3a://${ENV:ODP_WAREHOUSE_BUCKET}/catalog" in catalog
    assert "${ENV:POSTGRES_PASSWORD}" in catalog
    assert "${ENV:MINIO_ROOT_PASSWORD}" in catalog
    assert "minioadmin" not in catalog


def test_compose_pins_trino_on_the_lakehouse_profile() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    assert f"image: {TRINO_IMAGE}" in compose
    assert "spark-catalog:" in compose
    assert compose.count("profiles: [lakehouse]") == 3
    hadoop_script = HADOOP_SCRIPT.read_text(encoding="utf-8")
    assert "spark.sql.catalog.local.type=hadoop" in hadoop_script


def test_make_target_queries_trino_after_spark() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    assert "test-catalog-probe:" in text
    assert TRINO_COUNT in text
    assert "spark-catalog" in text
    assert "ICEBERG_CATALOG_DB=iceberg" in ENV_EXAMPLE.read_text(encoding="utf-8")
