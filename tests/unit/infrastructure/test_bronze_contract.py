"""Contract checks for the P2-03 Bronze Spark job. No Spark, Trino, or MinIO."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUN_SCRIPT = ROOT / "docker" / "spark" / "run_bronze.sh"
CATALOG_SCRIPT = ROOT / "docker" / "spark" / "run_catalog_probe.sh"
COMPOSE = ROOT / "docker-compose.yml"
MAKEFILE = ROOT / "Makefile"

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
POSTGRES_JDBC = "org.postgresql:postgresql:42.7.14"


def test_bronze_run_script_reuses_the_jdbc_catalog() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")
    catalog = CATALOG_SCRIPT.read_text(encoding="utf-8")
    assert ICEBERG_RUNTIME in script
    assert HADOOP_AWS in script
    assert AWS_SDK_BUNDLE in script
    assert POSTGRES_JDBC in script
    assert "spark.sql.catalog.local.type=jdbc" in script
    assert "spark.sql.catalog.local.jdbc.schema-version=V1" in script
    assert "spark.sql.catalog.local.jdbc.init-catalog-tables=true" in script
    assert "s3a://${ODP_WAREHOUSE_BUCKET}/catalog" in script
    assert "spark.sql.caseSensitive=true" in script
    assert 'SQL_FILE="${ODP_SPARK_SQL:?ODP_SPARK_SQL is required}"' in script
    assert '-f "${SQL_FILE}"' in script
    assert "type=hadoop" not in script
    assert "minioadmin" not in script
    assert "jdbc:postgresql://postgres:5432/${ICEBERG_CATALOG_DB}" in script
    assert catalog.count("spark.sql.catalog.local.type=jdbc") == 1


def test_compose_keeps_bronze_on_the_shared_spark_runtime() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    assert "spark-bronze:" not in compose
    assert compose.count("profiles: [lakehouse]") == 2
    assert "./docker/spark:/opt/odp/spark:ro" in compose
    assert "./docker/spark:/opt/odp:ro" not in compose
    assert "docker compose config --services" not in compose


def test_make_live_bronze_uses_odp_raw_not_a_fixture_bucket() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    assert "live-bronze:" in text
    assert "live-silver:" in text
    bronze_block = text.split("live-bronze:", maxsplit=1)[1].split(
        "\nlive-silver:",
        maxsplit=1,
    )[0]
    assert "$(call medallion_step,bronze);" in bronze_block
    assert "$(call medallion_step,bronze-checks)" in bronze_block
    assert "uv run python" not in bronze_block
    assert "odp-bronze-live" not in bronze_block
    assert "--prepare-unit-fixtures" not in bronze_block
    assert "--prepare-live-fixtures" not in bronze_block
    silver_block = text.split("live-silver:", maxsplit=1)[1].split(
        "\nup:",
        maxsplit=1,
    )[0]
    assert "$(call medallion_step,silver);" in silver_block
    assert "$(call medallion_step,silver-checks)" in silver_block
    assert "odp-bronze-live" not in silver_block
