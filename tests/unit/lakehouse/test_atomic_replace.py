"""P3-11: every lakehouse writer replaces a read table in one Iceberg commit.

Readers (Trino, dbt, checks) must never see a read table missing or empty
because a writer dropped it first. No Spark, Trino, or MinIO.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from open_data_platform.contracts.load import load_contracts
from open_data_platform.lakehouse.bronze import render_bronze_sql
from open_data_platform.lakehouse.quarantine_stage import render_load_sql
from open_data_platform.lakehouse.silver.cli import policy_for
from open_data_platform.lakehouse.silver.common import render_silver_sql

ROOT = Path(__file__).resolve().parents[3]


def _assert_one_atomic_replace(sql: str, table_prefix: str) -> None:
    upper = sql.upper()
    assert "DROP TABLE" not in upper
    assert "INSERT OVERWRITE" not in upper
    assert "CREATE TABLE" not in upper
    assert sql.count(f"CREATE OR REPLACE TABLE {table_prefix}") == 1
    assert "AS SELECT" in sql


@pytest.mark.parametrize("row_count", [0, 3])
def test_bronze_replaces_each_table_in_one_statement(row_count: int) -> None:
    for contract in load_contracts():
        sql = render_bronze_sql([(contract, row_count)], "odp-warehouse")
        _assert_one_atomic_replace(sql, "bronze.")


def test_silver_replaces_each_table_in_one_statement() -> None:
    for contract in load_contracts():
        sql = render_silver_sql(contract, "odp-warehouse", policy_for(contract))
        _assert_one_atomic_replace(sql, "silver.")


@pytest.mark.parametrize("row_count", [0, 3])
def test_quarantine_staging_replaces_its_table_in_one_statement(row_count: int) -> None:
    sql = render_load_sql(warehouse_bucket="odp-warehouse", row_count=row_count)
    _assert_one_atomic_replace(sql, "gold_staging.")


def test_dbt_gold_tables_replace_in_place() -> None:
    """dbt-trino ``on_table_exists: replace`` renders CREATE OR REPLACE TABLE.

    The default (``rename``) leaves a window where the target name is absent.
    """
    project = yaml.safe_load((ROOT / "dbt" / "dbt_project.yml").read_text())
    models = project["models"]["open_data_platform"]
    assert models["+materialized"] == "table"
    assert models["+on_table_exists"] == "replace"
    for path in (ROOT / "dbt" / "models").rglob("*.sql"):
        assert "on_table_exists" not in path.read_text(), path
