"""Unit tests for quarantine JSONL staging helpers (no MinIO / Trino)."""

from __future__ import annotations

import pytest

from open_data_platform.lakehouse.quarantine_stage import (
    QuarantineFindingRow,
    QuarantineStageError,
    parse_finding_line,
    parse_quarantine_key,
    render_create_replace_sql,
)


def test_parse_quarantine_key() -> None:
    key = (
        "quarantine/olist_brazilian_ecommerce/orders/"
        "dt=2026-11-01/execution_id=run-001/orders.jsonl"
    )
    assert parse_quarantine_key(key) == ("olist_brazilian_ecommerce", "orders")


def test_parse_quarantine_key_rejects_bad_shape() -> None:
    with pytest.raises(QuarantineStageError, match="unexpected quarantine key"):
        parse_quarantine_key("raw/olist_brazilian_ecommerce/orders/x.jsonl")


def test_parse_finding_line() -> None:
    line = (
        '{"record_index":3,"reason":"null_violation","field":"order_id",'
        '"primary_key":{"order_id":null},"record":{"order_id":null}}'
    )
    row = parse_finding_line(
        line,
        source_id="olist_brazilian_ecommerce",
        dataset="orders",
        object_key="quarantine/olist_brazilian_ecommerce/orders/x.jsonl",
    )
    assert row.source_dataset == "olist_brazilian_ecommerce.orders"
    assert row.reason == "null_violation"
    assert row.field == "order_id"
    assert row.record_index == 3
    assert "order_id" in row.business_key


def test_render_create_replace_sql_empty() -> None:
    sql = render_create_replace_sql((), warehouse_bucket="odp-warehouse")
    assert "CREATE SCHEMA IF NOT EXISTS iceberg.gold_staging" in sql
    assert "CREATE OR REPLACE TABLE iceberg.gold_staging.quarantine_findings" in sql
    assert "AS" not in sql.split("quarantine_findings", 1)[1]


def test_render_create_replace_sql_with_rows() -> None:
    row = QuarantineFindingRow(
        source_id="demo_storefront",
        dataset="clickstream",
        source_dataset="demo_storefront.clickstream",
        reason="type_mismatch",
        field="event_type",
        business_key='{"event_id":"e1"}',
        record_index=0,
        quarantine_object_key="quarantine/demo_storefront/clickstream/x.jsonl",
        detail="type_mismatch at record_index=0",
    )
    sql = render_create_replace_sql((row,), warehouse_bucket="odp-warehouse")
    assert "AS" in sql
    assert "demo_storefront.clickstream" in sql
    assert "s3a://odp-warehouse/gold/gold_staging/quarantine_findings" in sql


def test_render_create_replace_sql_null_field_is_typed() -> None:
    row = QuarantineFindingRow(
        source_id="x",
        dataset="y",
        source_dataset="x.y",
        reason="null_violation",
        field=None,
        business_key="{}",
        record_index=1,
        quarantine_object_key="quarantine/x/y/z.jsonl",
        detail="d",
    )
    sql = render_create_replace_sql((row,), warehouse_bucket="odp-warehouse")
    assert "CAST(NULL AS varchar)" in sql
