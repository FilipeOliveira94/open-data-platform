"""Unit tests for quarantine JSONL staging helpers (no MinIO / Spark)."""

from __future__ import annotations

from pathlib import Path

import pytest

from open_data_platform.lakehouse.quarantine_stage import (
    STAGING_KEY,
    QuarantineFindingRow,
    QuarantineStageError,
    encode_findings_jsonl,
    parse_finding_line,
    parse_quarantine_key,
    prepare_quarantine_staging,
    render_load_sql,
)

ROOT = Path(__file__).resolve().parents[3]
_SECRET = "cust-secret-42"
_CITY = "Campinas"


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


def test_load_sql_names_the_staging_path_and_omits_finding_values() -> None:
    row = _row(business_key=f'{{"customer_id":"{_SECRET}"}}', field=None)
    sql = render_load_sql(warehouse_bucket="odp-warehouse", row_count=1)
    body = encode_findings_jsonl((row,)).decode("utf-8")
    assert (
        "s3a://odp-warehouse/gold/_staging/gold_staging/"
        "quarantine_findings/rows.jsonl" in sql
    )
    assert "VALUES" not in sql
    assert _SECRET not in sql
    assert '"field_name":null' in body
    assert _SECRET in body


def test_empty_load_sql_does_not_read_a_staging_object() -> None:
    sql = render_load_sql(warehouse_bucket="odp-warehouse", row_count=0)
    assert "WHERE 1 = 0" in sql
    assert "rows.jsonl" not in sql
    assert "CREATE NAMESPACE IF NOT EXISTS gold_staging" in sql


def test_load_sql_rejects_a_bucket_that_could_break_the_path() -> None:
    with pytest.raises(QuarantineStageError, match="simple S3 identifier"):
        render_load_sql(warehouse_bucket="odp/warehouse", row_count=0)


def test_prepare_writes_the_jsonl_and_keeps_the_sql_free_of_values() -> None:
    key = (
        "quarantine/olist_brazilian_ecommerce/customers/"
        "dt=2026-11-01/execution_id=run-001/customers.jsonl"
    )
    line = (
        '{"record_index":1,"reason":"null_violation","field":"customer_city",'
        f'"primary_key":{{"customer_id":"{_SECRET}"}},'
        f'"record":{{"customer_id":"{_SECRET}","customer_city":"{_CITY}"}}}}'
    )
    store = _Store({("odp-raw", key): line.encode("utf-8")})
    staged = prepare_quarantine_staging(
        client=store,
        bucket="odp-raw",
        warehouse_bucket="odp-warehouse",
        keys=(key,),
    )
    assert staged.row_count == 1
    assert _SECRET not in staged.sql
    assert _CITY not in staged.sql
    written = store.objects[("odp-warehouse", STAGING_KEY)].decode("utf-8")
    assert _SECRET in written
    assert _CITY not in written


def test_prepare_deletes_the_staging_object_when_there_are_no_findings() -> None:
    store = _Store({})
    staged = prepare_quarantine_staging(
        client=store,
        bucket="odp-raw",
        warehouse_bucket="odp-warehouse",
        keys=(),
    )
    assert staged.row_count == 0
    assert "rows.jsonl" not in staged.sql
    assert ("odp-warehouse", STAGING_KEY) in store.deleted
    assert ("odp-warehouse", STAGING_KEY) not in store.objects


def test_make_stage_quarantine_loads_through_spark() -> None:
    text = (ROOT / "Makefile").read_text(encoding="utf-8")
    block = text.split("stage-quarantine:", maxsplit=1)[1].split(
        "\ndbt-parse:",
        maxsplit=1,
    )[0]
    assert "--sql-out .local/quarantine-sql" in block
    assert "ODP_SPARK_SQL=/tmp/quarantine.sql" in block
    assert "spark /opt/odp/run_bronze.sh" in block
    assert "trino" not in block


def _row(
    *, business_key: str, field: str | None = "customer_city"
) -> QuarantineFindingRow:
    return QuarantineFindingRow(
        source_id="olist_brazilian_ecommerce",
        dataset="customers",
        source_dataset="olist_brazilian_ecommerce.customers",
        reason="null_violation",
        field=field,
        business_key=business_key,
        record_index=1,
        quarantine_object_key="quarantine/olist_brazilian_ecommerce/customers/x.jsonl",
        detail="null_violation at record_index=1",
    )


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _Store:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)
        self.deleted: list[tuple[str, str]] = []

    def head_bucket(self, *, Bucket: str) -> dict[str, str]:
        return {"Bucket": Bucket}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> None:
        del ContentType
        self.objects[(Bucket, Key)] = Body

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.deleted.append((Bucket, Key))
        self.objects.pop((Bucket, Key), None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, _Body]:
        return {"Body": _Body(self.objects[(Bucket, Key)])}
