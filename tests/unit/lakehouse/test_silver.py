"""Silver SQL generation and source policies. No Spark, Trino, or MinIO."""

from __future__ import annotations

import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.lakehouse.silver.cli import (
    SOURCE_MODULES,
    live_checks,
    main,
    policy_for,
    selected_entries,
)
from open_data_platform.lakehouse.silver.common import (
    DedupPolicy,
    SilverError,
    TimestampStyle,
    iceberg_type,
    json_path_timestamp,
    render_silver_sql,
    resolve_output_columns,
    silver_table_name,
    timestamp_sql,
)
from open_data_platform.lakehouse.write_mode import WriteMode


def _by_identity() -> dict[tuple[str, str], object]:
    return {(item.source_id, item.dataset): item for item in load_contracts()}


def test_every_shipped_contract_has_a_silver_policy() -> None:
    for contract in load_contracts():
        policy = policy_for(contract)
        assert isinstance(policy, DedupPolicy)


def test_source_modules_cover_registered_sources() -> None:
    sources = {item.source_id for item in load_contracts()}
    assert set(SOURCE_MODULES) == sources


def test_order_reviews_uses_composite_primary_key() -> None:
    contract = _by_identity()[("olist_brazilian_ecommerce", "order_reviews")]
    assert contract.primary_key == ("review_id", "order_id")
    assert contract.contract_version == 3
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert "PARTITION BY `review_id`, `order_id`" in sql
    assert "TRY_TO_TIMESTAMP(`review_answer_timestamp`" in sql


def test_geolocation_partitions_payload_fields() -> None:
    contract = _by_identity()[("olist_brazilian_ecommerce", "geolocation")]
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert "PARTITION BY `geolocation_zip_code_prefix`" in sql
    assert "`geolocation_lat`" in sql
    assert "TRY_CAST(`geolocation_lat` AS DOUBLE)" in sql


def test_postgres_orders_by_updated_at() -> None:
    contract = _by_identity()[("postgres_oltp", "orders")]
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert "ORDER BY `updated_at` DESC" in sql
    assert "TRY_TO_TIMESTAMP(`updated_at`)" in sql


def test_clickstream_orders_by_event_time_and_keeps_null_product() -> None:
    contract = _by_identity()[("demo_storefront", "clickstream")]
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert "ORDER BY `event_time` DESC" in sql
    assert "PARTITION BY `event_id`" in sql
    assert "`product_id` STRING" in sql


def test_dummyjson_expands_nested_json_columns() -> None:
    contract = _by_identity()[("dummyjson_products", "products")]
    policy = policy_for(contract)
    names = [col.name for col in resolve_output_columns(contract, policy)]
    assert "dimensions" not in names
    assert "meta" not in names
    assert "dimensions_width" in names
    assert "meta_created_at" in names
    assert "meta_barcode" in names
    assert names.count("tags") == 1
    assert names.count("reviews") == 1
    assert names.count("images") == 1
    sql = render_silver_sql(contract, "odp-warehouse", policy)
    assert "get_json_object(`dimensions`, '$.width')" in sql
    assert "from_json(`tags`, 'ARRAY<STRING>')" in sql
    assert "dimensions_width" in sql
    assert "ARRAY<STRUCT<" in sql
    assert "DROP TABLE IF EXISTS silver." in sql
    assert "write_mode=full_overwrite" in sql
    # Timestamp parsing for nested meta uses the shared helper.
    assert json_path_timestamp("`meta`", "$.createdAt") in sql


def test_timestamp_sql_helpers_are_reusable() -> None:
    iso = timestamp_sql("`event_time`", TimestampStyle.ISO)
    assert "TRY_TO_TIMESTAMP(`event_time`)" in iso
    olist = timestamp_sql("`shipping_limit_date`", TimestampStyle.OLIST_CSV)
    assert "yyyy-MM-dd HH:mm:ss" in olist


def test_incremental_write_mode_is_a_placeholder() -> None:
    contract = _by_identity()[("demo_storefront", "clickstream")]
    with pytest.raises(SilverError, match="placeholder"):
        render_silver_sql(
            contract,
            "odp-warehouse",
            DedupPolicy(
                timestamp_style=TimestampStyle.ISO,
                write_mode=WriteMode.INCREMENTAL_MERGE,
            ),
        )


def test_category_sql_dedups_on_category_name() -> None:
    contract = _by_identity()[
        ("olist_brazilian_ecommerce", "product_category_name_translation")
    ]
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert silver_table_name(contract.source_id, contract.dataset) in sql
    assert "PARTITION BY `product_category_name`" in sql
    assert "CAST(`record_index` AS BIGINT) DESC" in sql
    location = (
        "LOCATION 's3a://odp-warehouse/silver/"
        "olist_brazilian_ecommerce/product_category_name_translation'"
    )
    assert location in sql


def test_boolean_cast_for_closed_deals() -> None:
    contract = _by_identity()[("olist_marketing_funnel", "closed_deals")]
    sql = render_silver_sql(
        contract,
        "odp-warehouse",
        policy_for(contract),
    )
    assert "WHEN lower(`has_company`) IN ('true') THEN true" in sql
    assert "TRY_CAST(`declared_monthly_revenue` AS DOUBLE)" in sql


def test_iceberg_type_map() -> None:
    assert iceberg_type("integer") == "BIGINT"
    assert iceberg_type("number") == "DOUBLE"
    assert iceberg_type("json") == "STRING"
    with pytest.raises(SilverError):
        iceberg_type("uuid")


def test_selected_entries_require_source_for_dataset() -> None:
    with pytest.raises(SilverError, match="--dataset requires --source"):
        selected_entries(source_id=None, datasets=["orders"], all_sources=False)


def test_cli_prints_live_checks(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--print-live-checks"]) == 0
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == len(live_checks())
    assert "iceberg.silver." in lines[0]
    assert "dimensions_width" in out or "cardinality(tags)" in out


def test_cli_writes_all_sources(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODP_WAREHOUSE_BUCKET", "odp-warehouse")
    out = tmp_path / "silver-sql"
    assert main(["--all", "--sql-out", str(out)]) == 0
    names = sorted(path.name for path in out.glob("*.sql"))
    assert "dummyjson_products__products.sql" in names
    assert "olist_brazilian_ecommerce__orders.sql" in names
    assert len(names) == sum(len(policies) for policies in SOURCE_MODULES.values())


def test_cli_writes_one_source(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODP_WAREHOUSE_BUCKET", "odp-warehouse")
    out = tmp_path / "silver-sql"
    assert (
        main(
            [
                "--source",
                "demo_storefront",
                "--sql-out",
                str(out),
            ]
        )
        == 0
    )
    assert list(out.glob("*.sql")) == [out / "demo_storefront__clickstream.sql"]


def test_empty_pk_without_payload_partition_fails() -> None:
    contract = _by_identity()[("olist_brazilian_ecommerce", "geolocation")]
    with pytest.raises(SilverError, match="partition_payload_fields"):
        render_silver_sql(
            contract,
            "odp-warehouse",
            DedupPolicy(
                partition_keys=(),
                partition_payload_fields=False,
                timestamp_style=TimestampStyle.OLIST_CSV,
            ),
        )
