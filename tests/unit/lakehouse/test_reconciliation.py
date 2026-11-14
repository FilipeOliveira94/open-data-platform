"""Count checks derived from a run, and Silver types taken from contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.lakehouse.bronze import bronze_columns, render_bronze_sql
from open_data_platform.lakehouse.reconciliation import (
    ReconciliationError,
    bronze_count_checks,
    format_counts,
    parse_counts,
    require_expected,
    shape_checks,
    silver_live_checks,
    trino_ident,
)
from open_data_platform.lakehouse.silver.cli import (
    SOURCE_MODULES,
    live_checks,
    policy_for,
)
from open_data_platform.lakehouse.silver.common import (
    ICEBERG_TYPES,
    resolve_output_columns,
)

ROOT = Path(__file__).resolve().parents[3]


def _sql_with(
    checks: tuple[tuple[str, str], ...],
    table: str,
    *,
    grouped: bool = False,
) -> str:
    for _expected, sql in checks:
        if table not in sql:
            continue
        if grouped and "GROUP BY" not in sql:
            continue
        return sql
    msg = f"missing check for {table}"
    raise AssertionError(msg)


def test_count_file_round_trip() -> None:
    text = format_counts(
        (
            ("dummyjson_products.products", 0),
            ("ibge_localidades.municipios", 12),
        )
    )
    assert parse_counts(text) == {
        "dummyjson_products.products": 0,
        "ibge_localidades.municipios": 12,
    }


def test_parse_counts_rejects_a_bad_line() -> None:
    with pytest.raises(ReconciliationError, match="name and count"):
        parse_counts("dummyjson_products.products 7\n")


def test_bronze_checks_use_the_file_not_a_snapshot_pin() -> None:
    contracts = load_contracts()
    counts = {f"{item.source_id}.{item.dataset}": 7 for item in contracts}
    checks = bronze_count_checks(counts, contracts)
    assert len(checks) == len(contracts)
    orders = (
        "SELECT COUNT(*) AS row_count FROM "
        "iceberg.bronze.olist_brazilian_ecommerce__orders"
    )
    by_sql = {sql: count for count, sql in checks}
    assert by_sql[orders] == "7"
    rendered = "\n".join(f"{count}\t{sql}" for count, sql in checks)
    assert "99441" not in rendered
    assert "194\t" not in rendered


def test_bronze_checks_reject_a_partial_file() -> None:
    contracts = load_contracts()
    first = contracts[0]
    counts = {f"{first.source_id}.{first.dataset}": 1}
    with pytest.raises(ReconciliationError, match="missing="):
        bronze_count_checks(counts, contracts)


def test_require_expected_names_both_sides() -> None:
    with pytest.raises(ReconciliationError, match="orders: expected 7 got 8"):
        require_expected("7", "8", check="orders")
    require_expected("ok", "ok", check="orders")


def test_silver_checks_compare_bronze_keys_and_do_not_pin_sizes() -> None:
    contracts = {(item.source_id, item.dataset): item for item in load_contracts()}
    entries = tuple(
        (contracts[(source_id, dataset)], policy)
        for source_id, policies in SOURCE_MODULES.items()
        for dataset, policy in policies.items()
    )
    checks = silver_live_checks(entries)
    assert all(expected == "ok" for expected, _sql in checks)
    rendered = "\n".join(sql for _expected, sql in checks)
    assert "738332" not in rendered
    assert "99441" not in rendered
    assert "194" not in rendered
    products = _sql_with(checks, "dummyjson_products__products", grouped=True)
    assert "GROUP BY id" in products
    geolocation = _sql_with(checks, "olist_brazilian_ecommerce__geolocation")
    assert "geolocation_zip_code_prefix" in geolocation
    assert "GROUP BY geolocation_zip_code_prefix, geolocation_lat" in geolocation
    postgres = _sql_with(checks, "postgres_oltp__customers", grouped=True)
    assert "GROUP BY customer_id" in postgres
    assert "updated_at" not in postgres.split("GROUP BY", maxsplit=1)[1]
    municipios = _sql_with(checks, "ibge_localidades__municipios", grouped=True)
    assert 'GROUP BY "municipio-id"' in municipios
    shape = "\n".join(sql for _ok, sql in shape_checks())
    assert "dimensions_width" in shape
    assert "meta_barcode" in shape
    assert "cardinality(tags)" in shape
    assert "cart_or_purchase_without_product" in shape
    assert "page_view" not in shape


def test_cli_live_checks_match_every_silver_dataset() -> None:
    checks = live_checks()
    dataset_groups = [sql for _ok, sql in checks if "GROUP BY" in sql]
    dataset_count = sum(len(policies) for policies in SOURCE_MODULES.values())
    assert len(dataset_groups) == dataset_count
    assert len(checks) == len(dataset_groups) + len(shape_checks())
    assert all("\t" not in sql for _ok, sql in checks)


def test_silver_types_follow_the_contract_and_json_expansion() -> None:
    for contract in load_contracts():
        policy = policy_for(contract)
        resolved = resolve_output_columns(contract, policy)
        columns = {column.name: column.iceberg_type for column in resolved}
        metadata = (
            "source_id",
            "execution_id",
            "_ingestion_ts",
            "raw_object_key",
            "record_index",
            "dt",
        )
        for name in metadata:
            assert columns[name] == "STRING"
        expanded = {item.source_field for item in policy.json_expansions}
        for field in (*contract.fields, *contract.envelope_fields):
            locator = contract.record_locator
            if locator is not None and field.name == locator:
                continue
            if field.name in expanded:
                expansion = next(
                    item
                    for item in policy.json_expansions
                    if item.source_field == field.name
                )
                produced = {
                    column.name: column.iceberg_type for column in expansion.columns
                }
                if field.name in produced:
                    assert columns[field.name] == produced[field.name]
                else:
                    assert field.name not in columns
                continue
            assert columns[field.name] == ICEBERG_TYPES[field.field_type]
    products = next(
        item
        for item in load_contracts()
        if item.source_id == "dummyjson_products" and item.dataset == "products"
    )
    expanded = {
        column.name: column.iceberg_type
        for column in resolve_output_columns(products, policy_for(products))
    }
    assert expanded["dimensions_width"] == "DOUBLE"
    assert expanded["meta_barcode"] == "STRING"
    assert expanded["meta_created_at"] == "TIMESTAMP"
    assert expanded["tags"].startswith("ARRAY<")
    assert "reviews" in expanded


def test_bronze_load_columns_stay_string() -> None:
    for contract in load_contracts():
        sql = render_bronze_sql([(contract, 0)], "odp-warehouse")
        for name in bronze_columns(contract):
            assert f"`{name}` STRING" in sql


def test_trino_ident_quotes_hyphenated_names() -> None:
    assert trino_ident("order_id") == "order_id"
    assert trino_ident("municipio-id") == '"municipio-id"'


def test_reconciliation_sql_covers_every_gold_model() -> None:
    models = {
        path.relative_to(ROOT / "dbt" / "models").with_suffix("")
        for path in (ROOT / "dbt" / "models").rglob("*.sql")
    }
    tests = {
        path.relative_to(ROOT / "dbt" / "tests" / "reconciliation").with_suffix("")
        for path in (ROOT / "dbt" / "tests" / "reconciliation").rglob("*.sql")
    }
    assert models == tests


def test_reconciliation_is_outside_the_gold_build() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    project = (ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    runner = (
        ROOT / "src" / "open_data_platform" / "lakehouse" / "medallion_build.py"
    ).read_text(encoding="utf-8")
    assert '"--exclude", "tag:reconciliation"' in runner
    assert '"--select", "tag:reconciliation"' in runner
    assert "dbt-test-reconciliation:" in makefile
    assert "$(call medallion_step,dbt-build)" in makefile
    assert "$(call medallion_step,dbt-reconcile)" in makefile
    assert "live-gold: stage-quarantine dbt-build" in makefile
    for domain in (
        "gold_shared",
        "brazil_geography",
        "marketplace_operations",
        "customer_crm",
        "seller_acquisition",
        "catalog_commerce",
        "storefront_behavior",
        "oltp_operations",
    ):
        assert f'+tags: ["reconciliation", "{domain}"]' in project


def test_quality_schema_names_every_table_model() -> None:
    ephemeral = {"int_order_primary_payment"}
    models = {
        path.stem
        for path in (ROOT / "dbt" / "models").rglob("*.sql")
        if path.stem not in ephemeral
    }
    named: set[str] = set()
    for path in (ROOT / "dbt" / "models").rglob("schema.yml"):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("- name: "):
                named.add(stripped.removeprefix("- name: "))
    assert models <= named
