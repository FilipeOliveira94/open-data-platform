"""Structural tests for planned-source data contracts."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from open_data_platform.contracts.load import contract_from_mapping, load_contracts
from open_data_platform.contracts.model import ContractError

EXPECTED_DATASETS = {
    ("olist_brazilian_ecommerce", "customers"),
    ("olist_brazilian_ecommerce", "geolocation"),
    ("olist_brazilian_ecommerce", "order_items"),
    ("olist_brazilian_ecommerce", "order_payments"),
    ("olist_brazilian_ecommerce", "order_reviews"),
    ("olist_brazilian_ecommerce", "orders"),
    ("olist_brazilian_ecommerce", "products"),
    ("olist_brazilian_ecommerce", "sellers"),
    ("olist_brazilian_ecommerce", "product_category_name_translation"),
    ("olist_marketing_funnel", "qualified_leads"),
    ("olist_marketing_funnel", "closed_deals"),
    ("dummyjson_products", "products"),
    ("postgres_oltp", "sellers"),
    ("postgres_oltp", "customers"),
    ("postgres_oltp", "orders"),
    ("postgres_oltp", "order_items"),
    ("ibge_localidades", "municipios"),
    ("demo_storefront", "clickstream"),
}


def _by_identity() -> dict[tuple[str, str], object]:
    loaded = load_contracts()
    return {(item.source_id, item.dataset): item for item in loaded}


def test_shipped_contracts_cover_planned_sources() -> None:
    loaded = load_contracts()
    identities = {(item.source_id, item.dataset) for item in loaded}
    assert identities == EXPECTED_DATASETS


def test_shipped_contracts_declare_governance_and_invalid_policy() -> None:
    for contract in load_contracts():
        assert contract.owner == "platform"
        assert contract.on_invalid == "quarantine"


def test_classification_is_the_strictest_field() -> None:
    orders = _by_identity()[("olist_brazilian_ecommerce", "orders")]
    classes = {field.classification for field in orders.fields}
    assert classes == {"internal", "pii"}
    assert orders.classification == "pii"


def test_olist_products_keep_published_column_spelling() -> None:
    products = _by_identity()[("olist_brazilian_ecommerce", "products")]
    names = [field.name for field in products.fields]
    assert "product_name_lenght" in names
    assert "product_description_lenght" in names


def test_geolocation_has_no_primary_key() -> None:
    geolocation = _by_identity()[("olist_brazilian_ecommerce", "geolocation")]
    assert geolocation.primary_key == ()
    assert geolocation.classification == "pii"


def test_dummyjson_page_envelope_is_not_an_olist_key() -> None:
    products = _by_identity()[("dummyjson_products", "products")]
    assert products.record_locator == "products"
    assert products.domain == "product_analysis"
    assert [field.name for field in products.envelope_fields] == [
        "products",
        "total",
        "skip",
        "limit",
    ]
    assert products.classification == "public"


def test_postgres_v1_is_windowed_and_has_updated_at() -> None:
    contracts = _by_identity()
    for dataset in ("sellers", "customers", "orders", "order_items"):
        contract = contracts[("postgres_oltp", dataset)]
        assert contract.update_pattern == "incremental_window"
        assert contract.payload_format == "json"
        updated_at = next(
            field for field in contract.fields if field.name == "updated_at"
        )
        assert updated_at.nullable is False
        assert updated_at.field_type == "timestamp"


def test_demo_storefront_clickstream_is_jsonl_without_a_join_key() -> None:
    clickstream = _by_identity()[("demo_storefront", "clickstream")]
    assert clickstream.payload_format == "jsonl"
    assert clickstream.provenance_system == "generator"
    assert clickstream.domain == "product_analysis"
    assert clickstream.update_pattern == "snapshot"
    assert clickstream.primary_key == ("event_id",)
    names = [field.name for field in clickstream.fields]
    assert names == [
        "event_id",
        "event_time",
        "event_type",
        "session_id",
        "user_id",
        "product_id",
    ]
    product_id = clickstream.fields[-1]
    assert product_id.nullable is True
    assert clickstream.classification == "public"


def test_ibge_municipios_include_region_attributes() -> None:
    municipios = _by_identity()[("ibge_localidades", "municipios")]
    by_name = {field.name: field for field in municipios.fields}
    assert municipios.primary_key == ("municipio-id",)
    assert by_name["microrregiao-id"].nullable is True
    assert "UF-sigla" in by_name
    assert "regiao-nome" in by_name
    assert municipios.update_pattern == "full_refresh"


def _minimal_mapping() -> dict[str, object]:
    return {
        "contract_version": 1,
        "source_id": "dummyjson_products",
        "dataset": "products",
        "description": "Example contract for structural tests.",
        "payload_format": "json",
        "grain": "One product.",
        "primary_key": ["id"],
        "update_pattern": "full_refresh",
        "on_invalid": "quarantine",
        "on_structural_error": "fail_batch",
        "governance": {"domain": "product_analysis", "owner": "platform"},
        "provenance": {"system": "dummyjson"},
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": False,
                "classification": "public",
                "description": "Identifier.",
            }
        ],
        "changelog": [{"version": 1, "note": "Initial."}],
    }


def test_contract_from_mapping_accepts_minimal_document() -> None:
    contract = contract_from_mapping(_minimal_mapping(), origin="test")
    assert contract.dataset == "products"
    assert contract.classification == "public"


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        ("drop_nullable", "missing keys: nullable"),
        ("drop_policy", "on_invalid must be one of"),
        ("unknown_key", "unknown contract keys: retention_days"),
        ("bad_primary_key", "primary_key field 'missing' is not in fields"),
        ("duplicate_field", "duplicate field name 'id'"),
        ("structural_error_not_fail", "on_structural_error must be one of"),
        ("envelope_without_locator", "envelope_fields requires record_locator"),
        ("missing_domain", "missing governance keys: domain"),
    ],
)
def test_contract_from_mapping_rejects_structural_errors(
    mutate: str,
    expected: str,
) -> None:
    data = _minimal_mapping()
    if mutate == "drop_nullable":
        del data["fields"][0]["nullable"]  # type: ignore[index]
    elif mutate == "drop_policy":
        data["on_invalid"] = "drop"
    elif mutate == "unknown_key":
        data["retention_days"] = 30
    elif mutate == "bad_primary_key":
        data["primary_key"] = ["missing"]
    elif mutate == "duplicate_field":
        data["fields"] = [data["fields"][0], dict(data["fields"][0])]  # type: ignore[index]
    elif mutate == "structural_error_not_fail":
        data["on_structural_error"] = "quarantine"
    elif mutate == "envelope_without_locator":
        data["envelope_fields"] = data["fields"]
    elif mutate == "missing_domain":
        del data["governance"]["domain"]  # type: ignore[index]
    with pytest.raises(ContractError, match=expected):
        contract_from_mapping(data, origin="test")


def test_load_contract_file_requires_matching_directory(tmp_path: Path) -> None:
    payload = _minimal_mapping()
    target = tmp_path / "wrong_source" / "products.yaml"
    target.parent.mkdir()
    target.write_text(yaml.safe_dump(payload), encoding="utf-8")
    from open_data_platform.contracts.load import load_contract_file

    with pytest.raises(ContractError, match="parent directory"):
        load_contract_file(target)


def test_load_contracts_rejects_unexpected_files(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("not a contract", encoding="utf-8")
    with pytest.raises(ContractError, match="unexpected file"):
        load_contracts(tmp_path)
