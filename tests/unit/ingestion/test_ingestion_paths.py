"""Unit tests for raw object-key builder."""

from __future__ import annotations

from datetime import date

import pytest

from open_data_platform.ingestion.paths import build_raw_object_key


def test_build_raw_object_key_deterministic() -> None:
    kwargs = {
        "source_id": "dummyjson_products",
        "dataset": "products",
        "execution_id": "run-001",
        "object_name": "products.json",
        "ingestion_date": date(2026, 10, 18),
    }
    key_a = build_raw_object_key(**kwargs)
    key_b = build_raw_object_key(**kwargs)
    assert key_a == key_b
    assert key_a == (
        "raw/dummyjson_products/products/dt=2026-10-18/"
        "execution_id=run-001/products.json"
    )


def test_build_raw_object_key_accepts_uuid_execution_id() -> None:
    key = build_raw_object_key(
        source_id="postgres_oltp",
        dataset="orders",
        execution_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        object_name="orders.json",
        ingestion_date=date(2026, 10, 18),
    )
    assert "execution_id=a1b2c3d4-e5f6-7890-abcd-ef1234567890" in key


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_id", "BadSource"),
        ("source_id", "has/slash"),
        ("dataset", "../escape"),
        ("execution_id", "bad/id"),
        ("object_name", "nested/path.json"),
        ("object_name", ""),
    ],
)
def test_build_raw_object_key_rejects_invalid_segments(
    field: str,
    value: str,
) -> None:
    kwargs: dict[str, object] = {
        "source_id": "olist_brazilian_ecommerce",
        "dataset": "orders",
        "execution_id": "run-001",
        "object_name": "orders.csv",
        "ingestion_date": date(2026, 10, 18),
    }
    kwargs[field] = value
    with pytest.raises(ValueError):
        build_raw_object_key(**kwargs)  # type: ignore[arg-type]
