"""Unit tests for ingestion metadata helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from open_data_platform.ingestion.metadata import IngestionMetadata


def test_to_object_metadata_uses_documented_field_names() -> None:
    meta = IngestionMetadata(
        source_id="ibge_localidades",
        execution_id="run-42",
        ingestion_ts=datetime(2026, 10, 18, 15, 30, 0, tzinfo=UTC),
    )
    assert meta.to_object_metadata() == {
        "source_id": "ibge_localidades",
        "execution_id": "run-42",
        "_ingestion_ts": "2026-10-18T15:30:00Z",
    }


def test_create_defaults_ingestion_ts_to_utc_now() -> None:
    before = datetime.now(UTC)
    meta = IngestionMetadata.create(source_id="synthetic_events", execution_id="e1")
    after = datetime.now(UTC)
    assert meta.source_id == "synthetic_events"
    assert before <= meta.ingestion_ts <= after
    assert meta.ingestion_ts.tzinfo is not None


def test_naive_ingestion_ts_formatted_as_utc() -> None:
    meta = IngestionMetadata(
        source_id="dummyjson_products",
        execution_id="run-1",
        ingestion_ts=datetime(2026, 10, 18, 12, 0, 0),
    )
    assert meta.to_object_metadata()["_ingestion_ts"] == "2026-10-18T12:00:00Z"


@pytest.mark.parametrize("field", ["source_id", "execution_id"])
def test_empty_identity_fields_rejected(field: str) -> None:
    kwargs = {
        "source_id": "ok",
        "execution_id": "ok",
        "ingestion_ts": datetime(2026, 10, 18, tzinfo=UTC),
    }
    kwargs[field] = "   "
    with pytest.raises(ValueError):
        IngestionMetadata(**kwargs)
