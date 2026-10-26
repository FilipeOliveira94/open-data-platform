"""Quarantine sidecar writes. The storage client is a mock."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from open_data_platform.contracts.load import contract_from_mapping
from open_data_platform.contracts.model import DataContract
from open_data_platform.ingestion.quarantine import (
    InvalidRecordsError,
    prepare_quarantine,
    write_quarantine_object,
)
from open_data_platform.ingestion.writer import write_raw_object

_TS = datetime(2026, 10, 26, 12, tzinfo=UTC)


def _contract() -> DataContract:
    return contract_from_mapping(
        {
            "contract_version": 1,
            "source_id": "demo_storefront",
            "dataset": "clickstream",
            "description": "Quarantine fixture.",
            "payload_format": "json",
            "grain": "One row.",
            "primary_key": ["id"],
            "update_pattern": "snapshot",
            "on_invalid": "quarantine",
            "on_structural_error": "fail_batch",
            "governance": {"domain": "product_analysis", "owner": "platform"},
            "provenance": {"system": "generator"},
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": False,
                    "classification": "public",
                    "description": "Identifier.",
                },
                {
                    "name": "label",
                    "type": "string",
                    "nullable": True,
                    "classification": "public",
                    "description": "Optional label.",
                },
            ],
            "changelog": [{"version": 1, "note": "Fixture."}],
        },
        origin="test",
    )


def _client() -> MagicMock:
    client = MagicMock()
    client.head_bucket.return_value = {}
    return client


def _land(
    client: MagicMock,
    contract: DataContract,
    records: list[dict[str, object]],
    *,
    body: bytes,
    execution_id: str,
) -> None:
    sidecar = prepare_quarantine(contract, records)
    result = write_raw_object(
        client,
        bucket="odp-raw",
        body=body,
        source_id=contract.source_id,
        dataset=contract.dataset,
        execution_id=execution_id,
        object_name="rows.json",
        ingestion_ts=_TS,
        content_type="application/json",
    )
    write_quarantine_object(
        client,
        bucket="odp-raw",
        source_id=contract.source_id,
        dataset=contract.dataset,
        execution_id=execution_id,
        object_name="rows.json",
        ingestion_ts=result.metadata.ingestion_ts,
        body=sidecar,
    )


def test_invalid_rows_keep_the_raw_body_and_write_jsonl() -> None:
    client = _client()

    _land(
        client,
        _contract(),
        [{"id": None, "label": "a"}],
        body=b'[{"id": null}]',
        execution_id="run-a",
    )

    raw, sidecar = (call.kwargs for call in client.put_object.call_args_list)
    assert raw["Key"] == (
        "raw/demo_storefront/clickstream/dt=2026-10-26/execution_id=run-a/rows.json"
    )
    assert raw["Body"] == b'[{"id": null}]'
    assert sidecar["Key"] == (
        "quarantine/demo_storefront/clickstream/dt=2026-10-26/"
        "execution_id=run-a/rows.json.jsonl"
    )
    assert sidecar["ContentType"] == "application/x-ndjson"
    assert sidecar["Metadata"]["_ingestion_ts"] == "2026-10-26T12:00:00Z"
    finding = json.loads(sidecar["Body"])
    assert finding["reason"] == "null_violation"
    assert finding["field"] == "id"
    assert finding["record"] == {"id": None, "label": "a"}
    assert client.delete_object.call_count == 0


def test_fail_batch_writes_neither_object() -> None:
    client = _client()
    contract = replace(_contract(), on_invalid="fail_batch")

    with pytest.raises(InvalidRecordsError, match="invalid records fail the batch"):
        _land(
            client,
            contract,
            [{"id": None, "label": "a"}],
            body=b"raw",
            execution_id="run-a",
        )

    assert client.put_object.call_count == 0
    assert client.delete_object.call_count == 0


def test_clean_rerun_deletes_only_that_execution() -> None:
    client = _client()
    contract = _contract()
    _land(
        client,
        contract,
        [{"id": None, "label": "a"}],
        body=b"first",
        execution_id="run-a",
    )
    stale = client.put_object.call_args.kwargs["Key"]

    _land(
        client,
        contract,
        [{"id": 1, "label": None}],
        body=b"second",
        execution_id="run-a",
    )
    _land(
        client,
        contract,
        [{"id": 2, "label": None}],
        body=b"third",
        execution_id="run-b",
    )

    deleted = [call.kwargs["Key"] for call in client.delete_object.call_args_list]
    assert deleted == [
        stale,
        stale.replace("execution_id=run-a", "execution_id=run-b"),
    ]
    raw_bodies = [
        call.kwargs["Body"]
        for call in client.put_object.call_args_list
        if call.kwargs["Key"].startswith("raw/")
    ]
    assert raw_bodies == [b"first", b"second", b"third"]
