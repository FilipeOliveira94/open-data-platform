"""Unit tests for the minimal raw writer (mocked S3 client)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from open_data_platform.ingestion.writer import write_raw_object


def _client_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )


def test_write_raw_object_puts_key_and_metadata() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = _client_error()
    ts = datetime(2026, 10, 18, 15, 0, 0, tzinfo=UTC)
    body = b'{"ok": true}'

    result = write_raw_object(
        client,
        bucket="odp-raw",
        body=body,
        source_id="dummyjson_products",
        dataset="products",
        execution_id="run-001",
        object_name="products.json",
        ingestion_ts=ts,
        content_type="application/json",
    )

    assert result.bucket == "odp-raw"
    assert result.key == (
        "raw/dummyjson_products/products/dt=2026-10-18/"
        "execution_id=run-001/products.json"
    )
    assert result.metadata.source_id == "dummyjson_products"
    assert result.metadata.execution_id == "run-001"
    assert result.metadata.to_object_metadata()["_ingestion_ts"] == (
        "2026-10-18T15:00:00Z"
    )

    client.create_bucket.assert_called_once_with(Bucket="odp-raw")
    client.put_object.assert_called_once_with(
        Bucket="odp-raw",
        Key=result.key,
        Body=body,
        Metadata={
            "source_id": "dummyjson_products",
            "execution_id": "run-001",
            "_ingestion_ts": "2026-10-18T15:00:00Z",
        },
        ContentType="application/json",
    )


def test_write_raw_object_same_identity_same_key() -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}
    ts = datetime(2026, 10, 18, 12, 0, 0, tzinfo=UTC)
    kwargs: dict[str, Any] = {
        "bucket": "odp-raw",
        "source_id": "olist_brazilian_ecommerce",
        "dataset": "orders",
        "execution_id": "backfill-1",
        "object_name": "orders.csv",
        "ingestion_ts": ts,
    }

    first = write_raw_object(client, body=b"v1", **kwargs)
    second = write_raw_object(client, body=b"v2", **kwargs)

    assert first.key == second.key
    assert client.put_object.call_count == 2
    assert client.put_object.call_args_list[0].kwargs["Body"] == b"v1"
    assert client.put_object.call_args_list[1].kwargs["Body"] == b"v2"
