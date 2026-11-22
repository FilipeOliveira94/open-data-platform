"""MinIO round-trip for Olist raw writes. Uses synthetic CSVs only."""

from __future__ import annotations

import csv
import io
import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion.olist import (
    ECOMMERCE,
    FUNNEL,
    ingest_olist_historical,
)
from open_data_platform.object_storage import client_from_env

pytestmark = pytest.mark.integration


def _minio_env() -> dict[str, str]:
    required = ("MINIO_ENDPOINT_URL", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Missing MinIO settings: {', '.join(missing)}")
    return {
        "MINIO_ENDPOINT_URL": os.environ["MINIO_ENDPOINT_URL"],
        "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
        "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
        "MINIO_REGION": os.environ.get("MINIO_REGION", "us-east-1"),
    }


def _stage(directory: Path, source_id: str, files: tuple, contracts: dict) -> None:
    directory.mkdir(parents=True)
    for spec in files:
        contract = contracts[(source_id, spec.dataset)]
        buffer = io.StringIO()
        csv.writer(buffer).writerow([field.name for field in contract.fields])
        (directory / spec.object_name).write_text(buffer.getvalue(), encoding="utf-8")


def _replacement_row(contract: Any) -> bytes:
    """One data row with the contract's column count (a structural match)."""
    buffer = io.StringIO()
    csv.writer(buffer).writerow(["replacement"] * len(contract.fields))
    return buffer.getvalue().encode("utf-8")


def test_olist_ingest_round_trip_overwrites_the_same_object(
    tmp_path: Path,
    test_raw_bucket: str,
    purge_executions: Callable[[Any, str, str], None],
) -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = test_raw_bucket
    execution_id = f"p104-{uuid.uuid4().hex}"
    purge_executions(client, bucket, execution_id)
    ingestion_ts = datetime(2026, 10, 22, 15, 0, tzinfo=UTC)
    contracts = {(item.source_id, item.dataset): item for item in load_contracts()}
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage(ecommerce, ECOMMERCE.source_id, ECOMMERCE.files, contracts)
    _stage(funnel, FUNNEL.source_id, FUNNEL.files, contracts)
    orders_path = ecommerce / "olist_orders_dataset.csv"
    original = orders_path.read_bytes()
    replaced = original + _replacement_row(contracts[(ECOMMERCE.source_id, "orders")])

    try:
        results = ingest_olist_historical(
            client,
            bucket=bucket,
            execution_id=execution_id,
            ingestion_ts=ingestion_ts,
            ecommerce_dir=ecommerce,
            funnel_dir=funnel,
        )
    except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
        pytest.skip(f"MinIO not reachable: {exc}")

    orders = next(
        item for item in results if item.key.endswith("/olist_orders_dataset.csv")
    )
    stored = client.get_object(Bucket=orders.bucket, Key=orders.key)
    assert stored["Body"].read() == original

    orders_path.write_bytes(replaced)
    again = ingest_olist_historical(
        client,
        bucket=bucket,
        execution_id=execution_id,
        ingestion_ts=ingestion_ts,
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
    )
    assert again[0].key == results[0].key
    reread = client.get_object(Bucket=orders.bucket, Key=orders.key)
    assert reread["Body"].read() == replaced
    assert len(results) == 11
