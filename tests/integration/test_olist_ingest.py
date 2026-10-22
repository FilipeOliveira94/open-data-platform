"""MinIO round-trip for Olist raw writes. Uses synthetic CSVs only."""

from __future__ import annotations

import csv
import io
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

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


def test_olist_ingest_round_trip_overwrites_the_same_object(tmp_path: Path) -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = os.environ.get("ODP_RAW_BUCKET", "odp-raw")
    execution_id = f"p104-{uuid.uuid4().hex}"
    ingestion_ts = datetime(2026, 10, 22, 15, 0, tzinfo=UTC)
    contracts = {(item.source_id, item.dataset): item for item in load_contracts()}
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage(ecommerce, ECOMMERCE.source_id, ECOMMERCE.files, contracts)
    _stage(funnel, FUNNEL.source_id, FUNNEL.files, contracts)
    orders_path = ecommerce / "olist_orders_dataset.csv"
    original = orders_path.read_bytes()

    results = None
    try:
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

        orders_path.write_bytes(original + b"replacement\n")
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
        assert reread["Body"].read() == original + b"replacement\n"
        assert len(results) == 11
    finally:
        if results is not None:
            for item in results:
                client.delete_object(Bucket=item.bucket, Key=item.key)
