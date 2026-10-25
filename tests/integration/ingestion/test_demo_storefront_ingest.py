"""MinIO round-trip for the fictional storefront clickstream."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.ingestion.demo_storefront import ingest_demo_storefront
from open_data_platform.object_storage import client_from_env

pytestmark = pytest.mark.integration

_START = datetime(2026, 10, 18, tzinfo=UTC)
_INGESTION_TS = datetime(2026, 10, 25, 15, 0, tzinfo=UTC)


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


def test_demo_storefront_round_trip_overwrites_one_key_and_keeps_another() -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = os.environ.get("ODP_RAW_BUCKET", "odp-raw")
    execution_id = f"p107-{uuid.uuid4().hex}"
    other_execution_id = f"p107-{uuid.uuid4().hex}"
    first = None
    second = None
    try:
        try:
            first = ingest_demo_storefront(
                client,
                bucket=bucket,
                execution_id=execution_id,
                seed=11,
                count=4,
                event_time_start=_START,
                ingestion_ts=_INGESTION_TS,
            )
        except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
            pytest.skip(f"MinIO not reachable: {exc}")

        stored = client.get_object(Bucket=first.bucket, Key=first.key)
        original = stored["Body"].read()
        assert original.count(b"\n") == 4
        assert first.key.startswith(
            "raw/demo_storefront/clickstream/dt=2026-10-25/execution_id="
        )
        assert first.key.endswith("/events.jsonl")

        replaced = ingest_demo_storefront(
            client,
            bucket=bucket,
            execution_id=execution_id,
            seed=12,
            count=4,
            event_time_start=_START,
            ingestion_ts=_INGESTION_TS,
        )
        assert replaced.key == first.key
        reread = client.get_object(Bucket=first.bucket, Key=first.key)
        replaced_body = reread["Body"].read()
        assert replaced_body != original

        second = ingest_demo_storefront(
            client,
            bucket=bucket,
            execution_id=other_execution_id,
            seed=11,
            count=4,
            event_time_start=_START,
            ingestion_ts=_INGESTION_TS,
        )
        assert second.key != first.key
        kept = client.get_object(Bucket=first.bucket, Key=first.key)
        assert kept["Body"].read() == replaced_body
        other = client.get_object(Bucket=second.bucket, Key=second.key)
        assert other["Body"].read() == original
    finally:
        for item in (first, second):
            if item is None:
                continue
            client.delete_object(Bucket=item.bucket, Key=item.key)
