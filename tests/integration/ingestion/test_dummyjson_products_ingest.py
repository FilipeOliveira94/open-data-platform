"""MinIO round-trip for DummyJSON product pages. Uses fixture bytes only."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.ingestion.dummyjson_products import (
    PageRequest,
    PageResponse,
    Transport,
    ingest_dummyjson_products,
)
from open_data_platform.object_storage import client_from_env

pytestmark = pytest.mark.integration

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "dummyjson_products"


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


def _pages(bodies: dict[int, bytes]) -> Transport:
    def transport(request: PageRequest, timeout: float) -> PageResponse:
        del timeout
        skip = int(parse_qs(urlsplit(request.url).query)["skip"][0])
        return PageResponse(status=200, headers={}, body=bodies[skip])

    return transport


def test_dummyjson_products_round_trip_overwrites_the_same_pages() -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = os.environ.get("ODP_RAW_BUCKET", "odp-raw")
    execution_id = f"p105-{uuid.uuid4().hex}"
    ingestion_ts = datetime(2026, 10, 24, 18, 0, tzinfo=UTC)
    original = {
        0: (_FIXTURES / "page-skip-0.json").read_bytes(),
        2: (_FIXTURES / "page-skip-2.json").read_bytes(),
    }
    updated_page = json.loads(original[0])
    updated_page["products"][0]["title"] = "replacement"
    updated = dict(original)
    updated[0] = json.dumps(updated_page).encode()
    results = None
    try:
        try:
            results = ingest_dummyjson_products(
                client,
                bucket=bucket,
                execution_id=execution_id,
                ingestion_ts=ingestion_ts,
                limit=2,
                base_url="https://dummyjson.test",
                transport=_pages(original),
                sleep=lambda _seconds: None,
            )
        except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
            pytest.skip(f"MinIO not reachable: {exc}")

        assert len(results) == 2
        assert results[0].key.endswith("/products-skip-000000.json")
        assert results[1].key.endswith("/products-skip-000002.json")
        assert "/dummyjson_products/products/" in results[0].key
        stored = client.get_object(Bucket=results[0].bucket, Key=results[0].key)
        assert stored["Body"].read() == original[0]

        again = ingest_dummyjson_products(
            client,
            bucket=bucket,
            execution_id=execution_id,
            ingestion_ts=ingestion_ts,
            limit=2,
            base_url="https://dummyjson.test",
            transport=_pages(updated),
            sleep=lambda _seconds: None,
        )
        assert again[0].key == results[0].key
        reread = client.get_object(Bucket=results[0].bucket, Key=results[0].key)
        assert reread["Body"].read() == updated[0]
    finally:
        if results is not None:
            for item in results:
                client.delete_object(Bucket=item.bucket, Key=item.key)
