"""Live MinIO smoke test (requires Compose stack)."""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.object_storage import client_from_env, put_and_get_object

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


def test_minio_put_and_get_object() -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = os.environ.get("MINIO_SMOKE_BUCKET", "odp-smoke")
    key = f"phase0/{uuid.uuid4().hex}.txt"
    payload = b"open-data-platform-minio-smoke"

    try:
        result = put_and_get_object(client, bucket=bucket, key=key, body=payload)
    except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
        pytest.skip(f"MinIO not reachable: {exc}")

    assert result == payload
