"""MinIO round-trip for IBGE municipalities. Uses fixture bytes only."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.ingestion.http import HttpRequest, HttpResponse, Transport
from open_data_platform.ingestion.ibge_localidades import ingest_ibge_localidades
from open_data_platform.object_storage import client_from_env

pytestmark = pytest.mark.integration

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "ibge_localidades"
_INGESTION_TS = datetime(2026, 10, 25, 18, 0, tzinfo=UTC)


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


def _body(payload: bytes) -> Transport:
    def transport(request: HttpRequest, timeout: float) -> HttpResponse:
        del request, timeout
        return HttpResponse(status=200, headers={}, body=payload)

    return transport


def test_ibge_municipios_round_trip_overwrites_one_key_and_keeps_another() -> None:
    env = _minio_env()
    client = client_from_env(env)
    bucket = os.environ.get("ODP_RAW_BUCKET", "odp-raw")
    execution_id = f"p108-{uuid.uuid4().hex}"
    other_execution_id = f"p108-{uuid.uuid4().hex}"
    original = (_FIXTURES / "municipios.json").read_bytes()
    updated_payload = json.loads(original)
    updated_payload[0]["municipio-nome"] = "Replacement"
    updated = json.dumps(updated_payload).encode()
    first = None
    second = None
    try:
        try:
            first = ingest_ibge_localidades(
                client,
                bucket=bucket,
                execution_id=execution_id,
                ingestion_ts=_INGESTION_TS,
                base_url="https://ibge.test",
                transport=_body(original),
                sleep=lambda _seconds: None,
            )
        except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
            pytest.skip(f"MinIO not reachable: {exc}")

        assert first.key.startswith(
            "raw/ibge_localidades/municipios/dt=2026-10-25/execution_id="
        )
        assert first.key.endswith("/municipios.json")
        stored = client.get_object(Bucket=first.bucket, Key=first.key)
        assert stored["Body"].read() == original

        replaced = ingest_ibge_localidades(
            client,
            bucket=bucket,
            execution_id=execution_id,
            ingestion_ts=_INGESTION_TS,
            base_url="https://ibge.test",
            transport=_body(updated),
            sleep=lambda _seconds: None,
        )
        assert replaced.key == first.key
        reread = client.get_object(Bucket=first.bucket, Key=first.key)
        assert reread["Body"].read() == updated

        second = ingest_ibge_localidades(
            client,
            bucket=bucket,
            execution_id=other_execution_id,
            ingestion_ts=_INGESTION_TS,
            base_url="https://ibge.test",
            transport=_body(original),
            sleep=lambda _seconds: None,
        )
        assert second.key != first.key
        kept = client.get_object(Bucket=first.bucket, Key=first.key)
        assert kept["Body"].read() == updated
        other = client.get_object(Bucket=second.bucket, Key=second.key)
        assert other["Body"].read() == original
    finally:
        for item in (first, second):
            if item is None:
                continue
            client.delete_object(Bucket=item.bucket, Key=item.key)
