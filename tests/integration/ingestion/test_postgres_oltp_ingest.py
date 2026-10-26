"""MinIO and PostgreSQL round-trip for a windowed OLTP extract."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from open_data_platform.ingestion.postgres_oltp import (
    DATASETS,
    PostgresOltpError,
    apply_oltp_schema,
    connect_from_env,
    default_fixture_path,
    extract_postgres_oltp,
    seed_synthetic,
)
from open_data_platform.object_storage import client_from_env

pytestmark = pytest.mark.integration

_DAY_START = datetime(2026, 10, 17, tzinfo=UTC)
_DAY_END = datetime(2026, 10, 18, tzinfo=UTC)
_WEEK_END = datetime(2026, 10, 24, tzinfo=UTC)
_INGESTION_TS = datetime(2026, 10, 25, 15, tzinfo=UTC)
_PRIMARY_KEYS = {
    "sellers": ("seller_id",),
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
}


def _postgres_or_skip() -> Any:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Missing PostgreSQL settings: {', '.join(missing)}")
    try:
        return connect_from_env(os.environ)
    except PostgresOltpError as exc:
        reason = exc.__cause__ or exc
        pytest.skip(f"PostgreSQL not reachable: {reason}")


def _minio_or_skip() -> tuple[Any, str]:
    required = ("MINIO_ENDPOINT_URL", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Missing MinIO settings: {', '.join(missing)}")
    client = client_from_env(
        {
            "MINIO_ENDPOINT_URL": os.environ["MINIO_ENDPOINT_URL"],
            "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
            "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
            "MINIO_REGION": os.environ.get("MINIO_REGION", "us-east-1"),
        }
    )
    try:
        client.list_buckets()
    except (EndpointConnectionError, ClientError, BotoCoreError, OSError) as exc:
        pytest.skip(f"MinIO not reachable: {exc}")
    return client, os.environ.get("ODP_RAW_BUCKET", "odp-raw")


def _fixture_rows(dataset: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    payload = json.loads(default_fixture_path().read_text(encoding="utf-8"))
    selected = []
    for row in payload[dataset]:
        updated_at = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        if start <= updated_at < end:
            selected.append(row)
    keys = _PRIMARY_KEYS[dataset]
    return sorted(selected, key=lambda item: tuple(item[name] for name in keys))


def _stored_rows(
    client: Any, results: tuple[Any, ...], dataset: str
) -> list[dict[str, Any]]:
    match = next(item for item in results if f"/{dataset}/" in item.key)
    stored = client.get_object(Bucket=match.bucket, Key=match.key)
    rows = json.loads(stored["Body"].read())
    assert stored["Metadata"]["source_id"] == "postgres_oltp"
    assert stored["Metadata"]["execution_id"] == match.metadata.execution_id
    return rows


def _delete_execution(client: Any, bucket: str, execution_id: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for prefix in ("raw/postgres_oltp/", "quarantine/postgres_oltp/"):
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if f"/execution_id={execution_id}/" in key:
                    client.delete_object(Bucket=bucket, Key=key)


def test_windowed_extract_round_trip_overwrites_the_same_object() -> None:
    connection = _postgres_or_skip()
    client = None
    bucket = ""
    execution_a = f"p106-{uuid.uuid4().hex}"
    execution_b = f"p106-{uuid.uuid4().hex}"
    try:
        client, bucket = _minio_or_skip()
        apply_oltp_schema(connection)
        seed_synthetic(connection)
        seed_synthetic(connection)
        daily = extract_postgres_oltp(
            client,
            connection,
            bucket=bucket,
            execution_id=execution_a,
            window_start=_DAY_START,
            window_end=_DAY_END,
            ingestion_ts=_INGESTION_TS,
        )
        weekly = extract_postgres_oltp(
            client,
            connection,
            bucket=bucket,
            execution_id=execution_a,
            window_start=_DAY_START,
            window_end=_WEEK_END,
            ingestion_ts=_INGESTION_TS,
        )
        assert len(daily) == len(DATASETS)
        assert len(weekly) == len(DATASETS)
        assert daily[0].key.endswith(
            f"/dt=2026-10-25/execution_id={execution_a}/"
            "sellers_2026-10-17_2026-10-18.json"
        )
        assert weekly[0].key.endswith("/sellers_2026-10-17_2026-10-24.json")
        for dataset in DATASETS:
            assert _stored_rows(client, daily, dataset) == _fixture_rows(
                dataset, _DAY_START, _DAY_END
            )
            assert _stored_rows(client, weekly, dataset) == _fixture_rows(
                dataset, _DAY_START, _WEEK_END
            )

        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE oltp.sellers SET seller_city = %s WHERE seller_id = %s",
                ("recife", "seller-17"),
            )
        connection.commit()
        rewritten = extract_postgres_oltp(
            client,
            connection,
            bucket=bucket,
            execution_id=execution_a,
            window_start=_DAY_START,
            window_end=_DAY_END,
            ingestion_ts=_INGESTION_TS,
        )
        assert rewritten[0].key == daily[0].key
        sellers = _stored_rows(client, rewritten, "sellers")
        assert sellers[0]["seller_city"] == "recife"
        assert sellers[1]["seller_city"] == "campinas"

        separate = extract_postgres_oltp(
            client,
            connection,
            bucket=bucket,
            execution_id=execution_b,
            window_start=_DAY_START,
            window_end=_DAY_END,
            ingestion_ts=_INGESTION_TS,
        )
        assert separate[0].key != rewritten[0].key
        assert execution_b in separate[0].key
    finally:
        if client is not None:
            _delete_execution(client, bucket, execution_a)
            _delete_execution(client, bucket, execution_b)
            seed_synthetic(connection)
        connection.close()
