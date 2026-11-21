"""MinIO and PostgreSQL round-trip for a windowed OLTP extract.

The test seeds a temporary database, never ``POSTGRES_DB``: the seed
truncates the ``oltp`` tables. The coverage ledger lives in the same
temporary database. Raw objects go to the integration test bucket.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from psycopg import sql

from open_data_platform.ingestion.coverage import (
    DAILY,
    Cadence,
    PostgresCoverageLedger,
    coverage_report,
)
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


def _admin(env: dict[str, str]) -> psycopg.Connection:
    return psycopg.connect(
        host=env.get("POSTGRES_HOST", "").strip() or "localhost",
        port=int(env.get("POSTGRES_PORT", "").strip() or "5432"),
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        dbname="postgres",
        autocommit=True,
        connect_timeout=5,
    )


@pytest.fixture
def scratch_env() -> Iterator[dict[str, str]]:
    """Environment whose ``POSTGRES_DB`` and catalog are a temporary database."""
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Missing PostgreSQL settings: {', '.join(missing)}")
    env = dict(os.environ)
    name = f"odp_it_{uuid.uuid4().hex[:12]}"
    try:
        admin = _admin(env)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            yield {**env, "POSTGRES_DB": name, "ICEBERG_CATALOG_DB": name}
        finally:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


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
    return client


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


def test_windowed_extract_round_trip_overwrites_the_same_object(
    scratch_env: dict[str, str],
    test_raw_bucket: str,
    purge_executions: Callable[[Any, str, str], None],
) -> None:
    try:
        connection = connect_from_env(scratch_env)
    except PostgresOltpError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    ledger = PostgresCoverageLedger(scratch_env)
    bucket = test_raw_bucket
    execution_a = f"p106-{uuid.uuid4().hex}"
    execution_b = f"p106-{uuid.uuid4().hex}"
    try:
        client = _minio_or_skip()
        purge_executions(client, bucket, execution_a)
        purge_executions(client, bucket, execution_b)
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
            coverage=ledger,
            frequency=DAILY,
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
            coverage=ledger,
            frequency=DAILY,
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
            coverage=ledger,
            frequency=DAILY,
        )
        assert separate[0].key != rewritten[0].key
        assert execution_b in separate[0].key

        # Coverage ledger (P3-06): a re-run of one execution overwrites its
        # rows; a second execution of the same window is a re-run, not a gap.
        recorded = ledger.load("postgres_oltp", DAILY)
        assert sorted((item.execution_id, item.dataset) for item in recorded) == sorted(
            (execution, dataset)
            for execution in (execution_a, execution_b)
            for dataset in DATASETS
        )
        for item in recorded:
            assert (item.window_start, item.window_end) == (_DAY_START, _DAY_END)
            assert item.row_count == len(
                _fixture_rows(item.dataset, _DAY_START, _DAY_END)
            )
        report = coverage_report(
            DAILY,
            recorded,
            datasets=DATASETS,
            cadence=Cadence(_DAY_START, timedelta(days=1)),
        )
        assert report.clean
        assert report.expected_windows == 1
        assert report.rerun_windows == ((_DAY_START, _DAY_END),)
    finally:
        connection.close()
