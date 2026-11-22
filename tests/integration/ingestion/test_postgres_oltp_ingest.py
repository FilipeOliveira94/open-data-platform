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
            # An explicit updated_at keeps the row in the window; without it
            # the P3-16 trigger moves the row to the time of the UPDATE.
            cursor.execute(
                "UPDATE oltp.sellers SET seller_city = %s, updated_at = %s "
                "WHERE seller_id = %s",
                ("recife", _DAY_START + timedelta(seconds=1), "seller-17"),
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
            as_of=_DAY_END,
            cadence=Cadence(_DAY_START, timedelta(days=1)),
        )
        assert report.clean
        assert report.expected_windows == 1
        assert report.rerun_windows == ((_DAY_START, _DAY_END),)
    finally:
        connection.close()


def _updated_at(connection: psycopg.Connection, seller_id: str) -> datetime:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT updated_at FROM oltp.sellers WHERE seller_id = %s", (seller_id,)
        )
        row = cursor.fetchone()
    connection.commit()
    assert row is not None
    return row[0]


def test_trigger_maintains_updated_at_when_an_update_omits_it(
    scratch_env: dict[str, str],
) -> None:
    """P3-16 (review R8): the database, not the writer, keeps the watermark."""
    try:
        connection = connect_from_env(scratch_env)
    except PostgresOltpError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    try:
        apply_oltp_schema(connection)
        apply_oltp_schema(connection)  # idempotent: CREATE OR REPLACE
        seed_synthetic(connection)
        seeded = _updated_at(connection, "seller-17")
        assert seeded == _DAY_START  # INSERT keeps the fixture value

        with connection.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            before = cursor.fetchone()[0]
            cursor.execute(
                "UPDATE oltp.sellers SET seller_city = %s WHERE seller_id = %s",
                ("olinda", "seller-17"),
            )
        connection.commit()
        bumped = _updated_at(connection, "seller-17")
        assert bumped >= before > seeded

        explicit = _DAY_START + timedelta(hours=2)
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE oltp.sellers SET seller_city = %s, updated_at = %s "
                "WHERE seller_id = %s",
                ("recife", explicit, "seller-17"),
            )
        connection.commit()
        assert _updated_at(connection, "seller-17") == explicit
    finally:
        connection.close()


class _CommitAfterOrders:
    """Connection proxy: once ``orders`` is read, another session commits."""

    def __init__(self, inner: psycopg.Connection, side_effect: Callable[[], None]):
        self._inner = inner
        self._side_effect = side_effect
        self.fired = False

    def cursor(self) -> _CommitAfterOrdersCursor:
        return _CommitAfterOrdersCursor(self._inner.cursor(), self)

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()


class _CommitAfterOrdersCursor:
    def __init__(self, inner: Any, owner: _CommitAfterOrders) -> None:
        self._inner = inner
        self._owner = owner

    @property
    def description(self) -> Any:
        return self._inner.description

    def __enter__(self) -> _CommitAfterOrdersCursor:
        self._inner.__enter__()
        return self

    def __exit__(self, *exc: object) -> object:
        return self._inner.__exit__(*exc)

    def execute(self, query: str, params: Any = None) -> object:
        result = self._inner.execute(query, params)
        if '"oltp"."orders"' in query and not self._owner.fired:
            self._owner.fired = True
            self._owner._side_effect()
        return result

    def fetchall(self) -> Any:
        return self._inner.fetchall()


def test_one_window_is_read_from_one_snapshot(
    scratch_env: dict[str, str],
    test_raw_bucket: str,
    purge_executions: Callable[[Any, str, str], None],
) -> None:
    """P3-16 (review R13): a commit between two reads does not split a window."""
    try:
        connection = connect_from_env(scratch_env)
        writer = connect_from_env(scratch_env)
    except PostgresOltpError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    execution = f"p316-{uuid.uuid4().hex}"
    try:
        client = _minio_or_skip()
        purge_executions(client, test_raw_bucket, execution)
        apply_oltp_schema(connection)
        seed_synthetic(connection)
        in_window = _DAY_START + timedelta(hours=12)

        def concurrent_write() -> None:
            with writer.cursor() as cursor:
                cursor.execute(
                    "UPDATE oltp.order_items SET price = price + 1, updated_at = %s "
                    "WHERE order_id = %s",
                    (in_window, "order-17"),
                )
            writer.commit()

        proxy = _CommitAfterOrders(connection, concurrent_write)
        results = extract_postgres_oltp(
            client,
            proxy,  # type: ignore[arg-type]
            bucket=test_raw_bucket,
            execution_id=execution,
            window_start=_DAY_START,
            window_end=_DAY_END,
            ingestion_ts=_INGESTION_TS,
            datasets=("orders", "order_items"),
        )
        assert proxy.fired
        items = _stored_rows(client, results, "order_items")
        expected = _fixture_rows("order_items", _DAY_START, _DAY_END)
        # The extract saw order_items as of the orders read, not the commit.
        assert [row["price"] for row in items] == [row["price"] for row in expected]
        with writer.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM oltp.order_items WHERE updated_at = %s",
                (in_window,),
            )
            assert cursor.fetchone()[0] == 1
        writer.commit()
    finally:
        writer.close()
        connection.close()


def test_extract_fails_when_a_table_lost_its_trigger(
    scratch_env: dict[str, str],
    test_raw_bucket: str,
) -> None:
    try:
        connection = connect_from_env(scratch_env)
    except PostgresOltpError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    try:
        client = _minio_or_skip()
        apply_oltp_schema(connection)
        seed_synthetic(connection)
        with connection.cursor() as cursor:
            cursor.execute("DROP TRIGGER set_updated_at ON oltp.orders")
        connection.commit()
        with pytest.raises(PostgresOltpError, match="without the set_updated_at"):
            extract_postgres_oltp(
                client,
                connection,
                bucket=test_raw_bucket,
                execution_id=f"p316-{uuid.uuid4().hex}",
                window_start=_DAY_START,
                window_end=_DAY_END,
                ingestion_ts=_INGESTION_TS,
            )
    finally:
        connection.close()
