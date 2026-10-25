"""Unit tests for the PostgreSQL windowed extract. No database connection."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import ContractField, DataContract
from open_data_platform.ingestion import extract_postgres_oltp as exported_extract
from open_data_platform.ingestion.postgres_oltp import (
    DATASETS,
    SOURCE_ID,
    PostgresOltpError,
    apply_oltp_schema,
    default_fixture_path,
    extract_postgres_oltp,
    main,
    schema_sql,
    seed_synthetic,
    validate_window,
)

_TS = datetime(2026, 10, 25, 15, tzinfo=UTC)
_DAY_START = datetime(2026, 10, 17, tzinfo=UTC)
_DAY_END = datetime(2026, 10, 18, tzinfo=UTC)
_WEEK_END = datetime(2026, 10, 24, tzinfo=UTC)


@dataclass
class _Table:
    columns: list[str]
    rows: list[tuple[object, ...]]


class _RecordingCursor:
    def __init__(self, queries: list[tuple[str, tuple[object, ...]]]) -> None:
        self._queries = queries
        self.description: list[tuple[str]] | None = None

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: Sequence[object] | None = None) -> None:
        self._queries.append((query, tuple(params or ())))

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _RecordingConnection:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self.queries)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _ScriptedCursor:
    def __init__(self, owner: _ScriptedConnection) -> None:
        self._owner = owner
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple[object, ...]] = []

    def __enter__(self) -> _ScriptedCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: Sequence[object] | None = None) -> None:
        self._owner.queries.append((query, tuple(params or ())))
        if not self._owner.steps:
            msg = "unexpected query"
            raise AssertionError(msg)
        step = self._owner.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        self.description = [(name,) for name in step.columns]
        self._rows = list(step.rows)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _ScriptedConnection:
    def __init__(self, steps: list[_Table | Exception]) -> None:
        self.steps = steps
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _ScriptedCursor:
        return _ScriptedCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _client() -> MagicMock:
    client = MagicMock()
    client.head_bucket.return_value = {}
    return client


def _contracts() -> dict[str, DataContract]:
    return {
        item.dataset: item for item in load_contracts() if item.source_id == SOURCE_ID
    }


def _sample(field: ContractField) -> object:
    if field.field_type == "timestamp":
        return datetime(2026, 10, 17, 8, tzinfo=UTC)
    if field.field_type == "integer":
        return 7
    if field.field_type == "number":
        return Decimal("10.50")
    return "value"


def _row(contract: DataContract, **overrides: object) -> tuple[object, ...]:
    values: list[object] = []
    for field in contract.fields:
        if field.name in overrides:
            values.append(overrides[field.name])
        elif field.nullable:
            values.append(None)
        else:
            values.append(_sample(field))
    return tuple(values)


def _table(contract: DataContract, *rows: tuple[object, ...]) -> _Table:
    return _Table([field.name for field in contract.fields], list(rows))


def _extract(
    connection: _ScriptedConnection,
    client: MagicMock | None = None,
    **overrides: Any,
) -> tuple[Any, ...]:
    storage = _client() if client is None else client
    kwargs: dict[str, Any] = {
        "bucket": "odp-raw",
        "execution_id": "run-001",
        "window_start": _DAY_START,
        "window_end": _DAY_END,
        "ingestion_ts": _TS,
        "datasets": ("sellers",),
    }
    kwargs.update(overrides)
    return extract_postgres_oltp(storage, connection, **kwargs)


def _puts(client: MagicMock) -> list[Any]:
    return client.put_object.call_args_list


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    monkeypatch.setenv("POSTGRES_USER", "odp")
    monkeypatch.setenv("POSTGRES_PASSWORD", "odp")
    monkeypatch.setenv("POSTGRES_DB", "odp")


def test_package_exports_extract() -> None:
    assert exported_extract is extract_postgres_oltp


def test_schema_covers_every_contract_column() -> None:
    script = schema_sql()

    assert script.count("updated_at timestamptz NOT NULL") == 4
    for dataset, contract in _contracts().items():
        assert f"CREATE TABLE IF NOT EXISTS oltp.{dataset}" in script
        for field in contract.fields:
            assert field.name in script


def test_apply_schema_executes_the_shipped_script() -> None:
    connection = _RecordingConnection()

    apply_oltp_schema(connection)

    statements = [query for query, _params in connection.queries]
    assert len(statements) == 5
    assert "CREATE SCHEMA IF NOT EXISTS oltp" in statements[0]
    created = [query for query in statements if query.startswith("CREATE TABLE")]
    assert len(created) == 4
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_empty_schema_script_is_rejected() -> None:
    connection = _RecordingConnection()

    with pytest.raises(PostgresOltpError, match="schema script is empty"):
        apply_oltp_schema(connection, script="  ")

    assert connection.queries == []
    assert connection.commits == 0


def test_seed_replaces_rows_with_parameterized_inserts() -> None:
    connection = _RecordingConnection()

    seed_synthetic(connection)
    seed_synthetic(connection)

    first_pass = connection.queries[: 1 + 11]
    truncate, inserts = first_pass[0], first_pass[1:]
    assert truncate[0].startswith("TRUNCATE TABLE ")
    for dataset in DATASETS:
        assert f'"oltp"."{dataset}"' in truncate[0]
    assert all(query.startswith("INSERT INTO ") for query, _params in inserts)
    assert all("%s" in query for query, _params in inserts)
    seller_params = [params for query, params in inserts if '"sellers"' in query]
    assert any(params[0] == "seller-17" for params in seller_params)
    assert all("seller-17" not in query for query, _params in inserts)
    assert connection.commits == 2
    assert connection.queries[12][0].startswith("TRUNCATE TABLE ")


def test_fixture_windows_keep_boundary_rows() -> None:
    payload = json.loads(default_fixture_path().read_text(encoding="utf-8"))

    def selected(start: datetime, end: datetime) -> list[str]:
        found = []
        for row in payload["sellers"]:
            updated_at = datetime.fromisoformat(
                row["updated_at"].replace("Z", "+00:00")
            )
            if start <= updated_at < end:
                found.append(row["seller_id"])
        return found

    assert selected(_DAY_START, _DAY_END) == ["seller-17", "seller-17b"]
    assert selected(_DAY_START, _WEEK_END) == ["seller-17", "seller-17b", "seller-18"]


def test_duplicate_fixture_key_is_rejected(tmp_path: Path) -> None:
    payload = json.loads(default_fixture_path().read_text(encoding="utf-8"))
    payload["sellers"].append(payload["sellers"][0])
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    connection = _RecordingConnection()

    with pytest.raises(PostgresOltpError, match="primary key is duplicated"):
        seed_synthetic(connection, fixture_path=path)

    assert connection.queries == []


def test_daily_and_weekly_windows_share_one_extract() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection(
        [_table(contract, _row(contract)), _table(contract, _row(contract))]
    )
    client = _client()

    _extract(connection, client, window_end=_DAY_END)
    _extract(connection, client, window_end=_WEEK_END)

    assert connection.queries[0][0] == connection.queries[1][0]
    assert connection.queries[0][1] == (_DAY_START, _DAY_END)
    assert connection.queries[1][1] == (_DAY_START, _WEEK_END)
    assert connection.queries[0][0].count("%s") == 2
    assert "2026-10-17" not in connection.queries[0][0]
    assert 'ORDER BY "seller_id"' in connection.queries[0][0]
    keys = [call.kwargs["Key"] for call in _puts(client)]
    assert keys[0] == (
        "raw/postgres_oltp/sellers/dt=2026-10-25/"
        "execution_id=run-001/sellers_2026-10-17_2026-10-18.json"
    )
    assert keys[1].endswith("/sellers_2026-10-17_2026-10-24.json")
    assert keys[0] != keys[1]


def test_offset_midnight_is_normalized_to_utc() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection([_table(contract, _row(contract))])
    client = _client()
    local = timezone(timedelta(hours=3))

    _extract(
        connection,
        client,
        window_start=datetime(2026, 10, 17, 3, tzinfo=local),
        window_end=datetime(2026, 10, 18, 3, tzinfo=local),
    )

    assert connection.queries[0][1] == (_DAY_START, _DAY_END)
    assert (
        _puts(client)[0].kwargs["Key"].endswith("/sellers_2026-10-17_2026-10-18.json")
    )


def test_extract_encodes_contract_types_and_nulls() -> None:
    contract = _contracts()["orders"]
    row = _row(contract)
    connection = _ScriptedConnection([_table(contract, row)])
    client = _client()

    _extract(connection, client, datasets=("orders",))

    stored = json.loads(_puts(client)[0].kwargs["Body"])
    assert list(stored[0]) == [field.name for field in contract.fields]
    assert stored[0]["order_approved_at"] is None
    assert stored[0]["order_purchase_timestamp"] == "2026-10-17T08:00:00Z"
    assert _puts(client)[0].kwargs["ContentType"] == "application/json"
    assert _puts(client)[0].kwargs["Metadata"]["source_id"] == SOURCE_ID
    assert _puts(client)[0].kwargs["Metadata"]["execution_id"] == "run-001"


def test_numbers_and_shifted_timestamps_are_json_values() -> None:
    contract = _contracts()["order_items"]
    shifted = datetime(2026, 10, 17, 11, tzinfo=timezone(timedelta(hours=3)))
    row = _row(
        contract,
        price=Decimal("10.50"),
        freight_value=Decimal("3.25"),
        updated_at=shifted,
    )
    connection = _ScriptedConnection([_table(contract, row)])
    client = _client()

    _extract(connection, client, datasets=("order_items",))

    stored = json.loads(_puts(client)[0].kwargs["Body"])
    assert stored[0]["price"] == 10.5
    assert stored[0]["freight_value"] == 3.25
    assert stored[0]["updated_at"] == "2026-10-17T08:00:00Z"
    assert 'ORDER BY "order_id", "order_item_id"' in connection.queries[0][0]


def test_empty_window_writes_an_empty_array() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection([_table(contract)])
    client = _client()

    _extract(connection, client)

    assert _puts(client)[0].kwargs["Body"] == b"[]\n"


def test_same_execution_overwrites_the_same_key() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection(
        [
            _table(contract, _row(contract)),
            _table(contract, _row(contract, seller_city="recife")),
        ]
    )
    client = _client()

    first = _extract(connection, client)
    second = _extract(connection, client)

    assert first[0].key == second[0].key
    assert json.loads(_puts(client)[1].kwargs["Body"])[0]["seller_city"] == "recife"


def test_datasets_run_in_canonical_order() -> None:
    contracts = _contracts()
    connection = _ScriptedConnection(
        [
            _table(contracts["sellers"], _row(contracts["sellers"])),
            _table(contracts["order_items"], _row(contracts["order_items"])),
        ]
    )

    _extract(connection, datasets=("order_items", "sellers"))

    assert '"sellers"' in connection.queries[0][0]
    assert '"order_items"' in connection.queries[1][0]


def test_null_primary_key_stops_before_the_write() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection([_table(contract, _row(contract, seller_id=None))])
    client = _client()

    with pytest.raises(PostgresOltpError, match="seller_id is null"):
        _extract(connection, client, datasets=DATASETS)

    assert len(connection.queries) == 1
    assert client.put_object.call_count == 0


def test_column_mismatch_keeps_the_dataset_already_written() -> None:
    contracts = _contracts()
    broken = _Table(["customer_id"], [("customer-17",)])
    connection = _ScriptedConnection(
        [
            _table(contracts["sellers"], _row(contracts["sellers"])),
            broken,
            _table(contracts["orders"], _row(contracts["orders"])),
        ]
    )
    client = _client()

    with pytest.raises(PostgresOltpError, match="result columns do not match"):
        _extract(connection, client, datasets=("sellers", "customers", "orders"))

    assert len(connection.queries) == 2
    assert client.put_object.call_count == 1
    assert connection.steps


def test_database_error_stops_the_remaining_datasets() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection(
        [_table(contract, _row(contract)), psycopg.OperationalError("secret-row")]
    )
    client = _client()

    with pytest.raises(PostgresOltpError, match="PostgreSQL extract failed") as caught:
        _extract(connection, client, datasets=("sellers", "customers"))

    assert "secret-row" not in str(caught.value)
    assert client.put_object.call_count == 1
    assert len(connection.queries) == 2
    assert connection.rollbacks == 1
    assert connection.commits == 0


def test_naive_timestamp_value_fails_the_batch() -> None:
    contract = _contracts()["sellers"]
    connection = _ScriptedConnection(
        [_table(contract, _row(contract, updated_at=datetime(2026, 10, 17, 8)))]
    )
    client = _client()

    with pytest.raises(PostgresOltpError, match="timezone"):
        _extract(connection, client)

    assert client.put_object.call_count == 0


def test_boolean_and_non_finite_numbers_are_rejected() -> None:
    contract = _contracts()["sellers"]
    client = _client()
    boolean_row = _ScriptedConnection(
        [_table(contract, _row(contract, seller_zip_code_prefix=True))]
    )

    with pytest.raises(PostgresOltpError, match="unexpected type"):
        _extract(boolean_row, client)

    items = _contracts()["order_items"]
    infinite = _ScriptedConnection([_table(items, _row(items, price=float("nan")))])
    with pytest.raises(PostgresOltpError, match="finite number"):
        _extract(infinite, _client(), datasets=("order_items",))

    assert client.put_object.call_count == 0


def test_unsafe_identifier_never_reaches_the_database() -> None:
    sellers = _contracts()["sellers"]
    bad_fields = tuple(
        replace(field, name="seller_id;drop") if field.name == "seller_id" else field
        for field in sellers.fields
    )
    bad = replace(sellers, fields=bad_fields, primary_key=("seller_id;drop",))
    others = [
        contract for contract in _contracts().values() if contract.dataset != "sellers"
    ]
    connection = _ScriptedConnection([])
    client = _client()

    with pytest.raises(PostgresOltpError, match="invalid SQL identifier"):
        _extract(
            connection,
            client,
            datasets=("sellers",),
            contracts=(bad, *others),
        )

    assert connection.queries == []
    assert client.put_object.call_count == 0


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (datetime(2026, 10, 17), _DAY_END, "timezone"),
        (_DAY_END, _DAY_START, "after window start"),
        (_DAY_START, _DAY_START, "after window start"),
        (
            datetime(2026, 10, 17, 12, tzinfo=UTC),
            _DAY_END,
            "UTC midnight",
        ),
    ],
)
def test_window_bounds_are_rejected(
    start: datetime,
    end: datetime,
    message: str,
) -> None:
    with pytest.raises(PostgresOltpError, match=message):
        validate_window(start, end)


def test_unknown_dataset_is_rejected() -> None:
    connection = _ScriptedConnection([])

    with pytest.raises(PostgresOltpError, match="unknown dataset"):
        _extract(connection, datasets=("sellers;drop",))

    assert connection.queries == []


def test_cli_requires_postgres_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    monkeypatch.delenv("POSTGRES_USER", raising=False)

    assert main(["seed"]) == 1


def test_cli_requires_the_raw_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.setenv("ODP_RAW_BUCKET", "  ")

    code = main(
        [
            "extract",
            "--window-start",
            "2026-10-17T00:00:00Z",
            "--window-end",
            "2026-10-18T00:00:00Z",
            "--execution-id",
            "run-001",
        ]
    )

    assert code == 1


def test_cli_rejects_a_window_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch)

    def explode(_env: dict[str, str]) -> None:
        msg = "connected"
        raise AssertionError(msg)

    monkeypatch.setattr(
        "open_data_platform.ingestion.postgres_oltp.connect_from_env",
        explode,
    )

    code = main(
        [
            "extract",
            "--window-start",
            "2026-10-17T12:00:00Z",
            "--window-end",
            "2026-10-18T00:00:00Z",
            "--execution-id",
            "run-001",
        ]
    )

    assert code == 1


def test_cli_rejects_a_naive_ingestion_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch)

    code = main(
        [
            "extract",
            "--window-start",
            "2026-10-17T00:00:00Z",
            "--window-end",
            "2026-10-18T00:00:00Z",
            "--execution-id",
            "run-001",
            "--ingestion-ts",
            "2026-10-25T15:00:00",
        ]
    )

    assert code == 1


def test_cli_rejects_a_non_integer_port(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.setenv("POSTGRES_PORT", "abc")

    assert main(["seed"]) == 1
