"""Extract a half-open ``updated_at`` window from the local OLTP schema.

One function serves every cadence. A daily run passes a one-day window and a
weekly run passes a seven-day window. Both bounds are timezone-aware and must
fall on UTC midnight, so the raw object name stays unique:

``{dataset}_{start_date}_{end_date}.json``

The predicate is ``updated_at >= start AND updated_at < end``. An empty
window is stored as a JSON array. Scheduling, catchup, and quarantine stay
outside this module. The synthetic seed replaces the four ``oltp`` tables.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import psycopg

from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion.paths import build_raw_object_key
from open_data_platform.ingestion.writer import RawWriteResult, write_raw_object
from open_data_platform.object_storage import client_from_env

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import ContractField, DataContract

SOURCE_ID = "postgres_oltp"
SCHEMA = "oltp"
WATERMARK = "updated_at"
DATASETS: tuple[str, ...] = ("sellers", "customers", "orders", "order_items")
_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_FIXTURE_NAME = "synthetic.json"


class PostgresOltpError(ValueError):
    """A PostgreSQL window could not be seeded or landed."""


class DbCursor(Protocol):
    """The cursor methods the extract and seed paths call."""

    description: Sequence[object] | None

    def execute(
        self,
        query: str,
        params: Sequence[object] | None = None,
    ) -> object: ...

    def fetchall(self) -> Sequence[Sequence[object]]: ...

    def __enter__(self) -> DbCursor: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> object: ...


class DbConnection(Protocol):
    """The connection methods the extract and seed paths call."""

    def cursor(self) -> DbCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


def validate_window(
    window_start: datetime, window_end: datetime
) -> tuple[datetime, datetime]:
    """Return UTC bounds for a half-open midnight window."""
    if window_start.tzinfo is None or window_end.tzinfo is None:
        msg = "window bounds must include a timezone"
        raise PostgresOltpError(msg)
    start = window_start.astimezone(UTC)
    end = window_end.astimezone(UTC)
    if end <= start:
        msg = "window end must be after window start"
        raise PostgresOltpError(msg)
    if not _is_utc_midnight(start) or not _is_utc_midnight(end):
        msg = "window bounds must fall on UTC midnight"
        raise PostgresOltpError(msg)
    return start, end


def window_object_name(
    dataset: str,
    window_start: datetime,
    window_end: datetime,
) -> str:
    """Return the raw object name for one dataset window."""
    start, end = validate_window(window_start, window_end)
    if dataset not in DATASETS:
        msg = f"unknown dataset: {dataset}"
        raise PostgresOltpError(msg)
    return f"{dataset}_{start.date().isoformat()}_{end.date().isoformat()}.json"


def schema_sql() -> str:
    """Return the OLTP DDL shipped next to this module."""
    path = Path(__file__).resolve().parent / "schema.sql"
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = "OLTP schema script could not be read"
        raise PostgresOltpError(msg) from exc


def default_fixture_path() -> Path:
    """Find the synthetic fixture by walking parents of this module.

    The fixture stays a repository file under ``tests/fixtures``. It is not
    packaged into the wheel, matching the contract loader.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "tests" / "fixtures" / "postgres_oltp" / _FIXTURE_NAME
        if candidate.is_file():
            return candidate
    msg = "synthetic PostgreSQL fixture was not found"
    raise PostgresOltpError(msg)


def apply_oltp_schema(
    connection: DbConnection,
    *,
    script: str | None = None,
) -> None:
    """Create the ``oltp`` schema and tables when they are missing."""
    statements = _statements(schema_sql() if script is None else script)
    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
    except psycopg.Error as exc:
        _rollback(connection)
        msg = "PostgreSQL schema apply failed"
        raise PostgresOltpError(msg) from exc


def seed_synthetic(
    connection: DbConnection,
    *,
    fixture_path: Path | None = None,
    contracts: tuple[DataContract, ...] | None = None,
) -> None:
    """Replace the four OLTP tables with the synthetic fixture.

    The load runs in one transaction: a failed insert rolls the truncate back.
    """
    path = default_fixture_path() if fixture_path is None else fixture_path
    loaded = _load_fixture(path, contracts)
    try:
        by_dataset = _contracts_by_dataset(contracts)
        with connection.cursor() as cursor:
            cursor.execute(_truncate_sql())
            for dataset in DATASETS:
                statement = _insert_sql(by_dataset[dataset])
                for row in loaded[dataset]:
                    cursor.execute(statement, row)
        connection.commit()
    except psycopg.Error as exc:
        _rollback(connection)
        msg = "PostgreSQL seed failed"
        raise PostgresOltpError(msg) from exc


def extract_postgres_oltp(
    client: BaseClient,
    connection: DbConnection,
    *,
    bucket: str,
    execution_id: str,
    window_start: datetime,
    window_end: datetime,
    ingestion_ts: datetime | None = None,
    datasets: Sequence[str] | None = None,
    contracts: tuple[DataContract, ...] | None = None,
) -> tuple[RawWriteResult, ...]:
    """Extract each selected dataset for ``[window_start, window_end)``.

    The same ``execution_id``, ingestion date, and window overwrite the same
    keys. A structural failure stops the remaining datasets. Objects already
    written for this execution stay in place.
    """
    _require_token("bucket", bucket)
    _require_execution_id(execution_id)
    start, end = validate_window(window_start, window_end)
    selected = _resolve_datasets(datasets)
    written_at = _require_ingestion_ts(ingestion_ts)
    by_dataset = _contracts_by_dataset(contracts)
    results: list[RawWriteResult] = []
    for dataset in selected:
        contract = by_dataset[dataset]
        _require_extract_contract(contract)
        columns, rows = _fetch_window(connection, contract, start, end)
        body = _encode_rows(dataset, contract, columns, rows)
        try:
            results.append(
                write_raw_object(
                    client,
                    bucket=bucket,
                    body=body,
                    source_id=SOURCE_ID,
                    dataset=dataset,
                    execution_id=execution_id,
                    object_name=window_object_name(dataset, start, end),
                    ingestion_ts=written_at,
                    content_type="application/json",
                )
            )
        except ValueError as exc:
            raise PostgresOltpError(str(exc)) from exc
    return tuple(results)


def connect_from_env(env: Mapping[str, str]) -> psycopg.Connection:
    """Open a PostgreSQL connection from ``POSTGRES_*`` settings."""
    settings = _postgres_settings(env)
    try:
        return psycopg.connect(**settings, connect_timeout=5)
    except psycopg.Error as exc:
        msg = "PostgreSQL connection failed"
        raise PostgresOltpError(msg) from exc


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Settings and the window are checked before connecting."""
    parser = argparse.ArgumentParser(
        description="Seed or extract the local PostgreSQL OLTP window.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "seed",
        help="Replace the oltp tables with the synthetic fixture.",
    )
    extract = commands.add_parser(
        "extract",
        help="Write one JSON array per dataset for a half-open window.",
    )
    extract.add_argument("--window-start", required=True)
    extract.add_argument("--window-end", required=True)
    extract.add_argument("--execution-id", required=True)
    extract.add_argument(
        "--ingestion-ts",
        default=None,
        help="UTC ISO-8601 timestamp. The same value overwrites the same keys.",
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "seed":
            _run_seed()
        else:
            _run_extract(
                window_start=args.window_start,
                window_end=args.window_end,
                execution_id=args.execution_id,
                ingestion_ts=args.ingestion_ts,
            )
    except (PostgresOltpError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_seed() -> None:
    env = os.environ
    _postgres_settings(env)
    connection = connect_from_env(env)
    try:
        apply_oltp_schema(connection)
        seed_synthetic(connection)
    finally:
        connection.close()


def _run_extract(
    *,
    window_start: str,
    window_end: str,
    execution_id: str,
    ingestion_ts: str | None,
) -> None:
    env = os.environ
    bucket = env.get("ODP_RAW_BUCKET", "").strip()
    if not bucket:
        msg = "ODP_RAW_BUCKET is required"
        raise PostgresOltpError(msg)
    _postgres_settings(env)
    start = _parse_instant(window_start, "window bound")
    end = _parse_instant(window_end, "window bound")
    validate_window(start, end)
    _require_execution_id(execution_id)
    parsed_ts = _parse_instant(ingestion_ts, "ingestion-ts") if ingestion_ts else None
    connection = connect_from_env(env)
    try:
        client = client_from_env(env)
        extract_postgres_oltp(
            client,
            connection,
            bucket=bucket,
            execution_id=execution_id,
            window_start=start,
            window_end=end,
            ingestion_ts=parsed_ts,
        )
    finally:
        connection.close()


def _parse_instant(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{label} must be ISO-8601"
        raise PostgresOltpError(msg) from exc
    if parsed.tzinfo is None:
        msg = f"{label} must include a timezone"
        raise PostgresOltpError(msg)
    return parsed


def _postgres_settings(env: Mapping[str, str]) -> dict[str, object]:
    missing = [
        name
        for name in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
        if not env.get(name, "").strip()
    ]
    if missing:
        names = ", ".join(missing)
        msg = f"PostgreSQL settings are required: {names}"
        raise PostgresOltpError(msg)
    port_text = env.get("POSTGRES_PORT", "").strip() or "5432"
    try:
        port = int(port_text)
    except ValueError as exc:
        msg = "POSTGRES_PORT must be an integer"
        raise PostgresOltpError(msg) from exc
    if port < 1:
        msg = "POSTGRES_PORT must be an integer"
        raise PostgresOltpError(msg)
    host = env.get("POSTGRES_HOST", "").strip() or "localhost"
    return {
        "host": host,
        "port": port,
        "user": env["POSTGRES_USER"].strip(),
        "password": env["POSTGRES_PASSWORD"],
        "dbname": env["POSTGRES_DB"].strip(),
    }


def _contracts_by_dataset(
    contracts: tuple[DataContract, ...] | None,
) -> dict[str, DataContract]:
    loaded = load_contracts() if contracts is None else contracts
    found: dict[str, DataContract] = {}
    for contract in loaded:
        if contract.source_id != SOURCE_ID or contract.dataset not in DATASETS:
            continue
        if contract.dataset in found:
            msg = f"duplicate contract for {SOURCE_ID}.{contract.dataset}"
            raise PostgresOltpError(msg)
        found[contract.dataset] = contract
    missing = [name for name in DATASETS if name not in found]
    if missing:
        names = ", ".join(missing)
        msg = f"missing contract for {SOURCE_ID}: {names}"
        raise PostgresOltpError(msg)
    return found


def _require_extract_contract(contract: DataContract) -> None:
    if contract.payload_format != "json":
        msg = f"{contract.dataset}: contract payload must be JSON"
        raise PostgresOltpError(msg)
    if contract.update_pattern != "incremental_window":
        msg = f"{contract.dataset}: contract must use an incremental window"
        raise PostgresOltpError(msg)
    if not contract.primary_key:
        msg = f"{contract.dataset}: contract has no primary key"
        raise PostgresOltpError(msg)
    watermark = next(
        (field for field in contract.fields if field.name == WATERMARK),
        None,
    )
    if watermark is None or watermark.nullable or watermark.field_type != "timestamp":
        msg = f"{contract.dataset}: contract is missing a non-null updated_at timestamp"
        raise PostgresOltpError(msg)


def _resolve_datasets(datasets: Sequence[str] | None) -> tuple[str, ...]:
    if datasets is None:
        return DATASETS
    if isinstance(datasets, str) or not isinstance(datasets, Sequence):
        msg = "datasets must be a sequence of names"
        raise PostgresOltpError(msg)
    unknown = [name for name in datasets if name not in DATASETS]
    if unknown:
        names = ", ".join(unknown)
        msg = f"unknown dataset: {names}"
        raise PostgresOltpError(msg)
    if not datasets:
        msg = "at least one dataset is required"
        raise PostgresOltpError(msg)
    selected = set(datasets)
    return tuple(name for name in DATASETS if name in selected)


def _fetch_window(
    connection: DbConnection,
    contract: DataContract,
    window_start: datetime,
    window_end: datetime,
) -> tuple[list[str], list[Sequence[object]]]:
    query = _select_sql(contract)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (window_start, window_end))
            description = cursor.description
            rows = cursor.fetchall()
    except psycopg.Error as exc:
        _rollback(connection)
        msg = "PostgreSQL extract failed"
        raise PostgresOltpError(msg) from exc
    if not description:
        msg = f"{contract.dataset}: query result is missing column names"
        raise PostgresOltpError(msg)
    return [_column_name(column) for column in description], list(rows)


def _encode_rows(
    dataset: str,
    contract: DataContract,
    columns: Sequence[str],
    rows: Sequence[Sequence[object]],
) -> bytes:
    expected = [field.name for field in contract.fields]
    if list(columns) != expected:
        msg = f"{dataset}: result columns do not match the contract"
        raise PostgresOltpError(msg)
    payload = [_encode_row(dataset, contract, row) for row in rows]
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        msg = f"{dataset}: row could not be encoded as JSON"
        raise PostgresOltpError(msg) from exc
    return encoded.encode("utf-8") + b"\n"


def _encode_row(
    dataset: str,
    contract: DataContract,
    row: Sequence[object],
) -> dict[str, object]:
    if not isinstance(row, tuple | list) or len(row) != len(contract.fields):
        msg = f"{dataset}: query row does not match the contract"
        raise PostgresOltpError(msg)
    encoded: dict[str, object] = {}
    for field, value in zip(contract.fields, row, strict=True):
        encoded[field.name] = _json_value(dataset, field, value)
    return encoded


def _json_value(dataset: str, field: ContractField, value: object) -> object:
    if value is None:
        if field.nullable:
            return None
        msg = f"{dataset}: field {field.name} is null"
        raise PostgresOltpError(msg)
    if isinstance(value, bool) or not _matches_type(field, value):
        msg = f"{dataset}: field {field.name} has an unexpected type"
        raise PostgresOltpError(msg)
    if field.field_type == "timestamp":
        if not isinstance(value, datetime):
            msg = f"{dataset}: field {field.name} has an unexpected type"
            raise PostgresOltpError(msg)
        return _format_timestamp(dataset, field.name, value)
    if field.field_type == "number":
        return _json_number(dataset, field.name, value)
    return value


def _matches_type(field: ContractField, value: object) -> bool:
    if field.field_type == "string":
        return isinstance(value, str)
    if field.field_type == "integer":
        return isinstance(value, int)
    if field.field_type == "number":
        return isinstance(value, int | float | Decimal)
    if field.field_type == "timestamp":
        return isinstance(value, datetime)
    return False


def _format_timestamp(dataset: str, field_name: str, value: datetime) -> str:
    if value.tzinfo is None:
        msg = f"{dataset}: field {field_name} must include a timezone"
        raise PostgresOltpError(msg)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _json_number(dataset: str, field_name: str, value: object) -> int | float:
    if isinstance(value, bool):
        msg = f"{dataset}: field {field_name} has an unexpected type"
        raise PostgresOltpError(msg)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not _finite_float(value):
            msg = f"{dataset}: field {field_name} must be a finite number"
            raise PostgresOltpError(msg)
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            msg = f"{dataset}: field {field_name} must be a finite number"
            raise PostgresOltpError(msg)
        rendered = float(value)
        if not _finite_float(rendered):
            msg = f"{dataset}: field {field_name} must be a finite number"
            raise PostgresOltpError(msg)
        return rendered
    msg = f"{dataset}: field {field_name} has an unexpected type"
    raise PostgresOltpError(msg)


def _finite_float(value: float) -> bool:
    return value == value and value not in {float("inf"), float("-inf")}


def _load_fixture(
    path: Path,
    contracts: tuple[DataContract, ...] | None,
) -> dict[str, list[tuple[object, ...]]]:
    if not path.is_file():
        msg = "synthetic fixture does not exist"
        raise PostgresOltpError(msg)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        msg = "synthetic fixture is not valid JSON"
        raise PostgresOltpError(msg) from exc
    if not isinstance(payload, dict):
        msg = "synthetic fixture must be a JSON object"
        raise PostgresOltpError(msg)
    if set(payload) != set(DATASETS):
        msg = "synthetic fixture datasets do not match the OLTP tables"
        raise PostgresOltpError(msg)
    by_dataset = _contracts_by_dataset(contracts)
    loaded: dict[str, list[tuple[object, ...]]] = {}
    for dataset in DATASETS:
        rows = payload[dataset]
        if not isinstance(rows, list):
            msg = f"{dataset}: fixture rows must be a list"
            raise PostgresOltpError(msg)
        contract = by_dataset[dataset]
        _require_extract_contract(contract)
        parsed = [_fixture_row(dataset, contract, item) for item in rows]
        _require_unique_keys(dataset, contract, parsed)
        loaded[dataset] = parsed
    return loaded


def _fixture_row(
    dataset: str,
    contract: DataContract,
    item: object,
) -> tuple[object, ...]:
    if not isinstance(item, dict):
        msg = f"{dataset}: fixture row must be an object"
        raise PostgresOltpError(msg)
    expected = {field.name for field in contract.fields}
    if set(item) != expected:
        msg = f"{dataset}: fixture row fields do not match the contract"
        raise PostgresOltpError(msg)
    return tuple(
        _fixture_value(dataset, field, item[field.name]) for field in contract.fields
    )


def _fixture_value(dataset: str, field: ContractField, value: object) -> object:
    if value is None:
        if field.nullable:
            return None
        msg = f"{dataset}: field {field.name} is null"
        raise PostgresOltpError(msg)
    if field.field_type == "timestamp":
        if not isinstance(value, str):
            msg = f"{dataset}: field {field.name} has an unexpected type"
            raise PostgresOltpError(msg)
        return _parse_instant(value, f"{dataset}.{field.name}")
    if field.field_type == "string":
        if not isinstance(value, str):
            msg = f"{dataset}: field {field.name} has an unexpected type"
            raise PostgresOltpError(msg)
        return value
    if field.field_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            msg = f"{dataset}: field {field.name} has an unexpected type"
            raise PostgresOltpError(msg)
        return value
    if field.field_type == "number":
        if isinstance(value, bool) or not isinstance(value, int | Decimal):
            msg = f"{dataset}: field {field.name} has an unexpected type"
            raise PostgresOltpError(msg)
        return value
    msg = f"{dataset}: field {field.name} has an unexpected type"
    raise PostgresOltpError(msg)


def _require_unique_keys(
    dataset: str,
    contract: DataContract,
    rows: Sequence[tuple[object, ...]],
) -> None:
    positions = [
        next(index for index, field in enumerate(contract.fields) if field.name == name)
        for name in contract.primary_key
    ]
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        key = tuple(row[index] for index in positions)
        if key in seen:
            msg = f"{dataset}: fixture primary key is duplicated"
            raise PostgresOltpError(msg)
        seen.add(key)


def _select_sql(contract: DataContract) -> str:
    columns = ", ".join(_quote_ident(field.name) for field in contract.fields)
    order_by = ", ".join(_quote_ident(name) for name in contract.primary_key)
    return (
        f"SELECT {columns} FROM {_table(contract.dataset)} "
        f"WHERE {_quote_ident(WATERMARK)} >= %s AND {_quote_ident(WATERMARK)} < %s "
        f"ORDER BY {order_by}"
    )


def _insert_sql(contract: DataContract) -> str:
    columns = ", ".join(_quote_ident(field.name) for field in contract.fields)
    placeholders = ", ".join(["%s"] * len(contract.fields))
    return f"INSERT INTO {_table(contract.dataset)} ({columns}) VALUES ({placeholders})"


def _truncate_sql() -> str:
    tables = ", ".join(_table(name) for name in DATASETS)
    return f"TRUNCATE TABLE {tables}"


def _table(dataset: str) -> str:
    if dataset not in DATASETS:
        msg = f"unknown dataset: {dataset}"
        raise PostgresOltpError(msg)
    return f"{_quote_ident(SCHEMA)}.{_quote_ident(dataset)}"


def _statements(script: str) -> tuple[str, ...]:
    statements = tuple(part.strip() for part in script.split(";") if part.strip())
    if not statements:
        msg = "schema script is empty"
        raise PostgresOltpError(msg)
    return statements


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        msg = f"invalid SQL identifier: {name!r}"
        raise PostgresOltpError(msg)
    return f'"{name}"'


def _column_name(column: object) -> str:
    name = getattr(column, "name", None)
    if isinstance(name, str):
        return name
    if isinstance(column, tuple) and column and isinstance(column[0], str):
        return column[0]
    msg = "query result is missing column names"
    raise PostgresOltpError(msg)


def _is_utc_midnight(value: datetime) -> bool:
    return (
        value.hour == 0
        and value.minute == 0
        and value.second == 0
        and value.microsecond == 0
    )


def _require_token(name: str, value: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        msg = f"{name} must be a non-empty token"
        raise PostgresOltpError(msg)
    return value


def _require_execution_id(execution_id: str) -> str:
    _require_token("execution_id", execution_id)
    try:
        build_raw_object_key(
            source_id=SOURCE_ID,
            dataset=DATASETS[0],
            execution_id=execution_id,
            object_name="sellers_2026-10-17_2026-10-18.json",
            ingestion_date=datetime.now(UTC).date(),
        )
    except ValueError as exc:
        msg = "execution_id has invalid characters"
        raise PostgresOltpError(msg) from exc
    return execution_id


def _require_ingestion_ts(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        msg = "ingestion_ts must include a timezone"
        raise PostgresOltpError(msg)
    return value.astimezone(UTC)


def _rollback(connection: DbConnection) -> None:
    try:
        connection.rollback()
    except psycopg.Error:
        return
