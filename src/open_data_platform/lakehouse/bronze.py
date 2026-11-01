"""Promote raw objects into Bronze staging rows and Spark SQL.

Bronze keeps one string column per contract field, plus ingestion metadata.
It does not cast logical types, drop invalid rows, or deduplicate keys.
Spark, invoked separately, loads the staging file into the JDBC catalog.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.records import (
    PayloadStructureError,
    load_jsonl_records,
)
from open_data_platform.object_storage import client_from_env, ensure_bucket

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import ContractField, DataContract

METADATA_COLUMNS: tuple[str, ...] = (
    "source_id",
    "execution_id",
    "_ingestion_ts",
    "raw_object_key",
    "record_index",
    "dt",
)

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RAW_KEY_RE = re.compile(
    r"^raw/"
    r"(?P<source_id>[a-z0-9][a-z0-9_-]*)/"
    r"(?P<dataset>[a-z0-9][a-z0-9_-]*)/"
    r"dt=(?P<dt>\d{4}-\d{2}-\d{2})/"
    r"execution_id=(?P<execution_id>[A-Za-z0-9][A-Za-z0-9._:+-]*)/"
    r"(?P<object_name>[a-zA-Z0-9][a-zA-Z0-9._-]*)$"
)


class BronzeError(ValueError):
    """Bronze promotion cannot read raw objects or build the Spark script."""


class BronzeParseError(BronzeError):
    """One raw object does not match the contract shape. The table is unchanged."""


@dataclass(frozen=True, slots=True)
class RawObject:
    """Bytes and user-metadata for one raw object."""

    key: str
    body: bytes
    metadata: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class _RawKey:
    source_id: str
    dataset: str
    execution_id: str
    dt: str


@dataclass(frozen=True, slots=True)
class BronzePromotion:
    """Staging uploads and one Spark script per dataset."""

    sql: str
    scripts: tuple[tuple[str, str], ...]
    counts: tuple[tuple[str, int], ...]


def require_bucket_name(bucket: str) -> str:
    """Reject bucket names that cannot be embedded in an ``s3a://`` path."""
    if _BUCKET_RE.fullmatch(bucket) is None:
        msg = f"bucket name is not a simple S3 identifier: {bucket!r}"
        raise BronzeError(msg)
    return bucket


def bronze_table_name(source_id: str, dataset: str) -> str:
    """Iceberg table name for one contract. The namespace is ``bronze``."""
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    return f"{source_id}__{dataset}"


def bronze_columns(contract: DataContract) -> tuple[str, ...]:
    """Metadata columns, then contract fields, then page envelope fields.

    The ``record_locator`` array is exploded and is not itself a column.
    """
    names = list(METADATA_COLUMNS)
    seen = set(names)
    for name in _payload_column_names(contract):
        if name in seen:
            msg = f"{contract.source_id}.{contract.dataset}: duplicate column {name}"
            raise BronzeError(msg)
        seen.add(name)
        names.append(name)
    return tuple(names)


def staging_object_key(source_id: str, dataset: str) -> str:
    """Warehouse key of the JSONL file Spark reads for one dataset."""
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    return f"bronze/_staging/{source_id}/{dataset}/rows.jsonl"


def parse_payload(
    contract: DataContract,
    body: bytes,
) -> tuple[dict[str, str | None], ...]:
    """Return one string row per source record. Structural mismatches raise.

    Empty cells, JSON nulls, and omitted nullable keys become ``None``.
    A value with the wrong logical type is kept as text. Nested JSON stays
    compact JSON text. Duplicate keys are not removed.
    """
    if contract.payload_format == "csv":
        _reject_page_shape(contract)
        return _parse_csv(contract, body)
    if contract.payload_format == "jsonl":
        _reject_page_shape(contract)
        return _parse_jsonl(contract, body)
    if contract.payload_format == "json":
        return _parse_json(contract, body)
    msg = f"unsupported payload format: {contract.payload_format}"
    raise BronzeParseError(msg)


def rows_from_object(
    contract: DataContract,
    raw: RawObject,
) -> tuple[dict[str, str | None], ...]:
    """Attach ingestion metadata from the object key and user-metadata."""
    parsed = _parse_key(raw.key)
    if parsed.source_id != contract.source_id or parsed.dataset != contract.dataset:
        msg = f"{raw.key}: key does not match the contract"
        raise BronzeParseError(msg)
    metadata = _metadata_strings(raw.metadata)
    source_id = metadata.get("source_id")
    execution_id = metadata.get("execution_id")
    ingestion_ts = metadata.get("_ingestion_ts")
    if source_id != contract.source_id or execution_id != parsed.execution_id:
        msg = f"{raw.key}: object metadata does not match the key"
        raise BronzeParseError(msg)
    if ingestion_ts is None or ingestion_ts == "":
        msg = f"{raw.key}: missing _ingestion_ts"
        raise BronzeParseError(msg)
    columns = bronze_columns(contract)
    staged: list[dict[str, str | None]] = []
    for index, payload in enumerate(parse_payload(contract, raw.body)):
        row: dict[str, str | None] = {
            "source_id": source_id,
            "execution_id": execution_id,
            "_ingestion_ts": ingestion_ts,
            "raw_object_key": raw.key,
            "record_index": str(index),
            "dt": parsed.dt,
        }
        row.update(payload)
        missing = [name for name in columns if name not in row]
        if missing:
            msg = f"{raw.key}: missing columns {missing}"
            raise BronzeParseError(msg)
        staged.append({name: row[name] for name in columns})
    return tuple(staged)


def encode_staging_jsonl(
    columns: Sequence[str],
    rows: Sequence[Mapping[str, str | None]],
) -> bytes:
    """Encode rows as JSONL. Every value is a JSON string or null."""
    lines: list[str] = []
    for row in rows:
        payload: dict[str, str | None] = {}
        for column in columns:
            value = row[column]
            if value is not None and not isinstance(value, str):
                msg = f"{column}: staging value must be a string or null"
                raise BronzeError(msg)
            payload[column] = value
        lines.append(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")


def render_dataset_sql(
    contract: DataContract,
    row_count: int,
    warehouse_bucket: str,
) -> str:
    """Spark SQL for one Bronze table.

    A Spark invocation runs one of these scripts. A failure then leaves the
    other selected tables at their previous snapshot.
    """
    if row_count < 0:
        msg = "row_count must be >= 0"
        raise BronzeError(msg)
    bucket = require_bucket_name(warehouse_bucket)
    return (
        "CREATE NAMESPACE IF NOT EXISTS bronze;\n\n"
        + _table_sql(contract, bucket, row_count).rstrip()
        + "\n"
    )


def dataset_sql_name(contract: DataContract) -> str:
    """File name for one dataset script."""
    return f"{bronze_table_name(contract.source_id, contract.dataset)}.sql"


def dataset_scripts(
    entries: Sequence[tuple[DataContract, int]],
    warehouse_bucket: str,
) -> tuple[tuple[str, str], ...]:
    """One ``(filename, sql)`` pair per dataset, in the given order."""
    return tuple(
        (
            dataset_sql_name(contract),
            render_dataset_sql(contract, row_count, warehouse_bucket),
        )
        for contract, row_count in entries
    )


def render_bronze_sql(
    entries: Sequence[tuple[DataContract, int]],
    warehouse_bucket: str,
) -> str:
    """Spark SQL that creates each Bronze table and overwrites it from staging.

    The JDBC warehouse URI stays ``s3a://{bucket}/catalog``. Each table sets
    its own location under ``s3a://{bucket}/bronze/{source_id}/{dataset}``.
    The returned text is the per-dataset scripts joined for inspection.
    Spark runs each script on its own.
    """
    return "".join(sql for _name, sql in dataset_scripts(entries, warehouse_bucket))


def promote_bronze(
    client: BaseClient,
    *,
    raw_bucket: str,
    warehouse_bucket: str,
    contracts: Sequence[DataContract],
) -> BronzePromotion:
    """Read each dataset prefix, then write staging and return the Spark SQL.

    A prefix with no objects fails before any staging put. A parse error
    fails before any staging put, so the existing Iceberg table stays.
    """
    if not contracts:
        msg = "no datasets selected"
        raise BronzeError(msg)
    raw_name = require_bucket_name(raw_bucket)
    warehouse_name = require_bucket_name(warehouse_bucket)
    prepared: list[tuple[DataContract, tuple[dict[str, str | None], ...]]] = []
    for contract in contracts:
        keys = list_raw_keys(
            client,
            bucket=raw_name,
            source_id=contract.source_id,
            dataset=contract.dataset,
        )
        if not keys:
            msg = f"no raw objects for {contract.source_id}.{contract.dataset}"
            raise BronzeError(msg)
        rows: list[dict[str, str | None]] = []
        for key in keys:
            response = client.get_object(Bucket=raw_name, Key=key)
            body = response["Body"].read()
            metadata = response.get("Metadata") or {}
            rows.extend(
                rows_from_object(
                    contract,
                    RawObject(key=key, body=body, metadata=metadata),
                )
            )
        prepared.append((contract, tuple(rows)))
    counted = [(contract, len(rows)) for contract, rows in prepared]
    sql = render_bronze_sql(counted, warehouse_name)
    scripts = dataset_scripts(counted, warehouse_name)
    ensure_bucket(client, warehouse_name)
    counts: list[tuple[str, int]] = []
    for contract, rows in prepared:
        client.put_object(
            Bucket=warehouse_name,
            Key=staging_object_key(contract.source_id, contract.dataset),
            Body=encode_staging_jsonl(bronze_columns(contract), rows),
        )
        counts.append((f"{contract.source_id}.{contract.dataset}", len(rows)))
    return BronzePromotion(sql=sql, scripts=scripts, counts=tuple(counts))


def list_raw_keys(
    client: BaseClient,
    *,
    bucket: str,
    source_id: str,
    dataset: str,
) -> tuple[str, ...]:
    """List object keys under ``raw/{source_id}/{dataset}/``, in key order."""
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    prefix = f"raw/{source_id}/{dataset}/"
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            keys.append(item["Key"])
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or token == "":
            break
    return tuple(sorted(keys))


def main(argv: list[str] | None = None) -> int:
    """Write Bronze staging and Spark SQL, or print the live Trino checks."""
    parser = argparse.ArgumentParser(
        description=(
            "Read raw objects and write Bronze staging JSONL plus Spark SQL. "
            "Spark is a separate step."
        ),
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Contract to promote, as source_id/dataset. Repeatable.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Promote every contract. An empty raw prefix fails the run.",
    )
    parser.add_argument(
        "--prepare-live-fixtures",
        action="store_true",
        help=(
            "Write one synthetic raw object per payload format, then promote "
            "those four datasets."
        ),
    )
    parser.add_argument(
        "--sql-out",
        type=str,
        default=None,
        help="Directory for one Spark SQL file per dataset.",
    )
    parser.add_argument(
        "--print-live-checks",
        action="store_true",
        help="Print expected-count and Trino SQL pairs for the live fixtures.",
    )
    args = parser.parse_args(argv)
    try:
        if args.print_live_checks:
            _print_live_checks(args)
            return 0
        _promote_cli(args)
    except BronzeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    return 0


def _print_live_checks(args: argparse.Namespace) -> None:
    if args.sql_out or args.dataset or args.all or args.prepare_live_fixtures:
        msg = "--print-live-checks does not take other arguments"
        raise BronzeError(msg)
    from open_data_platform.lakehouse.fixtures import live_checks

    for expected, statement in live_checks():
        print(f"{expected}\t{statement}")


def _promote_cli(args: argparse.Namespace) -> None:
    if args.all and args.dataset:
        msg = "pass either --all or --dataset"
        raise BronzeError(msg)
    if not args.all and not args.dataset and not args.prepare_live_fixtures:
        msg = "pass --dataset, --all, or --prepare-live-fixtures"
        raise BronzeError(msg)
    if args.sql_out is None:
        msg = "--sql-out is required"
        raise BronzeError(msg)
    env = os.environ
    client = client_from_env(env)
    raw_bucket = env["ODP_RAW_BUCKET"]
    warehouse_bucket = env["ODP_WAREHOUSE_BUCKET"]
    if args.prepare_live_fixtures:
        from open_data_platform.lakehouse.fixtures import write_live_fixtures

        write_live_fixtures(client, bucket=raw_bucket)
    contracts = _selected_contracts(args)
    promotion = promote_bronze(
        client,
        raw_bucket=raw_bucket,
        warehouse_bucket=warehouse_bucket,
        contracts=contracts,
    )
    _write_dataset_scripts(args.sql_out, promotion.scripts)
    for name, count in promotion.counts:
        print(f"{name} {count}")


def _write_dataset_scripts(
    path: str,
    scripts: tuple[tuple[str, str], ...],
) -> None:
    destination = Path(path)
    if destination.exists() and not destination.is_dir():
        msg = "--sql-out must be a directory"
        raise BronzeError(msg)
    destination.mkdir(parents=True, exist_ok=True)
    expected = {name for name, _sql in scripts}
    for child in destination.glob("*.sql"):
        if child.name not in expected:
            child.unlink()
    for name, sql in scripts:
        (destination / name).write_text(sql, encoding="utf-8")


def _selected_contracts(args: argparse.Namespace) -> tuple[DataContract, ...]:
    loaded = load_contracts()
    index = {(item.source_id, item.dataset): item for item in loaded}
    if args.all:
        return loaded
    if args.dataset:
        selected: list[DataContract] = []
        for value in args.dataset:
            identity = _parse_dataset_arg(value)
            contract = index.get(identity)
            if contract is None:
                msg = f"unknown dataset: {value}"
                raise BronzeError(msg)
            selected.append(contract)
        return tuple(selected)
    if args.prepare_live_fixtures:
        from open_data_platform.lakehouse.fixtures import LIVE_DATASETS

        return tuple(index[identity] for identity in LIVE_DATASETS)
    msg = "pass --dataset, --all, or --prepare-live-fixtures"
    raise BronzeError(msg)


def _parse_dataset_arg(value: str) -> tuple[str, str]:
    parts = value.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        msg = "--dataset must be source_id/dataset"
        raise BronzeError(msg)
    return parts[0], parts[1]


def _table_sql(contract: DataContract, bucket: str, row_count: int) -> str:
    source_id = _require_segment("source_id", contract.source_id)
    dataset = _require_segment("dataset", contract.dataset)
    table = bronze_table_name(source_id, dataset)
    quoted_table = _quote_ident(table)
    columns = bronze_columns(contract)
    column_sql = ",\n".join(f"    {_quote_ident(name)} STRING" for name in columns)
    select_sql = ",\n".join(
        f"  CAST({_quote_ident(name)} AS STRING)" for name in columns
    )
    column_list = ", ".join(_quote_ident(name) for name in columns)
    location = f"s3a://{bucket}/bronze/{source_id}/{dataset}"
    lines = [
        f"-- {source_id}.{dataset}",
        f"CREATE TABLE IF NOT EXISTS bronze.{quoted_table} (",
        column_sql,
        ") USING iceberg",
        f"LOCATION '{location}'",
        "TBLPROPERTIES ('format-version' = '2');",
        "",
    ]
    if row_count == 0:
        nulls = ",\n".join(
            f"  CAST(NULL AS STRING) AS {_quote_ident(name)}" for name in columns
        )
        lines.extend(
            [
                f"INSERT OVERWRITE bronze.{quoted_table} ({column_list})",
                "SELECT * FROM (",
                "SELECT",
                nulls,
                ") empty_staging",
                "WHERE 1 = 0;",
            ]
        )
    else:
        staging = f"s3a://{bucket}/bronze/_staging/{source_id}/{dataset}/rows.jsonl"
        view = _quote_ident(f"stage_{table}")
        lines.extend(
            [
                f"CREATE OR REPLACE TEMPORARY VIEW {view}",
                "USING json",
                "OPTIONS (",
                f"  path '{staging}',",
                "  mode 'FAILFAST',",
                "  multiline 'false'",
                ");",
                "",
                f"INSERT OVERWRITE bronze.{quoted_table} ({column_list})",
                "SELECT",
                select_sql,
                f"FROM {view};",
            ]
        )
    lines.append(f"SELECT COUNT(*) AS row_count FROM bronze.{quoted_table};")
    return "\n".join(lines)


def _payload_column_names(contract: DataContract) -> tuple[str, ...]:
    names = [field.name for field in contract.fields]
    locator = contract.record_locator
    for field in contract.envelope_fields:
        if locator is not None and field.name == locator:
            continue
        names.append(field.name)
    return tuple(names)


def _reject_page_shape(contract: DataContract) -> None:
    if contract.record_locator is not None or contract.envelope_fields:
        msg = "envelope fields are only valid for a JSON page"
        raise BronzeParseError(msg)


def _parse_csv(
    contract: DataContract,
    body: bytes,
) -> tuple[dict[str, str | None], ...]:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        msg = "CSV payload is not UTF-8"
        raise BronzeParseError(msg) from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:
        msg = "CSV payload has no header"
        raise BronzeParseError(msg) from exc
    expected = [field.name for field in contract.fields]
    if (
        not header
        or any(name == "" for name in header)
        or len(header) != len(set(header))
        or set(header) != set(expected)
    ):
        msg = "CSV header does not match the contract"
        raise BronzeParseError(msg)
    rows: list[dict[str, str | None]] = []
    for index, row in enumerate(reader):
        if len(row) != len(header):
            msg = f"CSV row {index} does not match the contract"
            raise BronzeParseError(msg)
        cells = dict(zip(header, row, strict=True))
        rows.append(
            {name: None if cells[name] == "" else cells[name] for name in expected}
        )
    return tuple(rows)


def _parse_jsonl(
    contract: DataContract,
    body: bytes,
) -> tuple[dict[str, str | None], ...]:
    payload = body[3:] if body.startswith(b"\xef\xbb\xbf") else body
    try:
        records = load_jsonl_records(payload)
    except PayloadStructureError as exc:
        raise BronzeParseError(str(exc)) from exc
    return tuple(
        _record_strings(contract.fields, record, f"JSONL line {index}")
        for index, record in enumerate(records)
    )


def _parse_json(
    contract: DataContract,
    body: bytes,
) -> tuple[dict[str, str | None], ...]:
    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "payload is not UTF-8 JSON"
        raise BronzeParseError(msg) from exc
    if contract.record_locator is not None:
        return _parse_located_page(contract, payload)
    if contract.envelope_fields:
        msg = "JSON array contracts cannot declare envelope fields"
        raise BronzeParseError(msg)
    if not isinstance(payload, list):
        msg = "JSON payload must be an array"
        raise BronzeParseError(msg)
    return tuple(
        _record_strings(contract.fields, item, f"item {index}")
        for index, item in enumerate(payload)
    )


def _parse_located_page(
    contract: DataContract,
    payload: object,
) -> tuple[dict[str, str | None], ...]:
    if not isinstance(payload, dict):
        msg = "JSON page must be an object"
        raise BronzeParseError(msg)
    expected = {field.name for field in contract.envelope_fields}
    if set(payload) != expected:
        msg = "JSON page envelope does not match the contract"
        raise BronzeParseError(msg)
    locator = contract.record_locator
    if locator is None or locator not in payload:
        msg = "JSON page is missing its record locator"
        raise BronzeParseError(msg)
    items = payload[locator]
    if not isinstance(items, list):
        msg = "JSON page record locator must be an array"
        raise BronzeParseError(msg)
    envelope_fields = tuple(
        field for field in contract.envelope_fields if field.name != locator
    )
    envelope = _record_strings(
        envelope_fields,
        {field.name: payload[field.name] for field in envelope_fields},
        "envelope",
    )
    rows: list[dict[str, str | None]] = []
    for index, item in enumerate(items):
        record = _record_strings(contract.fields, item, f"item {index}")
        record.update(envelope)
        rows.append(record)
    return tuple(rows)


def _record_strings(
    fields: Sequence[ContractField],
    item: object,
    label: str,
) -> dict[str, str | None]:
    if not isinstance(item, dict):
        msg = f"{label} must be a JSON object"
        raise BronzeParseError(msg)
    allowed = {field.name for field in fields}
    required = {field.name for field in fields if not field.nullable}
    keys = set(item)
    if not required <= keys or not keys <= allowed:
        msg = f"{label} does not match the contract"
        raise BronzeParseError(msg)
    rendered: dict[str, str | None] = {}
    for field in fields:
        if field.name not in item or item[field.name] is None:
            rendered[field.name] = None
            continue
        rendered[field.name] = _json_text(item[field.name], f"{label}.{field.name}")
    return rendered


def _json_text(value: object, label: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            msg = f"{label}: non-finite number"
            raise BronzeParseError(msg)
        return json.dumps(value)
    if isinstance(value, dict | list):
        try:
            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            msg = f"{label}: nested JSON could not be encoded"
            raise BronzeParseError(msg) from exc
    msg = f"{label}: unsupported JSON value"
    raise BronzeParseError(msg)


def _parse_key(key: str) -> _RawKey:
    match = _RAW_KEY_RE.fullmatch(key)
    if match is None:
        msg = f"{key}: raw object key does not match the layout"
        raise BronzeParseError(msg)
    try:
        date.fromisoformat(match.group("dt"))
    except ValueError as exc:
        msg = f"{key}: dt is not a calendar date"
        raise BronzeParseError(msg) from exc
    return _RawKey(
        source_id=match.group("source_id"),
        dataset=match.group("dataset"),
        execution_id=match.group("execution_id"),
        dt=match.group("dt"),
    )


def _metadata_strings(metadata: Mapping[str, str]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for key, value in metadata.items():
        if not isinstance(key, str) or not isinstance(value, str):
            msg = "object metadata must be strings"
            raise BronzeParseError(msg)
        rendered[key.lower()] = value
    return rendered


def _require_segment(name: str, value: str) -> str:
    if _SEGMENT_RE.fullmatch(value) is None:
        msg = f"{name} must be a raw path segment: {value!r}"
        raise BronzeError(msg)
    return value


def _quote_ident(name: str) -> str:
    if name == "" or "`" in name or "\x00" in name:
        msg = f"identifier cannot be quoted: {name!r}"
        raise BronzeError(msg)
    return f"`{name}`"


if __name__ == "__main__":
    sys.exit(main())
