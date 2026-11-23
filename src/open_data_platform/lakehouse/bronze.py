"""Promote raw objects into Bronze staging rows and Spark SQL.

Bronze keeps one string column per contract field, plus ingestion metadata
and the quarantine reasons of the row's sidecar finding (P3-07). It does
not cast logical types, drop invalid rows, or deduplicate keys.
Spark, invoked separately, loads the staging file into the JDBC catalog.

Since P3-13 (ADR-0024) a promotion is incremental. Only data objects of a
complete execution (a valid ``_COMPLETE`` marker) are promoted, and only
those whose key is absent from the promotion registry or whose ETag changed.
Objects are read and staged one at a time: each object's rows go to a local
JSONL file under the run's work directory, which is uploaded to a staging
key that carries the run label. The Spark script per dataset is either:

- ``INCREMENTAL_APPEND``: one ``MERGE INTO`` that inserts staged rows not
  already present (same ``raw_object_key``, ``_raw_etag``, ``record_index``).
  When a registered key has a new ETag, the same ``MERGE`` deletes that key's
  old rows.
- ``FULL_OVERWRITE``: one ``CREATE OR REPLACE TABLE ... AS SELECT`` from every
  complete object, for ``--full-rebuild`` and for a dataset the registry has
  never seen.

A dataset with nothing new gets no script and its table is not written.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from botocore.exceptions import ClientError

from open_data_platform.contracts import decimals
from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.records import (
    PayloadStructureError,
    load_jsonl_records,
)
from open_data_platform.lakehouse.promotion_registry import PromotedObject
from open_data_platform.lakehouse.write_mode import (
    BRONZE_WRITE_MODE,
    REBUILD_WRITE_MODE,
    WriteMode,
    WriteModeError,
    render_append_only_sql,
    render_incremental_merge_sql,
    require_implemented,
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
    "_execution_completed_ts",
)

# Bronze-only lineage: the ETag of the raw object a row came from. Silver
# does not carry it.
ETAG_COLUMN = "_raw_etag"

# The quarantine reasons the ingestion sidecar records for a row, sorted and
# comma-joined, or NULL when the row has no finding (P3-07). Silver reads it
# to exclude or flag the row as the contract ``on_invalid`` says.
QUARANTINE_COLUMN = "_quarantine_reasons"

# Name of the marker an ingestion writes after the last object of one
# execution (``ingestion.writer.write_completion_marker``).
COMPLETION_MARKER_NAME = "_COMPLETE"

_RUN_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

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
    """Bytes and user-metadata for one raw object.

    ``execution_completed_ts`` is the ``completed_at`` of the execution's
    completion marker, or ``None`` when the execution is incomplete.
    """

    key: str
    body: bytes
    metadata: Mapping[str, str]
    execution_completed_ts: str | None = None
    etag: str | None = None
    quarantine_reasons: Mapping[int, str] | None = None


@dataclass(frozen=True, slots=True)
class RawListing:
    """One listed raw key and its ETag (quotes stripped)."""

    key: str
    etag: str


class RegistryReader(Protocol):
    """The registry lookup Bronze needs: ``raw_object_key -> etag``."""

    def promoted(self, source_id: str, dataset: str) -> dict[str, str]: ...


@dataclass(frozen=True, slots=True)
class _RawKey:
    source_id: str
    dataset: str
    execution_id: str
    dt: str


@dataclass(frozen=True, slots=True)
class DatasetPromotion:
    """What one Bronze run does to one dataset.

    ``objects`` are the raw objects staged in this run. ``script`` is the
    Spark SQL file name, or ``None`` when no table is written.
    """

    source_id: str
    dataset: str
    mode: WriteMode
    objects_listed: int
    objects_incomplete: int
    objects: tuple[PromotedObject, ...]
    replaced_keys: tuple[str, ...]
    rows_staged: int
    script: str | None
    staging_key: str | None

    @property
    def name(self) -> str:
        return f"{self.source_id}.{self.dataset}"


@dataclass(frozen=True, slots=True)
class BronzePromotion:
    """Staging uploads and one Spark script per dataset that changes."""

    sql: str
    scripts: tuple[tuple[str, str], ...]
    counts: tuple[tuple[str, int], ...]
    datasets: tuple[DatasetPromotion, ...] = ()


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
    """Metadata columns, ``_raw_etag``, ``_quarantine_reasons``, then payload.

    Payload is the contract fields, then envelope fields. The
    ``record_locator`` array is exploded and is not itself a column.
    """
    names = [*METADATA_COLUMNS, ETAG_COLUMN, QUARANTINE_COLUMN]
    seen = set(names)
    for name in _payload_column_names(contract):
        if name in seen:
            msg = f"{contract.source_id}.{contract.dataset}: duplicate column {name}"
            raise BronzeError(msg)
        seen.add(name)
        names.append(name)
    return tuple(names)


def staging_object_key(source_id: str, dataset: str, run_label: str) -> str:
    """Warehouse key of the JSONL file Spark reads for one dataset in one run.

    The run label is part of the key, so a ``make`` run and an Airflow run do
    not overwrite each other's staging.
    """
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    label = require_run_label(run_label)
    return f"bronze/_staging/{label}/{source_id}/{dataset}/rows.jsonl"


def require_run_label(run_label: str) -> str:
    """Reject a run label that is not one safe path segment."""
    if _RUN_LABEL_RE.fullmatch(run_label) is None:
        msg = f"run label must be a path segment: {run_label!r}"
        raise BronzeError(msg)
    return run_label


def parse_payload(
    contract: DataContract,
    body: bytes,
) -> tuple[dict[str, str | None], ...]:
    """Return one string row per source record. Structural mismatches raise.

    Empty cells, JSON nulls, and omitted nullable keys become ``None``.
    A value with the wrong logical type is kept as text. Nested JSON stays
    compact JSON text. A JSON number in a ``decimal`` field keeps its exact
    digits. Duplicate keys are not removed.
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
    payloads = parse_payload(contract, raw.body)
    reasons = raw.quarantine_reasons or {}
    if any(index >= len(payloads) for index in reasons):
        msg = f"{raw.key}: quarantine sidecar names a record_index past the payload"
        raise BronzeParseError(msg)
    staged: list[dict[str, str | None]] = []
    for index, payload in enumerate(payloads):
        row: dict[str, str | None] = {
            "source_id": source_id,
            "execution_id": execution_id,
            "_ingestion_ts": ingestion_ts,
            "raw_object_key": raw.key,
            "record_index": str(index),
            "dt": parsed.dt,
            "_execution_completed_ts": raw.execution_completed_ts,
            ETAG_COLUMN: raw.etag,
            QUARANTINE_COLUMN: reasons.get(index),
        }
        row.update(payload)
        missing = [name for name in columns if name not in row]
        if missing:
            msg = f"{raw.key}: missing columns {missing}"
            raise BronzeParseError(msg)
        staged.append({name: row[name] for name in columns})
    return tuple(staged)


def encode_staging_line(
    columns: Sequence[str],
    row: Mapping[str, str | None],
) -> bytes:
    """Encode one row as a JSONL line. Every value is a JSON string or null."""
    payload: dict[str, str | None] = {}
    for column in columns:
        value = row[column]
        if value is not None and not isinstance(value, str):
            msg = f"{column}: staging value must be a string or null"
            raise BronzeError(msg)
        payload[column] = value
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def encode_staging_jsonl(
    columns: Sequence[str],
    rows: Sequence[Mapping[str, str | None]],
) -> bytes:
    """Encode rows as JSONL. Every value is a JSON string or null."""
    return b"".join(encode_staging_line(columns, row) for row in rows)


def render_dataset_sql(
    contract: DataContract,
    row_count: int,
    warehouse_bucket: str,
    *,
    run_label: str = "adhoc",
    write_mode: WriteMode = REBUILD_WRITE_MODE,
    replaced_keys: Sequence[str] = (),
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
        + _table_sql(
            contract,
            bucket,
            row_count,
            run_label=require_run_label(run_label),
            write_mode=write_mode,
            replaced_keys=replaced_keys,
        ).rstrip()
        + "\n"
    )


def dataset_sql_name(contract: DataContract) -> str:
    """File name for one dataset script."""
    return f"{bronze_table_name(contract.source_id, contract.dataset)}.sql"


def dataset_scripts(
    entries: Sequence[tuple[DataContract, int]],
    warehouse_bucket: str,
) -> tuple[tuple[str, str], ...]:
    """One full-rebuild ``(filename, sql)`` pair per dataset, in order."""
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
    """Full-rebuild Spark SQL for each dataset, joined for inspection.

    The JDBC warehouse URI stays ``s3a://{bucket}/catalog``. Each table sets
    its own location under ``s3a://{bucket}/bronze/{source_id}/{dataset}``.
    Spark runs each script on its own.
    """
    return "".join(sql for _name, sql in dataset_scripts(entries, warehouse_bucket))


def promote_bronze(
    client: BaseClient,
    *,
    raw_bucket: str,
    warehouse_bucket: str,
    contracts: Sequence[DataContract],
    run_label: str = "adhoc",
    registry: RegistryReader | None = None,
    full_rebuild: bool = False,
    work_dir: Path | None = None,
) -> BronzePromotion:
    """Stage the new raw objects of each dataset and return the Spark SQL.

    Objects are read one at a time and their rows are written to a local
    file, so no step holds a dataset in memory. A dataset with nothing new
    gets no script. ``full_rebuild`` stages every complete object and fails
    when a dataset has none, so a rebuild never empties a table by accident.
    A parse error fails before that dataset's staging upload, so its Iceberg
    table stays as it was.
    """
    if not contracts:
        msg = "no datasets selected"
        raise BronzeError(msg)
    label = require_run_label(run_label)
    raw_name = require_bucket_name(raw_bucket)
    warehouse_name = require_bucket_name(warehouse_bucket)
    if work_dir is None:
        with tempfile.TemporaryDirectory(prefix="odp-bronze-") as scratch:
            return promote_bronze(
                client,
                raw_bucket=raw_name,
                warehouse_bucket=warehouse_name,
                contracts=contracts,
                run_label=label,
                registry=registry,
                full_rebuild=full_rebuild,
                work_dir=Path(scratch),
            )
    staging_dir = work_dir / "bronze-staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    state = _UploadState(client, warehouse_name)
    datasets: list[DatasetPromotion] = []
    scripts: list[tuple[str, str]] = []
    for contract in contracts:
        entry, script = _promote_dataset(
            client,
            contract,
            raw_bucket=raw_name,
            warehouse_bucket=warehouse_name,
            run_label=label,
            registry=registry,
            full_rebuild=full_rebuild,
            staging_dir=staging_dir,
            uploads=state,
        )
        datasets.append(entry)
        if script is not None:
            scripts.append(script)
    counts = tuple(
        (item.name, item.rows_staged) for item in datasets if item.script is not None
    )
    return BronzePromotion(
        sql="".join(sql for _name, sql in scripts),
        scripts=tuple(scripts),
        counts=counts,
        datasets=tuple(datasets),
    )


class _UploadState:
    """Creates the warehouse bucket once, before the first staging upload."""

    def __init__(self, client: BaseClient, bucket: str) -> None:
        self.client = client
        self.bucket = bucket
        self.ready = False

    def upload(self, path: Path, key: str) -> None:
        if not self.ready:
            ensure_bucket(self.client, self.bucket)
            self.ready = True
        self.client.upload_file(str(path), self.bucket, key)


def _promote_dataset(
    client: BaseClient,
    contract: DataContract,
    *,
    raw_bucket: str,
    warehouse_bucket: str,
    run_label: str,
    registry: RegistryReader | None,
    full_rebuild: bool,
    staging_dir: Path,
    uploads: _UploadState,
) -> tuple[DatasetPromotion, tuple[str, str] | None]:
    name = f"{contract.source_id}.{contract.dataset}"
    data, markers = split_marker_listing(
        list_raw_objects(
            client,
            bucket=raw_bucket,
            source_id=contract.source_id,
            dataset=contract.dataset,
        )
    )
    if full_rebuild and not data:
        msg = f"no raw objects for {name}"
        raise BronzeError(msg)
    registered = (
        {}
        if registry is None
        else registry.promoted(contract.source_id, contract.dataset)
    )
    rebuild = full_rebuild or not registered
    # Only executions with an object to stage need their marker read, so a
    # run with nothing new reads no object body at all.
    candidates = (
        list(data)
        if rebuild
        else [item for item in data if registered.get(item.key) != item.etag]
    )
    prefixes = {execution_prefix(item.key) for item in candidates}
    marker_bodies = {
        execution_prefix(item.key): client.get_object(Bucket=raw_bucket, Key=item.key)[
            "Body"
        ].read()
        for item in markers
        if execution_prefix(item.key) in prefixes
    }
    completion = execution_completion(
        [item.key for item in data if execution_prefix(item.key) in prefixes],
        marker_bodies,
    )
    selected = [item for item in candidates if completion[execution_prefix(item.key)]]
    if full_rebuild and not selected:
        msg = f"no complete execution for {name}; a rebuild would empty the table"
        raise BronzeError(msg)
    if rebuild:
        mode = REBUILD_WRITE_MODE
        replaced: tuple[str, ...] = ()
    else:
        mode = BRONZE_WRITE_MODE
        replaced = tuple(
            sorted(item.key for item in selected if item.key in registered)
        )
    objects: list[PromotedObject] = []
    rows_staged = 0
    table = bronze_table_name(contract.source_id, contract.dataset)
    local = staging_dir / f"{table}.jsonl"
    columns = bronze_columns(contract)
    with local.open("wb") as out:
        for item in selected:
            count = _stage_object(
                client, contract, raw_bucket, item, completion, out, columns
            )
            objects.append(PromotedObject(item.key, item.etag, count))
            rows_staged += count
    script: tuple[str, str] | None = None
    staging_key: str | None = None
    if selected and (rebuild or rows_staged > 0 or replaced):
        if rows_staged > 0:
            staging_key = staging_object_key(
                contract.source_id, contract.dataset, run_label
            )
            uploads.upload(local, staging_key)
        script = (
            dataset_sql_name(contract),
            render_dataset_sql(
                contract,
                rows_staged,
                warehouse_bucket,
                run_label=run_label,
                write_mode=mode,
                replaced_keys=replaced,
            ),
        )
    local.unlink(missing_ok=True)
    entry = DatasetPromotion(
        source_id=contract.source_id,
        dataset=contract.dataset,
        mode=mode,
        objects_listed=len(data),
        objects_incomplete=len(candidates) - len(selected),
        objects=tuple(objects),
        replaced_keys=replaced,
        rows_staged=rows_staged,
        script=None if script is None else script[0],
        staging_key=staging_key,
    )
    return entry, script


def _stage_object(
    client: BaseClient,
    contract: DataContract,
    raw_bucket: str,
    item: RawListing,
    completion: Mapping[str, str | None],
    out: io.BufferedWriter,
    columns: Sequence[str],
) -> int:
    """Read one raw object and append its rows to ``out``. Return the count."""
    response = client.get_object(Bucket=raw_bucket, Key=item.key)
    etag = _clean_etag(response.get("ETag"))
    if etag is not None and etag != item.etag:
        msg = f"{item.key}: object changed after it was listed; rerun the refresh"
        raise BronzeError(msg)
    rows = rows_from_object(
        contract,
        RawObject(
            key=item.key,
            body=response["Body"].read(),
            metadata=response.get("Metadata") or {},
            execution_completed_ts=completion[execution_prefix(item.key)],
            etag=item.etag,
            quarantine_reasons=read_quarantine_reasons(client, raw_bucket, item.key),
        ),
    )
    for row in rows:
        out.write(encode_staging_line(columns, row))
    return len(rows)


def quarantine_key_for(raw_key: str) -> str:
    """The sidecar key the ingestion writes next to one raw object."""
    _parse_key(raw_key)
    return "quarantine/" + raw_key.removeprefix("raw/") + ".jsonl"


def parse_quarantine_reasons(body: bytes, *, key: str) -> dict[int, str]:
    """``record_index -> reasons`` from one sidecar body.

    Reasons of one record are sorted and comma-joined. Errors name the key
    and line, never a finding value.
    """
    found: dict[int, set[str]] = {}
    for line_no, line in enumerate(body.decode("utf-8").splitlines()):
        if line.strip() == "":
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            msg = f"{key}: line {line_no} is not JSON"
            raise BronzeParseError(msg) from exc
        if not isinstance(payload, dict):
            msg = f"{key}: line {line_no} is not an object"
            raise BronzeParseError(msg)
        index = payload.get("record_index")
        reason = payload.get("reason")
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            msg = f"{key}: line {line_no} has no valid record_index"
            raise BronzeParseError(msg)
        if not isinstance(reason, str) or re.fullmatch(r"[a-z_]+", reason) is None:
            msg = f"{key}: line {line_no} has no valid reason"
            raise BronzeParseError(msg)
        found.setdefault(index, set()).add(reason)
    return {index: ",".join(sorted(names)) for index, names in found.items()}


def read_quarantine_reasons(
    client: BaseClient, raw_bucket: str, raw_key: str
) -> dict[int, str]:
    """Reasons per record from the object's sidecar; empty when there is none."""
    key = quarantine_key_for(raw_key)
    try:
        response = client.get_object(Bucket=raw_bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return {}
        raise
    return parse_quarantine_reasons(response["Body"].read(), key=key)


def list_raw_objects(
    client: BaseClient,
    *,
    bucket: str,
    source_id: str,
    dataset: str,
) -> tuple[RawListing, ...]:
    """List keys and ETags under ``raw/{source_id}/{dataset}/``, in key order."""
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    prefix = f"raw/{source_id}/{dataset}/"
    found: list[RawListing] = []
    token: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = item["Key"]
            etag = _clean_etag(item.get("ETag"))
            if etag is None:
                msg = f"{key}: listing has no ETag"
                raise BronzeError(msg)
            found.append(RawListing(key=key, etag=etag))
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or token == "":
            break
    return tuple(sorted(found, key=lambda item: item.key))


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


def split_marker_listing(
    listing: Sequence[RawListing],
) -> tuple[tuple[RawListing, ...], tuple[RawListing, ...]]:
    """Split a listing into data objects and completion markers."""
    data = tuple(
        item
        for item in listing
        if item.key.rsplit("/", 1)[-1] != COMPLETION_MARKER_NAME
    )
    markers = tuple(
        item
        for item in listing
        if item.key.rsplit("/", 1)[-1] == COMPLETION_MARKER_NAME
    )
    return data, markers


def _clean_etag(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().strip('"')
    return cleaned or None


def split_marker_keys(
    keys: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split listed keys into data object keys and completion marker keys."""
    data: list[str] = []
    markers: list[str] = []
    for key in keys:
        if key.rsplit("/", 1)[-1] == COMPLETION_MARKER_NAME:
            markers.append(key)
        else:
            data.append(key)
    return tuple(data), tuple(markers)


def execution_prefix(key: str) -> str:
    """The ``.../execution_id=X/`` prefix that holds one raw object or marker."""
    return key.rsplit("/", 1)[0] + "/"


def execution_completion(
    data_keys: Sequence[str],
    markers: Mapping[str, bytes],
) -> dict[str, str | None]:
    """Completion time per execution prefix, or ``None`` when incomplete.

    An execution is complete when its prefix has a marker whose
    ``object_names`` equal the data objects found under that prefix. A
    missing marker, an unreadable marker, or a different object set (a
    partial page set, a stray object) leaves the execution incomplete.
    """
    names_by_prefix: dict[str, set[str]] = {}
    for key in data_keys:
        names_by_prefix.setdefault(execution_prefix(key), set()).add(
            key.rsplit("/", 1)[-1]
        )
    completion: dict[str, str | None] = {}
    for prefix, names in names_by_prefix.items():
        body = markers.get(prefix)
        completion[prefix] = None if body is None else _marker_completed_at(body, names)
    return completion


def _marker_completed_at(body: bytes, names: set[str]) -> str | None:
    try:
        marker = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(marker, dict):
        return None
    listed = marker.get("object_names")
    completed_at = marker.get("completed_at")
    if not isinstance(listed, list) or not isinstance(completed_at, str):
        return None
    if not all(isinstance(name, str) for name in listed):
        return None
    if set(listed) != names or len(listed) != len(names):
        return None
    if completed_at == "":
        return None
    return completed_at


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
        "--prepare-unit-fixtures",
        "--prepare-live-fixtures",
        dest="prepare_unit_fixtures",
        action="store_true",
        help=(
            "Unit-test helper only: write synthetic raw objects for the four "
            "payload formats into ODP_RAW_BUCKET, then promote those datasets. "
            "Not used by the full-data live run (make live-bronze)."
        ),
    )
    parser.add_argument(
        "--sql-out",
        type=str,
        default=None,
        help="Directory for one Spark SQL file per dataset.",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default="cli",
        help=(
            "Run label in the staging key. The CLI has no promotion registry, "
            "so it renders a full rebuild of each selected dataset."
        ),
    )
    parser.add_argument(
        "--counts-out",
        type=str,
        default=None,
        help=(
            "Write source.dataset<TAB>count for the raw objects read in this "
            "promotion. make live-bronze passes that file to --print-live-checks."
        ),
    )
    parser.add_argument(
        "--counts-file",
        type=str,
        default=None,
        help=(
            "Count file from --counts-out. Required with --print-live-checks. "
            "The expected Trino count is the raw read for that run."
        ),
    )
    parser.add_argument(
        "--print-live-checks",
        action="store_true",
        help=(
            "Print expected-count and Trino SQL pairs for the Bronze live run. "
            "Requires --counts-file from the promotion that just read raw."
        ),
    )
    parser.add_argument(
        "--print-fixture-checks",
        action="store_true",
        help="Print expected-count and Trino SQL pairs for unit-test fixtures.",
    )
    args = parser.parse_args(argv)
    try:
        if args.print_live_checks:
            _print_count_checks(args, live=True)
            return 0
        if args.print_fixture_checks:
            _print_count_checks(args, live=False)
            return 0
        _promote_cli(args)
    except BronzeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    return 0


def _print_count_checks(args: argparse.Namespace, *, live: bool) -> None:
    if (
        args.sql_out
        or args.dataset
        or args.all
        or args.prepare_unit_fixtures
        or args.counts_out
        or (live and args.print_fixture_checks)
        or ((not live) and (args.print_live_checks or args.counts_file))
    ):
        msg = "count-check flags do not take other arguments"
        raise BronzeError(msg)
    if live:
        from open_data_platform.lakehouse.reconciliation import (
            ReconciliationError,
            bronze_count_checks,
            parse_counts,
        )

        if not args.counts_file:
            msg = "pass --counts-file"
            raise BronzeError(msg)
        try:
            text = Path(args.counts_file).read_text(encoding="utf-8")
        except OSError as exc:
            msg = f"cannot read count file: {args.counts_file}"
            raise BronzeError(msg) from exc
        try:
            checks = bronze_count_checks(parse_counts(text), load_contracts())
        except ReconciliationError as exc:
            raise BronzeError(str(exc)) from exc
    else:
        from open_data_platform.lakehouse.fixtures import live_checks

        checks = live_checks()
    for expected, statement in checks:
        print(f"{expected}\t{statement}")


def _promote_cli(args: argparse.Namespace) -> None:
    if args.all and args.dataset:
        msg = "pass either --all or --dataset"
        raise BronzeError(msg)
    if not args.all and not args.dataset and not args.prepare_unit_fixtures:
        msg = "pass --dataset, --all, or --prepare-unit-fixtures"
        raise BronzeError(msg)
    if args.sql_out is None:
        msg = "--sql-out is required"
        raise BronzeError(msg)
    env = os.environ
    client = client_from_env(env)
    raw_bucket = env["ODP_RAW_BUCKET"]
    warehouse_bucket = env["ODP_WAREHOUSE_BUCKET"]
    if args.prepare_unit_fixtures:
        from open_data_platform.lakehouse.fixtures import write_live_fixtures

        write_live_fixtures(client, bucket=raw_bucket)
    contracts = _selected_contracts(args)
    promotion = promote_bronze(
        client,
        raw_bucket=raw_bucket,
        warehouse_bucket=warehouse_bucket,
        contracts=contracts,
        run_label=args.run_label,
        full_rebuild=True,
    )
    _write_dataset_scripts(args.sql_out, promotion.scripts)
    if args.counts_out:
        from open_data_platform.lakehouse.reconciliation import format_counts

        destination = Path(args.counts_out)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(format_counts(promotion.counts), encoding="utf-8")
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
    if args.prepare_unit_fixtures:
        from open_data_platform.lakehouse.fixtures import LIVE_DATASETS

        return tuple(index[identity] for identity in LIVE_DATASETS)
    msg = "pass --dataset, --all, or --prepare-unit-fixtures"
    raise BronzeError(msg)


def _parse_dataset_arg(value: str) -> tuple[str, str]:
    parts = value.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        msg = "--dataset must be source_id/dataset"
        raise BronzeError(msg)
    return parts[0], parts[1]


def _table_sql(
    contract: DataContract,
    bucket: str,
    row_count: int,
    *,
    run_label: str,
    write_mode: WriteMode,
    replaced_keys: Sequence[str],
) -> str:
    context = f"{contract.source_id}.{contract.dataset}"
    try:
        if write_mode is WriteMode.INCREMENTAL_MERGE:
            return render_incremental_merge_sql(context=context)
        if write_mode is WriteMode.APPEND_ONLY:
            return render_append_only_sql(context=context)
        require_implemented(write_mode, context=context)
    except WriteModeError as exc:
        raise BronzeError(str(exc)) from exc
    source_id = _require_segment("source_id", contract.source_id)
    dataset = _require_segment("dataset", contract.dataset)
    table = bronze_table_name(source_id, dataset)
    quoted_table = _quote_ident(table)
    columns = bronze_columns(contract)
    view = _quote_ident(f"stage_{table}")
    staging = f"s3a://{bucket}/{staging_object_key(source_id, dataset, run_label)}"
    stage_view = [
        f"CREATE OR REPLACE TEMPORARY VIEW {view}",
        "USING json",
        "OPTIONS (",
        f"  path '{staging}',",
        "  mode 'FAILFAST',",
        "  multiline 'false'",
        ");",
        "",
    ]
    if row_count == 0:
        source_view: list[str] = []
        source_select = [
            "SELECT * FROM (",
            "SELECT",
            ",\n".join(
                f"  CAST(NULL AS STRING) AS {_quote_ident(name)}" for name in columns
            ),
            ") empty_staging",
            "WHERE 1 = 0",
        ]
    else:
        source_view = stage_view
        source_select = [
            "SELECT",
            ",\n".join(
                f"  CAST({_quote_ident(name)} AS STRING) AS {_quote_ident(name)}"
                for name in columns
            ),
            f"FROM {view}",
        ]
    lines = [f"-- {source_id}.{dataset}"]
    if write_mode is WriteMode.FULL_OVERWRITE:
        if replaced_keys:
            msg = f"{context}: a full rebuild does not take replaced keys"
            raise BronzeError(msg)
        location = f"s3a://{bucket}/bronze/{source_id}/{dataset}"
        # One atomic replace: readers see the previous snapshot or the new one.
        lines.extend(
            [
                f"-- write_mode={write_mode.value} (full rebuild, one atomic replace)",
                *source_view,
                f"CREATE OR REPLACE TABLE bronze.{quoted_table}",
                "USING iceberg",
                f"LOCATION '{location}'",
                "TBLPROPERTIES ('format-version' = '2')",
                "AS " + "\n".join(source_select) + ";",
            ]
        )
    else:
        lines.extend(
            [
                f"-- write_mode={write_mode.value} "
                "(new raw objects only, one MERGE commit)",
                *source_view,
                *_merge_sql(quoted_table, columns, source_select, replaced_keys),
            ]
        )
    lines.append(f"SELECT COUNT(*) AS row_count FROM bronze.{quoted_table};")
    return "\n".join(lines)


def _merge_sql(
    quoted_table: str,
    columns: Sequence[str],
    source_select: Sequence[str],
    replaced_keys: Sequence[str],
) -> list[str]:
    """``MERGE`` that inserts staged rows not yet present.

    The match key is the row's lineage (object key, ETag, record index), so a
    retry after a lost registry write inserts nothing. For a registered key
    whose ETag changed, the old rows of that key are deleted in the same
    commit. Object keys are paths that match the raw layout; no row value is
    rendered into the statement.
    """
    quoted = [_quote_ident(name) for name in columns]
    match = " AND ".join(
        f"target.{_quote_ident(name)} = source.{_quote_ident(name)}"
        for name in ("raw_object_key", ETAG_COLUMN, "record_index")
    )
    lines = [
        f"MERGE INTO bronze.{quoted_table} AS target",
        "USING (",
        *source_select,
        ") AS source",
        f"ON {match}",
        "WHEN NOT MATCHED THEN INSERT (" + ", ".join(quoted) + ")",
        "VALUES (" + ", ".join(f"source.{name}" for name in quoted) + ")",
    ]
    if replaced_keys:
        literals = ", ".join(_key_literal(key) for key in sorted(set(replaced_keys)))
        lines.extend(
            [
                "WHEN NOT MATCHED BY SOURCE",
                f"  AND target.`raw_object_key` IN ({literals})",
                "THEN DELETE",
            ]
        )
    lines[-1] += ";"
    return lines


def _key_literal(key: str) -> str:
    """A raw object key as a SQL string literal. Only layout keys are accepted."""
    _parse_key(key)
    if "'" in key or "\\" in key:
        msg = f"{key}: raw key cannot be rendered as a literal"
        raise BronzeError(msg)
    return f"'{key}'"


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
        payload = decimals.json_loads(body.decode("utf-8-sig"))
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
        rendered[field.name] = _json_text(
            item[field.name], f"{label}.{field.name}", field
        )
    return rendered


def _json_text(value: object, label: str, field: ContractField) -> str:
    """Bronze text of one JSON value.

    A number in a ``decimal`` field keeps its exact digits (``10.50`` stays
    ``10.50``). Other numbers keep the text they had before decimals existed:
    the shortest text of the binary float.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if field.field_type == "decimal":
            return _decimal_text(value, field)
        value = float(value)
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
                default=_decimal_as_float,
            )
        except (TypeError, ValueError) as exc:
            msg = f"{label}: nested JSON could not be encoded"
            raise BronzeParseError(msg) from exc
    msg = f"{label}: unsupported JSON value"
    raise BronzeParseError(msg)


def _decimal_text(value: Decimal, field: ContractField) -> str:
    """Plain text when the value fits the field; otherwise its compact text.

    Silver casts only plain text that fits, so a value outside the contract
    becomes NULL there instead of a rounded number.
    """
    if (
        field.precision is not None
        and field.scale is not None
        and decimals.fits(value, field.precision, field.scale)
    ):
        return decimals.plain_text(value)
    return str(value)


def _decimal_as_float(value: object) -> float:
    """Nested JSON numbers keep the text they had before decimals existed."""
    if isinstance(value, Decimal):
        return float(value)
    msg = f"unsupported nested JSON value: {type(value).__name__}"
    raise TypeError(msg)


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
