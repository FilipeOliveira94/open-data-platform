"""Stage MinIO quarantine JSONL into an Iceberg table for Gold quality facts.

Findings are written as a warehouse JSONL object. Spark SQL names that path
and the columns. It does not inline finding values, so the driver log and the
Trino query history stay free of primary keys. P2-06 unions contract
violations into ``fct_*_quality_findings``. Reprocessing quarantined rows
remains P3-07.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from open_data_platform.object_storage import client_from_env, ensure_bucket

SCHEMA = "gold_staging"
TABLE = "quarantine_findings"
STAGING_KEY = f"gold/_staging/{SCHEMA}/{TABLE}/rows.jsonl"
TABLE_LOCATION = f"gold/{SCHEMA}/{TABLE}"
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_id", "STRING"),
    ("dataset", "STRING"),
    ("source_dataset", "STRING"),
    ("reason", "STRING"),
    ("field_name", "STRING"),
    ("business_key", "STRING"),
    ("record_index", "BIGINT"),
    ("quarantine_object_key", "STRING"),
    ("detail", "STRING"),
)


@dataclass(frozen=True, slots=True)
class QuarantineFindingRow:
    """One staged contract-violation row for Gold."""

    source_id: str
    dataset: str
    source_dataset: str
    reason: str
    field: str | None
    business_key: str
    record_index: int
    quarantine_object_key: str
    detail: str


class QuarantineStageError(ValueError):
    """Staging inputs or Trino write failed."""


def list_quarantine_keys(client: BaseClient, bucket: str) -> tuple[str, ...]:
    """Return every object key under ``quarantine/`` in ``bucket``."""
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": "quarantine/"}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = item["Key"]
            if key.endswith(".jsonl"):
                keys.append(key)
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return tuple(sorted(keys))


def parse_quarantine_key(key: str) -> tuple[str, str]:
    """Extract ``source_id`` and ``dataset`` from a quarantine object key."""
    # quarantine/{source_id}/{dataset}/dt=…/execution_id=…/{object}.jsonl
    parts = key.split("/")
    if len(parts) < 6 or parts[0] != "quarantine":
        msg = f"unexpected quarantine key: {key}"
        raise QuarantineStageError(msg)
    return parts[1], parts[2]


def parse_finding_line(
    line: str,
    *,
    source_id: str,
    dataset: str,
    object_key: str,
) -> QuarantineFindingRow:
    """Map one JSONL finding into a staging row."""
    payload = json.loads(line)
    if not isinstance(payload, dict):
        msg = f"{object_key}: finding line must be an object"
        raise QuarantineStageError(msg)
    reason = payload.get("reason")
    if not isinstance(reason, str) or reason == "":
        msg = f"{object_key}: finding reason is required"
        raise QuarantineStageError(msg)
    primary_key = payload.get("primary_key")
    if primary_key is None:
        primary_key = {}
    if not isinstance(primary_key, dict):
        msg = f"{object_key}: primary_key must be an object"
        raise QuarantineStageError(msg)
    field = payload.get("field")
    if field is not None and not isinstance(field, str):
        msg = f"{object_key}: field must be a string or null"
        raise QuarantineStageError(msg)
    record_index = payload.get("record_index")
    if not isinstance(record_index, int) or isinstance(record_index, bool):
        msg = f"{object_key}: record_index must be an integer"
        raise QuarantineStageError(msg)
    business_key = json.dumps(primary_key, ensure_ascii=False, sort_keys=True)
    detail = f"{reason} at record_index={record_index}"
    return QuarantineFindingRow(
        source_id=source_id,
        dataset=dataset,
        source_dataset=f"{source_id}.{dataset}",
        reason=reason,
        field=field,
        business_key=business_key,
        record_index=record_index,
        quarantine_object_key=object_key,
        detail=detail,
    )


def iter_quarantine_rows(
    client: BaseClient,
    bucket: str,
    keys: Sequence[str] | None = None,
) -> Iterator[QuarantineFindingRow]:
    """Yield staged rows for every quarantine object (or ``keys``)."""
    for key in keys if keys is not None else list_quarantine_keys(client, bucket):
        source_id, dataset = parse_quarantine_key(key)
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line_no, line in enumerate(body.splitlines()):
            if line == "":
                continue
            try:
                yield parse_finding_line(
                    line,
                    source_id=source_id,
                    dataset=dataset,
                    object_key=key,
                )
            except json.JSONDecodeError as exc:
                msg = f"{key}: line {line_no} is not JSON"
                raise QuarantineStageError(msg) from exc


def encode_findings_jsonl(rows: Sequence[QuarantineFindingRow]) -> bytes:
    """Encode staged findings as JSONL. Every value is a JSON string or null."""
    lines: list[str] = []
    for row in rows:
        payload = {
            "source_id": _text(row.source_id, "source_id"),
            "dataset": _text(row.dataset, "dataset"),
            "source_dataset": _text(row.source_dataset, "source_dataset"),
            "reason": _text(row.reason, "reason"),
            "field_name": None if row.field is None else _text(row.field, "field"),
            "business_key": _text(row.business_key, "business_key"),
            "record_index": str(row.record_index),
            "quarantine_object_key": _text(
                row.quarantine_object_key, "quarantine_object_key"
            ),
            "detail": _text(row.detail, "detail"),
        }
        lines.append(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")


def render_load_sql(*, warehouse_bucket: str, row_count: int) -> str:
    """Spark SQL that replaces the findings table from the staging object.

    The statement names the warehouse path and the columns. Finding values
    stay in the JSONL object.
    """
    bucket = _require_bucket(warehouse_bucket)
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        msg = f"row_count must be a non-negative integer: {row_count!r}"
        raise QuarantineStageError(msg)
    location = f"s3a://{bucket}/{TABLE_LOCATION}"
    quoted_table = _quote(TABLE)
    column_sql = ",\n".join(f"    {_quote(name)} {kind}" for name, kind in _COLUMNS)
    column_list = ", ".join(_quote(name) for name, _kind in _COLUMNS)
    select_sql = ",\n".join(
        f"  CAST({_quote(name)} AS {kind})" for name, kind in _COLUMNS
    )
    lines = [
        f"-- {SCHEMA}.{TABLE}",
        "-- values stay in the staging JSONL; this script names the path only",
        f"CREATE NAMESPACE IF NOT EXISTS {SCHEMA};",
        "",
        f"DROP TABLE IF EXISTS {SCHEMA}.{quoted_table};",
        f"CREATE TABLE {SCHEMA}.{quoted_table} (",
        column_sql,
        ") USING iceberg",
        f"LOCATION '{location}'",
        "TBLPROPERTIES ('format-version' = '2');",
        "",
    ]
    if row_count == 0:
        nulls = ",\n".join(
            f"  CAST(NULL AS {kind}) AS {_quote(name)}" for name, kind in _COLUMNS
        )
        lines.extend(
            [
                f"INSERT OVERWRITE {SCHEMA}.{quoted_table} ({column_list})",
                "SELECT * FROM (",
                "SELECT",
                nulls,
                ") empty_staging",
                "WHERE 1 = 0;",
            ]
        )
    else:
        staging = f"s3a://{bucket}/{STAGING_KEY}"
        view = _quote(f"stage_{TABLE}")
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
                f"INSERT OVERWRITE {SCHEMA}.{quoted_table} ({column_list})",
                "SELECT",
                select_sql,
                f"FROM {view};",
            ]
        )
    lines.append(f"SELECT COUNT(*) AS row_count FROM {SCHEMA}.{quoted_table};")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class QuarantineStaging:
    """Row count and path-only Spark SQL for one staging run."""

    row_count: int
    sql: str


def prepare_quarantine_staging(
    *,
    client: BaseClient | None = None,
    env: Mapping[str, str] | None = None,
    bucket: str | None = None,
    warehouse_bucket: str | None = None,
    keys: Sequence[str] | None = None,
) -> QuarantineStaging:
    """Read quarantine sidecars and write the staging JSONL when rows exist.

    Uses ``object_storage.client_from_env`` when ``client`` is omitted.
    An empty read deletes a previous staging object. Spark applies ``sql``.
    """
    settings = env if env is not None else os.environ
    s3 = client or client_from_env(settings)
    raw_bucket = _require_bucket(bucket or settings.get("ODP_RAW_BUCKET", "odp-raw"))
    wh = _require_bucket(
        warehouse_bucket or settings.get("ODP_WAREHOUSE_BUCKET", "odp-warehouse")
    )
    rows = tuple(iter_quarantine_rows(s3, raw_bucket, keys))
    ensure_bucket(s3, wh)
    if rows:
        s3.put_object(
            Bucket=wh,
            Key=STAGING_KEY,
            Body=encode_findings_jsonl(rows),
            ContentType="application/x-ndjson",
        )
    else:
        _delete_staging_object(s3, wh)
    return QuarantineStaging(
        row_count=len(rows),
        sql=render_load_sql(warehouse_bucket=wh, row_count=len(rows)),
    )


def write_sql(destination: Path, sql: str) -> None:
    """Write the single Spark script, replacing a previous file in the directory."""
    if destination.exists() and not destination.is_dir():
        msg = "--sql-out must be a directory"
        raise QuarantineStageError(msg)
    destination.mkdir(parents=True, exist_ok=True)
    expected = "quarantine_findings.sql"
    for child in destination.glob("*.sql"):
        if child.name != expected:
            child.unlink()
    (destination / expected).write_text(sql, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Write the staging JSONL and the path-only Spark SQL. Print the row count."""
    parser = argparse.ArgumentParser(
        description=(
            "Stage quarantine findings as warehouse JSONL and write Spark SQL "
            "that loads that file. Spark is a separate step."
        ),
    )
    parser.add_argument(
        "--sql-out",
        type=str,
        required=True,
        help="Directory for quarantine_findings.sql.",
    )
    args = parser.parse_args(argv)
    try:
        endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"}:
            msg = f"MINIO_ENDPOINT_URL must be http(s): {endpoint}"
            raise QuarantineStageError(msg)
        staged = prepare_quarantine_staging()
        write_sql(Path(args.sql_out), staged.sql)
        print(staged.row_count)
    except QuarantineStageError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    return 0


def _text(value: str, name: str) -> str:
    if not isinstance(value, str):
        msg = f"{name} must be a string"
        raise QuarantineStageError(msg)
    return value


def _require_bucket(bucket: str) -> str:
    if _BUCKET_RE.fullmatch(bucket) is None:
        msg = f"bucket name is not a simple S3 identifier: {bucket!r}"
        raise QuarantineStageError(msg)
    return bucket


def _quote(name: str) -> str:
    if name == "" or "`" in name or "\x00" in name:
        msg = f"identifier cannot be quoted: {name!r}"
        raise QuarantineStageError(msg)
    return f"`{name}`"


def _delete_staging_object(client: BaseClient, bucket: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=STAGING_KEY)
    except ClientError:
        return


if __name__ == "__main__":
    raise SystemExit(main())
