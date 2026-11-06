"""Stage MinIO quarantine JSONL into an Iceberg table for Gold quality facts.

P2-06 only unions contract violations into ``fct_*_quality_findings``.
Reprocessing quarantined rows remains P3-07.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from botocore.client import BaseClient
from trino.dbapi import connect

from open_data_platform.object_storage import client_from_env

SCHEMA = "gold_staging"
TABLE = "quarantine_findings"
FULL_TABLE = f"iceberg.{SCHEMA}.{TABLE}"


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


def _sql_str(value: str | None) -> str:
    if value is None:
        return "CAST(NULL AS varchar)"
    return "'" + value.replace("'", "''") + "'"


def render_create_replace_sql(
    rows: Sequence[QuarantineFindingRow],
    *,
    warehouse_bucket: str,
) -> str:
    """Build CREATE OR REPLACE TABLE SQL for the staged findings."""
    location = f"s3a://{warehouse_bucket}/gold/{SCHEMA}/{TABLE}"
    create_schema = f"CREATE SCHEMA IF NOT EXISTS iceberg.{SCHEMA}"
    if not rows:
        create_table = (
            f"CREATE OR REPLACE TABLE {FULL_TABLE} (\n"
            "  source_id varchar,\n"
            "  dataset varchar,\n"
            "  source_dataset varchar,\n"
            "  reason varchar,\n"
            "  field_name varchar,\n"
            "  business_key varchar,\n"
            "  record_index bigint,\n"
            "  quarantine_object_key varchar,\n"
            "  detail varchar\n"
            f")\nWITH (format = 'PARQUET', location = '{location}')"
        )
        return create_schema + ";\n" + create_table
    values = ",\n".join(
        "("
        + ", ".join(
            [
                _sql_str(row.source_id),
                _sql_str(row.dataset),
                _sql_str(row.source_dataset),
                _sql_str(row.reason),
                _sql_str(row.field),
                _sql_str(row.business_key),
                str(row.record_index),
                _sql_str(row.quarantine_object_key),
                _sql_str(row.detail),
            ]
        )
        + ")"
        for row in rows
    )
    create_table = (
        f"CREATE OR REPLACE TABLE {FULL_TABLE}\n"
        f"WITH (format = 'PARQUET', location = '{location}')\n"
        "AS\n"
        "SELECT * FROM (VALUES\n" + values + "\n) AS t (\n"
        "  source_id, dataset, source_dataset, reason, field_name,\n"
        "  business_key, record_index, quarantine_object_key, detail\n"
        ")"
    )
    return create_schema + ";\n" + create_table


def trino_connection_from_env():
    """Open a Trino connection using ``TRINO_*`` env vars."""
    host = os.environ.get("TRINO_HOST", "localhost")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    user = os.environ.get("TRINO_USER", "odp")
    return connect(
        host=host,
        port=port,
        user=user,
        catalog="iceberg",
        schema=SCHEMA,
        http_scheme="http",
    )


def stage_quarantine_findings(
    *,
    client: BaseClient | None = None,
    env: Mapping[str, str] | None = None,
    bucket: str | None = None,
    warehouse_bucket: str | None = None,
    conn=None,
) -> int:
    """Replace ``iceberg.gold_staging.quarantine_findings`` from MinIO sidecars.

    Uses ``object_storage.client_from_env`` when ``client`` is omitted.
    Returns the number of finding rows written.
    """
    settings = env if env is not None else os.environ
    s3 = client or client_from_env(settings)
    raw_bucket = bucket or settings.get("ODP_RAW_BUCKET", "odp-raw")
    wh = warehouse_bucket or settings.get("ODP_WAREHOUSE_BUCKET", "odp-warehouse")
    rows = tuple(iter_quarantine_rows(s3, raw_bucket))
    sql = render_create_replace_sql(rows, warehouse_bucket=wh)
    own_conn = conn is None
    connection = conn or trino_connection_from_env()
    try:
        cursor = connection.cursor()
        for statement in sql.split(";\n"):
            text = statement.strip().rstrip(";")
            if text:
                cursor.execute(text)
        return len(rows)
    finally:
        if own_conn:
            connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry: stage quarantine findings and print the row count."""
    del argv  # reserved for future flags
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"}:
        msg = f"MINIO_ENDPOINT_URL must be http(s): {endpoint}"
        raise QuarantineStageError(msg)
    count = stage_quarantine_findings()
    print(count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
