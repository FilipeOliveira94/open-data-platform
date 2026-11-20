"""Raw object writer (bytes + ingestion metadata) and completion markers."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from open_data_platform.ingestion.metadata import IngestionMetadata
from open_data_platform.ingestion.paths import (
    build_completion_marker_key,
    build_quarantine_object_key,
    build_raw_object_key,
)
from open_data_platform.object_storage import ensure_bucket

_PREFIXES = frozenset({"raw", "quarantine"})

if TYPE_CHECKING:
    from botocore.client import BaseClient


@dataclass(frozen=True, slots=True)
class RawWriteResult:
    """Outcome of a raw put."""

    bucket: str
    key: str
    metadata: IngestionMetadata


def write_raw_object(
    client: BaseClient,
    *,
    bucket: str,
    body: bytes,
    source_id: str,
    dataset: str,
    execution_id: str,
    object_name: str,
    ingestion_ts: datetime | None = None,
    content_type: str | None = None,
    prefix: str = "raw",
) -> RawWriteResult:
    """Write ``body`` with required ingestion metadata.

    ``prefix`` is ``raw`` or ``quarantine``. The quarantine prefix appends
    ``.jsonl`` to ``object_name``. The same identity overwrites the same key.
    """
    if prefix not in _PREFIXES:
        msg = "prefix must be raw or quarantine"
        raise ValueError(msg)
    meta = IngestionMetadata.create(
        source_id=source_id,
        execution_id=execution_id,
        ingestion_ts=ingestion_ts,
    )
    ts = meta.ingestion_ts
    if ts.tzinfo is None:
        ts_utc = ts.replace(tzinfo=UTC)
    else:
        ts_utc = ts.astimezone(UTC)
    key_args = {
        "source_id": source_id,
        "dataset": dataset,
        "execution_id": execution_id,
        "object_name": object_name,
        "ingestion_date": ts_utc.date(),
    }
    if prefix == "raw":
        key = build_raw_object_key(**key_args)
    else:
        key = build_quarantine_object_key(**key_args)

    ensure_bucket(client, bucket)
    put_kwargs: dict[str, object] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "Metadata": meta.to_object_metadata(),
    }
    if content_type is not None:
        put_kwargs["ContentType"] = content_type
    client.put_object(**put_kwargs)

    return RawWriteResult(bucket=bucket, key=key, metadata=meta)


def clear_completion_marker(
    client: BaseClient,
    *,
    bucket: str,
    source_id: str,
    dataset: str,
    execution_id: str,
    ingestion_ts: datetime,
) -> str:
    """Remove the marker of one execution before its objects are (re)written.

    A retry with the same identity then stays incomplete until it writes its
    last object, so a run that fails halfway never keeps an earlier marker.
    Returns the marker key. A missing marker is not an error.
    """
    key = build_completion_marker_key(
        source_id=source_id,
        dataset=dataset,
        execution_id=execution_id,
        ingestion_date=_utc(ingestion_ts).date(),
    )
    ensure_bucket(client, bucket)
    client.delete_object(Bucket=bucket, Key=key)
    return key


def write_completion_marker(
    client: BaseClient,
    *,
    bucket: str,
    source_id: str,
    dataset: str,
    execution_id: str,
    ingestion_ts: datetime,
    object_names: Sequence[str],
    completed_at: datetime | None = None,
) -> str:
    """Write the marker that closes one execution of one dataset.

    Call it after the last data object of the execution. The body lists the
    data object names (names only, no row values) and the completion time.
    Bronze treats the execution as complete only when the listed names equal
    the data objects found under the same prefix.
    """
    if not object_names:
        msg = "a completion marker needs at least one object name"
        raise ValueError(msg)
    meta = IngestionMetadata.create(
        source_id=source_id,
        execution_id=execution_id,
        ingestion_ts=ingestion_ts,
    )
    key = build_completion_marker_key(
        source_id=source_id,
        dataset=dataset,
        execution_id=execution_id,
        ingestion_date=_utc(ingestion_ts).date(),
    )
    done = datetime.now(UTC) if completed_at is None else completed_at
    body = {
        "source_id": source_id,
        "dataset": dataset,
        "execution_id": execution_id,
        "object_count": len(object_names),
        "object_names": sorted(object_names),
        "completed_at": _utc(done).isoformat().replace("+00:00", "Z"),
    }
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, sort_keys=True).encode("utf-8"),
        Metadata=meta.to_object_metadata(),
        ContentType="application/json",
    )
    return key


def object_name_of(key: str) -> str:
    """Last path segment of a raw object key."""
    return key.rsplit("/", 1)[-1]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
