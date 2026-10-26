"""Minimal raw object writer (bytes + ingestion metadata)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from open_data_platform.ingestion.metadata import IngestionMetadata
from open_data_platform.ingestion.paths import (
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
