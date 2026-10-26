"""Classify records, then let the object writer land the sidecar."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from open_data_platform.contracts.records import classify_records, encode_findings
from open_data_platform.ingestion.paths import build_quarantine_object_key
from open_data_platform.ingestion.writer import write_raw_object

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import DataContract


class InvalidRecordsError(ValueError):
    """``on_invalid: fail_batch`` rejected the payload before any write."""


def prepare_quarantine(
    contract: DataContract,
    records: Sequence[Mapping[str, object]],
) -> bytes | None:
    """Classify ``records`` before any object is written.

    Returns the sidecar body when there are findings and the policy is
    ``quarantine``. Returns ``None`` when every record is valid. Raises
    ``InvalidRecordsError`` when findings meet ``fail_batch``.
    """
    findings = classify_records(contract, records)
    if not findings:
        return None
    if contract.on_invalid == "fail_batch":
        msg = f"{contract.dataset}: invalid records fail the batch"
        raise InvalidRecordsError(msg)
    return encode_findings(findings)


def write_quarantine_object(
    client: BaseClient,
    *,
    bucket: str,
    source_id: str,
    dataset: str,
    execution_id: str,
    object_name: str,
    ingestion_ts: datetime,
    body: bytes | None,
) -> None:
    """Write the sidecar through ``write_raw_object``, or delete it.

    ``body is None`` means this execution had no invalid records. The
    previous sidecar for that identity is removed. A different
    ``execution_id`` is a different key.
    """
    if body is None:
        if ingestion_ts.tzinfo is None:
            ts_utc = ingestion_ts.replace(tzinfo=UTC)
        else:
            ts_utc = ingestion_ts.astimezone(UTC)
        key = build_quarantine_object_key(
            source_id=source_id,
            dataset=dataset,
            execution_id=execution_id,
            object_name=object_name,
            ingestion_date=ts_utc.date(),
        )
        client.delete_object(Bucket=bucket, Key=key)
        return
    write_raw_object(
        client,
        bucket=bucket,
        body=body,
        source_id=source_id,
        dataset=dataset,
        execution_id=execution_id,
        object_name=object_name,
        ingestion_ts=ingestion_ts,
        content_type="application/x-ndjson",
        prefix="quarantine",
    )
