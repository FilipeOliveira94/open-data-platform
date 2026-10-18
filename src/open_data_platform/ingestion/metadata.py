"""Ingestion metadata fields for auditable raw objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def _format_ingestion_ts(value: datetime) -> str:
    """Return UTC ISO-8601 with ``Z`` suffix."""
    if value.tzinfo is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    return aware.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class IngestionMetadata:
    """Required lineage fields recorded on every raw write.

    Field names match the documented conventions (``_ingestion_ts``,
    ``source_id``, ``execution_id``).
    """

    source_id: str
    execution_id: str
    ingestion_ts: datetime

    def __post_init__(self) -> None:
        if not self.source_id.strip():
            msg = "source_id must be non-empty"
            raise ValueError(msg)
        if not self.execution_id.strip():
            msg = "execution_id must be non-empty"
            raise ValueError(msg)

    @classmethod
    def create(
        cls,
        *,
        source_id: str,
        execution_id: str,
        ingestion_ts: datetime | None = None,
    ) -> IngestionMetadata:
        """Build metadata, defaulting ``ingestion_ts`` to UTC now."""
        ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
        return cls(
            source_id=source_id,
            execution_id=execution_id,
            ingestion_ts=ts,
        )

    def to_object_metadata(self) -> dict[str, str]:
        """Map to S3/MinIO user-metadata string values."""
        return {
            "source_id": self.source_id,
            "execution_id": self.execution_id,
            "_ingestion_ts": _format_ingestion_ts(self.ingestion_ts),
        }
