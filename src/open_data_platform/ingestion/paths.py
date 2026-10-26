"""Raw object-key path builder for the lakehouse landing zone."""

from __future__ import annotations

import re
from datetime import date

_SOURCE_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
# UUID, Airflow-style run ids, and similar opaque tokens (no path separators).
_EXECUTION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]*$")
_OBJECT_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def _reject_path_junk(name: str, value: str) -> None:
    if not value or value.strip() != value:
        msg = f"{name} must be non-empty and have no leading/trailing whitespace"
        raise ValueError(msg)
    if "/" in value or "\\" in value or ".." in value:
        msg = f"{name} must not contain path separators or '..'"
        raise ValueError(msg)


def _validate_source_segment(name: str, value: str) -> str:
    _reject_path_junk(name, value)
    if not _SOURCE_SEGMENT_RE.fullmatch(value):
        msg = (
            f"{name} must be snake_case-like "
            f"(lowercase alphanumeric, underscore, hyphen); got {value!r}"
        )
        raise ValueError(msg)
    return value


def _validate_execution_id(execution_id: str) -> str:
    _reject_path_junk("execution_id", execution_id)
    if not _EXECUTION_ID_RE.fullmatch(execution_id):
        msg = f"execution_id has invalid characters: {execution_id!r}"
        raise ValueError(msg)
    return execution_id


def _validate_object_name(object_name: str) -> str:
    _reject_path_junk("object_name", object_name)
    if not _OBJECT_NAME_RE.fullmatch(object_name):
        msg = f"object_name has invalid characters: {object_name!r}"
        raise ValueError(msg)
    return object_name


def build_raw_object_key(
    *,
    source_id: str,
    dataset: str,
    execution_id: str,
    object_name: str,
    ingestion_date: date,
) -> str:
    """Build a deterministic raw object key.

    Layout::

        raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}

    The same arguments always produce the same key (idempotent overwrite target).
    """
    source = _validate_source_segment("source_id", source_id)
    ds = _validate_source_segment("dataset", dataset)
    exec_id = _validate_execution_id(execution_id)
    name = _validate_object_name(object_name)
    dt = ingestion_date.isoformat()
    return f"raw/{source}/{ds}/dt={dt}/execution_id={exec_id}/{name}"


def build_quarantine_object_key(
    *,
    source_id: str,
    dataset: str,
    execution_id: str,
    object_name: str,
    ingestion_date: date,
) -> str:
    """Build the quarantine key that belongs to one raw object.

    Layout::

        quarantine/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}.jsonl

    The same arguments always produce the same key. ``object_name`` is the
    raw object name; the ``.jsonl`` suffix marks the sidecar.
    """
    source = _validate_source_segment("source_id", source_id)
    ds = _validate_source_segment("dataset", dataset)
    exec_id = _validate_execution_id(execution_id)
    name = _validate_object_name(object_name)
    dt = ingestion_date.isoformat()
    return f"quarantine/{source}/{ds}/dt={dt}/execution_id={exec_id}/{name}.jsonl"
