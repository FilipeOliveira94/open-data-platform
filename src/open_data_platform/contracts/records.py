"""Classify records against a contract. This module does not read or write storage."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from open_data_platform.contracts.model import ContractField, DataContract

NULL_VIOLATION = "null_violation"
TYPE_MISMATCH = "type_mismatch"
DUPLICATE_PRIMARY_KEY = "duplicate_primary_key"

_INTEGER_RE = re.compile(r"-?(0|[1-9][0-9]*)\Z")
_NUMBER_RE = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?\Z")
_TEXT_TYPES = frozenset({"string", "timestamp", "date"})


class PayloadStructureError(ValueError):
    """A payload cannot be read as records for its contract."""


@dataclass(frozen=True, slots=True)
class InvalidRecord:
    """One contract violation inside an otherwise readable payload."""

    record_index: int
    reason: str
    field: str | None
    primary_key: dict[str, object]
    record: dict[str, object]


def classify_records(
    contract: DataContract,
    records: Sequence[Mapping[str, object]],
) -> tuple[InvalidRecord, ...]:
    """Return null, type, and duplicate findings. The input records stay unchanged.

    A null primary-key value is a null violation and is not grouped as a
    duplicate. When a key repeats, every row in that group is reported.
    An empty ``primary_key`` skips the duplicate check. Nullable nulls are
    valid. ``timestamp`` and ``date`` are non-empty strings, with no
    calendar check.
    """
    pk_names = frozenset(contract.primary_key)
    findings: list[InvalidRecord] = []
    snapshots: list[dict[str, object]] = []
    groups: dict[tuple[object, ...], list[int]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            msg = f"record {index} must be an object"
            raise PayloadStructureError(msg)
        snapshot = dict(record)
        snapshots.append(snapshot)
        pk_ok = True
        for field in contract.fields:
            value = snapshot.get(field.name)
            missing = field.name not in snapshot or value is None
            if missing and (not field.nullable or field.name in pk_names):
                findings.append(
                    _finding(index, NULL_VIOLATION, field.name, contract, snapshot)
                )
                if field.name in pk_names:
                    pk_ok = False
                continue
            if missing:
                continue
            if not _matches(field, value):
                findings.append(
                    _finding(index, TYPE_MISMATCH, field.name, contract, snapshot)
                )
                if field.name in pk_names:
                    pk_ok = False
        if contract.primary_key and pk_ok:
            token = tuple(snapshot[name] for name in contract.primary_key)
            try:
                hash(token)
            except TypeError:
                continue
            groups.setdefault(token, []).append(index)
    duplicated = {
        index for indexes in groups.values() if len(indexes) > 1 for index in indexes
    }
    for index, snapshot in enumerate(snapshots):
        if index not in duplicated:
            continue
        findings.append(
            _finding(index, DUPLICATE_PRIMARY_KEY, None, contract, snapshot)
        )
    return tuple(findings)


def parsed_csv_record(
    contract: DataContract,
    cells: Mapping[str, str],
) -> dict[str, object]:
    """Convert one CSV row into values the classifier understands.

    An empty cell is null. Integer, number, and boolean cells that do not
    match their token stay as the original text so the classifier can report
    a type mismatch. ``timestamp`` and ``date`` stay text.
    """
    return {
        field.name: _csv_cell(field, cells[field.name]) for field in contract.fields
    }


def load_jsonl_records(body: bytes) -> tuple[dict[str, object], ...]:
    """Parse UTF-8 JSONL into objects. A bad line is a structural error."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        msg = "JSONL payload is not UTF-8"
        raise PayloadStructureError(msg) from exc
    records: list[dict[str, object]] = []
    for index, line in enumerate(text.splitlines()):
        if line == "":
            msg = f"JSONL line {index} is empty"
            raise PayloadStructureError(msg)
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            msg = f"JSONL line {index} is not JSON"
            raise PayloadStructureError(msg) from exc
        if not isinstance(item, dict):
            msg = f"JSONL line {index} must be a JSON object"
            raise PayloadStructureError(msg)
        records.append(item)
    return tuple(records)


def encode_findings(findings: Sequence[InvalidRecord]) -> bytes:
    """Encode findings as JSONL. Each object keeps a fixed key order."""
    lines = [
        json.dumps(
            {
                "record_index": item.record_index,
                "reason": item.reason,
                "field": item.field,
                "primary_key": item.primary_key,
                "record": item.record,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        for item in findings
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _finding(
    index: int,
    reason: str,
    field: str | None,
    contract: DataContract,
    record: dict[str, object],
) -> InvalidRecord:
    primary_key = {name: record.get(name) for name in contract.primary_key}
    return InvalidRecord(
        record_index=index,
        reason=reason,
        field=field,
        primary_key=primary_key,
        record=record,
    )


def _matches(field: ContractField, value: object) -> bool:
    if field.field_type == "string":
        return isinstance(value, str)
    if field.field_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if field.field_type == "number":
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        return isinstance(value, float) and _finite(value)
    if field.field_type == "boolean":
        return isinstance(value, bool)
    if field.field_type in _TEXT_TYPES:
        return isinstance(value, str) and value != ""
    if field.field_type == "json":
        return isinstance(value, dict | list)
    return False


def _finite(value: float) -> bool:
    return value == value and value not in {float("inf"), float("-inf")}


def _csv_cell(field: ContractField, text: str) -> object:
    if text == "":
        return None
    if field.field_type in _TEXT_TYPES:
        return text
    if field.field_type == "integer" and _INTEGER_RE.fullmatch(text):
        return int(text)
    if field.field_type == "number" and _NUMBER_RE.fullmatch(text):
        if "." in text:
            return float(text)
        return int(text)
    if field.field_type == "boolean" and text in {"true", "false", "True", "False"}:
        return text in {"true", "True"}
    return text
