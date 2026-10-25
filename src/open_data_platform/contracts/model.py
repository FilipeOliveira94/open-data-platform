"""Structural model for dataset contracts.

Contracts declare schema, null rules, classification and provenance.
They do not validate source payloads and they do not store a lineage graph.
"""

from __future__ import annotations

from dataclasses import dataclass

FIELD_TYPES: frozenset[str] = frozenset(
    {"string", "integer", "number", "boolean", "timestamp", "date", "json"}
)
CLASSIFICATIONS: frozenset[str] = frozenset({"public", "internal", "pii"})
CLASSIFICATION_RANK: dict[str, int] = {"public": 0, "internal": 1, "pii": 2}
PAYLOAD_FORMATS: frozenset[str] = frozenset({"csv", "json", "jsonl"})
ON_INVALID_POLICIES: frozenset[str] = frozenset({"quarantine", "fail_batch"})
UPDATE_PATTERNS: frozenset[str] = frozenset(
    {"snapshot", "full_refresh", "incremental_window"}
)
GOLD_DOMAINS: frozenset[str] = frozenset({"olist_analysis", "product_analysis"})
PROVENANCE_SYSTEMS: frozenset[str] = frozenset(
    {"kaggle", "dummyjson", "postgres", "ibge", "generator"}
)


class ContractError(ValueError):
    """A contract document failed structural validation."""


@dataclass(frozen=True, slots=True)
class ContractField:
    """One expected field on a record or JSON envelope."""

    name: str
    field_type: str
    nullable: bool
    classification: str
    description: str


@dataclass(frozen=True, slots=True)
class ChangelogEntry:
    """One documented contract revision."""

    version: int
    note: str


@dataclass(frozen=True, slots=True)
class DataContract:
    """Explicit contract for one ``source_id`` / ``dataset`` pair."""

    contract_version: int
    source_id: str
    dataset: str
    description: str
    payload_format: str
    grain: str
    primary_key: tuple[str, ...]
    fields: tuple[ContractField, ...]
    on_invalid: str
    on_structural_error: str
    update_pattern: str
    domain: str
    owner: str
    provenance_system: str
    changelog: tuple[ChangelogEntry, ...]
    record_locator: str | None = None
    envelope_fields: tuple[ContractField, ...] = ()

    @property
    def classification(self) -> str:
        """Strictest field classification on the record and its envelope."""
        present = [
            field.classification for field in (*self.envelope_fields, *self.fields)
        ]
        return max(present, key=CLASSIFICATION_RANK.__getitem__)
