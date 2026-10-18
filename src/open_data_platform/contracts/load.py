"""Load YAML dataset contracts and check their structure.

Payload bytes are not validated here. Quarantine and batch failure are
declared on the contract and enforced by later ingestion issues.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml

from open_data_platform.contracts.model import (
    CLASSIFICATIONS,
    FIELD_TYPES,
    GOLD_DOMAINS,
    ON_INVALID_POLICIES,
    PAYLOAD_FORMATS,
    PROVENANCE_SYSTEMS,
    UPDATE_PATTERNS,
    ChangelogEntry,
    ContractError,
    ContractField,
    DataContract,
)
from open_data_platform.ingestion.paths import build_raw_object_key

_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "contract_version",
        "source_id",
        "dataset",
        "description",
        "payload_format",
        "grain",
        "primary_key",
        "fields",
        "on_invalid",
        "on_structural_error",
        "update_pattern",
        "governance",
        "provenance",
        "changelog",
        "record_locator",
        "envelope_fields",
    }
)
_FIELD_KEYS: frozenset[str] = frozenset(
    {"name", "type", "nullable", "classification", "description"}
)
_GOVERNANCE_KEYS: frozenset[str] = frozenset({"domain", "owner"})
_PROVENANCE_KEYS: frozenset[str] = frozenset({"system"})
_CHANGELOG_KEYS: frozenset[str] = frozenset({"version", "note"})
_REQUIRED_TOP_LEVEL: frozenset[str] = _TOP_LEVEL_KEYS - {
    "record_locator",
    "envelope_fields",
}


def default_contracts_dir() -> Path:
    """Find ``config/contracts`` by walking parents of this module.

    Contracts stay repository files. They are not packaged into the wheel.
    """
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "config" / "contracts"
        if candidate.is_dir():
            return candidate
    msg = "config/contracts was not found by walking parents of the contracts package"
    raise ContractError(msg)


def load_contracts(directory: Path | None = None) -> tuple[DataContract, ...]:
    """Load every ``.yaml`` contract under ``directory`` (default: repo config)."""
    root = default_contracts_dir() if directory is None else directory
    if not root.is_dir():
        msg = f"contracts directory does not exist: {root}"
        raise ContractError(msg)

    contracts: list[DataContract] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.name == "README.md" and path.parent == root:
            continue
        if path.suffix != ".yaml":
            msg = f"unexpected file in contracts tree: {path}"
            raise ContractError(msg)
        contracts.append(load_contract_file(path))

    if not contracts:
        msg = f"no contracts found under {root}"
        raise ContractError(msg)

    identities = [(item.source_id, item.dataset) for item in contracts]
    if len(identities) != len(set(identities)):
        msg = "duplicate source_id/dataset among contracts"
        raise ContractError(msg)
    return tuple(contracts)


def load_contract_file(path: Path) -> DataContract:
    """Load one contract and require the path to match ``source_id``/``dataset``."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"cannot read contract {path}: {exc}"
        raise ContractError(msg) from exc
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        msg = f"{path}: invalid YAML: {exc}"
        raise ContractError(msg) from exc
    if not isinstance(loaded, dict):
        msg = f"{path}: contract must be a mapping"
        raise ContractError(msg)

    contract = contract_from_mapping(loaded, origin=str(path))
    if path.parent.name != contract.source_id:
        msg = (
            f"{path}: parent directory {path.parent.name!r} "
            f"must equal source_id {contract.source_id!r}"
        )
        raise ContractError(msg)
    if path.stem != contract.dataset:
        msg = f"{path}: file stem {path.stem!r} must equal dataset {contract.dataset!r}"
        raise ContractError(msg)
    return contract


def contract_from_mapping(data: dict[str, Any], *, origin: str) -> DataContract:
    """Validate a decoded mapping and return a contract."""
    _reject_unknown(data, _TOP_LEVEL_KEYS, origin, "contract")
    missing = _REQUIRED_TOP_LEVEL - data.keys()
    if missing:
        names = ", ".join(sorted(missing))
        msg = f"{origin}: missing keys: {names}"
        raise ContractError(msg)

    record_locator = data.get("record_locator")
    envelope_present = "envelope_fields" in data
    if record_locator is None and envelope_present:
        msg = f"{origin}: envelope_fields requires record_locator"
        raise ContractError(msg)
    if record_locator is not None and not envelope_present:
        msg = f"{origin}: record_locator requires envelope_fields"
        raise ContractError(msg)

    fields = _parse_fields(data["fields"], origin=origin, label="fields")
    envelope_fields: tuple[ContractField, ...] = ()
    parsed_locator: str | None = None
    if record_locator is not None:
        parsed_locator = _non_empty_str(record_locator, origin, "record_locator")
        envelope_fields = _parse_fields(
            data["envelope_fields"],
            origin=origin,
            label="envelope_fields",
        )

    field_names = [field.name for field in fields]
    primary_key = _parse_primary_key(data["primary_key"], field_names, origin)
    changelog = _parse_changelog(data["changelog"], origin)
    version = _positive_int(data["contract_version"], origin, "contract_version")
    if version not in {entry.version for entry in changelog}:
        msg = f"{origin}: changelog must include contract_version {version}"
        raise ContractError(msg)

    on_structural_error = _choice(
        data["on_structural_error"],
        frozenset({"fail_batch"}),
        origin,
        "on_structural_error",
    )
    governance = _mapping(data["governance"], origin, "governance")
    _reject_unknown(governance, _GOVERNANCE_KEYS, origin, "governance")
    _require_keys(governance, _GOVERNANCE_KEYS, origin, "governance")
    provenance = _mapping(data["provenance"], origin, "provenance")
    _reject_unknown(provenance, _PROVENANCE_KEYS, origin, "provenance")
    _require_keys(provenance, _PROVENANCE_KEYS, origin, "provenance")

    contract = DataContract(
        contract_version=version,
        source_id=_non_empty_str(data["source_id"], origin, "source_id"),
        dataset=_non_empty_str(data["dataset"], origin, "dataset"),
        description=_non_empty_str(data["description"], origin, "description"),
        payload_format=_choice(
            data["payload_format"], PAYLOAD_FORMATS, origin, "payload_format"
        ),
        grain=_non_empty_str(data["grain"], origin, "grain"),
        primary_key=primary_key,
        fields=fields,
        on_invalid=_choice(
            data["on_invalid"], ON_INVALID_POLICIES, origin, "on_invalid"
        ),
        on_structural_error=on_structural_error,
        update_pattern=_choice(
            data["update_pattern"], UPDATE_PATTERNS, origin, "update_pattern"
        ),
        domain=_choice(governance["domain"], GOLD_DOMAINS, origin, "governance.domain"),
        owner=_non_empty_str(governance["owner"], origin, "governance.owner"),
        provenance_system=_choice(
            provenance["system"],
            PROVENANCE_SYSTEMS,
            origin,
            "provenance.system",
        ),
        changelog=changelog,
        record_locator=parsed_locator,
        envelope_fields=envelope_fields,
    )
    _assert_raw_path_segments(contract, origin)
    return contract


def _assert_raw_path_segments(contract: DataContract, origin: str) -> None:
    suffix = "csv" if contract.payload_format == "csv" else "json"
    try:
        build_raw_object_key(
            source_id=contract.source_id,
            dataset=contract.dataset,
            execution_id="contract-check",
            object_name=f"part.{suffix}",
            ingestion_date=date(2026, 1, 1),
        )
    except ValueError as exc:
        msg = f"{origin}: source_id/dataset are not valid raw path segments: {exc}"
        raise ContractError(msg) from exc


def _parse_fields(
    value: object,
    *,
    origin: str,
    label: str,
) -> tuple[ContractField, ...]:
    if not isinstance(value, list) or not value:
        msg = f"{origin}: {label} must be a non-empty list"
        raise ContractError(msg)
    fields: list[ContractField] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        item_origin = f"{origin}: {label}[{index}]"
        mapping = _mapping(item, item_origin, "field")
        _reject_unknown(mapping, _FIELD_KEYS, item_origin, "field")
        missing = _FIELD_KEYS - mapping.keys()
        if missing:
            names = ", ".join(sorted(missing))
            msg = f"{item_origin}: missing keys: {names}"
            raise ContractError(msg)
        name = _non_empty_str(mapping["name"], item_origin, "name")
        if name in seen:
            msg = f"{item_origin}: duplicate field name {name!r}"
            raise ContractError(msg)
        seen.add(name)
        fields.append(
            ContractField(
                name=name,
                field_type=_choice(mapping["type"], FIELD_TYPES, item_origin, "type"),
                nullable=_bool(mapping["nullable"], item_origin, "nullable"),
                classification=_choice(
                    mapping["classification"],
                    CLASSIFICATIONS,
                    item_origin,
                    "classification",
                ),
                description=_non_empty_str(
                    mapping["description"], item_origin, "description"
                ),
            )
        )
    return tuple(fields)


def _parse_primary_key(
    value: object,
    field_names: list[str],
    origin: str,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        msg = f"{origin}: primary_key must be a list"
        raise ContractError(msg)
    keys: list[str] = []
    allowed = set(field_names)
    for index, item in enumerate(value):
        key = _non_empty_str(item, origin, f"primary_key[{index}]")
        if key not in allowed:
            msg = f"{origin}: primary_key field {key!r} is not in fields"
            raise ContractError(msg)
        if key in keys:
            msg = f"{origin}: duplicate primary_key field {key!r}"
            raise ContractError(msg)
        keys.append(key)
    return tuple(keys)


def _parse_changelog(value: object, origin: str) -> tuple[ChangelogEntry, ...]:
    if not isinstance(value, list) or not value:
        msg = f"{origin}: changelog must be a non-empty list"
        raise ContractError(msg)
    entries: list[ChangelogEntry] = []
    seen: set[int] = set()
    for index, item in enumerate(value):
        item_origin = f"{origin}: changelog[{index}]"
        mapping = _mapping(item, item_origin, "changelog entry")
        _reject_unknown(mapping, _CHANGELOG_KEYS, item_origin, "changelog entry")
        missing = _CHANGELOG_KEYS - mapping.keys()
        if missing:
            names = ", ".join(sorted(missing))
            msg = f"{item_origin}: missing keys: {names}"
            raise ContractError(msg)
        version = _positive_int(mapping["version"], item_origin, "version")
        if version in seen:
            msg = f"{item_origin}: duplicate changelog version {version}"
            raise ContractError(msg)
        seen.add(version)
        entries.append(
            ChangelogEntry(
                version=version,
                note=_non_empty_str(mapping["note"], item_origin, "note"),
            )
        )
    return tuple(entries)


def _require_keys(
    mapping: dict[str, Any],
    required: frozenset[str],
    origin: str,
    label: str,
) -> None:
    missing = required - mapping.keys()
    if missing:
        names = ", ".join(sorted(missing))
        msg = f"{origin}: missing {label} keys: {names}"
        raise ContractError(msg)


def _reject_unknown(
    mapping: dict[str, Any],
    allowed: frozenset[str],
    origin: str,
    label: str,
) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        names = ", ".join(sorted(unknown))
        msg = f"{origin}: unknown {label} keys: {names}"
        raise ContractError(msg)


def _mapping(value: object, origin: str, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        msg = f"{origin}: {label} must be a mapping"
        raise ContractError(msg)
    return value


def _non_empty_str(value: object, origin: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.strip() != value:
        msg = f"{origin}: {label} must be a non-empty string"
        raise ContractError(msg)
    return value


def _bool(value: object, origin: str, label: str) -> bool:
    if not isinstance(value, bool):
        msg = f"{origin}: {label} must be a boolean"
        raise ContractError(msg)
    return value


def _positive_int(value: object, origin: str, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = f"{origin}: {label} must be an integer >= 1"
        raise ContractError(msg)
    return value


def _choice(value: object, allowed: frozenset[str], origin: str, label: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        options = ", ".join(sorted(allowed))
        msg = f"{origin}: {label} must be one of: {options}"
        raise ContractError(msg)
    return value
