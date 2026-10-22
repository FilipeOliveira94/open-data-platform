"""Toolchain smoke tests (no external services)."""

from open_data_platform import __version__
from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion import ingest_olist_historical


def test_package_version_is_semver_like() -> None:
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_contract_loader_and_olist_export_import_together() -> None:
    assert callable(ingest_olist_historical)
    assert load_contracts()
