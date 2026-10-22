"""Raw ingestion helpers and the Olist historical writer."""

from open_data_platform.ingestion.metadata import IngestionMetadata
from open_data_platform.ingestion.paths import build_raw_object_key
from open_data_platform.ingestion.writer import RawWriteResult, write_raw_object

__all__ = [
    "IngestionMetadata",
    "RawWriteResult",
    "build_raw_object_key",
    "fetch_olist_snapshots",
    "ingest_olist_historical",
    "run_olist_historical",
    "write_raw_object",
]

_OLIST_EXPORTS = frozenset(
    {
        "fetch_olist_snapshots",
        "ingest_olist_historical",
        "run_olist_historical",
    }
)


def __getattr__(name: str) -> object:
    """Load Olist callables on first use.

    Importing them at module level cycles through the contract loader, which
    itself imports ``ingestion.paths`` while this package is initializing.
    """
    if name not in _OLIST_EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    from open_data_platform.ingestion import olist as olist_module

    value = getattr(olist_module, name)
    globals()[name] = value
    return value
