"""Raw ingestion helpers, the Olist writer, and DummyJSON products."""

from open_data_platform.ingestion.metadata import IngestionMetadata
from open_data_platform.ingestion.paths import build_raw_object_key
from open_data_platform.ingestion.writer import RawWriteResult, write_raw_object

__all__ = [
    "IngestionMetadata",
    "RawWriteResult",
    "build_raw_object_key",
    "fetch_olist_snapshots",
    "ingest_dummyjson_products",
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
_DUMMYJSON_PRODUCTS_EXPORTS = frozenset({"ingest_dummyjson_products"})


def __getattr__(name: str) -> object:
    """Load source writers on first use.

    Importing them at module level cycles through the contract loader, which
    itself imports ``ingestion.paths`` while this package is initializing.
    """
    if name in _OLIST_EXPORTS:
        from open_data_platform.ingestion import olist as source_module
    elif name in _DUMMYJSON_PRODUCTS_EXPORTS:
        from open_data_platform.ingestion import (
            dummyjson_products as source_module,
        )
    else:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = getattr(source_module, name)
    globals()[name] = value
    return value
