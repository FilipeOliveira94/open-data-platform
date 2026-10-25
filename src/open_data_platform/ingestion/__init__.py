"""Raw ingestion helpers and the Phase 1 source writers."""

from open_data_platform.ingestion.metadata import IngestionMetadata
from open_data_platform.ingestion.paths import build_raw_object_key
from open_data_platform.ingestion.writer import RawWriteResult, write_raw_object

__all__ = [
    "IngestionMetadata",
    "RawWriteResult",
    "build_raw_object_key",
    "extract_postgres_oltp",
    "fetch_olist_snapshots",
    "ingest_demo_storefront",
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
_POSTGRES_OLTP_EXPORTS = frozenset({"extract_postgres_oltp"})
_DEMO_STOREFRONT_EXPORTS = frozenset({"ingest_demo_storefront"})


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
    elif name in _POSTGRES_OLTP_EXPORTS:
        from open_data_platform.ingestion import postgres_oltp as source_module
    elif name in _DEMO_STOREFRONT_EXPORTS:
        from open_data_platform.ingestion import demo_storefront as source_module
    else:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = getattr(source_module, name)
    globals()[name] = value
    return value
