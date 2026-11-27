"""Silver rules for ``olist_synth_cdc``.

Overlapping extract windows can repeat a primary key. The business clock is
``updated_at``; the latest watermark wins before ingestion metadata.
"""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "olist_synth_cdc"

_WINDOW = DedupPolicy(
    order_by=("updated_at",),
    timestamp_style=TimestampStyle.ISO,
)

POLICIES: dict[str, DedupPolicy] = {
    "customers": _WINDOW,
    "orders": _WINDOW,
    "order_items": _WINDOW,
    "sellers": _WINDOW,
}
