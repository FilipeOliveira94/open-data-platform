"""Silver rules for ``demo_storefront``."""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "demo_storefront"

POLICIES: dict[str, DedupPolicy] = {
    "clickstream": DedupPolicy(
        order_by=("event_time",),
        timestamp_style=TimestampStyle.ISO,
    ),
}
