"""Silver rules for ``olist_marketing_funnel``."""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "olist_marketing_funnel"

POLICIES: dict[str, DedupPolicy] = {
    "qualified_leads": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "closed_deals": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
}
