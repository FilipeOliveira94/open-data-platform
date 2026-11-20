"""Silver rules for ``demo_storefront``.

The clickstream contract is ``append_only``: every complete batch adds
events, and a repeated ``event_id`` keeps the first ingested row (events are
immutable). ``event_time`` stays the event clock; Gold places a late event by
it, not by ingestion time.
"""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "demo_storefront"

POLICIES: dict[str, DedupPolicy] = {
    "clickstream": DedupPolicy(timestamp_style=TimestampStyle.ISO),
}
