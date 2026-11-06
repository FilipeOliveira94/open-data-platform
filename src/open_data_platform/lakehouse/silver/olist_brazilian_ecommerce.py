"""Silver rules for ``olist_brazilian_ecommerce``."""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "olist_brazilian_ecommerce"

# Snapshot CSVs. order_reviews uses contract PK (review_id, order_id).
# geolocation has no PK: collapse identical payload rows only.
POLICIES: dict[str, DedupPolicy] = {
    "customers": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "geolocation": DedupPolicy(
        partition_keys=(),
        partition_payload_fields=True,
        timestamp_style=TimestampStyle.OLIST_CSV,
    ),
    "order_items": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "order_payments": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "order_reviews": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "orders": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "products": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "sellers": DedupPolicy(timestamp_style=TimestampStyle.OLIST_CSV),
    "product_category_name_translation": DedupPolicy(
        timestamp_style=TimestampStyle.OLIST_CSV,
    ),
}
