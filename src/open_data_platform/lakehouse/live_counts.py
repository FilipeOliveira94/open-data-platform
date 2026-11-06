"""Expected Trino counts for full-data Bronze and Silver live runs.

These values pin the current ``odp-raw`` snapshot under ``execution_id=run-001``
(and the overlapping Postgres windows). They are **not** a growth contract.

Follow-up (P2-07): replace hardcoded snapshot sizes with counts derived from
Bronze/source for that run, especially for ``full_refresh`` and
``incremental_window`` datasets that change size over time. Until then,
editing this module is required when the live raw snapshot changes.
"""

from __future__ import annotations

# Bronze: one row per raw record (all executions/objects), STRING columns.
BRONZE_COUNTS: dict[str, int] = {
    "iceberg.bronze.olist_brazilian_ecommerce__customers": 99_441,
    "iceberg.bronze.olist_brazilian_ecommerce__geolocation": 1_000_163,
    "iceberg.bronze.olist_brazilian_ecommerce__order_items": 112_650,
    "iceberg.bronze.olist_brazilian_ecommerce__order_payments": 103_886,
    "iceberg.bronze.olist_brazilian_ecommerce__order_reviews": 99_224,
    "iceberg.bronze.olist_brazilian_ecommerce__orders": 99_441,
    "iceberg.bronze.olist_brazilian_ecommerce__products": 32_951,
    "iceberg.bronze.olist_brazilian_ecommerce__sellers": 3_095,
    "iceberg.bronze.olist_brazilian_ecommerce__product_category_name_translation": 71,
    "iceberg.bronze.olist_marketing_funnel__qualified_leads": 8_000,
    "iceberg.bronze.olist_marketing_funnel__closed_deals": 842,
    "iceberg.bronze.dummyjson_products__products": 194,
    "iceberg.bronze.demo_storefront__clickstream": 100,
    "iceberg.bronze.ibge_localidades__municipios": 5_571,
    # Two window objects per table (daily + weekly) share some keys.
    "iceberg.bronze.postgres_oltp__customers": 3,
    "iceberg.bronze.postgres_oltp__orders": 3,
    "iceberg.bronze.postgres_oltp__order_items": 3,
    "iceberg.bronze.postgres_oltp__sellers": 5,
}

# Silver: after typing, PK / payload dedup, and JSON expansion.
SILVER_COUNTS: dict[str, int] = {
    "iceberg.silver.olist_brazilian_ecommerce__customers": 99_441,
    # Exact payload-row collapse (not by ZIP prefix).
    "iceberg.silver.olist_brazilian_ecommerce__geolocation": 738_332,
    "iceberg.silver.olist_brazilian_ecommerce__order_items": 112_650,
    "iceberg.silver.olist_brazilian_ecommerce__order_payments": 103_886,
    "iceberg.silver.olist_brazilian_ecommerce__order_reviews": 99_224,
    "iceberg.silver.olist_brazilian_ecommerce__orders": 99_441,
    "iceberg.silver.olist_brazilian_ecommerce__products": 32_951,
    "iceberg.silver.olist_brazilian_ecommerce__sellers": 3_095,
    "iceberg.silver.olist_brazilian_ecommerce__product_category_name_translation": 71,
    "iceberg.silver.olist_marketing_funnel__qualified_leads": 8_000,
    "iceberg.silver.olist_marketing_funnel__closed_deals": 842,
    "iceberg.silver.dummyjson_products__products": 194,
    "iceberg.silver.demo_storefront__clickstream": 100,
    "iceberg.silver.ibge_localidades__municipios": 5_571,
    "iceberg.silver.postgres_oltp__customers": 2,
    "iceberg.silver.postgres_oltp__orders": 2,
    "iceberg.silver.postgres_oltp__order_items": 2,
    "iceberg.silver.postgres_oltp__sellers": 3,
}


def bronze_count_checks() -> tuple[tuple[str, str], ...]:
    """Expected count and Trino SQL for every Bronze live table."""
    return tuple(
        (str(count), f"SELECT COUNT(*) AS row_count FROM {table}")
        for table, count in sorted(BRONZE_COUNTS.items())
    )


def silver_count_checks() -> tuple[tuple[str, str], ...]:
    """Expected count and Trino SQL for every Silver live table."""
    return tuple(
        (str(count), f"SELECT COUNT(*) AS row_count FROM {table}")
        for table, count in sorted(SILVER_COUNTS.items())
    )


def silver_shape_checks() -> tuple[tuple[str, str], ...]:
    """Extra Silver checks for JSON expansion and intentional nulls."""
    products = "iceberg.silver.dummyjson_products__products"
    clickstream = "iceberg.silver.demo_storefront__clickstream"
    return (
        (
            "194",
            f"SELECT COUNT(*) AS row_count FROM {products} "
            "WHERE dimensions_width IS NOT NULL",
        ),
        (
            "194",
            f"SELECT COUNT(*) AS row_count FROM {products} "
            "WHERE meta_barcode IS NOT NULL",
        ),
        (
            "194",
            f"SELECT COUNT(*) AS row_count FROM {products} "
            "WHERE cardinality(tags) >= 1",
        ),
        (
            "16",
            f"SELECT COUNT(*) AS row_count FROM {clickstream} WHERE product_id IS NULL",
        ),
    )
