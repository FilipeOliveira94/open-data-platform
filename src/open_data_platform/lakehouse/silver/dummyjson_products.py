"""Silver rules for ``dummyjson_products``.

Declares which nested Bronze JSON fields expand into typed columns.
Cast/SQL helpers live in ``silver.common``; this module only maps field names.
Product grain is unchanged: one Silver row per product ``id``.
"""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import (
    DedupPolicy,
    JsonExpansion,
    OutputColumn,
    TimestampStyle,
    json_array,
    json_path_double,
    json_path_string,
    json_path_timestamp,
    json_typed,
    quote_ident,
)
from open_data_platform.lakehouse.write_mode import CURRENT_WRITE_MODE

SOURCE_ID = "dummyjson_products"

_DIM = quote_ident("dimensions")
_META = quote_ident("meta")
_TAGS = quote_ident("tags")
_IMAGES = quote_ident("images")
_REVIEWS = quote_ident("reviews")

_REVIEW_TYPE = (
    "ARRAY<STRUCT<"
    "rating:INT,"
    "comment:STRING,"
    "date:TIMESTAMP,"
    "reviewerName:STRING,"
    "reviewerEmail:STRING"
    ">>"
)

PRODUCTS_EXPANSIONS: tuple[JsonExpansion, ...] = (
    JsonExpansion(
        source_field="tags",
        columns=(
            OutputColumn(
                name="tags",
                iceberg_type="ARRAY<STRING>",
                select_sql=json_array(_TAGS, "STRING"),
            ),
        ),
    ),
    JsonExpansion(
        source_field="dimensions",
        columns=(
            OutputColumn(
                name="dimensions_width",
                iceberg_type="DOUBLE",
                select_sql=json_path_double(_DIM, "$.width"),
            ),
            OutputColumn(
                name="dimensions_height",
                iceberg_type="DOUBLE",
                select_sql=json_path_double(_DIM, "$.height"),
            ),
            OutputColumn(
                name="dimensions_depth",
                iceberg_type="DOUBLE",
                select_sql=json_path_double(_DIM, "$.depth"),
            ),
        ),
    ),
    JsonExpansion(
        source_field="reviews",
        columns=(
            OutputColumn(
                name="reviews",
                iceberg_type=_REVIEW_TYPE,
                select_sql=json_typed(_REVIEWS, _REVIEW_TYPE),
            ),
        ),
    ),
    JsonExpansion(
        source_field="meta",
        columns=(
            OutputColumn(
                name="meta_created_at",
                iceberg_type="TIMESTAMP",
                select_sql=json_path_timestamp(_META, "$.createdAt"),
            ),
            OutputColumn(
                name="meta_updated_at",
                iceberg_type="TIMESTAMP",
                select_sql=json_path_timestamp(_META, "$.updatedAt"),
            ),
            OutputColumn(
                name="meta_barcode",
                iceberg_type="STRING",
                select_sql=json_path_string(_META, "$.barcode"),
            ),
            OutputColumn(
                name="meta_qr_code",
                iceberg_type="STRING",
                select_sql=json_path_string(_META, "$.qrCode"),
            ),
        ),
    ),
    JsonExpansion(
        source_field="images",
        columns=(
            OutputColumn(
                name="images",
                iceberg_type="ARRAY<STRING>",
                select_sql=json_array(_IMAGES, "STRING"),
            ),
        ),
    ),
)

POLICIES: dict[str, DedupPolicy] = {
    "products": DedupPolicy(
        timestamp_style=TimestampStyle.ISO,
        json_expansions=PRODUCTS_EXPANSIONS,
        write_mode=CURRENT_WRITE_MODE,
    ),
}
