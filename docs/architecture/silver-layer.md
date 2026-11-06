# Silver layer

Typed, deduplicated Iceberg tables built from [Bronze](bronze-layer.md). Decision context: [ADR-0012](../decisions/0012-medallion-layers.md) (still `proposed` until Gold exists). Bronze stays string-typed and keeps duplicate keys. This layer does not replace Bronze or raw objects.

**Write mode (P2-04):** `WriteMode.FULL_OVERWRITE` only — each live run rebuilds the whole Silver table (`DROP` + `CREATE` + `INSERT OVERWRITE`). Placeholders for `INCREMENTAL_MERGE` and `APPEND_ONLY` exist in `open_data_platform.lakehouse.write_mode` and fail if selected. Incremental/CDC commit paths are later work (P2-10 docs, P3 backfills, P4 CDC).

## Split by source

Bronze uses one generic promotion path. Silver is **one module per `source_id`** under `open_data_platform.lakehouse.silver`. Each module owns cast style, JSON expansion, and survivor rules for its datasets. Gold domains stay separate (`olist_analysis`, `product_analysis`); Silver tables remain keyed by source and dataset so Airflow (P3-03) can schedule sources independently. DAGs must call the package CLI; they must not reimplement transforms.

| Module | `source_id` |
| --- | --- |
| `olist_brazilian_ecommerce` | Olist e-commerce CSVs |
| `olist_marketing_funnel` | Olist funnel CSVs |
| `postgres_oltp` | Windowed PostgreSQL extracts |
| `dummyjson_products` | DummyJSON product pages (JSON expanded) |
| `demo_storefront` | Generated clickstream |
| `ibge_localidades` | IBGE municipalities |

## Table

One Iceberg table per contract, namespace `silver`, name `{source_id}__{dataset}`, format version 2, JDBC catalog from P2-02. Location:

```text
s3a://{ODP_WAREHOUSE_BUCKET}/silver/{source_id}/{dataset}
```

Spark writes. Trino reads `iceberg.silver.{table}`. `spark.sql.caseSensitive` stays true. Quote mixed-case, hyphenated, and reserved names in Trino. The Spark script drops and recreates the Silver table so schema changes (for example JSON expansion) apply on the next live run.

## Typing

Contract logical types become Iceberg types. Metadata columns (`source_id`, `execution_id`, `_ingestion_ts`, `raw_object_key`, `record_index`, `dt`) stay `STRING`.

| Contract | Iceberg |
| --- | --- |
| `string` | `STRING` |
| `integer` | `BIGINT` |
| `number` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `timestamp` | `TIMESTAMP` |
| `date` | `DATE` |
| `json` (default) | `STRING` unless the source module expands it |

Casts use Spark `TRY_*` helpers. A failed cast becomes null; the row remains if the partition key is still non-null. Olist CSV timestamps use `yyyy-MM-dd HH:mm:ss` first. ISO sources parse ISO-8601 text. Closed-deal booleans accept `True` / `False` text.

### DummyJSON JSON expansion

`dummyjson_products` only maps which nested fields expand. Shared Spark SQL helpers
(`timestamp_sql`, `json_path_*`, `json_array`, `json_typed`) live in
`lakehouse.silver.common` so other sources can reuse the same casts.

`dummyjson_products.products` replaces nested Bronze JSON strings with typed columns (product grain unchanged):

| Bronze field | Silver |
| --- | --- |
| `tags` | `ARRAY<STRING>` |
| `dimensions` | `dimensions_width`, `dimensions_height`, `dimensions_depth` (`DOUBLE`) |
| `meta` | `meta_created_at`, `meta_updated_at` (`TIMESTAMP`), `meta_barcode`, `meta_qr_code` (`STRING`) |
| `images` | `ARRAY<STRING>` |
| `reviews` | `ARRAY<STRUCT<rating, comment, date, reviewerName, reviewerEmail>>` |

## Deduplication

Default survivor: last valid row by `_ingestion_ts`, `execution_id`, `raw_object_key`, then numeric `record_index` (all descending). “Valid” means every partition-key column is non-null after casting.

| Dataset rule | Policy |
| --- | --- |
| Contract `primary_key` | `ROW_NUMBER` partitioned by those columns |
| `order_reviews` | Key is `(review_id, order_id)` (contract v2) |
| `geolocation` | No primary key. Collapse **identical payload rows** only; do not collapse by ZIP prefix |
| `postgres_oltp.*` | Order by `updated_at` first (overlapping extract windows) |
| `demo_storefront.clickstream` | Partition by `event_id`; order by `event_time` first |

The write is a full-table overwrite. There is no `MERGE` and no exactly-once claim.

## Live run vs unit fixtures

| Mode | Command | Data |
| --- | --- | --- |
| **Live run** | `make live-silver` | Every Silver module against existing Bronze tables from `odp-raw` |
| **Unit fixtures** | pytest + `lakehouse.fixtures` | Synthetic payloads only |

```bash
make live-bronze   # prerequisite: full Bronze from odp-raw
make live-silver
```

Expected Trino counts (and DummyJSON shape checks) live in `open_data_platform.lakehouse.live_counts`.

Per source:

```bash
uv run python -m open_data_platform.lakehouse.silver \
  --source demo_storefront \
  --sql-out .local/silver-sql
```

## Result (2026-11-05 full-data live)

`make live-silver` after the full Bronze live (`WriteMode.FULL_OVERWRITE`). Trino matched 22/22 checks (`SILVER_COUNTS` + shape): geolocation 738_332 after identical-row dedup; DummyJSON 194 with expanded `dimensions_width` / `meta_barcode` / `tags`; postgres_oltp survivors 2/2/2/3; clickstream null `product_id` 16. Chronological note: [status log](../status-log.md).

## Pins

Same Spark, Iceberg, JDBC, and Trino pins as [catalog and query](phase-2-catalog-query.md).

## Limitations

- `INSERT OVERWRITE` / table recreate leaves previous Parquet files in the bucket (P5-07).
- CI does not start Compose. Unit tests cover SQL text, policies, and expansions.
- D-012 stays `proposed` until Gold exists.
- Broader quality and reconciliation tests are P2-07; orchestration is Phase 3.
