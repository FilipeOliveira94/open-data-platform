# Bronze layer

Near-source Iceberg tables built from the raw objects in [raw storage layout](raw-storage-layout.md). Decision context: [ADR-0012](../decisions/0012-medallion-layers.md) (still `proposed`) and [ADR-0020](../decisions/0020-raw-storage-layout.md). Silver typing and deduplication are P2-04. This layer does not replace the raw objects.

## Table

One Iceberg table per contract, in the namespace `bronze`. The table name is `{source_id}__{dataset}`. Format version is 2. The JDBC catalog and warehouse URI stay the ones verified in P2-02 (`s3a://{ODP_WAREHOUSE_BUCKET}/catalog`, catalog name `local`, schema `V1`). Each table sets its own location:

```text
s3a://{ODP_WAREHOUSE_BUCKET}/bronze/{source_id}/{dataset}
```

Spark `3.5.8-java17` writes. Trino `484` reads `iceberg.bronze.{table}`. `spark.sql.caseSensitive` is true for this job so mixed-case and hyphenated contract names stay as written. Quote those names in Trino, and quote reserved words such as `"skip"` and `"limit"`.

## Grain and columns

The grain is the contract grain: one CSV row, one JSONL line, one JSON array element, or one element of the DummyJSON `products` array. `total`, `skip`, and `limit` are repeated on each product row. The `products` array is not a column.

Every value is Iceberg `STRING`. A logical `integer`, `number`, `boolean`, `timestamp`, or `date` is stored as text. Nested `json` is compact JSON text. An empty CSV cell, a JSON null, and an omitted nullable key become null. A value of the wrong logical type stays in the row. Duplicate primary keys stay in the row. The contract `primary_key` is not an Iceberg identifier and is not unique here.

Columns, in order:

| Column | Source |
| --- | --- |
| `source_id` | Object user-metadata. Must match the key. |
| `execution_id` | Object user-metadata. Must match the key. |
| `_ingestion_ts` | Object user-metadata, unchanged. |
| `raw_object_key` | Full raw key. |
| `record_index` | Zero-based index inside that object. |
| `dt` | `dt=` segment of the raw key. A column, not an Iceberg partition. |
| Contract fields | Contract order. |
| Envelope fields | Page fields other than the `record_locator`, in contract order. |

`dt` is the ingestion date already used to lay out raw objects. The write replaces the whole table, so there is no Iceberg partition spec in this issue.

Quarantine objects are not read. Invalid rows that remain inside a raw object are promoted with it. A payload that does not match the contract shape fails that dataset before the table is overwritten.

## Write

Each run reads every object under `raw/{source_id}/{dataset}/`. Keys are sorted. After every object parses, the job writes one staging file and then Spark commits one overwrite per table:

```text
s3a://{ODP_WAREHOUSE_BUCKET}/bronze/_staging/{source_id}/{dataset}/rows.jsonl
```

Staging is a build artifact. The auditable bytes stay under `raw/`. An empty prefix fails and does not delete the table. A second run with the same objects produces the same rows. Several `execution_id` values all remain. There is no `MERGE` and no exactly-once claim.

An empty JSON array is a successful object with zero records. The table overwrite then commits zero rows. PostgreSQL can land that array for an empty window. A prefix with no objects is a different case and fails.

## Command

`docker compose up` does not start Spark or Trino. PostgreSQL and MinIO must be healthy. The catalog database `iceberg` is created when it is missing.

```bash
make test-bronze
```

The target writes four synthetic objects into the bucket `odp-bronze-live`, one for each payload shape, promotes those datasets with one Spark invocation each, and checks the Trino counts. It does not read or write `ODP_RAW_BUCKET` from `.env`. Run it again to overwrite the same four tables and read the same counts. The fixture helper is `open_data_platform.lakehouse.fixtures`.

| Payload | Dataset | Execution | Rows |
| --- | --- | --- | --- |
| CSV, with a leading BOM | `olist_brazilian_ecommerce.product_category_name_translation` | `p203-csv` | 3, including one repeated category name and one empty English label |
| JSONL | `demo_storefront.clickstream` | `p203-jsonl` | 100 actions from seed 7 |
| JSON array | `ibge_localidades.municipios` | `p203-json-array` | 2, one with null microregion and mesoregion |
| JSON page | `dummyjson_products.products` | `p203-json-page` | 2 products, one without `brand` |

These fixtures are not the published Olist file, the IBGE municipality list, or a DummyJSON response. The clickstream body is the repository generator. To promote objects that are already in the configured raw bucket:

```bash
uv run python -m open_data_platform.lakehouse.bronze \
  --dataset demo_storefront/clickstream \
  --sql-out .local/bronze-sql
```

Then run the shared Spark runtime once per file in that directory. Mount the file at `/tmp/bronze.sql` and set `ODP_SPARK_SQL` to that path. `--all` promotes every contract and fails if any prefix is empty. Each Spark process loads one dataset, so a failed dataset leaves the others at the snapshot written by their own invocation.

## Result (2026-11-01)

`make test-bronze` ran again with one Spark process per dataset. Spark printed `100`, `2`, `2`, and `3` for clickstream, products, municipalities, and the category CSV, in the alphabetical order of the SQL files. Trino matched the same 18 checks.

## Result (2026-10-31)

`make test-bronze` completed on WSL2. Spark printed `3`, `100`, `2`, and `2`. Trino matched 18 checks:

| Check | Count |
| --- | --- |
| Category CSV rows, and rows with the live `_ingestion_ts` | 3 |
| Repeated `categoria_teste` | 2 |
| Null English label, `test_category`, and `test_category_again` | 1 each |
| Clickstream rows, and rows with the live execution metadata | 100 |
| Null `product_id` | 16 |
| Municipality rows, and rows with the live execution metadata | 2 |
| `São Teste`, and a null `microrregiao-id` | 1 each |
| Product rows, tags text `["demo"]`, envelope `total`/`skip`/`limit`, and live execution metadata | 2 |
| Null `brand` | 1 |

Earlier Spark overwrites the same evening printed the same four table counts. `docker compose config --services` listed `minio` and `postgres` only. `uv run pytest tests/unit` → 245 passed in 10.96s.

## Pins

The Spark image, Iceberg runtime, S3A jars, PostgreSQL JDBC driver, JDBC schema `V1`, and Trino image are the P2-02 pins. See [catalog and query](phase-2-catalog-query.md).

## Limitations

- `INSERT OVERWRITE` leaves the previous Parquet files in the bucket. Expiring snapshots and deleting orphan files is P5-07.
- The first `CREATE TABLE` fixes the table location. A later run does not move it.
- Logical contract types are not cast. That cast is P2-04.
- `make test-bronze` overwrites the four Bronze tables named above. It does not delete raw objects in the Phase 1 bucket.
- Each Spark run downloads the pinned jars. The container is removed afterwards.
- CI does not start Compose. Unit tests cover parsing, staging, and the SQL text.
- D-012 stays `proposed`.
