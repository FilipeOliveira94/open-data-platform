# Bronze layer

Near-source Iceberg tables built from the raw objects in [raw storage layout](raw-storage-layout.md). Decision context: [ADR-0012](../decisions/0012-medallion-layers.md) (still `proposed`) and [ADR-0020](../decisions/0020-raw-storage-layout.md). Silver typing, JSON expansion, and deduplication are documented in [silver layer](silver-layer.md). This layer does not replace the raw objects.

**Write mode (P2-03):** `WriteMode.FULL_OVERWRITE` only — each run overwrites the whole Bronze table from every object under that dataset’s raw prefix. Placeholders for incremental MERGE / append live in `open_data_platform.lakehouse.write_mode` for later phases.

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

Each run reads every object under `raw/{source_id}/{dataset}/` in `ODP_RAW_BUCKET` (default `odp-raw`). Keys are sorted. After every object parses, the job writes one staging file and then Spark commits one overwrite per table:

```text
s3a://{ODP_WAREHOUSE_BUCKET}/bronze/_staging/{source_id}/{dataset}/rows.jsonl
```

Staging is a build artifact in the warehouse bucket. The auditable bytes stay under `raw/`. An empty prefix fails and does not delete the table. A second run with the same objects produces the same rows. Several `execution_id` values all remain. There is no `MERGE` and no exactly-once claim.

An empty JSON array is a successful object with zero records. The table overwrite then commits zero rows. PostgreSQL can land that array for an empty window. A prefix with no objects is a different case and fails.

## Live run vs unit fixtures

| Mode | Command | Data |
| --- | --- | --- |
| **Live run** | `make live-bronze` | Every contract from `ODP_RAW_BUCKET` (default `odp-raw`) |
| **Unit fixtures** | `open_data_platform.lakehouse.fixtures` via pytest / `--prepare-unit-fixtures` | Synthetic objects for deterministic unit tests only |

The live run does **not** use a separate fixture bucket. Buckets in normal local use are `odp-raw` (landing) and `odp-warehouse` (Iceberg). `odp-smoke` remains the Phase 0 MinIO smoke bucket.

```bash
make live-bronze
```

PostgreSQL and MinIO must be healthy. The catalog database `iceberg` is created when it is missing. Expected Trino counts for the current `odp-raw` snapshot are in `open_data_platform.lakehouse.live_counts`.

To promote a subset after objects are already in the raw bucket:

```bash
uv run python -m open_data_platform.lakehouse.bronze \
  --dataset demo_storefront/clickstream \
  --sql-out .local/bronze-sql
```

Then run the shared Spark runtime once per file in that directory. Mount the file at `/tmp/bronze.sql` and set `ODP_SPARK_SQL` to that path. `--all` promotes every contract and fails if any prefix is empty. Each Spark process loads one dataset, so a failed dataset leaves the others at the snapshot written by their own invocation.

## Result (2026-11-05 full-data live)

`make live-bronze` against `odp-raw` (`WriteMode.FULL_OVERWRITE`). Spark promoted all 18 contract tables. Trino matched `live_counts.BRONZE_COUNTS` (18/18), including geolocation 1_000_163, DummyJSON products 194, Olist customers 99_441, and postgres_oltp window stacks 3/3/3/5. Evidence: `PROJECT_STATUS.md`, [status log](../status-log.md).

## Earlier fixture-shaped check (2026-10-31 / 2026-11-01)

An earlier verification used synthetic objects only to prove payload-shape handling. That path is retained as **unit fixtures**, not as the layer live run. Historical counts (3 / 100 / 2 / 2) are archived in `docs/status-log.md`.

## Pins

The Spark image, Iceberg runtime, S3A jars, PostgreSQL JDBC driver, JDBC schema `V1`, and Trino image are the P2-02 pins. See [catalog and query](phase-2-catalog-query.md).

## Limitations

- `INSERT OVERWRITE` leaves the previous Parquet files in the bucket. Expiring snapshots and deleting orphan files is P5-07.
- The first `CREATE TABLE` fixes the table location. A later run does not move it.
- Logical contract types are not cast. Nested JSON stays compact text. Typing and expansion are in [silver layer](silver-layer.md).
- Each Spark run downloads the pinned jars. The container is removed afterwards.
- CI does not start Compose. Unit tests cover parsing, staging, and the SQL text.
- D-012 stays `proposed`.
