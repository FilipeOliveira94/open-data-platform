# Bronze layer

Near-source Iceberg tables built from the raw objects in [raw storage layout](raw-storage-layout.md). Decision context: [ADR-0012](../decisions/0012-medallion-layers.md) (still `proposed`) and [ADR-0020](../decisions/0020-raw-storage-layout.md). Silver typing, JSON expansion, and deduplication are documented in [silver layer](silver-layer.md). This layer does not replace the raw objects.

**Write mode (P3-13, [ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md)):** the scheduled refresh is `WriteMode.INCREMENTAL_APPEND`. Bronze promotes only raw objects of complete executions that the promotion registry has not seen, and appends their rows with one `MERGE INTO` per dataset. `--full-rebuild` and a dataset's first promotion use `WriteMode.FULL_OVERWRITE`: one `CREATE OR REPLACE TABLE … AS SELECT` from every complete object (one Iceberg commit since P3-11; see [atomic table replace](phase-2-lakehouse.md#atomic-table-replace-p3-11)). The upsert and windowed placeholders in `open_data_platform.lakehouse.write_mode` still fail if selected. The phase strategy is [Phase 2 lakehouse](phase-2-lakehouse.md).

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
| `_execution_completed_ts` | `completed_at` of the execution's `_COMPLETE` marker (P3-12). Never null for rows promoted since P3-13. |
| `_raw_etag` | ETag of the raw object the row came from (P3-13). Bronze only; Silver does not carry it. |
| Contract fields | Contract order. |
| Envelope fields | Page fields other than the `record_locator`, in contract order. |

`dt` is the ingestion date already used to lay out raw objects. There is no Iceberg partition spec. Each incremental append writes its own data files, so the rows of one run sit in files of their own.

Quarantine objects are not read. Invalid rows that remain inside a raw object are promoted with it. A payload that does not match the contract shape fails that dataset before the table is overwritten.

## Write

### Incremental refresh (P3-13)

Each run lists `raw/{source_id}/{dataset}/` in `ODP_RAW_BUCKET` (default `odp-raw`) with ETags and reads the promotion registry, `lakehouse_state.bronze_promotions` in the Iceberg catalog database. A data object is a candidate when its key is not registered or its ETag differs. Only candidates in a **complete** execution are promoted: the execution's `_COMPLETE` marker lists exactly the data objects under its prefix. Markers are read only for executions that hold a candidate. An incomplete execution is counted and waits for its marker.

Objects are read one at a time. Each object's rows go to a local JSONL file in the run's work directory, and the file is uploaded once per dataset:

```text
s3a://{ODP_WAREHOUSE_BUCKET}/bronze/_staging/{run_label}/{source_id}/{dataset}/rows.jsonl
```

Airflow passes its run id as the run label; Make passes `make-<UTC timestamp>`. The Spark script per changed dataset is one of:

| Mode | When | Statement |
| --- | --- | --- |
| `incremental_append` | Scheduled refresh of a registered dataset | `MERGE INTO bronze.{table}` on `raw_object_key`, `_raw_etag`, `record_index`; `WHEN NOT MATCHED THEN INSERT`. When a registered key has a new ETag, `WHEN NOT MATCHED BY SOURCE AND raw_object_key IN (…) THEN DELETE` removes its old rows in the same commit |
| `full_overwrite` | `--full-rebuild`, or a dataset the registry has never seen | `CREATE OR REPLACE TABLE … AS SELECT` from every complete object |

After Spark commits, one registry transaction upserts the promoted objects and marks the dataset pending for Silver. Then the staging object is deleted. A crash between the commit and the registry write leaves the objects unregistered; the retry stages them again and the `MERGE` inserts nothing. `lakehouse_state.bronze_refreshes` records, per run and dataset, the objects listed, waiting for a marker, and promoted, the rows appended, and the table total. A failed dataset is recorded with mode `…:failed`.

A dataset with nothing new gets no script, and its table is not written. A refresh where no dataset changed and no later layer is pending writes no table, and the Airflow task ends as skipped. An empty prefix is recorded, not failed, on the scheduled path. `--full-rebuild` fails for a dataset with no complete execution instead of emptying its table.

An empty JSON array is a successful object with zero records. The incremental path registers it without a Spark run. A full rebuild of a dataset whose complete objects hold zero rows commits an empty table.

Staging is a build artifact in the warehouse bucket. The auditable bytes stay under `raw/`. There is no exactly-once claim: the `MERGE` key makes a repeated promotion a no-op, and the registry records what was promoted.

## Live run vs unit fixtures

| Mode | Command | Data |
| --- | --- | --- |
| **Live run** | `make live-bronze` | Every contract from `ODP_RAW_BUCKET` (default `odp-raw`) |
| **Unit fixtures** | `open_data_platform.lakehouse.fixtures` via pytest / `--prepare-unit-fixtures` | Synthetic objects for deterministic unit tests only |

The live run does **not** use a separate fixture bucket. Buckets in normal local use are `odp-raw` (landing) and `odp-warehouse` (Iceberg). `odp-smoke` remains the Phase 0 MinIO smoke bucket.

```bash
make live-bronze     # incremental Bronze, then count checks
make live-refresh    # every transform step, incremental (same as transform_lakehouse)
make live-rebuild    # every table from all complete raw objects
```

PostgreSQL and MinIO must be healthy. The catalog database `iceberg` and the `lakehouse_state` schema are created when they are missing. The bronze step writes `bronze-source-counts.tsv` in the run's work directory: one line per table written in that run, with the registry row total of the dataset. `bronze-checks` compares each with Trino `COUNT(*)`. An empty file means no table was written, and the check step is skipped.

The standalone CLI below has no registry. It renders a full rebuild of each selected dataset. To render a subset after objects are already in the raw bucket:

```bash
uv run python -m open_data_platform.lakehouse.bronze \
  --dataset demo_storefront/clickstream \
  --sql-out .local/bronze-sql
```

Then run the shared Spark runtime once per file in that directory. Mount the file at `/tmp/bronze.sql` and set `ODP_SPARK_SQL` to that path. The CLI does not update the registry, so prefer `make live-rebuild`. Each Spark process loads one dataset, so a failed dataset leaves the others at the snapshot written by their own invocation.

## Result (2026-11-20 incremental refresh, P3-13)

Local Compose, runtime image `odp-runtime:airflow3.4.0-spark3.5.8`, one Spark JVM per dataset. Raw held 102 data objects in 18 prefixes, 28 of them in complete executions; the other 74 predate the P3-12 markers and wait as incomplete. Before the run, the two `postgres_oltp` windows that existed only without markers were re-extracted under `p3-13-reland-*`.

| Refresh | Raw objects read / rows appended | Tables written | Step durations | Wall |
| --- | --- | --- | --- | --- |
| `make live-rebuild` (`--full-rebuild`, also the registry bootstrap) | 28 / 1_565_638 (geolocation 1_000_163) | Bronze 18, Silver 18, quarantine, Gold | bronze 251.7 s, bronze-checks 2.3 s (18 passed), silver 255.4 s, silver-checks 5.2 s (20 passed), stage-quarantine 15.1 s, dbt-build 26.5 s (PASS=287) | 565 s |
| `make live-refresh`, no new raw object | 0 / 0; 102 keys listed, no object body or marker read | None | bronze 1.0 s (0.2 s promotion), every later step skipped in 0.5–0.7 s | 13 s |
| `make live-refresh` after one new daily window (`postgres_oltp` `[2026-10-17, 2026-10-18)`) | 4 / 5 | Bronze 4 (`MERGE`), Silver 4, quarantine, Gold | bronze 50.7 s, bronze-checks 0.7 s (4 passed), silver 50.3 s, silver-checks 0.8 s (4 passed), stage-quarantine 14.2 s, dbt-build 26.2 s (PASS=287) | 151 s |
| `make live-bronze` after the same execution was re-ingested with 60, then 100 events | 1 / 60, then 1 / 100 | Bronze 1 (`MERGE` with delete) | bronze 16.8 s, 15.8 s; Bronze count 60, then 100, equal to the registry total | — |
| `make live-refresh` with Silver and Gold still pending, no new raw object | 0 / 0 | Silver 1, quarantine, Gold | bronze 1.0 s, silver 16.3 s, stage-quarantine 14.3 s, dbt-build 25.2 s | 67 s |

Iceberg snapshot summaries show what the commits did. Each insert-only `MERGE` into `bronze.postgres_oltp__orders` committed as `append`, one data file added and none rewritten. The re-ingested clickstream object committed as `overwrite`: 60 added and 100 deleted, then 100 added and 60 deleted, one file replaced each time. `bronze.olist_brazilian_ecommerce__geolocation` kept 14 snapshots and `silver.…__geolocation` kept 4 through the daily-window refresh, so untouched tables were not rewritten.

Airflow (`make airflow-up`, same stack):

- With the default `batch_asset_events=False`, four raw Asset events produced four `transform_lakehouse` runs, one event each. The last two wrote nothing (8–9 s). The DAG now sets `batch_asset_events=True`.
- After the change, `ingest_demo_storefront` started run A (1 event). Three `ingest_postgres_oltp` runs landed while A ran. When A ended, one follow-up run consumed all 3 events and promoted the 3 executions at once (12 objects, 15 rows). No third run was created, and the queue was empty.
- A manual `transform_lakehouse` run with nothing new finished in 8.2 s with all six tasks `skipped`.

Bronze time in an incremental run is mostly one Spark JVM start per changed dataset (about 12 s). `dbt-build` took 25–27 s in every run against Silver rebuilds of 16–255 s, so Gold stays a full dbt build ([ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md)). A `MERGE` into a large Bronze table was not measured: no new Olist release landed. Raw listing still grows with the number of keys under a prefix (metadata only).

## Result (2026-11-05 full-data live)

`make live-bronze` against `odp-raw` (`WriteMode.FULL_OVERWRITE`). Spark promoted all 18 contract tables. Trino matched the raw counts recorded for that run (18/18), including geolocation 1_000_163, DummyJSON products 194, Olist customers 99_441, and postgres_oltp window stacks 3/3/3/5. Those figures describe the 2026-11-05 snapshot. Chronological note: [status log](../status-log.md).

## Earlier fixture-shaped check (2026-10-31 / 2026-11-01)

An earlier verification used synthetic objects only to prove payload-shape handling. That path is retained as **unit fixtures**, not as the layer live run. Historical counts (3 / 100 / 2 / 2) are archived in `docs/status-log.md`.

## Pins

The Spark image, Iceberg runtime, S3A jars, PostgreSQL JDBC driver, JDBC schema `V1`, and Trino image are the P2-02 pins. See [catalog and query](phase-2-catalog-query.md).

## Limitations

- Each replace adds a snapshot and leaves the previous Parquet files in the bucket. Expiring snapshots and deleting orphan files is P5-07.
- Every replace names the same table location, `bronze/{source_id}/{dataset}`. A replace does not move it.
- Logical contract types are not cast. Nested JSON stays compact text. Typing and expansion are in [silver layer](silver-layer.md).
- Spark `--packages` pins are unchanged. Jars resolve into the host Ivy cache at `.local/spark-ivy` (P2-08); see [Spark Ivy cache](phase-2-spark-ivy-cache.md).
- CI does not start Compose. Unit tests cover parsing, staging, and the SQL text.
- D-012 is `approved`. Medallion namespaces and Gold schemas are in [ADR-0012](../decisions/0012-medallion-layers.md).
