# Phase 2 lakehouse

Index of the verified Bronze, Silver, and Gold path. Grain, columns, and mart definitions stay in the layer notes. This page records component versions, partitioning, file sizing, the incremental strategy, known limitations, and the citation of the live result.

Phase 2 does not schedule Airflow, capture CDC, or expire Iceberg snapshots.

## Path

| Layer | Writer | Note |
| --- | --- | --- |
| Raw | Python ingestion | [Phase 1 ingestion](phase-1-ingestion.md) |
| Bronze | Spark | [Bronze layer](bronze-layer.md) |
| Silver | Spark | [Silver layer](silver-layer.md) |
| Gold | dbt-trino | [Gold layer](gold-layer.md). Grain: [Gold model design](gold-model-design.md) |

Spark writes the Iceberg JDBC catalog on the Compose PostgreSQL database `iceberg`. Trino reads that catalog. The Hadoop catalog remains the one-table probe in [P2-01](phase-2-spark-iceberg-poc.md). The durable catalog choice is [P2-02](phase-2-catalog-query.md).

## Versions

Observed on the 2026-11-11 local run and recorded in [P2-09 live run](phase-2-live-run.md). The same pins are in the probe, catalog, and Gold notes. P2-09 ran Spark in `spark:3.5.8-java17` and dbt on the host. P3-03 moved both into one runtime image with the same versions. The Make targets keep their names and call the same `medallion_build` module as the Airflow DAGs.

| Piece | Version |
| --- | --- |
| PostgreSQL image | `postgres:16.15-bookworm` |
| MinIO image | `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` |
| Spark image | `spark:3.5.8-java17`. Since P3-03 the same Spark `3.5.8` and JRE are copied into the runtime image `odp-runtime:airflow3.4.0-spark3.5.8`, which also holds dbt ([Local Airflow](airflow-local.md)) |
| Iceberg runtime | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0` |
| Hadoop AWS | `org.apache.hadoop:hadoop-aws:3.3.4` |
| AWS SDK bundle | `com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| PostgreSQL JDBC | `org.postgresql:postgresql:42.7.14` |
| Trino image | `trinodb/trino:484` |
| dbt Core | `1.12.6` |
| dbt-trino | `1.11.0` |

Format version is 2. JDBC catalog schema is `V1`. Spark `--packages` resolve into `.local/spark-ivy` ([Ivy cache](phase-2-spark-ivy-cache.md)).

## Partitioning

Raw objects use the ingestion date in the key:

```text
raw/{source_id}/{dataset}/dt={YYYY-MM-DD}/execution_id={execution_id}/{object_name}
```

`dt=` is the UTC date of `_ingestion_ts`. It is not an event-time partition. Layout: [raw storage](raw-storage-layout.md).

Bronze tables are `PARTITIONED BY (dt)` since P3-08 ([ADR-0026](../decisions/0026-bronze-partitions-and-run-metadata.md)). `dt` is the `STRING` column copied from the raw key, an identity partition. The scheduled Bronze `MERGE` adds `target.dt IN (<staged dt values>)` to its `ON` clause, so it scans only the partitions it writes ([review R5](../reviews/2026-11-21-verified-issues-review.md)); `table_io` in `lakehouse_state` records the files and bytes each `MERGE` scanned. A table created before P3-08 fails the next incremental write until `make live-rebuild` recreates it.

Silver and Gold Iceberg tables have no partition spec. `CREATE TABLE` sets `format-version` 2 and a location. dbt does not set `partitioned_by`.

Silver `PARTITION BY` is the window of `ROW_NUMBER` used to keep one survivor per key. The contract primary key is that list, except geolocation, which uses the payload columns. That clause does not create an Iceberg partition.

## File sizing

No target file size is configured. Bronze writes one staging object per changed dataset and run, `bronze/_staging/{run_label}/{source_id}/{dataset}/rows.jsonl` in the warehouse bucket, then one `MERGE INTO` (incremental) or `CREATE OR REPLACE TABLE … AS SELECT` (full rebuild) since P3-13 ([ADR-0024](../decisions/0024-incremental-lakehouse-refresh.md)). Silver and Gold do not set a Spark or dbt target size. dbt sets the file format to Parquet and replaces the table. The engine chooses the Parquet files for that overwrite or replace.

This page does not record byte sizes or file counts for the medallion tables. The synthetic probes recorded object counts for their own tables only. Expiring snapshots and deleting orphan files is P5-07.

## Incremental strategy

The PostgreSQL extract is the only windowed incremental landing. Bronze and Silver ignore that window and rebuild each table from every object currently under the dataset prefix. Gold replaces each table.

| Step | Commit | Window |
| --- | --- | --- |
| PostgreSQL raw | One JSON array per dataset and window. The same execution overwrites that object. A new `execution_id` keeps the previous object. | `updated_at >= start AND updated_at < end`, both bounds on UTC midnight. One-day and seven-day windows share the function. |
| Other raw sources | Snapshot or full-refresh object for that execution. | None |
| Bronze | `WriteMode.FULL_OVERWRITE`: one `CREATE OR REPLACE TABLE … AS SELECT` of the whole table (P3-11). | Every object under `raw/{source_id}/{dataset}/` |
| Silver | `FULL_OVERWRITE`: one `CREATE OR REPLACE TABLE … AS SELECT` from all Bronze rows, then dedup (P3-11; before it, `DROP TABLE`, `CREATE TABLE`, `INSERT OVERWRITE`). | The whole Bronze table |
| Gold | dbt `materialized: table` and `on_table_exists: replace`. | The Silver input of each model |

`INCREMENTAL_MERGE` and `APPEND_ONLY` are placeholders in `open_data_platform.lakehouse.write_mode`. Selecting either raises `WriteModeError` before SQL is rendered. A windowed or CDC commit in Bronze or Silver is not implemented. Schedules and backfills are Phase 3. CDC is Phase 4.

A second Bronze or Silver run with the same raw objects produces the same rows. Several `execution_id` values in the prefix all remain in the table, subject to Silver dedup. There is no `MERGE` and no exactly-once claim.

## Atomic table replace (P3-11)

Every writer of a table that Trino, dbt, or a check reads replaces it in one commit. No writer drops a read table, so a reader sees the previous snapshot or the new one, never a missing, empty, or half-written table. A failed write commits nothing and leaves the previous snapshot.

| Writer | Tables | Replace statement | Code |
| --- | --- | --- | --- |
| Bronze (Spark) | `bronze.*` | Full rebuild: `CREATE OR REPLACE TABLE bronze.<t> USING iceberg LOCATION … AS SELECT CAST(<c> AS STRING) AS <c> … FROM <staging view>`. Incremental (P3-13): `MERGE INTO bronze.<t> … WHEN NOT MATCHED THEN INSERT`, one commit; never drops the table | `lakehouse/bronze.py` `_table_sql`, `_merge_sql` |
| Silver (Spark) | `silver.*` | `CREATE OR REPLACE TABLE silver.<t> USING iceberg LOCATION … AS SELECT CAST(<c> AS <type>) AS <c> … FROM (<dedup over bronze.<t>>)` | `lakehouse/silver/common.py` `render_silver_sql` |
| Quarantine staging (Spark) | `gold_staging.quarantine_findings` | `CREATE OR REPLACE TABLE gold_staging.quarantine_findings USING iceberg LOCATION … AS SELECT … FROM <staging view>` | `lakehouse/quarantine_stage.py` `render_load_sql` |
| Gold (dbt-trino 1.11) | the seven Gold schemas | `materialized: table` with `on_table_exists: replace` renders `create or replace table … as select` in Trino | `dbt/dbt_project.yml` |

- **Zero rows.** Bronze and quarantine replace with `AS SELECT * FROM (SELECT CAST(NULL AS <type>) AS <c> …) WHERE 1 = 0`, so the empty table keeps its columns and types.
- **Types.** Spark rejects a column list in `CREATE TABLE … AS SELECT`, so each output column is cast to its contract type in the select. The table schema follows those casts.
- **Schema change.** A new contract version that adds, drops, or retypes a column changes the rendered select. The replace commits the new schema in the same commit as the rows; there is no drop and no separate rebuild step. Readers that depend on a removed column fail on the new snapshot, not on a missing table.
- **Catalog.** Spark uses `org.apache.iceberg.spark.SparkCatalog` over the JDBC catalog. Replace is atomic there; under `SparkSessionCatalog` it is not, so the catalog class is part of this guarantee.
- **History.** A replace keeps the table's snapshot history and its location. Snapshots and old files stay until P5-07 expires them.
- **Concurrency.** Atomic replace protects readers, not a second writer. The 1-slot `lakehouse_gold` pool still serializes Airflow writers (P3-04). A `make` target and a DAG run must not write the same table at the same time (P3-12).
- **Tests.** `tests/unit/lakehouse/test_atomic_replace.py` renders every shipped contract and quarantine staging (with and without rows) and fails on `DROP TABLE`, `CREATE TABLE`, or `INSERT OVERWRITE`; it also pins `on_table_exists: replace`.
- **Live evidence.** `scripts/p3_11_replace_check.sh` writes counts, durations, and query totals to `.local/p3-11/`. Results below.

Live run on 2026-11-20, local Compose, runtime image `odp-runtime:airflow3.4.0-spark3.5.8`:

| Check | Result |
| --- | --- |
| RTAS probe | Before the change, a probe table in the JDBC catalog was replaced twice with the same `LOCATION`, the second time with an added column. Spark kept 2 snapshots, and Trino read 2 rows with the new column |
| Forced Silver failure | The rendered `olist_brazilian_ecommerce.geolocation` Silver SQL, copied into the work volume with `raise_error('p3_11_forced_failure')` in the final filter, failed in the write job (Spark exit 1, task `RuntimeException: p3_11_forced_failure`). Trino count 738_332 before and after; snapshot count 1 before and after |
| Full rebuild under readers | A Trino loop counted `bronze` and `silver` `olist_brazilian_ecommerce__geolocation`, `gold_staging.quarantine_findings`, and `marketplace_operations.fct_orders` in one query while `make live-bronze` (309 s), `make live-silver` (260 s), `make stage-quarantine` (19 s), and `make dbt-build` (36 s) ran in sequence. 410 queries, 0 errors, 0 zero counts. Bronze checks 18 passed, Silver checks 20 passed, dbt PASS=287 |
| Snapshot history | Snapshots before → after the rebuild: Bronze geolocation 11 → 12, Silver geolocation 1 → 2, quarantine 1 → 2, `fct_orders` 10 → 11. The Silver and quarantine tables had 1 snapshot because the earlier writer dropped them on every run |
| dbt rendered SQL | `dbt/target/run` holds 55 `create or replace table` statements and no `drop table` |

The loop measures what a reader sees between statements. It does not prove isolation for a reader that is already scanning when a commit lands; Iceberg snapshot reads give that, and this check does not exercise it separately.

## Live result

[P2-09 live run](phase-2-live-run.md) (2026-11-11). This page does not add a run. `make test-catalog-probe` was retired on 2026-11-25.

| Command | Result |
| --- | --- |
| `make test-catalog-probe` | Spark `2`, Trino `"2"` |
| `make live-bronze` | 18/18 Trino counts matched that run's raw counts |
| `make live-silver` | 20/20 checks returned `ok` |
| `make live-gold` | `dbt build` PASS=287 (55 tables and 232 schema tests) |
| `make dbt-test-reconciliation` | PASS=56 |

Headline anchors match the 2026-11-05 / 2026-11-06 baseline, including Silver geolocation 738_332, late orders 6_534, and on-time deliveries 89_936 / 96_470. The Silver checker count is 20 statements after P2-07. The 2026-11-05 note said 22/22. Row anchors are unchanged. No reconciliation test returned a mismatch row.

## Limitations

- Bronze and Silver implement full overwrite only. The other write modes fail if selected.
- Silver and Gold tables are unpartitioned; Bronze is partitioned by `dt` (P3-08). File size is the engine default. A per-table partition spec for Silver and Gold is listed under Candidates, not issues, in [`ROADMAP.md`](../../ROADMAP.md). It is not scheduled. `INSERT OVERWRITE` and Gold table replace leave previous Parquet files in the bucket until P5-07.
- Exactly-once end-to-end is not claimed.
- Spark and Trino are both pinned to JDBC catalog schema `V1`.
- The Hadoop catalog is a single-writer probe. Bronze, Silver, and Gold do not use it.
- `chmod` on `.local/spark-ivy` can print `Permission denied` for jars owned by the Spark user. The recipe continues. The 2026-11-11 submits reported 6 jars already retrieved.
- CI does not start Compose.
- Olist files are CC BY-NC-SA 4.0 and are not vendored.
- Airflow schedules and CDC are later work. D-006 was approved in P3-01. D-007 stays `proposed`.

## Out of scope

Daily and weekly schedules (Phase 3). Streaming CDC (Phase 4). Snapshot expiration (P5-07). JupyterLab and Superset (Phase 8).
