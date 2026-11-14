# Phase 2 lakehouse

Index of the verified Bronze, Silver, and Gold path. Grain, columns, and mart definitions stay in the layer notes. This page records component versions, partitioning, file sizing, the incremental strategy, known limitations, and the citation of the live result.

Phase 2 does not schedule Airflow, capture CDC, expire Iceberg snapshots, or apply Terraform.

## Path

| Layer | Writer | Note |
| --- | --- | --- |
| Raw | Python ingestion | [Phase 1 ingestion](phase-1-ingestion.md) |
| Bronze | Spark | [Bronze layer](bronze-layer.md) |
| Silver | Spark | [Silver layer](silver-layer.md) |
| Gold | dbt-trino | [Gold layer](gold-layer.md). Grain: [Gold model design](gold-model-design.md) |

Spark writes the Iceberg JDBC catalog on the Compose PostgreSQL database `iceberg`. Trino reads that catalog. The Hadoop catalog remains the one-table probe in [P2-01](phase-2-spark-iceberg-poc.md). The durable catalog choice is [P2-02](phase-2-catalog-query.md).

## Versions

Observed on the 2026-11-11 local run and recorded in [P2-09 live run](phase-2-live-run.md). The same pins are in the probe, catalog, and Gold notes. P2-09 ran Spark in `spark:3.5.8-java17` and dbt on the host. P3-03 moved both into one runtime image with the same versions. The Make targets keep their names and call the `medallion_build` module in that image.

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

Bronze, Silver, and Gold Iceberg tables have no partition spec. `CREATE TABLE` sets `format-version` 2 and a location. It does not set `PARTITIONED BY`. dbt does not set `partitioned_by`.

Bronze keeps `dt` as a `STRING` column copied from the raw key. The write replaces the whole table, so the column is not an Iceberg partition.

Silver `PARTITION BY` is the window of `ROW_NUMBER` used to keep one survivor per key. The contract primary key is that list, except geolocation, which uses the payload columns. That clause does not create an Iceberg partition.

## File sizing

No target file size is configured. Bronze writes one staging object per dataset, `bronze/_staging/{source_id}/{dataset}/rows.jsonl` in the warehouse bucket, then `INSERT OVERWRITE`. Silver and Gold do not set a Spark or dbt target size. dbt sets the file format to Parquet and replaces the table. The engine chooses the Parquet files for that overwrite or replace.

This page does not record byte sizes or file counts for the medallion tables. The synthetic probes recorded object counts for their own tables only. Expiring snapshots and deleting orphan files is P5-07.

## Incremental strategy

The PostgreSQL extract is the only windowed incremental landing. Bronze and Silver ignore that window and rebuild each table from every object currently under the dataset prefix. Gold replaces each table.

| Step | Commit | Window |
| --- | --- | --- |
| PostgreSQL raw | One JSON array per dataset and window. The same execution overwrites that object. A new `execution_id` keeps the previous object. | `updated_at >= start AND updated_at < end`, both bounds on UTC midnight. One-day and seven-day windows share the function. |
| Other raw sources | Snapshot or full-refresh object for that execution. | None |
| Bronze | `WriteMode.FULL_OVERWRITE`: `INSERT OVERWRITE` of the whole table. | Every object under `raw/{source_id}/{dataset}/` |
| Silver | `FULL_OVERWRITE`: `DROP TABLE`, `CREATE TABLE`, `INSERT OVERWRITE` from all Bronze rows, then dedup. | The whole Bronze table |
| Gold | dbt `materialized: table` and `on_table_exists: replace`. | The Silver input of each model |

`INCREMENTAL_MERGE` and `APPEND_ONLY` are placeholders in `open_data_platform.lakehouse.write_mode`. Selecting either raises `WriteModeError` before SQL is rendered. A windowed or CDC commit in Bronze or Silver is not implemented. Schedules and backfills are Phase 3. CDC is Phase 4.

A second Bronze or Silver run with the same raw objects produces the same rows. Several `execution_id` values in the prefix all remain in the table, subject to Silver dedup. There is no `MERGE` and no exactly-once claim.

## Live result

[P2-09 live run](phase-2-live-run.md) (2026-11-11). This page does not add a run.

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
- Iceberg tables are unpartitioned. File size is the engine default. A per-table partition spec for Silver and Gold is listed under Candidates, not issues, in [`ROADMAP.md`](../../ROADMAP.md). It is not scheduled. `INSERT OVERWRITE` and Gold table replace leave previous Parquet files in the bucket until P5-07.
- Exactly-once end-to-end is not claimed.
- The JDBC catalog is not the Glue protocol. Both sides are pinned to schema `V1`.
- The Hadoop catalog is a single-writer probe. Bronze, Silver, and Gold do not use it.
- `chmod` on `.local/spark-ivy` can print `Permission denied` for jars owned by the Spark user. The recipe continues. The 2026-11-11 submits reported 6 jars already retrieved.
- CI does not start Compose.
- Olist files are CC BY-NC-SA 4.0 and are not vendored.
- Airflow schedules, CDC, and AWS apply are later work. D-006 was approved in P3-01. D-007 stays `proposed`.

## Out of scope

Daily and weekly schedules (Phase 3). Streaming CDC (Phase 4). Snapshot expiration (P5-07). JupyterLab and Superset (Phase 8). Terraform apply (Phase 7).
