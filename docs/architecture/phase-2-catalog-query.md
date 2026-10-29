# P2-02 — Local catalog and query engine

Spark writes one synthetic Iceberg table through the Iceberg JDBC catalog. Trino reads that same table. The warehouse is the existing MinIO bucket, under a prefix separate from the P2-01 Hadoop-catalog probe. D-003 and D-004 record the format and this catalog.

The JDBC catalog stores table pointers in PostgreSQL. On AWS, the catalog role is Glue and the SQL role is Athena. The protocols are not the same. Phase 7 documents that mapping. This probe does not create cloud resources.

## Pins

| Piece | Pin |
| --- | --- |
| Spark image | `spark:3.5.8-java17` |
| Iceberg | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0` |
| S3A | `org.apache.hadoop:hadoop-aws:3.3.4` |
| AWS SDK | `com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| PostgreSQL JDBC | `org.postgresql:postgresql:42.7.14` |
| Transitive runtime | `org.wildfly.openssl:wildfly-openssl:1.0.7.Final` (from `hadoop-aws` 3.3.4); `org.checkerframework:checker-qual:3.55.1` (from the PostgreSQL driver). Neither was chosen separately |
| Trino image | `trinodb/trino:484` (Iceberg 1.12.0 in that release's build) |
| Catalog | Iceberg JDBC, schema version `V1`, catalog name `local` |
| Catalog database | PostgreSQL database `iceberg` on the Compose server. Not `POSTGRES_DB` and not schema `oltp` |
| Warehouse | `s3a://odp-warehouse/catalog` |
| Table | `catalog_probe.probe` (`id INT`, `label STRING`, `format-version` 2) |

`catalog-name` is `local` because that is the Spark catalog identifier Iceberg writes into `iceberg_tables.catalog_name`. The Trino catalog file is `iceberg.properties`, so the SQL name is `iceberg.catalog_probe.probe`.

Trino 484 enables its S3 file system with `fs.s3.enabled`. That file system opens `s3a://` locations. `s3.expect-continue-enabled` and `s3.chunked-encoding-enabled` are `false`, which is the Trino 484 guidance for S3-compatible storage that rejects those upload behaviors. This probe only reads through Trino.

The catalog file uses Trino's `${ENV:NAME}` substitution. The image's own `config.properties` uses the same mechanism. Secrets stay in the environment.

## Command

`docker compose up -d` does not start Spark or Trino. Both services are on the `lakehouse` profile. PostgreSQL and MinIO must be healthy. The catalog database must exist before Spark runs. Trino must start after Spark has created the JDBC tables. With GNU Make:

```bash
make test-catalog-probe
```

The target sources `.env.example` and then `.env` if present, creates `ODP_WAREHOUSE_BUCKET` and the `ICEBERG_CATALOG_DB` database when that database is missing, runs Spark, starts Trino, and queries the count. Run the target again to overwrite the same two rows and read the count a second time.

SQL: `docker/spark/catalog_probe.sql`. Spark settings: `docker/spark/run_catalog_probe.sh`. Trino catalog: `docker/trino/catalog/iceberg.properties`.

## Result (2026-10-29)

Two runs of `make test-catalog-probe`. Each Spark `SELECT` printed `2` (1 row fetched). Each Trino query printed `"2"` and exited 0. The quotes are the Trino CLI CSV output. `iceberg_tables` holds one row: catalog name `local`, namespace `catalog_probe`, table `probe`, type `TABLE`.

The current metadata location after those runs is `s3a://odp-warehouse/catalog/catalog_probe/probe/metadata/00004-c2193a8c-0cac-468e-bf2b-4e16179cf6c8.metadata.json`. That file's `format-version` is `2`. Listing `odp-warehouse` under prefix `catalog/` returned 24 objects. Earlier overwrites leave their Parquet and metadata files in place. The warehouse prefix is `catalog` and the namespace is `catalog_probe`, so the key is `catalog/catalog_probe/probe/...`.

`docker compose config --services` without the profile listed `postgres` and `minio` only.

## Limitations

- `INSERT OVERWRITE` adds a snapshot and leaves the previous Parquet files in the bucket.
- The JDBC catalog is not the Glue protocol. A future Iceberg change to the JDBC metadata tables can break the pair. Both sides are pinned to schema `V1`.
- `spark-sql` still logs Hive `ObjectStore` warnings from its session catalog. No Hive Metastore was added.
- Each Spark run downloads the pinned jars from Maven Central. The container is removed afterwards, so the Ivy cache is not kept.
- CI does not start Compose. The unit test checks the probe files only.
- Trino stays up after the target. It is not part of `docker compose up`. Stop it with `docker compose --profile lakehouse stop trino` when it is no longer needed. That command does not remove volumes.
- P2-03 and later lakehouse writes use this catalog. The P2-01 Hadoop catalog probe stays as its own evidence.
