# P2-01 — Spark + Iceberg probe

One synthetic Iceberg table, created, overwritten, and queried with Spark SQL. The warehouse is the existing MinIO bucket. This does not choose the durable catalog or the SQL engine (P2-02). D-003 and D-004 stay `proposed`.

## Pins

| Piece | Pin |
| --- | --- |
| Spark image | `spark:3.5.8-java17` (Scala 2.12.18, OpenJDK 17.0.20) |
| Iceberg | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0` |
| S3A | `org.apache.hadoop:hadoop-aws:3.3.4` |
| AWS SDK | `com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| Transitive runtime | `org.wildfly.openssl:wildfly-openssl:1.0.7.Final` (pulled by `hadoop-aws` 3.3.4; not chosen separately) |
| Catalog | `org.apache.iceberg.spark.SparkCatalog`, `type=hadoop`, name `local` |
| Warehouse | `s3a://odp-warehouse/poc` |
| Table | `poc.probe` (`id INT`, `label STRING`) |

The image's Hadoop client jars are 3.3.4 and do not include `S3AFileSystem`. The first run failed with `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`. The probe then added `hadoop-aws` 3.3.4 and the AWS SDK bundle version declared by that artifact. The catalog stayed the Hadoop catalog.

`spark:3.5.8` (the Java 11 tag) is rejected for this probe and was not run. Iceberg 1.11 dropped Java 11. See the [Iceberg 1.11.0 release notes](https://iceberg.apache.org/releases/).

## Command

`docker compose up -d` does not start Spark. The service is on the `lakehouse` profile. MinIO must be healthy. With GNU Make:

```bash
docker compose up -d minio
make test-iceberg-poc
```

The Make target sources `.env.example` and then `.env` if present, creates `ODP_WAREHOUSE_BUCKET` with the existing S3 helper, and runs:

```bash
docker compose --profile lakehouse run --rm -T spark
```

SQL: `docker/spark/poc_probe.sql`. Spark settings: `docker/spark/run_poc.sh`. Credentials are the MinIO placeholders from the environment. The container reads MinIO at `http://minio:9000`.

## Result (2026-10-28)

Two executions of that Compose command, after the S3A jars were added. Each `SELECT COUNT(*) AS row_count FROM poc.probe` printed `2` and fetched 1 row. The second run wrote `poc/poc/probe/metadata/v3.metadata.json`. Listing `odp-warehouse` under prefix `poc/` returned 13 objects (Parquet data files plus Iceberg metadata). The key has `poc` twice because the warehouse prefix is `poc` and the namespace is also `poc`. The Hadoop catalog writes `{warehouse}/{namespace}/{table}`.

`make` is not installed in the shell used for this run (`Command 'make' not found`). The recipe above was executed directly. `docker compose config --services` without the profile lists `minio` and `postgres` only.

## Limitations

- The Hadoop catalog is a single-writer proof. It is not the AWS Glue counterpart, and it is not a safe catalog for concurrent writers. P2-02 still chooses the local catalog and the query engine.
- `INSERT OVERWRITE` adds a new snapshot. Older Parquet files stay in the bucket. This probe does not expire snapshots.
- `spark-sql` logs Hive `ObjectStore` warnings from its session catalog. No Hive Metastore service was added. The Iceberg table metadata is the Hadoop catalog on MinIO.
- Each run downloads the pinned jars from Maven Central. The container is removed afterwards, so the Ivy cache is not kept.
- CI does not start Compose. The unit test checks the probe files only.
