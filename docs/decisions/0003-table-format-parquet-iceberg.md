# ADR-0003: Table format

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-003 |
| Related issues | P2-01, P2-03–P2-09 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Analytics need a table format that supports evolution, incremental processing and engine interoperability. Version mismatches between Spark, Iceberg, catalogs and SQL engines are a known failure mode.

## Decision

Target **Parquet** files managed with **Apache Iceberg**, but only **after** a minimal create / write / catalog / query PoC (P2-01) proves a pinned, compatible version set. Reject incompatible combinations rather than forcing the stack.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Parquet + Iceberg after PoC (chosen) | ACID/time-travel; multi-engine story; Glue/Athena path | Compatibility PoC required | — |
| Hive-style Parquet only | Simpler | Weaker ACID / time-travel story | Fallback if Iceberg PoC fails |
| Delta Lake | Strong Spark story | Different AWS catalog story; pick one primary format | Deferred unless PoC favors it |
| Apache Hudi | Incremental upserts / CDC-friendly | Another ecosystem to operate; overlaps Iceberg goals | Deferred |
| ORC as primary format | Efficient columnar | Weaker lakehouse multi-engine narrative here vs Parquet+Iceberg | Rejected as primary |
| Avro as primary tabular format | Good for streaming schemas | Not ideal as primary lakehouse analytics format | Rejected as primary (may still appear in streams) |
| Iceberg without PoC gate | Faster to declare a stack | High risk of unusable combination | Rejected |

## Consequences and trade-offs

- Phase 2 is gated on evidence, not aspiration.
- Documentation must list pinned versions and known limitations.

## Evidence / validation

P2-01 (2026-10-28) created, overwrote, and queried one Iceberg table. Detail: [`docs/architecture/phase-2-spark-iceberg-poc.md`](../architecture/phase-2-spark-iceberg-poc.md).

| Piece | Pin |
| --- | --- |
| Spark | `spark:3.5.8-java17` (Scala 2.12.18, OpenJDK 17.0.20) |
| Iceberg | `iceberg-spark-runtime-3.5_2.12:1.12.0` |
| Catalog | Spark `SparkCatalog`, `type=hadoop` |
| Warehouse | MinIO bucket `odp-warehouse`, `s3a://odp-warehouse/poc` |
| S3A jars | `hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262` |

Both runs returned `row_count` 2. The official image's Hadoop 3.3.4 client jars omit `S3AFileSystem`; the first attempt failed with that `ClassNotFoundException` and was rerun only after those two jars were pinned. `wildfly-openssl:1.0.7.Final` came in as a transitive dependency of `hadoop-aws`.

Rejected without a live run: tag `spark:3.5.8` (Java 11) with Iceberg 1.12. Iceberg 1.11 dropped Java 11.

This does not approve the table format. The Hadoop catalog is not the durable catalog, and no second engine was queried. D-004 is unchanged. Status stays `proposed`.

## Revisit when

PoC fails; or dbt/query-engine support forces a format change.
