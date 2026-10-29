# ADR-0004: Table catalog and SQL engines

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-10-29 |
| Owner | Platform architect |
| Index ID | D-004 |
| Related issues | P2-01, P2-02, P7-01, P7-03 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Tables need a **table catalog** (metadata for Iceberg/Hive-style tables) and an interactive/analytical SQL path. Local and AWS catalogs differ; document the mapping without treating products as identical. Discovery/metadata UIs are a separate decision ([ADR-0016](0016-data-discovery.md)).

## Decision

Local catalog: **Iceberg JDBC** on the Compose PostgreSQL, database `iceberg`, schema version `V1`, catalog name `local`. Query engine: **Trino** `trinodb/trino:484`. Spark `3.5.8-java17` writes. On the AWS path, **AWS Glue Catalog** and **Amazon Athena** remain the counterparts. JDBC is not the Glue protocol. Phase 7 records that mapping and does not copy this database URL.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Iceberg JDBC on existing PostgreSQL + Trino; Glue + Athena (chosen) | One new process (Trino); commit row in Postgres; Spark and Trino share the table | JDBC schema can break across Iceberg versions; not the Glue protocol | — |
| Hive Metastore (local) | Widely supported; closer to Glue's historical API | Extra service and version pins | Not selected. JDBC met the probe without it |
| Project Nessie | Git-like catalog; Iceberg-friendly | Extra component | Not selected for this probe |
| Apache Polaris / Iceberg REST | Closest local stand-in for Glue's Iceberg REST endpoint | Extra service, auth, and bootstrap | Not selected. Reopen if a local REST protocol is required |
| Spark session catalog only | Fewer services | Weaker multi-engine story | Not the durable catalog |
| Hadoop catalog (P2-01) | Already proven for one writer | Not a concurrent catalog; weak AWS mapping | Kept as the P2-01 probe only |
| DataHub / Amundsen as table catalog | Rich UI | Not a substitute for a table catalog | Rejected here — see D-016 |

## Consequences and trade-offs

- Case study must explain catalog differences. JDBC and Glue share a role, not a protocol.
- Later lakehouse writes (from P2-03) use this catalog. The Hadoop catalog stays the P2-01 probe.
- dbt profiles stay environment-specific without duplicating business models.
- Discovery platforms (D-016) must not replace this decision.
- Trino is opt-in on the `lakehouse` profile. Its image sizes the JVM from the memory Docker assigns to the container.

## Evidence / validation

P2-02 (2026-10-29). Detail: [`docs/architecture/phase-2-catalog-query.md`](../architecture/phase-2-catalog-query.md).

Two Spark runs printed `2`. Two Trino runs printed `"2"`. `iceberg_tables` has one row for `local.catalog_probe.probe`. The current metadata file records `format-version` 2. `docker compose config --services` without the profile listed `postgres` and `minio` only.

## Revisit when

The JDBC schema version diverges between Spark and Trino; a local Iceberg REST catalog is required to mirror Glue; or the pinned Trino image no longer reads tables written by the pinned Spark runtime. Update this ADR and the Phase 7 mapping together.
