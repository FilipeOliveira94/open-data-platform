# ADR-0004: Table catalog and SQL engines

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-004 |
| Related issues | P2-01, P2-02, P7-01, P7-03 |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Tables need a **table catalog** (metadata for Iceberg/Hive-style tables) and an interactive/analytical SQL path. Local and AWS catalogs differ; document the mapping without treating products as identical. Discovery/metadata UIs are a separate decision ([ADR-0016](0016-data-discovery.md)).

## Decision

Choose the **local table catalog and query engine after the Phase 2 PoC** (candidate direction: local catalog + **Trino**). On the AWS path, use **AWS Glue Catalog** and **Amazon Athena** as counterparts. Record the local choice when P2-02 completes.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Local catalog after PoC + Trino; Glue + Athena (chosen direction) | Clear local↔AWS roles; interactive SQL | Local catalog product still PoC-gated | — |
| Hive Metastore (local) | Widely supported | Ops + version pin burden | Candidate in PoC |
| Project Nessie | Git-like catalog; Iceberg-friendly | Extra component; PoC complexity | Candidate in PoC |
| Apache Polaris | Iceberg REST catalog direction | Maturity/ops unknowns for this stack | Candidate in PoC |
| Iceberg REST catalog (generic) | Standard protocol | Still need a concrete implementation | Candidate |
| Spark session catalog only | Fewer services | Weaker multi-engine + AWS mapping demo | Possible interim, not final AWS mapping |
| Pre-commit to one catalog before PoC | Clear docs early | May be incompatible | Rejected until P2-01 |
| DataHub / Amundsen as table catalog | Rich UI | Not a substitute for Glue/Hive-style table catalogs | Rejected here — see D-016 |

## Consequences and trade-offs

- Case study must explain catalog differences.
- dbt profiles stay environment-specific without duplicating business models.
- Discovery platforms (D-016) must not replace this decision.

## Evidence / validation

None yet — awaiting P2-01 / P2-02.

## Revisit when

PoC selects a different query engine or catalog; update this ADR and P7 mapping together.
