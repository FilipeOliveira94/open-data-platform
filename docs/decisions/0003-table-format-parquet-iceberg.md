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

None yet — hard gate at P2-01.

## Revisit when

PoC fails; or dbt/query-engine support forces a format change.
