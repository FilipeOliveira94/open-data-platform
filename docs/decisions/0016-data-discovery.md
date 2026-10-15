# ADR-0016: Data discovery and metadata platforms

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-016 |
| Related issues | After Phase 2+; optional Phase 5+ |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Engineers and analysts need search, ownership and glossary-style metadata. That is **not** the same problem as a table catalog for Iceberg/Spark/Athena ([ADR-0004](0004-table-catalog-and-sql-engines.md)).

## Decision

**Defer** selecting a discovery platform until lakehouse tables and some lineage/dbt docs exist. Then evaluate open-source discovery tools. Discovery UIs are optional and **must not** replace Glue/Hive-style table catalogs.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Defer, then evaluate (chosen direction) | Avoids premature platform weight | No discovery UI early | — |
| DataHub | Rich metadata + lineage overlap | Heavy ops | Strong candidate later |
| Amundsen | Search/ownership focus | Lineage depth varies; maintenance | Candidate later |
| OpenMetadata | All-in-one metadata | Ops cost | Candidate later |
| Glue console as only discovery UX | Native AWS | Weak local story; limited glossary/search | Insufficient alone |
| Skip dedicated discovery UI | Less ops | Harder navigation as table count grows | Acceptable interim |

## Consequences and trade-offs

- Cross-link lineage (D-010): a tool may serve both roles but decisions stay separate.
- Do not block Phase 2 on discovery.

## Evidence / validation

None yet — deferred.

## Revisit when

Stakeholders require a discovery UI earlier; pick one candidate and document ops cost.
