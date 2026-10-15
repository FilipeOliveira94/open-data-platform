# ADR-0017: Analytics visualization

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-017 |
| Related issues | After Gold models (Phase 2+); case study |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Business users need charts and dashboards over Gold marts. That is separate from **ops** dashboards in Grafana ([ADR-0009](0009-observability.md)) and from raw SQL in Trino/Athena ([ADR-0004](0004-table-catalog-and-sql-engines.md)).

## Decision

**Defer** a BI/dataviz product until Gold models with documented grain exist. Prefer an open-source tool for local demos and document an AWS counterpart (e.g. QuickSight). Do not dual-purpose Grafana as the primary business BI tool unless explicitly revisited.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Defer until Gold; OSS local + AWS counterpart (chosen direction) | Avoids BI without marts; keeps local reproducible | No fancy dashboards early | — |
| Apache Superset | Open-source BI; SQLAlchemy/Trino-friendly | Another service to operate | Strong candidate |
| Metabase | Simple UX | Fewer advanced modeling features | Candidate |
| Amazon QuickSight | Native AWS BI | Cost; weaker local parity | Document as AWS counterpart |
| Tableau / Power BI | Familiar enterprise BI | Licensing; weak Compose reproducibility | External optional; not core |
| SQL only (Trino/Athena) with no BI tool | Minimal | Poor non-technical demo path | Acceptable interim |
| Grafana as primary business BI | Already in stack | Ops-oriented; weak semantic BI modeling | Rejected as primary BI |

## Consequences and trade-offs

- Case study can show SQL exploration before a BI tool lands.
- Dataviz must not invent metrics; charts query verified Gold models.

## Evidence / validation

None yet — deferred.

## Revisit when

Gold marts are stable and a demo requires BI; select Superset or Metabase and pin versions.
