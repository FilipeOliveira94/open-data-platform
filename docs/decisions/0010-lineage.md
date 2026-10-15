# ADR-0010: Data lineage

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — |
| Owner | Platform architect |
| Index ID | D-010 |
| Related issues | Phase 5 (`P5-04`), dbt Phase 2+ |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Lineage must reflect **actual executions**. Static diagrams without emitters are not evidence. Discovery/search UIs are a separate decision ([ADR-0016](0016-data-discovery.md)).

## Decision

Integrate **OpenLineage** only for components with **verified** support, and demonstrate lineage from a real run. Use **dbt** native lineage/docs for analytical models. Optional lineage UIs (Marquez, or DataHub as a lineage consumer) may be evaluated later without blocking Phase 5.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| OpenLineage where verified + dbt lineage (chosen) | Honest, executable evidence; fits open stack | Not all tools emit OpenLineage | — |
| Marquez (OpenLineage backend) | Natural OpenLineage UI/store | Another service to operate | Candidate backend/UI |
| DataHub as lineage UI | Rich graph + discovery overlap | Heavier; overlaps D-016 | Optional later; cross-link discovery |
| Amundsen as lineage UI | Discovery-oriented | Lineage depth varies; overlaps D-016 | Optional later |
| dbt docs / lineage only | Simple for Gold | Misses Spark/ingest/CDC paths | Insufficient alone |
| Static architecture diagrams as “lineage” | Easy | Not tied to real runs | Rejected as evidence |

## Consequences and trade-offs

- Document gaps when a component lacks OpenLineage support instead of faking emitters.
- Discovery platforms (D-016) may consume lineage but do not replace emitters.

## Evidence / validation

None yet — Phase 5.

## Revisit when

No supported OpenLineage path for a critical component; narrow the claim and document the gap.
