# ADR-0012: Lakehouse medallion layers

| Field | Value |
| --- | --- |
| Status | `proposed` |
| Date opened | 2026-10-15 |
| Date decided | — (Gold domain split recorded 2026-10-17; layer choice still proposed until Phase 2) |
| Owner | Platform architect |
| Index ID | D-012 |
| Related issues | Phase 2 (`P2-03`–`P2-06`) |
| Supersedes | — |
| Superseded by | — |

## Context / problem

Without explicit layer contracts, “Bronze/Silver/Gold” becomes a folder naming habit rather than an architecture. Consumers need to know what guarantees each layer provides.

## Decision

Adopt **Bronze / Silver / Gold**:
- **Bronze:** near-source payloads plus ingestion metadata; auditable; minimal destructive cleansing.
- **Silver:** typed, deduplicated, validated entities; intentional null/duplicate/invalid handling.
- **Gold:** business marts and analytical models (dbt) with documented grain and keys.

**Gold domains (separate analytical products — do not merge schemas):**

| Domain | Primary sources | Example layout |
| --- | --- | --- |
| `olist_analysis` | Olist files, Postgres seed/CDC, funnel, IBGE/CEP enrichment when present | e.g. `gold/olist_analysis/` or dbt package/schema `olist_analysis` |
| `product_analysis` | DummyJSON products (P1-05) | e.g. `gold/product_analysis/` or dbt package/schema `product_analysis` |

Shared platform infrastructure (Spark, Iceberg, catalog) may serve both; **marts, grains and keys stay domain-scoped**.

## Alternatives considered

| Option | Pros | Cons | Why not (if rejected) |
| --- | --- | --- | --- |
| Bronze / Silver / Gold (chosen) | Clear quality progression; common lakehouse practice | More paths to operate | — |
| Two-layer raw / curated only | Simpler | Weaker analytics vs engineering separation | Rejected |
| Warehouse-only (no lake layers) | Fewer stores | Conflicts with lakehouse + Iceberg path | Rejected |
| Skip Bronze (land straight into Silver) | Faster curated data | Loses auditable raw and replay story | Rejected |

## Consequences and trade-offs

- Layer contracts must be documented with grain/keys per table.
- Invalid records follow D-013; they are not silently dropped between layers.

## Evidence / validation

None yet — Phase 2 implementation.

## Revisit when

PoC or source constraints force a different layering; update contracts and docs together.
