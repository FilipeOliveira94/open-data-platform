# ADR-0012: Lakehouse medallion layers

| Field | Value |
| --- | --- |
| Status | `approved` |
| Date opened | 2026-10-15 |
| Date decided | 2026-11-06 (source families recorded 2026-10-17; Gold schemas revised 2026-11-06; approved when Gold tables existed) |
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

**Gold schemas (separate analytical products — do not merge schemas):**

The 2026-10-17 split named two source families, `olist_analysis` and `product_analysis`. Ingestion contracts store that family on `governance.source_family`. P2-05 assigns each business audience its own Gold schema. One family feeds several schemas. DummyJSON still does not join Olist. The storefront still does not join either. PostgreSQL ids still do not join the historical orders.

| Schema | Audience | Primary sources |
| --- | --- | --- |
| `marketplace_operations` | Operations and finance | Olist orders, items, payments, products, sellers |
| `customer_crm` | CRM | Olist customers, reviews, purchase address |
| `seller_acquisition` | Marketing | Marketing funnel, plus seller sales |
| `brazil_geography` | Shared place reference | IBGE municipalities, Olist geolocation |
| `catalog_commerce` | Catalog | DummyJSON products |
| `storefront_behavior` | Storefront product and CRM | Demo clickstream |
| `oltp_operations` | Operations on the live database | PostgreSQL OLTP |

Layout is a dbt schema of the same name. The model list, grains, and quality findings are in [`docs/architecture/gold-model-design.md`](../architecture/gold-model-design.md). `dim_date` is shared. Marts, grains, and keys stay inside the schema except for the links that document names (calendar, municipality, ZIP prefix, and the seller match from a closed deal).

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

P2-03 (2026-10-31) loaded Bronze tables from raw objects. Detail: [`docs/architecture/bronze-layer.md`](../architecture/bronze-layer.md). P2-04 (2026-11-05) loaded typed Silver tables per `source_id`. Detail: [`docs/architecture/silver-layer.md`](../architecture/silver-layer.md). P2-05 (2026-11-06) documented Gold grain, keys, and marts. Detail: [`docs/architecture/gold-model-design.md`](../architecture/gold-model-design.md). P2-06 (2026-11-06) built the named Gold tables with dbt-trino. Detail: [`docs/architecture/gold-layer.md`](../architecture/gold-layer.md).

## Revisit when

PoC or source constraints force a different layering; update contracts and docs together.
