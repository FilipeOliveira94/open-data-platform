# Data sources

Final chosen sources. Rationale: [ADR-0018](../decisions/0018-historical-dataset-redistribution.md), [ADR-0019](../decisions/0019-public-api-ingestion-source.md). Gold domains: [ADR-0012](../decisions/0012-medallion-layers.md).

Each source note uses the same metadata fields: **Name**, **Type**, **Role**, **URL**, **Gold Domain**, **License**, **Access**, **Decision**, **Related Issue**.

| Name | Type | Gold Domain | Related Issue | Doc |
| --- | --- | --- | --- | --- |
| Brazilian E-Commerce Public Dataset by Olist | Files (Kaggle) | `olist_analysis` | P1-04 | [olist-brazilian-ecommerce.md](olist-brazilian-ecommerce.md) |
| Marketing Funnel by Olist | Files (Kaggle) | `olist_analysis` | P1-04 | [olist-marketing-funnel.md](olist-marketing-funnel.md) |
| DummyJSON products | HTTP (paginated) | `product_analysis` | P1-05 | [dummyjson-products.md](dummyjson-products.md) |
| PostgreSQL OLTP | Database | `olist_analysis` | P1-06 / Phase 4 | [postgres-oltp.md](postgres-oltp.md) |
| Demo storefront clickstream | Generated event log | `product_analysis` | P1-07 | [demo-storefront.md](demo-storefront.md) |
| IBGE Localidades | HTTP (collection) | `olist_analysis` | P1-08 | [ibge-localidades.md](ibge-localidades.md) |
| BrasilAPI CEP | HTTP (point lookup) | — | P1-09 (withdrawn) | [brasilapi-cep.md](brasilapi-cep.md) |

Olist binaries are not vendored (ADR-0018).
