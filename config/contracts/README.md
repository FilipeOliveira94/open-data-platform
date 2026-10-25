# Dataset contracts

Schema, null rules, classification, and provenance for Phase 1 sources. Vocabulary and evolution rules: [`docs/architecture/data-contracts.md`](../../docs/architecture/data-contracts.md).

Path: `config/contracts/{source_id}/{dataset}.yaml`.

| Source | Files |
| --- | --- |
| `olist_brazilian_ecommerce` | `customers`, `geolocation`, `order_items`, `order_payments`, `order_reviews`, `orders`, `products`, `sellers`, `product_category_name_translation` |
| `olist_marketing_funnel` | `qualified_leads`, `closed_deals` |
| `dummyjson_products` | `products` |
| `postgres_oltp` | `sellers`, `customers`, `orders`, `order_items` |
| `ibge_localidades` | `municipios` |
| `demo_storefront` | `clickstream` |

Not contracted here: BrasilAPI CEP (P1-09).

No Olist rows and no API payloads are stored in this directory.
