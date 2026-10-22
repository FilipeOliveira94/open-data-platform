# Brazilian E-Commerce Public Dataset by Olist

| Field | Value |
| --- | --- |
| Name | Brazilian E-Commerce Public Dataset by Olist |
| Type | Files (Kaggle) |
| Role | Historical orders, customers, items, payments, reviews, products, sellers, geolocation |
| URL | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Gold Domain | `olist_analysis` |
| License | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (checked 2026-10-17) |
| Access | Package fetch via the Kaggle download API into gitignored `data/raw/…`. Set `KAGGLE_API_TOKEN`, or `KAGGLE_USERNAME` and `KAGGLE_KEY`. |
| Decision | ADR-0018 / D-018 |
| Related Issue | P1-04 (with [marketing funnel](olist-marketing-funnel.md)) |

**Published files:** `olist_customers_dataset.csv`, `olist_geolocation_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_orders_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `product_category_name_translation.csv`.

**Join:** `olist_closed_deals_dataset.seller_id` equals `olist_sellers_dataset.seller_id` or `olist_order_items_dataset.seller_id`. The raw landing keeps one object per CSV.

Slug: `olistbr/brazilian-ecommerce`. Default staging: `data/raw/olist/brazilian-ecommerce` (`ODP_OLIST_ECOMMERCE_DIR`).

From the repository root, with raw-bucket and MinIO settings in the environment:

```bash
uv run python -m open_data_platform.ingestion.olist --execution-id run-001
```

The command finishes the e-commerce slug, writing one CSV at a time, before it downloads the marketing funnel. A structural failure stops the remaining files. A second run with the same `--execution-id` and `--ingestion-ts` overwrites the same raw keys. `--force` downloads the zips again when the staged CSVs are already present.
