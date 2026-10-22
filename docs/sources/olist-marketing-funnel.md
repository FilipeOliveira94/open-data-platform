# Marketing Funnel by Olist

| Field | Value |
| --- | --- |
| Name | Marketing Funnel by Olist |
| Type | Files (Kaggle) |
| Role | Seller MQLs and closed deals |
| URL | https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist |
| Gold Domain | `olist_analysis` |
| License | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (checked 2026-10-17) |
| Access | Package fetch via the Kaggle download API into gitignored `data/raw/…`. Set `KAGGLE_API_TOKEN`, or `KAGGLE_USERNAME` and `KAGGLE_KEY`. |
| Decision | ADR-0018 / D-018 |
| Related Issue | P1-04 (with [Brazilian e-commerce](olist-brazilian-ecommerce.md)) |

**Published files:** `olist_marketing_qualified_leads_dataset.csv`, `olist_closed_deals_dataset.csv`.

**Join:** `olist_closed_deals_dataset.seller_id` equals `olist_sellers_dataset.seller_id` or `olist_order_items_dataset.seller_id`. `qualified_leads` has no `seller_id`. The raw landing keeps one object per CSV.

Slug: `olistbr/marketing-funnel-olist`. Default staging: `data/raw/olist_marketing_funnel` (`ODP_OLIST_FUNNEL_DIR`), the same name as the contract directory and `source_id`.

From the repository root, with raw-bucket and MinIO settings in the environment:

```bash
uv run python -m open_data_platform.ingestion.olist --execution-id run-001
```

The command finishes the e-commerce slug before this one. Each CSV is read and written on its own. A second run with the same `--execution-id` and `--ingestion-ts` overwrites the same raw keys. `--force` downloads the zips again when the staged CSVs are already present.
