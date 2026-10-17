# Brazilian E-Commerce Public Dataset by Olist

| Field | Value |
| --- | --- |
| Publisher | Olist (`olistbr` on Kaggle) |
| Canonical URL | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Checked on | 2026-10-17 |
| License (Kaggle listing) | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) |
| Legal code | https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode |
| Related dataset | [Marketing Funnel by Olist](olist-marketing-funnel.md) (join on `seller_id`) |
| Roadmap | P1-01 (license check); ingestion is P1-04 |

## Summary

Public Brazilian e-commerce orders dataset (~100k orders, 2016–2018) with customers, items, payments, reviews, products, sellers and geolocation. Real commercial data, anonymized. Companion marketing-funnel dataset is published separately and is in scope for the same historical source family.

## Files published (Kaggle, as of check date)

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

Exact schemas and null rules are deferred to data contracts (P1-03).

## License obligations (conservative reading)

Under **CC BY-NC-SA 4.0**, for NonCommercial use you may reproduce and share the material (and adapted material) only if you:

1. **Attribution (BY):** credit Olist / the Kaggle dataset, link to the license, and note changes if you adapt the data.
2. **NonCommercial (NC):** do not use the licensed material for commercial purposes as defined by the license.
3. **ShareAlike (SA):** if you **share** adapted material, apply CC BY-NC-SA 4.0 (or a CC-compatible SA license) to that adapted material.

Also respect **Kaggle’s terms of use** for download and redistribution; use the official dataset page/API as the acquisition channel.

This note is an operational summary for the program, not legal advice.

## Redistribution policy for this repository

| Action | Allowed in this repo? |
| --- | --- |
| Commit CSV / Parquet / zip copies of the dataset | **No** |
| Commit large derived dumps for redistribution | **No** |
| Document how to download locally for development | **Yes** |
| Keep local copies under ignored paths (e.g. `data/raw/`, `data/private/`) | **Yes** (developer machine only) |
| Tiny synthetic fixtures invented for unit tests (not copied from Olist) | **Yes** (P1-04+) |

Rationale: the repository is **MIT-licensed software**. Bundling CC BY-NC-SA datasets would mix incompatible redistribution terms and risk unauthorized public redistribution. See [ADR-0018](../decisions/0018-historical-dataset-redistribution.md).

## Fetch-from-source (local only)

Do **not** add dataset binaries to git. After Kaggle authentication on the developer machine:

```bash
# Example only — requires a local Kaggle API token (~/.kaggle/kaggle.json).
# Never commit kaggle.json or downloaded archives.
mkdir -p data/raw/olist/brazilian-ecommerce
kaggle datasets download -d olistbr/brazilian-ecommerce \
  -p data/raw/olist/brazilian-ecommerce --unzip
```

`.gitignore` already ignores `data/raw/`, `data/private/`, `*.parquet` and `*.csv.gz`. Prefer keeping unpacked CSVs under `data/raw/` so they stay untracked.

Ingestion automation (P1-04) must download or read from a configured local path; it must not assume the files are in version control.

## Attribution text (suggested)

> Brazilian E-Commerce Public Dataset by Olist, available at
> https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce,
> licensed under CC BY-NC-SA 4.0
> (https://creativecommons.org/licenses/by-nc-sa/4.0/).

## Out of scope for P1-01

Downloading data, writing ingest code, defining Iceberg/Bronze layouts, and publishing adapted tables. Those belong to later Phase 1 issues.
