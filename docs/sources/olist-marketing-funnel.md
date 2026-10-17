# Marketing Funnel by Olist

| Field | Value |
| --- | --- |
| Publisher | Olist (`olistbr` on Kaggle) |
| Canonical URL | https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist |
| Checked on | 2026-10-17 |
| License (Kaggle listing) | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) |
| Legal code | https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode |
| Related dataset | [Brazilian E-Commerce Public Dataset by Olist](olist-brazilian-ecommerce.md) |
| Join key (publisher) | `seller_id` |
| Roadmap | P1-01 (license check); ingestion with the e-commerce set is P1-04 |

## Summary

Marketing funnel dataset for sellers that requested contact to sell on Olist (~8k marketing qualified leads sampled between 2017-06-01 and 2018-06-01), plus closed-deal attributes. Real data, anonymized and sampled. Intended to be joined with the Brazilian e-commerce orders dataset on `seller_id` so an order can be viewed from a marketing/sales-funnel perspective.

## Join with Brazilian e-commerce

| Side | Dataset | Link field |
| --- | --- | --- |
| Funnel / closed deals | This dataset | `seller_id` (present after a deal is won) |
| Orders / sellers | [olist-brazilian-ecommerce](olist-brazilian-ecommerce.md) | `seller_id` in seller and order-item tables |

Publisher notes: a seller MQL may come from multiple sources; not every lead becomes a seller with orders. Grain and null rules for the join are deferred to contracts (P1-03) and ingestion (P1-04).

## Files published (Kaggle, as of check date)

Kaggle publishes multiple CSVs for MQLs and closed deals (exact filenames to be locked when contracts are written). Do not treat this list as a schema contract.

## License obligations (conservative reading)

Same license family as the e-commerce dataset: **CC BY-NC-SA 4.0**.

1. **Attribution (BY):** credit Olist / the Kaggle dataset, link to the license, note changes if adapted.
2. **NonCommercial (NC):** non-commercial use of the licensed material only.
3. **ShareAlike (SA):** shared adapted material must use a compatible BY-NC-SA license.

Respect **Kaggle’s terms of use**. This note is an operational summary, not legal advice.

## Redistribution policy for this repository

Identical to the e-commerce dataset:

| Action | Allowed in this repo? |
| --- | --- |
| Commit CSV / Parquet / zip copies | **No** |
| Commit large derived dumps for redistribution | **No** |
| Document local download | **Yes** |
| Local copies under ignored paths | **Yes** |
| Synthetic unit-test fixtures (not copied from Olist) | **Yes** (P1-04+) |

See [ADR-0018](../decisions/0018-historical-dataset-redistribution.md).

## Fetch-from-source (local only)

```bash
# Example only — requires a local Kaggle API token (~/.kaggle/kaggle.json).
# Never commit kaggle.json or downloaded archives.
mkdir -p data/raw/olist/marketing-funnel
kaggle datasets download -d olistbr/marketing-funnel-olist \
  -p data/raw/olist/marketing-funnel --unzip
```

Keep files under `data/raw/` (gitignored). P1-04 must not require these files to be committed.

## Attribution text (suggested)

> Marketing Funnel by Olist, available at
> https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist,
> licensed under CC BY-NC-SA 4.0
> (https://creativecommons.org/licenses/by-nc-sa/4.0/).

## Out of scope for P1-01

Download automation, join implementation, contracts and Bronze writers.
