# Data sources

Provenance, license and redistribution notes for planned ingestion sources.

| Source | Doc | License (as published) | Repo policy |
| --- | --- | --- | --- |
| Brazilian E-Commerce Public Dataset by Olist | [olist-brazilian-ecommerce.md](olist-brazilian-ecommerce.md) | CC BY-NC-SA 4.0 | Do not vendor; fetch locally |
| Marketing Funnel by Olist | [olist-marketing-funnel.md](olist-marketing-funnel.md) | CC BY-NC-SA 4.0 | Do not vendor; fetch locally |

Join key between the two Olist datasets: `seller_id` (documented on both Kaggle pages). Detailed join semantics belong in contracts/ingestion work (P1-03 / P1-04), not here.

**Policy:** see [ADR-0018](../decisions/0018-historical-dataset-redistribution.md) (D-018). The repository MIT license covers **software and documentation**, not bundled copies of these datasets.
