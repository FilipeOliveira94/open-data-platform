# Marketing Funnel by Olist

| Field | Value |
| --- | --- |
| Name | Marketing Funnel by Olist |
| Type | Files (Kaggle) |
| Role | Seller MQLs and closed deals; join to e-commerce on `seller_id` |
| URL | https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist |
| Gold Domain | `olist_analysis` |
| License | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (checked 2026-10-17) |
| Access | Fetch locally via Kaggle; do not vendor in git (`data/raw/…`) |
| Decision | ADR-0018 / D-018 |
| Related Issue | P1-04 (with [Brazilian e-commerce](olist-brazilian-ecommerce.md)) |

```bash
mkdir -p data/raw/olist/marketing-funnel
kaggle datasets download -d olistbr/marketing-funnel-olist \
  -p data/raw/olist/marketing-funnel --unzip
```
