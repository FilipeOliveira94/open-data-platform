# Brazilian E-Commerce Public Dataset by Olist

| Field | Value |
| --- | --- |
| Name | Brazilian E-Commerce Public Dataset by Olist |
| Type | Files (Kaggle) |
| Role | Historical orders, customers, items, payments, reviews, products, sellers, geolocation |
| URL | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Gold Domain | `olist_analysis` |
| License | [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (checked 2026-10-17) |
| Access | Fetch locally via Kaggle; do not vendor in git (`data/raw/…`) |
| Decision | ADR-0018 / D-018 |
| Related Issue | P1-04 (with [marketing funnel](olist-marketing-funnel.md); join `seller_id`) |

**Published files:** `olist_customers_dataset.csv`, `olist_geolocation_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_orders_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `product_category_name_translation.csv`.

```bash
mkdir -p data/raw/olist/brazilian-ecommerce
kaggle datasets download -d olistbr/brazilian-ecommerce \
  -p data/raw/olist/brazilian-ecommerce --unzip
```
