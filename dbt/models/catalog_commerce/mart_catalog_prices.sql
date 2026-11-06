{{ config(
    schema='catalog_commerce',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('catalog_commerce', 'mart_catalog_prices') ~ "'"
    }
) }}

select
  category,
  count(*) as product_count,
  min(price) as min_price,
  approx_percentile(price, 0.5) as median_price,
  max(price) as max_price,
  count_if(brand is null or trim(brand) = '') as products_missing_brand
from {{ ref('dim_catalog_products') }}
group by 1

