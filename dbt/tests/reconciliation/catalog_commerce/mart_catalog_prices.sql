with expected as (
  select
    category,
    count(*) as product_count,
    min(price) as min_price,
    approx_percentile(price, 0.5) as median_price,
    max(price) as max_price,
    count_if(brand is null or trim(brand) = '') as products_missing_brand
  from {{ ref('dim_catalog_products') }}
  group by 1
)
select
  'mart_catalog_prices' as check_name,
  cast(coalesce(m.category, e.category) as varchar) as left_value,
  'drift' as right_value
from {{ ref('mart_catalog_prices') }} m
full outer join expected e
  on e.category is not distinct from m.category
where m.product_count is distinct from e.product_count
   or m.min_price is distinct from e.min_price
   or m.median_price is distinct from e.median_price
   or m.max_price is distinct from e.max_price
   or m.products_missing_brand is distinct from e.products_missing_brand
   or m.category is null and e.category is not null
   or e.category is null and m.category is not null
