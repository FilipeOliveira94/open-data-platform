{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'dim_storefront_products') ~ "'"
    }
) }}

select distinct product_id
from {{ source('silver', 'demo_storefront__clickstream') }}
where product_id is not null

