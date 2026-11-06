{{ config(
    schema='catalog_commerce',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('catalog_commerce', 'dim_catalog_products') ~ "'"
    }
) }}

select
  id as product_id,
  title,
  category,
  brand,
  price,
  dimensions_width,
  dimensions_height,
  dimensions_depth,
  meta_created_at as created_at,
  meta_updated_at as updated_at
from {{ source('silver', 'dummyjson_products__products') }}

