{{ keys_differ(
  'dim_catalog_products',
  "select cast(product_id as varchar) as key_value from " ~ ref('dim_catalog_products'),
  "select cast(id as varchar) as key_value from " ~ source('silver', 'dummyjson_products__products')
) }}
